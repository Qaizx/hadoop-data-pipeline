# jobs/utils/retry.py
"""
Retry + Atomic write utility สำหรับ Spark ETL

Config ใน .env:
    ETL_MAX_RETRIES=3
    ETL_RETRY_DELAY=5
"""

import os
import time
from typing import Callable, Any
from pyspark.sql import DataFrame

from logger import get_logger

log = get_logger(__name__)

# ── Config (อ่านจาก env) ──────────────────────────────────────────
MAX_RETRIES = int(os.getenv("ETL_MAX_RETRIES", 3))
RETRY_DELAY = int(os.getenv("ETL_RETRY_DELAY", 5))  # วินาที (x2 ทุกรอบ)
# ─────────────────────────────────────────────────────────────────


def with_retry(fn: Callable, *args, label: str = "", max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY, **kwargs) -> Any:
    """
    รัน function พร้อม retry และ exponential backoff

    Usage:
        with_retry(some_function, arg1, arg2, label="read CSV")
    """
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result = fn(*args, **kwargs)
            if attempt > 1:
                log.info("Retry succeeded", label=label, attempt=attempt)
            return result
        except Exception as e:
            last_error = e
            wait = delay * (2 ** (attempt - 1))  # 5 → 10 → 20 วินาที
            if attempt < max_retries:
                log.warning(
                    "Step failed — retrying",
                    label=label,
                    attempt=attempt,
                    max_retries=max_retries,
                    wait_seconds=wait,
                    error=str(e),
                )
                time.sleep(wait)
            else:
                log.error(
                    "All retries exhausted",
                    label=label,
                    attempt=attempt,
                    max_retries=max_retries,
                    error=str(e),
                )
    raise last_error


def atomic_write_table(
    df: DataFrame,
    table_path: str,
    table_name: str,
    database: str,
    partition_col: str = "year",
    partition_value: Any = None,
    max_retries: int = MAX_RETRIES,
):
    """
    Atomic write ด้วย swap pattern เฉพาะ partition ที่เปลี่ยน:
        /datalake/.../year=2023/  ← ไม่แตะ
        /datalake/.../year=2024/  ← swap เฉพาะตรงนี้
        /datalake/.../year=2025/  ← ไม่แตะ

    Flow:
        1. เขียนลง {partition_path}_tmp
        2. rename partition เดิม → _old  (backup)
        3. rename _tmp → partition        (สลับทันที)
        4. ลบ _old                        (ค่อยลบทีหลัง)

    ถ้า crash ทุก step มี recovery:
        - crash step 1 → partition เดิมยังอยู่, ลบ _tmp ทิ้ง
        - crash step 2 → partition เดิมยังอยู่
        - crash step 3 → มี _old เป็น backup, rollback ได้
        - crash step 4 → partition ใหม่ใช้งานได้แล้ว แค่ _old ค้างอยู่
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # path ของ partition จริงๆ เช่น /datalake/.../year=2024
    partition_path = f"{table_path}/{partition_col}={partition_value}"
    tmp_path = f"{partition_path}_tmp"

    def _do_write():
        log.info("Writing to temp partition", tmp_path=tmp_path, table=table_name, partition_value=partition_value)

        # เขียนลง _tmp เฉพาะ partition นี้
        df.write.mode("overwrite") \
            .option("path", tmp_path) \
            .parquet(tmp_path)

        log.info("Swapping partition", table=table_name, partition_value=partition_value)
        _hdfs_swap(sc, tmp_path, partition_path)

        # อัพเดท Hive metastore
        spark.sql(f"ALTER TABLE {database}.{table_name} DROP IF EXISTS PARTITION ({partition_col}={partition_value})")
        spark.sql(f"ALTER TABLE {database}.{table_name} ADD PARTITION ({partition_col}={partition_value}) LOCATION '{partition_path}'")

        log.info("Atomic write completed", table=table_name, partition_value=partition_value)

    def _cleanup_tmp():
        try:
            _hdfs_delete(sc, tmp_path)
            log.info("Temp partition cleaned up", tmp_path=tmp_path)
        except Exception as e:
            log.warning("Could not clean up temp partition", tmp_path=tmp_path, error=str(e))

    try:
        with_retry(_do_write, label=f"write {table_name} partition={partition_value}", max_retries=max_retries)
    except Exception as e:
        log.error("Atomic write failed — cleaning up", table=table_name, partition_value=partition_value, error=str(e))
        _cleanup_tmp()
        raise


def _hdfs_swap(sc, src: str, dst: str):
    """
    Swap pattern — ป้องกันข้อมูลสูญหาย
        1. rename dst → dst_old  (backup)
        2. rename src → dst      (สลับทันที)
        3. ลบ dst_old            (ค่อยลบทีหลัง)
    """
    fs = _get_fs(sc)
    src_path = sc._jvm.org.apache.hadoop.fs.Path(src)
    dst_path = sc._jvm.org.apache.hadoop.fs.Path(dst)
    old_path = sc._jvm.org.apache.hadoop.fs.Path(f"{dst}_old")

    # Step 1: backup partition เดิม (ถ้ามี)
    if fs.exists(dst_path):
        if fs.exists(old_path):
            fs.delete(old_path, True)  # ลบ _old เก่าที่ค้างอยู่
        success = fs.rename(dst_path, old_path)
        if not success:
            raise RuntimeError(f"HDFS backup failed: {dst} -> {dst}_old")
        log.info("Partition backed up", src=dst, backup=f"{dst}_old")

    # Step 2: สลับ _tmp → partition
    success = fs.rename(src_path, dst_path)
    if not success:
        # rollback: คืน _old กลับ
        if fs.exists(old_path):
            fs.rename(old_path, dst_path)
            log.warning("Rename failed — rolled back from backup", dst=dst)
        raise RuntimeError(f"HDFS rename failed: {src} -> {dst}")

    log.info("Partition swapped", new=dst)

    # Step 3: ลบ backup
    if fs.exists(old_path):
        fs.delete(old_path, True)
        log.info("Backup deleted", path=f"{dst}_old")


def _hdfs_delete(sc, path: str):
    fs = _get_fs(sc)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(hadoop_path):
        fs.delete(hadoop_path, True)


def _get_fs(sc):
    uri = sc._jvm.java.net.URI.create("hdfs://namenode:8020")
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
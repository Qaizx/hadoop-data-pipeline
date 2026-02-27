# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_extract, input_file_name, expr
import subprocess
from typing import List, Dict, Optional

spark = SparkSession.builder \
    .appName("Finance ITSC ETL") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

# ===== Config =====
raw_path = "hdfs://namenode:8020/datalake/raw/finance-itsc"
staging_path = "hdfs://namenode:8020/datalake/staging/finance-itsc_wide"
curated_path = "hdfs://namenode:8020/datalake/curated/finance-itsc_long"
wide_table = "finance_itsc_wide"
long_table = "finance_itsc_long"
database_name = "default"


# ============================================================
# Helper: HDFS utils ใช้ Spark FileSystem API
# ============================================================

def get_fs():
    """ดึง Hadoop FileSystem object"""
    uri = sc._jvm.java.net.URI.create(raw_path)
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)


def hdfs_ls_recursive(path: str) -> List[str]:
    """List ไฟล์ทั้งหมดใน path แบบ recursive"""
    fs = get_fs()
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    
    if not fs.exists(hadoop_path):
        return []
    
    files = []
    iterator = fs.listFiles(hadoop_path, True)  # recursive=True
    while iterator.hasNext():
        status = iterator.next()
        files.append(status.getPath().toString())
    return files


def hdfs_exists(path: str) -> bool:
    fs = get_fs()
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))


def hdfs_touch(path: str):
    """สร้าง marker file เปล่า"""
    fs = get_fs()
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs.create(hadoop_path).close()
    print(f"   ✅ Marker created: {path}")


def extract_year_from_path(path: str) -> Optional[int]:
    import re
    m = re.search(r"year=(\d{4})", path)
    return int(m.group(1)) if m else None


# ============================================================
# PART 1: Raw -> Staging (Wide) — Incremental
# ============================================================
print("=" * 60)
print("PART 1: Raw -> Staging (Wide) — Incremental")
print("=" * 60)

all_files = hdfs_ls_recursive(raw_path)

csv_files = [f for f in all_files if f.endswith(".csv")]
done_files = set(f for f in all_files if f.endswith(".done"))

pending_files = [f for f in csv_files if f + ".done" not in done_files]

print(f"\n📁 CSV files found   : {len(csv_files)}")
print(f"✅ Already processed : {len(csv_files) - len(pending_files)}")
print(f"🆕 Pending files     : {len(pending_files)}")

if not pending_files:
    print("\n⏭️  No new files to process. Skipping Part 1.")
    pending_by_year: Dict[int, List[str]] = {}
else:
    pending_by_year = {}
    for f in pending_files:
        year = extract_year_from_path(f)
        if year:
            pending_by_year.setdefault(year, []).append(f)
        else:
            print(f"   ⚠️  Cannot extract year from: {f} — skipping")

    print(f"\n📅 Years to update: {sorted(pending_by_year.keys())}")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    for year, files in sorted(pending_by_year.items()):
        print(f"\n🔄 Processing year={year} ({len(files)} file(s))")

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(files)

        df = df.withColumn("year", lit(year).cast("int"))

        for c in df.columns:
            if c in ["date", "details"]:
                df = df.withColumn(c, col(c).cast("string"))
            elif c == "year":
                pass
            else:
                df = df.withColumn(c, col(c).cast("double"))

        print(f"   Rows: {df.count()}")

        df.write \
            .mode("overwrite") \
            .partitionBy("year") \
            .option("path", staging_path) \
            .saveAsTable(f"{database_name}.{wide_table}")

        print(f"   ✅ Written to wide table partition year={year}")

        for f in files:
            hdfs_touch(f + ".done")

print(f"\n✅ Part 1 done!")


# ============================================================
# PART 2: Staging (Wide) -> Curated (Long)
# ============================================================
print("\n" + "=" * 60)
print("PART 2: Staging (Wide) -> Curated (Long)")
print("=" * 60)

years_to_update = sorted(pending_by_year.keys()) if pending_files else []

if not years_to_update:
    print("\n⏭️  No new data. Skipping Part 2.")
else:
    print(f"\n📅 Years to update in long table: {years_to_update}")

    id_columns = ["date", "details", "year"]
    exclude_columns = ["total_amount"]

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    for year in years_to_update:
        print(f"\n🔄 Converting year={year} wide -> long")

        df_wide = spark.sql(f"""
            SELECT * FROM {database_name}.{wide_table}
            WHERE year = {year}
        """)

        df_wide = df_wide.filter(
            col("date").rlike(r"^\d{4}-\d{2}$") |
            (col("date") == "all-year-budget")
        )

        amount_columns = [
            c for c in df_wide.columns
            if c not in id_columns and c not in exclude_columns
        ]

        print(f"   Amount columns: {len(amount_columns)}")

        stack_expr = ", ".join([f"'{c}', `{c}`" for c in amount_columns])
        n_cols = len(amount_columns)

        df_long = df_wide.select(
            *id_columns,
            expr(f"stack({n_cols}, {stack_expr}) as (category, amount)")
        ).filter(col("amount").isNotNull())

        print(f"   Long rows: {df_long.count()}")

        df_long.write \
            .mode("overwrite") \
            .partitionBy("year") \
            .option("path", curated_path) \
            .saveAsTable(f"{database_name}.{long_table}")

        print(f"   ✅ Long table partition year={year} updated")

print("\n✅ ETL Pipeline completed!")
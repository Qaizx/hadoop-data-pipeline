# -*- coding: utf-8 -*-
# jobs/finance_itsc_pipeline_test_quality.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr
from typing import List, Dict

from data_quality import run_quality_checks
from utils.hdfs import hdfs_ls_recursive, hdfs_touch, extract_year_from_path
from utils.alerts import send_quality_alert
from utils.retry import atomic_write_table, with_retry
from utils.versioning import create_version, cleanup_old_versions
from logger import setup_logger

log = setup_logger("etl")

spark = SparkSession.builder \
    .appName("Finance ITSC ETL") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

# ===== Config =====
raw_path      = "hdfs://namenode:8020/datalake/raw/finance-itsc"
staging_path  = "hdfs://namenode:8020/datalake/staging/finance-itsc_wide"
curated_path  = "hdfs://namenode:8020/datalake/curated/finance-itsc_long"
wide_table    = "finance_itsc_wide"
long_table    = "finance_itsc_long"
database_name = "default"


# ============================================================
# PART 1: Raw -> Staging (Wide) — Incremental
# ============================================================
log.info("PART 1 started: Raw -> Staging (Wide) — Incremental")

# ── Step 1: Scan HDFS ────────────────────────────────────────
all_files = with_retry(hdfs_ls_recursive, sc, raw_path, label="scan HDFS")

csv_files    = [f for f in all_files if f.endswith(".csv")]
done_files   = set(f for f in all_files if f.endswith(".done"))
failed_files = set(f for f in all_files if f.endswith(".failed"))

pending_files = [
    f for f in csv_files
    if f + ".done" not in done_files and f + ".failed" not in failed_files
]

log.info(
    "File scan complete",
    csv_found=len(csv_files),
    already_processed=len([f for f in csv_files if f + ".done" in done_files]),
    dq_failed=len([f for f in csv_files if f + ".failed" in failed_files]),
    pending=len(pending_files),
)

pending_by_year: Dict[int, List[str]] = {}

if not pending_files:
    log.info("No new files to process — skipping Part 1")
else:
    for f in pending_files:
        year = extract_year_from_path(f)
        if year:
            pending_by_year.setdefault(year, []).append(f)
        else:
            log.warning("Cannot extract year from file — skipping", file=f)

    log.info("Years to update", years=sorted(pending_by_year.keys()))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    for year, files in sorted(pending_by_year.items()):
        log.info("Processing year", year=year, files=len(files))

        # ── Step 2: Read CSV ─────────────────────────────────
        try:
            def _read_csv():
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(files)
                df = df.withColumn("year", lit(year).cast("int"))
                for c in df.columns:
                    if c in ["date", "details"]:
                        df = df.withColumn(c, col(c).cast("string"))
                    elif c != "year":
                        df = df.withColumn(c, col(c).cast("double"))
                return df

            df = with_retry(_read_csv, label=f"read CSV year={year}")
        except Exception as e:
            log.error("Failed to read CSV — skipping year", year=year, error=str(e))
            del pending_by_year[year]
            continue

        # ── Step 3: Data Quality ──────────────────────────────
        try:
            dq_passed, dq_report = with_retry(
                run_quality_checks, df, files[0],
                label=f"data quality year={year}"
            )
        except Exception as e:
            log.error("Data quality check error — skipping year", year=year, error=str(e))
            for f in files:
                hdfs_touch(sc, f + ".failed")
            send_quality_alert(files[0], dq_report)
            del pending_by_year[year]
            continue

        if not dq_passed:
            log.error("DQ failed — skipping load", year=year)
            for f in files:
                hdfs_touch(sc, f + ".failed")
            send_quality_alert(files[0], dq_report)
            del pending_by_year[year]
            continue

        # ── Step 4: Atomic Write ──────────────────────────────
        try:
            atomic_write_table(
                df=df,
                table_path=staging_path,
                table_name=wide_table,
                database=database_name,
                partition_col="year",
                partition_value=year,
            )
            for f in files:
                with_retry(hdfs_touch, sc, f + ".done", label=f"touch .done {f}")

            # ===== VERSIONING =====
            create_version(sc, df, files[0], year)
            cleanup_old_versions(sc, year)

            log.info("Year written to staging", year=year)
        except Exception as e:
            log.error("Failed to write staging after retries — skipping", year=year, error=str(e))
            del pending_by_year[year]
            continue

log.info("PART 1 completed")


# ============================================================
# PART 2: Staging (Wide) -> Curated (Long)
# ============================================================
log.info("PART 2 started: Staging (Wide) -> Curated (Long)")

years_to_update = sorted(pending_by_year.keys())

if not years_to_update:
    log.info("No new data — skipping Part 2")
else:
    id_columns      = ["date", "details", "year"]
    exclude_columns = ["total_amount"]
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    for year in years_to_update:
        log.info("Converting wide -> long", year=year)

        # ── Step 5: Read Wide ─────────────────────────────────
        try:
            def _read_wide():
                df = spark.sql(f"SELECT * FROM {database_name}.{wide_table} WHERE year = {year}")
                return df.filter(
                    col("date").rlike(r"^\d{4}-\d{2}$") | (col("date") == "all-year-budget")
                )

            df_wide = with_retry(_read_wide, label=f"read wide year={year}")
        except Exception as e:
            log.error("Failed to read wide table — skipping year", year=year, error=str(e))
            continue

        # ── Step 6: Transform Wide -> Long ────────────────────
        try:
            def _transform():
                amount_cols = [c for c in df_wide.columns if c not in id_columns + exclude_columns]
                stack_expr = ", ".join([f"'{c}', `{c}`" for c in amount_cols])
                return df_wide.select(
                    *id_columns,
                    expr(f"stack({len(amount_cols)}, {stack_expr}) as (category, amount)")
                ).filter(col("amount").isNotNull())

            df_long = with_retry(_transform, label=f"transform wide->long year={year}")
        except Exception as e:
            log.error("Failed to transform wide->long — skipping year", year=year, error=str(e))
            continue

        long_rows = df_long.count()

        # ── Step 7: Atomic Write ──────────────────────────────
        try:
            atomic_write_table(
                df=df_long,
                table_path=curated_path,
                table_name=long_table,
                database=database_name,
                partition_col="year",
                partition_value=year,
            )
            log.info("Long table updated", year=year, rows=long_rows)
        except Exception as e:
            log.error("Failed to write curated after retries", year=year, error=str(e))

log.info("ETL Pipeline completed")
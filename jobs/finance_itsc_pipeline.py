# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr
from typing import List, Dict

from utils.hdfs import hdfs_ls_recursive, hdfs_touch, extract_year_from_path
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

all_files     = hdfs_ls_recursive(sc, raw_path)
csv_files     = [f for f in all_files if f.endswith(".csv")]
done_files    = set(f for f in all_files if f.endswith(".done"))
pending_files = [f for f in csv_files if f + ".done" not in done_files]

log.info(
    "File scan complete",
    csv_found=len(csv_files),
    already_processed=len(csv_files) - len(pending_files),
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

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(files)
        df = df.withColumn("year", lit(year).cast("int"))
        for c in df.columns:
            if c in ["date", "details"]:
                df = df.withColumn(c, col(c).cast("string"))
            elif c != "year":
                df = df.withColumn(c, col(c).cast("double"))

        row_count = df.count()
        log.info("Rows read", year=year, rows=row_count)

        df.write.mode("overwrite").partitionBy("year") \
            .option("path", staging_path) \
            .saveAsTable(f"{database_name}.{wide_table}")

        for f in files:
            hdfs_touch(sc, f + ".done")

        log.info("Year written to staging", year=year, rows=row_count)

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

        df_wide = spark.sql(f"SELECT * FROM {database_name}.{wide_table} WHERE year = {year}")
        df_wide = df_wide.filter(
            col("date").rlike(r"^\d{4}-\d{2}$") | (col("date") == "all-year-budget")
        )

        amount_columns = [c for c in df_wide.columns if c not in id_columns + exclude_columns]
        stack_expr = ", ".join([f"'{c}', `{c}`" for c in amount_columns])

        df_long = df_wide.select(
            *id_columns,
            expr(f"stack({len(amount_columns)}, {stack_expr}) as (category, amount)")
        ).filter(col("amount").isNotNull())

        long_rows = df_long.count()
        log.info("Long table rows", year=year, rows=long_rows)

        df_long.write.mode("overwrite").partitionBy("year") \
            .option("path", curated_path) \
            .saveAsTable(f"{database_name}.{long_table}")

        log.info("Long table updated", year=year, rows=long_rows)

log.info("ETL Pipeline completed")
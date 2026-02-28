# -*- coding: utf-8 -*-
# jobs/finance_itsc_pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr
from typing import List, Dict

from data_quality import run_quality_checks
from utils.hdfs import hdfs_ls_recursive, hdfs_touch, extract_year_from_path
from utils.alerts import send_quality_alert

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
print("=" * 60)
print("PART 1: Raw -> Staging (Wide) — Incremental")
print("=" * 60)

all_files    = hdfs_ls_recursive(sc, raw_path)
csv_files    = [f for f in all_files if f.endswith(".csv")]
done_files   = set(f for f in all_files if f.endswith(".done"))
failed_files = set(f for f in all_files if f.endswith(".failed"))

pending_files = [
    f for f in csv_files
    if f + ".done" not in done_files and f + ".failed" not in failed_files
]

print(f"\n📁 CSV files found   : {len(csv_files)}")
print(f"✅ Already processed : {len([f for f in csv_files if f + '.done' in done_files])}")
print(f"❌ Failed (DQ)       : {len([f for f in csv_files if f + '.failed' in failed_files])}")
print(f"🆕 Pending files     : {len(pending_files)}")

pending_by_year: Dict[int, List[str]] = {}

if not pending_files:
    print("\n⏭️  No new files. Skipping Part 1.")
else:
    for f in pending_files:
        year = extract_year_from_path(f)
        if year:
            pending_by_year.setdefault(year, []).append(f)
        else:
            print(f"   ⚠️  Cannot extract year: {f} — skipping")

    print(f"\n📅 Years to update: {sorted(pending_by_year.keys())}")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    for year, files in sorted(pending_by_year.items()):
        print(f"\n🔄 Processing year={year} ({len(files)} file(s))")

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(files)
        df = df.withColumn("year", lit(year).cast("int"))
        for c in df.columns:
            if c in ["date", "details"]:
                df = df.withColumn(c, col(c).cast("string"))
            elif c != "year":
                df = df.withColumn(c, col(c).cast("double"))

        # ===== DATA QUALITY =====
        dq_passed, dq_report = run_quality_checks(df, files[0])

        if not dq_passed:
            print(f"\n❌ DQ FAILED year={year} — skipping load")
            for f in files:
                hdfs_touch(sc, f + ".failed")
            send_quality_alert(files[0], dq_report)
            del pending_by_year[year]
            continue

        df.write.mode("overwrite").partitionBy("year") \
            .option("path", staging_path) \
            .saveAsTable(f"{database_name}.{wide_table}")

        for f in files:
            hdfs_touch(sc, f + ".done")
        print(f"   ✅ year={year} written")

print(f"\n✅ Part 1 done!")


# ============================================================
# PART 2: Staging (Wide) -> Curated (Long)
# ============================================================
print("\n" + "=" * 60)
print("PART 2: Staging (Wide) -> Curated (Long)")
print("=" * 60)

years_to_update = sorted(pending_by_year.keys())

if not years_to_update:
    print("\n⏭️  No new data. Skipping Part 2.")
else:
    id_columns      = ["date", "details", "year"]
    exclude_columns = ["total_amount"]
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    for year in years_to_update:
        print(f"\n🔄 Converting year={year} wide -> long")

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

        df_long.write.mode("overwrite").partitionBy("year") \
            .option("path", curated_path) \
            .saveAsTable(f"{database_name}.{long_table}")

        print(f"   ✅ year={year} long table updated ({df_long.count()} rows)")

print("\n✅ ETL Pipeline completed!")
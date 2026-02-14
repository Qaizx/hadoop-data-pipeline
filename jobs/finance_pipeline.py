# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, col
import re

spark = SparkSession.builder \
    .appName("Finance ETL Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

RAW_BASE = "hdfs://namenode:8020/datalake/raw/finance"
STAGING_BASE = "hdfs://namenode:8020/datalake/staging/finance_wide"
CURATED_BASE = "hdfs://namenode:8020/datalake/curated/finance_long"

print("Reading raw finance data...")

# Read all year partitions
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("{}/year=*/*.csv".format(RAW_BASE))

# print(df)

# # Extract year from file path
df = df.withColumn("source_file", input_file_name())

df = df.withColumn(
    "year",
    regexp_extract("source_file", r"finance_(\d{4})\.csv", 1).cast("int")
)

# df = df.drop("source_file")

# # ========================
# # STEP 1: RAW → STAGING
# # ========================

print("Cleaning wide table...")

# df = df.dropDuplicates()

key_cols = ["period_month", "transaction_type", "year"]

for c in df.columns:
    if c not in key_cols:
        df = df.withColumn(c, col(c).cast("double"))

print("Writing staging...")

df = df.repartition("year")

df.write \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year") \
    .parquet(STAGING_BASE)

print("Repairing Hive partitions...")

spark.sql("MSCK REPAIR TABLE finance_wide")

# ========================
# STEP 2: STAGING → CURATED
# ========================

print("Reading staging data...")

staging_df = spark.read.parquet(STAGING_BASE)

print("Wide → long transform...")

key_cols = ["period_month", "transaction_type", "year"]

value_cols = [
    c for c in staging_df.columns
    if c not in key_cols + ["source_file"]
]

# Build stack expression
stack_expr = "stack({}, {}) as (account_name, amount)".format(
    len(value_cols),
    ",".join(["'{}', {}".format(c, c) for c in value_cols])
)

long_df = staging_df.selectExpr(
    "period_month",
    "transaction_type",
    "year",
    stack_expr
)

# Remove null / zero values (optional but recommended)
long_df = long_df.filter(
    (col("amount").isNotNull()) & (col("amount") != 0)
)

print("Writing curated...")

long_df = long_df.repartition("year")

long_df.write \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("year") \
    .parquet(CURATED_BASE)

print("Repairing Hive partitions...")

spark.sql("MSCK REPAIR TABLE finance_long")

print("Pipeline completed!")


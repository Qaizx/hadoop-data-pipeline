# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name, regexp_extract, expr

spark = SparkSession.builder \
    .appName("Finance ITSC ETL") \
    .enableHiveSupport() \
    .getOrCreate()

# ===== Config =====
raw_path = "hdfs://namenode:8020/datalake/raw/finance-itsc"
staging_path = "hdfs://namenode:8020/datalake/staging/finance-itsc_wide"
curated_path = "hdfs://namenode:8020/datalake/curated/finance-itsc_long"
wide_table = "finance_itsc_wide"
long_table = "finance_itsc_long"
database_name = "default"

# ============================================================
# PART 1: Raw -> Staging (Wide)
# ============================================================
print("=" * 60)
print("PART 1: Raw -> Staging (Wide)")
print("=" * 60)

print(f"\n📖 Reading from: {raw_path}")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("basePath", raw_path) \
    .csv(f"{raw_path}/year=*")

# ดึง year จาก path ถ้ายังไม่มี column year
if "year" not in df.columns:
    df = df.withColumn(
        "year",
        regexp_extract(input_file_name(), r"year=(\d+)", 1).cast("int")
    )

print(f"   Rows: {df.count()}, Columns: {len(df.columns)}")
print(f"   Years: {[r.year for r in df.select('year').distinct().collect()]}")

# Cast columns ให้ถูกต้อง
for c in df.columns:
    if c in ['date', 'details']:
        df = df.withColumn(c, col(c).cast("string"))
    elif c == 'year':
        df = df.withColumn(c, col(c).cast("int"))
    else:
        df = df.withColumn(c, col(c).cast("double"))

# เขียนไป staging พร้อม partition by year
print(f"\n📝 Writing to: {staging_path}")

df.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .option("path", staging_path) \
    .saveAsTable(f"{database_name}.{wide_table}")

print(f"\n✅ Wide table done!")

# ============================================================
# PART 2: Staging (Wide) -> Curated (Long)
# ============================================================
print("\n" + "=" * 60)
print("PART 2: Staging (Wide) -> Curated (Long)")
print("=" * 60)

print(f"\n📖 Reading from: {database_name}.{wide_table}")
df_wide = spark.sql(f"SELECT * FROM {database_name}.{wide_table}")

# ===== กรองข้อมูล =====
print(f"\n🔍 Before filter: {df_wide.count()} rows")

# แสดง date ทั้งหมดก่อน filter
print("   Unique dates:")
df_wide.select("date").distinct().show(truncate=False)

# กรองเฉพาะ date ที่เป็นรูปแบบ YYYY-MM (เช่น 2023-10, 2024-01)
df_wide = df_wide.filter(
    col("date").rlike(r"^\d{4}-\d{2}$") | 
    (col("date") == "all-year-budget")
)

print(f"   After filter: {df_wide.count()} rows")

# หา columns ที่เป็น amount
id_columns = ['date', 'details', 'year']
exclude_columns = ['total_amount']  # columns ที่ไม่ต้องการ
amount_columns = [c for c in df_wide.columns if c not in id_columns and c not in exclude_columns]

print(f"   ID columns: {id_columns}")
print(f"   Excluded columns: {exclude_columns}")
print(f"   Amount columns: {len(amount_columns)}")

# Unpivot (wide to long)
stack_expr = ", ".join([f"'{c}', `{c}`" for c in amount_columns])
n_cols = len(amount_columns)

df_long = df_wide.select(
    *id_columns,
    expr(f"stack({n_cols}, {stack_expr}) as (category, amount)")
)

# Clean data - ลบ rows ที่ amount เป็น null
df_long = df_long.filter(col("amount").isNotNull())

print(f"\n📊 Long format:")
print(f"   Rows: {df_long.count()}, Columns: {len(df_long.columns)}")

# เขียนไป curated
print(f"\n📝 Writing to: {curated_path}")

df_long.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .option("path", curated_path) \
    .saveAsTable(f"{database_name}.{long_table}")

print(f"\n✅ Long table done!")

# ============================================================
# PART 3: Testing
# ============================================================
print("\n" + "=" * 60)
print("PART 3: Testing")
print("=" * 60)

print(f"\n🔍 Check details values:")
spark.sql(f"""
    SELECT details, COUNT(*) as cnt 
    FROM {database_name}.{long_table} 
    GROUP BY details
""").show()

print(f"\n🔍 Check date values:")
spark.sql(f"""
    SELECT date, COUNT(*) as cnt 
    FROM {database_name}.{long_table} 
    GROUP BY date 
    ORDER BY date
""").show()

print(f"\n🔍 Total spent:")
spark.sql(f"""
    SELECT SUM(amount) as total_spent
    FROM {database_name}.{long_table}
    WHERE details = 'spent' AND year = 2024
""").show()

print(f"\n🔍 Total budget:")
spark.sql(f"""
    SELECT SUM(amount) as total_budget
    FROM {database_name}.{long_table}
    WHERE details = 'budget' AND year = 2024
""").show()

print(f"\n📈 Budget vs Spent by month:")
spark.sql(f"""
    SELECT 
        date,
        SUM(CASE WHEN details = 'budget' THEN amount ELSE 0 END) as budget,
        SUM(CASE WHEN details = 'spent' THEN amount ELSE 0 END) as spent,
        SUM(CASE WHEN details = 'remaining' THEN amount ELSE 0 END) as remaining
    FROM {database_name}.{long_table}
    WHERE year = 2024
    GROUP BY date
    ORDER BY date
""").show()

print("\n✅ ETL Pipeline completed!")
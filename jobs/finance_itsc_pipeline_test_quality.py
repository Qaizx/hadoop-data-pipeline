# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, regexp_extract, input_file_name, expr, abs as spark_abs, lag
from pyspark.sql.window import Window
from typing import List, Dict, Optional, Tuple
import smtplib
import os
from email.mime.text import MIMEText

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

EXPECTED_COLUMNS = [
    "date", "total_amount", "details",
    "general_fund_admin_wifi_grant", "compensation_budget", "expense_budget",
    "material_budget", "utilities", "grant_welfare_health", "grant_ms_365",
    "education_fund_academic_computer_service_salary_staff", "government_staff",
    "asset_fund_academic_computer_service_equipment_budget", "equipment_budget_over_1m",
    "permanent_asset_fund_land_construction", "equipment_firewall", "grant_siem",
    "grant_data_center", "grant_wifi_satit",
    "research_fund_research_admin_personnel_research_grant",
    "reserve_fund_general_admin_other_expenses_reserve",
    "contribute_development_fund", "contribute_personnel_development_fund_cmu",
    "personnel_development_fund_education_management_support_special_grant",
    "art_preservation_fund_general_grant", "wifi_jumboplus", "firewall",
    "cmu_cloud", "siem", "digital_health", "benefit_access_request_system",
    "ups", "ups_rent_wifi_care", "uplift", "open_data"
]

AMOUNT_COLUMNS = [c for c in EXPECTED_COLUMNS if c not in ["date", "details", "total_amount"]]


# ============================================================
# Helper: HDFS utils
# ============================================================

def get_fs():
    uri = sc._jvm.java.net.URI.create(raw_path)
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)


def hdfs_ls_recursive(path: str) -> List[str]:
    fs = get_fs()
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hadoop_path):
        return []
    files = []
    iterator = fs.listFiles(hadoop_path, True)
    while iterator.hasNext():
        files.append(iterator.next().getPath().toString())
    return files


def hdfs_touch(path: str):
    fs = get_fs()
    fs.create(sc._jvm.org.apache.hadoop.fs.Path(path)).close()
    print(f"   ✅ Marker created: {path}")


def extract_year_from_path(path: str) -> Optional[int]:
    import re
    m = re.search(r"year=(\d{4})", path)
    return int(m.group(1)) if m else None


# ============================================================
# Data Quality Checks
# ============================================================

def check_schema(df: DataFrame, filepath: str) -> Tuple[bool, List[str]]:
    """ตรวจ schema ว่า column ครบและถูกต้อง"""
    errors = []
    actual_cols = set(df.columns)
    expected_cols = set(EXPECTED_COLUMNS)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    if missing:
        errors.append(f"❌ Missing columns: {sorted(missing)}")
    if extra:
        errors.append(f"⚠️  Extra columns (unexpected): {sorted(extra)}")

    return len(errors) == 0 or len(errors) == 1 and extra, errors


def check_null_values(df: DataFrame) -> Tuple[bool, List[str]]:
    """ตรวจ null ใน column สำคัญ"""
    errors = []
    critical_cols = ["date", "details"]

    for c in critical_cols:
        if c not in df.columns:
            continue
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            errors.append(f"❌ Column '{c}' มี null {null_count} rows")

    return len(errors) == 0, errors


def check_date_format(df: DataFrame) -> Tuple[bool, List[str]]:
    """ตรวจ date format และ special rows ที่ต้องมี"""
    errors = []
    dates = [r.date for r in df.select("date").distinct().collect()]

    required_special = {"all-year-budget", "total spent", "remaining"}
    missing_special = required_special - set(dates)
    if missing_special:
        errors.append(f"❌ Missing required rows: {missing_special}")

    import re
    pattern = re.compile(r"^\d{4}-\d{2}$")
    invalid_dates = [
        d for d in dates
        if d not in required_special and not pattern.match(str(d))
    ]
    if invalid_dates:
        errors.append(f"❌ Invalid date format: {invalid_dates}")

    return len(errors) == 0, errors


def check_total_amount(df: DataFrame) -> Tuple[bool, List[str]]:
    """ตรวจว่า total_amount ≈ sum ของทุก amount column (tolerance 1%)"""
    errors = []
    if "total_amount" not in df.columns:
        return True, []

    amount_cols_present = [c for c in AMOUNT_COLUMNS if c in df.columns]
    if not amount_cols_present:
        return True, []

    sum_expr = " + ".join([f"COALESCE(`{c}`, 0)" for c in amount_cols_present])
    df_check = df.filter(
        col("date").rlike(r"^\d{4}-\d{2}$") | (col("date") == "all-year-budget")
    )

    df_check = df_check.selectExpr(
        "date", "details", "total_amount",
        f"({sum_expr}) AS computed_sum"
    ).filter(
        spark_abs(col("total_amount") - col("computed_sum")) > col("total_amount") * 0.01
    )

    mismatch_count = df_check.count()
    if mismatch_count > 0:
        sample = df_check.limit(3).collect()
        for row in sample:
            errors.append(
                f"⚠️  total_amount mismatch at {row.date}/{row.details}: "
                f"total={row.total_amount:.0f}, computed={row.computed_sum:.0f}"
            )

    return len(errors) == 0, errors


def check_remaining_decreasing(df: DataFrame) -> Tuple[bool, List[str]]:
    """ตรวจว่า remaining ลดหลั่นทุกเดือน"""
    errors = []

    df_remaining = df.filter(
        (col("details") == "remaining") &
        col("date").rlike(r"^\d{4}-\d{2}$")
    ).select("date", "total_amount").orderBy("date")

    rows = df_remaining.collect()
    for i in range(1, len(rows)):
        if rows[i].total_amount > rows[i-1].total_amount:
            errors.append(
                f"⚠️  Remaining เพิ่มขึ้นที่ {rows[i].date}: "
                f"{rows[i-1].total_amount:.0f} → {rows[i].total_amount:.0f}"
            )

    return len(errors) == 0, errors


def run_quality_checks(df: DataFrame, filepath: str) -> Tuple[bool, str]:
    """รัน checks ทั้งหมด return (passed, report)"""
    print(f"\n🔍 Running Data Quality Checks: {filepath.split('/')[-1]}")

    all_errors = []
    all_warnings = []

    checks = [
        ("Schema", check_schema(df, filepath)),
        ("Null Values", check_null_values(df)),
        ("Date Format", check_date_format(df)),
        ("Total Amount", check_total_amount(df)),
        ("Remaining Decreasing", check_remaining_decreasing(df)),
    ]

    passed = True
    for check_name, (ok, errors) in checks:
        if ok:
            print(f"   ✅ {check_name}: passed")
        else:
            # schema extra columns เป็น warning ไม่ใช่ error
            has_fatal = any("❌" in e for e in errors)
            if has_fatal:
                passed = False
                print(f"   ❌ {check_name}: FAILED")
                all_errors.extend(errors)
            else:
                print(f"   ⚠️  {check_name}: warning")
                all_warnings.extend(errors)

        for e in errors:
            print(f"      {e}")

    report = f"File: {filepath}\n\n"
    if all_errors:
        report += "ERRORS (pipeline หยุด):\n" + "\n".join(all_errors) + "\n\n"
    if all_warnings:
        report += "WARNINGS:\n" + "\n".join(all_warnings)

    return passed, report


def send_quality_alert(filepath: str, report: str):
    """ส่ง email แจ้ง Data Quality failure"""
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")

    if not smtp_user or not smtp_password:
        print("   ⚠️  SMTP not configured, skipping email alert")
        return

    subject = f"❌ [ETL] Data Quality Failed: {filepath.split('/')[-1]}"
    body = f"<pre>{report}</pre>"

    msg = MIMEText(body, "html")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, smtp_user, msg.as_string())
        print(f"   📧 Alert email sent")
    except Exception as e:
        print(f"   ⚠️  Email failed: {e}")


# ============================================================
# PART 1: Raw -> Staging (Wide) — Incremental
# ============================================================
print("=" * 60)
print("PART 1: Raw -> Staging (Wide) — Incremental")
print("=" * 60)

all_files = hdfs_ls_recursive(raw_path)
csv_files = [f for f in all_files if f.endswith(".csv")]
done_files = set(f for f in all_files if f.endswith(".done"))
failed_files = set(f for f in all_files if f.endswith(".failed"))

pending_files = [
    f for f in csv_files
    if f + ".done" not in done_files and f + ".failed" not in failed_files
]

print(f"\n📁 CSV files found   : {len(csv_files)}")
print(f"✅ Already processed : {len([f for f in csv_files if f + '.done' in done_files])}")
print(f"❌ Failed (DQ)       : {len([f for f in csv_files if f + '.failed' in failed_files])}")
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

        # ===== DATA QUALITY CHECK =====
        dq_passed, dq_report = run_quality_checks(df, files[0])

        if not dq_passed:
            print(f"\n❌ Data Quality FAILED for year={year} — skipping load")
            print(dq_report)
            # สร้าง .failed marker แทน .done
            for f in files:
                hdfs_touch(f + ".failed")
            send_quality_alert(files[0], dq_report)
            # ลบปีนี้ออกจาก pending เพื่อไม่ให้ Part 2 process
            del pending_by_year[year]
            continue

        print(f"   ✅ Data Quality passed — proceeding to load")
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
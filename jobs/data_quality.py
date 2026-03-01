# jobs/data_quality.py
import re
from typing import List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs

from logger import get_logger

log = get_logger(__name__)

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


def check_schema(df: DataFrame, filepath: str) -> Tuple[bool, List[str]]:
    errors = []
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    extra = set(df.columns) - set(EXPECTED_COLUMNS)
    if missing:
        errors.append(f"❌ Missing columns: {sorted(missing)}")
    if extra:
        errors.append(f"⚠️  Extra columns: {sorted(extra)}")
    has_fatal = any("❌" in e for e in errors)
    return not has_fatal, errors


def check_null_values(df: DataFrame) -> Tuple[bool, List[str]]:
    errors = []
    for c in ["date", "details"]:
        if c not in df.columns:
            continue
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            errors.append(f"❌ Column '{c}' มี null {null_count} rows")
    return len(errors) == 0, errors


def check_date_format(df: DataFrame) -> Tuple[bool, List[str]]:
    errors = []
    dates = [r.date for r in df.select("date").distinct().collect()]
    required_special = {"all-year-budget", "total spent", "remaining"}
    missing_special = required_special - set(dates)
    if missing_special:
        errors.append(f"❌ Missing required rows: {missing_special}")
    pattern = re.compile(r"^\d{4}-\d{2}$")
    invalid = [d for d in dates if d not in required_special and not pattern.match(str(d))]
    if invalid:
        errors.append(f"❌ Invalid date format: {invalid}")
    return len(errors) == 0, errors


def check_total_amount(df: DataFrame) -> Tuple[bool, List[str]]:
    errors = []
    if "total_amount" not in df.columns:
        return True, []
    amount_cols = [c for c in AMOUNT_COLUMNS if c in df.columns]
    if not amount_cols:
        return True, []
    sum_expr = " + ".join([f"COALESCE(`{c}`, 0)" for c in amount_cols])
    df_check = df.filter(
        col("date").rlike(r"^\d{4}-\d{2}$") | (col("date") == "all-year-budget")
    ).selectExpr(
        "date", "details", "total_amount", f"({sum_expr}) AS computed_sum"
    ).filter(
        spark_abs(col("total_amount") - col("computed_sum")) > col("total_amount") * 0.01
    )
    for row in df_check.limit(3).collect():
        errors.append(
            f"⚠️  total_amount mismatch at {row.date}/{row.details}: "
            f"total={row.total_amount:.0f}, computed={row.computed_sum:.0f}"
        )
    return len(errors) == 0, errors


def check_remaining_decreasing(df: DataFrame) -> Tuple[bool, List[str]]:
    errors = []
    rows = df.filter(
        (col("details") == "remaining") & col("date").rlike(r"^\d{4}-\d{2}$")
    ).select("date", "total_amount").orderBy("date").collect()
    for i in range(1, len(rows)):
        if rows[i].total_amount > rows[i-1].total_amount:
            errors.append(
                f"⚠️  Remaining เพิ่มขึ้นที่ {rows[i].date}: "
                f"{rows[i-1].total_amount:.0f} → {rows[i].total_amount:.0f}"
            )
    return len(errors) == 0, errors


def run_quality_checks(df: DataFrame, filepath: str) -> Tuple[bool, str]:
    filename = filepath.split("/")[-1]
    log.info("Data quality check started", file=filename)

    all_errors, all_warnings = [], []
    passed = True

    checks = [
        ("Schema", check_schema(df, filepath)),
        ("Null Values", check_null_values(df)),
        ("Date Format", check_date_format(df)),
        ("Total Amount", check_total_amount(df)),
        ("Remaining Decreasing", check_remaining_decreasing(df)),
    ]

    for name, (ok, errors) in checks:
        if ok:
            log.info("Check passed", file=filename, check=name)
        else:
            has_fatal = any("❌" in e for e in errors)
            if has_fatal:
                passed = False
                log.error("Check failed", file=filename, check=name, errors=errors)
                all_errors.extend(errors)
            else:
                log.warning("Check warning", file=filename, check=name, warnings=errors)
                all_warnings.extend(errors)

    if passed:
        log.info("All quality checks passed", file=filename)
    else:
        log.error("Quality checks failed", file=filename, error_count=len(all_errors))

    report = f"File: {filepath}\n\n"
    if all_errors:
        report += "ERRORS:\n" + "\n".join(all_errors) + "\n\n"
    if all_warnings:
        report += "WARNINGS:\n" + "\n".join(all_warnings)

    return passed, report
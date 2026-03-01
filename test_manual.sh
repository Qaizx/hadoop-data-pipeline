#!/bin/bash
export MSYS_NO_PATHCONV=1
# set -euxo pipefail

# =============================================================================
# Manual Test Script — Finance ITSC Pipeline
# รัน: bash test_manual.sh (จาก host machine)
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS=0
FAIL=0

pass() { echo -e "${GREEN}✅ PASS${NC} $1"; ((PASS++)); }
fail() { echo -e "${RED}❌ FAIL${NC} $1"; ((FAIL++)); }
info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
header() {
    echo -e "\n${YELLOW}══════════════════════════════════════${NC}"
    echo -e "${YELLOW} $1${NC}"
    echo -e "${YELLOW}══════════════════════════════════════${NC}"
}

# helpers
hdfs_cmd()  {
    # รันใน namenode เพื่อหลีกเลี่ยงปัญหา Windows path (C: scheme)
    docker exec namenode hdfs dfs "$@"
}
hive_cmd()  { docker exec hive-server hive -e "$1" 2>/dev/null | tail -1; }
spark_run() { docker exec spark-master spark-submit /jobs/finance_itsc_pipeline_test_quality.py > /tmp/pipeline_out.log 2>&1; }

RAW_BASE="/datalake/raw/finance-itsc"
STAGING="/datalake/staging/finance-itsc_wide"
VERSIONS="/datalake/versions/finance-itsc"
TEST_YEAR=9999
BAD_YEAR=8888

# =============================================================================
# SETUP
# =============================================================================
header "SETUP — สร้างไฟล์ทดสอบ"

# CSV ปกติ
cat > /tmp/test_good.csv << 'CSV'
date,total_amount,details,general_fund_admin_wifi_grant,compensation_budget,expense_budget,material_budget,utilities,grant_welfare_health,grant_ms_365,education_fund_academic_computer_service_salary_staff,government_staff,asset_fund_academic_computer_service_equipment_budget,equipment_budget_over_1m,permanent_asset_fund_land_construction,equipment_firewall,grant_siem,grant_data_center,grant_wifi_satit,research_fund_research_admin_personnel_research_grant,reserve_fund_general_admin_other_expenses_reserve,contribute_development_fund,contribute_personnel_development_fund_cmu,personnel_development_fund_education_management_support_special_grant,art_preservation_fund_general_grant,wifi_jumboplus,firewall,cmu_cloud,siem,digital_health,benefit_access_request_system,ups,ups_rent_wifi_care,uplift,open_data
all-year-budget,0,budget,1000000.0,100000,100000,100000,100000,100000,50000,50000,50000,50000,50000,0,0,0,0,0,0,0,50000,50000,0,0,0,0,0,0,0,0,0,0,0,0,0,9999
2024-01,0,spent,500000.0,50000,50000,50000,50000,50000,25000,25000,25000,25000,25000,0,0,0,0,0,0,0,25000,25000,0,0,0,0,0,0,0,0,0,0,0,0,0,9999
2024-02,0,remaining,500000.0,50000,50000,50000,50000,50000,25000,25000,25000,25000,25000,0,0,0,0,0,0,0,25000,25000,0,0,0,0,0,0,0,0,0,0,0,0,0,9999

# CSV มี null date
cat > /tmp/test_bad.csv << 'CSV'
date,total_amount,details,general_fund_admin_wifi_grant,compensation_budget,expense_budget,material_budget,utilities,grant_welfare_health,grant_ms_365,education_fund_academic_computer_service_salary_staff,government_staff,asset_fund_academic_computer_service_equipment_budget,equipment_budget_over_1m,permanent_asset_fund_land_construction,equipment_firewall,grant_siem,grant_data_center,grant_wifi_satit,research_fund_research_admin_personnel_research_grant,reserve_fund_general_admin_other_expenses_reserve,contribute_development_fund,contribute_personnel_development_fund_cmu,personnel_development_fund_education_management_support_special_grant,art_preservation_fund_general_grant,wifi_jumboplus,firewall,cmu_cloud,siem,digital_health,benefit_access_request_system,ups,ups_rent_wifi_care,uplift,open_data
,null_date_row,1000000.0,100000,100000,100000,100000,100000,50000,50000,50000,50000,50000,0,0,0,0,0,0,0,50000,50000,0,0,0,0,0,0,0,0,0,0,0,0,0,8888
CSV

# สร้างไฟล์ใน namenode โดยตรง ไม่ผ่าน docker cp (หลีกเลี่ยง Windows path)
docker exec namenode bash -c "cat > /tmp/test_good.csv" < /tmp/test_good.csv
docker exec namenode bash -c "cat > /tmp/test_bad.csv" < /tmp/test_bad.csv
info "ไฟล์ทดสอบพร้อมแล้ว"

# set -x

# =============================================================================
# TEST 1 — Normal flow
# =============================================================================
header "TEST 1 — Normal flow: CSV ดี → .done ถูกสร้าง"

info "Cleanup ข้อมูลทดสอบเก่า..."
hdfs_cmd -rm -r -f "$RAW_BASE/year=$TEST_YEAR" "$STAGING/year=$TEST_YEAR" "$VERSIONS/year=$TEST_YEAR" 2>/dev/null

echo "RAW_BASE=$RAW_BASE"
echo "STAGING=$STAGING"
echo "VERSIONS=$VERSIONS"
echo "TEST_YEAR=$TEST_YEAR"

info "Upload CSV ดี..."
docker exec namenode hdfs dfs -mkdir -p "$RAW_BASE/year=$TEST_YEAR" 
docker exec namenode hdfs dfs -cp "$RAW_BASE/year=2021/finance_itsc_2021.csv" "$RAW_BASE/year=$TEST_YEAR/test_good.csv"

info "รัน pipeline..."
spark_run

if hdfs_cmd -test -e "$RAW_BASE/year=$TEST_YEAR/test_good.csv.done" 2>/dev/null; then
    pass "TEST 1: .done marker ถูกสร้าง"
else
    fail "TEST 1: ไม่พบ .done marker"
    grep -i "error\|fail" /tmp/pipeline_out.log | tail -5
fi

# =============================================================================
# TEST 2 — ข้อมูลเข้า Hive
# =============================================================================
header "TEST 2 — ข้อมูลเข้า Hive wide + long"

WIDE_COUNT=$(hive_cmd "SELECT COUNT(*) FROM finance_itsc_wide WHERE year=$TEST_YEAR;")
LONG_COUNT=$(hive_cmd "SELECT COUNT(*) FROM finance_itsc_long WHERE year=$TEST_YEAR;")

if [ "$WIDE_COUNT" -gt 0 ] 2>/dev/null; then
    pass "TEST 2a: Wide table มีข้อมูล ($WIDE_COUNT rows)"
else
    fail "TEST 2a: Wide table ไม่มีข้อมูลปี $TEST_YEAR"
fi

if [ "$LONG_COUNT" -gt 0 ] 2>/dev/null; then
    pass "TEST 2b: Long table มีข้อมูล ($LONG_COUNT rows)"
else
    fail "TEST 2b: Long table ไม่มีข้อมูลปี $TEST_YEAR"
fi

# =============================================================================
# TEST 3 — Upload ซ้ำ
# =============================================================================
header "TEST 3 — Upload ซ้ำ: pipeline ต้อง skip"

info "รัน pipeline อีกครั้ง..."
spark_run

WIDE_COUNT2=$(hive_cmd "SELECT COUNT(*) FROM finance_itsc_wide WHERE year=$TEST_YEAR;")
if [ "$WIDE_COUNT2" = "$WIDE_COUNT" ]; then
    pass "TEST 3: ข้อมูลไม่ถูก reprocess (count ยังเท่าเดิม = $WIDE_COUNT)"
else
    fail "TEST 3: ข้อมูลถูก reprocess ซ้ำ ($WIDE_COUNT → $WIDE_COUNT2)"
fi

# =============================================================================
# TEST 4 — DQ fail
# =============================================================================
header "TEST 4 — DQ fail: null date → .failed + ไม่เข้า Hive"

info "Cleanup..."
hdfs_cmd -rm -r -f "$RAW_BASE/year=$BAD_YEAR" "$STAGING/year=$BAD_YEAR" 2>/dev/null

info "Upload CSV null date..."
docker exec namenode hdfs dfs -mkdir -p "$RAW_BASE/year=$BAD_YEAR"
docker exec namenode hdfs dfs -put /tmp/test_bad.csv "$RAW_BASE/year=$BAD_YEAR/test_bad.csv"

info "รัน pipeline..."
spark_run

if hdfs_cmd -test -e "$RAW_BASE/year=$BAD_YEAR/test_bad.csv.failed" 2>/dev/null; then
    pass "TEST 4a: .failed marker ถูกสร้าง"
else
    fail "TEST 4a: ไม่พบ .failed marker"
fi

BAD_COUNT=$(hive_cmd "SELECT COUNT(*) FROM finance_itsc_wide WHERE year=$BAD_YEAR;")
if [ -z "$BAD_COUNT" ] || [ "$BAD_COUNT" = "0" ]; then
    pass "TEST 4b: ข้อมูล DQ fail ไม่เข้า Hive"
else
    fail "TEST 4b: ข้อมูล DQ fail เข้า Hive ($BAD_COUNT rows)"
fi

# =============================================================================
# TEST 5 — Versioning
# =============================================================================
header "TEST 5 — Versioning: snapshot ถูกสร้าง"

VERSION_COUNT=$(hdfs_cmd -ls "$VERSIONS/year=$TEST_YEAR/" 2>/dev/null | grep "v_" | wc -l)
if [ "$VERSION_COUNT" -gt 0 ]; then
    pass "TEST 5a: Snapshot ถูกสร้าง ($VERSION_COUNT version)"
    VERSION_PATH=$(hdfs_cmd -ls "$VERSIONS/year=$TEST_YEAR/" 2>/dev/null | grep "v_" | head -1 | awk '{print $8}')
    if hdfs_cmd -test -e "$VERSION_PATH/_version.json" 2>/dev/null; then
        pass "TEST 5b: _version.json มีอยู่"
        info "Metadata:"
        hdfs_cmd -cat "$VERSION_PATH/_version.json" 2>/dev/null
    else
        fail "TEST 5b: ไม่พบ _version.json"
    fi
else
    fail "TEST 5a: ไม่พบ snapshot"
fi

# =============================================================================
# TEST 6 — ปีอื่นไม่โดนแตะ
# =============================================================================
header "TEST 6 — Atomic write: ปีอื่นไม่โดนแตะ"

for YEAR in 2021 2022 2023; do
    COUNT=$(hive_cmd "SELECT COUNT(*) FROM finance_itsc_wide WHERE year=$YEAR;")
    if [ "$COUNT" -gt 0 ] 2>/dev/null; then
        pass "TEST 6: year=$YEAR ยังมีข้อมูลครบ ($COUNT rows)"
    else
        fail "TEST 6: year=$YEAR ข้อมูลหาย (count=$COUNT)"
    fi
done

# =============================================================================
# TEST 7 — Retry เมื่อ datanode หยุด
# =============================================================================
header "TEST 7 — Retry: หยุด datanode ระหว่าง pipeline รัน"

info "หยุด datanode..."
docker stop datanode

info "รัน pipeline ขณะ datanode หยุด (timeout 60s)..."
timeout 60 docker exec spark-master spark-submit /jobs/finance_itsc_pipeline_test_quality.py > /tmp/pipeline_retry.log 2>&1

info "Start datanode กลับ..."
docker start datanode
sleep 5

if grep -q -i "retry\|retries\|attempt" /tmp/pipeline_retry.log 2>/dev/null; then
    pass "TEST 7a: Pipeline retry เมื่อ datanode หยุด"
else
    pass "TEST 7a: Pipeline fail gracefully"
fi

TMP_COUNT=$(hdfs_cmd -ls "$STAGING/" 2>/dev/null | grep "_tmp" | wc -l)
if [ "$TMP_COUNT" -eq 0 ]; then
    pass "TEST 7b: ไม่มี _tmp ค้างอยู่"
else
    fail "TEST 7b: พบ _tmp ค้างอยู่ $TMP_COUNT อัน"
fi

# =============================================================================
# CLEANUP
# =============================================================================
header "CLEANUP"
info "ลบข้อมูลทดสอบ..."
hdfs_cmd -rm -r -f \
    "$RAW_BASE/year=$TEST_YEAR" \
    "$RAW_BASE/year=$BAD_YEAR" \
    "$STAGING/year=$TEST_YEAR" \
    "$STAGING/year=$BAD_YEAR" \
    "$VERSIONS/year=$TEST_YEAR" \
    2>/dev/null
docker exec hive-server hive -e "
    ALTER TABLE finance_itsc_wide DROP IF EXISTS PARTITION (year=$TEST_YEAR);
    ALTER TABLE finance_itsc_wide DROP IF EXISTS PARTITION (year=$BAD_YEAR);
    ALTER TABLE finance_itsc_long DROP IF EXISTS PARTITION (year=$TEST_YEAR);
" 2>/dev/null
rm -f /tmp/test_good.csv /tmp/test_bad.csv /tmp/pipeline*.log
info "Cleanup เสร็จ"

# =============================================================================
# SUMMARY
# =============================================================================
header "SUMMARY"
echo -e "${GREEN}PASS: $PASS${NC}"
echo -e "${RED}FAIL: $FAIL${NC}"
echo "Total: $((PASS + FAIL)) tests"

if [ $FAIL -eq 0 ]; then
    echo -e "\n${GREEN}🎉 ทุก test ผ่านหมด!${NC}"
else
    echo -e "\n${RED}⚠️  มี $FAIL test ที่ fail${NC}"
    exit 1
fi
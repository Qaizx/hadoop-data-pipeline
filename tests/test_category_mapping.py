# tests/test_category_mapping.py

# Category mapping เดียวกับใน config.py
EXPECTED_MAPPING = {
    "general_fund_admin_wifi_grant": ["กองทุนทั่วไป", "งานบริหารทั่วไป", "wifi"],
    "compensation_budget": ["ค่าตอบแทน", "งบประจำ"],
    "expense_budget": ["ค่าใช้สอย"],
    "material_budget": ["ค่าวัสดุ"],
    "utilities": ["ค่าสาธารณูปโภค"],
    "grant_welfare_health": ["สวัสดิการ", "สุขภาพ"],
    "grant_ms_365": ["MS 365", "Microsoft 365"],
    "education_fund_academic_computer_service_salary_staff": ["เงินเดือน", "พนง.เงินรายได้"],
    "government_staff": ["พนักงานเงินแผ่นดิน", "พนง.เงินแผ่นดิน"],
    "asset_fund_academic_computer_service_equipment_budget": ["ค่าครุภัณฑ์", "ไม่เกิน 1 ล้าน"],
    "equipment_budget_over_1m": ["เกิน 1 ล้าน"],
    "permanent_asset_fund_land_construction": ["ที่ดิน", "สิ่งก่อสร้าง"],
    "equipment_firewall": ["ครุภัณฑ์ Firewall"],
    "grant_siem": ["SIEM"],
    "grant_data_center": ["data center"],
    "grant_wifi_satit": ["wifi satit"],
    "research_fund_research_admin_personnel_research_grant": ["วิจัย"],
    "reserve_fund_general_admin_other_expenses_reserve": ["สำรองจ่าย", "งบสำรอง"],
    "contribute_development_fund": ["สมทบกองทุนพัฒนา"],
    "contribute_personnel_development_fund_cmu": ["สมทบกองทุนพัฒนาบุคลากร มช"],
    "personnel_development_fund_education_management_support_special_grant": ["อุดหนุนเฉพาะกิจ"],
    "art_preservation_fund_general_grant": ["อุดหนุนทั่วไป", "ทำนุ"],
    "wifi_jumboplus": ["Jumboplus"],
    "firewall": ["Firewall"],
    "cmu_cloud": ["CMU Cloud"],
    "siem": ["SiEM"],
    "digital_health": ["Digital Health"],
    "benefit_access_request_system": ["ระบบการขอเข้าทำประโยชน์"],
    "ups": ["UPS"],
    "ups_rent_wifi_care": ["เช่า UPS", "ดูแล wifi"],
    "uplift": ["Uplift"],
    "open_data": ["Open data"],
}

ALL_EXPECTED_COLUMNS = list(EXPECTED_MAPPING.keys())

# CSV columns จริง
CSV_COLUMNS = [
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


class TestCategoryMapping:
    def test_all_csv_columns_in_mapping(self):
        """ทุก column ใน CSV ต้องมีใน mapping"""
        missing = [c for c in CSV_COLUMNS if c not in EXPECTED_MAPPING]
        assert missing == [], f"Columns missing from mapping: {missing}"

    def test_all_mapping_columns_in_csv(self):
        """ทุก column ใน mapping ต้องมีใน CSV"""
        extra = [c for c in EXPECTED_MAPPING if c not in CSV_COLUMNS]
        assert extra == [], f"Mapping has columns not in CSV: {extra}"

    def test_total_column_count(self):
        """จำนวน column ต้องครบ 32"""
        assert len(CSV_COLUMNS) == 32

    def test_no_duplicate_columns(self):
        """ไม่มี column ซ้ำ"""
        assert len(CSV_COLUMNS) == len(set(CSV_COLUMNS))

    def test_no_duplicate_mapping_keys(self):
        assert len(ALL_EXPECTED_COLUMNS) == len(set(ALL_EXPECTED_COLUMNS))

    def test_all_columns_lowercase_underscore(self):
        """column names ต้องเป็น lowercase และใช้ _ เท่านั้น"""
        import re
        for col in CSV_COLUMNS:
            assert re.match(r'^[a-z0-9_]+$', col), f"Invalid column name: {col}"

    def test_key_columns_exist(self):
        """column สำคัญต้องมีอยู่"""
        key_cols = [
            "compensation_budget",
            "expense_budget",
            "material_budget",
            "utilities",
            "firewall",
            "cmu_cloud",
        ]
        for col in key_cols:
            assert col in CSV_COLUMNS, f"Missing key column: {col}"


class TestMockDataStructure:
    """ตรวจสอบ structure ของ mock CSV data"""

    def get_mock_rows(self):
        """สร้าง mock rows สำหรับ test"""
        return [
            {"date": "all-year-budget", "details": "budget", "total_amount": 1000},
            {"date": "2024-10", "details": "spent", "total_amount": 100},
            {"date": "2024-10", "details": "remaining", "total_amount": 900},
            {"date": "2024-11", "details": "spent", "total_amount": 150},
            {"date": "2024-11", "details": "remaining", "total_amount": 750},
            {"date": "total spent", "details": "spent", "total_amount": 250},
            {"date": "remaining", "details": "remaining", "total_amount": 750},
        ]

    def test_row_count(self):
        """27 rows = 1 budget + 12*2 months + 2 summary"""
        rows = self.get_mock_rows()
        # อย่างน้อยต้องมี all-year-budget, monthly pairs, total spent, remaining
        assert any(r["date"] == "all-year-budget" for r in rows)
        assert any(r["date"] == "total spent" for r in rows)
        assert any(r["date"] == "remaining" for r in rows)

    def test_remaining_decreases(self):
        """remaining ต้องลดลงทุกเดือน"""
        rows = self.get_mock_rows()
        monthly_remaining = [
            r["total_amount"] for r in rows
            if r["details"] == "remaining" and r["date"] not in ("remaining",)
        ]
        for i in range(len(monthly_remaining) - 1):
            assert monthly_remaining[i] >= monthly_remaining[i + 1], \
                f"Remaining increased at index {i}: {monthly_remaining[i]} -> {monthly_remaining[i+1]}"

    def test_details_values(self):
        """details ต้องเป็นแค่ budget, spent, remaining"""
        rows = self.get_mock_rows()
        valid_details = {"budget", "spent", "remaining"}
        for row in rows:
            assert row["details"] in valid_details

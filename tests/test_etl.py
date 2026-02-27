# tests/test_etl.py
import pytest
import re
from typing import Optional


# ============================================================
# Import functions จาก pipeline โดยไม่ trigger Spark
# ============================================================

def extract_year_from_path(path: str) -> Optional[int]:
    m = re.search(r"year=(\d{4})", path)
    return int(m.group(1)) if m else None


def get_pending_files(csv_files, done_files):
    done_set = set(done_files)
    return [f for f in csv_files if f + ".done" not in done_set]


def group_by_year(pending_files):
    result = {}
    for f in pending_files:
        year = extract_year_from_path(f)
        if year:
            result.setdefault(year, []).append(f)
    return result


# ============================================================
# Tests: extract_year_from_path
# ============================================================

class TestExtractYear:
    def test_standard_path(self):
        path = "hdfs://namenode:8020/datalake/raw/finance-itsc/year=2024/finance_itsc_2024.csv"
        assert extract_year_from_path(path) == 2024

    def test_different_year(self):
        path = "/datalake/raw/year=2021/file.csv"
        assert extract_year_from_path(path) == 2021

    def test_no_year_in_path(self):
        path = "/datalake/raw/finance-itsc/file.csv"
        assert extract_year_from_path(path) is None

    def test_year_in_filename_only(self):
        path = "/data/finance_2024.csv"
        assert extract_year_from_path(path) is None

    def test_multiple_year_takes_first(self):
        path = "/year=2023/subdir/year=2024/file.csv"
        assert extract_year_from_path(path) == 2023


# ============================================================
# Tests: pending files logic
# ============================================================

class TestPendingFiles:
    def setup_method(self):
        self.csv_files = [
            "hdfs://namenode/year=2024/finance_2024.csv",
            "hdfs://namenode/year=2024/finance_2024_v2.csv",
            "hdfs://namenode/year=2023/finance_2023.csv",
        ]

    def test_all_pending_when_no_done(self):
        pending = get_pending_files(self.csv_files, done_files=[])
        assert len(pending) == 3

    def test_skip_processed_file(self):
        done = ["hdfs://namenode/year=2024/finance_2024.csv.done"]
        pending = get_pending_files(self.csv_files, done_files=done)
        assert len(pending) == 2
        assert "hdfs://namenode/year=2024/finance_2024.csv" not in pending

    def test_all_processed(self):
        done = [f + ".done" for f in self.csv_files]
        pending = get_pending_files(self.csv_files, done_files=done)
        assert pending == []

    def test_new_file_detected_after_first_processed(self):
        done = ["hdfs://namenode/year=2024/finance_2024.csv.done"]
        pending = get_pending_files(self.csv_files, done_files=done)
        assert "hdfs://namenode/year=2024/finance_2024_v2.csv" in pending


# ============================================================
# Tests: group by year
# ============================================================

class TestGroupByYear:
    def test_single_year(self):
        files = ["hdfs://namenode/year=2024/a.csv", "hdfs://namenode/year=2024/b.csv"]
        result = group_by_year(files)
        assert list(result.keys()) == [2024]
        assert len(result[2024]) == 2

    def test_multiple_years(self):
        files = [
            "hdfs://namenode/year=2024/a.csv",
            "hdfs://namenode/year=2023/b.csv",
            "hdfs://namenode/year=2024/c.csv",
        ]
        result = group_by_year(files)
        assert set(result.keys()) == {2023, 2024}
        assert len(result[2024]) == 2
        assert len(result[2023]) == 1

    def test_skip_files_without_year(self):
        files = [
            "hdfs://namenode/year=2024/a.csv",
            "hdfs://namenode/no-year/b.csv",
        ]
        result = group_by_year(files)
        assert list(result.keys()) == [2024]


# ============================================================
# Tests: date filter pattern
# ============================================================

class TestDateFilter:
    DATE_PATTERN = re.compile(r"^\d{4}-\d{2}$")

    def _is_valid(self, date_str):
        return bool(self.DATE_PATTERN.match(date_str)) or date_str == "all-year-budget"

    def test_valid_monthly_dates(self):
        assert self._is_valid("2024-01")
        assert self._is_valid("2023-12")
        assert self._is_valid("2020-10")

    def test_all_year_budget(self):
        assert self._is_valid("all-year-budget")

    def test_invalid_rows_filtered(self):
        assert not self._is_valid("total spent")
        assert not self._is_valid("remaining")
        assert not self._is_valid("2024-1")
        assert not self._is_valid("24-01")
        assert not self._is_valid("")


# ============================================================
# Tests: amount column detection
# ============================================================

class TestAmountColumns:
    def get_amount_columns(self, all_columns):
        id_columns = ["date", "details", "year"]
        exclude_columns = ["total_amount"]
        return [c for c in all_columns if c not in id_columns and c not in exclude_columns]

    def test_excludes_id_columns(self):
        cols = ["date", "details", "year", "compensation_budget", "expense_budget"]
        result = self.get_amount_columns(cols)
        assert "date" not in result
        assert "details" not in result
        assert "year" not in result

    def test_excludes_total_amount(self):
        cols = ["date", "total_amount", "compensation_budget"]
        result = self.get_amount_columns(cols)
        assert "total_amount" not in result

    def test_keeps_category_columns(self):
        cols = ["date", "details", "year", "total_amount",
                "compensation_budget", "expense_budget", "firewall"]
        result = self.get_amount_columns(cols)
        assert result == ["compensation_budget", "expense_budget", "firewall"]

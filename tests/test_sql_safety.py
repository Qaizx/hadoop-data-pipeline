# tests/test_sql_safety.py
import pytest
import re


# ============================================================
# Copy functions จาก hive_gpt.py (ไม่ต้อง import OpenAI)
# ============================================================

def fix_hive_reserved_keywords(sql: str) -> str:
    sql = re.sub(r'(?<!`)\bdate\b(?!`)', '`date`', sql)
    return sql


def has_bad_remaining_sum(sql: str) -> bool:
    pattern = r"SUM\s*\(\s*CASE\s+WHEN\s+\S*details\S*\s*=\s*['\"`]remaining['\"`]"
    return bool(re.search(pattern, sql, re.IGNORECASE))


# ============================================================
# Tests: fix_hive_reserved_keywords
# ============================================================

class TestFixReservedKeywords:
    def test_fixes_bare_date_in_where(self):
        sql = "SELECT * FROM t WHERE date = '2024-01'"
        result = fix_hive_reserved_keywords(sql)
        assert "`date`" in result
        assert "WHERE `date`" in result

    def test_fixes_date_in_order_by(self):
        sql = "SELECT date, amount FROM t ORDER BY date DESC"
        result = fix_hive_reserved_keywords(sql)
        assert result.count("`date`") == 2

    def test_fixes_date_in_max(self):
        sql = "SELECT MAX(date) FROM t"
        result = fix_hive_reserved_keywords(sql)
        assert "MAX(`date`)" in result

    def test_does_not_double_wrap(self):
        sql = "SELECT `date` FROM t WHERE `date` = '2024-01'"
        result = fix_hive_reserved_keywords(sql)
        assert "``date``" not in result
        assert result.count("`date`") == 2

    def test_does_not_affect_other_columns(self):
        sql = "SELECT amount, category FROM t"
        result = fix_hive_reserved_keywords(sql)
        assert result == sql

    def test_fixes_date_in_select(self):
        sql = "SELECT date, amount FROM finance_itsc_long"
        result = fix_hive_reserved_keywords(sql)
        assert "SELECT `date`" in result


# ============================================================
# Tests: has_bad_remaining_sum
# ============================================================

class TestBadRemainingSum:
    def test_detects_sum_case_remaining(self):
        sql = """
        SELECT category,
               SUM(CASE WHEN details = 'remaining' THEN amount ELSE 0 END) AS remaining_budget
        FROM finance_itsc_long
        GROUP BY category
        """
        assert has_bad_remaining_sum(sql) is True

    def test_detects_with_table_alias(self):
        sql = "SUM(CASE WHEN t.details = 'remaining' THEN t.amount ELSE 0 END)"
        assert has_bad_remaining_sum(sql) is True

    def test_detects_case_insensitive(self):
        sql = "sum(case when details = 'remaining' then amount else 0 end)"
        assert has_bad_remaining_sum(sql) is True

    def test_allows_correct_remaining_query(self):
        sql = """
        SELECT t.category, t.amount AS remaining_budget
        FROM finance_itsc_long t
        JOIN (
          SELECT category, MAX(`date`) AS max_date
          FROM finance_itsc_long
          WHERE details = 'remaining'
          GROUP BY category
        ) latest ON t.category = latest.category AND t.`date` = latest.max_date
        WHERE t.details = 'remaining'
        """
        assert has_bad_remaining_sum(sql) is False

    def test_allows_sum_on_spent(self):
        sql = "SELECT SUM(amount) FROM t WHERE details = 'spent'"
        assert has_bad_remaining_sum(sql) is False

    def test_allows_sum_on_budget(self):
        sql = "SELECT category, SUM(amount) AS total FROM t WHERE details = 'budget' GROUP BY category"
        assert has_bad_remaining_sum(sql) is False

    def test_allows_order_by_limit_pattern(self):
        sql = """
        SELECT `date`, category, amount
        FROM finance_itsc_long
        WHERE details = 'remaining' AND category = 'expense_budget'
        ORDER BY `date` DESC
        LIMIT 1
        """
        assert has_bad_remaining_sum(sql) is False

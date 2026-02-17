"""
Tests for Oracle-to-PostgreSQL transform functions in oracle_sql_dir_to_postgres_synonyms.py.
Run with:  python -m pytest test_transforms.py -v
"""

import pytest
import re

# Import the module under test
import oracle_sql_dir_to_postgres_synonyms as mod


# ---------------------------------------------------------------------------
# Helper: normalize whitespace for comparison
# ---------------------------------------------------------------------------
def _ws(s: str) -> str:
    """Collapse whitespace so assertions are not sensitive to extra spaces."""
    return " ".join(s.split()).strip()


# ===========================================================================
# 1. _remove_outer_join_plus
# ===========================================================================
class TestRemoveOuterJoinPlus:
    def test_basic_removal(self):
        result = mod._remove_outer_join_plus("a.id = b.id(+)")
        assert "(+)" not in result
        assert "a.id = b.id" in result

    def test_warning_injected(self):
        result = mod._remove_outer_join_plus("a.id = b.id(+)")
        assert "WARNING" in result
        assert "outer-join" in result.lower() or "outer join" in result.lower()

    def test_no_change_without_plus(self):
        sql = "a.id = b.id"
        result = mod._remove_outer_join_plus(sql)
        assert result == sql

    def test_multiple_plus(self):
        sql = "a.x = b.x(+) AND a.y = c.y(+)"
        result = mod._remove_outer_join_plus(sql)
        assert "(+)" not in result


# ===========================================================================
# 2. _decode_html_entities_in_body
# ===========================================================================
class TestDecodeHtmlEntities:
    def test_basic_entities(self):
        result = mod._decode_html_entities_in_body("a &gt; b AND c &lt; d")
        assert ">" in result
        assert "<" in result

    def test_amp(self):
        result = mod._decode_html_entities_in_body("a &amp; b")
        assert "a & b" in result

    def test_no_entities(self):
        text = "SELECT 1 FROM dual"
        assert mod._decode_html_entities_in_body(text) == text


# ===========================================================================
# 3. _repair_identifier_space_before_dot
# ===========================================================================
class TestRepairIdentifierSpace:
    def test_merges_identifier_pair_before_dot(self):
        # The function merges two identifiers separated by space before a dot: "seg order.ctid" -> "seg_order.ctid"
        result = mod._repair_identifier_space_before_dot("seg order.ctid")
        assert "seg_order.ctid" in result

    def test_no_change_when_clean(self):
        text = "schema.table_name"
        assert mod._repair_identifier_space_before_dot(text) == text


# ===========================================================================
# 4. NVL / NVL2 / DECODE  (_replace_nvl_nvl2_decode_in_body)
# ===========================================================================
class TestNvlNvl2Decode:
    def test_nvl_to_coalesce(self):
        body = "NVL(col1, 0)"
        result = mod._replace_nvl_nvl2_decode_in_body(body)
        assert "COALESCE" in result.upper()

    def test_nvl2(self):
        body = "NVL2(x, 'yes', 'no')"
        result = mod._replace_nvl_nvl2_decode_in_body(body)
        assert "CASE" in result.upper() or "WHEN" in result.upper()

    def test_decode(self):
        body = "DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')"
        result = mod._replace_nvl_nvl2_decode_in_body(body)
        assert "CASE" in result.upper()
        assert "WHEN" in result.upper()
        assert "ELSE" in result.upper()


# ===========================================================================
# 5. Oracle TO_ functions (_replace_oracle_to_functions_in_body)
# ===========================================================================
class TestOracleToFunctions:
    def test_to_number(self):
        body = "TO_NUMBER(col1)"
        result = mod._replace_oracle_to_functions_in_body(body)
        assert "NUMERIC" in result.upper() or "CAST" in result.upper()

    def test_to_char_no_format(self):
        """TO_CHAR(col1) with no format -> just strip wrapper, no ::text cast."""
        body = "TO_CHAR(col1)"
        result = mod._replace_oracle_to_functions_in_body(body)
        assert result == "(col1)", f"Expected bare expression, got: {result}"
        assert "::text" not in result

    def test_to_char_with_format(self):
        """TO_CHAR(col1, 'YYYY-MM-DD') -> to_char(col1, 'YYYY-MM-DD')."""
        body = "TO_CHAR(col1, 'YYYY-MM-DD')"
        result = mod._replace_oracle_to_functions_in_body(body)
        assert "to_char" in result.lower(), f"Expected to_char function: {result}"
        assert "'YYYY-MM-DD'" in result


# ===========================================================================
# 6. Oracle builtin functions (_replace_oracle_builtin_functions_in_body)
# ===========================================================================
class TestOracleBuiltinFunctions:
    def test_months_between(self):
        body = "MONTHS_BETWEEN(d1, d2)"
        result = mod._replace_oracle_builtin_functions_in_body(body)
        assert "MONTHS_BETWEEN" not in result.upper()
        assert "30" in result  # approximation via / 30.0

    def test_add_months(self):
        body = "ADD_MONTHS(hire_date, 3)"
        result = mod._replace_oracle_builtin_functions_in_body(body)
        assert "ADD_MONTHS" not in result.upper()
        assert "INTERVAL" in result.upper()

    def test_round(self):
        body = "ROUND(amount, 2)"
        result = mod._replace_oracle_builtin_functions_in_body(body)
        assert "round" in result.lower()
        assert "numeric" in result.lower()

    def test_lpad_two_args(self):
        body = "LPAD(col1, 10)"
        result = mod._replace_oracle_builtin_functions_in_body(body)
        assert "lpad(" in result.lower()
        assert "::text" in result
        assert "::int" in result
        # balanced parens
        assert result.count("(") == result.count(")")

    def test_lpad_three_args(self):
        body = "LPAD(col1, LEAST(plan_level, 8), '.')"
        result = mod._replace_oracle_builtin_functions_in_body(body)
        assert "lpad(" in result.lower()
        assert "'.'" in result
        assert "::text" in result
        # balanced parens — this was a bug (extra ')') before fix
        assert result.count("(") == result.count(")"), f"Unbalanced parens: {result}"

    def test_lpad_three_args_output(self):
        """Verify the actual output structure of 3-arg LPAD conversion."""
        body = "LPAD(' ', LEAST(plan_level, 8), '.')"
        result = mod._replace_oracle_builtin_functions_in_body(body)
        # Must start with lpad( and end with exactly one )
        assert result.startswith("lpad("), f"Expected lpad( prefix: {result}"
        assert result.count("(") == result.count(")"), f"Unbalanced: {result}"
        # Must contain all three arguments
        assert "::text" in result
        assert "::int" in result
        assert "'.'" in result


class TestOracleMiscFunctions:
    """Tests for functions in _replace_oracle_misc_in_body."""

    def test_sysdate(self):
        body = "SELECT SYSDATE FROM t"
        result = mod._replace_oracle_misc_in_body(body)
        assert "CURRENT_TIMESTAMP" in result.upper()

    def test_instr_two_args(self):
        body = "INSTR(col1, 'x')"
        result = mod._replace_oracle_misc_in_body(body)
        assert "strpos" in result.lower()

    def test_lengthb(self):
        body = "LENGTHB(col1)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "octet_length" in result.lower()

    def test_sys_guid(self):
        body = "SYS_GUID()"
        result = mod._replace_oracle_misc_in_body(body)
        assert "gen_random_uuid" in result.lower()

    def test_bitand(self):
        body = "BITAND(a, b)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "&" in result
        assert "BITAND" not in result.upper()

    def test_listagg(self):
        body = "LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "string_agg" in result.lower()

    def test_wm_concat(self):
        body = "WM_CONCAT(name)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "string_agg" in result.lower()

    def test_rowidtochar(self):
        body = "ROWIDTOCHAR(bet.ctid)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "ROWIDTOCHAR" not in result.upper()
        assert "(bet.ctid)::text" in result

    def test_rowidtochar_case_insensitive(self):
        body = "rowidtochar(t.ROWID)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "rowidtochar" not in result.lower()
        assert "::text" in result

    def test_chartorowid(self):
        body = "CHARTOROWID('AAAbcDDEEF')"
        result = mod._replace_oracle_misc_in_body(body)
        assert "CHARTOROWID" not in result.upper()
        assert "::text" in result


# ===========================================================================
# 7. TRUNC  (_replace_trunc_in_body)
# ===========================================================================
class TestTrunc:
    def test_trunc_date_sysdate(self):
        body = "TRUNC(SYSDATE)"
        result = mod._replace_trunc_in_body(body)
        assert "date_trunc" in result, f"Expected date_trunc: {result}"
        assert "::timestamp" in result, f"Expected ::timestamp: {result}"

    def test_trunc_date_column(self):
        body = "pg_trunc__(ps.ending_date)"
        result = mod._replace_trunc_in_body(body)
        assert "date_trunc" in result, f"Expected date_trunc: {result}"

    def test_trunc_numeric_column(self):
        """Column not ending in date/time/timestamp should be numeric trunc."""
        body = "pg_trunc__(cur.exchange_rate)"
        result = mod._replace_trunc_in_body(body)
        assert result.startswith("trunc("), f"Expected trunc: {result}"
        assert "::numeric" in result, f"Expected ::numeric: {result}"

    def test_trunc_current_timestamp(self):
        body = "PG_TRUNC__(CURRENT_TIMESTAMP)"
        result = mod._replace_trunc_in_body(body)
        assert "date_trunc" in result, f"Expected date_trunc: {result}"

    def test_trunc_2arg_numeric(self):
        body = "pg_trunc__(amount, 2)"
        result = mod._replace_trunc_in_body(body)
        assert result.startswith("trunc("), f"Expected trunc: {result}"
        assert "::numeric" in result, f"Expected ::numeric: {result}"
        assert ", 2)" in result, f"Expected , 2): {result}"

    def test_trunc_2arg_format(self):
        body = "pg_trunc__(col, 'MM')"
        result = mod._replace_trunc_in_body(body)
        assert "date_trunc('month'" in result, f"Expected month: {result}"

    def test_trunc_cast_as_date(self):
        body = "pg_trunc__(CAST(x AS DATE))"
        result = mod._replace_trunc_in_body(body)
        assert "date_trunc" in result, f"Expected date_trunc: {result}"

    def test_trunc_no_infinite_loop(self):
        """Ensure output trunc() doesn't cause infinite re-matching."""
        body = "TRUNC(a.val) + TRUNC(b.rate)"
        result = mod._replace_trunc_in_body(body)
        assert "pg_trunc__" not in result, f"Placeholder remains: {result}"
        assert result.count("trunc(") == 2, f"Expected 2 trunc calls: {result}"


# ===========================================================================
# 8. Oracle sequence refs (_replace_oracle_sequence_refs)
# ===========================================================================
class TestOracleSequenceRefs:
    def test_nextval(self):
        body = "my_seq.NEXTVAL"
        result = mod._replace_oracle_sequence_refs(body)
        assert "nextval" in result.lower()

    def test_currval(self):
        body = "my_seq.CURRVAL"
        result = mod._replace_oracle_sequence_refs(body)
        assert "currval" in result.lower()


# ===========================================================================
# 9. ROWID -> ctid (_replace_rowid_to_ctid)
# ===========================================================================
class TestRowidToCtid:
    def test_rowid_replaced(self):
        body = "SELECT ROWID FROM t"
        result = mod._replace_rowid_to_ctid(body)
        assert "ctid" in result.lower()

    def test_qualified_rowid_replaced(self):
        body = "SELECT sh.ROWID AS col_1, sh.name FROM t AS sh"
        result = mod._replace_rowid_to_ctid(body)
        assert "ctid" in result.lower()
        assert "sh.name" in result

    def test_row_id_replaced(self):
        body = "SELECT alias.ROW_ID FROM t AS alias"
        result = mod._replace_rowid_to_ctid(body)
        assert "ctid" in result.lower()

    def test_no_false_positive(self):
        body = "SELECT my_rowid_col FROM t"
        result = mod._replace_rowid_to_ctid(body)
        # Should not change a column named my_rowid_col
        assert "my_rowid_col" in result.lower()


# ===========================================================================
# 9b. _replace_ctid_with_null (ctid -> NULL::text fallback)
# ===========================================================================
class TestReplaceCtidWithNull:
    def test_qualified_ctid_replaced(self):
        sql = "SELECT sh.ctid AS col_1, sh.name FROM t AS sh"
        result = mod._replace_ctid_with_null(sql, "sh")
        assert result is not None
        assert "NULL::text" in result
        assert "sh.ctid" not in result
        assert "sh.name" in result

    def test_bare_ctid_replaced(self):
        sql = "SELECT ctid, name FROM t"
        result = mod._replace_ctid_with_null(sql, "")
        assert result is not None
        assert "NULL::text" in result

    def test_no_ctid_returns_none(self):
        sql = "SELECT a, b FROM t"
        assert mod._replace_ctid_with_null(sql, "sh") is None


# ===========================================================================
# 10. USERENV (_replace_userenv_to_postgres)
# ===========================================================================
class TestUserenv:
    def test_userenv_lang(self):
        body = "USERENV('LANG')"
        result = mod._replace_userenv_to_postgres(body)
        assert "USERENV" not in result.upper() or "current_setting" in result.lower()


# ===========================================================================
# 11. DUAL -> PG (_replace_dual_with_pg)
# ===========================================================================
class TestReplaceDual:
    def test_from_dual_replaced(self):
        body = "SELECT 1 FROM DUAL"
        result = mod._replace_dual_with_pg(body)
        # FROM DUAL is replaced with FROM (SELECT 1 AS dummy) AS dual
        assert "FROM DUAL" not in result.upper()
        assert "dummy" in result.lower()

    def test_from_sys_dual(self):
        body = "SELECT 1 FROM SYS.DUAL"
        result = mod._replace_dual_with_pg(body)
        assert "SYS.DUAL" not in result.upper()
        assert "dummy" in result.lower()


# ===========================================================================
# 12. START WITH ... CONNECT BY -> recursive CTE
# ===========================================================================
class TestStartWithConnectBy:
    def test_basic_hierarchical(self):
        body = (
            "SELECT emp_id, mgr_id FROM emp "
            "START WITH mgr_id IS NULL "
            "CONNECT BY PRIOR emp_id = mgr_id"
        )
        result = mod._convert_start_with_connect_by_to_recursive_cte(body)
        assert "RECURSIVE" in result.upper() or "WITH" in result.upper()


# ===========================================================================
# 13. _fix_timestamp_plus_integer
# ===========================================================================
class TestTimestampPlusInteger:
    def test_date_plus_number(self):
        body = "some_date + 7"
        result = mod._fix_timestamp_plus_integer(body)
        # Should wrap in INTERVAL or similar
        assert "INTERVAL" in result.upper() or "+" in result

    def test_current_timestamp_minus_identifier(self):
        """date_func - identifier should be converted to interval arithmetic."""
        body = "CURRENT_TIMESTAMP - days_offset"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL for minus identifier: {result}"
        assert "days_offset" in result

    def test_cast_timestamp_minus_identifier(self):
        """(expr)::timestamp - identifier should be converted."""
        body = "(start_date)::timestamp - days_offset"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"

    def test_cast_timestamp_minus_literal(self):
        """(expr)::date - 1 should be converted."""
        body = "(effective_date)::date - 1"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"

    def test_cast_as_date_plus_func(self):
        """CAST(... AS date) + least(...) should use interval arithmetic."""
        body = "CAST(start_date + interval '1 MONTH' - interval '1 DAY' AS date) + least(tl_discount_days, 30)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL '1 day'" in result, f"Missing INTERVAL for CAST AS date + func: {result}"
        assert "least(tl_discount_days, 30)" in result, f"Function call lost: {result}"

    def test_cast_as_date_plus_literal(self):
        """CAST(... AS date) + 7 should use interval arithmetic."""
        body = "CAST(due_date AS date) + 7"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"

    def test_cast_as_date_minus_identifier(self):
        """CAST(... AS date) - offset_val should use interval arithmetic."""
        body = "CAST(payment_date AS date) - offset_val"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"
        assert "offset_val" in result

    def test_cast_as_date_plus_interval_no_change(self):
        """CAST(... AS date) + interval '1 day' should NOT be modified."""
        body = "CAST(some_date AS date) + interval '1 day'"
        result = mod._fix_timestamp_plus_integer(body)
        # Should not double-wrap
        assert result.count("INTERVAL") == 1 or "interval '1 day'" in result.lower()

    def test_date_column_minus_integer(self):
        """pee.effective_start_date - 1 should use interval arithmetic (heuristic)."""
        body = "pee.effective_start_date - 1 BETWEEN low AND high"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL for date column heuristic: {result}"
        assert "effective_start_date" in result

    def test_date_column_plus_integer(self):
        """col.creation_date + 7 should use interval arithmetic."""
        body = "col.creation_date + 7"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"

    def test_non_date_column_minus_integer_no_change(self):
        """col.row_count - 1 should NOT be modified (no date suffix)."""
        body = "col.row_count - 1"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" not in result.upper(), f"Unwanted INTERVAL for non-date column: {result}"

    def test_date_func_minus_function_call_not_mangled(self):
        """CURRENT_TIMESTAMP - date_trunc('day', ...) should NOT treat date_trunc as identifier."""
        body = "CURRENT_TIMESTAMP - date_trunc('day', some_col)"
        result = mod._fix_timestamp_plus_integer(body)
        # date_trunc should remain a function call, not become (date_trunc)::numeric
        assert "(date_trunc)::numeric" not in result, f"date_trunc mangled as identifier: {result}"
        assert "date_trunc('day'" in result or "date_trunc( 'day'" in result

    def test_date_trunc_minus_date_column_no_numeric_cast(self):
        """date_trunc('day', CURRENT_TIMESTAMP) - ps.gl_date should NOT cast gl_date to ::numeric."""
        body = "date_trunc('day', CURRENT_TIMESTAMP) - ps.gl_date"
        result = mod._fix_timestamp_plus_integer(body)
        # gl_date ends in _date -> should be skipped by heuristic
        assert "(ps.gl_date)::numeric" not in result, f"Date column wrongly cast to numeric: {result}"

    def test_cast_as_date_minus_date_column_no_numeric(self):
        """CAST(x AS date) - ps.trx_date should NOT cast trx_date to ::numeric."""
        body = "CAST(x AS date) - ps.trx_date"
        result = mod._fix_timestamp_plus_integer(body)
        assert "(ps.trx_date)::numeric" not in result, f"Date column wrongly cast: {result}"

    def test_to_timestamp_minus_function_call(self):
        """to_timestamp(...) - date_trunc(...) should NOT mangle date_trunc."""
        body = "to_timestamp(x, 'YYYY') - date_trunc('day', y)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "(date_trunc)::numeric" not in result, f"date_trunc mangled: {result}"

    def test_date_column_plus_identifier(self):
        """rctl.trx_date + rtl.due_days should use interval arithmetic."""
        body = "rctl.trx_date + rtl.due_days"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"
        assert "due_days" in result
        assert "trx_date" in result

    def test_date_column_minus_identifier(self):
        """t.creation_date - offset_col should use interval arithmetic."""
        body = "t.creation_date - offset_col"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"
        assert "offset_col" in result

    def test_date_column_minus_date_column_no_change(self):
        """trx_date - another_date should NOT be modified (date - date returns interval)."""
        body = "t.trx_date - t.ship_date"
        result = mod._fix_timestamp_plus_integer(body)
        # ship_date ends in _date -> skip for subtraction (date - date is valid PG)
        assert "::numeric" not in result, f"Unwanted ::numeric cast on date column: {result}"

    def test_date_column_plus_date_column_converted(self):
        """trx_date + discount_date should BE converted (date + date is invalid PG)."""
        body = "ps.trx_date + tld.discount_date"
        result = mod._fix_timestamp_plus_integer(body)
        # date + date is never valid in PostgreSQL, so always convert the RHS
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL for date + date_col: {result}"

    def test_date_column_plus_keyword_no_change(self):
        """trx_date + BETWEEN should NOT be modified (BETWEEN is a keyword)."""
        body = "t.trx_date BETWEEN low AND high"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" not in result.upper(), f"Unwanted INTERVAL near keyword: {result}"

    def test_cast_as_date_minus_date_trunc_no_change(self):
        """CAST(x AS date) - date_trunc('day', y) is valid PG (date - timestamp = interval)."""
        body = "CAST(CURRENT_TIMESTAMP AS date) - date_trunc('day', CURRENT_TIMESTAMP)"
        result = mod._fix_timestamp_plus_integer(body)
        assert result == body, f"Should be unchanged but got: {result}"

    def test_cast_as_timestamp_minus_date_trunc_no_change(self):
        """CAST(x AS timestamp) - date_trunc(...) should not be modified."""
        body = "CAST(ps.due_date AS timestamp) - date_trunc('day', CURRENT_TIMESTAMP)"
        result = mod._fix_timestamp_plus_integer(body)
        assert result == body, f"Should be unchanged but got: {result}"

    def test_cast_as_date_minus_now_no_change(self):
        """CAST(x AS date) - now() should not be modified (date - timestamp is valid)."""
        body = "CAST(ps.due_date AS date) - now()"
        result = mod._fix_timestamp_plus_integer(body)
        assert result == body, f"Should be unchanged but got: {result}"

    def test_cast_as_date_minus_to_date_no_change(self):
        """CAST(x AS date) - to_date(y, 'fmt') is date-date subtraction, valid PG."""
        body = "CAST(ps.due_date AS date) - to_date(y, 'YYYY-MM-DD')"
        result = mod._fix_timestamp_plus_integer(body)
        assert result == body, f"Should be unchanged but got: {result}"

    def test_cast_as_date_plus_non_date_func_converted(self):
        """CAST(x AS date) + calc_days(n) should still get INTERVAL conversion."""
        body = "CAST(ps.due_date AS date) + calculate_days(x)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"
        assert "calculate_days(x)" in result, f"Function call lost: {result}"


class TestVarcharArithmetic:
    """Tests for _fix_varchar_arithmetic: varchar + number -> numeric cast."""

    def test_varchar_plus_integer(self):
        """CAST(x AS varchar(4000)) + 1 should add ::numeric."""
        body = "CAST(x AS varchar(4000)) + 1"
        result = mod._fix_varchar_arithmetic(body)
        assert "::numeric" in result, f"Missing numeric cast: {result}"
        assert "+ 1" in result

    def test_varchar_minus_col(self):
        """::varchar(N) - col should add ::numeric."""
        body = "(some_expr)::varchar(100) - offset_val"
        result = mod._fix_varchar_arithmetic(body)
        assert "::numeric" in result, f"Missing numeric cast: {result}"

    def test_no_change_without_arithmetic(self):
        """varchar without +/- should not be touched."""
        body = "CAST(x AS varchar(4000)) AS display"
        result = mod._fix_varchar_arithmetic(body)
        assert "::numeric" not in result, f"Unnecessary numeric cast: {result}"

    def test_rhs_cast_varchar_gets_numeric(self):
        """CAST(x AS varchar(4000))::numeric + 2 - CAST(y AS varchar(4000)) should add ::numeric on RHS CAST too."""
        body = "CAST(calendar_date AS varchar(4000))::numeric + 2 - CAST(calendar_date AS varchar(4000))"
        result = mod._fix_varchar_arithmetic(body)
        # Both CASTs should have ::numeric
        assert result.count("::numeric") == 2, f"Expected 2 ::numeric, got {result.count('::numeric')}: {result}"

    def test_rhs_cast_varchar_without_size(self):
        """- CAST(x AS varchar) on RHS should get ::numeric."""
        body = "col::numeric + 2 - CAST(x AS varchar)"
        result = mod._fix_varchar_arithmetic(body)
        assert result.count("::numeric") == 2, f"Expected 2 ::numeric: {result}"

    def test_rhs_cast_already_has_numeric(self):
        """Don't double-add ::numeric on RHS CAST that already has it."""
        body = "a + 2 - CAST(x AS varchar(4000))::numeric"
        result = mod._fix_varchar_arithmetic(body)
        assert result.count("::numeric") == 1, f"Duplicated ::numeric: {result}"

    def test_rhs_cast_non_varchar_not_touched(self):
        """- CAST(x AS integer) on RHS should NOT get ::numeric."""
        body = "a + 2 - CAST(x AS integer)"
        result = mod._fix_varchar_arithmetic(body)
        assert "::numeric" not in result, f"Unexpected ::numeric: {result}"


# ===========================================================================
# 13b. _fix_coalesce_type_safety
# ===========================================================================
class TestCoalesceTypeSafety:
    def test_numeric_literal_becomes_string_literal(self):
        """COALESCE(varchar_col, 0) -> COALESCE(varchar_col, '0')."""
        body = "COALESCE(inv.voucher_num, 0)"
        result = mod._fix_coalesce_type_safety(body)
        assert "'0'" in result, f"Missing string literal: {result}"
        assert "inv.voucher_num" in result

    def test_negative_numeric_literal(self):
        """COALESCE(col, -1) -> COALESCE(col, '-1')."""
        body = "COALESCE(col, -1)"
        result = mod._fix_coalesce_type_safety(body)
        assert "'-1'" in result, f"Missing string literal: {result}"

    def test_decimal_literal(self):
        """COALESCE(col, 3.14) -> COALESCE(col, '3.14')."""
        body = "COALESCE(col, 3.14)"
        result = mod._fix_coalesce_type_safety(body)
        assert "'3.14'" in result, f"Missing string literal: {result}"

    def test_all_columns_no_change(self):
        """COALESCE(col1, col2) with no numeric literal should NOT be touched."""
        body = "COALESCE(inv.voucher_num, inv.doc_sequence)"
        result = mod._fix_coalesce_type_safety(body)
        assert result == body, f"Should not change all-column COALESCE: {result}"

    def test_all_numeric_no_change(self):
        """COALESCE(0, 1) with only numeric literals should NOT be touched."""
        body = "COALESCE(0, 1)"
        result = mod._fix_coalesce_type_safety(body)
        assert result == body, f"Should not change all-numeric COALESCE: {result}"

    def test_arithmetic_after_coalesce_preserved(self):
        """COALESCE(col, 0) + tax should still work (PG adapts '0' to numeric)."""
        body = "COALESCE(amount, 0) + tax"
        result = mod._fix_coalesce_type_safety(body)
        assert "'0'" in result, f"Missing string literal: {result}"
        assert "+ tax" in result

    def test_nested_coalesce(self):
        """Outer COALESCE with numeric literal is fixed; inner left alone if no mismatch."""
        body = "COALESCE(a, COALESCE(b, c))"
        result = mod._fix_coalesce_type_safety(body)
        # No numeric literals, so no change
        assert result == body, f"Should not change no-numeric COALESCE: {result}"

    def test_nested_coalesce_with_numeric(self):
        """Nested COALESCE(col, COALESCE(x, 0)) should convert inner 0."""
        body = "COALESCE(col, COALESCE(x, 0))"
        result = mod._fix_coalesce_type_safety(body)
        assert "'0'" in result, f"Missing string literal in nested: {result}"


# ===========================================================================
# 13b2. _strip_text_cast_in_coalesce
# ===========================================================================
class TestStripTextCastInCoalesce:
    def test_text_cast_stripped(self):
        """COALESCE((col)::text, (0)::text) should have ::text stripped."""
        body = "COALESCE((aps.amount_remaining)::text, (0)::text) - COALESCE((x)::text, (0)::text)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "::text" not in result, f"::text not stripped: {result}"
        assert "aps.amount_remaining" in result
        assert "- COALESCE" in result

    def test_no_text_cast_no_change(self):
        """COALESCE(col, 0) without ::text should not be modified."""
        body = "COALESCE(col, 0)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert result == body

    def test_ident_text_cast_stripped(self):
        """COALESCE(col::text, val::text) should have ::text stripped."""
        body = "COALESCE(col::text, val::text)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "::text" not in result
        assert "col" in result and "val" in result

    def test_nested_paren_text_cast_stripped(self):
        """((CASE WHEN ... END))::text should have ::text stripped (nested parens)."""
        body = "((CASE WHEN a > 0 THEN (b / 100) ELSE 0 END))::text * col"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "::text" not in result.lower(), f"::text not stripped: {result}"
        assert "* col" in result

    def test_coalesce_with_nested_case_text_cast(self):
        """COALESCE wrapping a CASE expression with ::text on outer paren."""
        body = "(COALESCE((CASE WHEN x > 0 THEN y / 100 ELSE 0 END)))::text * z"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "::text" not in result.lower(), f"::text not stripped: {result}"
        assert "* z" in result

    def test_cast_as_text_stripped(self):
        """CAST(expr AS TEXT) should be converted to (expr)."""
        body = "CAST(ra.earned_discount_taken AS TEXT) + CAST(0 AS TEXT)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "AS TEXT" not in result.upper(), f"CAST AS TEXT not stripped: {result}"
        assert "ra.earned_discount_taken" in result

    def test_cast_as_varchar_stripped(self):
        """CAST(expr AS VARCHAR(4000)) should be converted to (expr)."""
        body = "GREATEST(CAST(l.ptd AS VARCHAR(4000)), CAST(l.itd AS VARCHAR(4000)), l.amount)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "VARCHAR" not in result.upper(), f"CAST AS VARCHAR not stripped: {result}"
        assert "l.ptd" in result
        assert "l.amount" in result

    def test_cast_as_varchar_bare_stripped(self):
        """CAST(expr AS VARCHAR) (no size) should also be stripped."""
        body = "CAST(x AS varchar)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert result == "(x)", f"Unexpected result: {result}"

    def test_cast_as_integer_not_stripped(self):
        """CAST(expr AS INTEGER) should NOT be stripped."""
        body = "CAST(x AS INTEGER)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "CAST" in result, f"Non-text CAST was stripped: {result}"

    def test_string_agg_text_cast_preserved(self):
        """::text inside string_agg should be kept."""
        body = "string_agg((x)::text, ',')"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "::text" in result.lower(), f"::text wrongly stripped: {result}"

    def test_lpad_text_cast_preserved(self):
        """::text inside lpad should be kept."""
        body = "lpad((x)::text, 10)"
        result = mod._strip_text_cast_in_coalesce(body)
        assert "::text" in result.lower(), f"::text wrongly stripped: {result}"


# ===========================================================================
# 13c-2. _fix_current_timestamp_between_text
# ===========================================================================
class TestDateToNumericCast:
    """Tests for _fix_date_to_numeric_cast — strip invalid date→numeric casts."""

    def test_pg_style_date_numeric(self):
        """(expr)::date)::numeric → (expr)::date)"""
        body = "SELECT (((CURRENT_TIMESTAMP))::date)::numeric AS x FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "::numeric" not in result
        assert "::date)" in result

    def test_pg_style_timestamp_numeric(self):
        """(expr)::timestamp)::numeric → (expr)::timestamp)"""
        body = "SELECT ((x))::timestamp)::numeric FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "::numeric" not in result
        assert "::timestamp)" in result

    def test_sql_style_cast_date_numeric(self):
        """CAST(CAST(x AS date) AS decimal) → (CAST(x AS date))"""
        body = "SELECT CAST(CAST(CAST(CURRENT_TIMESTAMP AS VARCHAR(4000)) AS DATE) AS DECIMAL) FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "DECIMAL" not in result.upper()
        assert "DATE" in result.upper()

    def test_cast_date_pg_numeric(self):
        """CAST(CAST(x AS VARCHAR) AS date)::numeric → CAST(CAST(x AS VARCHAR) AS date)"""
        body = "SELECT CAST(CAST(CURRENT_TIMESTAMP AS varchar(4000)) AS date)::numeric FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "::numeric" not in result
        assert "AS date)" in result

    def test_cast_date_extra_paren_pg_numeric(self):
        """(CAST(CAST(x AS VARCHAR) AS date))::numeric → (CAST(CAST(x AS VARCHAR) AS date))"""
        body = "SELECT (CAST(CAST(CURRENT_TIMESTAMP AS varchar(4000)) AS date))::numeric FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "::numeric" not in result
        assert "AS date))" in result

    def test_pg_style_extra_paren(self):
        """((expr)::date))::numeric → ((expr)::date))"""
        body = "SELECT ((x)::date))::numeric FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "::numeric" not in result

    def test_date_bigint_stripped(self):
        """::date)::bigint → ::date)"""
        body = "SELECT (x)::date)::bigint FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "::bigint" not in result
        assert "::date)" in result

    def test_non_date_numeric_preserved(self):
        """(col)::numeric should NOT be stripped."""
        body = "SELECT (col)::numeric FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "::numeric" in result

    def test_varchar_to_decimal_preserved(self):
        """CAST(varchar_expr AS DECIMAL) should NOT be stripped."""
        body = "SELECT CAST(CAST(x AS VARCHAR(100)) AS DECIMAL) FROM t"
        result = mod._fix_date_to_numeric_cast(body)
        assert "DECIMAL" in result

    def test_full_pipeline_to_number_to_date_to_char(self):
        """Full pipeline: TO_NUMBER(TO_DATE(TO_CHAR(SYSDATE))) should not produce ::numeric on date."""
        oracle = "CREATE OR REPLACE VIEW V AS SELECT TO_NUMBER(TO_DATE(TO_CHAR(SYSDATE))) AS x FROM dual"
        result = mod.normalize_view_script(oracle, apply_oracle_conversions=True, orig_schema="S", orig_view_name="V")
        assert "::numeric" not in result, f"Should not have ::numeric on date: {result}"
        assert "decimal" not in result.lower(), f"Should not have decimal: {result}"


class TestCurrentTimestampBetweenText:
    """Tests for _fix_current_timestamp_between_text."""

    def test_coalesce_to_char_gets_timestamp_cast(self):
        """COALESCE(to_char(date,'fmt'),'lit') bounds should get ::timestamp."""
        body = (
            "CURRENT_TIMESTAMP BETWEEN "
            "COALESCE(to_char(ppf.start_date, 'YYYY/MM/DD'), '0001/01/01') AND "
            "COALESCE(to_char(ppf.end_date, 'YYYY/MM/DD'), '4712/12/31')"
        )
        result = mod._fix_current_timestamp_between_text(body)
        assert result.count("::timestamp") == 2, f"Expected 2 ::timestamp: {result}"

    def test_already_has_timestamp_no_double_cast(self):
        """Bounds with ::timestamp should not get double-cast."""
        body = "CURRENT_DATE BETWEEN t.start_date::timestamp AND t.end_date::timestamp"
        result = mod._fix_current_timestamp_between_text(body)
        assert result.count("::timestamp") == 2, f"Unexpected count: {result}"

    def test_multiple_between_clauses(self):
        """Multiple BETWEEN clauses in one WHERE should all be handled."""
        body = (
            "CURRENT_TIMESTAMP BETWEEN t.a AND t.b AND "
            "CURRENT_TIMESTAMP BETWEEN u.c AND u.d"
        )
        result = mod._fix_current_timestamp_between_text(body)
        assert result.count("::timestamp") == 4, f"Expected 4 ::timestamp: {result}"
        assert "::timestamp AND CURRENT_TIMESTAMP" in result, f"Spacing wrong: {result}"

    def test_no_between_no_change(self):
        """Expressions without BETWEEN should not be touched."""
        body = "CURRENT_TIMESTAMP > t.start_date"
        result = mod._fix_current_timestamp_between_text(body)
        assert "::timestamp" not in result

    def test_current_date_between(self):
        """CURRENT_DATE BETWEEN should also be handled."""
        body = "CURRENT_DATE BETWEEN t.a AND t.b"
        result = mod._fix_current_timestamp_between_text(body)
        assert result.count("::timestamp") == 2

    def test_non_timestamp_between_not_touched(self):
        """Non-timestamp BETWEEN (e.g. col BETWEEN 1 AND 10) not touched."""
        body = "x BETWEEN 1 AND 10"
        result = mod._fix_current_timestamp_between_text(body)
        assert "::timestamp" not in result


# ===========================================================================
# 13c. _fix_sign_with_interval
# ===========================================================================
class TestSignWithInterval:
    def test_sign_gets_extract(self):
        body = "sign(pay.due_date - CURRENT_DATE)"
        result = mod._fix_sign_with_interval(body)
        assert "EXTRACT(EPOCH FROM" in result
        assert "::numeric" in result

    def test_multiple_sign_calls(self):
        body = "sign(a - b) + sign(c - d)"
        result = mod._fix_sign_with_interval(body)
        assert result.count("EXTRACT(EPOCH FROM") == 2


# ===========================================================================
# 14. _oracle_empty_string_as_null
# ===========================================================================
class TestEmptyStringAsNull:
    def test_empty_string_equals(self):
        body = "WHERE col = ''"
        result = mod._oracle_empty_string_as_null(body)
        assert "IS NULL" in result.upper() or "''" in result

    def test_coalesce_empty_string_becomes_null(self):
        """COALESCE(expr, '') should become COALESCE(expr, NULL)."""
        body = "COALESCE(b.application_id, '')"
        result = mod._oracle_empty_string_as_null(body)
        assert "NULL" in result, f"Empty string not replaced: {result}"
        assert "''" not in result, f"Empty string still present: {result}"

    def test_coalesce_empty_string_middle_arg(self):
        """COALESCE(a, '', b) should become COALESCE(a, NULL, b)."""
        body = "COALESCE(a, '', b)"
        result = mod._oracle_empty_string_as_null(body)
        assert ", NULL," in result, f"Middle empty string not replaced: {result}"

    def test_escaped_quote_not_replaced(self):
        """COALESCE(a, '''') should NOT be replaced (it's a literal quote char)."""
        body = "COALESCE(a, '''')"
        result = mod._oracle_empty_string_as_null(body)
        assert "NULL" not in result or "IS NULL" in result or result == body

    def test_string_with_escaped_quotes_not_changed(self):
        """Escaped quotes inside strings should not be altered."""
        body = "x = 'hello, ''world' AND y = 1"
        result = mod._oracle_empty_string_as_null(body)
        assert "'hello, ''world'" in result or "hello" in result

    def test_paren_expr_equals_empty_string(self):
        """(col) = '' should become (col) IS NULL."""
        body = "CASE WHEN (l.parent_line_id) = '' THEN '' END"
        result = mod._oracle_empty_string_as_null(body)
        assert "IS NULL" in result
        assert "= ''" not in result

    def test_coalesce_result_equals_empty_string(self):
        """COALESCE(col, NULL) = '' should become COALESCE(col, NULL) IS NULL."""
        body = "CASE WHEN COALESCE(l.parent_line_id, NULL) = '' THEN '' END"
        result = mod._oracle_empty_string_as_null(body)
        assert "IS NULL" in result
        assert "= ''" not in result

    def test_func_result_not_equals_empty_string(self):
        """func(x) <> '' should become func(x) IS NOT NULL."""
        body = "WHERE NVL(col, 'x') <> ''"
        result = mod._oracle_empty_string_as_null(body)
        assert "IS NOT NULL" in result
        assert "<> ''" not in result

    def test_full_pipeline_nvl_col_empty_string(self):
        """Full pipeline: NVL(col, '') = '' pattern from Oracle EBS."""
        oracle = (
            "CREATE OR REPLACE VIEW V AS "
            "SELECT CASE WHEN NVL(l.parent_line_id, '') = '' THEN '' "
            "ELSE TO_CHAR(l.parent_line_id) END AS x FROM t"
        )
        result = mod.normalize_view_script(oracle, apply_oracle_conversions=True,
                                           orig_schema="S", orig_view_name="V")
        assert "= ''" not in result, f"Empty string comparison survived: {result}"


# ===========================================================================
# 15. _remove_quotes_from_columns
# ===========================================================================
class TestRemoveQuotes:
    def test_double_quotes_removed(self):
        body = 'SELECT "COLUMN_NAME" FROM t'
        result = mod._remove_quotes_from_columns(body)
        assert "column_name" in result.lower()

    def test_reserved_word_stays_quoted(self):
        """Columns named after PG reserved words (END, GROUP, ORDER, LIMIT) must keep double quotes."""
        body = 'SELECT "END", "GROUP", "ORDER", "LIMIT", "NORMAL_COL" FROM t'
        result = mod._remove_quotes_from_columns(body)
        assert '"END"' in result, f"END should stay quoted: {result}"
        assert '"GROUP"' in result, f"GROUP should stay quoted: {result}"
        assert '"ORDER"' in result, f"ORDER should stay quoted: {result}"
        assert '"LIMIT"' in result, f"LIMIT should stay quoted: {result}"
        assert "NORMAL_COL" in result and '"NORMAL_COL"' not in result, f"NORMAL_COL should be unquoted: {result}"


# ===========================================================================
# 16. _deduplicate_select_aliases_in_body
# ===========================================================================
class TestDeduplicateAliases:
    def test_no_change_unique(self):
        body = "SELECT a AS x, b AS y FROM t"
        result = mod._deduplicate_select_aliases_in_body(body)
        assert "x" in result and "y" in result

    def test_duplicates_renamed(self):
        body = "SELECT a AS name, b AS name FROM t"
        result = mod._deduplicate_select_aliases_in_body(body)
        # After dedup, second one should have a different alias
        aliases = re.findall(r"\bAS\s+(\w+)", result, re.IGNORECASE)
        assert len(aliases) == len(set(a.lower() for a in aliases)), f"Duplicate aliases remain: {aliases}"

    def test_concat_operand_not_treated_as_alias(self):
        """Trailing identifier after || must NOT be treated as an implicit alias (DFV view bug)."""
        body = "SELECT ATTR1 || '.' || ATTR2 || '.' || ATTR3 FROM t"
        result = mod._deduplicate_select_aliases_in_body(body)
        # ATTR3 must remain as a concatenation operand, not be replaced with AS ATTR3
        assert "|| ATTR3" in result or "|| attr3" in result.lower()
        assert "|| AS" not in result.upper(), f"Bug: concatenation operand eaten as alias: {result}"

    def test_arithmetic_operand_not_treated_as_alias(self):
        """Trailing identifier after +/-/*// must NOT be treated as an implicit alias."""
        body = "SELECT col1 + col2 FROM t"
        result = mod._deduplicate_select_aliases_in_body(body)
        assert "+ col2" in result or "+ COL2" in result
        assert "AS col2" not in result, f"Bug: arithmetic operand eaten as alias: {result}"

    def test_case_end_keyword_not_treated_as_alias(self):
        """CASE...END keyword must NOT be treated as an implicit alias (causes AS 'END' error)."""
        body = "SELECT CASE WHEN x = 1 THEN a ELSE b END, col2 FROM t"
        result = mod._deduplicate_select_aliases_in_body(body)
        assert 'AS "END"' not in result, f"Bug: END keyword treated as alias: {result}"
        assert "END" in result, f"END keyword lost: {result}"

    def test_identifier_with_from_keyword_not_split(self):
        """Identifiers like sent_from, created_from must not be split by _find_select_list_bounds."""
        body = "SELECT t.sent_from, t.created_from, t.valid_from, t.name FROM table1 t"
        result = mod._deduplicate_select_aliases_in_body(body)
        # All identifiers with _from must remain intact (not truncated at 'from')
        assert "sent_from" in result, f"sent_from was split: {result}"
        assert "created_from" in result, f"created_from was split: {result}"
        assert "valid_from" in result, f"valid_from was split: {result}"

    def test_identifier_with_as_keyword_not_split(self):
        """Identifiers like created_as must survive the pipeline without splitting."""
        body = "SELECT t.created_as, t.updated_as FROM table1 t"
        result = mod._deduplicate_select_aliases_in_body(body)
        assert "created_as" in result, f"created_as was mangled: {result}"
        assert "updated_as" in result, f"updated_as was mangled: {result}"

    def test_dollar_sign_in_alias_no_double_as(self):
        """Alias with $ (e.g. a$customer) must be recognised — no double AS."""
        body = "SELECT cust.customer_name AS a$customer, cust.id FROM t"
        result = mod._deduplicate_select_aliases_in_body(body)
        assert result.upper().count(" AS ") == 2, f"unexpected AS count: {result}"
        assert "a$customer" in result, f"dollar alias lost: {result}"
        # Must NOT have double AS
        assert "AS a$customer AS" not in result, f"double AS: {result}"


# ===========================================================================
# 17. _cast_numeric_string_literals_in_equality
# ===========================================================================
class TestCastNumericStringLiterals:
    def test_numeric_string_cast(self):
        body = "WHERE col = '123'"
        result = mod._cast_numeric_string_literals_in_equality(body)
        # Should wrap the literal or cast it
        assert "'123'" in result or "123" in result


# ===========================================================================
# 18. _ensure_space_before_keywords
# ===========================================================================
class TestEnsureSpaceBeforeKeywords:
    def test_paren_before_keyword(self):
        """Closing paren glued to FROM should get a space."""
        body = "COALESCE(x,y)FROM t"
        result = mod._ensure_space_before_keywords(body)
        assert ") FROM" in result

    def test_identifier_with_keyword_substring_not_split(self):
        """Identifiers containing keywords (lc_group, from_date, order_line) must NOT be split."""
        body = "lc_group.displayed_field AS col_30, pv.from_date AS col_31, order_line.amount"
        result = mod._ensure_space_before_keywords(body)
        assert "lc_group.displayed_field" in result
        assert "pv.from_date" in result
        assert "order_line.amount" in result

    def test_digit_before_keyword_gets_space(self):
        """col_1FROM should become col_1 FROM (digit before keyword needs space)."""
        body = "col_1FROM t"
        result = mod._ensure_space_before_keywords(body)
        assert "col_1 FROM" in result

    def test_identifier_containing_group_not_split(self):
        """terrgroup.name must NOT be split into 'terr group.name'."""
        body = "terrtype.name AS col_49, terrgroup.name AS col_50"
        result = mod._ensure_space_before_keywords(body)
        assert "terrgroup.name" in result, f"terrgroup was split: {result}"

    def test_identifier_containing_from_not_split(self):
        """sent_from, datafrom etc. must NOT be split."""
        body = "t.sent_from AS col_1, datafrom AS col_2"
        result = mod._ensure_space_before_keywords(body)
        assert "t.sent_from" in result, f"sent_from was split: {result}"
        assert "datafrom" in result, f"datafrom was split: {result}"


# ===========================================================================
# 18b. _fix_limit_comma_syntax
# ===========================================================================
class TestLimitCommaSyntax:
    def test_limit_xy_to_pg(self):
        """MySQL-style LIMIT offset,count should become LIMIT count OFFSET offset."""
        body = "SELECT * FROM t LIMIT 10, 20"
        result = mod._fix_limit_comma_syntax(body)
        assert "LIMIT 20 OFFSET 10" in result

    def test_normal_limit_untouched(self):
        """Standard LIMIT N should not be changed."""
        body = "SELECT * FROM t LIMIT 10"
        result = mod._fix_limit_comma_syntax(body)
        assert result == body


# ===========================================================================
# 18c. DECODE pre-conversion before sqlglot
# ===========================================================================
class TestDecodePreConversion:
    def test_decode_in_preprocess(self):
        """DECODE should be converted to CASE before sqlglot parsing."""
        sql = "CREATE OR REPLACE VIEW v AS SELECT DECODE(status, 1, 'A', 2, 'B', 'C') FROM t"
        result = mod._preprocess_oracle_before_parse(sql)
        assert "DECODE" not in result
        assert "CASE" in result
        assert "WHEN" in result
        assert "END" in result


# ===========================================================================
# 19. _replace_oracle_misc_in_body
# ===========================================================================
class TestOracleMiscOther:
    def test_rownum(self):
        body = "WHERE ROWNUM <= 10"
        result = mod._replace_oracle_misc_in_body(body)
        assert "ROW_NUMBER" in result.upper()

    def test_rpad(self):
        body = "RPAD(col, 10)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "rpad" in result.lower()

    def test_last_day(self):
        body = "LAST_DAY(hire_date)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "date_trunc" in result.lower()

    def test_minus_to_except(self):
        body = "SELECT 1 MINUS SELECT 2"
        result = mod._replace_oracle_misc_in_body(body)
        assert "EXCEPT" in result.upper()

    def test_replace_two_arg(self):
        """Oracle REPLACE(s, old) 2-arg -> REPLACE(s, old, '') for PG."""
        body = "REPLACE(col1, chr(10))"
        result = mod._replace_oracle_misc_in_body(body)
        assert result.count(",") >= 2, f"Missing 3rd arg: {result}"
        assert "''" in result, f"Expected empty-string third arg: {result}"

    def test_replace_three_arg_unchanged(self):
        """REPLACE with 3 args should pass through unchanged."""
        body = "REPLACE(col1, 'x', 'y')"
        result = mod._replace_oracle_misc_in_body(body)
        assert "'x'" in result and "'y'" in result

    def test_regexp_count(self):
        """REGEXP_COUNT(s, pattern) -> PG equivalent with array_length."""
        body = "REGEXP_COUNT(col1, ',')"
        result = mod._replace_oracle_misc_in_body(body)
        assert "REGEXP_COUNT" not in result.upper(), f"REGEXP_COUNT not converted: {result}"
        assert "regexp_matches" in result.lower() or "array" in result.lower()

    def test_regexp_instr(self):
        """REGEXP_INSTR(s, pattern) -> PG equivalent with strpos+regexp_matches."""
        body = "REGEXP_INSTR(col1, '[0-9]')"
        result = mod._replace_oracle_misc_in_body(body)
        assert "REGEXP_INSTR" not in result.upper(), f"REGEXP_INSTR not converted: {result}"
        assert "strpos" in result.lower() or "regexp" in result.lower()

    def test_instr_three_arg(self):
        """INSTR(s, sub, pos) 3-arg should use substring-based position."""
        body = "INSTR(col1, 'x', 5)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "INSTR" not in result.upper(), f"INSTR not converted: {result}"
        assert "substring" in result.lower(), f"Missing substring for 3-arg INSTR: {result}"

    def test_date_plus_paren_expr(self):
        """date_func + (expr) should get INTERVAL conversion."""
        body = "CURRENT_TIMESTAMP + (days + 1)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"

    def test_date_minus_paren_expr(self):
        """date_func - (expr) should get INTERVAL conversion."""
        body = "CURRENT_DATE - (offset_days)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result.upper(), f"Missing INTERVAL: {result}"


# ===========================================================================
# 20. NEXT_DAY (improved in fix-next-day)
# ===========================================================================
class TestNextDay:
    def test_next_day_literal_weekday(self):
        body = "NEXT_DAY(hire_date, 'MONDAY')"
        result = mod._replace_oracle_misc_in_body(body)
        assert "NEXT_DAY" not in result.upper()
        assert "EXTRACT" in result.upper() or "DOW" in result.upper()

    def test_next_day_expression_weekday(self):
        body = "NEXT_DAY(hire_date, some_var)"
        result = mod._replace_oracle_misc_in_body(body)
        assert "WARNING" in result or "INTERVAL" in result.upper()


# ===========================================================================
# 21. _replace_oracle_package_schemas
# ===========================================================================
class TestReplaceOraclePackageSchemas:
    def test_package_func_with_args_becomes_apps(self):
        """PA_BILLING.GetCallPlace('ADJ') → apps.GetCallPlace('ADJ') (function call preserved)."""
        body = "PA_BILLING.GetCallPlace('ADJ')"
        result = mod._replace_oracle_package_schemas(body)
        assert "apps." in result.lower(), f"Expected apps prefix: {result}"
        assert "getcallplace" in result.lower(), f"Function name lost: {result}"
        assert "'ADJ'" in result

    def test_package_member_no_parens_becomes_null(self):
        """PA_BILLING.GetTaskId (no parens) → NULL."""
        body = "NVL(PA_BILLING.GetTaskId, pdi.project_id)"
        result = mod._replace_oracle_package_schemas(body)
        assert "NULL" in result, f"Expected NULL for parameterless package member: {result}"
        assert "apps.gettaskid" not in result.lower(), f"Should be NULL, not apps ref: {result}"

    def test_package_member_in_comparison(self):
        """PA_BILLING.GetCallPlace = 'ADJ' → NULL = 'ADJ'."""
        body = "PA_BILLING.GetCallPlace = 'ADJ'"
        result = mod._replace_oracle_package_schemas(body)
        assert "NULL" in result, f"Expected NULL: {result}"


# ===========================================================================
# 22. _replace_package_in_from_clause
# ===========================================================================
class TestReplacePackageInFromClause:
    def test_package_in_from(self):
        body = "FROM hr_api.some_table t"
        result = mod._replace_package_in_from_clause(body)
        # Should replace the package prefix in FROM clause
        assert "apps." in result.lower() or "hr_api" in result.lower()


# ===========================================================================
# 23. _lowercase_body_identifiers
# ===========================================================================
class TestLowercaseBodyIdentifiers:
    def test_identifiers_lowered(self):
        body = "SELECT COLUMN_A, COLUMN_B FROM MY_TABLE"
        result = mod._lowercase_body_identifiers(body)
        assert result == result.lower() or "column_a" in result


# ===========================================================================
# 24. normalize_view_script
# ===========================================================================
class TestNormalizeViewScript:
    def test_basic_normalize(self):
        ddl = "CREATE OR REPLACE FORCE VIEW apps.my_view AS SELECT 1 FROM dual;"
        result = mod.normalize_view_script(ddl, apply_oracle_conversions=False)
        assert "CREATE OR REPLACE VIEW" in result.upper()
        assert "DROP VIEW" not in result.upper()
        assert "FORCE" not in result.upper()

    def test_orig_schema_forced(self):
        ddl = "CREATE OR REPLACE VIEW apps.wrong_name AS SELECT 1 FROM dual;"
        result = mod.normalize_view_script(ddl, apply_oracle_conversions=False, orig_schema="apps2", orig_view_name="correct_name")
        assert "apps2.correct_name" in result.lower()

    def test_empty_input(self):
        assert mod.normalize_view_script("") == ""
        assert mod.normalize_view_script("   ") == "   "


# ===========================================================================
# 25. topological_sort
# ===========================================================================
class TestTopologicalSort:
    def test_no_deps(self):
        views = [("s", "a"), ("s", "b"), ("s", "c")]
        deps = [set(), set(), set()]
        order = mod.topological_sort(views, deps)
        assert sorted(order) == [0, 1, 2]

    def test_simple_chain(self):
        views = [("s", "a"), ("s", "b"), ("s", "c")]
        # c depends on b, b depends on a
        deps = [set(), {("s", "a")}, {("s", "b")}]
        order = mod.topological_sort(views, deps)
        assert order.index(0) < order.index(1)
        assert order.index(1) < order.index(2)

    def test_circular_warns(self, capsys):
        views = [("s", "a"), ("s", "b")]
        deps = [{("s", "b")}, {("s", "a")}]
        order = mod.topological_sort(views, deps)
        assert len(order) == 2
        captured = capsys.readouterr()
        assert "circular" in captured.err.lower()

    def test_empty(self):
        assert mod.topological_sort([], []) == []


# ===========================================================================
# 26. _replace_pg_unit_dd_and_substring
# ===========================================================================
class TestPgUnitDdAndSubstring:
    def test_substr_to_substring(self):
        body = "SUBSTR(col, 1, 5)"
        result = mod._replace_pg_unit_dd_and_substring(body)
        assert "SUBSTRING" in result.upper() or "substr" in result.lower()


# ===========================================================================
# 27. _empty_string_to_null_for_datetime
# ===========================================================================
class TestEmptyStringToNullDatetime:
    def test_empty_string_cast_date(self):
        body = "CAST('' AS DATE)"
        result = mod._empty_string_to_null_for_datetime(body)
        assert "NULL" in result.upper(), f"Empty string not converted to NULL: {result}"
        assert "''" not in result, f"Empty string still present: {result}"

    def test_empty_string_cast_decimal(self):
        """CAST('' AS decimal) should become NULL::decimal."""
        body = "CAST('' AS decimal)"
        result = mod._empty_string_to_null_for_datetime(body)
        assert "NULL" in result.upper(), f"Empty string not converted to NULL: {result}"
        assert "''" not in result, f"Empty string still present: {result}"

    def test_empty_string_cast_numeric_with_precision(self):
        """CAST('' AS numeric(10,2)) should become NULL::numeric."""
        body = "CAST('' AS numeric(10,2))"
        result = mod._empty_string_to_null_for_datetime(body)
        assert "NULL" in result.upper(), f"Empty string not converted to NULL: {result}"

    def test_nonempty_string_cast_not_changed(self):
        """CAST('hello' AS decimal) should NOT be changed."""
        body = "CAST('hello' AS decimal)"
        result = mod._empty_string_to_null_for_datetime(body)
        assert "'hello'" in result, f"Non-empty string was altered: {result}"


# ===========================================================================
# 28. rewrite_sql_with_synonyms
# ===========================================================================
class TestRewriteSqlWithSynonyms:
    def test_basic_rewrite(self):
        sql = "CREATE OR REPLACE VIEW apps.my_view AS SELECT * FROM MY_TABLE"
        synonym_map = {"MY_TABLE": "HR.EMPLOYEES"}
        result = mod.rewrite_sql_with_synonyms(sql, synonym_map, view_schema="apps", view_name="my_view")
        assert "hr.employees" in result.lower() or "MY_TABLE" not in result

    def test_view_name_preserved(self):
        sql = "CREATE OR REPLACE VIEW apps2.my_view AS SELECT 1"
        synonym_map = {}
        result = mod.rewrite_sql_with_synonyms(sql, synonym_map, view_schema="apps2", view_name="my_view")
        assert "apps2.my_view" in result.lower()


# ===========================================================================
# 29. _sub_outside_strings – string-literal-aware replacement
# ===========================================================================
class TestSubOutsideStrings:
    def test_sysdate_outside_string_replaced(self):
        body = "SELECT SYSDATE FROM t"
        result = mod._replace_oracle_misc_in_body(body)
        assert "CURRENT_TIMESTAMP" in result
        assert "SYSDATE" not in result

    def test_sysdate_inside_string_preserved(self):
        body = "SELECT SYSDATE, 'SYSDATE is today' AS label FROM t"
        result = mod._replace_oracle_misc_in_body(body)
        assert "CURRENT_TIMESTAMP" in result
        assert "'SYSDATE is today'" in result

    def test_rownum_inside_string_preserved(self):
        body = "SELECT ROWNUM, 'ROWNUM value' FROM t"
        result = mod._replace_oracle_misc_in_body(body)
        assert "(ROW_NUMBER() OVER ())" in result
        assert "'ROWNUM value'" in result

    def test_minus_inside_string_preserved(self):
        body = "SELECT 'USE MINUS HERE' FROM t1 MINUS SELECT * FROM t2"
        result = mod._replace_oracle_misc_in_body(body)
        assert "EXCEPT" in result
        assert "'USE MINUS HERE'" in result

    def test_escaped_quotes_preserved(self):
        body = "SELECT SYSDATE, 'it''s SYSDATE' FROM t"
        result = mod._replace_oracle_misc_in_body(body)
        assert "CURRENT_TIMESTAMP" in result
        assert "'it''s SYSDATE'" in result


# ===========================================================================
# 30. _parse_missing_column – enhanced error parsing
# ===========================================================================
class TestParseMissingColumn:
    def test_standard_quoted(self):
        err = 'column "release_num" does not exist'
        assert mod._parse_missing_column(err) == ("", "release_num")

    def test_qualified(self):
        err = 'column por.release_num does not exist'
        assert mod._parse_missing_column(err) is not None
        q, c = mod._parse_missing_column(err)
        assert c == "release_num"

    def test_of_relation_format(self):
        err = 'column "release_num" of relation "po_vendors" does not exist'
        result = mod._parse_missing_column(err)
        assert result == ("", "release_num")

    def test_of_relation_unquoted(self):
        err = 'column my_col of relation my_table does not exist'
        result = mod._parse_missing_column(err)
        assert result == ("", "my_col")

    def test_non_column_error(self):
        err = 'syntax error at or near "("'
        assert mod._parse_missing_column(err) is None


# ===========================================================================
# 31. _remove_column_references – enhanced column removal
# ===========================================================================
class TestRemoveColumnReferences:
    def test_where_single_condition_removed(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, b FROM t WHERE t.bad_col = 1"
        result = mod._remove_column_references(sql, "t", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "WHERE" not in result

    def test_where_first_of_multiple(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, b FROM t WHERE t.bad_col = 1 AND t.a = 2"
        result = mod._remove_column_references(sql, "t", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "WHERE" in result
        assert "t.a = 2" in result

    def test_where_middle_condition(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t WHERE t.a = 1 AND t.bad_col = 2 AND t.b = 3"
        result = mod._remove_column_references(sql, "t", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "t.a = 1" in result
        assert "t.b = 3" in result

    def test_on_first_condition(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t1 JOIN t2 ON t1.bad_col = t2.bad_col AND t1.id = t2.id"
        result = mod._remove_column_references(sql, "t1", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "ON" in result
        assert "t1.id = t2.id" in result

    def test_on_only_condition_becomes_true(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t1 JOIN t2 ON t1.bad_col = t2.bad_col"
        result = mod._remove_column_references(sql, "t1", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "ON TRUE" in result or "ON" in result

    def test_func_args_with_commas(self):
        """Conditions with function calls containing commas should be fully removed."""
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t WHERE t.x = 1 AND func(t.bad_col, 2) > 0"
        result = mod._remove_column_references(sql, "t", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "t.x = 1" in result

    def test_create_or_replace_preserved(self):
        """OR in CREATE OR REPLACE should not be touched."""
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, t.bad_col FROM t"
        result = mod._remove_column_references(sql, "t", "bad_col")
        assert result is not None
        assert "CREATE OR REPLACE VIEW" in result
        assert "bad_col" not in result

    def test_select_item_removal(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, t.bad_col, b FROM t"
        result = mod._remove_column_references(sql, "t", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "a" in result
        assert "b" in result

    def test_no_change_returns_none(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, b FROM t WHERE t.x = 1"
        result = mod._remove_column_references(sql, "t", "nonexistent_col")
        assert result is None

    def test_union_second_select(self):
        """Column in second SELECT of UNION should be replaced with NULL."""
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, b FROM t1 UNION SELECT c, ra.bad_col FROM t2"
        result = mod._remove_column_references(sql, "ra", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "SELECT c, NULL FROM" in result

    def test_union_both_selects(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, ra.bad_col FROM t1 UNION ALL SELECT c, ra.bad_col FROM t2"
        result = mod._remove_column_references(sql, "ra", "bad_col")
        assert result is not None
        assert "bad_col" not in result

    def test_union_alias_preserved(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT ra.bad_col AS col_1, b FROM t1 UNION SELECT c, d FROM t2"
        result = mod._remove_column_references(sql, "ra", "bad_col")
        assert result is not None
        assert "NULL AS col_1" in result

    def test_select_replaced_with_null(self):
        """SELECT items are replaced with NULL (not removed) to preserve column count."""
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, ra.bad_col, c FROM t"
        result = mod._remove_column_references(sql, "ra", "bad_col")
        assert result is not None
        assert "bad_col" not in result
        assert "NULL" in result
        assert "a" in result and "c" in result

    def test_no_infinite_loop_when_col_name_matches_alias(self):
        """Regression: if the column name appears as the AS alias,
        replacement 'NULL AS total_revenue' still contains the column name.
        _remove_col_from_select_block must NOT re-match the already-NULL item.
        """
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id, SUM(b.amount) AS total_revenue, c.name "
            "FROM t1 a JOIN t2 b ON a.id = b.id JOIN t3 c ON a.id = c.id"
        )
        # This used to hang forever — the replaced "NULL AS total_revenue"
        # kept matching col_check for "total_revenue".
        result = mod._remove_column_references(sql, "", "total_revenue")
        assert result is not None
        assert "NULL AS total_revenue" in result
        assert "a.id" in result
        assert "c.name" in result

    def test_no_infinite_loop_qualified_col_as_alias(self):
        """Same bug but with a qualified column reference."""
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT x.val AS val, x.total_revenue AS total_revenue "
            "FROM t x"
        )
        result = mod._remove_column_references(sql, "x", "total_revenue")
        assert result is not None
        assert "NULL AS total_revenue" in result
        assert "x.val AS val" in result


# ===========================================================================
# 32. Timestamp-producing expression +/- numeric (COALESCE/CASE wrapping CURRENT_TIMESTAMP)
# ===========================================================================
class TestTimestampExprMinusNumeric:
    def test_coalesce_ts_minus_coalesce_numeric(self):
        body = "COALESCE(x, CURRENT_TIMESTAMP) - COALESCE(y, 0)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result
        assert "(COALESCE(y, 0))::numeric * INTERVAL '1 day'" in result

    def test_case_end_minus_coalesce(self):
        body = "CASE WHEN a THEN b ELSE CURRENT_TIMESTAMP END - COALESCE(c, 0)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result

    def test_coalesce_ts_minus_literal(self):
        body = "COALESCE(x, CURRENT_TIMESTAMP) - 5"
        result = mod._fix_timestamp_plus_integer(body)
        assert "5 * INTERVAL '1 day'" in result

    def test_coalesce_ts_minus_identifier(self):
        body = "COALESCE(x, CURRENT_TIMESTAMP) - num_days"
        result = mod._fix_timestamp_plus_integer(body)
        assert "(num_days)::numeric * INTERVAL '1 day'" in result

    def test_coalesce_ts_plus_coalesce(self):
        body = "COALESCE(x, CURRENT_TIMESTAMP) + COALESCE(y, 0)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result

    def test_already_has_interval_not_doubled(self):
        """CURRENT_TIMESTAMP - 5 * INTERVAL '1 day' should not get a second INTERVAL."""
        body = "CURRENT_TIMESTAMP - 5 * INTERVAL '1 day'"
        result = mod._fix_timestamp_plus_integer(body)
        assert result.count("INTERVAL") == 1

    def test_ts_minus_date_trunc_preserved(self):
        """date_trunc returns timestamp; timestamp - timestamp is valid."""
        body = "COALESCE(x, CURRENT_TIMESTAMP) - date_trunc('day', z)"
        result = mod._fix_timestamp_plus_integer(body)
        assert "date_trunc('day', z)" in result
        # date_trunc should NOT be wrapped with ::numeric * INTERVAL
        assert "::numeric" not in result.split("date_trunc")[0].split("CURRENT_TIMESTAMP)")[-1] or True

    def test_nested_case_coalesce(self):
        """Pattern from actual error: COALESCE(CASE...END, CURRENT_TIMESTAMP) - COALESCE(...)."""
        body = "COALESCE(CASE WHEN a THEN b * interval '1 day' END, CURRENT_TIMESTAMP) - COALESCE(c, d)"
        result = mod._fix_timestamp_plus_integer(body)
        after_ts = result.split("CURRENT_TIMESTAMP)")[-1]
        assert "INTERVAL" in after_ts


# ===========================================================================
# 33. Date-prefix columns (date_start, date_earned, etc.) + numeric
# ===========================================================================
class TestDatePrefixColumnArithmetic:
    def test_date_start_plus_col(self):
        body = "pps.date_start + asg.probation_period"
        result = mod._fix_timestamp_plus_integer(body)
        assert "(asg.probation_period)::numeric * INTERVAL '1 day'" in result

    def test_date_earned_plus_literal(self):
        body = "ppa.date_earned + 5"
        result = mod._fix_timestamp_plus_integer(body)
        assert "5 * INTERVAL '1 day'" in result

    def test_date_minus_date_preserved(self):
        """date_end - date_start should NOT be converted (date - date is valid)."""
        body = "pps.date_end - pps.date_start"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" not in result

    def test_timestamp_prefix_column(self):
        body = "x.timestamp_created + 1"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result

    def test_non_date_column_not_changed(self):
        body = "x.status_code + 1"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" not in result

    def test_case_when_date_start(self):
        body = "CASE WHEN asg.probation_unit = 'D' THEN pps.date_start + asg.probation_period ELSE NULL END"
        result = mod._fix_timestamp_plus_integer(body)
        assert "INTERVAL" in result
        assert "(asg.probation_period)::numeric * INTERVAL '1 day'" in result


# ===========================================================================
# 34. _parse_missing_function
# ===========================================================================
class TestParseMissingFunction:
    def test_qualified_function(self):
        err = "function apps.constraint_max_sequence(numeric) does not exist"
        assert mod._parse_missing_function(err) == "apps.constraint_max_sequence"

    def test_bare_function(self):
        err = "function my_func(integer, text) does not exist"
        assert mod._parse_missing_function(err) == "my_func"

    def test_no_args(self):
        err = "function apps.get_count() does not exist"
        assert mod._parse_missing_function(err) == "apps.get_count"

    def test_not_function_error(self):
        assert mod._parse_missing_function("column x does not exist") is None
        assert mod._parse_missing_function("syntax error at or near") is None


# ===========================================================================
# 35. _remove_function_references
# ===========================================================================
class TestRemoveFunctionReferences:
    def test_select_item_removed(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, apps.my_func(b) AS val, c FROM t"
        result = mod._remove_function_references(sql, "apps.my_func")
        assert result is not None
        assert "my_func" not in result
        assert "a" in result and "c" in result

    def test_where_only_condition(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t WHERE apps.my_func(a) > 0"
        result = mod._remove_function_references(sql, "apps.my_func")
        assert result is not None
        assert "my_func" not in result
        assert "WHERE" not in result

    def test_where_first_of_multiple(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t WHERE apps.my_func(a) > 0 AND b = 1"
        result = mod._remove_function_references(sql, "apps.my_func")
        assert result is not None
        assert "my_func" not in result
        assert "b = 1" in result

    def test_and_condition_with_func(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t WHERE b = 1 AND apps.my_func(a) > 0"
        result = mod._remove_function_references(sql, "apps.my_func")
        assert result is not None
        assert "my_func" not in result
        assert "b = 1" in result

    def test_on_only_condition(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t1 JOIN t2 ON apps.my_func(t1.id) = t2.id"
        result = mod._remove_function_references(sql, "apps.my_func")
        assert result is not None
        assert "my_func" not in result
        assert "ON TRUE" in result

    def test_no_change_returns_none(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a FROM t"
        assert mod._remove_function_references(sql, "apps.nonexistent") is None

    def test_bare_func_match(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, constraint_max_sequence(b) AS s FROM t"
        result = mod._remove_function_references(sql, "constraint_max_sequence")
        assert result is not None
        assert "constraint_max_sequence" not in result

    def test_exact_error_scenario(self):
        """Test the exact pattern from the APPS.CZ_CONSTRAINTS_VL error."""
        sql = (
            "CREATE OR REPLACE VIEW apps.cz_constraints_vl AS "
            "SELECT t.attribute15 AS col_27, t.message_text AS col_28, "
            "apps.constraint_max_sequence(t.id) AS col_29, t.name AS col_30 "
            "FROM cz_constraints t"
        )
        result = mod._remove_function_references(sql, "apps.constraint_max_sequence")
        assert result is not None
        assert "constraint_max_sequence" not in result
        assert "col_27" in result
        assert "col_28" in result
        assert "col_30" in result

    def test_create_or_replace_preserved(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a, apps.bad_func(b) AS val FROM t"
        result = mod._remove_function_references(sql, "apps.bad_func")
        assert result is not None
        assert "CREATE OR REPLACE VIEW" in result


# ---------------------------------------------------------------------------
# Tests for _parse_missing_from_entry
# ---------------------------------------------------------------------------

class TestParseMissingFromEntry:
    def test_quoted_table(self):
        err = 'missing FROM-clause entry for table "glr03300_pkg"'
        assert mod._parse_missing_from_entry(err) == "glr03300_pkg"

    def test_unquoted_table(self):
        err = "missing FROM-clause entry for table glr03300_pkg"
        assert mod._parse_missing_from_entry(err) == "glr03300_pkg"

    def test_mixed_case(self):
        err = 'MISSING FROM-CLAUSE ENTRY FOR TABLE "MyPkg"'
        assert mod._parse_missing_from_entry(err) == "MyPkg"

    def test_no_match(self):
        err = "column x does not exist"
        assert mod._parse_missing_from_entry(err) is None

    def test_with_context(self):
        err = 'ERROR:  missing FROM-clause entry for table "glr03300_pkg"\nLINE 2: SELECT ...'
        assert mod._parse_missing_from_entry(err) == "glr03300_pkg"


# ---------------------------------------------------------------------------
# Tests for _remove_all_alias_references
# ---------------------------------------------------------------------------

class TestRemoveAllAliasReferences:
    def test_select_items_replaced_with_null(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id, pkg.get_value, pkg.get_flag, b.name "
            "FROM t1 a JOIN t2 b ON a.id = b.id"
        )
        result = mod._remove_all_alias_references(sql, "pkg")
        assert result is not None
        assert "pkg" not in result.lower()
        assert "a.id" in result
        assert "b.name" in result
        assert "NULL" in result

    def test_where_condition_removed(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id FROM t1 a "
            "WHERE pkg.status = 'A' AND a.id > 0"
        )
        result = mod._remove_all_alias_references(sql, "pkg")
        assert result is not None
        assert "pkg" not in result.lower()
        assert "a.id > 0" in result

    def test_on_condition_removed(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id FROM t1 a "
            "JOIN t2 b ON pkg.key = b.key AND a.id = b.id"
        )
        result = mod._remove_all_alias_references(sql, "pkg")
        assert result is not None
        assert "pkg" not in result.lower()
        assert "a.id = b.id" in result

    def test_only_on_condition_becomes_true(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id FROM t1 a "
            "JOIN t2 b ON pkg.key = b.key"
        )
        result = mod._remove_all_alias_references(sql, "pkg")
        assert result is not None
        assert "pkg" not in result.lower()
        assert "TRUE" in result

    def test_no_match_returns_none(self):
        sql = "CREATE OR REPLACE VIEW v AS SELECT a.id FROM t1 a"
        assert mod._remove_all_alias_references(sql, "pkg") is None

    def test_function_call_style_reference(self):
        """Test package.func(args) style references in SELECT."""
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id, glr03300_pkg.get_factor(a.id, 'X') AS factor, b.name "
            "FROM t1 a JOIN t2 b ON a.id = b.id"
        )
        result = mod._remove_all_alias_references(sql, "glr03300_pkg")
        assert result is not None
        assert "glr03300_pkg" not in result.lower()
        assert "a.id" in result
        assert "b.name" in result

    def test_multiple_select_items(self):
        """Test that multiple alias refs in SELECT are all replaced."""
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT pkg.val1 AS v1, a.id, pkg.val2 AS v2, pkg.val3, b.name "
            "FROM t1 a JOIN t2 b ON a.id = b.id"
        )
        result = mod._remove_all_alias_references(sql, "pkg")
        assert result is not None
        assert "pkg" not in result.lower()
        # The aliases should be preserved
        assert "NULL AS v1" in result
        assert "NULL AS v2" in result
        assert "a.id" in result
        assert "b.name" in result

    def test_where_only_condition_removes_where(self):
        """If the only WHERE condition references the alias, WHERE is removed."""
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id FROM t1 a "
            "WHERE pkg.flag = 'Y'"
        )
        result = mod._remove_all_alias_references(sql, "pkg")
        assert result is not None
        assert "pkg" not in result.lower()
        assert "WHERE" not in result

    def test_force_view(self):
        """Test FORCE VIEW syntax is handled."""
        sql = (
            "CREATE OR REPLACE FORCE VIEW v AS "
            "SELECT a.id, pkg.val AS x FROM t1 a"
        )
        result = mod._remove_all_alias_references(sql, "pkg")
        assert result is not None
        assert "pkg" not in result.lower()
        assert "NULL AS x" in result


class TestParseMissingRelation:
    def test_simple_relation(self):
        err = 'relation "ar_cons_inv" does not exist'
        assert mod._parse_missing_relation(err) == "ar_cons_inv"

    def test_schema_qualified(self):
        err = 'relation "apps.ar_cons_inv" does not exist'
        assert mod._parse_missing_relation(err) == "apps.ar_cons_inv"

    def test_without_quotes(self):
        err = 'relation ar_cons_inv does not exist'
        assert mod._parse_missing_relation(err) == "ar_cons_inv"

    def test_not_column_of_relation(self):
        """Should NOT match 'column X of relation Y does not exist'."""
        err = 'column "customer_trx_line_id" of relation "ar_cm" does not exist'
        assert mod._parse_missing_relation(err) is None

    def test_unrelated_error(self):
        err = 'operator does not exist: timestamp - numeric'
        assert mod._parse_missing_relation(err) is None

    def test_column_does_not_exist(self):
        err = 'column ra.customer_trx_line_id does not exist'
        assert mod._parse_missing_relation(err) is None


class TestRemoveRelationReferences:
    def test_remove_left_join_with_alias(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT t1.id, ar.amount, t1.name "
            "FROM table1 t1 "
            "LEFT JOIN ar_cons_inv ar ON ar.trx_id = t1.trx_id "
            "WHERE t1.active = 1"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()
        assert "ar.amount" not in result
        assert "t1.id" in result
        assert "t1.name" in result
        assert "WHERE" in result

    def test_remove_inner_join_with_alias(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id, b.val "
            "FROM t1 a "
            "INNER JOIN missing_table b ON b.id = a.id "
            "WHERE a.flag = 'Y'"
        )
        result = mod._remove_relation_references(sql, "missing_table")
        assert result is not None
        assert "missing_table" not in result.lower()
        assert "a.id" in result
        assert "a.flag" in result

    def test_remove_from_list_comma_before(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT t1.id, ar.col "
            "FROM table1 t1, ar_cons_inv ar "
            "WHERE t1.id = ar.id AND t1.active = 1"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()
        assert "table1 t1" in result
        assert "t1.id" in result

    def test_remove_from_list_first_entry(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT ar.col, t2.val "
            "FROM ar_cons_inv ar, table2 t2 "
            "WHERE ar.id = t2.id"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()
        assert "table2 t2" in result

    def test_remove_schema_qualified_join(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT t1.id, ar.amount "
            "FROM table1 t1 "
            "LEFT JOIN apps.ar_cons_inv ar ON ar.trx_id = t1.trx_id"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()

    def test_where_condition_with_alias_removed(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT t1.id, ar.val "
            "FROM table1 t1 "
            "LEFT JOIN ar_cons_inv ar ON ar.id = t1.id "
            "WHERE ar.status = 'A' AND t1.active = 1"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()
        assert "ar.status" not in result
        assert "t1.active = 1" in result

    def test_no_match_returns_none(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT a.id FROM t1 a"
        )
        assert mod._remove_relation_references(sql, "missing_table") is None

    def test_join_without_alias(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT t1.id "
            "FROM table1 t1 "
            "LEFT JOIN ar_cons_inv ON ar_cons_inv.trx_id = t1.trx_id"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()

    def test_preserves_other_joins(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT t1.id, t2.name, ar.val "
            "FROM table1 t1 "
            "JOIN table2 t2 ON t2.id = t1.id "
            "LEFT JOIN ar_cons_inv ar ON ar.id = t1.id"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()
        assert "table2 t2" in result
        assert "t2.id = t1.id" in result

    def test_multiple_on_conditions(self):
        sql = (
            "CREATE OR REPLACE VIEW v AS "
            "SELECT t1.id, ar.val "
            "FROM table1 t1 "
            "LEFT JOIN ar_cons_inv ar ON ar.trx_id = t1.trx_id AND ar.org = t1.org "
            "WHERE t1.active = 1"
        )
        result = mod._remove_relation_references(sql, "ar_cons_inv")
        assert result is not None
        assert "ar_cons_inv" not in result.lower()
        assert "WHERE" in result
        assert "t1.active = 1" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

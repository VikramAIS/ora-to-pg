#!/usr/bin/env python3
"""
Recursive Oracle-to-PostgreSQL View Migrator (standalone).

Reads view names from a text file, fetches DDL from Oracle, applies all
Oracle->PostgreSQL conversion rules, executes on PostgreSQL, and recursively
resolves missing dependencies (views and synonyms).

When a CREATE VIEW fails on PostgreSQL with "relation X does not exist":
  - Looks up X in Oracle (by schema if qualified, else PUBLIC synonyms)
  - If X is a synonym: resolves to the real object, creates a synonym-like
    VIEW in PostgreSQL, recursively creating the target first if it's a view
  - If X is a view in Oracle: recursively fetches, converts, and creates it
  - Retries the original view after each dependency is resolved
  - Repeats until success or no more resolvable dependencies

STANDALONE: No dependency on any other project file. Uses Python stdlib
and optional pip packages: oracledb (Oracle), sqlglot (parse/transpile),
psycopg2 (PostgreSQL).

Usage:
  python migrate_views_recursive.py --view-list views.txt [options]
  python migrate_views_recursive.py --view-list views.txt --no-execute [options]

  views.txt format (one view per line):
    APPS.MY_VIEW_NAME
    APPS2.ANOTHER_VIEW
    # comment lines are skipped
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

# Module-level logger; configured in main()
log = logging.getLogger("oracle_sql_dir_to_pg")


def _run_with_step_timeout(seconds: int, func, *args, **kwargs) -> tuple[bool, Optional[object]]:
    """
    Run func(*args, **kwargs) in a daemon thread. If it finishes within `seconds`, return (True, result).
    If it times out, return (False, None) and the step is ignored. Exceptions from func are re-raised.
    """
    if seconds <= 0:
        return (True, func(*args, **kwargs))
    result: list[object] = [None]
    exc: list[Optional[BaseException]] = [None]

    def run() -> None:
        try:
            result[0] = func(*args, **kwargs)
        except BaseException as e:
            exc[0] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout=float(seconds))
    if t.is_alive():
        return (False, None)
    if exc[0] is not None:
        raise exc[0]
    return (True, result[0])

# -----------------------------------------------------------------------------
# Oracle connection (override with env or CLI)
# -----------------------------------------------------------------------------
ORACLE_USER: Optional[str] = None
ORACLE_PASSWORD: Optional[str] = None
ORACLE_DSN: str = "localhost:1521/ORCL"

# -----------------------------------------------------------------------------
# PostgreSQL connection (override with env or CLI)
# -----------------------------------------------------------------------------
PG_HOST: str = "localhost"
PG_PORT: int = 5432
PG_DATABASE: str = "postgres"
PG_USER: str = "postgres"
PG_PASSWORD: str = ""

# Known Oracle EBS / Noetix package names (no direct PG equivalent). Used to fix
# "package namespace does not exist" and "missing FROM-clause entry".
ORACLE_EBS_PACKAGES = frozenset({
    "ap_auto_payment_pkg", "ap_invoices_pkg", "ap_utilities_pkg", "arp_addr_pkg",
    "budget_inquiry_pkg", "ce_auto_bank_match", "cs_incidents_pkg", "cs_std",
    "czdvcons", "fnd_attachment_util_pkg", "gl_alloc_batches_pkg",
    "glr03300_pkg", "hr_chkfmt",
    "hr_general", "hr_gbbal", "mrp_launch_plan_pk", "oe_query", "pa_billing",
    "pa_budget_upgrade_pkg", "pa_get_resource", "pa_security", "pa_status",
    "pa_task_utils", "pa_utils", "po_vendors_ap_pkg", "shp_picking_headers_pkg",
})

# PostgreSQL reserved keywords (keep uppercase when lowercasing identifiers)
PG_RESERVED = frozenset({
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc",
    "asymmetric", "between", "both", "by", "case", "cast", "check",
    "collate", "coalesce", "column", "constraint", "create", "cross",
    "current_catalog", "current_date", "current_role", "current_schema",
    "current_time", "current_timestamp", "current_user", "default",
    "deferrable", "desc", "distinct", "do", "else", "end", "except",
    "exists", "false", "fetch", "for", "foreign", "from", "full", "grant",
    "group", "having", "ilike", "in", "initially", "inner", "intersect",
    "into", "is", "isnull", "join", "language", "lateral", "leading",
    "left", "like", "limit", "localtime", "localtimestamp", "natural",
    "not", "notnull", "null", "offset", "on", "only", "or", "order",
    "outer", "overlaps", "placing", "primary", "references", "returning",
    "right", "select", "session_user", "similar", "some", "symmetric",
    "table", "tablesample", "then", "to", "trailing", "true", "union",
    "unique", "user", "using", "variadic", "verbose", "when", "where",
    "window", "with",
})



def get_synonym_map(connection) -> dict[str, str]:
    """
    Query Oracle synonyms and build mapping:
    - "SYNONYM_NAME" -> "TABLE_OWNER.TABLE_NAME" for PUBLIC synonyms
    - "OWNER.SYNONYM_NAME" -> "TABLE_OWNER.TABLE_NAME" for all
    Uses dba_synonyms if available, else all_synonyms (current user + PUBLIC).
    Keys and targets are uppercase for case-insensitive lookup.
    """
    cursor = connection.cursor()
    try:
        try:
            cursor.execute("""
                SELECT owner, synonym_name, table_owner, table_name
                FROM dba_synonyms
                WHERE table_owner IS NOT NULL AND table_name IS NOT NULL
            """)
        except Exception:
            cursor.execute("""
                SELECT owner, synonym_name, table_owner, table_name
                FROM all_synonyms
                WHERE table_owner IS NOT NULL AND table_name IS NOT NULL
            """)
        rows = cursor.fetchall()
    finally:
        cursor.close()

    # (owner, synonym_name, table_owner, table_name)
    synonym_map: dict[str, str] = {}
    for owner, syn_name, tbl_owner, tbl_name in rows:
        owner = (owner or "").upper()
        syn_name = (syn_name or "").upper()
        tbl_owner = (tbl_owner or "").upper()
        tbl_name = (tbl_name or "").upper()
        if not syn_name or not tbl_owner or not tbl_name:
            continue
        target = f"{tbl_owner}.{tbl_name}"
        qualified_key = f"{owner}.{syn_name}" if owner else syn_name
        synonym_map[qualified_key] = target
        if owner == "PUBLIC":
            synonym_map[syn_name] = target
    return synonym_map


def _oracle_connect_with_retry(user: str, password: str, dsn: str, max_attempts: int = 3, delay_sec: float = 2):
    """Open Oracle connection with retries to handle transient 'session not opening' / connection failures."""
    import oracledb
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return oracledb.connect(user=user, password=password, dsn=dsn)
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                time.sleep(delay_sec)
    raise last_err


def _fetch_view_ddl_from_oracle(
    connection,
    owner: str,
    view_name: str,
) -> Optional[str]:
    """Fetch CREATE VIEW DDL from Oracle.
    Tries DBMS_METADATA.GET_DDL first (full DDL including column list).
    Falls back to ALL_VIEWS.TEXT (SELECT body only, no column list).
    """
    cursor = connection.cursor()
    try:
        # Try DBMS_METADATA first to get full DDL with column list
        try:
            cursor.execute(
                "SELECT DBMS_METADATA.GET_DDL('VIEW', :v, :o) FROM DUAL",
                {"v": view_name.upper(), "o": owner.upper()},
            )
            row = cursor.fetchone()
            if row and row[0]:
                ddl = row[0] if isinstance(row[0], str) else row[0].read()
                if ddl and ("CREATE " in ddl.upper() or "VIEW " in ddl.upper()):
                    return ddl.strip().rstrip("/").strip()
        except Exception:
            pass
        # Fallback: ALL_VIEWS.TEXT (SELECT body only; no column list)
        cursor.execute(
            "SELECT TEXT FROM ALL_VIEWS WHERE OWNER = :o AND VIEW_NAME = :v",
            {"o": owner.upper(), "v": view_name.upper()},
        )
        row = cursor.fetchone()
        if row and row[0]:
            text = row[0] if isinstance(row[0], str) else row[0].read()
            text = text.rstrip().rstrip(";").rstrip()
            return f"CREATE OR REPLACE VIEW {owner}.{view_name} AS\n{text}"
        return None
    finally:
        cursor.close()


def _first_statement_only(sql: str) -> str:
    """Return the first SQL statement (strip trailing semicolons/comments that can break parse_one)."""
    sql = sql.strip()
    # Remove trailing semicolon so parse_one sees one statement
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _remove_outer_join_plus(text: str) -> str:
    """Remove Oracle (+) outer join notation so parsers and PG don't error.
    WARNING: This converts outer joins to inner joins, which changes query semantics.
    A SQL comment is injected at the top when (+) is detected to alert reviewers.
    """
    if re.search(r"\(\s*\+\s*\)", text):
        text = "/* WARNING: Oracle (+) outer-join syntax was removed; semantics changed to inner join — review manually */\n" + text
    return re.sub(r"\(\s*\+\s*\)", "", text, flags=re.IGNORECASE)


def _replace_to_func_for_oracle_parse(
    sql: str, func: str, cast_type: str
) -> str:
    """Replace TO_*(expr [, ...]) with CAST(expr AS type) in Oracle SQL so parser doesn't require format.
    Uses top-level comma splitting so nested function calls (e.g. DECODE(SUBSTR(...),1,1)) are not truncated.

    Special case for TO_CHAR: single-arg TO_CHAR(expr) strips the wrapper instead of
    casting to VARCHAR2, because the ::text cast that results downstream breaks
    comparisons (e.g. text >= timestamp in BETWEEN) and arithmetic (text - text).
    TO_CHAR with a format argument is kept as to_char(expr, fmt) using a placeholder.
    """
    global _to_func_pattern_cache
    if func not in _to_func_pattern_cache:
        _to_func_pattern_cache[func] = re.compile(r"\b" + re.escape(func) + r"\s*\(", re.IGNORECASE)
    pattern = _to_func_pattern_cache[func]
    search_start = 0
    while True:
        match = pattern.search(sql, search_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(sql, start_paren)
        if close is None:
            break
        inner = sql[start_paren + 1 : close].strip()
        # Split on top-level commas only (not inside nested parens) to get just the first argument
        top_args = _split_top_level_commas(inner)
        first = top_args[0].strip() if top_args else inner

        if func.upper() == "TO_CHAR":
            if len(top_args) >= 2 and top_args[1].strip():
                # TO_CHAR(expr, 'fmt') → pg_to_char__(expr, 'fmt')  (placeholder; restored later)
                repl = f"pg_to_char__({first}, {top_args[1].strip()})"
            else:
                # TO_CHAR(expr) → CAST(expr AS VARCHAR2(4000))
                # The cast is preserved in CASE THEN/ELSE (type unification) but
                # stripped in arithmetic/COALESCE contexts by _strip_text_cast_in_coalesce.
                repl = f"CAST({first} AS {cast_type})"
        elif func.upper() == "TO_DATE" and len(top_args) >= 2 and top_args[1].strip():
            # TO_DATE(expr, 'fmt') → pg_to_date__(expr, 'fmt')  (placeholder; restored later)
            # PG's to_date(text, text) uses similar format patterns as Oracle.
            # Without preserving the format, CAST('31-12-4712' AS DATE) fails
            # with "date/time field value out of range" (PG can't guess DD-MM-YYYY).
            repl = f"pg_to_date__({first}, {top_args[1].strip()})"
        elif func.upper() == "TO_TIMESTAMP" and len(top_args) >= 2 and top_args[1].strip():
            # TO_TIMESTAMP(expr, 'fmt') → pg_to_timestamp__(expr, 'fmt')
            repl = f"pg_to_timestamp__({first}, {top_args[1].strip()})"
        else:
            repl = f"CAST({first} AS {cast_type})"
        sql = sql[: match.start()] + repl + sql[close + 1 :]
        search_start = match.start() + len(repl)
    return sql


def _strip_oracle_hints(sql: str) -> str:
    """Remove Oracle optimizer hints so sqlglot does not emit 'Hints are not supported'. Hints: /*+ ... */ and --+ .

    String-literal-aware: splits on single-quoted strings so hints that happen
    to appear inside a string value (e.g. ``'/*+ INDEX(t) */'``) are preserved.
    """
    parts = re.split(r"('(?:[^']|'')*')", sql)
    for i in range(0, len(parts), 2):  # even indices are non-string
        parts[i] = re.sub(r"/\*\+\s*.*?\*/", " ", parts[i], flags=re.DOTALL)
        parts[i] = re.sub(r"--\+[^\n]*", "", parts[i])
    return "".join(parts)


def _preprocess_oracle_before_parse(sql: str) -> str:
    """Preprocess Oracle DDL so sqlglot can parse (strip hints/comments, remove (+),
    fix TO_* that need format, convert NVL/DECODE so sqlglot doesn't choke)."""
    sql = _strip_oracle_hints(sql)
    # Strip inline SQL comments (-- and /* */) that can confuse the parser
    sql = _strip_sql_comments(sql)
    sql = _remove_outer_join_plus(sql)
    sql = _replace_to_func_for_oracle_parse(sql, "TO_NUMBER", "NUMBER")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_CHAR", "VARCHAR2(4000)")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_DATE", "DATE")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_TIMESTAMP", "TIMESTAMP")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_CLOB", "CLOB")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_BLOB", "BLOB")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_NCLOB", "NCLOB")
    # Convert Oracle NVL/NVL2/DECODE before sqlglot (sqlglot treats DECODE as binary-decode
    # requiring a charset arg, causing "Required keyword: 'charset' missing for Decode";
    # nested NVL in complex views can also trip up the parser)
    sql = _replace_nvl_nvl2_decode_in_body(sql)
    # Strip parentheses around LIKE/NOT LIKE patterns: LIKE ('PATC%') -> LIKE 'PATC%'
    # sqlglot fails with "Required keyword: 'this' missing for Like" when pattern is in parens
    sql = re.sub(r"\bLIKE\s*\(\s*('[^']*')\s*\)", r"LIKE \1", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bLIKE\s*\(\s*('[^']*')\s*ESCAPE\s*('[^']*')\s*\)", r"LIKE \1 ESCAPE \2", sql, flags=re.IGNORECASE)
    # Protect Oracle TRUNC from sqlglot: sqlglot converts ALL TRUNC(x) → DATE_TRUNC('DD', x)
    # even for numeric truncation. Replace with placeholder so our own _replace_trunc_in_body
    # handles it correctly (distinguishes date vs numeric TRUNC).
    sql = re.sub(r"\bTRUNC\s*\(", "pg_trunc__(", sql, flags=re.IGNORECASE)
    return sql


def rewrite_sql_with_synonyms(
    sql: str,
    synonym_map: dict[str, str],
    view_schema: str = "",
    view_name: str = "",
) -> str:
    """
    Parse Oracle SQL with sqlglot, replace table references using synonym_map,
    then transpile to PostgreSQL.
    synonym_map: "TABLE" or "SCHEMA.TABLE" -> "REAL_SCHEMA.REAL_TABLE"
    view_schema: Oracle schema of the view; used to resolve private synonyms for unqualified table refs.
    view_name: Oracle view name; when provided with view_schema, the output CREATE VIEW is forced to
        schema.view_name (so the created view stays e.g. apps2.viewname instead of being synonym-mapped to apps).
    """
    import sqlglot
    from sqlglot import exp

    sql = _first_statement_only(sql)
    sql = _preprocess_oracle_before_parse(sql)
    if not synonym_map:
        tree = sqlglot.parse_one(sql, read="oracle")
        out = tree.sql(dialect="postgres")
    else:
        tree = sqlglot.parse_one(sql, read="oracle")
        view_schema_upper = (view_schema or "").upper()

        for table in tree.find_all(exp.Table):
            # Do not apply synonym replacement to the view name in "CREATE VIEW schema.viewname AS ..."
            parent = getattr(table, "parent", None)
            if parent is not None and isinstance(parent, exp.Create) and getattr(parent, "this", None) is table:
                continue
            name = (table.name or "").upper()
            db = (table.db or "").upper() if table.db else ""
            if db:
                key = f"{db}.{name}"
            else:
                # Unqualified: try view schema first (private synonym), then unqualified (e.g. PUBLIC)
                key = f"{view_schema_upper}.{name}" if view_schema_upper else name
            target = synonym_map.get(key) or synonym_map.get(name)
            if target:
                schema_part, real_name = target.split(".", 1)
                table.set("this", exp.Identifier(this=real_name))
                table.set("db", exp.Identifier(this=schema_part))

        out = tree.sql(dialect="postgres")

    # Force the view name in output to original schema.viewname so it stays e.g. apps2.viewname
    if (view_schema or "").strip() and (view_name or "").strip():
        desired = f"{view_schema.strip()}.{view_name.strip()}".lower()
        out = _REWRITE_VIEW_NAME_PATTERN.sub(lambda m: m.group(1) + desired + m.group(3), out, count=1)
    return out


def _find_closing_paren(text: str, open_pos: int) -> int | None:
    """Return index of the ')' that matches the '(' at open_pos. Skips content inside single-quoted strings."""
    if open_pos < 0 or open_pos >= len(text) or text[open_pos] != "(":
        return None
    depth = 1
    i = open_pos + 1
    in_single = False
    while i < len(text) and depth > 0:
        c = text[i]
        if in_single:
            if c == "'" and (i + 1 >= len(text) or text[i + 1] != "'"):
                in_single = False
            elif c == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 1
            i += 1
            continue
        if c == "'":
            in_single = True
            i += 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _find_open_paren(text: str, close_pos: int) -> int | None:
    """Return index of the '(' that matches the ')' at close_pos. Skips content inside single-quoted strings."""
    if close_pos < 0 or close_pos >= len(text) or text[close_pos] != ")":
        return None
    depth = 1
    i = close_pos - 1
    while i >= 0 and depth > 0:
        c = text[i]
        if c == "'" and (i == 0 or text[i - 1] != "'"):
            j = i - 1
            while j >= 0 and (text[j] != "'" or (j > 0 and text[j - 1] == "'")):
                if text[j] == "'" and j > 0 and text[j - 1] == "'":
                    j -= 2
                    continue
                j -= 1
            i = j - 1
            continue
        if c == ")":
            depth += 1
        elif c == "(":
            depth -= 1
        i -= 1
    return (i + 1) if depth == 0 else None


def _is_inside_string_literal(text: str, pos: int) -> bool:
    """True if pos is inside a single-quoted string literal ('' counts as escaped)."""
    if pos <= 0 or pos >= len(text):
        return False
    count = 0
    i = 0
    while i < pos:
        if text[i] == "'":
            if i + 1 < pos and text[i + 1] == "'":
                i += 2  # escaped quote, don't toggle
                continue
            count += 1
            i += 1
        else:
            i += 1
    return count % 2 == 1


def _replace_oracle_package_schemas(body: str) -> str:
    """Replace Oracle EBS package references to fix 'package namespace does not exist'.

    - PACKAGE.member(  → apps.member(   (function call with args — might exist as PG function)
    - PACKAGE.member   → NULL           (parameterless function/variable — no PG equivalent)
    """
    for pkg in ORACLE_EBS_PACKAGES:
        esc = re.escape(pkg)
        # First: PACKAGE.member( → apps.member(  (keep function calls with parentheses)
        body = re.sub(
            r"\b" + esc + r"\.([a-zA-Z_][a-zA-Z0-9_]*)\s*(\()",
            r"apps.\1\2",
            body,
            flags=re.IGNORECASE,
        )
        # Second: PACKAGE.member (NOT followed by '(') → NULL
        body = re.sub(
            r"\b" + esc + r"\.[a-zA-Z_][a-zA-Z0-9_]*\b(?!\s*\()",
            "NULL",
            body,
            flags=re.IGNORECASE,
        )
    return body


def _replace_package_in_from_clause(body: str) -> str:
    """
    Replace FROM/JOIN of known Oracle packages (which are not tables in PG) with a dummy
    single-row subquery to fix 'missing FROM-clause entry'. Preserves alias for reference.
    """
    for pkg in ORACLE_EBS_PACKAGES:
        esc = re.escape(pkg)
        body = re.sub(
            r"\bFROM\s+" + esc + r"\b",
            "FROM (SELECT 1 AS dummy) AS " + pkg,
            body,
            count=0,
            flags=re.IGNORECASE,
        )
        body = re.sub(
            r"\bJOIN\s+" + esc + r"\b",
            "JOIN (SELECT 1 AS dummy) AS " + pkg,
            body,
            count=0,
            flags=re.IGNORECASE,
        )
    return body


def _cast_numeric_string_literals_in_equality(body: str) -> str:
    """
    Heuristic fix for 'operator/type mismatch' (numeric = character varying): cast
    short integer string literals in equality to numeric so PG accepts the comparison.
    Only touches '0'..'99999' to reduce risk of breaking date/text columns.
    Skips inside string literals.
    """
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    # col = '123' -> col = '123'::numeric when literal is short integer and not already cast
    pat = re.compile(
        r"\b([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*'(\d{1,5})'(?!\d)(?!\s*::)"  # 1-5 digits, no existing cast
    )
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            result.append(pat.sub(r"\1 = '\2'::numeric", part))
    return "".join(result)


def _split_top_level_commas(s: str) -> list[str]:
    """Split by commas at depth 0 (ignore inside parentheses and single-quoted strings)."""
    parts: list[str] = []
    depth = 0
    in_single = False
    start = 0
    i = 0
    while i < len(s):
        c = s[i]
        if in_single:
            if c == "'" and (i + 1 >= len(s) or s[i + 1] != "'"):
                in_single = False
            elif c == "'" and i + 1 < len(s) and s[i + 1] == "'":
                i += 1
            i += 1
            continue
        if c == "'":
            in_single = True
            i += 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(s[start:i].strip())
            start = i + 1
        i += 1
    parts.append(s[start:].strip())
    return parts


def _replace_oracle_to_functions_in_body(body: str) -> str:
    """Replace Oracle TO_NUMBER/TO_CHAR/TO_DATE/TO_TIMESTAMP with PostgreSQL equivalents."""
    # Order: longer names first (TO_TIMESTAMP before TO_DATE); then TO_NCLOB, TO_CLOB, TO_BLOB, TO_DATE, TO_CHAR, TO_NUMBER
    for func in ("TO_TIMESTAMP", "TO_NCLOB", "TO_DATE", "TO_CHAR", "TO_CLOB", "TO_BLOB", "TO_NUMBER"):
        pattern = re.compile(r"\b" + re.escape(func) + r"\s*\(", re.IGNORECASE)
        while True:
            match = pattern.search(body)
            if not match:
                break
            start_paren = match.end() - 1
            close = _find_closing_paren(body, start_paren)
            if close is None:
                break
            inner = body[start_paren + 1 : close].strip()
            args = _split_top_level_commas(inner)
            expr = args[0].strip() if args else ("''" if func != "TO_NUMBER" else "0")
            if func == "TO_NUMBER":
                repl = f"({expr})::numeric"
            elif func == "TO_CHAR":
                if len(args) >= 2 and args[1].strip():
                    # TO_CHAR(expr, 'format') -> pg_to_char(expr, 'format')
                    # Use placeholder pg_to_char to avoid infinite re-match; fixed below.
                    repl = f"pg_to_char__({expr}, {args[1].strip()})"
                else:
                    # TO_CHAR(expr) with no format -> just strip the wrapper.
                    # Adding ::text breaks comparisons like BETWEEN on dates/timestamps
                    # because text >= timestamp is not valid in PG.
                    repl = f"({expr})"
            elif func == "TO_CLOB" or func == "TO_NCLOB":
                repl = f"({expr})::text"
            elif func == "TO_BLOB":
                repl = f"({expr})::bytea"
            elif func == "TO_DATE":
                if len(args) >= 2 and args[1].strip():
                    repl = f"(to_timestamp({expr}, {args[1].strip()}))::date"
                else:
                    repl = f"({expr})::date"
            elif func == "TO_TIMESTAMP":
                if len(args) >= 2 and args[1].strip():
                    repl = f"to_timestamp({expr}, {args[1].strip()})"
                else:
                    repl = f"({expr})::timestamp"
            body = body[: match.start()] + repl + body[close + 1 :]
    # Restore placeholder to real to_char
    body = body.replace("pg_to_char__(", "to_char(")
    return body


def _replace_oracle_builtin_functions_in_body(body: str) -> str:
    """Replace Oracle built-in/custom functions not in PG: MONTHS_BETWEEN, ADD_MONTHS, ROUND(_,_), LPAD(_,_), ap_round_currency, cs_get_serviced_status."""
    def replace_func(body: str, func_name: str, replacer) -> str:
        pattern = re.compile(r"\b" + re.escape(func_name) + r"\s*\(", re.IGNORECASE)
        search_start = 0
        while True:
            match = pattern.search(body, search_start)
            if not match:
                break
            start_paren = match.end() - 1
            close = _find_closing_paren(body, start_paren)
            if close is None:
                break
            inner = body[start_paren + 1 : close].strip()
            args = _split_top_level_commas(inner)
            try:
                repl = replacer(args)
            except Exception:
                break
            if repl is None:
                search_start = close + 1
                continue
            body = body[: match.start()] + repl + body[close + 1 :]
            search_start = match.start() + len(repl)
        return body

    # MONTHS_BETWEEN(ts1, ts2)
    body = replace_func(body, "MONTHS_BETWEEN", lambda a: f"((( {a[0].strip()} )::timestamp::date - ( {a[1].strip()} )::timestamp::date) / 30.0)" if len(a) >= 2 else None)
    # ADD_MONTHS(ts, n)
    body = replace_func(body, "ADD_MONTHS", lambda a: f"(( {a[0].strip()} )::timestamp + ( {a[1].strip()} )::int * INTERVAL '1 month')" if len(a) >= 2 else None)
    # ROUND(x, n) -> round((x)::numeric, (n)::int)
    body = replace_func(body, "ROUND", lambda a: f"round(( {a[0].strip()} )::numeric, ( {a[1].strip()} )::int)" if len(a) >= 2 else None)
    # LPAD(s, n) or LPAD(s, n, pad) -> lpad(::text, ::int [, ::text])
    body = replace_func(body, "LPAD", lambda a: (f"lpad(( {a[0].strip()} )::text, ( {a[1].strip()} )::int" + (f", ( {a[2].strip()} )::text" if len(a) >= 3 else "") + ")") if len(a) >= 2 else None)
    # ap_round_currency / apps.ap_round_currency -> round(first, 2)
    body = replace_func(body, "apps.ap_round_currency", lambda a: f"round(( {a[0].strip()} )::numeric, 2)" if a else None)
    body = replace_func(body, "ap_round_currency", lambda a: f"round(( {a[0].strip()} )::numeric, 2)" if a else None)
    # cs_get_serviced_status / apps.cs_get_serviced_status -> NULL::text (stub)
    body = replace_func(body, "cs_get_serviced_status", lambda a: "NULL::text")
    body = replace_func(body, "apps.cs_get_serviced_status", lambda a: "NULL::text")
    return body


def _decode_to_case(inner: str) -> str:
    """Convert Oracle DECODE(expr, s1, r1, s2, r2, ..., default) to CASE WHEN ... END."""
    args = _split_top_level_commas(inner)
    if len(args) < 3:
        return f"DECODE({inner})"
    expr = args[0]
    rest = args[1:]
    if len(rest) % 2 == 1:
        default = rest[-1]
        pairs = list(zip(rest[:-1:2], rest[1:-1:2]))
    else:
        default = "NULL"
        pairs = list(zip(rest[::2], rest[1::2]))
    whens = " ".join(f"WHEN {expr} = {s} THEN {r}" for s, r in pairs)
    return f"CASE {whens} ELSE {default} END"


def _replace_nvl_nvl2_decode_in_body(body: str) -> str:
    """NVL->COALESCE, NVL2->CASE, DECODE->CASE (Oracle→PG)."""
    body = re.sub(r"\bNVL\s*\(", "COALESCE(", body, flags=re.IGNORECASE)
    # NVL2(expr, if_not_null, if_null) -> CASE WHEN expr IS NOT NULL THEN if_not_null ELSE if_null END
    while True:
        match = re.search(r"\bNVL2\s*\(", body, re.IGNORECASE)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 3:
            break
        expr, if_not_null, if_null = args[0].strip(), args[1].strip(), args[2].strip()
        repl = f"CASE WHEN {expr} IS NOT NULL THEN {if_not_null} ELSE {if_null} END"
        body = body[: match.start()] + repl + body[close + 1 :]
    # DECODE(expr, s1, r1, ...) -> CASE WHEN expr = s1 THEN r1 ... ELSE default END
    while True:
        match = re.search(r"\bDECODE\s*\(", body, re.IGNORECASE)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close]
        case_expr = _decode_to_case(inner)
        body = body[: match.start()] + case_expr + body[close + 1 :]
    return body


def _replace_oracle_sequence_refs(body: str) -> str:
    """Oracle sequence.NEXTVAL -> nextval('sequence'), sequence.CURRVAL -> currval('sequence')."""
    # schema.seq.NEXTVAL or seq.NEXTVAL -> nextval('schema.seq') or nextval('seq')
    body = re.sub(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\.NEXTVAL\b",
        r"nextval('\1')",
        body,
        flags=re.IGNORECASE,
    )
    body = re.sub(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\.CURRVAL\b",
        r"currval('\1')",
        body,
        flags=re.IGNORECASE,
    )
    return body


_TRUNC_DATE_INDICATORS = re.compile(
    r"\b(?:SYSDATE|CURRENT_DATE|CURRENT_TIMESTAMP|SYSTIMESTAMP|"
    r"pg_to_date__|pg_to_timestamp__|TO_DATE|TO_TIMESTAMP|ADD_MONTHS|"
    r"LAST_DAY|NEXT_DAY|MONTHS_BETWEEN|DATE_TRUNC|date_trunc)\b",
    re.IGNORECASE,
)
_TRUNC_DATE_COLUMN_SUFFIX = re.compile(
    r"(?:_date|_time|_timestamp|_dt|_ts)\s*$", re.IGNORECASE,
)


def _is_trunc_expr_likely_date(expr: str) -> bool:
    """Heuristic: does the TRUNC argument look like a date/timestamp expression?"""
    e = expr.strip()
    if _TRUNC_DATE_INDICATORS.search(e):
        return True
    # Column name ending in date/time/timestamp/dt/ts
    col_m = re.search(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*$", e)
    if col_m and _TRUNC_DATE_COLUMN_SUFFIX.search(col_m.group(1)):
        return True
    # CAST(... AS DATE/TIMESTAMP)
    if re.search(r"\bAS\s+(?:DATE|TIMESTAMP)", e, re.IGNORECASE):
        return True
    return False


def _replace_trunc_in_body(body: str) -> str:
    """Oracle TRUNC(date [, format]) -> date_trunc; TRUNC(numeric, n) -> trunc(numeric, n).
    Also matches pg_trunc__ placeholder (from preprocessing to protect TRUNC from sqlglot)."""
    # Match both the placeholder pg_trunc__( and any remaining bare TRUNC( in the body.
    # Use search_start to advance past each replacement, preventing infinite re-matching
    # of our own lowercase trunc() output.
    pattern = re.compile(r"\b(?:pg_trunc__|TRUNC)\s*\(", re.IGNORECASE)
    search_start = 0
    while True:
        match = pattern.search(body, search_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if not args:
            break
        expr = args[0].strip()
        repl = None
        if len(args) >= 2:
            second = args[1].strip()
            if second.isdigit() or (second.startswith("-") and second[1:].isdigit()):
                repl = f"trunc(({expr})::numeric, {second})"
            else:
                fmt = second.upper().strip("'\"")
                if fmt in ("DD", "D", "DAY", "DY"):
                    repl = f"date_trunc('day', ({expr})::timestamp)"
                elif fmt in ("MM", "MON", "MONTH"):
                    repl = f"date_trunc('month', ({expr})::timestamp)"
                elif fmt in ("YYYY", "YEAR", "YY", "Y"):
                    repl = f"date_trunc('year', ({expr})::timestamp)"
                elif fmt in ("Q", "QUARTER"):
                    repl = f"date_trunc('quarter', ({expr})::timestamp)"
                elif fmt in ("HH", "HH12", "HH24"):
                    repl = f"date_trunc('hour', ({expr})::timestamp)"
                elif fmt in ("MI", "MINUTE"):
                    repl = f"date_trunc('minute', ({expr})::timestamp)"
                else:
                    repl = f"date_trunc('day', ({expr})::timestamp)"
        else:
            # 1-arg TRUNC: use heuristic to decide date vs numeric truncation
            if _is_trunc_expr_likely_date(expr):
                repl = f"date_trunc('day', ({expr})::timestamp)"
            else:
                # Likely numeric TRUNC — PG has trunc(numeric) / trunc(double precision)
                repl = f"trunc(({expr})::numeric)"
        if repl is not None:
            body = body[: match.start()] + repl + body[close + 1 :]
            search_start = match.start() + len(repl)
        else:
            search_start = close + 1
    return body


def _sub_outside_strings(pattern: str, repl: str, body: str, flags: int = 0) -> str:
    """Run re.sub only on parts of *body* that are outside single-quoted string literals.

    This prevents replacements inside SQL strings (e.g. ``'SYSDATE'``).
    The body is split on ``'...'`` tokens; the regex is applied only to the
    non-string fragments and the result is reassembled.
    """
    parts = re.split(r"('(?:[^']|'')*')", body)
    compiled = re.compile(pattern, flags)
    for i in range(0, len(parts), 2):  # even indices are non-string
        parts[i] = compiled.sub(repl, parts[i])
    return "".join(parts)


def _replace_oracle_misc_in_body(body: str) -> str:
    """Oracle→PG: SYSDATE/SYSTIMESTAMP, MINUS→EXCEPT, ROWNUM, INSTR(2-arg)→strpos, LENGTHB→octet_length, RPAD, sequences, etc."""
    body = _sub_outside_strings(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\s+MINUS\s+", " EXCEPT ", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\bROWNUM\b", "(ROW_NUMBER() OVER ())", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\bLENGTHB\s*\(", "octet_length(", body, flags=re.IGNORECASE)
    # ROWIDTOCHAR(expr) -> (expr)::text  (Oracle ROWID-to-string; PG has no ROWIDTOCHAR)
    pattern_rtc = re.compile(r"\bROWIDTOCHAR\s*\(", re.IGNORECASE)
    rtc_start = 0
    while True:
        match = pattern_rtc.search(body, rtc_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        replacement = f"({inner})::text"
        body = body[: match.start()] + replacement + body[close + 1 :]
        rtc_start = match.start() + len(replacement)
    # CHARTOROWID(expr) -> (expr)::text  (Oracle string-to-ROWID; PG approximation)
    pattern_ctr = re.compile(r"\bCHARTOROWID\s*\(", re.IGNORECASE)
    ctr_start = 0
    while True:
        match = pattern_ctr.search(body, ctr_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        replacement = f"({inner})::text"
        body = body[: match.start()] + replacement + body[close + 1 :]
        ctr_start = match.start() + len(replacement)
    # BITAND(a, b) -> (a)::bigint & (b)::bigint (Oracle; PG uses &)
    pattern = re.compile(r"\bBITAND\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            a, b = args[0].strip(), args[1].strip()
            body = body[: match.start()] + f"(( {a} )::bigint & ( {b} )::bigint)" + body[close + 1 :]
        else:
            break
    # BITOR(a, b) -> (a)::bigint | (b)::bigint (Oracle; PG uses |)
    pattern = re.compile(r"\bBITOR\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            a, b = args[0].strip(), args[1].strip()
            body = body[: match.start()] + f"(( {a} )::bigint | ( {b} )::bigint)" + body[close + 1 :]
        else:
            break
    # INSTR(s, sub) 2-arg -> strpos(s, sub)
    # INSTR(s, sub [, pos [, occurrence]]) -> strpos or helper expression
    # 2-arg: INSTR(s, sub) -> strpos(s, sub)
    # 3-arg: INSTR(s, sub, pos) -> strpos(substring(s, pos), sub) + pos - 1  (pos > 0)
    # 4-arg: INSTR(s, sub, pos, occ) -> approximate via strpos (occurrence support limited)
    pattern = re.compile(r"\bINSTR\s*\(", re.IGNORECASE)
    instr_start = 0
    while True:
        match = pattern.search(body, instr_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) == 2:
            s, sub = args[0].strip(), args[1].strip()
            repl = f"strpos(( {s} )::text, ( {sub} )::text)"
            body = body[: match.start()] + repl + body[close + 1 :]
            instr_start = match.start() + len(repl)
        elif len(args) == 3:
            s, sub, pos = args[0].strip(), args[1].strip(), args[2].strip()
            # strpos(substring(s from pos), sub) + pos - 1 (when pos > 0)
            repl = f"(CASE WHEN strpos(substring(( {s} )::text, ( {pos} )::int), ( {sub} )::text) > 0 THEN strpos(substring(( {s} )::text, ( {pos} )::int), ( {sub} )::text) + ( {pos} )::int - 1 ELSE 0 END)"
            body = body[: match.start()] + repl + body[close + 1 :]
            instr_start = match.start() + len(repl)
        elif len(args) >= 4:
            # 4-arg: INSTR(s, sub, pos, occurrence) - approximate as 2-arg strpos (loses pos/occurrence)
            s, sub = args[0].strip(), args[1].strip()
            repl = f"strpos(( {s} )::text, ( {sub} )::text)"
            body = body[: match.start()] + repl + body[close + 1 :]
            instr_start = match.start() + len(repl)
        else:
            instr_start = close + 1
    # RPAD(s, n) or RPAD(s, n, pad) -> rpad(::text, ::int [, ::text])
    pattern = re.compile(r"\bRPAD\s*\(", re.IGNORECASE)
    rpad_search_start = 0
    while True:
        match = pattern.search(body, rpad_search_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            s, n = args[0].strip(), args[1].strip()
            rpad_tail = f", ( {args[2].strip()} )::text)" if len(args) >= 3 else ")"
            replacement = f"rpad(( {s} )::text, ( {n} )::int{rpad_tail}"
            body = body[: match.start()] + replacement + body[close + 1 :]
            rpad_search_start = match.start() + len(replacement)
        else:
            rpad_search_start = close + 1
    # LAST_DAY(date) -> last day of month
    pattern = re.compile(r"\bLAST_DAY\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if args:
            d = args[0].strip()
            body = body[: match.start()] + f"((date_trunc('month', ( {d} )::timestamp) + INTERVAL '1 month - 1 day')::date)" + body[close + 1 :]
        else:
            break
    # NEXT_DAY(date, weekday) -> PG equivalent: date + cast((dow - extract(dow) + 7) % 7 as int) days
    # Oracle NEXT_DAY returns the next occurrence of the given weekday after date.
    _NEXT_DAY_DOW_MAP = {
        "SUNDAY": 0, "SUN": 0,
        "MONDAY": 1, "MON": 1,
        "TUESDAY": 2, "TUE": 2,
        "WEDNESDAY": 3, "WED": 3,
        "THURSDAY": 4, "THU": 4,
        "FRIDAY": 5, "FRI": 5,
        "SATURDAY": 6, "SAT": 6,
    }
    pattern = re.compile(r"\bNEXT_DAY\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            d = args[0].strip()
            weekday_raw = args[1].strip().strip("'\"").upper()
            target_dow = _NEXT_DAY_DOW_MAP.get(weekday_raw)
            if target_dow is not None:
                # (date + ((target_dow - extract(dow from date) + 7) % 7) * interval '1 day') -- 0 means +7
                repl = (
                    f"(( {d} )::date + "
                    f"(((({target_dow} - EXTRACT(DOW FROM ( {d} )::date)::int + 7) % 7) + "
                    f"CASE WHEN ({target_dow} - EXTRACT(DOW FROM ( {d} )::date)::int + 7) % 7 = 0 THEN 7 ELSE 0 END) "
                    f"* INTERVAL '1 day'))"
                )
            else:
                # Weekday is an expression, not a literal -- fall back to stub with warning
                repl = f"/* WARNING: NEXT_DAY stub; weekday expression not resolved */ (( {d} )::date + INTERVAL '7 days')"
            body = body[: match.start()] + repl + body[close + 1 :]
        elif args:
            d = args[0].strip()
            body = body[: match.start()] + f"/* WARNING: NEXT_DAY stub; missing weekday */ (( {d} )::date + INTERVAL '7 days')" + body[close + 1 :]
        else:
            break
    # LISTAGG(expr, delim) [WITHIN GROUP (ORDER BY ...)] -> string_agg
    pattern = re.compile(r"\bLISTAGG\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 2:
            break
        expr, delim = args[0].strip(), args[1].strip()
        rest = body[close + 1 :]
        within = re.match(r"\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\)", rest, re.IGNORECASE | re.DOTALL)
        if within:
            order_part = within.group(1).strip()
            agg = f"string_agg(( {expr} )::text, ( {delim} )::text ORDER BY {order_part})"
            span_end = close + 1 + within.end()
        else:
            agg = f"string_agg(( {expr} )::text, ( {delim} )::text)"
            span_end = close + 1
        body = body[: match.start()] + agg + body[span_end:]
    # REGEXP_LIKE(str, pattern [, 'i']) -> str ~ pattern or ~*
    pattern = re.compile(r"\bREGEXP_LIKE\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 2:
            break
        s, pat = args[0].strip(), args[1].strip()
        case_ins = len(args) >= 3 and "I" in (args[2].strip().upper().strip("'\""))
        op = "~*" if case_ins else "~"
        body = body[: match.start()] + f"(( {s} )::text {op} ( {pat} )::text)" + body[close + 1 :]
    # SYS_GUID() -> gen_random_uuid()::text
    body = re.sub(r"\bSYS_GUID\s*\(\s*\)", "gen_random_uuid()::text", body, flags=re.IGNORECASE)
    # WM_CONCAT(expr) -> string_agg(expr::text, ',')
    pattern = re.compile(r"\bWM_CONCAT\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if args:
            expr = args[0].strip()
            body = body[: match.start()] + f"string_agg(( {expr} )::text, ',')" + body[close + 1 :]
        else:
            break
    # REGEXP_SUBSTR(str, pattern) 2-arg -> (SELECT (r)[1] FROM regexp_matches(...) AS r LIMIT 1)
    pattern = re.compile(r"\bREGEXP_SUBSTR\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(body)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 2:
            break
        s, pat = args[0].strip(), args[1].strip()
        body = body[: match.start()] + f"(SELECT (r)[1] FROM regexp_matches(( {s} )::text, ( {pat} )::text) AS r LIMIT 1)" + body[close + 1 :]
    # REGEXP_COUNT(str, pattern) -> array_length(regexp_split_to_array(str, pattern), 1) - 1
    # Oracle: returns count of matches; PG has no REGEXP_COUNT function.
    pattern = re.compile(r"\bREGEXP_COUNT\s*\(", re.IGNORECASE)
    rc_start = 0
    while True:
        match = pattern.search(body, rc_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            s, pat = args[0].strip(), args[1].strip()
            flags = ""
            if len(args) >= 3:
                flags_arg = args[2].strip().strip("'\"").lower()
                if "i" in flags_arg:
                    flags = ", 'gi'"
                else:
                    flags = ", 'g'"
            else:
                flags = ", 'g'"
            repl = f"COALESCE(array_length((SELECT array_agg(m) FROM regexp_matches(( {s} )::text, ( {pat} )::text{flags}) AS m), 1), 0)"
            body = body[: match.start()] + repl + body[close + 1 :]
            rc_start = match.start() + len(repl)
        else:
            rc_start = close + 1
    # REGEXP_INSTR(str, pattern) -> COALESCE((SELECT strpos(str, (regexp_matches(str, pattern))[1])), 0)
    # Oracle: returns position of first match; PG has no REGEXP_INSTR function.
    pattern = re.compile(r"\bREGEXP_INSTR\s*\(", re.IGNORECASE)
    ri_start = 0
    while True:
        match = pattern.search(body, ri_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            s, pat = args[0].strip(), args[1].strip()
            repl = f"COALESCE((SELECT strpos(( {s} )::text, (regexp_matches(( {s} )::text, ( {pat} )::text))[1])), 0)"
            body = body[: match.start()] + repl + body[close + 1 :]
            ri_start = match.start() + len(repl)
        else:
            ri_start = close + 1
    # REPLACE(str, old) 2-arg -> REPLACE(str, old, '') (PG requires 3 args)
    # Oracle: 2-arg REPLACE removes all occurrences of old; PG REPLACE needs explicit empty-string third arg.
    pattern = re.compile(r"\bREPLACE\s*\(", re.IGNORECASE)
    rpl_start = 0
    while True:
        match = pattern.search(body, rpl_start)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1 : close].strip()
        args = _split_top_level_commas(inner)
        if len(args) == 2:
            # 2-arg form: add empty string as 3rd arg
            s, old = args[0].strip(), args[1].strip()
            repl = f"REPLACE({s}, {old}, '')"
            body = body[: match.start()] + repl + body[close + 1 :]
            rpl_start = match.start() + len(repl)
        else:
            rpl_start = close + 1
    return body


def _replace_pg_unit_dd_and_substring(body: str) -> str:
    """Fix PG-incompatible patterns: date_trunc('dd',...) -> 'day'; INTERVAL 'N dd' -> 'N day'; substring(_,_,len) -> GREATEST(len,0)."""
    body = re.sub(r"date_trunc\s*\(\s*['\"]dd['\"]\s*,", "date_trunc('day',", body, flags=re.IGNORECASE)
    # PostgreSQL INTERVAL uses 'day' not 'dd' (unit "dd" not recognized for type timestamp with time zone)
    body = re.sub(r"INTERVAL\s*'\s*([\d.]+)\s*dd\s*'", r"INTERVAL '\1 day'", body, flags=re.IGNORECASE)
    body = re.sub(r"INTERVAL\s*\"\s*([\d.]+)\s*dd\s*\"", r"INTERVAL '\1 day'", body, flags=re.IGNORECASE)
    for func in ("substring", "substr"):
        pattern = re.compile(r"\b" + func + r"\s*\(", re.IGNORECASE)
        search_start = 0
        while True:
            match = pattern.search(body, search_start)
            if not match:
                break
            start_paren = match.end() - 1
            close = _find_closing_paren(body, start_paren)
            if close is None:
                break
            inner = body[start_paren + 1 : close].strip()
            args = _split_top_level_commas(inner)
            if len(args) < 3:
                search_start = close + 1
                continue
            expr, start_arg, length_arg = args[0].strip(), args[1].strip(), args[2].strip()
            new_inner = f"{expr}, {start_arg}, GREATEST({length_arg}, 0)"
            replacement = func + "(" + new_inner + ")"
            body = body[: match.start()] + replacement + body[close + 1 :]
            search_start = match.start() + len(replacement)
    return body


def _replace_dual_with_pg(body: str) -> str:
    """Replace Oracle sys.dual / dual with a single-row source (PostgreSQL has no DUAL)."""
    # FROM sys.dual, FROM dual, JOIN dual, etc. -> FROM (SELECT 1 AS dummy) AS dual
    single_row = "(SELECT 1 AS dummy) AS dual"
    body = re.sub(r"\bFROM\s+sys\.dual\b", "FROM " + single_row, body, flags=re.IGNORECASE)
    body = re.sub(r"\bFROM\s+dual\b", "FROM " + single_row, body, flags=re.IGNORECASE)
    body = re.sub(r"\bJOIN\s+sys\.dual\b", "JOIN " + single_row, body, flags=re.IGNORECASE)
    body = re.sub(r"\bJOIN\s+dual\b", "JOIN " + single_row, body, flags=re.IGNORECASE)
    return body


def _convert_start_with_connect_by_to_recursive_cte(body: str) -> str:
    """
    Rewrite Oracle hierarchical queries (START WITH ... CONNECT BY) to PostgreSQL recursive CTE.
    Fixes 'syntax error at or near start' and similar. Handles LEVEL, PRIOR, and strips ORDER SIBLINGS BY.
    """
    start_with_m = re.search(r"\bSTART\s+WITH\s+", body, re.IGNORECASE)
    if not start_with_m:
        return body
    rest_after_start = body[start_with_m.end() :]
    connect_m = re.search(r"\s+CONNECT\s+BY\s+", rest_after_start, re.IGNORECASE)
    if not connect_m:
        return body
    start_with_condition = rest_after_start[: connect_m.start()].strip()
    connect_by_rest = rest_after_start[connect_m.end() :].strip()
    # Strip NOCYCLE if present
    connect_by_rest = re.sub(r"^\s*NOCYCLE\s+", "", connect_by_rest, flags=re.IGNORECASE)
    # Extract CONNECT BY condition (until ORDER SIBLINGS BY or end)
    order_sib = re.search(r"\s+ORDER\s+SIBLINGS\s+BY\s+", connect_by_rest, re.IGNORECASE)
    if order_sib:
        connect_by_condition = connect_by_rest[: order_sib.start()].strip()
    else:
        connect_by_condition = connect_by_rest.strip()
    if not start_with_condition or not connect_by_condition:
        return body
    main_query_orig = body[: start_with_m.start()].strip()
    main_query_orig = re.sub(r"[\s,]+$", "", main_query_orig)
    # Base: augment WHERE with START WITH condition (only for base branch)
    main_query_base = main_query_orig
    where_m = re.search(r"\bWHERE\s+", main_query_base, re.IGNORECASE)
    if where_m:
        main_query_base = main_query_base[: where_m.end()] + f" ( {start_with_condition} ) AND " + main_query_base[where_m.end() :]
    else:
        order_m = re.search(r"\bORDER\s+BY\s+", main_query_base, re.IGNORECASE)
        insert_pos = order_m.start() if order_m else len(main_query_base)
        main_query_base = main_query_base[:insert_pos] + f" WHERE ( {start_with_condition} ) " + main_query_base[insert_pos:]
    # Get first table/alias from FROM for recursive join (use original query)
    from_m = re.search(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?:WHERE|JOIN|START|$)", main_query_orig, re.IGNORECASE)
    if not from_m:
        return body
    table_ref = from_m.group(1).strip()
    table_alias = (from_m.group(2) or table_ref).strip()
    # Build join condition: PRIOR col_a = col_b -> r.col_a = rcte_t.col_b; col_b = PRIOR col_a -> rcte_t.col_b = r.col_a
    join_cond = connect_by_condition
    join_cond = re.sub(
        r"\bPRIOR\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*)\b",
        r"r.\1 = rcte_t.\2",
        join_cond,
        flags=re.IGNORECASE,
    )
    join_cond = re.sub(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*PRIOR\s+([a-zA-Z_][a-zA-Z0-9_]*)\b",
        r"rcte_t.\1 = r.\2",
        join_cond,
        flags=re.IGNORECASE,
    )
    # LEVEL: base -> 1 AS level, recursive -> (r.level + 1) AS level. Recursive uses original query (no START WITH filter).
    base_sql = re.sub(r"\bLEVEL\b", "1 AS level", main_query_base, flags=re.IGNORECASE)
    rec_sql = re.sub(r"\bLEVEL\b", "(r.level + 1) AS level", main_query_orig, flags=re.IGNORECASE)
    rec_from = f" FROM {table_ref} rcte_t JOIN rcte r ON {join_cond}"
    rec_sql = re.sub(
        r"\bFROM\s+" + re.escape(table_ref) + r"\s*(?:" + re.escape(table_alias) + r")?\s*(?=\s+WHERE|\s+JOIN|$)",
        rec_from,
        rec_sql,
        count=1,
        flags=re.IGNORECASE,
    )
    if " JOIN rcte r ON " not in rec_sql:
        rec_sql = re.sub(r"\bFROM\s+(\S+)\s*", rec_from + " ", rec_sql, count=1, flags=re.IGNORECASE)
    # If base has WHERE, recursive part must also have a JOIN; ensure we didn't break the FROM
    if " JOIN rcte r ON " not in rec_sql:
        return body
    cte_body = f"( {base_sql} UNION ALL {rec_sql} )"
    return f"WITH RECURSIVE rcte AS {cte_body} SELECT * FROM rcte"


_EMPTY_STR_PLACEHOLDER = "__ORACLE_EMPTY_PH__"


def _oracle_empty_string_as_null(body: str) -> str:
    """Oracle: empty string '' equals NULL in comparisons and CASE branches.
    Convert column = '' -> column IS NULL (and <> '' -> IS NOT NULL).
    Also convert THEN '' and ELSE '' to THEN NULL and ELSE NULL (Oracle '' IS NULL).

    NOTE: replacements are done on the raw body (before string-literal splitting)
    because '' is itself a string literal token that gets split out, making it
    invisible to regex patterns applied only on non-string parts.
    The patterns are specific enough (preceded by '=', '<>', '!=', 'THEN', 'ELSE')
    to avoid matching escaped quotes inside longer string literals.
    """
    col_ref = r"\b(?:[a-zA-Z_][a-zA-Z0-9_]*)(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?"
    # Column comparisons with empty string → IS NULL / IS NOT NULL
    # Use (?!') negative lookahead to avoid matching '' that is part of '...''...'
    body = re.sub(rf"({col_ref})\s*=\s*''(?!')", r"\1 IS NULL", body, flags=re.IGNORECASE)
    body = re.sub(rf'({col_ref})\s*=\s*""', r"\1 IS NULL", body, flags=re.IGNORECASE)
    body = re.sub(rf"({col_ref})\s*<>\s*''(?!')", r"\1 IS NOT NULL", body, flags=re.IGNORECASE)
    body = re.sub(rf"({col_ref})\s*!=\s*''(?!')", r"\1 IS NOT NULL", body, flags=re.IGNORECASE)
    body = re.sub(rf'({col_ref})\s*<>\s*""', r"\1 IS NOT NULL", body, flags=re.IGNORECASE)
    body = re.sub(rf'({col_ref})\s*!=\s*""', r"\1 IS NOT NULL", body, flags=re.IGNORECASE)
    # Expression result (closing paren) compared with empty string → IS NULL / IS NOT NULL
    # Handles: COALESCE(col, NULL) = ''  and  (col) = ''  and  func(x) <> ''
    body = re.sub(r"(\))\s*=\s*''(?!')", r"\1 IS NULL", body)
    body = re.sub(r'(\))\s*=\s*""', r"\1 IS NULL", body)
    body = re.sub(r"(\))\s*<>\s*''(?!')", r"\1 IS NOT NULL", body)
    body = re.sub(r"(\))\s*!=\s*''(?!')", r"\1 IS NOT NULL", body)
    body = re.sub(r'(\))\s*<>\s*""', r"\1 IS NOT NULL", body)
    body = re.sub(r'(\))\s*!=\s*""', r"\1 IS NOT NULL", body)
    # Oracle '' IS NULL, so THEN '' / ELSE '' should become THEN NULL / ELSE NULL
    body = re.sub(r"\bTHEN\s+''(?!')", "THEN NULL", body, flags=re.IGNORECASE)
    body = re.sub(r"\bELSE\s+''(?!')", "ELSE NULL", body, flags=re.IGNORECASE)
    # Oracle '' IS NULL: replace '' as a function/COALESCE argument.
    # Pattern: comma followed by '' followed by ')' or ',' (end/next arg).
    # (?!') prevents matching '' inside longer strings like 'it''s'.
    # This prevents COALESCE(numeric_col, '') from resolving as text type,
    # which would break comparisons like  bigint_col = COALESCE(..., '')
    body = re.sub(r",\s*''(?!')\s*(?=[),])", ", NULL", body)
    return body


def _empty_string_to_null_for_datetime(body: str) -> str:
    """('' )::date/numeric/integer etc. -> NULL::... (Oracle treats '' as NULL; PG invalid input syntax)."""
    parts = re.split(r"('(?:[^']|'')*')", body)
    for i in range(1, len(parts), 2):
        if parts[i] == "''":
            parts[i] = _EMPTY_STR_PLACEHOLDER
    text = "".join(parts)
    text = re.sub(
        rf"\(\s*{re.escape(_EMPTY_STR_PLACEHOLDER)}\s*\)\s*::\s*(date|timestamp(?:\s+without\s+time\s+zone|\s+with\s+time\s+zone)?)\b",
        r"NULL::\1", text, flags=re.IGNORECASE)
    text = re.sub(
        rf"\(\s*{re.escape(_EMPTY_STR_PLACEHOLDER)}\s*\)\s*::\s*(numeric|integer|int|bigint|smallint)\b",
        r"NULL::\1", text, flags=re.IGNORECASE)
    # SQL-style CAST('' AS type) -> NULL::type
    # Matches: CAST('' AS decimal), CAST('' AS numeric(10,2)), CAST('' AS date), etc.
    _cast_types = r"(?:decimal|numeric|integer|int|bigint|smallint|real|double\s+precision|float|date|timestamp(?:\s+without\s+time\s+zone|\s+with\s+time\s+zone)?)"
    text = re.sub(
        r"\bCAST\s*\(\s*" + re.escape(_EMPTY_STR_PLACEHOLDER) + r"\s+AS\s+(" + _cast_types + r")\s*(?:\([^)]*\))?\s*\)",
        r"NULL::\1",
        text,
        flags=re.IGNORECASE,
    )
    while True:
        match = re.search(r"\bto_timestamp\s*\(\s*" + re.escape(_EMPTY_STR_PLACEHOLDER) + r"\s*,", text, re.IGNORECASE)
        if not match:
            break
        start_paren = text.index("(", match.start())
        close = _find_closing_paren(text, start_paren)
        if close is None:
            break
        text = text[: match.start()] + "NULL::timestamp" + text[close + 1 :]
    text = text.replace(_EMPTY_STR_PLACEHOLDER, "''")
    return text


def _remove_quotes_from_columns(body: str) -> str:
    """Remove double quotes from quoted identifiers; skip inside single-quoted strings.
    Preserve quotes for PostgreSQL reserved words (END, GROUP, ORDER, LIMIT, etc.)
    so they stay valid as identifiers and don't get interpreted as SQL keywords."""
    def unquote(match):
        ident = match.group(1).replace('""', '"')
        # Keep identifier quoted if it would clash with a PG reserved keyword
        if ident.lower() in PG_RESERVED:
            return f'"{ident}"'
        return ident
    pattern = re.compile(r'"((?:[^"]|"")+)"')
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            result.append(pattern.sub(lambda m: unquote(m), part))
    return "".join(result)


def _ensure_space_before_keywords(body: str) -> str:
    """Ensure FROM, WHERE, GROUP, ORDER, etc. are preceded by space; GROUP BY / ORDER BY spaced.
    Uses [^\\s\\a-zA-Z_] so identifiers containing keywords as substrings (terrgroup,
    lc_group, purchase_order, from_date) stay intact, while non-letter/non-underscore
    chars before keywords ()FROM, 1FROM) get a space inserted.
    Skips content inside single-quoted strings AND double-quoted identifiers (e.g. "END", "GROUP")."""
    keywords = r"(FROM|WHERE|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|EXCEPT|INTERSECT)(?=\W|$)"
    # Split by both single-quoted strings and double-quoted identifiers to protect them
    parts = re.split(r"""('(?:[^']|'')*'|"(?:[^"]|"")*")""", body)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            # Only insert space when preceded by a non-letter, non-underscore, non-whitespace
            # character (digits, closing parens, punctuation).  Letters and underscores are
            # part of identifiers (e.g. terrgroup, lc_group) and must NOT be split.
            part = re.sub(rf"([^\sa-zA-Z_])({keywords})", r"\1 \2", part, flags=re.IGNORECASE)
            part = re.sub(r"\bGROUP\s*BY\b", "GROUP BY", part, flags=re.IGNORECASE)
            part = re.sub(r"\bORDER\s*BY\b", "ORDER BY", part, flags=re.IGNORECASE)
            part = re.sub(r"(GROUP)(BY)(?=\W|$)", r"\1 \2", part, flags=re.IGNORECASE)
            part = re.sub(r"(ORDER)(BY)(?=\W|$)", r"\1 \2", part, flags=re.IGNORECASE)
            result.append(part)
    return "".join(result)


def _fix_limit_comma_syntax(body: str) -> str:
    """Convert MySQL-style LIMIT offset, count to PostgreSQL LIMIT count OFFSET offset.
    PG does not support LIMIT x,y; it needs LIMIT y OFFSET x."""
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            part = re.sub(
                r"\bLIMIT\s+(\d+)\s*,\s*(\d+)\b",
                r"LIMIT \2 OFFSET \1",
                part,
                flags=re.IGNORECASE,
            )
            result.append(part)
    return "".join(result)


def _fix_timestamp_plus_integer(body: str) -> str:
    """Oracle: date/timestamp + number = add days. PG needs interval. Converts + N, + 1.5, + numeric_col to * INTERVAL '1 day'."""
    date_funcs = r"(?:CURRENT_TIMESTAMP|SYSDATE|LOCALTIMESTAMP|CURRENT_DATE|LOCALTIME|CURRENT_TIME)"
    # + integer or decimal literal (string-literal-safe, skip if already has INTERVAL)
    body = _sub_outside_strings(rf"({date_funcs})\s*\+\s*(-?\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", r"\1 + \2 * INTERVAL '1 day'", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(rf"({date_funcs})\s*-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", r"\1 - (\2)::numeric * INTERVAL '1 day'", body, flags=re.IGNORECASE)
    # + numeric column/identifier (fixes 'operator does not exist: timestamp without time zone + numeric')
    # Skip: function calls (followed by '('), INTERVAL keyword, date-like column names
    # Matches column names likely to be date/timestamp — by suffix OR prefix.
    # Suffix: _date, _time, _timestamp, _dt, _ts  (e.g. trx_date, start_time)
    # Prefix: date_, time_, timestamp_             (e.g. date_start, date_earned)
    _date_id_suffix_re = re.compile(
        r"(?:_date|_time|_timestamp|_dt|_ts)$|^(?:date_|time_|timestamp_)",
        re.IGNORECASE,
    )
    def _replace_plus_id(m):
        ident = m.group(2)
        if ident.upper() == "INTERVAL":
            return m.group(0)
        # Skip identifiers that look like date/timestamp columns or functions
        bare = ident.rsplit(".", 1)[-1] if "." in ident else ident
        if _date_id_suffix_re.search(bare):
            return m.group(0)
        return m.group(1) + " + (" + ident + ")::numeric * INTERVAL '1 day'"
    body = re.sub(
        rf"({date_funcs})\s*\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)",
        _replace_plus_id,
        body,
        flags=re.IGNORECASE,
    )
    # - numeric column/identifier (fixes 'operator does not exist: timestamp without time zone - double precision')
    def _replace_minus_id(m):
        ident = m.group(2)
        if ident.upper() == "INTERVAL":
            return m.group(0)
        bare = ident.rsplit(".", 1)[-1] if "." in ident else ident
        if _date_id_suffix_re.search(bare):
            return m.group(0)
        return m.group(1) + " - (" + ident + ")::numeric * INTERVAL '1 day'"
    body = re.sub(
        rf"({date_funcs})\s*-\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)",
        _replace_minus_id,
        body,
        flags=re.IGNORECASE,
    )
    # + (expr) or - (expr) for date functions: date_func + (days + 1) -> + ((days + 1))::numeric * INTERVAL '1 day'
    paren_pat = re.compile(rf"({date_funcs})\s*([+\-])\s*(\()", re.IGNORECASE)
    paren_search = 0
    while True:
        m = paren_pat.search(body, paren_search)
        if not m:
            break
        open_pos = m.start(3)
        close_pos = _find_closing_paren(body, open_pos)
        if close_pos is None:
            break
        after_close = body[close_pos + 1:].lstrip()
        # Skip if already has * INTERVAL or ::numeric
        if after_close.startswith("::") or after_close.startswith("* INTERVAL") or after_close.upper().startswith("* INTERVAL"):
            paren_search = close_pos + 1
            continue
        paren_expr = body[open_pos : close_pos + 1]
        op = m.group(2)
        repl = f"{m.group(1)} {op} ({paren_expr})::numeric * INTERVAL '1 day'"
        body = body[: m.start()] + repl + body[close_pos + 1 :]
        paren_search = m.start() + len(repl)
    # date_trunc(...) + N / - N / + numeric_col / - numeric_col
    while True:
        match = re.search(r"\bdate_trunc\s*\(", body, re.IGNORECASE)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        after_raw = body[close + 1 :]
        after = after_raw.lstrip()
        plus_m = re.match(r"\+\s*(-?\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        plus_id = re.match(r"\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", after)
        minus_m = re.match(r"-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        minus_id = re.match(r"-\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", after)
        span_start = match.start()
        def span_len(m):
            return (close + 1 - span_start) + (len(after_raw) - len(after)) + len(m.group(0))
        def _is_date_ident(ident):
            bare = ident.rsplit(".", 1)[-1] if "." in ident else ident
            return bool(_date_id_suffix_re.search(bare))
        if plus_m:
            body = body[: span_start] + body[span_start : close + 1] + f" + {plus_m.group(1)} * INTERVAL '1 day'" + body[span_start + span_len(plus_m) :]
        elif plus_id and plus_id.group(1).upper() != "INTERVAL" and not _is_date_ident(plus_id.group(1)):
            body = body[: span_start] + body[span_start : close + 1] + f" + ({plus_id.group(1)})::numeric * INTERVAL '1 day'" + body[span_start + span_len(plus_id) :]
        elif minus_m:
            body = body[: span_start] + body[span_start : close + 1] + f" - ({minus_m.group(1)})::numeric * INTERVAL '1 day'" + body[span_start + span_len(minus_m) :]
        elif minus_id and minus_id.group(1).upper() != "INTERVAL" and not _is_date_ident(minus_id.group(1)):
            body = body[: span_start] + body[span_start : close + 1] + f" - ({minus_id.group(1)})::numeric * INTERVAL '1 day'" + body[span_start + span_len(minus_id) :]
        else:
            break
    # (expr)::date or ::timestamp + N / - N / + numeric_col / - numeric_col
    cast_pat = re.compile(r"\)\s*::\s*(date|timestamp(?:\s+without\s+time\s+zone|\s+with\s+time\s+zone)?)\b", re.IGNORECASE)
    cast_start = 0
    while True:
        match = cast_pat.search(body, cast_start)
        if not match:
            break
        close_pos = match.start()
        if _is_inside_string_literal(body, close_pos):
            cast_start = close_pos + 1
            continue
        cast_start = 0
        open_pos = _find_open_paren(body, close_pos)
        if open_pos is None:
            break
        after_raw = body[match.end() :]
        after = after_raw.lstrip()
        plus_m = re.match(r"\+\s*(-?\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        plus_id = re.match(r"\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", after)
        minus_m = re.match(r"-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        minus_id = re.match(r"-\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", after)
        if plus_m:
            span_len = match.end() - open_pos + (len(after_raw) - len(after)) + len(plus_m.group(0))
            body = body[: open_pos] + body[open_pos : match.end()] + f" + {plus_m.group(1)} * INTERVAL '1 day'" + body[open_pos + span_len :]
        elif plus_id and plus_id.group(1).upper() != "INTERVAL" and not _date_id_suffix_re.search((plus_id.group(1).rsplit(".", 1)[-1] if "." in plus_id.group(1) else plus_id.group(1))):
            span_len = match.end() - open_pos + (len(after_raw) - len(after)) + len(plus_id.group(0))
            body = body[: open_pos] + body[open_pos : match.end()] + f" + ({plus_id.group(1)})::numeric * INTERVAL '1 day'" + body[open_pos + span_len :]
        elif minus_m:
            span_len = match.end() - open_pos + (len(after_raw) - len(after)) + len(minus_m.group(0))
            body = body[: open_pos] + body[open_pos : match.end()] + f" - ({minus_m.group(1)})::numeric * INTERVAL '1 day'" + body[open_pos + span_len :]
        elif minus_id and minus_id.group(1).upper() != "INTERVAL" and not _date_id_suffix_re.search((minus_id.group(1).rsplit(".", 1)[-1] if "." in minus_id.group(1) else minus_id.group(1))):
            span_len = match.end() - open_pos + (len(after_raw) - len(after)) + len(minus_id.group(0))
            body = body[: open_pos] + body[open_pos : match.end()] + f" - ({minus_id.group(1)})::numeric * INTERVAL '1 day'" + body[open_pos + span_len :]
        else:
            break
    # CAST(... AS date) or CAST(... AS timestamp) + N / - N / + id / - id / + func(...) / - func(...)
    # Handles SQL-style CAST ending with 'AS date)' or 'AS timestamp)'
    # Skip functions that return date/timestamp (date - date_trunc is valid PG: returns interval)
    _DATE_RETURNING_FUNCS = frozenset({
        "DATE_TRUNC", "NOW", "AGE", "MAKE_DATE", "MAKE_TIMESTAMP",
        "MAKE_TIMESTAMPTZ", "TO_DATE", "TO_TIMESTAMP", "CURRENT_DATE",
        "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "CLOCK_TIMESTAMP",
        "STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP",
        # COALESCE commonly wraps date columns (from Oracle NVL(date_col, default)).
        # Wrapping with ::numeric would break  date - COALESCE(date_col, date_default).
        "COALESCE", "NVL", "NVL2",
    })
    cast_as_pat = re.compile(
        r"AS\s+(date|timestamp(?:\s+without\s+time\s+zone|\s+with\s+time\s+zone)?)\s*\)",
        re.IGNORECASE,
    )
    cast_as_start = 0
    while True:
        match = cast_as_pat.search(body, cast_as_start)
        if not match:
            break
        end_paren = match.end() - 1  # position of ')'
        after_raw = body[match.end():]
        after = after_raw.lstrip()
        ws_len = len(after_raw) - len(after)
        replaced = False
        for op_char in ("+", "-"):
            op_pat_lit = re.match(rf"\{op_char}\s*(-?\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
            op_pat_func = re.match(rf"\{op_char}\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*\(", after)
            op_pat_id = re.match(rf"\{op_char}\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", after)
            if op_pat_lit:
                val = op_pat_lit.group(1)
                span_end = match.end() + ws_len + len(op_pat_lit.group(0))
                body = body[:match.end()] + f" {op_char} {val} * INTERVAL '1 day'" + body[span_end:]
                replaced = True
                break
            elif op_pat_func:
                fname = op_pat_func.group(1)
                if fname.upper() == "INTERVAL":
                    break
                # Skip date/timestamp-returning functions: date - date_trunc(...) is
                # valid PG (returns interval); wrapping with ::numeric would break it.
                if fname.upper() in _DATE_RETURNING_FUNCS:
                    break
                # Find the closing paren of the function call
                func_open = match.end() + ws_len + op_pat_func.end() - 1
                func_close = _find_closing_paren(body, func_open)
                if func_close is None:
                    break
                func_expr = body[match.end() + ws_len + op_pat_func.start() + len(op_char):func_close + 1].strip()
                span_end = func_close + 1
                body = body[:match.end()] + f" {op_char} ({func_expr})::numeric * INTERVAL '1 day'" + body[span_end:]
                replaced = True
                break
            elif op_pat_id:
                ident = op_pat_id.group(1)
                if ident.upper() == "INTERVAL":
                    break
                bare = ident.rsplit(".", 1)[-1] if "." in ident else ident
                if _date_id_suffix_re.search(bare):
                    break
                span_end = match.end() + ws_len + len(op_pat_id.group(0))
                body = body[:match.end()] + f" {op_char} ({ident})::numeric * INTERVAL '1 day'" + body[span_end:]
                replaced = True
                break
        if replaced:
            cast_as_start = match.start()  # re-scan from same position for chained ops
        else:
            cast_as_start = match.end()

    # to_timestamp(...) + N / - N / + numeric_col / - numeric_col
    ts_start = 0
    while True:
        match = re.search(r"\bto_timestamp\s*\(", body[ts_start:], re.IGNORECASE)
        if not match:
            break
        abs_start = ts_start + match.start()
        if _is_inside_string_literal(body, abs_start):
            ts_start = abs_start + 1
            continue
        ts_start = 0
        start_paren = abs_start + match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        after_raw = body[close + 1 :]
        after = after_raw.lstrip()
        plus_m = re.match(r"\+\s*(-?\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        plus_id = re.match(r"\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", after)
        minus_m = re.match(r"-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        minus_id = re.match(r"-\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", after)
        def _skip_date_ident(ident):
            bare = ident.rsplit(".", 1)[-1] if "." in ident else ident
            return bool(_date_id_suffix_re.search(bare))
        if plus_m:
            span_len = (close + 1 - abs_start) + (len(after_raw) - len(after)) + len(plus_m.group(0))
            body = body[: abs_start] + body[abs_start : close + 1] + f" + {plus_m.group(1)} * INTERVAL '1 day'" + body[abs_start + span_len :]
        elif plus_id and plus_id.group(1).upper() != "INTERVAL" and not _skip_date_ident(plus_id.group(1)):
            span_len = (close + 1 - abs_start) + (len(after_raw) - len(after)) + len(plus_id.group(0))
            body = body[: abs_start] + body[abs_start : close + 1] + f" + ({plus_id.group(1)})::numeric * INTERVAL '1 day'" + body[abs_start + span_len :]
        elif minus_m:
            span_len = (close + 1 - abs_start) + (len(after_raw) - len(after)) + len(minus_m.group(0))
            body = body[: abs_start] + body[abs_start : close + 1] + f" - ({minus_m.group(1)})::numeric * INTERVAL '1 day'" + body[abs_start + span_len :]
        elif minus_id and minus_id.group(1).upper() != "INTERVAL" and not _skip_date_ident(minus_id.group(1)):
            span_len = (close + 1 - abs_start) + (len(after_raw) - len(after)) + len(minus_id.group(0))
            body = body[: abs_start] + body[abs_start : close + 1] + f" - ({minus_id.group(1)})::numeric * INTERVAL '1 day'" + body[abs_start + span_len :]
        else:
            break
    # Heuristic: bare column references whose name ends in a date-related suffix
    # followed by +/- integer.  Oracle DATE - N subtracts days; PG supports
    # date - int but NOT timestamp - int.  Since Oracle DATE is mapped to
    # PG timestamp, these break.  Target column names ending in _date, _time,
    # _timestamp, _dt, _ts (covers Oracle EBS naming conventions).
    # Date column name pattern: suffix (_date, _time, etc.) OR prefix (date_, time_, etc.)
    _date_col_name_pat = (
        r"(?:[a-zA-Z_][a-zA-Z0-9_]*(?:_date|_time|_timestamp|_dt|_ts)"   # suffix
        r"|(?:date|time|timestamp)_[a-zA-Z0-9_]*)"                        # prefix
    )
    date_col_pat = re.compile(
        r"(\b[a-zA-Z_][a-zA-Z0-9_]*\.)?" + r"(" + _date_col_name_pat + r"\b)"
        r"\s*([+\-])\s*"
        r"(\d+(?:\.\d*)?)\b"
        r"(?!\s*\*\s*INTERVAL)",
        re.IGNORECASE,
    )
    body = date_col_pat.sub(
        lambda m: (m.group(1) or "") + m.group(2) + f" {m.group(3)} {m.group(4)} * INTERVAL '1 day'",
        body,
    )

    # Heuristic: date-named column +/- identifier (numeric column).
    # e.g. rctl.trx_date + rtl.due_days  →  rctl.trx_date + (rtl.due_days)::numeric * INTERVAL '1 day'
    # e.g. pps.date_start + asg.probation_period  →  + (...)::numeric * INTERVAL '1 day'
    # Skip if the RHS identifier itself has a date name (date - date is valid).
    date_col_id_pat = re.compile(
        r"(\b[a-zA-Z_][a-zA-Z0-9_]*\.)?" + r"(" + _date_col_name_pat + r"\b)"
        r"\s*([+\-])\s*"
        r"((?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)\b"
        r"(?!\s*\(|\s*\*\s*INTERVAL)",
        re.IGNORECASE,
    )

    def _date_col_id_repl(m):
        prefix = m.group(1) or ""
        date_col = m.group(2)
        op = m.group(3)
        rhs = m.group(4)
        # Skip if RHS is a keyword or itself a date column
        rhs_bare = rhs.rsplit(".", 1)[-1] if "." in rhs else rhs
        # Skip if RHS itself has a date suffix AND the op is '-' (date - date is valid PG,
        # returns interval).  For '+', date + date is NEVER valid, so always convert.
        if op == "-" and _date_id_suffix_re.search(rhs_bare):
            return m.group(0)
        if rhs_bare.upper() in ("INTERVAL", "AND", "OR", "THEN", "ELSE", "END",
                                 "WHEN", "FROM", "WHERE", "ON", "AS", "BETWEEN",
                                 "IN", "NOT", "NULL", "IS", "CASE", "SELECT"):
            return m.group(0)
        return prefix + date_col + f" {op} ({rhs})::numeric * INTERVAL '1 day'"

    body = date_col_id_pat.sub(_date_col_id_repl, body)

    # ---- Timestamp-producing expression followed by +/- func(...) or +/- numeric ----
    # Handles patterns like:
    #   COALESCE(..., CURRENT_TIMESTAMP) - COALESCE(numeric_expr)
    #   CASE ... WHEN ... THEN CURRENT_TIMESTAMP END - func(x)
    # where the LHS produces a timestamp (it ends with CURRENT_TIMESTAMP/SYSDATE/etc
    # followed by closing parens/END), and the RHS is a function call returning numeric.
    _ts_kw = r"(?:CURRENT_TIMESTAMP|LOCALTIMESTAMP|CURRENT_DATE|LOCALTIME|CURRENT_TIME|NOW\(\))"
    _ts_ending_pat = re.compile(
        rf"{_ts_kw}\s*(?:\)|END)"       # CURRENT_TIMESTAMP) or CURRENT_TIMESTAMP END
        r"(?:\s*(?:\)|END))*"            # optional additional )/END closers
        r"\s*([+\-])\s*"                 # the arithmetic operator
        r"([a-zA-Z_][a-zA-Z0-9_]*)\s*\(",  # followed by func_name(
        re.IGNORECASE,
    )
    ts_end_search = 0
    while True:
        m_te = _ts_ending_pat.search(body, ts_end_search)
        if not m_te:
            break
        func_name = m_te.group(2)
        op = m_te.group(1)
        # Skip if already has INTERVAL suffix or if the function returns date/timestamp
        # (e.g., date - to_timestamp() is valid)
        if func_name.upper() in ("INTERVAL",):
            ts_end_search = m_te.end()
            continue
        # Skip date-returning functions only for '-' (timestamp - timestamp is valid)
        if op == "-" and func_name.upper() in _DATE_RETURNING_FUNCS - {"COALESCE", "NVL", "NVL2"}:
            ts_end_search = m_te.end()
            continue
        # Find closing paren of the function call
        open_pos = m_te.end() - 1  # position of '('
        close_pos = _find_closing_paren(body, open_pos)
        if close_pos is None:
            ts_end_search = m_te.end()
            continue
        after_func = body[close_pos + 1:].lstrip()
        # Skip if already followed by * INTERVAL or ::numeric
        if (after_func.upper().startswith("* INTERVAL") or
                after_func.startswith("::numeric") or
                after_func.upper().startswith("::NUMERIC")):
            ts_end_search = close_pos + 1
            continue
        # Wrap the function call with ::numeric * INTERVAL '1 day'
        func_expr = body[m_te.start(2):close_pos + 1]
        replacement = f"({func_expr})::numeric * INTERVAL '1 day'"
        body = body[:m_te.start(2)] + replacement + body[close_pos + 1:]
        ts_end_search = m_te.start(2) + len(replacement)

    # Also handle: ...CURRENT_TIMESTAMP) - numeric_literal (when not already converted)
    _ts_ending_lit_pat = re.compile(
        rf"{_ts_kw}\s*(?:\)|END)"
        r"(?:\s*(?:\)|END))*"
        r"\s*([+\-])\s*"
        r"(\d+(?:\.\d*)?)\b"
        r"(?!\s*\*\s*INTERVAL)",
        re.IGNORECASE,
    )
    def _ts_ending_lit_repl(m):
        op = m.group(1)
        val = m.group(2)
        # Find what precedes the operator (the closing )/END + whitespace before op)
        prefix = m.group(0)[:m.start(1) - m.start()]
        return prefix + f"{op} {val} * INTERVAL '1 day'"
    body = _ts_ending_lit_pat.sub(_ts_ending_lit_repl, body)

    # Also handle: ...CURRENT_TIMESTAMP) - bare_identifier
    _ts_ending_id_pat = re.compile(
        rf"{_ts_kw}\s*(?:\)|END)"
        r"(?:\s*(?:\)|END))*"
        r"\s*([+\-])\s*"
        r"([a-zA-Z_][a-zA-Z0-9_.]*)\b"
        r"(?!\s*\(|\s*\*\s*INTERVAL)",
        re.IGNORECASE,
    )
    def _ts_ending_id_repl(m):
        op = m.group(1)
        ident = m.group(2)
        if ident.upper() in ("INTERVAL", "AND", "OR", "THEN", "ELSE", "END",
                              "WHEN", "FROM", "WHERE", "GROUP", "ORDER",
                              "HAVING", "LIMIT", "UNION", "EXCEPT", "INTERSECT",
                              "IN", "NOT", "NULL", "IS", "CASE", "SELECT", "AS"):
            return m.group(0)
        bare = ident.rsplit(".", 1)[-1] if "." in ident else ident
        if op == "-" and _date_id_suffix_re.search(bare):
            return m.group(0)  # date - date_col is valid
        prefix = m.group(0)[:m.start(1) - m.start()]
        return prefix + f"{op} ({ident})::numeric * INTERVAL '1 day'"
    body = _ts_ending_id_pat.sub(_ts_ending_id_repl, body)

    return body


def _fix_varchar_arithmetic(body: str) -> str:
    """Fix Oracle implicit numeric conversion: CAST(x AS VARCHAR(N)) + expr → CAST(x AS VARCHAR(N))::numeric + expr.

    Oracle silently converts varchar to number for arithmetic;
    PostgreSQL raises 'operator does not exist: character varying + integer'.
    Handles both SQL-style CAST and PG-style :: casts, on BOTH sides of the operator.
    """
    # Pattern 1: PG-style cast — )::varchar(N) followed by +/- (string-literal-safe)
    body = _sub_outside_strings(
        r"(::\s*(?:varchar|character\s+varying)\s*(?:\(\s*\d+\s*\))?)"
        r"(\s*[+\-]\s*)"
        r"(?=\d|[a-zA-Z_(])",
        r"\1::numeric\2", body, flags=re.IGNORECASE,
    )
    # Pattern 2: SQL-style CAST — CAST(expr AS VARCHAR(N)) followed by +/- (string-literal-safe)
    body = _sub_outside_strings(
        r"(AS\s+(?:varchar|character\s+varying)\s*(?:\(\s*\d+\s*\))?\s*\))"
        r"(\s*[+\-]\s*)"
        r"(?=\d|[a-zA-Z_(])",
        r"\1::numeric\2", body, flags=re.IGNORECASE,
    )
    # Pattern 3: +/- followed by CAST(... AS VARCHAR(N)) — VARCHAR on RHS of arithmetic
    # e.g. ... + 2 - CAST(x AS varchar(4000))  →  ... + 2 - CAST(x AS varchar(4000))::numeric
    # Uses a while-loop with _find_closing_paren to correctly handle nested expressions.
    rhs_cast_pat = re.compile(
        r"[+\-]\s*CAST\s*\(",
        re.IGNORECASE,
    )
    search_start = 0
    while True:
        m = rhs_cast_pat.search(body, search_start)
        if not m:
            break
        # Find the opening paren of CAST(
        open_pos = m.end() - 1
        close_pos = _find_closing_paren(body, open_pos)
        if close_pos is None:
            search_start = m.end()
            continue
        inner = body[open_pos + 1 : close_pos].strip()
        # Check if it ends with "AS VARCHAR(...)" or "AS CHARACTER VARYING(...)"
        as_varchar_match = re.search(
            r"\bAS\s+(?:varchar|character\s+varying)\s*(?:\(\s*\d+\s*\))?\s*$",
            inner, re.IGNORECASE,
        )
        if not as_varchar_match:
            search_start = close_pos + 1
            continue
        # Check that ::numeric is not already present after the closing paren
        after = body[close_pos + 1 : close_pos + 12]
        if re.match(r"\s*::\s*numeric", after, re.IGNORECASE):
            search_start = close_pos + 1
            continue
        # Insert ::numeric after the CAST closing paren
        body = body[: close_pos + 1] + "::numeric" + body[close_pos + 1 :]
        search_start = close_pos + 1 + len("::numeric")
    return body


def _fix_coalesce_type_safety(body: str) -> str:
    """Fix COALESCE type mismatches by converting bare numeric literals to string literals.

    Oracle NVL/COALESCE implicitly converts mismatched types; PostgreSQL requires
    all COALESCE arguments to share a common type.

    Strategy: convert bare numeric literals (0, 1, -1, 3.14, …) inside COALESCE
    to quoted string literals ('0', '1', '-1', '3.14').  PostgreSQL treats string
    literals as type ``unknown`` which adapts to the peer argument's type:
      - COALESCE(varchar_col, '0')  → resolves as text   ✓
      - COALESCE(numeric_col, '0')  → resolves as numeric ✓
      - COALESCE(numeric_col, '0') + tax → numeric arithmetic works ✓
    This avoids the blanket ::text cast that broke arithmetic (text + text).
    """
    _bare_num_re = re.compile(r"^-?\d+(?:\.\d*)?$")

    pattern = re.compile(r"\bCOALESCE\s*\(", re.IGNORECASE)
    result = body
    search_start = 0
    while True:
        m = pattern.search(result, search_start)
        if not m:
            break
        open_pos = m.end() - 1
        close_pos = _find_closing_paren(result, open_pos)
        if close_pos is None:
            search_start = m.end()
            continue
        inner = result[open_pos + 1 : close_pos]
        args = _split_top_level_commas(inner)
        if not args:
            search_start = m.end()
            continue

        # Check if there's at least one bare numeric literal AND at least one
        # non-numeric argument — only then is there a potential type mismatch.
        stripped_args = [a.strip() for a in args]
        has_numeric = any(_bare_num_re.match(a) for a in stripped_args)
        has_non_numeric = any(not _bare_num_re.match(a) for a in stripped_args)

        if has_numeric and has_non_numeric:
            # Convert only the bare numeric literals to string literals
            new_args = []
            for a in stripped_args:
                if _bare_num_re.match(a):
                    new_args.append(f"'{a}'")
                else:
                    new_args.append(a)
            new_inner = ", ".join(new_args)
            replacement = f"COALESCE({new_inner})"
            result = result[: m.start()] + replacement + result[close_pos + 1 :]
            # Continue from just after "COALESCE(" to process nested calls
            search_start = m.start() + len("COALESCE(")
        else:
            # Advance past "COALESCE(" to find nested COALESCE inside args
            search_start = m.end()
    return result


def _strip_text_cast_in_coalesce(body: str) -> str:
    """Remove spurious ::text casts added by sqlglot transpilation or TO_CLOB conversion.

    sqlglot adds ::text casts when transpiling Oracle→PostgreSQL to unify types,
    and converts CAST(... AS CLOB) to CAST(... AS TEXT).  Both break arithmetic
    (``text + text``), CASE type mismatches, and aggregate functions (``SUM(text)``).

    Strategy: strip ``(expr)::text``, ``ident::text``, and ``CAST(expr AS TEXT)``
    globally.  We keep ``::text`` that is an argument inside functions that genuinely
    need it (string_agg, lpad, rpad, etc.), which is safe because those ``::text``
    casts are on individual args, not on the function result that participates in
    arithmetic.
    """
    # Helper: check if position in body is inside a CASE THEN/ELSE branch.
    # If ::text is part of type unification in CASE, we must preserve it to avoid
    # "CASE types numeric and character varying cannot be matched" errors.
    _case_branch_re = re.compile(r"\b(?:THEN|ELSE)\s*$", re.IGNORECASE)

    def _is_case_branch_context(text_before_expr: str) -> bool:
        """Return True if the expression is directly after THEN/ELSE in a CASE."""
        pre = text_before_expr.rstrip()
        return bool(_case_branch_re.search(pre))

    # 1. (expr)::text → (expr)  — parenthesised expression, INCLUDING nested parens.
    #    Walk backwards from each "::text" to find the matching '(' and strip the cast.
    text_cast_pat = re.compile(r"\)::text\b", re.IGNORECASE)
    search_start = 0
    while True:
        m = text_cast_pat.search(body, search_start)
        if not m:
            break
        close_pos = m.start()  # position of ')'
        # Find matching '('
        open_pos = _find_open_paren(body, close_pos)
        if open_pos is None:
            search_start = m.end()
            continue
        pre = body[:open_pos].rstrip()
        # Skip if inside CASE THEN/ELSE — needed for type unification
        if _is_case_branch_context(pre):
            search_start = m.end()
            continue
        # Skip if the '(' is preceded by a function name that needs ::text args
        # (string_agg, lpad, rpad, etc.)  The pattern is func_name( (expr)::text
        # so we look back from '(' past optional whitespace and an optional '('.
        func_match = re.search(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(?$", pre)
        if func_match:
            fname = func_match.group(1).upper()
            if fname in ("STRING_AGG", "LPAD", "RPAD", "STRPOS", "SUBSTRING",
                         "REGEXP_MATCHES", "CONCAT", "FORMAT", "ARRAY_AGG",
                         "OVERLAY", "REPLACE", "TRANSLATE", "TRIM",
                         "BTRIM", "LTRIM", "RTRIM", "INITCAP",
                         "PG_TO_CHAR__", "TO_CHAR"):
                search_start = m.end()
                continue
        # Strip the ::text — keep the parenthesized expression as-is
        body = body[:close_pos + 1] + body[m.end():]
        search_start = close_pos + 1
    # 2. ident::text → ident  (bare identifier or qualified col.name)
    #    Preserve in CASE THEN/ELSE branches to avoid type mismatch.
    def _strip_ident_text(m):
        pre = body[:m.start()].rstrip()
        if _case_branch_re.search(pre):
            return m.group(0)  # preserve ::text in CASE branch
        return m.group(1)
    body = re.sub(r"(\b[a-zA-Z_][a-zA-Z0-9_.]*)::text\b", _strip_ident_text, body, flags=re.IGNORECASE)
    # 3. numeric_literal::text → numeric_literal  (e.g. 0::text from sqlglot)
    #    Preserve in CASE THEN/ELSE branches.
    def _strip_numlit_text(m):
        pre = body[:m.start()].rstrip()
        if _case_branch_re.search(pre):
            return m.group(0)  # preserve ::text in CASE branch
        return m.group(1)
    body = re.sub(r"(\b\d+(?:\.\d*)?)::text\b", _strip_numlit_text, body, flags=re.IGNORECASE)
    # 4. CAST(expr AS TEXT) or CAST(expr AS VARCHAR(N)) → (expr)
    #    CAST(... AS TEXT) comes from TO_CLOB/TO_NCLOB via sqlglot.
    #    CAST(... AS VARCHAR(N)) comes from TO_CHAR preprocessing (old or fallback).
    #    Both produce text/varchar type that breaks GREATEST/LEAST/COALESCE/arithmetic
    #    when mixed with numeric columns (PG treats varchar as text for type matching).
    cast_text_pat = re.compile(r"\bCAST\s*\(", re.IGNORECASE)
    search_start = 0
    while True:
        m = cast_text_pat.search(body, search_start)
        if not m:
            break
        # Skip if inside CASE THEN/ELSE — needed for type unification
        pre = body[:m.start()].rstrip()
        if _is_case_branch_context(pre):
            search_start = m.end()
            continue
        open_pos = m.end() - 1
        close_pos = _find_closing_paren(body, open_pos)
        if close_pos is None:
            search_start = m.end()
            continue
        inner = body[open_pos + 1 : close_pos].strip()
        # Match: AS TEXT  or  AS VARCHAR(N)  or  AS CHARACTER VARYING(N)
        as_text_match = re.search(
            r"\bAS\s+(?:TEXT|VARCHAR\s*(?:\(\s*\d+\s*\))?|CHARACTER\s+VARYING\s*(?:\(\s*\d+\s*\))?)\s*$",
            inner, re.IGNORECASE,
        )
        if not as_text_match:
            search_start = close_pos + 1
            continue
        expr = inner[: as_text_match.start()].strip()
        replacement = f"({expr})"
        body = body[: m.start()] + replacement + body[close_pos + 1 :]
        search_start = m.start() + len(replacement)
    return body


def _fix_date_to_numeric_cast(body: str) -> str:
    """Remove invalid date/timestamp → numeric casts.

    Oracle allows TO_NUMBER(TO_DATE(…)) via implicit conversion, but
    PostgreSQL raises ``cannot cast type date to numeric``.

    Patterns handled:

    1. PG-style: ``(expr)::date)::numeric`` → ``(expr)::date)``
       Also handles ``::timestamp`` variants.
    2. SQL-style: ``CAST(CAST(… AS date) AS decimal)`` → inner date expr.
       The outermost CAST-to-numeric wrapping a date CAST is removed.
    """
    # --- Pattern 1a: PG-style ::date)::numeric or ::timestamp)::numeric ---
    # Remove the trailing ::numeric / ::decimal / ::bigint / ::integer.
    # Allow optional extra closing parens between date type and ::numeric,
    # e.g.  (expr)::date)::numeric  or  ((expr)::date))::numeric
    body = re.sub(
        r"(::\s*(?:date|timestamp(?:\s+(?:with|without)\s+time\s+zone)?)\s*(?:\)\s*)+)"
        r"::\s*(?:numeric|decimal|bigint|integer)\b",
        r"\1",
        body,
        flags=re.IGNORECASE,
    )

    # --- Pattern 1b: SQL-style CAST(... AS date/timestamp) [)]*  ::numeric ---
    # Handles CAST(expr AS DATE)::numeric  and  (CAST(expr AS DATE))::numeric
    body = re.sub(
        r"(\bAS\s+(?:date|timestamp(?:\s+(?:with|without)\s+time\s+zone)?)\s*(?:\)\s*)+)"
        r"::\s*(?:numeric|decimal|bigint|integer)\b",
        r"\1",
        body,
        flags=re.IGNORECASE,
    )

    # --- Pattern 2: SQL-style CAST(date_expr AS DECIMAL/NUMERIC) ---
    # Detect CAST(… AS DECIMAL) where the inner expression ends with
    # "AS DATE)" (i.e. is itself a CAST to date).  Replace the outer
    # CAST with the inner date expression.
    cast_pat = re.compile(r"\bCAST\s*\(", re.IGNORECASE)
    search_start = 0
    while True:
        m = cast_pat.search(body, search_start)
        if not m:
            break
        open_pos = m.end() - 1
        close_pos = _find_closing_paren(body, open_pos)
        if close_pos is None:
            search_start = m.end()
            continue
        inner = body[open_pos + 1 : close_pos].strip()
        # Check: outer CAST ends with AS DECIMAL / NUMERIC / BIGINT / INTEGER
        outer_match = re.search(
            r"\bAS\s+(?:DECIMAL|NUMERIC|BIGINT|INTEGER)(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?\s*$",
            inner, re.IGNORECASE,
        )
        if not outer_match:
            search_start = close_pos + 1
            continue
        inner_expr = inner[: outer_match.start()].strip()
        # Check: the inner expression IS or ENDS WITH a date/timestamp type
        # e.g. CAST(x AS DATE) or (x)::date or …)::timestamp
        is_date_expr = bool(re.search(
            r"(?:AS\s+(?:DATE|TIMESTAMP(?:\s+(?:WITH|WITHOUT)\s+TIME\s+ZONE)?)\s*\)$"
            r"|::\s*(?:date|timestamp(?:\s+(?:with|without)\s+time\s+zone)?)\s*$"
            r"|::\s*(?:date|timestamp(?:\s+(?:with|without)\s+time\s+zone)?)\s*\)$)",
            inner_expr, re.IGNORECASE,
        ))
        if not is_date_expr:
            search_start = close_pos + 1
            continue
        # Replace: CAST(date_expr AS DECIMAL) → (date_expr)
        replacement = f"({inner_expr})"
        body = body[: m.start()] + replacement + body[close_pos + 1 :]
        search_start = m.start() + len(replacement)
    return body


def _fix_current_timestamp_between_text(body: str) -> str:
    """Cast BETWEEN bounds to timestamp when LHS is a timestamp expression.

    Oracle allows ``SYSDATE BETWEEN varchar_expr AND varchar_expr`` via implicit
    type conversion.  PostgreSQL requires explicit casting::

        CURRENT_TIMESTAMP BETWEEN (expr1)::timestamp AND (expr2)::timestamp

    Typical source: ``NVL(TO_CHAR(date_col, 'fmt'), 'default')`` which converts
    to ``COALESCE(to_char(date_col, 'fmt'), 'default')`` — returning text in PG.
    Adding ``::timestamp`` on the bounds lets PG parse the text back into timestamp.

    Also handles: ``CASE ... END BETWEEN ...`` and ``date_trunc(...) BETWEEN ...``.
    """
    # --- Phase 1: Handle date_trunc(...) BETWEEN patterns ---
    # date_trunc returns timestamp; if BETWEEN bounds are text, PG gives
    # "operator does not exist: timestamp without time zone >= text".
    dt_pat = re.compile(r"\bdate_trunc\s*\(", re.IGNORECASE)
    dt_search = 0
    while True:
        dt_m = dt_pat.search(body, dt_search)
        if not dt_m:
            break
        dt_open = dt_m.end() - 1
        dt_close = _find_closing_paren(body, dt_open)
        if dt_close is None:
            dt_search = dt_m.end()
            continue
        # Check if followed by BETWEEN (with optional whitespace)
        after_dt = body[dt_close + 1:].lstrip()
        if not after_dt.upper().startswith("BETWEEN "):
            dt_search = dt_close + 1
            continue
        # Inject a marker so the main pattern below can pick it up.
        # Replace "date_trunc(...)<ws>BETWEEN " with "date_trunc(...) __DT_BETWEEN__ "
        # Then the main loop handles the bounds.
        ws_after = len(body[dt_close + 1:]) - len(after_dt)
        between_kw_end = dt_close + 1 + ws_after + len("BETWEEN ")
        body = body[:dt_close + 1] + " __DT_BETWEEN__ " + body[between_kw_end:]
        dt_search = dt_close + 1 + len(" __DT_BETWEEN__ ")

    # Match CURRENT_TIMESTAMP/CURRENT_DATE/NOW() BETWEEN, and also
    # CASE ... END BETWEEN (a CASE returning timestamp used in BETWEEN),
    # and __DT_BETWEEN__ markers from phase 1 (date_trunc(...) BETWEEN).
    pat = re.compile(
        r"\b(CURRENT_TIMESTAMP|CURRENT_DATE|NOW\s*\(\s*\))\s+BETWEEN\s+"
        r"|\bEND\s+BETWEEN\s+"
        r"|\b__DT_BETWEEN__\s+",
        re.IGNORECASE,
    )
    search_start = 0
    while True:
        m = pat.search(body, search_start)
        if not m:
            break
        between_end = m.end()
        # ---- locate the low bound and the separating AND -----------------
        pos = between_end
        depth = 0
        and_pos: Optional[int] = None
        while pos < len(body):
            ch = body[pos]
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    break
                depth -= 1
            elif depth == 0 and body[pos : pos + 3].upper() == "AND":
                before_ch = body[pos - 1] if pos > 0 else " "
                after_ch = body[pos + 3] if pos + 3 < len(body) else " "
                if not (before_ch.isalnum() or before_ch == "_") and not (
                    after_ch.isalnum() or after_ch == "_"
                ):
                    and_pos = pos
                    break
            pos += 1
        if and_pos is None:
            search_start = m.end()
            continue
        low = body[between_end:and_pos].strip()
        # ---- locate the high bound (from after AND to next top-level keyword/paren) -
        high_start = and_pos + 3
        # skip whitespace
        while high_start < len(body) and body[high_start] == " ":
            high_start += 1
        pos = high_start
        depth = 0
        high_end = len(body)
        _end_keywords = ("AND ", "OR ", "ORDER ", "GROUP ", "HAVING ",
                         "LIMIT ", "UNION ", "EXCEPT ", "INTERSECT ",
                         "WHERE ", "THEN ", "ELSE ", "WHEN ", "END ")
        while pos < len(body):
            ch = body[pos]
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    high_end = pos
                    break
                depth -= 1
            elif depth == 0:
                tail = body[pos:]
                found_kw = False
                for kw in _end_keywords:
                    if tail[: len(kw)].upper() == kw:
                        before_ch = body[pos - 1] if pos > 0 else " "
                        if not (before_ch.isalnum() or before_ch == "_"):
                            high_end = pos
                            found_kw = True
                            break
                if found_kw:
                    break
            pos += 1
        high = body[high_start:high_end].strip()
        # ---- decide whether to cast each bound --------------------------------
        ts_re = re.compile(r"::\s*timestamp", re.IGNORECASE)
        needs_low = bool(low) and not ts_re.search(low)
        needs_high = bool(high) and not ts_re.search(high)
        if not needs_low and not needs_high:
            search_start = high_end
            continue
        new_low = f"({low})::timestamp" if needs_low else low
        new_high = f"({high})::timestamp" if needs_high else high
        # Build the prefix: restore __DT_BETWEEN__ marker back to BETWEEN
        prefix = m.group(0)
        if "__DT_BETWEEN__" in prefix:
            prefix = prefix.replace("__DT_BETWEEN__", "BETWEEN")
        replacement = f"{prefix}{new_low} AND {new_high}"
        # Ensure a space before the remainder if it starts with a keyword
        remainder = body[high_end:]
        if remainder and not remainder[0].isspace():
            replacement += " "
        body = body[: m.start()] + replacement + remainder
        search_start = m.start() + len(replacement)
    # Safety: restore any remaining markers that weren't processed
    body = body.replace("__DT_BETWEEN__", "BETWEEN")
    return body


def _fix_sign_with_interval(body: str) -> str:
    """Convert sign(expr) to sign(EXTRACT(EPOCH FROM (expr))::numeric).

    Oracle sign() works with dates/intervals (date subtraction returns a number).
    PostgreSQL sign() only accepts numeric; date subtraction returns an interval,
    causing 'function sign(interval) does not exist'.
    """
    pattern = re.compile(r"\bsign\s*\(", re.IGNORECASE)
    result = body
    search_start = 0
    while True:
        m = pattern.search(result, search_start)
        if not m:
            break
        open_pos = m.end() - 1
        close_pos = _find_closing_paren(result, open_pos)
        if close_pos is None:
            search_start = m.end()
            continue
        inner = result[open_pos + 1 : close_pos].strip()
        replacement = f"sign(EXTRACT(EPOCH FROM ({inner}))::numeric)"
        result = result[: m.start()] + replacement + result[close_pos + 1 :]
        search_start = m.start() + len(replacement)
    return result


def _find_select_list_bounds(body: str) -> tuple[int, int] | None:
    """Return (start, end) of the SELECT list (between SELECT and FROM) at top level, or None."""
    match = re.search(r"\bSELECT\s+", body, re.IGNORECASE)
    if not match:
        return None
    start = match.end()
    depth = 0
    in_single = False
    i = start
    while i < len(body):
        c = body[i]
        if in_single:
            if c == "'" and (i + 1 >= len(body) or body[i + 1] != "'"):
                in_single = False
            elif c == "'" and i + 1 < len(body) and body[i + 1] == "'":
                i += 1
            i += 1
            continue
        if c == "'":
            in_single = True
            i += 1
            continue
        if c == "(":
            depth += 1
            i += 1
            continue
        if c == ")":
            depth -= 1
            i += 1
            continue
        if depth == 0:
            # Manual leading word-boundary: char before position must not be a word character
            # (avoids matching 'from' inside identifiers like 'sent_from', 'created_from')
            prev_is_word = i > 0 and (body[i - 1].isalnum() or body[i - 1] == '_')
            from_match = re.match(r"FROM\b", body[i:], re.IGNORECASE) if not prev_is_word else None
            if from_match:
                end = i
                while end > start and body[end - 1] in " \t":
                    end -= 1
                return (start, end)
        i += 1
    return None


def _quote_alias_if_reserved(alias: str) -> str:
    """Return alias quoted for PostgreSQL if it is a reserved keyword (avoids 'syntax error at or near AS')."""
    if alias.lower() in PG_RESERVED:
        return f'"{alias}"'
    return alias


def _is_star_select_item(part: str) -> bool:
    """True if this select list item is * or qual.* (do not add AS; PostgreSQL does not allow aliasing *)."""
    p = part.strip()
    if p == "*":
        return True
    return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*\s*\.\s*\*\s*$", p))


def _deduplicate_select_aliases_in_body(body: str) -> str:
    """Rename duplicate column aliases so each output column has a unique label (avoids 'column specified more than once').
    Also: empty select items (trailing/double comma) -> NULL AS empty_N; never add AS to * or qual.*;
    quote reserved-word aliases for PostgreSQL (avoids 'syntax error at or near AS').
    Note: Only the list between SELECT and FROM is modified; the keyword SELECT is never replaced (body[:start] is preserved)."""
    bounds = _find_select_list_bounds(body)
    if not bounds:
        return body
    start, end = bounds
    # Replace only the select list (between SELECT and FROM); never touch the keyword SELECT
    select_list = body[start:end]
    # Strip trailing comma (fixes 'syntax error near ,' or 'near from' for SELECT a, b, FROM t)
    select_list = re.sub(r",\s*$", "", select_list).strip()
    parts = _split_top_level_commas(select_list)
    alias_count: dict[str, int] = {}
    unnamed_col_index = [0]
    new_parts: list[str] = []
    # Match explicit AS alias (unquoted or double-quoted): AS col_name or AS "END"
    # Include $ in identifier chars — Oracle allows $ in identifiers (e.g. a$customer_name)
    alias_re = re.compile(r'\s+AS\s+("?[a-zA-Z_$][a-zA-Z0-9_$]*"?)\s*$', re.IGNORECASE)
    implicit_alias_re = re.compile(r"\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*$")
    for part in parts:
        part_stripped = part.strip()
        # Empty item from trailing comma or double comma -> avoid " AS col" which causes syntax error at/near AS
        if not part_stripped:
            unnamed_col_index[0] += 1
            new_parts.append(f"NULL AS empty_{unnamed_col_index[0]}")
            continue
        # Do not add AS to * or table.* (invalid in PostgreSQL)
        if _is_star_select_item(part_stripped):
            new_parts.append(part_stripped)
            continue
        m = alias_re.search(part_stripped)
        alias_lower = None
        if m:
            alias_raw = m.group(1)
            # Strip surrounding double quotes for lookup/dedup; preserve quoting in output
            is_quoted = alias_raw.startswith('"') and alias_raw.endswith('"')
            alias_bare = alias_raw[1:-1] if is_quoted else alias_raw
            alias_lower = alias_bare.lower()
            alias_count[alias_lower] = alias_count.get(alias_lower, 0) + 1
            count = alias_count[alias_lower]
            if count > 1:
                new_alias_bare = f"{alias_bare}_{count}"
                out_alias = f'"{new_alias_bare}"' if is_quoted else _quote_alias_if_reserved(new_alias_bare)
                new_part = alias_re.sub(f" AS {out_alias}", part_stripped, count=1)
                new_parts.append(new_part)
            else:
                out_alias = alias_raw if is_quoted else _quote_alias_if_reserved(alias_bare)
                if out_alias != alias_raw:
                    new_part = alias_re.sub(f" AS {out_alias}", part_stripped, count=1)
                    new_parts.append(new_part)
                else:
                    new_parts.append(part_stripped)
        else:
            # Only treat trailing identifier as implicit alias when part is not a subquery/expression
            # (e.g. "(SELECT 1 FROM t)" would otherwise get " t" -> " AS t" -> invalid "FROM AS t")
            # Also skip when preceded by || (concatenation operand, not alias)
            imm = implicit_alias_re.search(part_stripped) if "(" not in part_stripped else None
            if imm:
                before_text = part_stripped[:imm.start()].rstrip()
                if before_text.endswith(("||", "+", "-", "*", "/")):
                    # Trailing identifier is an operand (concatenation/arithmetic), not an alias
                    imm = None
            if imm:
                candidate = imm.group(1)
                # CASE...END: the trailing END is a keyword, not an alias
                if candidate.upper() == "END" and re.search(r"\bCASE\b", part_stripped, re.IGNORECASE):
                    imm = None
                # Skip other SQL keywords (NULL, TRUE, FALSE) that appear as expression values
                elif candidate.lower() in ("null", "true", "false"):
                    imm = None
            if imm:
                alias = imm.group(1)
                alias_lower = alias.lower()
                alias_count[alias_lower] = alias_count.get(alias_lower, 0) + 1
                count = alias_count[alias_lower]
                out_alias = _quote_alias_if_reserved(alias if count == 1 else f"{alias}_{count}")
                if count > 1:
                    new_part = implicit_alias_re.sub(f" AS {out_alias}", part_stripped, count=1)
                    new_parts.append(new_part)
                else:
                    new_part = implicit_alias_re.sub(f" AS {out_alias}", part_stripped, count=1)
                    new_parts.append(new_part)
            else:
                # No alias found — leave the expression as-is.
                # PostgreSQL auto-generates column names for unnamed expressions
                # (e.g. CASE, function calls, arithmetic) just like Oracle does.
                new_parts.append(part_stripped)
    new_list = ", ".join(new_parts)
    return body[:start] + new_list + body[end:]


def _replace_userenv_to_postgres(body: str) -> str:
    """
    Replace USERENV(...) for PostgreSQL. Only USERENV('USER')/('SESSION_USER') -> current_user;
    others (e.g. CLIENT_INFO, TERMINAL) -> NULL::text to avoid 'invalid input syntax for type numeric'
    when the session returns a string like 'pgadmin4' that is later used in numeric context.
    """
    while True:
        match = re.search(r"\bUSERENV\s*\(", body, re.IGNORECASE)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        arg = body[start_paren + 1 : close].strip().strip("'\"").upper().replace(" ", "")
        # Oracle: USERENV('USER') / USERENV('SESSION_USER') = session username; CLIENT_INFO, TERMINAL, etc. = other
        if arg in ("USER", "SESSIONUSER", "SESSION_USER"):
            repl = "current_user"
        else:
            repl = "NULL::text"
        body = body[: match.start()] + repl + body[close + 1 :]
    return body


def _replace_rowid_to_ctid(body: str) -> str:
    """Convert Oracle ROWID/ROW_ID to PostgreSQL ctid (whole-word, case-insensitive)."""
    body = re.sub(r"\bROW_ID\b", "ctid", body, flags=re.IGNORECASE)
    body = re.sub(r"\bROWID\b", "ctid", body, flags=re.IGNORECASE)
    return body


def _decode_html_entities_in_body(body: str) -> str:
    """
    Decode common HTML entities that can appear in DDL (e.g. from export tools), causing
    'syntax error at or near from' when operators are corrupted (e.g. > as &gt;, < as &lt;).
    """
    body = body.replace("&gt;", ">").replace("&lt;", "<")
    body = body.replace("&amp;", "&").replace("&quot;", '"')
    return body


def _repair_identifier_space_before_dot(body: str) -> str:
    """
    Repair identifier corruption where a space was inserted in an alias/table name (e.g. seg order.ctid -> seg_order.ctid),
    causing 'syntax error at or near .'. Pattern: 'id id.column' -> 'id_id.column' only when followed by a dot.
    Does not merge SQL keywords with the next token (e.g. SELECT aps. must stay SELECT aps., not select_aps.).
    Skips inside single-quoted strings.
    """
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    # Match two identifier-like tokens separated by space, immediately followed by a dot (qualified ref)
    pat = re.compile(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*)(\.)"
    )

    def replace_if_not_keyword(m: re.Match) -> str:
        first = m.group(1)
        if first.lower() in PG_RESERVED:
            return m.group(0)  # keep "SELECT aps." as-is, do not merge to "SELECT_aps."
        return f"{m.group(1)}_{m.group(2)}{m.group(3)}"

    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            result.append(pat.sub(replace_if_not_keyword, part))
    return "".join(result)


def _lowercase_body_identifiers(body: str) -> str:
    """Lowercase identifiers in view body; keep PostgreSQL reserved keywords uppercase."""
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
            continue
        def replace_id(match):
            word = match.group(0)
            return word.upper() if word.lower() in PG_RESERVED else word.lower()
        result.append(re.sub(r"[a-zA-Z_][a-zA-Z0-9_]*", replace_id, part))
    return "".join(result)


_COMMENT_ALIAS_BLOCKLIST = frozenset({
    "todo", "fixme", "note", "hack", "bug", "xxx", "noqa", "noinspection",
    "nolint", "pragma", "hint", "inline", "suppress", "warning", "deprecated",
})

_COMMENT_ALIAS_RE = re.compile(
    r'/\*\s*([a-zA-Z_$#][a-zA-Z0-9_$#]*)\s*\*/'
)


def _sanitize_alias_name(alias: str) -> str:
    """Clean an alias so it is a valid, safe PostgreSQL identifier.

    - Replaces non-alphanumeric/_ chars (like ``$``, ``#``) with ``_``
    - Prefixes with ``col_`` if the result starts with a digit or is empty
    - Double-quotes if the alias is a PostgreSQL reserved word
    """
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', alias)
    if not clean or clean[0].isdigit():
        clean = "col_" + clean
    return _quote_alias_if_reserved(clean)


def _convert_column_comments_to_aliases(body: str) -> str:
    """Convert inline ``/* name */`` comments in the SELECT list to ``AS name``.

    Oracle DDL or sqlglot transpilation sometimes produces patterns like::

        expression /* column_name */

    instead of::

        expression AS column_name

    Processing is per-item (split by top-level commas) so that:

    - Comments inside string literals are never touched
    - ``CAST(x AS int) /* col */`` correctly adds the alias after the cast
    - An item that already has an explicit ``AS alias`` before the comment
      gets the comment removed rather than producing a double alias
    - Common non-alias comment words (TODO, FIXME, …) are skipped
    """
    bounds = _find_select_list_bounds(body)
    if not bounds:
        return body
    start, end = bounds
    select_list = body[start:end]

    parts = _split_top_level_commas(select_list)
    changed = False
    new_parts: list[str] = []

    explicit_as_tail_re = re.compile(
        r'\bAS\s+("?[a-zA-Z_$#][a-zA-Z0-9_$#]*"?)\s*$', re.IGNORECASE,
    )

    for part in parts:
        stripped = part.strip()
        if not stripped:
            new_parts.append(stripped)
            continue

        # Collect all /* identifier */ matches in this item (outside strings)
        # We process only the *last* one as a candidate alias.
        matches = list(_COMMENT_ALIAS_RE.finditer(stripped))
        if not matches:
            new_parts.append(stripped)
            continue

        last_m = matches[-1]
        candidate = last_m.group(1).strip()

        # Skip blocklisted words (non-alias comments)
        if candidate.lower() in _COMMENT_ALIAS_BLOCKLIST:
            new_parts.append(stripped)
            continue

        # Check whether the match is inside a string literal.
        # Count unescaped single-quotes before the match start; odd means inside string.
        pre = stripped[:last_m.start()]
        in_string = pre.count("'") % 2 == 1
        if in_string:
            new_parts.append(stripped)
            continue

        alias = _sanitize_alias_name(candidate)

        # Remove the comment first
        item_no_comment = stripped[:last_m.start()].rstrip() + stripped[last_m.end():]
        item_no_comment = item_no_comment.strip()

        # If the item already has an explicit AS alias, just drop the comment
        if explicit_as_tail_re.search(item_no_comment):
            new_parts.append(item_no_comment)
        else:
            new_parts.append(f"{item_no_comment} AS {alias}")
        changed = True

    if not changed:
        return body
    new_list = ", ".join(new_parts)
    return body[:start] + new_list + body[end:]


def _sanitize_select_aliases(body: str) -> str:
    """Sanitise existing ``AS alias`` tokens in the SELECT list.

    Operates per-item (split by top-level commas) so that ``CAST(x AS int)``
    inside a select expression is never mistaken for a column alias.

    Fixes applied to each item's trailing ``AS alias``:

    - Special characters (``#``, ``$``) replaced with ``_``
    - Leading-digit aliases prefixed with ``col_``
    - PostgreSQL reserved words double-quoted
    """
    bounds = _find_select_list_bounds(body)
    if not bounds:
        return body
    start, end = bounds
    select_list = body[start:end]

    parts = _split_top_level_commas(select_list)
    changed = False
    new_parts: list[str] = []

    # Trailing AS alias (unquoted) at end of item
    trailing_as_re = re.compile(
        r'^(.*\bAS\s+)([a-zA-Z_$#][a-zA-Z0-9_$#]*)\s*$',
        re.IGNORECASE | re.DOTALL,
    )

    for part in parts:
        stripped = part.strip()
        m = trailing_as_re.match(stripped)
        if not m:
            new_parts.append(stripped)
            continue

        prefix = m.group(1)
        alias = m.group(2)
        clean = _sanitize_alias_name(alias)
        if clean != alias:
            new_parts.append(prefix + clean)
            changed = True
        else:
            new_parts.append(stripped)

    if not changed:
        return body
    new_list = ", ".join(new_parts)
    return body[:start] + new_list + body[end:]


def normalize_view_script(
    pg_ddl: str,
    apply_oracle_conversions: bool = True,
    orig_schema: str = "",
    orig_view_name: str = "",
    orig_column_list: str | None = None,
) -> str:
    """
    Normalize view DDL to: CREATE OR REPLACE VIEW name AS body;
    (no FORCE). View name and column names are lowercased.
    If apply_oracle_conversions is True (default), apply all Oracle→PG body conversions.
    If False (e.g. --no-convert-oracle), only do structure and view name.
    When orig_schema and orig_view_name are provided, forces the view name to schema.name
    (so it stays e.g. apps2.viewname instead of being overwritten by regex extraction).
    """
    if not pg_ddl or not pg_ddl.strip():
        return pg_ddl
    text = pg_ddl.replace("\r\n", "\n").replace("\r", "\n").strip()
    text = _NORMALIZE_DROP_VIEW_PATTERN.sub("", text).strip()
    create_match = _NORMALIZE_CREATE_VIEW_PATTERN.search(text)
    if not create_match:
        return pg_ddl
    raw_name = create_match.group(1).strip()
    column_list_raw = create_match.group(2)
    body = create_match.group(3)
    # Force original schema.view_name when provided; otherwise extract from DDL
    if (orig_schema or "").strip() and (orig_view_name or "").strip():
        view_name = f"{orig_schema.strip()}.{orig_view_name.strip()}".lower()
    else:
        view_name = raw_name.replace('"', "").strip()
        view_name = re.sub(r"\s*\.\s*", ".", view_name).lower()
    if not view_name:
        view_name = "view_name"
    body = re.sub(r"\s*BEQUEATH\s+(?:DEFINER|CURRENT_USER)\s*", " ", body, flags=re.IGNORECASE)
    body = re.sub(r"\s*WITH\s+READ\s+ONLY\s*;?\s*$", "", body, flags=re.IGNORECASE)
    body = body.rstrip().rstrip(";").strip()
    body = " ".join(body.split())
    if apply_oracle_conversions:
        try:
            def _norm_step(name: str, fn, b: str) -> str:
                log.debug("[NORMALIZE] %s starting (body len=%d)", name, len(b))
                t0 = time.perf_counter()
                out = fn(b)
                log.debug("[NORMALIZE] %s done in %.2fs", name, time.perf_counter() - t0)
                return out
            body = _norm_step("column_comments_to_aliases", _convert_column_comments_to_aliases, body)
            body = _norm_step("strip_sql_comments", _strip_sql_comments, body)
            body = _norm_step("decode_html_entities", _decode_html_entities_in_body, body)
            body = _norm_step("repair_identifier_space", _repair_identifier_space_before_dot, body)
            body = _norm_step("remove_outer_join_plus", _remove_outer_join_plus, body)
            body = _norm_step("nvl_nvl2_decode", _replace_nvl_nvl2_decode_in_body, body)
            body = _norm_step("oracle_to_functions", _replace_oracle_to_functions_in_body, body)
            body = _norm_step("oracle_builtin_functions", _replace_oracle_builtin_functions_in_body, body)
            body = _norm_step("trunc", _replace_trunc_in_body, body)
            body = _norm_step("pg_unit_dd_substring", _replace_pg_unit_dd_and_substring, body)
            body = _norm_step("oracle_misc", _replace_oracle_misc_in_body, body)
            body = _norm_step("oracle_sequence_refs", _replace_oracle_sequence_refs, body)
            body = _norm_step("rowid_to_ctid", _replace_rowid_to_ctid, body)
            body = _norm_step("userenv", _replace_userenv_to_postgres, body)
            body = _norm_step("oracle_package_schemas", _replace_oracle_package_schemas, body)
            body = _norm_step("package_in_from_clause", _replace_package_in_from_clause, body)
            body = _norm_step("dual_with_pg", _replace_dual_with_pg, body)
            body = _norm_step("start_with_connect_by", _convert_start_with_connect_by_to_recursive_cte, body)
            body = _norm_step("timestamp_plus_integer", _fix_timestamp_plus_integer, body)
            body = _norm_step("varchar_arithmetic", _fix_varchar_arithmetic, body)
            # Strip ::text casts from COALESCE arguments (added by sqlglot transpilation)
            body = _norm_step("strip_text_cast_in_coalesce", _strip_text_cast_in_coalesce, body)
            body = _norm_step("date_to_numeric_cast", _fix_date_to_numeric_cast, body)
            body = _norm_step("current_timestamp_between_text", _fix_current_timestamp_between_text, body)
            # _fix_coalesce_type_safety is NOT run in the pipeline.
            # Without column-type information we cannot safely choose between
            # casting to text (breaks SUM/arithmetic) and casting to numeric
            # (breaks string context).  COALESCE type mismatches must be fixed
            # manually or via the retry script which has access to the actual error.
            body = _norm_step("sign_with_interval", _fix_sign_with_interval, body)
            body = _norm_step("limit_comma_syntax", _fix_limit_comma_syntax, body)
            body = _norm_step("empty_string_as_null", _oracle_empty_string_as_null, body)
            body = _norm_step("empty_string_to_null_datetime", _empty_string_to_null_for_datetime, body)
            body = _norm_step("remove_quotes_from_columns", _remove_quotes_from_columns, body)
            body = _norm_step("deduplicate_select_aliases", _deduplicate_select_aliases_in_body, body)
            body = _norm_step("sanitize_select_aliases", _sanitize_select_aliases, body)
            body = _norm_step("cast_numeric_string_literals", _cast_numeric_string_literals_in_equality, body)
            body = _norm_step("lowercase_body_identifiers", _lowercase_body_identifiers, body)
            body = _norm_step("ensure_space_before_keywords", _ensure_space_before_keywords, body)
        except Exception as e:
            log.warning("[NORMALIZE] exception in body conversions for view %s: %s", view_name, e)
    else:
        body = _lowercase_body_identifiers(body)
    # Safety: restore any remaining TO_CHAR/TO_DATE/TO_TIMESTAMP placeholders
    body = body.replace("pg_to_char__(", "to_char(")
    body = body.replace("pg_to_date__(", "to_date(")
    body = body.replace("pg_to_timestamp__(", "to_timestamp(")

    # Preserve explicit column list (col1, col2) when present
    # Use orig_column_list if sqlglot/rewrite dropped it from pg_ddl
    # Column names are lowercased; reserved words and names with spaces/special chars are quoted.
    column_list_suffix = " "
    col_list_source = column_list_raw or orig_column_list
    if col_list_source:
        col_content = col_list_source.strip().strip("()").strip()
        if col_content:
            col_parts = [c.strip() for c in col_content.split(",") if c.strip()]
            if col_parts:
                normalized_cols = []
                for c in col_parts:
                    bare = c.strip('"').strip()
                    lower = bare.lower()
                    # Quote if reserved, has spaces, or other non-identifier chars
                    needs_quote = (
                        lower in PG_RESERVED
                        or re.search(r"[^a-zA-Z0-9_]", lower)
                        or not lower
                    )
                    if not lower:
                        lower = "col"
                    normalized_cols.append(f'"{lower}"' if needs_quote else lower)
                col_normalized = ", ".join(normalized_cols)
                column_list_suffix = f" ({col_normalized}) "

    return f"CREATE OR REPLACE VIEW {view_name}{column_list_suffix}AS\n{body}\n;"


# Type alias for (schema, view_name) for dependency tracking
ViewKey = tuple[str, str]


def _extract_view_refs_from_body(
    body: str,
    view_keys: set[ViewKey],
    view_keys_by_name: Optional[dict[str, list[ViewKey]]] = None,
) -> set[ViewKey]:
    """
    Find FROM/JOIN references in body that are in view_keys (our view list).
    body = SELECT part after AS. Returns set of (schema, view_name) that this view depends on.
    view_keys_by_name: optional map name_lower -> [(s, v), ...] for O(1) unqualified lookup when large.
    """
    refs: set[ViewKey] = set()
    for m in _EXTRACT_VIEW_REFS_PATTERN.finditer(body):
        ref = m.group(1).strip()
        if "." in ref:
            s, v = ref.split(".", 1)
            key = (s.strip().lower(), v.strip().lower())
        else:
            key = ("", ref.strip().lower())
        if key in view_keys:
            refs.add(key)
        else:
            if view_keys_by_name is not None:
                refs.update(view_keys_by_name.get(key[1], []))
            else:
                for (s, v) in view_keys:
                    if v == key[1]:
                        refs.add((s, v))
    return refs


def _body_from_create_view_ddl(ddl: str) -> str:
    """Return the SELECT body from CREATE VIEW ... AS <body>."""
    match = _BODY_FROM_DDL_PATTERN.search(ddl)
    return match.group(1).strip() if match else ""


def get_oracle_object_type(connection, owner: str, object_name: str) -> Optional[str]:
    """
    Return Oracle object type for the given owner.object_name: 'VIEW', 'TABLE', or None if not found.
    Uses ALL_OBJECTS (objects accessible to current user).
    """
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT object_type FROM all_objects
            WHERE owner = :owner AND object_name = :objname
              AND object_type IN ('VIEW', 'TABLE')
            AND ROWNUM = 1
            """,
            {"owner": owner.upper(), "objname": object_name.upper()},
        )
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        cursor.close()


def get_oracle_schema_objects(connection, owner: str) -> dict[str, str]:
    """
    Return dict of object_name_upper -> 'VIEW'|'TABLE' for all VIEW/TABLE in the given schema.
    One query per schema; use this to avoid N queries per view in qualify step.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT object_name, object_type FROM all_objects
            WHERE owner = :owner AND object_type IN ('VIEW', 'TABLE')
            """,
            {"owner": owner.upper()},
        )
        return {row[0].upper(): row[1] for row in cursor.fetchall()}
    finally:
        cursor.close()


def _find_from_clause_spans(body: str) -> list[tuple[int, int]]:
    """Return list of (start, end) spans for each FROM clause in body (for scoping comma-separated table refs)."""
    spans: list[tuple[int, int]] = []
    depth = 0
    in_single = False
    i = 0
    n = len(body)
    from_end_keywords = re.compile(
        r"\b(WHERE|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT)\b",
        re.IGNORECASE,
    )
    while i < n:
        c = body[i]
        if in_single:
            if c == "'" and (i + 1 >= n or body[i + 1] != "'"):
                in_single = False
            elif c == "'" and i + 1 < n and body[i + 1] == "'":
                i += 1
            i += 1
            continue
        if c == "'":
            in_single = True
            i += 1
            continue
        if c == "(":
            depth += 1
            i += 1
            continue
        if c == ")":
            depth -= 1
            i += 1
            continue
        if depth == 0:
            prev = i > 0 and (body[i - 1].isalnum() or body[i - 1] == "_")
            if not prev and re.match(r"FROM\b", body[i:], re.IGNORECASE):
                start = i
                i += 4  # skip "FROM"
                while i < n and body[i] in " \t\n":
                    i += 1
                scan_depth = depth
                scan_single = in_single
                scan = i
                while scan < n:
                    cc = body[scan]
                    if scan_single:
                        if cc == "'" and (scan + 1 >= n or body[scan + 1] != "'"):
                            scan_single = False
                        elif cc == "'" and scan + 1 < n and body[scan + 1] == "'":
                            scan += 1
                        scan += 1
                        continue
                    if cc == "'":
                        scan_single = True
                        scan += 1
                        continue
                    if cc == "(":
                        scan_depth += 1
                        scan += 1
                        continue
                    if cc == ")":
                        scan_depth -= 1
                        scan += 1
                        continue
                    if scan_depth == 0:
                        kw = from_end_keywords.match(body[scan:])
                        if kw:
                            spans.append((start, scan))
                            i = scan
                            break
                    scan += 1
                else:
                    spans.append((start, n))
                    break
        i += 1
    return spans


def _get_unqualified_table_refs(body: str) -> set[str]:
    """Return set of unqualified identifiers used as table/view in FROM and JOIN (no dot in name).
    Includes comma-separated tables: FROM t1, t2, t3."""
    refs: set[str] = set()
    for m in _UNQUALIFIED_TABLE_REFS_PATTERN.finditer(body):
        name = m.group(1).strip()
        if "." not in name:
            refs.add(name)
    for start, end in _find_from_clause_spans(body):
        from_part = body[start:end]
        for m in _UNQUALIFIED_TABLE_REFS_COMMA_PATTERN.finditer(from_part):
            name = m.group(1).strip()
            if "." not in name:
                refs.add(name)
    return refs


def _qualify_unqualified_refs_in_body(
    body: str,
    view_schema: str,
    connection,
    schema_objects_cache: Optional[dict[str, dict[str, str]]] = None,
    synonym_map: Optional[dict[str, str]] = None,
) -> str:
    """
    Qualify unqualified table/view refs in body using Oracle object type and APPS/APPS2 rules.
    Skip qualification for refs that exist as PUBLIC synonyms (or as view/synonym in source);
    those stay unqualified so dependency resolution can convert and execute them first.
    Priority: first check if ref exists in view_schema; only apply APPS/APPS2 prefix rules if not found.
    - If ref is in synonym_map (PUBLIC synonym): leave unqualified (dependency resolution handles it)
    - Else if ref exists in view_schema (VIEW or TABLE): use view_schema.ref
    - Else if view_schema is APPS and ref found in prefix schema (chars before first _): use prefix.ref
    - Else if view_schema is APPS2 and ref found in prefix2 schema: use prefix2.ref
    - Otherwise leave unqualified.
    When schema_objects_cache is provided, uses it instead of per-ref Oracle queries.
    """
    view_schema_upper = (view_schema or "").strip().upper()
    unqualified = _get_unqualified_table_refs(body)
    if not unqualified:
        return body

    synonym_map = synonym_map or {}
    schema_objs = (schema_objects_cache or {}).get(view_schema_upper)
    replacement_map: dict[str, str] = {}
    for ref in unqualified:
        ref_upper = (ref or "").upper()
        # Skip qualification if ref exists as PUBLIC synonym (or PUBLIC.synonym_name)
        # Dependency resolution will convert and execute that first
        if synonym_map.get(ref_upper) or synonym_map.get(f"PUBLIC.{ref_upper}"):
            continue
        if schema_objs is not None:
            obj_type = schema_objs.get(ref_upper)
        else:
            obj_type = get_oracle_object_type(connection, view_schema_upper, ref_upper)
        if obj_type:
            # Ref exists in view_schema (VIEW or TABLE) -> use view_schema.ref
            qual = f"{view_schema_upper.lower()}.{ref}"
            replacement_map[ref] = qual
        elif view_schema_upper == "APPS":
            prefix = ref.split("_", 1)[0] if "_" in ref else ref
            prefix_u = prefix.upper()
            prefix_objs = (schema_objects_cache or {}).get(prefix_u) if schema_objects_cache else None
            if prefix_objs is not None:
                prefix_type = prefix_objs.get(ref_upper)
            else:
                prefix_type = get_oracle_object_type(connection, prefix_u, ref_upper) if connection else None
            if prefix_type == "TABLE" or prefix_type == "VIEW":
                qual = f"{prefix.lower()}.{ref}"
                replacement_map[ref] = qual
        elif view_schema_upper == "APPS2":
            prefix = ref.split("_", 1)[0] if "_" in ref else ref
            prefix2_u = (prefix.upper() + "2")
            prefix2_objs = (schema_objects_cache or {}).get(prefix2_u) if schema_objects_cache else None
            if prefix2_objs is not None:
                prefix2_type = prefix2_objs.get(ref_upper)
            else:
                prefix2_type = get_oracle_object_type(connection, prefix2_u, ref_upper) if connection else None
            if prefix2_type == "TABLE" or prefix2_type == "VIEW":
                qual = f"{prefix.lower()}2.{ref}"
                replacement_map[ref] = qual

    if not replacement_map:
        return body

    # Replace only in FROM/JOIN position: (FROM|JOIN) + whitespace + identifier (case-insensitive match for ref)
    ref_lower_to_qual: dict[str, str] = {k.lower(): v for k, v in replacement_map.items()}

    def from_join_replacer(m: re.Match) -> str:
        ref = m.group(2)
        qual = ref_lower_to_qual.get(ref.lower()) or replacement_map.get(ref)
        if qual is not None:
            return m.group(1) + " " + qual
        return m.group(0)

    def comma_replacer(m: re.Match) -> str:
        ref = m.group(2)
        qual = ref_lower_to_qual.get(ref.lower()) or replacement_map.get(ref)
        if qual is not None:
            return m.group(1) + qual
        return m.group(0)

    body = _QUALIFY_FROM_JOIN_PATTERN.sub(from_join_replacer, body)
    spans = _find_from_clause_spans(body)
    for start, end in reversed(spans):
        from_part = body[start:end]
        new_from = _QUALIFY_COMMA_TABLE_PATTERN.sub(comma_replacer, from_part)
        if new_from != from_part:
            body = body[:start] + new_from + body[end:]
    return body


def apply_qualify_unqualified_refs(
    pg_sql: str,
    view_schema: str,
    connection,
    schema_objects_cache: Optional[dict[str, dict[str, str]]] = None,
    synonym_map: Optional[dict[str, str]] = None,
) -> str:
    """
    Final step: qualify unqualified tables/views in the view body using Oracle object type and APPS/APPS2 rules.
    Skips qualification for refs that exist as PUBLIC synonyms (dependency resolution handles them first).
    Returns updated pg_sql (CREATE OR REPLACE VIEW ... AS body).
    Preserves explicit column list (col1, col2) when present.
    When schema_objects_cache is provided, qualification uses it (no per-ref Oracle queries).
    """
    create_match = _NORMALIZE_CREATE_VIEW_PATTERN.search(pg_sql)
    if not create_match:
        return pg_sql
    view_name_in_ddl = create_match.group(1).strip()
    column_list_raw = create_match.group(2)
    body = create_match.group(3).rstrip().rstrip(";").rstrip()
    body = _qualify_unqualified_refs_in_body(
        body, view_schema, connection, schema_objects_cache, synonym_map,
    )
    column_list_suffix = f" {column_list_raw}" if column_list_raw else " "
    return f"CREATE OR REPLACE VIEW {view_name_in_ddl}{column_list_suffix}AS\n{body}\n;"


def topological_sort(
    views: list[tuple[str, str]],
    deps: list[set[ViewKey]],
) -> list[int]:
    """
    Return indices so that dependencies come first.
    deps[i] = set of (schema, view_name) that view i depends on (from our view list).
    On cycle, append remaining in original order and warn.
    Uses O(n) lookup dicts instead of O(n^3) nested loops.
    """
    n = len(views)
    if n == 0:
        return []

    # Build (schema_lower, name_lower) -> list[index] for O(1) dep resolution
    key_to_indices: dict[tuple[str, str], list[int]] = {}
    name_to_indices: dict[str, list[int]] = {}  # for unqualified lookups
    for j in range(n):
        sj = (views[j][0] or "").lower()
        vj = (views[j][1] or "").lower()
        key_to_indices.setdefault((sj, vj), []).append(j)
        name_to_indices.setdefault(vj, []).append(j)

    # Build adjacency: dep_indices[i] = set of indices i depends on
    dep_indices: list[set[int]] = [set() for _ in range(n)]
    # Also build reverse adjacency for O(1) in-degree updates
    reverse_deps: list[set[int]] = [set() for _ in range(n)]
    for i in range(n):
        for (ds, dv) in deps[i]:
            resolved: list[int] = key_to_indices.get((ds, dv), [])
            if not resolved and not ds:
                resolved = name_to_indices.get(dv, [])
            for j in resolved:
                if j != i:
                    dep_indices[i].add(j)
                    reverse_deps[j].add(i)

    # Kahn's algorithm with O(n) queue
    in_degree = [len(dep_indices[i]) for i in range(n)]
    queue_list = [i for i in range(n) if in_degree[i] == 0]
    order: list[int] = []
    qi = 0
    while qi < len(queue_list):
        i = queue_list[qi]
        qi += 1
        order.append(i)
        for j in reverse_deps[i]:
            in_degree[j] -= 1
            if in_degree[j] == 0:
                queue_list.append(j)

    if len(order) < n:
        remaining = [i for i in range(n) if in_degree[i] > 0]
        order.extend(remaining)
        print("  Warning: circular dependency among views; order may be wrong.", file=sys.stderr, flush=True)

    return order if order else list(range(n))


def ensure_pg_schema(connection, schema: str) -> bool:
    """Create schema in PostgreSQL if it does not exist. Returns True on success."""
    if not schema or not schema.strip():
        return True
    try:
        from psycopg2 import sql
        with connection.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema.strip())))
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        return False


# ---------------------------------------------------------------------------
# Auto-remove missing columns
# ---------------------------------------------------------------------------

_COL_NOT_EXIST_RE = re.compile(
    r'column\s+(?:(?:"([^"]+)")|(\S+?)\.("?([^"]+)"?))\s+does\s+not\s+exist',
    re.IGNORECASE,
)


def _parse_missing_column(err: str) -> Optional[tuple[str, str]]:
    """Extract (table_or_alias, column_name) from a PG 'column X does not exist' error.

    Returns (qualifier, col) for ``por.release_num`` style errors, or
    ('', col) for unqualified ``release_num`` style errors.
    Returns None if the error is not a missing-column error.

    Handles multiple PostgreSQL error formats:
    - column por.release_num does not exist
    - column "release_num" does not exist
    - column "release_num" of relation "my_table" does not exist
    - column por."release_num" does not exist
    """
    m = _COL_NOT_EXIST_RE.search(err)
    if m:
        if m.group(1):
            return ("", m.group(1))
        qualifier = m.group(2) or ""
        col = m.group(4) or m.group(3) or ""
        return (qualifier, col)
    # Handle: column "col" of relation "table" does not exist
    m_of = re.search(
        r'column\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s+of\s+relation\s+"?[a-zA-Z_][a-zA-Z0-9_.]*"?\s+does\s+not\s+exist',
        err, re.IGNORECASE,
    )
    if m_of:
        return ("", m_of.group(1))
    # Simpler form: column "release_num" does not exist
    m2 = re.search(r'column\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s+does\s+not\s+exist', err, re.IGNORECASE)
    if m2:
        return ("", m2.group(1))
    return None


def _remove_column_references(sql: str, qualifier: str, col: str) -> Optional[str]:
    """Remove all references to qualifier.col (or bare col) from a CREATE VIEW SQL.

    Handles removal from:
    - SELECT list items (whole ``expr AS alias`` items)
    - WHERE/AND/OR conditions containing the column
    - ON join conditions containing the column
    - Column list in CREATE VIEW ... (col1, col2, ...) AS ...

    Returns the modified SQL, or None if no changes were made.
    """
    if not col:
        return None

    original = sql

    # Build patterns that match the qualified or bare column reference
    q_col = qualifier.strip().strip('"')
    c_col = col.strip().strip('"')
    if q_col:
        # Match: qualifier.col  or  "qualifier"."col"  or  qualifier."col"
        col_pat = (
            r'(?:'
            + re.escape(q_col) + r'|"' + re.escape(q_col) + r'"'
            + r')\s*\.\s*(?:'
            + re.escape(c_col) + r'|"' + re.escape(c_col) + r'"'
            + r')'
        )
    else:
        # Unqualified: match bare col or "col", optionally with any qualifier prefix
        col_pat = (
            r'(?:\b[a-zA-Z_][a-zA-Z0-9_]*\s*\.\s*)?(?:'
            + re.escape(c_col) + r'|"' + re.escape(c_col) + r'"'
            + r')'
        )

    # ---- 1. Remove from SELECT list ----
    # Strategy: find SELECT items that reference the column and remove the whole item.
    # A SELECT item is delimited by commas at the top level (not inside parens).
    # We work on each SELECT...FROM block, including those in UNION/INTERSECT/EXCEPT.
    create_match = re.match(
        r"(CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\S+\s*(?:\([^)]*\)\s*)?AS\s+)(.*)",
        sql, re.IGNORECASE | re.DOTALL,
    )
    if not create_match:
        return None
    header = create_match.group(1)
    body = create_match.group(2)

    col_check = re.compile(r'\b' + col_pat + r'\b', re.IGNORECASE)

    def _remove_col_from_select_block(text: str, block_start: int) -> tuple[str, bool]:
        """Remove column references from one SELECT...FROM block.

        *text* is the full body, *block_start* is the position of the SELECT keyword.
        Returns (new_text, changed).
        """
        select_match = re.match(r"(SELECT\s+(?:DISTINCT\s+)?)", text[block_start:], re.IGNORECASE)
        if not select_match:
            return text, False
        items_start = block_start + select_match.end()

        # Walk forward to find top-level commas and the FROM keyword
        depth = 0
        comma_positions: list[int] = []
        from_pos = None
        text_upper = text.upper()
        i = items_start
        while i < len(text):
            ch = text[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                comma_positions.append(i)
            elif depth == 0 and text_upper[i:i+5] in (' FROM', '\nFROM', '\tFROM'):
                if i + 5 < len(text) and not text[i+5:i+6].isalnum() and text[i+5:i+6] != '_':
                    from_pos = i + 1 if text[i] in (' ', '\n', '\t') else i
                    break
                elif i + 5 >= len(text):
                    from_pos = i + 1 if text[i] in (' ', '\n', '\t') else i
                    break
            i += 1
        if from_pos is None:
            m_from = re.search(r'\bFROM\b', text[items_start:], re.IGNORECASE)
            if m_from:
                from_pos = items_start + m_from.start()
        if from_pos is None:
            return text, False

        # Split into items
        boundaries = [items_start] + [c for c in comma_positions if items_start <= c < from_pos] + [from_pos]
        items = []
        for j in range(len(boundaries) - 1):
            start = boundaries[j]
            end = boundaries[j + 1]
            if j > 0:
                item_text = text[start + 1:end].strip().rstrip(',').strip()
            else:
                item_text = text[start:end].strip().rstrip(',').strip()
            items.append(item_text)

        new_items = []
        removed = False
        for item_text in items:
            # Skip items already replaced with NULL (avoids infinite loop when
            # the column name appears in the preserved AS alias, e.g.
            # "NULL AS total_revenue" still matches col_check for "total_revenue").
            if re.match(r'^NULL(?:\s+AS\s+\S+)?$', item_text.strip(), re.IGNORECASE):
                new_items.append(item_text)
                continue
            if col_check.search(item_text):
                removed = True
                log.debug("[COL-REMOVE] Replacing SELECT item with NULL: %s", item_text[:100])
                # Preserve alias if present (e.g. "ra.bad_col AS col_1" -> "NULL AS col_1")
                alias_m = re.search(r'\bAS\s+(\S+)\s*$', item_text, re.IGNORECASE)
                if alias_m:
                    new_items.append(f"NULL AS {alias_m.group(1)}")
                else:
                    new_items.append("NULL")
            else:
                new_items.append(item_text)

        if removed:
            after_from = text[from_pos:]
            new_text = text[:items_start] + ", ".join(new_items) + " " + after_from
            return new_text, True
        return text, False

    # Find ALL top-level SELECT keywords (handles UNION/INTERSECT/EXCEPT)
    _select_re = re.compile(r'\bSELECT\b', re.IGNORECASE)
    any_removed = False
    search_from = 0
    while True:
        m_sel = _select_re.search(body, search_from)
        if not m_sel:
            break
        # Skip if inside parentheses (subquery)
        depth = 0
        for ch in body[:m_sel.start()]:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
        if depth > 0:
            search_from = m_sel.end()
            continue
        body, changed = _remove_col_from_select_block(body, m_sel.start())
        if changed:
            any_removed = True
            # Don't advance — positions shifted, rescan from same spot
        else:
            search_from = m_sel.end()

    if any_removed:
        sql = header + body

    # ---- 2. Remove from WHERE/AND/OR conditions ----
    # Use parenthesis-aware matching to handle commas inside function args
    # (e.g. AND func(a, b) = col  should not break at the comma).
    col_check = re.compile(r'\b' + col_pat + r'\b', re.IGNORECASE)

    # Helper: find the end of a condition (balance parens, stop at AND/OR/keywords)
    _cond_end_kw = re.compile(
        r'\b(?:AND|OR|ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT|LIMIT|WINDOW)\b',
        re.IGNORECASE,
    )
    def _find_cond_end(text: str, start: int) -> int:
        """Return the position after the condition starting at *start*, respecting parentheses."""
        depth = 0
        pos = start
        while pos < len(text):
            ch = text[pos]
            if ch == '(':
                depth += 1
            elif ch == ')':
                if depth == 0:
                    return pos  # stop before unmatched ')'
                depth -= 1
            elif depth == 0:
                m_kw = _cond_end_kw.match(text, pos)
                if m_kw:
                    return pos
            pos += 1
        return pos

    # Remove if it's the FIRST (or only) condition after WHERE
    where_first = re.compile(r'(\bWHERE\s+)', re.IGNORECASE)
    for m_w in list(where_first.finditer(sql)):
        cond_start = m_w.end()
        cond_end = _find_cond_end(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if col_check.search(cond_text):
            # Check if followed by AND/OR (more conditions) — keep WHERE keyword
            after = sql[cond_end:].lstrip()
            if re.match(r'\b(?:AND|OR)\s+', after, re.IGNORECASE):
                # Remove the first condition and the following AND/OR
                and_or_m = re.match(r'\s*\b(?:AND|OR)\s+', sql[cond_end:], re.IGNORECASE)
                remove_end = cond_end + (and_or_m.end() if and_or_m else 0)
                sql = sql[:m_w.end()] + sql[remove_end:]
            else:
                # Only condition — remove entire WHERE clause
                sql = sql[:m_w.start()] + sql[cond_end:]
            log.debug("[COL-REMOVE] Removed WHERE condition: %s", cond_text.strip()[:120])
            break  # re-scan would need updated positions; one pass is sufficient

    # Remove "AND/OR <condition_with_col>" in WHERE/ON/HAVING clauses.
    # Only search after FROM to avoid matching OR in "CREATE OR REPLACE".
    from_anchor = re.search(r'\bFROM\b', sql, re.IGNORECASE)
    cond_leader = re.compile(r'\b(AND|OR)\s+', re.IGNORECASE)
    search_pos = from_anchor.start() if from_anchor else 0
    while True:
        m_lead = cond_leader.search(sql, search_pos)
        if not m_lead:
            break
        cond_start = m_lead.end()
        cond_end = _find_cond_end(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if col_check.search(cond_text):
            sql = sql[:m_lead.start()] + sql[cond_end:]
            log.debug("[COL-REMOVE] Removed AND/OR condition: %s", cond_text.strip()[:120])
            # don't advance — re-scan from same position
        else:
            search_pos = cond_end

    # ---- 3. Remove from ON join conditions ----
    # Handle both "ON col = expr" (first condition) and "AND col = expr" (additional)
    on_leader = re.compile(r'\b(ON)\s+', re.IGNORECASE)
    for m_on in list(on_leader.finditer(sql)):
        cond_start = m_on.end()
        cond_end = _find_cond_end(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if col_check.search(cond_text):
            after = sql[cond_end:].lstrip()
            if re.match(r'\b(?:AND)\s+', after, re.IGNORECASE):
                # First ON condition with more — remove condition + AND, keep ON
                and_m = re.match(r'\s*\bAND\s+', sql[cond_end:], re.IGNORECASE)
                remove_end = cond_end + (and_m.end() if and_m else 0)
                sql = sql[:m_on.end()] + sql[remove_end:]
            else:
                # Only ON condition — replace with ON TRUE
                sql = sql[:m_on.end()] + 'TRUE' + sql[cond_end:]
            log.debug("[COL-REMOVE] Removed ON condition: %s", cond_text.strip()[:120])
            break
    # Also remove "AND col_ref = ..." inside ON clauses
    on_and = re.compile(r'\s+AND\s+', re.IGNORECASE)
    search_pos = 0
    while True:
        m_a = on_and.search(sql, search_pos)
        if not m_a:
            break
        cond_start = m_a.end()
        cond_end = _find_cond_end(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if col_check.search(cond_text):
            sql = sql[:m_a.start()] + sql[cond_end:]
            log.debug("[COL-REMOVE] Removed AND-in-ON condition: %s", cond_text.strip()[:120])
        else:
            search_pos = cond_end

    # ---- 4. Remove from column list in CREATE VIEW (...) AS ----
    # CREATE OR REPLACE VIEW name (col1, col2, bad_col, col3) AS ...
    col_list_pat = re.compile(
        r'(CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\S+\s*)\(([^)]+)\)(\s+AS\b)',
        re.IGNORECASE,
    )
    m_cl = col_list_pat.search(sql)
    if m_cl:
        cols_str = m_cl.group(2)
        cols = [c.strip() for c in cols_str.split(',')]
        bare_col_re = re.compile(r'\b' + re.escape(c_col) + r'\b', re.IGNORECASE)
        new_cols = [c for c in cols if not bare_col_re.search(c)]
        if len(new_cols) < len(cols):
            if new_cols:
                sql = sql[:m_cl.start()] + m_cl.group(1) + '(' + ', '.join(new_cols) + ')' + m_cl.group(3) + sql[m_cl.end():]
            else:
                # Remove the entire column list
                sql = sql[:m_cl.start()] + m_cl.group(1) + m_cl.group(3) + sql[m_cl.end():]
            log.debug("[COL-REMOVE] Removed column from view column list: %s", c_col)

    # ---- 5. Clean up artifacts ----
    # Remove empty WHERE clauses
    sql = re.sub(r'\bWHERE\s*(?=\s*(?:ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT|LIMIT|;|\Z|\)))',
                 '', sql, flags=re.IGNORECASE)
    # Remove double AND/OR
    sql = re.sub(r'\b(AND|OR)\s+(AND|OR)\b', r'\1', sql, flags=re.IGNORECASE)
    # Clean double commas in SELECT
    while ',,' in sql or ', ,' in sql:
        sql = sql.replace(',,', ',').replace(', ,', ',')
    # Remove leading/trailing commas in SELECT
    sql = re.sub(r'(SELECT\s+(?:DISTINCT\s+)?)\s*,', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r',\s*(\bFROM\b)', r' \1', sql, flags=re.IGNORECASE)
    # Normalize whitespace
    sql = re.sub(r'  +', ' ', sql)

    if sql.strip() == original.strip():
        return None
    return sql


_AUTO_REMOVE_MAX_RETRIES = 500


def _replace_ctid_with_null(sql: str, qualifier: str) -> Optional[str]:
    """Replace ``qualifier.ctid`` (or bare ``ctid``) with ``NULL::text`` in SQL.

    Oracle ROWID is converted to ``ctid`` during transpilation, but PostgreSQL's
    ``ctid`` only works on base tables — not through views.  Instead of removing
    the column entirely (which changes the view's column count), this replaces
    the reference with ``NULL::text`` so the column position is preserved.

    Returns the modified SQL, or None if no replacement was made.
    """
    original = sql
    if qualifier:
        # Replace qualifier.ctid with NULL::text
        pat = re.compile(
            r'\b' + re.escape(qualifier) + r'\s*\.\s*ctid\b',
            re.IGNORECASE,
        )
    else:
        # Replace bare ctid with NULL::text (but not part of a larger identifier)
        pat = re.compile(r'\bctid\b', re.IGNORECASE)

    sql = pat.sub('NULL::text', sql)
    if sql.strip() == original.strip():
        return None
    return sql


# ---------------------------------------------------------------------------
# Auto-remove missing functions
# ---------------------------------------------------------------------------

_FUNC_NOT_EXIST_RE = re.compile(
    r'function\s+((?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)\s*\([^)]*\)\s+does\s+not\s+exist',
    re.IGNORECASE,
)


def _parse_missing_function(err: str) -> Optional[str]:
    """Extract the function name from a PG 'function X does not exist' error.

    Returns the fully-qualified or bare function name (e.g. ``apps.constraint_max_sequence``),
    or None if the error is not a missing-function error.

    Handles formats:
    - function apps.constraint_max_sequence(numeric) does not exist
    - function my_func(integer, text) does not exist
    """
    m = _FUNC_NOT_EXIST_RE.search(err)
    if m:
        return m.group(1)
    # Fallback: function "name"(...) does not exist
    m2 = re.search(
        r'function\s+"?([a-zA-Z_][a-zA-Z0-9_.]*)"?\s*\([^)]*\)\s+does\s+not\s+exist',
        err, re.IGNORECASE,
    )
    if m2:
        return m2.group(1)
    return None


def _replace_function_call_with_null(text: str, func_name: str) -> str:
    """Replace all occurrences of func_name(...) with NULL in *text*.

    Handles nested parentheses. func_name may be schema.func or bare func.
    """
    parts = func_name.rsplit(".", 1)
    if len(parts) == 2:
        schema_part, bare_func = parts
        func_start_pat = (
            r'(?:' + re.escape(schema_part) + r'\s*\.\s*)?' + re.escape(bare_func) + r'\s*\('
        )
    else:
        bare_func = parts[0]
        func_start_pat = (
            r'(?:[a-zA-Z_][a-zA-Z0-9_]*\s*\.\s*)?' + re.escape(bare_func) + r'\s*\('
        )
    func_start_re = re.compile(r'\b' + func_start_pat, re.IGNORECASE)

    result = []
    i = 0
    while i < len(text):
        m = func_start_re.search(text, i)
        if not m:
            result.append(text[i:])
            break
        result.append(text[i:m.start()])
        result.append("NULL")
        # Find matching closing paren
        depth = 1
        j = m.end()
        in_single = False
        while j < len(text) and depth > 0:
            c = text[j]
            if in_single:
                if c == "'" and (j + 1 >= len(text) or text[j + 1] != "'"):
                    in_single = False
                elif c == "'" and j + 1 < len(text) and text[j + 1] == "'":
                    j += 1
                j += 1
                continue
            if c == "'":
                in_single = True
                j += 1
                continue
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            j += 1
        i = j
    return "".join(result)


def _select_item_has_explicit_alias(item: str) -> bool:
    """True if the select item has AS alias or trailing implicit alias."""
    item = item.strip().rstrip()
    if re.search(r'\bAS\s+["\w]', item, re.IGNORECASE):
        return True
    m = re.search(r'\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$', item)
    if m:
        tail = m.group(1).upper()
        if tail not in ("END", "NULL", "TRUE", "FALSE", "CASE"):
            return True
    return False


def _find_all_select_list_bounds(body: str) -> list[tuple[int, int]]:
    """Return list of (start, end) for each top-level SELECT list (handles UNION/INTERSECT/EXCEPT)."""
    bounds_list: list[tuple[int, int]] = []
    search_from = 0
    _sel_re = re.compile(r"\bSELECT\s+", re.IGNORECASE)
    while True:
        m = _sel_re.search(body, search_from)
        if not m:
            break
        start = m.end()
        depth = 0
        in_single = False
        i = start
        n = len(body)
        while i < n:
            c = body[i]
            if in_single:
                if c == "'" and (i + 1 >= n or body[i + 1] != "'"):
                    in_single = False
                elif c == "'" and i + 1 < n and body[i + 1] == "'":
                    i += 1
                i += 1
                continue
            if c == "'":
                in_single = True
                i += 1
                continue
            if c == "(":
                depth += 1
                i += 1
                continue
            if c == ")":
                depth -= 1
                i += 1
                continue
            if depth == 0:
                prev_is_word = i > 0 and (body[i - 1].isalnum() or body[i - 1] == "_")
                if not prev_is_word and re.match(r"FROM\b", body[i:], re.IGNORECASE):
                    end = i
                    while end > start and body[end - 1] in " \t":
                        end -= 1
                    bounds_list.append((start, end))
                    search_from = i
                    break
            i += 1
        else:
            break
    return bounds_list


def _replace_function_with_null_and_alias(sql: str, func_name: str) -> Optional[str]:
    """Replace all function calls with NULL and add alias if the column has none.

    When a PostgreSQL error reports 'function X does not exist', this replaces
    each func_name(...) with NULL everywhere (SELECT, WHERE, ON, subqueries, UNION).
    For SELECT list items that become NULL without an alias, adds AS <alias>.
    """
    if not func_name:
        return None

    bare_func = func_name.rsplit(".", 1)[-1]
    alias_base = re.sub(r'[^a-zA-Z0-9_]', '_', bare_func)
    if not alias_base or alias_base[0].isdigit():
        alias_base = "col_" + alias_base
    alias_base = _quote_alias_if_reserved(alias_base)

    create_match = re.match(
        r"(CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\S+\s*(?:\([^)]*\)\s*)?AS\s+)(.*)",
        sql, re.IGNORECASE | re.DOTALL,
    )
    if not create_match:
        return None
    header = create_match.group(1)
    body = create_match.group(2)

    # Step 1: Global replace of func(...) with NULL everywhere (SELECT, WHERE, ON, subqueries, UNION)
    body = _replace_function_call_with_null(body, func_name)
    if body == create_match.group(2):
        return None

    # Step 2: Add aliases for SELECT items that are NULL-like without explicit alias
    bounds_list = _find_all_select_list_bounds(body)
    alias_counter = [0]

    def fix_select_list(select_text: str) -> str:
        parts = _split_top_level_commas(select_text)
        new_parts = []
        for part in parts:
            stripped = part.strip()
            needs_alias = (
                not _select_item_has_explicit_alias(stripped)
                and re.search(r'\bNULL\b', stripped, re.IGNORECASE)
            )
            if needs_alias:
                alias_counter[0] += 1
                suffix = f"_{alias_counter[0]}" if alias_counter[0] > 1 else ""
                new_parts.append(f"{stripped} AS {alias_base}{suffix}")
            else:
                new_parts.append(stripped)
        return ", ".join(new_parts)

    if bounds_list:
        new_body_parts = []
        prev_end = 0
        for start, end in bounds_list:
            new_body_parts.append(body[prev_end:start])
            new_body_parts.append(fix_select_list(body[start:end]))
            prev_end = end
        new_body_parts.append(body[prev_end:])
        body = "".join(new_body_parts)

    return header + body


def _remove_function_references(sql: str, func_name: str) -> Optional[str]:
    """Remove all references to *func_name* from a CREATE VIEW SQL.

    Similar to ``_remove_column_references`` but matches function calls
    (``func_name(...)`` or ``schema.func_name(...)``) instead of column refs.

    Handles removal from:
    - SELECT list items containing the function call
    - WHERE/AND/OR conditions containing the function call
    - ON join conditions containing the function call
    - Column list in CREATE VIEW ... (col1, col2, ...) AS ... when the alias
      is derived from the function name

    Returns the modified SQL, or None if no changes were made.
    """
    if not func_name:
        return None

    original = sql

    # Build a pattern that matches the function call: func_name( or schema.func_name(
    # Handle both qualified (apps.func_name) and bare (func_name)
    parts = func_name.rsplit(".", 1)
    if len(parts) == 2:
        schema_part, bare_func = parts
        # Match: schema.func( or func( (for flexibility)
        func_pat = (
            r'(?:(?:' + re.escape(schema_part) + r'\s*\.\s*)?' +
            re.escape(bare_func) + r')\s*\('
        )
    else:
        bare_func = parts[0]
        # Match: any_qualifier.func( or bare func(
        func_pat = (
            r'(?:[a-zA-Z_][a-zA-Z0-9_]*\s*\.\s*)?' +
            re.escape(bare_func) + r'\s*\('
        )

    func_check = re.compile(r'\b' + func_pat, re.IGNORECASE)

    # ---- 1. Remove from SELECT list ----
    create_match = re.match(
        r"(CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\S+\s*(?:\([^)]*\)\s*)?AS\s+)(.*)",
        sql, re.IGNORECASE | re.DOTALL,
    )
    if not create_match:
        return None
    header = create_match.group(1)
    body = create_match.group(2)

    select_match = re.match(r"(\s*SELECT\s+(?:DISTINCT\s+)?)", body, re.IGNORECASE)
    if not select_match:
        return None

    # Find all top-level commas and the FROM keyword to split SELECT items
    depth = 0
    items_start = select_match.end()
    comma_positions = []
    from_pos = None
    i = items_start
    body_upper = body.upper()
    while i < len(body):
        ch = body[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif ch == ',' and depth == 0:
            comma_positions.append(i)
        elif depth == 0 and body_upper[i:i+5] in (' FROM', '\nFROM', '\tFROM'):
            if i + 5 < len(body) and not body[i+5:i+6].isalnum() and body[i+5:i+6] != '_':
                from_pos = i + 1 if body[i] in (' ', '\n', '\t') else i
                break
            elif i + 5 >= len(body):
                from_pos = i + 1 if body[i] in (' ', '\n', '\t') else i
                break
        i += 1

    if from_pos is None:
        m_from = re.search(r'\bFROM\b', body[items_start:], re.IGNORECASE)
        if m_from:
            from_pos = items_start + m_from.start()

    if from_pos is not None:
        select_part = body[items_start:from_pos]
        after_from = body[from_pos:]

        boundaries = [items_start] + [c for c in comma_positions if items_start <= c < from_pos] + [from_pos]

        items = []
        for j in range(len(boundaries) - 1):
            start = boundaries[j]
            end = boundaries[j + 1]
            item_text = body[start:end].strip().rstrip(',').strip()
            if j > 0:
                item_text = body[boundaries[j]+1:boundaries[j+1]].strip().rstrip(',').strip()
            items.append((start, end, item_text))

        kept_items = []
        removed = False
        for start, end, item_text in items:
            if func_check.search(item_text):
                removed = True
                log.debug("[FUNC-REMOVE] Removing SELECT item: %s", item_text[:100])
            else:
                kept_items.append(item_text)

        if removed and kept_items:
            new_select = ", ".join(kept_items)
            body = body[:items_start] + new_select + " " + after_from
            sql = header + body

    # ---- 2. Remove from WHERE/AND/OR conditions ----
    _cond_end_kw = re.compile(
        r'\b(?:AND|OR|ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT|LIMIT|WINDOW)\b',
        re.IGNORECASE,
    )
    def _find_cond_end_f(text: str, start: int) -> int:
        depth = 0
        pos = start
        while pos < len(text):
            ch = text[pos]
            if ch == '(':
                depth += 1
            elif ch == ')':
                if depth == 0:
                    return pos
                depth -= 1
            elif depth == 0:
                m_kw = _cond_end_kw.match(text, pos)
                if m_kw:
                    return pos
            pos += 1
        return pos

    # First (or only) WHERE condition
    where_first = re.compile(r'(\bWHERE\s+)', re.IGNORECASE)
    for m_w in list(where_first.finditer(sql)):
        cond_start = m_w.end()
        cond_end = _find_cond_end_f(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if func_check.search(cond_text):
            after = sql[cond_end:].lstrip()
            if re.match(r'\b(?:AND|OR)\s+', after, re.IGNORECASE):
                and_or_m = re.match(r'\s*\b(?:AND|OR)\s+', sql[cond_end:], re.IGNORECASE)
                remove_end = cond_end + (and_or_m.end() if and_or_m else 0)
                sql = sql[:m_w.end()] + sql[remove_end:]
            else:
                sql = sql[:m_w.start()] + sql[cond_end:]
            log.debug("[FUNC-REMOVE] Removed WHERE condition: %s", cond_text.strip()[:120])
            break

    # AND/OR conditions after FROM
    from_anchor = re.search(r'\bFROM\b', sql, re.IGNORECASE)
    cond_leader = re.compile(r'\b(AND|OR)\s+', re.IGNORECASE)
    search_pos = from_anchor.start() if from_anchor else 0
    while True:
        m_lead = cond_leader.search(sql, search_pos)
        if not m_lead:
            break
        cond_start = m_lead.end()
        cond_end = _find_cond_end_f(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if func_check.search(cond_text):
            sql = sql[:m_lead.start()] + sql[cond_end:]
            log.debug("[FUNC-REMOVE] Removed AND/OR condition: %s", cond_text.strip()[:120])
        else:
            search_pos = cond_end

    # ---- 3. Remove from ON join conditions ----
    on_leader = re.compile(r'\b(ON)\s+', re.IGNORECASE)
    for m_on in list(on_leader.finditer(sql)):
        cond_start = m_on.end()
        cond_end = _find_cond_end_f(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if func_check.search(cond_text):
            after = sql[cond_end:].lstrip()
            if re.match(r'\b(?:AND)\s+', after, re.IGNORECASE):
                and_m = re.match(r'\s*\bAND\s+', sql[cond_end:], re.IGNORECASE)
                remove_end = cond_end + (and_m.end() if and_m else 0)
                sql = sql[:m_on.end()] + sql[remove_end:]
            else:
                sql = sql[:m_on.end()] + 'TRUE' + sql[cond_end:]
            log.debug("[FUNC-REMOVE] Removed ON condition: %s", cond_text.strip()[:120])
            break

    # ---- 4. Remove from column list in CREATE VIEW (...) AS ----
    # If a column alias was derived from the function name, remove it from the column list
    col_list_pat = re.compile(
        r'(CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\S+\s*)\(([^)]+)\)(\s+AS\b)',
        re.IGNORECASE,
    )
    m_cl = col_list_pat.search(sql)
    if m_cl:
        cols_str = m_cl.group(2)
        cols = [c.strip() for c in cols_str.split(',')]
        bare_re = re.compile(r'\b' + re.escape(bare_func) + r'\b', re.IGNORECASE)
        new_cols = [c for c in cols if not bare_re.search(c)]
        if len(new_cols) < len(cols):
            if new_cols:
                sql = sql[:m_cl.start()] + m_cl.group(1) + '(' + ', '.join(new_cols) + ')' + m_cl.group(3) + sql[m_cl.end():]
            else:
                sql = sql[:m_cl.start()] + m_cl.group(1) + m_cl.group(3) + sql[m_cl.end():]
            log.debug("[FUNC-REMOVE] Removed function-derived column from view column list: %s", bare_func)

    # ---- 5. Clean up artifacts ----
    sql = re.sub(r'\bWHERE\s*(?=\s*(?:ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT|LIMIT|;|\Z|\)))',
                 '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\b(AND|OR)\s+(AND|OR)\b', r'\1', sql, flags=re.IGNORECASE)
    while ',,' in sql or ', ,' in sql:
        sql = sql.replace(',,', ',').replace(', ,', ',')
    sql = re.sub(r'(SELECT\s+(?:DISTINCT\s+)?)\s*,', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r',\s*(\bFROM\b)', r' \1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'  +', ' ', sql)

    if sql.strip() == original.strip():
        return None
    return sql


# ---------------------------------------------------------------------------
# Auto-remove missing FROM-clause entries (alias references)
# ---------------------------------------------------------------------------

_MISSING_FROM_RE = re.compile(
    r'missing\s+FROM-clause\s+entry\s+for\s+table\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?',
    re.IGNORECASE,
)


def _parse_missing_from_entry(err: str) -> Optional[str]:
    """Extract the table/alias name from a PG 'missing FROM-clause entry for table' error.

    Returns the table/alias name (e.g. ``glr03300_pkg``), or None if error
    does not match.

    Handles formats:
    - missing FROM-clause entry for table "glr03300_pkg"
    - missing FROM-clause entry for table glr03300_pkg
    """
    m = _MISSING_FROM_RE.search(err)
    if m:
        return m.group(1)
    return None


# ---------------------------------------------------------------------------
# Auto-remove missing relations (table/view does not exist)
# ---------------------------------------------------------------------------

_RELATION_NOT_EXIST_RE = re.compile(
    r'relation\s+"?([a-zA-Z_][a-zA-Z0-9_.]*)"?\s+does\s+not\s+exist',
    re.IGNORECASE,
)


def _parse_missing_relation(err: str) -> Optional[str]:
    """Extract the relation name from a PG 'relation X does not exist' error.

    Returns the relation name (e.g. ``ar_cons_inv`` or ``apps.ar_cons_inv``),
    or None if error does not match.

    Avoids matching 'column X of relation Y does not exist' (handled by
    ``_parse_missing_column``).

    Handles formats:
    - relation "ar_cons_inv" does not exist
    - relation "apps.ar_cons_inv" does not exist
    - relation ar_cons_inv does not exist
    """
    # Exclude the "column ... of relation ..." variant
    if re.search(r'column\s+.*\bof\s+relation\b', err, re.IGNORECASE):
        return None
    m = _RELATION_NOT_EXIST_RE.search(err)
    if m:
        return m.group(1)
    return None


def _remove_all_alias_references(sql: str, alias: str) -> Optional[str]:
    """Replace ALL references to *alias*.column_name with NULL in a CREATE VIEW SQL.

    When PostgreSQL says "missing FROM-clause entry for table X", it means
    the SQL references ``X.something`` but ``X`` is not in FROM/JOIN.
    This is common for Oracle package references (``PKG.func_call``)
    that survived conversion.

    Strategy:
    - In SELECT lists: replace ``alias.whatever`` and ``alias.whatever(...)``
      items with NULL (preserving AS alias if present)
    - In WHERE/ON/HAVING conditions: remove conditions containing ``alias.``
    - Also handle bare function-call style: alias.func(args)

    Returns the modified SQL, or None if no changes were made.
    """
    if not alias:
        return None

    original = sql

    esc_alias = re.escape(alias)
    # Pattern matching ANY reference to alias.something  (column or function call)
    alias_ref_pat = r'(?:' + esc_alias + r'|"' + esc_alias + r'")\s*\.\s*[a-zA-Z_][a-zA-Z0-9_]*'
    alias_check = re.compile(r'\b' + alias_ref_pat, re.IGNORECASE)

    # ---- 1. Remove from SELECT list ----
    create_match = re.match(
        r"(CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+\S+\s*(?:\([^)]*\)\s*)?AS\s+)(.*)",
        sql, re.IGNORECASE | re.DOTALL,
    )
    if not create_match:
        return None
    header = create_match.group(1)
    body = create_match.group(2)

    def _remove_alias_from_select_block(text: str, block_start: int) -> tuple[str, bool]:
        """Replace alias references in one SELECT...FROM block with NULL."""
        select_match = re.match(r"(SELECT\s+(?:DISTINCT\s+)?)", text[block_start:], re.IGNORECASE)
        if not select_match:
            return text, False
        items_start = block_start + select_match.end()

        # Walk forward to find top-level commas and the FROM keyword
        depth = 0
        comma_positions: list[int] = []
        from_pos = None
        text_upper = text.upper()
        i = items_start
        while i < len(text):
            ch = text[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                comma_positions.append(i)
            elif depth == 0 and text_upper[i:i+5] in (' FROM', '\nFROM', '\tFROM'):
                if i + 5 < len(text) and not text[i+5:i+6].isalnum() and text[i+5:i+6] != '_':
                    from_pos = i + 1 if text[i] in (' ', '\n', '\t') else i
                    break
                elif i + 5 >= len(text):
                    from_pos = i + 1 if text[i] in (' ', '\n', '\t') else i
                    break
            i += 1
        if from_pos is None:
            m_from = re.search(r'\bFROM\b', text[items_start:], re.IGNORECASE)
            if m_from:
                from_pos = items_start + m_from.start()
        if from_pos is None:
            return text, False

        # Split into items
        boundaries = [items_start] + [c for c in comma_positions if items_start <= c < from_pos] + [from_pos]
        items = []
        for j in range(len(boundaries) - 1):
            start = boundaries[j]
            end = boundaries[j + 1]
            if j > 0:
                item_text = text[start + 1:end].strip().rstrip(',').strip()
            else:
                item_text = text[start:end].strip().rstrip(',').strip()
            items.append(item_text)

        new_items = []
        removed = False
        for item_text in items:
            # Skip items already replaced with NULL to avoid infinite loop
            if re.match(r'^NULL(?:\s+AS\s+\S+)?$', item_text.strip(), re.IGNORECASE):
                new_items.append(item_text)
                continue
            if alias_check.search(item_text):
                removed = True
                log.debug("[ALIAS-REMOVE] Replacing SELECT item with NULL: %s", item_text[:100])
                alias_m = re.search(r'\bAS\s+(\S+)\s*$', item_text, re.IGNORECASE)
                if alias_m:
                    new_items.append(f"NULL AS {alias_m.group(1)}")
                else:
                    new_items.append("NULL")
            else:
                new_items.append(item_text)

        if removed:
            after_from = text[from_pos:]
            new_text = text[:items_start] + ", ".join(new_items) + " " + after_from
            return new_text, True
        return text, False

    # Find ALL top-level SELECT keywords (handles UNION/INTERSECT/EXCEPT)
    _select_re = re.compile(r'\bSELECT\b', re.IGNORECASE)
    any_removed = False
    search_from = 0
    while True:
        m_sel = _select_re.search(body, search_from)
        if not m_sel:
            break
        # Skip if inside parentheses (subquery)
        depth = 0
        for ch in body[:m_sel.start()]:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
        if depth > 0:
            search_from = m_sel.end()
            continue
        body, changed = _remove_alias_from_select_block(body, m_sel.start())
        if changed:
            any_removed = True
        else:
            search_from = m_sel.end()

    if any_removed:
        sql = header + body

    # ---- 2. Remove from WHERE/AND/OR conditions ----
    _cond_end_kw = re.compile(
        r'\b(?:AND|OR|ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT|LIMIT|WINDOW)\b',
        re.IGNORECASE,
    )
    def _find_cond_end_a(text: str, start: int) -> int:
        depth = 0
        pos = start
        while pos < len(text):
            ch = text[pos]
            if ch == '(':
                depth += 1
            elif ch == ')':
                if depth == 0:
                    return pos
                depth -= 1
            elif depth == 0:
                m_kw = _cond_end_kw.match(text, pos)
                if m_kw:
                    return pos
            pos += 1
        return pos

    # First (or only) WHERE condition
    where_first = re.compile(r'(\bWHERE\s+)', re.IGNORECASE)
    for m_w in list(where_first.finditer(sql)):
        cond_start = m_w.end()
        cond_end = _find_cond_end_a(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if alias_check.search(cond_text):
            after = sql[cond_end:].lstrip()
            if re.match(r'\b(?:AND|OR)\s+', after, re.IGNORECASE):
                and_or_m = re.match(r'\s*\b(?:AND|OR)\s+', sql[cond_end:], re.IGNORECASE)
                remove_end = cond_end + (and_or_m.end() if and_or_m else 0)
                sql = sql[:m_w.end()] + sql[remove_end:]
            else:
                sql = sql[:m_w.start()] + sql[cond_end:]
            log.debug("[ALIAS-REMOVE] Removed WHERE condition: %s", cond_text.strip()[:120])
            break

    # AND/OR conditions after FROM
    from_anchor = re.search(r'\bFROM\b', sql, re.IGNORECASE)
    cond_leader = re.compile(r'\b(AND|OR)\s+', re.IGNORECASE)
    search_pos = from_anchor.start() if from_anchor else 0
    while True:
        m_lead = cond_leader.search(sql, search_pos)
        if not m_lead:
            break
        cond_start = m_lead.end()
        cond_end = _find_cond_end_a(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if alias_check.search(cond_text):
            sql = sql[:m_lead.start()] + sql[cond_end:]
            log.debug("[ALIAS-REMOVE] Removed AND/OR condition: %s", cond_text.strip()[:120])
        else:
            search_pos = cond_end

    # ---- 3. Remove from ON join conditions ----
    on_leader = re.compile(r'\b(ON)\s+', re.IGNORECASE)
    for m_on in list(on_leader.finditer(sql)):
        cond_start = m_on.end()
        cond_end = _find_cond_end_a(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if alias_check.search(cond_text):
            after = sql[cond_end:].lstrip()
            if re.match(r'\b(?:AND)\s+', after, re.IGNORECASE):
                and_m = re.match(r'\s*\bAND\s+', sql[cond_end:], re.IGNORECASE)
                remove_end = cond_end + (and_m.end() if and_m else 0)
                sql = sql[:m_on.end()] + sql[remove_end:]
            else:
                sql = sql[:m_on.end()] + 'TRUE' + sql[cond_end:]
            log.debug("[ALIAS-REMOVE] Removed ON condition: %s", cond_text.strip()[:120])
            break

    # Also remove "AND alias_ref ..." inside ON clauses
    on_and = re.compile(r'\s+AND\s+', re.IGNORECASE)
    search_pos = 0
    while True:
        m_a = on_and.search(sql, search_pos)
        if not m_a:
            break
        cond_start = m_a.end()
        cond_end = _find_cond_end_a(sql, cond_start)
        cond_text = sql[cond_start:cond_end]
        if alias_check.search(cond_text):
            sql = sql[:m_a.start()] + sql[cond_end:]
            log.debug("[ALIAS-REMOVE] Removed AND-in-ON condition: %s", cond_text.strip()[:120])
        else:
            search_pos = cond_end

    # ---- 4. Clean up artifacts ----
    sql = re.sub(r'\bWHERE\s*(?=\s*(?:ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT|LIMIT|;|\Z|\)))',
                 '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\b(AND|OR)\s+(AND|OR)\b', r'\1', sql, flags=re.IGNORECASE)
    while ',,' in sql or ', ,' in sql:
        sql = sql.replace(',,', ',').replace(', ,', ',')
    sql = re.sub(r'(SELECT\s+(?:DISTINCT\s+)?)\s*,', r'\1', sql, flags=re.IGNORECASE)
    sql = re.sub(r',\s*(\bFROM\b)', r' \1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'  +', ' ', sql)

    if sql.strip() == original.strip():
        return None
    return sql


# ---------------------------------------------------------------------------
# Auto-remove missing relations (table/view does not exist in PG)
# ---------------------------------------------------------------------------

def _remove_relation_references(sql: str, relation: str) -> Optional[str]:
    """Remove a missing relation and ALL its references from a CREATE VIEW SQL.

    When PostgreSQL says ``relation "X" does not exist``, the table/view is
    present in a FROM or JOIN clause but does not exist in PG.  We:

    1. Find the relation in FROM / JOIN clauses and determine its alias.
    2. Remove the JOIN clause (including ON/USING condition) or the FROM-list
       entry.
    3. Delegate to ``_remove_all_alias_references`` to replace every
       ``alias.column`` in SELECT with NULL and remove WHERE/ON conditions.

    Returns the modified SQL, or None if no changes could be made.
    """
    if not relation:
        return None
    original = sql

    # ---- build match pattern for [schema.]relation ----
    if '.' in relation:
        parts = relation.split('.', 1)
        esc_schema = re.escape(parts[0])
        esc_table = re.escape(parts[1])
        # Match with or without schema prefix; optionally quoted
        rel_full_pat = (r'(?:"?' + esc_schema + r'"?\s*\.\s*)?'
                        r'"?' + esc_table + r'"?')
        bare_name = parts[1]
    else:
        esc_table = re.escape(relation)
        rel_full_pat = (r'(?:"?[a-zA-Z_][a-zA-Z0-9_]*"?\s*\.\s*)?'
                        r'"?' + esc_table + r'"?')
        bare_name = relation

    # Keywords that terminate an ON / USING clause (next JOIN, WHERE, etc.)
    _join_end_kw = re.compile(
        r'\b(?:(?:LEFT|RIGHT|INNER|FULL|CROSS)\s+(?:OUTER\s+)?)?JOIN\b|'
        r'\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b|\bUNION\b|\bEXCEPT\b|'
        r'\bINTERSECT\b|\bLIMIT\b|\bWINDOW\b',
        re.IGNORECASE,
    )

    def _find_on_end(text: str, start: int) -> int:
        """Return the position where the ON/USING condition ends."""
        depth = 0
        pos = start
        while pos < len(text):
            ch = text[pos]
            if ch == '(':
                depth += 1
            elif ch == ')':
                if depth == 0:
                    return pos
                depth -= 1
            elif depth == 0:
                m_kw = _join_end_kw.match(text, pos)
                if m_kw:
                    return pos
            pos += 1
        return pos

    alias_found: Optional[str] = None

    # ---- Case 1: [LEFT|RIGHT|…] JOIN relation [AS] alias ON/USING … ----
    join_re = re.compile(
        r'(?:(?:LEFT|RIGHT|INNER|FULL|CROSS)\s+)*(?:OUTER\s+)?JOIN\s+'
        + rel_full_pat
        + r'(?:\s+AS\s+|\s+)([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:ON\s+|USING\s*)',
        re.IGNORECASE,
    )
    m_join = join_re.search(sql)
    if m_join:
        alias_found = m_join.group(1)
        on_end = _find_on_end(sql, m_join.end())
        sql = sql[:m_join.start()].rstrip() + ' ' + sql[on_end:].lstrip()
        log.debug("[REL-REMOVE] Removed JOIN for relation %s (alias %s)", relation, alias_found)

    if alias_found is None:
        # JOIN without explicit alias
        join_na_re = re.compile(
            r'(?:(?:LEFT|RIGHT|INNER|FULL|CROSS)\s+)*(?:OUTER\s+)?JOIN\s+'
            + rel_full_pat
            + r'\s+(?:ON\s+|USING\s*)',
            re.IGNORECASE,
        )
        m_jna = join_na_re.search(sql)
        if m_jna:
            alias_found = bare_name
            on_end = _find_on_end(sql, m_jna.end())
            sql = sql[:m_jna.start()].rstrip() + ' ' + sql[on_end:].lstrip()
            log.debug("[REL-REMOVE] Removed JOIN for relation %s (no alias)", relation)

    if alias_found is None:
        # CROSS JOIN with alias (no ON clause)
        cross_re = re.compile(
            r'CROSS\s+JOIN\s+' + rel_full_pat
            + r'(?:\s+AS\s+|\s+)([a-zA-Z_][a-zA-Z0-9_]*)',
            re.IGNORECASE,
        )
        m_cross = cross_re.search(sql)
        if m_cross:
            alias_found = m_cross.group(1)
            sql = sql[:m_cross.start()].rstrip() + ' ' + sql[m_cross.end():].lstrip()
            log.debug("[REL-REMOVE] Removed CROSS JOIN for %s (alias %s)", relation, alias_found)

    if alias_found is None:
        # CROSS JOIN without alias
        cross_na_re = re.compile(
            r'CROSS\s+JOIN\s+' + rel_full_pat + r'\b',
            re.IGNORECASE,
        )
        m_crna = cross_na_re.search(sql)
        if m_crna:
            alias_found = bare_name
            sql = sql[:m_crna.start()].rstrip() + ' ' + sql[m_crna.end():].lstrip()
            log.debug("[REL-REMOVE] Removed CROSS JOIN for %s (no alias)", relation)

    # ---- Case 2: FROM-list (comma-separated) ----
    if alias_found is None:
        # Not-first entry: , [schema.]relation [AS] alias
        from_comma_re = re.compile(
            r',\s*' + rel_full_pat + r'(?:\s+AS\s+|\s+)([a-zA-Z_][a-zA-Z0-9_]*)',
            re.IGNORECASE,
        )
        m_fc = from_comma_re.search(sql)
        if m_fc:
            alias_found = m_fc.group(1)
            sql = sql[:m_fc.start()] + sql[m_fc.end():]
            log.debug("[REL-REMOVE] Removed FROM-list entry for %s (alias %s)", relation, alias_found)

    if alias_found is None:
        # First entry: FROM [schema.]relation [AS] alias ,
        from_first_re = re.compile(
            r'(\bFROM\s+)' + rel_full_pat
            + r'(?:\s+AS\s+|\s+)([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*',
            re.IGNORECASE,
        )
        m_ff = from_first_re.search(sql)
        if m_ff:
            alias_found = m_ff.group(2)
            sql = sql[:m_ff.start()] + m_ff.group(1) + sql[m_ff.end():]
            log.debug("[REL-REMOVE] Removed first FROM entry for %s (alias %s)", relation, alias_found)

    if alias_found is None:
        # Not-first entry without alias: , [schema.]relation
        from_comma_na_re = re.compile(
            r',\s*' + rel_full_pat + r'(?=\s*(?:,|\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b'
            r'|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b|\bLIMIT\b|\bJOIN\b'
            r'|\bLEFT\b|\bRIGHT\b|\bINNER\b|\bFULL\b|\bCROSS\b|;|\)|\Z))',
            re.IGNORECASE,
        )
        m_fcna = from_comma_na_re.search(sql)
        if m_fcna:
            alias_found = bare_name
            sql = sql[:m_fcna.start()] + sql[m_fcna.end():]
            log.debug("[REL-REMOVE] Removed FROM-list entry for %s (no alias)", relation)

    if alias_found is None:
        # First entry without alias: FROM [schema.]relation ,
        from_first_na_re = re.compile(
            r'(\bFROM\s+)' + rel_full_pat + r'\s*,\s*',
            re.IGNORECASE,
        )
        m_ffna = from_first_na_re.search(sql)
        if m_ffna:
            alias_found = bare_name
            sql = sql[:m_ffna.start()] + m_ffna.group(1) + sql[m_ffna.end():]
            log.debug("[REL-REMOVE] Removed first FROM entry for %s (no alias)", relation)

    if alias_found is None:
        return None

    # ---- Remove all alias.column references ----
    result = _remove_all_alias_references(sql, alias_found)
    if result is not None:
        sql = result

    # Clean up empty WHERE
    sql = re.sub(
        r'\bWHERE\s*(?=\s*(?:ORDER|GROUP|HAVING|UNION|EXCEPT|INTERSECT|LIMIT|;|\Z|\)))',
        '', sql, flags=re.IGNORECASE,
    )
    sql = re.sub(r'  +', ' ', sql)

    if sql.strip() == original.strip():
        return None
    return sql


def execute_view_with_column_retry(
    connection, sql: str, view_schema: str, view_name: str,
    *, auto_remove_columns: bool = True, auto_remove_relations: bool = True,
) -> tuple[bool, str, str, list[str]]:
    """Execute a CREATE VIEW, auto-retrying by removing missing items.

    Always handled (no flag needed):
    * ``missing FROM-clause entry for table X`` -- dangling alias references
      (common with Oracle package references that survived conversion).

    Always handled (in addition to missing FROM-clause entry):
    * ``function X does not exist`` -- replace with NULL and retry.
    * ``column X.ctid does not exist`` -- replace with NULL::text and retry.

    Controlled by two independent flags:
    * **auto_remove_columns** -- handle ``column X does not exist`` (ctid handled always).
    * **auto_remove_relations** -- handle ``relation X does not exist``
      (table/view not present in PostgreSQL).

    Returns (success, error_message, final_sql, removed_items).
    ``removed_items`` is a list of 'qualifier.col', 'func:func_name',
    'alias:table_name', or 'rel:relation_name' strings.
    """
    removed_columns: list[str] = []
    current_sql = sql
    display = f"{view_schema}.{view_name}" if view_schema else view_name

    def _sql_preview(s: str, maxlen: int = 200) -> str:
        """Return a short preview of the SQL for logging."""
        flat = ' '.join(s.split())
        return flat[:maxlen] + ('...' if len(flat) > maxlen else '')

    seen_errors: set[str] = set()
    for attempt in range(_AUTO_REMOVE_MAX_RETRIES):
        log.info("[RETRY] %s: attempt %d -- SQL length=%d chars, removed_so_far=%d",
                 display, attempt + 1, len(current_sql), len(removed_columns))
        log.debug("[RETRY] %s: attempt %d -- SQL preview: %s",
                  display, attempt + 1, _sql_preview(current_sql))

        log.info("[RETRY] %s: attempt %d -- executing on PostgreSQL...", display, attempt + 1)
        ok, err = execute_view_on_postgres(connection, current_sql, view_schema, view_name)
        log.info("[RETRY] %s: attempt %d -- PG result: ok=%s, err=%s",
                 display, attempt + 1, ok, err[:200] if err else '')

        if ok:
            log.info("[RETRY] %s: SUCCESS after %d attempt(s), %d item(s) removed",
                     display, attempt + 1, len(removed_columns))
            return True, "", current_sql, removed_columns

        # --- Errors gated by auto_remove_columns ---
        if auto_remove_columns:
            # Try missing-column error first
            parsed = _parse_missing_column(err)
            log.debug("[RETRY] %s: _parse_missing_column -> %s", display, parsed)
            if parsed:
                qualifier, col = parsed
                col_display = f"{qualifier}.{col}" if qualifier else col
                err_key = f"col|{qualifier}|{col}"
                if err_key in seen_errors:
                    log.warning("[COL-REMOVE] %s: column %s still missing after removal -- giving up (seen_errors=%s)",
                                display, col_display, seen_errors)
                    return False, err, current_sql, removed_columns
                seen_errors.add(err_key)

                # ctid is handled in the "always" block below (like function); skip here
                if col.lower() != "ctid":
                    log.info("[COL-REMOVE] %s: removing column %s (attempt %d)", display, col_display, attempt + 1)
                    log.debug("[COL-REMOVE] %s: BEFORE _remove_column_references(qualifier=%r, col=%r) -- SQL length=%d",
                              display, qualifier, col, len(current_sql))
                    new_sql = _remove_column_references(current_sql, qualifier, col)
                    log.debug("[COL-REMOVE] %s: AFTER _remove_column_references -- result=%s, new_length=%s",
                              display,
                              'None' if new_sql is None else ('no_change' if new_sql.strip() == current_sql.strip() else 'changed'),
                              len(new_sql) if new_sql else 'N/A')
                    if new_sql is None or new_sql.strip() == current_sql.strip():
                        log.warning("[COL-REMOVE] %s: could not remove %s from SQL -- giving up", display, col_display)
                        log.debug("[COL-REMOVE] %s: SQL at give-up: %s", display, _sql_preview(current_sql, 500))
                        return False, err, current_sql, removed_columns
                    removed_columns.append(col_display)
                    log.info("[COL-REMOVE] %s: DONE removing %s -- new SQL length=%d (was %d, delta=%d)",
                             display, col_display, len(new_sql), len(current_sql), len(current_sql) - len(new_sql))
                    current_sql = new_sql
                    continue

        # --- Always handle missing-function error: replace with NULL and add alias if needed ---
        func_name = _parse_missing_function(err)
        log.debug("[RETRY] %s: _parse_missing_function -> %s", display, func_name)
        if func_name:
            err_key = f"func|{func_name}"
            if err_key in seen_errors:
                log.warning("[FUNC-REPLACE] %s: function %s still missing after replace -- giving up", display, func_name)
                return False, err, current_sql, removed_columns
            seen_errors.add(err_key)
            log.info("[FUNC-REPLACE] %s: replacing function %s with NULL (attempt %d)", display, func_name, attempt + 1)
            log.debug("[FUNC-REPLACE] %s: BEFORE _replace_function_with_null_and_alias -- SQL length=%d", display, len(current_sql))
            new_sql = _replace_function_with_null_and_alias(current_sql, func_name)
            log.debug("[FUNC-REPLACE] %s: AFTER _replace_function_with_null_and_alias -- result=%s",
                      display,
                      'None' if new_sql is None else ('no_change' if new_sql and new_sql.strip() == current_sql.strip() else 'changed'))
            if new_sql is None or (new_sql.strip() == current_sql.strip()):
                log.warning("[FUNC-REPLACE] %s: could not replace %s in SQL -- giving up", display, func_name)
                return False, err, current_sql, removed_columns
            removed_columns.append(f"func:{func_name}")
            log.info("[FUNC-REPLACE] %s: DONE replacing %s with NULL -- new SQL length=%d (was %d)",
                     display, func_name, len(new_sql), len(current_sql))
            current_sql = new_sql
            continue

        # --- Always handle ctid column does not exist: replace with NULL::text (like function) ---
        # Handles both "column b.ctid does not exist" (qualified) and "column ctid does not exist" (bare)
        parsed = _parse_missing_column(err)
        is_ctid_err = False
        qualifier = ""
        if parsed:
            q, c = parsed
            if c and c.lower() == "ctid":
                is_ctid_err = True
                qualifier = q
        if not is_ctid_err and err and "ctid" in err.lower() and "column" in err.lower() and "does not exist" in err.lower():
            is_ctid_err = True
        if is_ctid_err:
            col_display = f"{qualifier}.ctid" if qualifier else "ctid"
            err_key = f"ctid|{qualifier}|ctid"
            if err_key in seen_errors:
                log.warning("[CTID-REPLACE] %s: ctid still missing after replace -- giving up", display)
                return False, err, current_sql, removed_columns
            seen_errors.add(err_key)
            log.info("[CTID-REPLACE] %s: replacing %s with NULL::text (attempt %d)", display, col_display, attempt + 1)
            new_sql = _replace_ctid_with_null(current_sql, qualifier)
            if qualifier:
                new_sql = _replace_ctid_with_null(new_sql or current_sql, "")  # also replace any bare ctid
            if new_sql is None or new_sql.strip() == current_sql.strip():
                log.debug("[CTID-REPLACE] %s: ctid replace failed, falling back to _remove_column_references", display)
                new_sql = _remove_column_references(current_sql, qualifier, "ctid")
            if new_sql and new_sql.strip() != current_sql.strip():
                removed_columns.append(f"{col_display}->NULL")
                current_sql = new_sql
                continue

        # --- Always handle missing FROM-clause entry (Oracle package aliases) ---
        from_alias = _parse_missing_from_entry(err)
        log.debug("[RETRY] %s: _parse_missing_from_entry -> %s", display, from_alias)
        if from_alias:
            err_key = f"from|{from_alias}"
            if err_key in seen_errors:
                log.warning("[ALIAS-REMOVE] %s: alias %s still missing after removal -- giving up", display, from_alias)
                return False, err, current_sql, removed_columns
            seen_errors.add(err_key)
            log.info("[ALIAS-REMOVE] %s: removing all references to alias %s (attempt %d)", display, from_alias, attempt + 1)
            log.debug("[ALIAS-REMOVE] %s: BEFORE _remove_all_alias_references -- SQL length=%d", display, len(current_sql))
            new_sql = _remove_all_alias_references(current_sql, from_alias)
            log.debug("[ALIAS-REMOVE] %s: AFTER _remove_all_alias_references -- result=%s",
                      display,
                      'None' if new_sql is None else ('no_change' if new_sql.strip() == current_sql.strip() else 'changed'))
            if new_sql is None or new_sql.strip() == current_sql.strip():
                log.warning("[ALIAS-REMOVE] %s: could not remove alias %s from SQL -- giving up", display, from_alias)
                return False, err, current_sql, removed_columns
            removed_columns.append(f"alias:{from_alias}")
            log.info("[ALIAS-REMOVE] %s: DONE removing alias %s -- new SQL length=%d (was %d)",
                     display, from_alias, len(new_sql), len(current_sql))
            current_sql = new_sql
            continue

        # --- Errors gated by auto_remove_relations ---
        if auto_remove_relations:
            rel_name = _parse_missing_relation(err)
            log.debug("[RETRY] %s: _parse_missing_relation -> %s", display, rel_name)
            if rel_name:
                err_key = f"rel|{rel_name}"
                if err_key in seen_errors:
                    log.warning("[REL-REMOVE] %s: relation %s still missing after removal -- giving up", display, rel_name)
                    return False, err, current_sql, removed_columns
                seen_errors.add(err_key)
                log.info("[REL-REMOVE] %s: removing relation %s and its references (attempt %d)", display, rel_name, attempt + 1)
                log.debug("[REL-REMOVE] %s: BEFORE _remove_relation_references -- SQL length=%d", display, len(current_sql))
                new_sql = _remove_relation_references(current_sql, rel_name)
                log.debug("[REL-REMOVE] %s: AFTER _remove_relation_references -- result=%s",
                          display,
                          'None' if new_sql is None else ('no_change' if new_sql.strip() == current_sql.strip() else 'changed'))
                if new_sql is None or new_sql.strip() == current_sql.strip():
                    log.warning("[REL-REMOVE] %s: could not remove relation %s from SQL -- giving up", display, rel_name)
                    return False, err, current_sql, removed_columns
                removed_columns.append(f"rel:{rel_name}")
                log.info("[REL-REMOVE] %s: DONE removing relation %s -- new SQL length=%d (was %d)",
                         display, rel_name, len(new_sql), len(current_sql))
                current_sql = new_sql
                continue

        # No recognized error type — stop retrying
        log.warning("[RETRY] %s: unrecognized error type -- stopping. err=%s", display, err[:300])
        return False, err, current_sql, removed_columns

    log.warning("[RETRY] %s: exceeded %d retries", display, _AUTO_REMOVE_MAX_RETRIES)
    return False, f"Exceeded {_AUTO_REMOVE_MAX_RETRIES} column/function-removal retries", current_sql, removed_columns


def execute_view_on_postgres(connection, sql: str, view_schema: str, view_name: str,
                             *, timeout_seconds: int = 60) -> tuple[bool, str]:
    """
    Execute a single CREATE VIEW statement on PostgreSQL.
    Returns (success, message). message is empty on success or error text on failure.

    Uses a Python-level watchdog thread to cancel the query if it hangs
    beyond *timeout_seconds* (default 60).
    """
    import threading
    try:
        import psycopg2
    except ImportError:
        return False, "psycopg2 not installed"

    sql = sql.strip()
    if not sql:
        return False, "Empty SQL"

    import time as _time
    display = f"{view_schema}.{view_name}" if view_schema else view_name

    # Watchdog: cancel the PG query from a background thread if it takes too long
    cancelled = threading.Event()

    def _watchdog():
        if not cancelled.wait(timeout_seconds):
            # Timeout reached -- cancel the running query
            log.warning("[PG-EXEC] %s: WATCHDOG FIRED after %ds -- cancelling query", display, timeout_seconds)
            try:
                connection.cancel()
            except Exception:
                pass

    log.debug("[PG-EXEC] %s: starting cur.execute (SQL length=%d, timeout=%ds)...", display, len(sql), timeout_seconds)
    t0 = _time.monotonic()
    timer = threading.Thread(target=_watchdog, daemon=True)
    timer.start()
    try:
        with connection.cursor() as cur:
            cur.execute(sql)
        connection.commit()
        elapsed = _time.monotonic() - t0
        log.debug("[PG-EXEC] %s: cur.execute OK in %.2fs", display, elapsed)
        return True, ""
    except psycopg2.Error as e:
        elapsed = _time.monotonic() - t0
        err_msg = str(e).strip()
        log.debug("[PG-EXEC] %s: cur.execute FAILED in %.2fs -- %s", display, elapsed, err_msg[:200])
        connection.rollback()
        return False, err_msg
    finally:
        cancelled.set()  # stop the watchdog


def _first_error_line(full_sql: str, err_type: str) -> str:
    """Extract first comment line that looks like an error message from full_sql."""
    if not full_sql or not isinstance(full_sql, str):
        return ""
    for line in full_sql.splitlines():
        s = line.strip()
        if s.startswith("-- ERROR") or s.startswith("-- Execution"):
            return s[2:].strip()[:500]  # drop "-- ", cap length
    return (err_type or "")


def _sql_only(text: str) -> str:
    """Strip diagnostic comment lines (-- Source, -- ERROR, -- WARNING, -- Execution error)
    from SQL text so that output files contain only the raw SQL query."""
    if not text:
        return text
    lines = text.splitlines(True)  # keep line endings
    out: list[str] = []
    for line in lines:
        s = line.lstrip()
        if s.startswith("-- Source:") or s.startswith("-- ERROR") or s.startswith("-- WARNING") or s.startswith("-- Execution error"):
            continue
        out.append(line)
    # Strip leading blank lines
    result = "".join(out).lstrip("\n")
    return result if result.strip() else text  # fallback to original if nothing left



# Pattern for start of CREATE VIEW statement
_CREATE_VIEW_PATTERN = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+",
    re.IGNORECASE,
)

# Per-view hot-path regexes (compiled once to avoid repeated compile in loops)
_EXTRACT_VIEW_REFS_PATTERN = re.compile(
    r"\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+"
    r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
    re.IGNORECASE,
)
_UNQUALIFIED_TABLE_REFS_PATTERN = re.compile(
    r"\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+"
    r"([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)
_UNQUALIFIED_TABLE_REFS_COMMA_PATTERN = re.compile(
    r",\s+([a-zA-Z_][a-zA-Z0-9_]*)\b",
    re.IGNORECASE,
)
_QUALIFY_COMMA_TABLE_PATTERN = re.compile(
    r"(,\s+)([a-zA-Z_][a-zA-Z0-9_]*)\b",
    re.IGNORECASE,
)
_QUALIFY_FROM_JOIN_PATTERN = re.compile(
    r"(\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+)([a-zA-Z_][a-zA-Z0-9_]*)\b",
    re.IGNORECASE,
)
_BODY_FROM_DDL_PATTERN = re.compile(r"\bAS\s+(.*)\s*;?\s*$", re.IGNORECASE | re.DOTALL)
_CREATE_OR_REPLACE_VIEW_AS_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\S+)\s+AS",
    re.IGNORECASE,
)
_NORMALIZE_DROP_VIEW_PATTERN = re.compile(
    r"^\s*DROP\s+VIEW\s+IF\s+EXISTS\s+[^;]+;\s*",
    re.IGNORECASE,
)
_NORMALIZE_CREATE_VIEW_PATTERN = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+(.+?)\s+(\([^)]*\)\s+)?AS\s+(.*)",
    re.IGNORECASE | re.DOTALL,
)
# Extract column list from raw DDL (used when sqlglot drops it during transpilation)
_EXTRACT_ORIG_COLUMN_LIST_PATTERN = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+\S+\s+(\([^)]*\))\s+AS\b",
    re.IGNORECASE | re.DOTALL,
)
# Pattern to replace only the view name in CREATE VIEW ... name ... AS (used to force original schema.viewname)
_REWRITE_VIEW_NAME_PATTERN = re.compile(
    r"(CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+)(.+?)(\s+(?:\([^)]*\)\s+)?AS\s+)",
    re.IGNORECASE | re.DOTALL,
)

# Cache for TO_* function patterns in _replace_to_func_for_oracle_parse (avoid recompile per view)
_to_func_pattern_cache: dict[str, re.Pattern] = {}


def _strip_sql_comments(content: str) -> str:
    """
    Remove -- line comments and /* */ block comments from SQL.
    Preserves content inside single-quoted strings. Used so CREATE VIEW is easier to find.
    """
    result: list[str] = []
    i = 0
    in_single = False
    in_line_comment = False
    in_block_comment = False
    while i < len(content):
        c = content[i]
        if in_line_comment:
            if c == "\n":
                in_line_comment = False
                result.append(c)
            i += 1
            continue
        if in_block_comment:
            if c == "*" and i + 1 < len(content) and content[i + 1] == "/":
                in_block_comment = False
                i += 2
                result.append(" ")
            else:
                i += 1
            continue
        if in_single:
            result.append(c)
            if c == "'" and (i + 1 >= len(content) or content[i + 1] != "'"):
                in_single = False
            elif c == "'" and i + 1 < len(content) and content[i + 1] == "'":
                result.append(content[i + 1])
                i += 1
            i += 1
            continue
        if c == "'":
            in_single = True
            result.append(c)
            i += 1
            continue
        if c == "-" and i + 1 < len(content) and content[i + 1] == "-":
            in_line_comment = True
            i += 2
            result.append(" ")
            continue
        if c == "/" and i + 1 < len(content) and content[i + 1] == "*":
            in_block_comment = True
            i += 2
            result.append(" ")
            continue
        result.append(c)
        i += 1
    return "".join(result)


def _find_statement_end(text: str, start: int) -> int:
    """
    Find the end of the current SQL statement (next semicolon at top level).
    Ignores semicolons inside parentheses, single-quoted strings, and comments (-- and /* */).
    Returns index of the semicolon, or len(text) if not found.
    """
    depth = 0
    i = start
    in_single = False
    in_line_comment = False
    in_block_comment = False
    while i < len(text):
        c = text[i]
        if in_line_comment:
            if c == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            if c == "*" and i + 1 < len(text) and text[i + 1] == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue
        if in_single:
            if c == "'" and (i + 1 >= len(text) or text[i + 1] != "'"):
                in_single = False
            elif c == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 1
            i += 1
            continue
        if c == "'":
            in_single = True
            i += 1
            continue
        if c == "-" and i + 1 < len(text) and text[i + 1] == "-":
            in_line_comment = True
            i += 2
            continue
        if c == "/" and i + 1 < len(text) and text[i + 1] == "*":
            in_block_comment = True
            i += 2
            continue
        if c == "(":
            depth += 1
            i += 1
            continue
        if c == ")":
            depth -= 1
            i += 1
            continue
        if c == ";" and depth == 0:
            return i
        i += 1
    return len(text)


def split_content_into_view_ddls(content: str) -> list[tuple[str, str, str]]:
    """
    Split file content into individual CREATE VIEW DDL statements.
    Returns list of (schema, view_name, ddl_string) for each view found.
    Handles multiple CREATE VIEW statements in one file.
    Comment-stripped content is used for finding CREATE VIEW so that formats
    like 'CREATE /* comment */ OR REPLACE VIEW' are detected; DDL is extracted
    from the stripped content (comments are not preserved in returned DDL).
    """
    result: list[tuple[str, str, str]] = []
    content = content.strip()
    if not content:
        return result
    # Use comment-stripped content so CREATE ... VIEW with comments between keywords is found
    stripped = _strip_sql_comments(content)
    stripped = stripped.strip()
    if not stripped:
        return result
    pos = 0
    while True:
        match = _CREATE_VIEW_PATTERN.search(stripped, pos)
        if not match:
            break
        start = match.start()
        name_start = match.end()
        name_end = name_start
        while name_end < len(stripped) and stripped[name_end] not in " \t\n\r(":
            name_end += 1
        name_part = stripped[name_start:name_end].strip().strip('"')
        if "." in name_part:
            parts = name_part.split(".", 1)
            schema = parts[0].strip().strip('"')
            view_name = parts[1].strip().strip('"')
        else:
            schema = ""
            view_name = name_part if name_part else "unknown_view"
        end = _find_statement_end(stripped, name_end)
        ddl = stripped[start:end + 1].strip() if end < len(stripped) and stripped[end] == ";" else stripped[start:end].strip()
        if ddl:
            result.append((schema, view_name, ddl))
        pos = end + 1 if end < len(stripped) else len(stripped)
    return result


def parse_view_name_from_ddl(ddl: str) -> tuple[str, str]:
    """
    Parse schema and view name from Oracle CREATE VIEW DDL.
    Expects: CREATE [OR REPLACE] [FORCE] VIEW [schema.]view_name ...
    Returns (schema, view_name); schema may be "" if unqualified.
    """
    ddl_stripped = ddl.strip()
    for line in ddl_stripped.splitlines():
        line = line.strip()
        if line.startswith("--") or not line:
            continue
        match = re.search(
            r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+([^\s(]+)",
            line,
            re.IGNORECASE,
        )
        if match:
            name_part = match.group(1).strip().strip('"')
            if "." in name_part:
                parts = name_part.split(".", 1)
                return parts[0].strip().strip('"'), parts[1].strip().strip('"')
            return "", name_part
        break
    match = re.search(
        r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+([^\s(]+)",
        ddl_stripped,
        re.IGNORECASE,
    )
    if match:
        name_part = match.group(1).strip().strip('"')
        if "." in name_part:
            parts = name_part.split(".", 1)
            return parts[0].strip().strip('"'), parts[1].strip().strip('"')
        return "", name_part
    return "", "unknown_view"


def load_ddl_from_sql_directory(
    input_dir: str,
    pattern: str = "*.sql",
    recursive: bool = False,
) -> list[tuple[str, str, str]]:
    """
    Load Oracle view DDL from SQL files in a directory.
    Returns list of (schema, view_name, ddl_content) in file name order.
    Supports multiple CREATE VIEW statements per file; each is parsed and added separately.
    If the same (schema, view_name) appears in multiple files or statements, the first occurrence is kept.
    """
    path = Path(input_dir)
    if not path.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    log.info("[INPUT] Reading directory: %s (pattern=%s recursive=%s)", input_dir, pattern, recursive)
    if recursive:
        sql_files = sorted(path.rglob(pattern))
    else:
        sql_files = sorted(path.glob(pattern))
    log.info("[INPUT] Found %d SQL file(s)", len(sql_files))
    result: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for f in sql_files:
        if not f.is_file():
            continue
        log.debug("[INPUT] Reading file: %s", f.name)
        try:
            content = f.read_text(encoding="utf-8-sig", errors="replace").strip()
        except Exception as e:
            log.warning("[INPUT] Skip unreadable file %s: %s", f.name, e)
            continue
        if not content:
            log.debug("[INPUT] Skip empty file: %s", f.name)
            continue
        ddls = split_content_into_view_ddls(content)
        if ddls:
            log.info("[INPUT] File %s: split into %d view(s): %s", f.name, len(ddls), ", ".join(f"{s}.{v}" if s else v for s, v, _ in ddls))
            for schema, view_name, ddl in ddls:
                key = (schema, view_name)
                if key not in seen:
                    seen.add(key)
                    result.append((schema, view_name, ddl))
        else:
            schema, view_name = parse_view_name_from_ddl(content)
            key = (schema, view_name)
            log.info("[INPUT] File %s: single view (no split): %s", f.name, f"{schema}.{view_name}" if schema else view_name)
            if key not in seen:
                seen.add(key)
                result.append((schema, view_name, content))
    log.info("[INPUT] Loaded %d view(s) total (deduplicated)", len(result))
    return result


def _convert_single_view(
    schema: str,
    view_name: str,
    ddl: str,
    synonym_map: dict[str, str],
    apply_oracle_conversions: bool,
    do_qualify: bool,
    conn_o,
    schema_objects_cache: Optional[dict[str, dict[str, str]]],
    step_sec: int,
    qualify_lock: Optional[threading.Lock],
    skip_deps: bool,
    view_keys_lower: set,
    view_keys_by_name: Optional[dict],
) -> tuple[str, list[str], Optional[str], set]:
    """
    Shared conversion pipeline: rewrite -> normalize -> qualify.
    Returns (pg_sql, warning_lines, err_type, refs).
    - pg_sql: converted SQL (empty string on error)
    - warning_lines: list of warning/error comment strings to prepend
    - err_type: None on success, or a string like "timeout", "rewrite"
    - refs: dependency refs (empty set when skip_deps is True)
    """
    warning_lines: list[str] = []

    # Extract column list from original DDL before rewrite (sqlglot may drop it)
    orig_column_list: str | None = None
    m_col = _EXTRACT_ORIG_COLUMN_LIST_PATTERN.search(_first_statement_only(ddl))
    if m_col:
        orig_column_list = m_col.group(1).strip()

    # Step 1: rewrite (sqlglot parse + synonym replacement + transpile)
    rewrite_failed = False
    try:
        ok, res = _run_with_step_timeout(step_sec, rewrite_sql_with_synonyms, ddl, synonym_map, view_schema=schema, view_name=view_name)
        if not ok:
            return "", [f"-- ERROR: rewrite timeout after {step_sec or 15}s\n"], "timeout", set()
        pg_sql = res
    except Exception as rewrite_err:
        # Fallback: sqlglot failed (e.g. "Expecting )", unsupported syntax).
        # Skip synonym resolution and proceed with normalize on the raw DDL.
        display = f"{schema}.{view_name}" if schema else view_name
        log.warning("[VIEW %s] sqlglot rewrite failed (%s), falling back to normalize-only", display, rewrite_err)
        warning_lines.append(f"-- WARNING: sqlglot rewrite failed ({str(rewrite_err)[:120]}), synonym resolution skipped\n")
        pg_sql = ddl
        rewrite_failed = True

    # Step 2: normalize (Oracle->PG body conversions)
    ok, res = _run_with_step_timeout(
        step_sec, normalize_view_script, pg_sql, apply_oracle_conversions,
        orig_schema=schema, orig_view_name=view_name, orig_column_list=orig_column_list,
    )
    if not ok:
        display = f"{schema}.{view_name}" if schema else view_name
        log.warning("[VIEW %s] normalize TIMEOUT after %ds, skipping normalize", display, step_sec or 15)
        warning_lines.append(f"-- WARNING: normalize timeout after {step_sec or 15}s, step skipped\n")
    else:
        pg_sql = res

    # Step 3: qualify unqualified refs (skip refs in PUBLIC; dep resolution handles them first)
    if do_qualify:
        try:
            if qualify_lock:
                with qualify_lock:
                    ok, res = _run_with_step_timeout(
                        step_sec, apply_qualify_unqualified_refs,
                        pg_sql, schema, conn_o, schema_objects_cache, synonym_map,
                    )
                    if ok:
                        pg_sql = res
                    else:
                        display = f"{schema}.{view_name}" if schema else view_name
                        log.warning("[VIEW %s] qualify TIMEOUT after %ds, skipping qualify", display, step_sec or 15)
                        warning_lines.append(f"-- WARNING: qualify timeout after {step_sec or 15}s, step skipped\n")
            else:
                ok, res = _run_with_step_timeout(
                    step_sec, apply_qualify_unqualified_refs,
                    pg_sql, schema, conn_o, schema_objects_cache, synonym_map,
                )
                if ok:
                    pg_sql = res
                else:
                    display = f"{schema}.{view_name}" if schema else view_name
                    log.warning("[VIEW %s] qualify TIMEOUT after %ds, skipping qualify", display, step_sec or 15)
                    warning_lines.append(f"-- WARNING: qualify timeout after {step_sec or 15}s, step skipped\n")
        except Exception as e:
            display = f"{schema}.{view_name}" if schema else view_name
            log.warning("[VIEW %s] qualify skipped: %s", display, e)
            warning_lines.append(f"-- WARNING: qualify unqualified refs skipped: {e}\n")

    # Step 4: extract dependency refs
    body = _body_from_create_view_ddl(pg_sql)
    refs = set() if skip_deps else _extract_view_refs_from_body(body, view_keys_lower, view_keys_by_name)

    return pg_sql, warning_lines, None, refs



ViewKey = tuple[str, str]  # (schema_lower, view_name_lower)


class RecursiveViewMigrator:
    """Migrate Oracle views to PostgreSQL, recursively resolving missing dependencies."""

    MAX_RETRIES_PER_VIEW = 40
    MAX_RESOLVE_DEPTH = 30

    def __init__(
        self,
        oracle_conn,
        pg_conn,
        synonym_map: dict[str, str],
        schema_objects_cache: dict[str, dict[str, str]],
        step_timeout: int = 15,
        auto_remove_columns: bool = False,
        no_qualify: bool = False,
        output_dir: Optional[Path] = None,
    ):
        self.oracle_conn = oracle_conn
        self.pg_conn = pg_conn
        self.synonym_map = synonym_map
        self.schema_objects_cache = schema_objects_cache
        self.step_timeout = step_timeout
        self.auto_remove_columns = auto_remove_columns
        self.no_qualify = no_qualify
        self.output_dir = output_dir

        self.created: set[ViewKey] = set()
        self.failed: set[ViewKey] = set()
        self._in_progress: set[ViewKey] = set()
        self._ensured_schemas: set[str] = set()
        self._created_functions: set[str] = set()

        self.results: list[dict] = []  # per-view result records
        self.stats = {
            "ok": 0,
            "fail": 0,
            "skipped": 0,
            "synonyms_created": 0,
            "deps_resolved": 0,
        }

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def _ensure_schema(self, schema: str) -> None:
        s = (schema or "").strip().lower()
        if not s or s in self._ensured_schemas:
            return
        if ensure_pg_schema(self.pg_conn, s):
            self._ensured_schemas.add(s)
            log.info("Ensured PG schema: %s", s)
        else:
            log.warning("Failed to create PG schema: %s", s)

    # ------------------------------------------------------------------
    # Synonym chain resolution
    # ------------------------------------------------------------------

    def _resolve_synonym_chain(
        self, name: str, schema: Optional[str] = None, max_hops: int = 10
    ) -> Optional[tuple[str, str]]:
        """Follow the synonym map to the final (table_owner, table_name).

        Returns (target_schema, target_name) uppercased, or None.
        """
        for _ in range(max_hops):
            if schema:
                key = f"{schema.upper()}.{name.upper()}"
                target = self.synonym_map.get(key)
            else:
                target = None

            if target is None:
                target = self.synonym_map.get(name.upper())

            if target is None:
                return None

            parts = target.split(".", 1)
            if len(parts) == 2:
                t_schema, t_name = parts
            else:
                t_schema, t_name = "", parts[0]

            next_key_q = f"{t_schema}.{t_name}" if t_schema else t_name
            if next_key_q not in self.synonym_map and t_name not in self.synonym_map:
                return (t_schema, t_name)

            schema, name = t_schema, t_name

        log.warning("Synonym chain exceeded %d hops for %s.%s", max_hops, schema, name)
        return (schema or "", name) if schema or name else None

    # ------------------------------------------------------------------
    # Oracle object lookup
    # ------------------------------------------------------------------

    def _lookup_in_oracle(
        self, relation: str, parent_schema: str = ""
    ) -> tuple[Optional[str], str, str]:
        """Determine what a missing PG relation corresponds to in Oracle.

        Returns (kind, resolved_schema, resolved_name) where kind is one of:
          'SYNONYM'  - relation is a synonym; resolved_* is the final target
          'VIEW'     - relation is a view
          'TABLE'    - relation is a table
          None       - could not resolve

        The relation string comes from the PG error (lowercased). We uppercase
        for Oracle lookups.
        """
        if "." in relation:
            schema, name = relation.split(".", 1)
        else:
            schema, name = "", relation

        schema_u = schema.upper()
        name_u = name.upper()

        # 1) Check synonym map
        resolved = self._resolve_synonym_chain(name_u, schema_u if schema else None)
        if resolved:
            return ("SYNONYM", resolved[0], resolved[1])

        # 2) Check ALL_OBJECTS for the given schema (or parent schema / common schemas)
        schemas_to_try = []
        if schema_u:
            schemas_to_try.append(schema_u)
        else:
            if parent_schema:
                schemas_to_try.append(parent_schema.upper())
            schemas_to_try.append("PUBLIC")

        for s in schemas_to_try:
            cached = self.schema_objects_cache.get(s)
            if cached is not None:
                obj_type = cached.get(name_u)
            else:
                # Dynamically cache this schema for future lookups
                try:
                    objs = get_oracle_schema_objects(self.oracle_conn, s)
                    self.schema_objects_cache[s] = objs
                    obj_type = objs.get(name_u)
                    log.info("Dynamically cached schema %s (%d objects)", s, len(objs))
                except Exception:
                    obj_type = get_oracle_object_type(self.oracle_conn, s, name_u)
            if obj_type:
                return (obj_type, s, name_u)

        return (None, "", "")

    # ------------------------------------------------------------------
    # DDL fetch + convert pipeline
    # ------------------------------------------------------------------

    def _fetch_and_convert(
        self, schema: str, view_name: str
    ) -> tuple[Optional[str], list[str]]:
        """Fetch DDL from Oracle and run the full Oracle->PG conversion pipeline.

        Returns (pg_sql, warnings). pg_sql is None on failure.
        """
        ddl = _fetch_view_ddl_from_oracle(self.oracle_conn, schema, view_name)
        if not ddl:
            return None, [f"No DDL found in Oracle for {schema}.{view_name}"]

        view_keys_lower: set[ViewKey] = set()
        view_keys_by_name: dict[str, list[ViewKey]] = {}

        try:
            pg_sql, warns, err_type, _ = _convert_single_view(
                schema,
                view_name,
                ddl,
                self.synonym_map,
                True,  # apply_oracle_conversions
                bool(self.oracle_conn) and not self.no_qualify,  # do_qualify
                self.oracle_conn,
                self.schema_objects_cache,
                self.step_timeout,
                None,  # qualify_lock (single-threaded)
                True,  # skip_deps (we handle deps ourselves)
                view_keys_lower,
                view_keys_by_name,
            )
            if err_type:
                return None, [w.strip() for w in warns] + [f"Conversion error: {err_type}"]
            return pg_sql, [w.strip() for w in warns]
        except Exception as e:
            log.warning("Conversion failed for %s.%s: %s", schema, view_name, e)
            return None, [f"Conversion exception: {e}"]

    # ------------------------------------------------------------------
    # Execute helpers
    # ------------------------------------------------------------------

    def _execute_on_pg(
        self, sql: str, schema: str, view_name: str
    ) -> tuple[bool, str]:
        return execute_view_on_postgres(self.pg_conn, sql, schema, view_name)

    # ------------------------------------------------------------------
    # Create synonym-like view in PG
    # ------------------------------------------------------------------

    def _create_synonym_view(
        self,
        syn_schema: str,
        syn_name: str,
        target_schema: str,
        target_name: str,
    ) -> tuple[bool, str]:
        """Create a VIEW in PG that acts as a synonym (SELECT * FROM target)."""
        syn_schema_l = syn_schema.lower()
        syn_name_l = syn_name.lower()
        tgt_schema_l = target_schema.lower()
        tgt_name_l = target_name.lower()

        self._ensure_schema(syn_schema_l)
        if tgt_schema_l:
            self._ensure_schema(tgt_schema_l)
        qualified_target = f"{tgt_schema_l}.{tgt_name_l}" if tgt_schema_l else tgt_name_l
        sql = (
            f"CREATE OR REPLACE VIEW {syn_schema_l}.{syn_name_l} AS\n"
            f"SELECT * FROM {qualified_target}\n;"
        )
        log.info("Creating synonym-view: %s.%s -> %s", syn_schema_l, syn_name_l, qualified_target)
        ok, err = self._execute_on_pg(sql, syn_schema_l, syn_name_l)
        if ok:
            self.stats["synonyms_created"] += 1
        return ok, err

    # ------------------------------------------------------------------
    # Oracle function/procedure fetch & create on PG
    # ------------------------------------------------------------------

    def _fetch_function_source_from_oracle(
        self, owner: str, func_name: str
    ) -> Optional[str]:
        """Fetch function/procedure source from Oracle ALL_SOURCE.

        Returns the full source text (lines concatenated), or None.
        """
        cursor = self.oracle_conn.cursor()
        try:
            cursor.execute(
                """
                SELECT text FROM all_source
                WHERE owner = :o AND name = :n
                ORDER BY type, line
                """,
                {"o": owner.upper(), "n": func_name.upper()},
            )
            rows = cursor.fetchall()
            if not rows:
                return None
            return "".join(r[0] if isinstance(r[0], str) else r[0].read() for r in rows)
        except Exception as e:
            log.warning("Failed to fetch source for %s.%s: %s", owner, func_name, e)
            return None
        finally:
            cursor.close()

    def _fetch_function_ddl_from_oracle(
        self, owner: str, func_name: str
    ) -> Optional[str]:
        """Fetch full CREATE FUNCTION/PROCEDURE DDL via DBMS_METADATA (preferred)
        or fall back to ALL_SOURCE.
        """
        cursor = self.oracle_conn.cursor()
        try:
            cursor.execute(
                """
                SELECT object_type FROM all_objects
                WHERE owner = :o AND object_name = :n
                  AND object_type IN ('FUNCTION', 'PROCEDURE', 'PACKAGE')
                AND ROWNUM = 1
                """,
                {"o": owner.upper(), "n": func_name.upper()},
            )
            row = cursor.fetchone()
            if not row:
                return None
            obj_type = row[0]
        except Exception:
            obj_type = "FUNCTION"
        finally:
            cursor.close()

        # Try DBMS_METADATA first
        cursor = self.oracle_conn.cursor()
        try:
            cursor.execute(
                "SELECT DBMS_METADATA.GET_DDL(:otype, :oname, :oowner) FROM DUAL",
                {"otype": obj_type, "oname": func_name.upper(), "oowner": owner.upper()},
            )
            row = cursor.fetchone()
            if row and row[0]:
                ddl = row[0] if isinstance(row[0], str) else row[0].read()
                return ddl.strip()
        except Exception:
            pass
        finally:
            cursor.close()

        # Fall back to ALL_SOURCE
        source = self._fetch_function_source_from_oracle(owner, func_name)
        if source:
            return f"CREATE OR REPLACE {source}"
        return None

    def _convert_oracle_function_to_pg(self, ddl: str, owner: str) -> Optional[str]:
        """Convert Oracle PL/SQL function/procedure to PL/pgSQL.

        Strategy:
          1. Try sqlglot transpile (Oracle -> Postgres) for the full DDL.
             sqlglot handles signature syntax (RETURN->RETURNS, type mappings,
             parameter modes, IS/AS body wrapping) structurally.
          2. Apply regex-based fixups for anything sqlglot misses or gets wrong
             (PL/SQL body constructs, Oracle builtins, $$ wrapping).
          3. If sqlglot fails entirely, fall back to pure regex conversion.
        """
        if not ddl:
            return None

        text = ddl.strip()
        if text.endswith("/"):
            text = text[:-1].rstrip()

        # Skip PACKAGE / PACKAGE BODY
        if re.search(r"\bPACKAGE\b", text, re.IGNORECASE):
            log.info("Skipping PACKAGE %s.* -- package auto-conversion not supported", owner)
            return None

        # Normalize CREATE OR REPLACE
        text = re.sub(
            r"^\s*CREATE\s+OR\s+REPLACE\s+",
            "CREATE OR REPLACE ",
            text, count=1, flags=re.IGNORECASE,
        )
        if not re.match(r"CREATE\s+", text, re.IGNORECASE):
            text = "CREATE OR REPLACE " + text

        # ---- Step 1: sqlglot transpile ----
        pg_sql = self._sqlglot_transpile_function(text, owner)

        # ---- Step 2 / Fallback: regex fixups ----
        if pg_sql is None:
            log.debug("sqlglot transpile failed for %s; falling back to regex conversion", owner)
            pg_sql = self._regex_convert_function(text, owner)

        if pg_sql is None:
            return None

        # ---- Step 3: post-processing fixups applied to both paths ----
        pg_sql = self._postprocess_pg_function(pg_sql, owner)
        return pg_sql

    def _sqlglot_transpile_function(self, ddl: str, owner: str) -> Optional[str]:
        """Use sqlglot to transpile an Oracle CREATE FUNCTION/PROCEDURE to Postgres."""
        try:
            import sqlglot

            # sqlglot.transpile returns a list of SQL strings
            results = sqlglot.transpile(ddl, read="oracle", write="postgres")
            if not results:
                return None
            pg_sql = results[0]

            # sqlglot may not wrap PL/pgSQL body in $$ delimiters -- check and fix
            if "$$" not in pg_sql:
                pg_sql = self._wrap_plpgsql_body(pg_sql)

            # Force schema to lowercase owner
            pg_sql = re.sub(
                r'(CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+)'
                r'"?' + re.escape(owner.upper()) + r'"?\.',
                rf"\g<1>{owner.lower()}.",
                pg_sql, count=1, flags=re.IGNORECASE,
            )
            pg_sql = re.sub(
                r'(CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+)'
                r'"?' + re.escape(owner) + r'"?\.',
                rf"\g<1>{owner.lower()}.",
                pg_sql, count=1, flags=re.IGNORECASE,
            )

            log.debug("sqlglot transpile succeeded for %s (%d chars)", owner, len(pg_sql))
            return pg_sql
        except Exception as e:
            log.debug("sqlglot transpile error for %s: %s", owner, e)
            return None

    def _regex_convert_function(self, text: str, owner: str) -> Optional[str]:
        """Pure regex-based Oracle->PG function conversion (fallback)."""
        is_function = bool(re.search(r"\bFUNCTION\b", text, re.IGNORECASE))

        # Lowercase owner in the object name
        text = re.sub(
            r'(CREATE\s+OR\s+REPLACE\s+(?:FUNCTION|PROCEDURE)\s+)"?' + re.escape(owner) + r'"?\.',
            rf"\g<1>{owner.lower()}.",
            text, count=1, flags=re.IGNORECASE,
        )

        # Find the body boundary: IS|AS (followed by DECLARE or variable decls or BEGIN)
        body_match = re.search(r"\b(IS|AS)\s*\n", text, re.IGNORECASE)
        if not body_match:
            # Try IS/AS followed by anything (single-line)
            body_match = re.search(r"\b(IS|AS)\s+", text, re.IGNORECASE)
        if not body_match:
            return None

        signature = text[:body_match.start()]
        body_and_rest = text[body_match.end():]

        # Extract RETURN type from signature (Oracle: RETURN type, PG: RETURNS type)
        return_clause = ""
        if is_function:
            ret_match = re.search(r"\bRETURN\s+(\S+)", signature, re.IGNORECASE)
            if ret_match:
                return_clause = ret_match.group(1)
                signature = signature[:ret_match.start()].rstrip() + signature[ret_match.end():]

        # Parameter default := -> DEFAULT
        signature = re.sub(r"\s*:=\s*", " DEFAULT ", signature)

        # Build PG signature
        pg_sig = signature.rstrip()
        if return_clause:
            pg_sig += f"\nRETURNS {return_clause}"

        pg_sig += "\nLANGUAGE plpgsql\nAS $$\n"

        # Replace END function_name; with END;
        body_and_rest = re.sub(
            r"\bEND\s+[a-zA-Z_][a-zA-Z0-9_]*\s*;",
            "END;",
            body_and_rest, flags=re.IGNORECASE,
        )
        body_and_rest = body_and_rest.rstrip().rstrip(";").rstrip()

        return pg_sig + body_and_rest + ";\n$$;"

    def _wrap_plpgsql_body(self, pg_sql: str) -> str:
        """If sqlglot output has an IS/AS body but no $$ delimiters, wrap it."""
        # Find IS|AS boundary
        m = re.search(r"\b(AS|IS)\s+(?=DECLARE\b|BEGIN\b)", pg_sql, re.IGNORECASE)
        if not m:
            m = re.search(r"\b(AS|IS)\s*\n", pg_sql, re.IGNORECASE)
        if not m:
            return pg_sql

        before = pg_sql[:m.start()]
        keyword = m.group(1)
        after = pg_sql[m.end():]

        # Replace END func_name; with END;
        after = re.sub(
            r"\bEND\s+[a-zA-Z_][a-zA-Z0-9_]*\s*;",
            "END;",
            after, flags=re.IGNORECASE,
        )
        after = after.rstrip().rstrip(";").rstrip()

        # Check if LANGUAGE is already specified
        if not re.search(r"\bLANGUAGE\b", before, re.IGNORECASE):
            return before.rstrip() + "\nLANGUAGE plpgsql\nAS $$\n" + after + ";\n$$;"
        return before.rstrip() + "\nAS $$\n" + after + ";\n$$;"

    def _postprocess_pg_function(self, pg_sql: str, owner: str) -> str:
        """Apply common fixups to the transpiled PG function DDL."""
        # Oracle type -> PG type mappings
        type_map = [
            (r"\bNUMBER\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\)", lambda m: "NUMERIC" + m.group()[6:] if "," in m.group() else "NUMERIC"),
            (r"\bNUMBER\b", "NUMERIC"),
            (r"\bVARCHAR2\s*\([^)]*\)", "TEXT"),
            (r"\bVARCHAR2\b", "TEXT"),
            (r"\bNVARCHAR2\s*\([^)]*\)", "TEXT"),
            (r"\bCLOB\b", "TEXT"),
            (r"\bNCLOB\b", "TEXT"),
            (r"\bBLOB\b", "BYTEA"),
            (r"\bRAW\s*\([^)]*\)", "BYTEA"),
            (r"\bLONG\s+RAW\b", "BYTEA"),
            (r"\bLONG\b", "TEXT"),
            (r"\bPLS_INTEGER\b", "INTEGER"),
            (r"\bBINARY_INTEGER\b", "INTEGER"),
            (r"\bSIMPLE_INTEGER\b", "INTEGER"),
            (r"\bNATURAL\b", "INTEGER"),
            (r"\bNATURALN\b", "INTEGER"),
            (r"\bPOSITIVE\b", "INTEGER"),
            (r"\bPOSITIVEN\b", "INTEGER"),
            (r"\bSIGNTYPE\b", "INTEGER"),
        ]
        for pat, repl in type_map:
            if callable(repl):
                pg_sql = re.sub(pat, repl, pg_sql, flags=re.IGNORECASE)
            else:
                pg_sql = re.sub(pat, repl, pg_sql, flags=re.IGNORECASE)

        # Oracle %TYPE / %ROWTYPE -> TEXT (safe fallback; real type is unknown)
        pg_sql = re.sub(r"[a-zA-Z_][a-zA-Z0-9_.]*%TYPE\b", "TEXT", pg_sql, flags=re.IGNORECASE)
        pg_sql = re.sub(r"[a-zA-Z_][a-zA-Z0-9_.]*%ROWTYPE\b", "RECORD", pg_sql, flags=re.IGNORECASE)

        # Oracle builtins in the body
        pg_sql = re.sub(r"\bNVL\s*\(", "COALESCE(", pg_sql, flags=re.IGNORECASE)
        pg_sql = re.sub(r"\bNVL2\s*\(", "/* NVL2 */ COALESCE(", pg_sql, flags=re.IGNORECASE)
        pg_sql = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", pg_sql, flags=re.IGNORECASE)
        pg_sql = re.sub(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", pg_sql, flags=re.IGNORECASE)
        pg_sql = re.sub(r"\bDBMS_OUTPUT\.PUT_LINE\s*\(", "RAISE NOTICE '%', (", pg_sql, flags=re.IGNORECASE)
        pg_sql = re.sub(r"\bUSERENV\s*\(\s*'[^']*'\s*\)", "current_setting('application_name')", pg_sql, flags=re.IGNORECASE)

        # Remove Oracle pragmas
        pg_sql = re.sub(r"\bPRAGMA\s+[^;]+;", "", pg_sql, flags=re.IGNORECASE)

        # Oracle exception names -> PG
        pg_sql = re.sub(r"\bNO_DATA_FOUND\b", "NO_DATA_FOUND", pg_sql)
        pg_sql = re.sub(r"\bTOO_MANY_ROWS\b", "TOO_MANY_ROWS", pg_sql)
        pg_sql = re.sub(r"\bDUP_VAL_ON_INDEX\b", "UNIQUE_VIOLATION", pg_sql, flags=re.IGNORECASE)

        # Oracle RETURN -> PG RETURN (inside body, both work, but ensure consistency)
        # Oracle: RETURN expr;  PG: RETURN expr;  -- same syntax, no change needed

        # := is valid in PL/pgSQL, no change needed

        # CONCAT operator || is the same in both, no change needed

        # Ensure schema is lowercase
        pg_sql = re.sub(
            r'(CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+)"?' + re.escape(owner.upper()) + r'"?\.',
            rf"\g<1>{owner.lower()}.",
            pg_sql, count=1, flags=re.IGNORECASE,
        )

        return pg_sql

    def _resolve_and_create_function(
        self, func_name_full: str, parent_schema: str
    ) -> bool:
        """Fetch a missing function from Oracle, convert to PL/pgSQL, execute on PG.

        func_name_full may be 'schema.func' or just 'func'.
        Returns True if the function was created successfully.
        """
        func_key = func_name_full.lower()
        if func_key in self._created_functions:
            return True

        if "." in func_name_full:
            func_schema, func_name = func_name_full.split(".", 1)
        else:
            func_schema = parent_schema if parent_schema else ""
            func_name = func_name_full

        if not func_schema:
            log.warning("Cannot resolve function '%s': no schema context", func_name_full)
            return False

        log.info("Fetching function %s.%s from Oracle...", func_schema, func_name)
        ddl = self._fetch_function_ddl_from_oracle(func_schema, func_name)
        if not ddl:
            log.warning("No source found in Oracle for function %s.%s", func_schema, func_name)
            return False

        log.info("Converting function %s.%s to PL/pgSQL (%d chars)...", func_schema, func_name, len(ddl))
        pg_func = self._convert_oracle_function_to_pg(ddl, func_schema)
        if not pg_func:
            log.warning("Could not convert function %s.%s to PL/pgSQL", func_schema, func_name)
            return False

        self._ensure_schema(func_schema.lower())

        log.info("Executing function %s.%s on PostgreSQL...", func_schema.lower(), func_name.lower())
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(pg_func)
            self.pg_conn.commit()
            self._created_functions.add(func_key)
            self.stats.setdefault("functions_created", 0)
            self.stats["functions_created"] += 1
            log.info("Function %s.%s created successfully on PG", func_schema.lower(), func_name.lower())

            # Write the function SQL to output
            if self.output_dir:
                func_dir = self.output_dir / "functions"
                func_dir.mkdir(parents=True, exist_ok=True)
                safe = re.sub(r'[<>:"/\\|?*]', "_", f"{func_schema}_{func_name}".lower())
                (func_dir / f"{safe}.sql").write_text(
                    f"-- Source: {func_schema}.{func_name} (auto-fetched from Oracle)\n\n{pg_func}\n",
                    encoding="utf-8",
                )
            return True
        except Exception as e:
            self.pg_conn.rollback()
            log.warning("Failed to create function %s.%s on PG: %s", func_schema, func_name, str(e)[:300])
            return False

    # ------------------------------------------------------------------
    # Recursive dependency resolution
    # ------------------------------------------------------------------

    def _resolve_and_create_dependency(
        self, relation: str, parent_schema: str, depth: int
    ) -> bool:
        """Look up a missing PG relation in Oracle and create it.

        Returns True if the dependency was successfully created.
        """
        if depth > self.MAX_RESOLVE_DEPTH:
            log.warning("Max resolve depth (%d) reached for %s", self.MAX_RESOLVE_DEPTH, relation)
            return False

        kind, resolved_schema, resolved_name = self._lookup_in_oracle(relation, parent_schema)
        log.info(
            "Resolving missing relation '%s' (parent=%s): kind=%s target=%s.%s",
            relation, parent_schema, kind, resolved_schema, resolved_name,
        )

        if kind is None:
            log.warning("Cannot resolve relation '%s' in Oracle", relation)
            return False

        rs_lower = resolved_schema.lower()
        rn_lower = resolved_name.lower()

        if kind == "SYNONYM":
            target_type = None
            cached = self.schema_objects_cache.get(resolved_schema)
            if cached is not None:
                target_type = cached.get(resolved_name)
            else:
                target_type = get_oracle_object_type(
                    self.oracle_conn, resolved_schema, resolved_name
                )

            if target_type == "VIEW":
                tgt_key = (rs_lower, rn_lower)
                if tgt_key not in self.created:
                    log.info("Synonym target %s.%s is a VIEW; creating it first", resolved_schema, resolved_name)
                    if not self._migrate_single(rs_lower, rn_lower, depth + 1):
                        log.warning("Failed to create synonym target view %s.%s; cannot create synonym-view", rs_lower, rn_lower)
                        return False

            # Determine PG schema/name for the synonym-view.
            # Unqualified relations (public synonyms) go in "public" so search_path finds them.
            if "." in relation:
                syn_schema, syn_name = relation.split(".", 1)
            else:
                syn_schema = "public"
                syn_name = relation

            ok, err = self._create_synonym_view(syn_schema, syn_name, rs_lower, rn_lower)
            if ok:
                self.stats["deps_resolved"] += 1
                return True

            # Synonym-view creation failed (target might also be missing).
            # If target is a table that doesn't exist in PG, we can't help.
            log.warning(
                "Synonym-view %s.%s -> %s.%s failed: %s",
                syn_schema, syn_name, rs_lower, rn_lower, err[:200],
            )
            return False

        if kind == "VIEW":
            tgt_key = (rs_lower, rn_lower)
            if tgt_key in self.created:
                return True
            log.info("Missing relation '%s' is a VIEW in Oracle (%s.%s); creating it", relation, rs_lower, rn_lower)
            success = self._migrate_single(rs_lower, rn_lower, depth + 1)
            if success:
                self.stats["deps_resolved"] += 1

                # If the original relation name differs from the resolved name
                # (e.g., relation was unqualified or in a different schema),
                # also create a synonym-view so the original reference works.
                if "." in relation:
                    rel_schema, rel_name = relation.split(".", 1)
                else:
                    rel_schema = parent_schema if parent_schema else "public"
                    rel_name = relation

                if (rel_schema.lower(), rel_name.lower()) != (rs_lower, rn_lower):
                    ok, bridge_err = self._create_synonym_view(rel_schema, rel_name, rs_lower, rn_lower)
                    if not ok:
                        log.warning(
                            "Bridge synonym-view %s.%s -> %s.%s failed: %s",
                            rel_schema, rel_name, rs_lower, rn_lower, bridge_err[:200],
                        )

            return success

        if kind == "TABLE":
            log.warning(
                "Missing relation '%s' is a TABLE (%s.%s) in Oracle. "
                "Tables must be migrated separately.",
                relation, resolved_schema, resolved_name,
            )
            return False

        return False

    # ------------------------------------------------------------------
    # Core: migrate a single view (recursive)
    # ------------------------------------------------------------------

    def _migrate_single(self, schema: str, view_name: str, depth: int = 0) -> bool:
        """Fetch, convert, execute a single view with recursive dep resolution.

        Returns True on success.
        """
        key: ViewKey = (schema.lower(), view_name.lower())

        if key in self.created:
            return True
        if key in self.failed:
            return False
        if key in self._in_progress:
            log.warning("Circular dependency detected: %s.%s (skipping)", schema, view_name)
            return False

        self._in_progress.add(key)
        display = f"{schema}.{view_name}" if schema else view_name
        log.info("Migrating view: %s (depth=%d)", display, depth)

        try:
            self._ensure_schema(schema)

            pg_sql, warnings = self._fetch_and_convert(schema, view_name)
            for w in warnings:
                if w:
                    log.info("  [%s] %s", display, w)

            if pg_sql is None:
                log.error("Cannot obtain converted SQL for %s", display)
                self.failed.add(key)
                self._record_result(schema, view_name, False, "conversion_failed", warnings)
                return False

            success = self._execute_with_dep_resolution(schema, view_name, pg_sql, depth)

            if success:
                self.created.add(key)
                if depth > 0:
                    self._write_output(schema, view_name, success=True)
            else:
                self.failed.add(key)
                if depth > 0:
                    self._write_output(schema, view_name, success=False)
            return success
        except Exception as exc:
            log.error("Unexpected error migrating %s: %s", display, exc)
            self.failed.add(key)
            self._record_result(schema, view_name, False, "unexpected_error", [str(exc)])
            return False
        finally:
            self._in_progress.discard(key)

    def _execute_with_dep_resolution(
        self, schema: str, view_name: str, pg_sql: str, depth: int
    ) -> bool:
        """Execute CREATE VIEW on PG; on 'relation not found', resolve and retry."""
        display = f"{schema}.{view_name}" if schema else view_name
        seen_relations: set[str] = set()

        for attempt in range(1, self.MAX_RETRIES_PER_VIEW + 1):
            # Execute with retry: always handles function errors (replace with NULL);
            # column/relation removal gated by auto_remove_columns / we handle relations ourselves
            ok, err, final_sql, removed = execute_view_with_column_retry(
                self.pg_conn,
                pg_sql,
                schema,
                view_name,
                auto_remove_columns=self.auto_remove_columns,
                auto_remove_relations=False,  # we handle relations ourselves
            )
            if ok:
                if removed:
                    log.info("[%s] OK (auto-removed: %s)", display, ", ".join(removed))
                self._record_result(schema, view_name, True, "ok", removed_items=removed, final_sql=final_sql)
                return True
            pg_sql = final_sql  # use the cleaned-up SQL for further retries (e.g. after function replace)

            # Check for "schema X does not exist" -- create the schema and retry
            schema_miss = re.search(r'schema\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s+does\s+not\s+exist', err or "", re.IGNORECASE)
            if schema_miss:
                missing_schema = schema_miss.group(1)
                if missing_schema not in seen_relations:
                    seen_relations.add(missing_schema)
                    log.info("[%s] Attempt %d: schema '%s' does not exist; creating it", display, attempt, missing_schema)
                    self._ensure_schema(missing_schema)
                    continue
                # Already tried this schema, fall through to give up

            # Check for "function X does not exist" -- do NOT fetch from Oracle; record and fail
            missing_func = _parse_missing_function(err)
            if missing_func:
                self._record_result(
                    schema, view_name, False, "missing_function",
                    [f"Function does not exist (not fetched from source): {missing_func}", err[:500]], final_sql=pg_sql,
                )
                return False

            # Check for "relation does not exist"
            missing_rel = _parse_missing_relation(err)
            if not missing_rel:
                log.warning("[%s] Failed with non-relation error: %s", display, err[:200])
                self._record_result(schema, view_name, False, "execute_error", [err[:500]], final_sql=pg_sql)
                return False

            log.info(
                "[%s] Attempt %d: relation '%s' does not exist; resolving...",
                display, attempt, missing_rel,
            )

            if missing_rel in seen_relations:
                log.warning(
                    "[%s] Relation '%s' still missing after previous resolution attempt; giving up",
                    display, missing_rel,
                )
                self._record_result(
                    schema, view_name, False, "unresolvable_dep",
                    [f"Cannot resolve: {missing_rel}", err[:500]], final_sql=pg_sql,
                )
                return False
            seen_relations.add(missing_rel)

            resolved = self._resolve_and_create_dependency(missing_rel, schema, depth)
            if not resolved:
                log.warning("[%s] Could not resolve dependency '%s'", display, missing_rel)
                self._record_result(
                    schema, view_name, False, "dep_resolution_failed",
                    [f"Could not resolve: {missing_rel}", err[:500]], final_sql=pg_sql,
                )
                return False

            log.info("[%s] Dependency '%s' resolved; retrying view creation...", display, missing_rel)

        log.warning("[%s] Exceeded %d retries", display, self.MAX_RETRIES_PER_VIEW)
        self._record_result(schema, view_name, False, "max_retries", final_sql=pg_sql)
        return False

    # ------------------------------------------------------------------
    # Result tracking
    # ------------------------------------------------------------------

    def _record_result(
        self,
        schema: str,
        view_name: str,
        success: bool,
        status: str,
        messages: Optional[list[str]] = None,
        removed_items: Optional[list[str]] = None,
        final_sql: Optional[str] = None,
    ) -> None:
        self.results.append({
            "schema": schema,
            "view_name": view_name,
            "display": f"{schema}.{view_name}" if schema else view_name,
            "success": success,
            "status": status,
            "messages": messages or [],
            "removed_items": removed_items or [],
            "final_sql": final_sql or "",
        })

    # ------------------------------------------------------------------
    # Public API: migrate the full list with multi-pass retry
    # ------------------------------------------------------------------

    def migrate_all(self, view_list: list[tuple[str, str]]) -> None:
        """Migrate all views from the list.

        Does multiple passes: views that fail in pass N might succeed in
        pass N+1 if their dependencies were created by sibling views.
        """
        remaining = list(view_list)
        pass_num = 0
        max_passes = 5

        while remaining and pass_num < max_passes:
            pass_num += 1
            log.info("=== Pass %d: %d view(s) remaining ===", pass_num, len(remaining))
            print(f"\n--- Pass {pass_num}: {len(remaining)} view(s) to process ---", flush=True)

            still_failing: list[tuple[str, str]] = []
            progress_before = len(self.created)

            for i, (schema, view_name) in enumerate(remaining, 1):
                key = (schema.lower(), view_name.lower())
                display = f"{schema}.{view_name}" if schema else view_name

                if key in self.created:
                    print(f"  [{i}/{len(remaining)}] SKIP (already created): {display}", flush=True)
                    self.stats["skipped"] += 1
                    continue

                # Reset failed state for re-attempts in later passes
                self.failed.discard(key)

                print(f"  [{i}/{len(remaining)}] Migrating: {display} ...", end="", flush=True)
                success = self._migrate_single(schema, view_name)

                if success:
                    self.stats["ok"] += 1
                    print(" OK", flush=True)
                    self._write_output(schema, view_name, success=True)
                else:
                    still_failing.append((schema, view_name))
                    print(" FAIL", flush=True)
                    self._write_output(schema, view_name, success=False)

            new_created = len(self.created) - progress_before
            log.info(
                "Pass %d complete: %d created this pass, %d still failing",
                pass_num, new_created, len(still_failing),
            )
            print(
                f"  Pass {pass_num} done: {new_created} created, {len(still_failing)} failed",
                flush=True,
            )

            if not still_failing or new_created == 0:
                break  # no progress or nothing left

            remaining = still_failing

        # Mark anything still remaining as failed
        for schema, view_name in remaining if remaining else []:
            key = (schema.lower(), view_name.lower())
            if key not in self.created:
                self.stats["fail"] += 1

    # ------------------------------------------------------------------
    # Output file writing
    # ------------------------------------------------------------------

    def _write_output(self, schema: str, view_name: str, success: bool) -> None:
        if not self.output_dir:
            return

        safe_name = f"{schema}_{view_name}".replace(".", "_") if schema else view_name
        safe_name = re.sub(r'[<>:"/\\|?*]', "_", safe_name)

        last_result = None
        for r in reversed(self.results):
            if r["schema"].lower() == schema.lower() and r["view_name"].lower() == view_name.lower():
                last_result = r
                break

        sql = (last_result or {}).get("final_sql", "")
        if not sql:
            return

        if success:
            out_dir = self.output_dir / "success"
        else:
            out_dir = self.output_dir / "failed"
        out_dir.mkdir(parents=True, exist_ok=True)

        display = f"{schema}.{view_name}" if schema else view_name
        header = f"-- Source: {display}\n"
        if last_result and last_result.get("removed_items"):
            header += "-- Auto-removed: " + ", ".join(last_result["removed_items"]) + "\n"
        if not success and last_result:
            for msg in last_result.get("messages", []):
                header += f"-- {msg}\n"

        out_path = out_dir / f"{safe_name}.sql"
        out_path.write_text(header + "\n" + sql + "\n", encoding="utf-8")

    def print_summary(self) -> None:
        total = self.stats["ok"] + self.stats["fail"]
        total_created = len(self.created)
        dep_views = total_created - self.stats["ok"]
        print(f"\n{'='*60}", flush=True)
        print("Migration Summary", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"  Input views processed : {total}", flush=True)
        print(f"  Succeeded             : {self.stats['ok']}", flush=True)
        print(f"  Failed                : {self.stats['fail']}", flush=True)
        print(f"  Dep views auto-created: {dep_views}", flush=True)
        print(f"  Total views in PG     : {total_created}", flush=True)
        print(f"  Dependencies resolved : {self.stats['deps_resolved']}", flush=True)
        print(f"  Synonym-views created : {self.stats['synonyms_created']}", flush=True)
        print(f"  Functions created     : {self.stats.get('functions_created', 0)}", flush=True)
        if self.output_dir:
            print(f"  Output directory      : {self.output_dir}", flush=True)
        print(f"{'='*60}", flush=True)

        if self.failed:
            print("\nFailed views:", flush=True)
            for s, v in sorted(self.failed):
                display = f"{s}.{v}" if s else v
                reason = ""
                for r in reversed(self.results):
                    if r["schema"].lower() == s and r["view_name"].lower() == v:
                        reason = r.get("status", "")
                        msgs = r.get("messages", [])
                        if msgs:
                            reason += f" - {msgs[0][:100]}"
                        break
                print(f"  {display}: {reason}", flush=True)

    def write_error_log(self) -> None:
        if not self.output_dir:
            return
        failed_dir = self.output_dir / "failed"
        failed_dir.mkdir(parents=True, exist_ok=True)
        log_path = failed_dir / "error_log.txt"

        lines = ["view\tstatus\terror_message"]
        for r in self.results:
            if not r["success"]:
                msg = "; ".join(r.get("messages", []))[:500]
                lines.append(f"{r['display']}\t{r['status']}\t{msg}")

        if len(lines) > 1:
            log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            log.info("Error log written: %s (%d entries)", log_path, len(lines) - 1)


# ======================================================================
# CLI entry point
# ======================================================================


def _read_view_list(path: str) -> list[tuple[str, str]]:
    """Read view names from a text file. One per line: SCHEMA.VIEW_NAME or VIEW_NAME."""
    vl_path = Path(path)
    if not vl_path.exists():
        print(f"Error: view list file not found: {path}", file=sys.stderr, flush=True)
        sys.exit(1)

    entries: list[tuple[str, str]] = []
    with open(vl_path, encoding="utf-8-sig") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            if "." in ln:
                parts = ln.split(".", 1)
                entries.append((parts[0].strip(), parts[1].strip()))
            else:
                entries.append(("", ln.strip()))
    return entries


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recursive Oracle-to-PostgreSQL view migrator with automatic dependency resolution.",
    )
    parser.add_argument(
        "--view-list",
        required=True,
        metavar="FILE",
        help="Text file with view names (one per line: SCHEMA.VIEW_NAME)",
    )
    parser.add_argument(
        "--no-execute",
        action="store_true",
        default=False,
        help="Only convert and write SQL files to output dir (do not execute on PostgreSQL). "
             "Files are numbered sequentially in dependency order.",
    )
    parser.add_argument(
        "--no-qualify",
        action="store_true",
        default=False,
        help="Skip qualifying unqualified table/view refs with schema (default: qualify when Oracle connected).",
    )
    parser.add_argument("--oracle-user", default=None, help="Oracle user (or ORACLE_USER env)")
    parser.add_argument("--oracle-password", default=None, help="Oracle password (or ORACLE_PASSWORD env)")
    parser.add_argument("--oracle-dsn", default=None, help="Oracle DSN (or ORACLE_DSN env, default: localhost:1521/ORCL)")
    parser.add_argument("--pg-host", default=None, help="PostgreSQL host (or PG_HOST env)")
    parser.add_argument("--pg-port", type=int, default=None, help="PostgreSQL port (or PG_PORT env)")
    parser.add_argument("--pg-database", default=None, help="PostgreSQL database (or PG_DATABASE env)")
    parser.add_argument("--pg-user", default=None, help="PostgreSQL user (or PG_USER env)")
    parser.add_argument("--pg-password", default=None, help="PostgreSQL password (or PG_PASSWORD env)")
    parser.add_argument(
        "--output-dir",
        default="migrate_output",
        help="Output directory for SQL files (default: migrate_output)",
    )
    parser.add_argument(
        "--step-timeout",
        type=int,
        default=15,
        help="Timeout per conversion step in seconds (default: 15)",
    )
    parser.add_argument(
        "--auto-remove-columns",
        action="store_true",
        default=False,
        help="Auto-remove missing columns on PG execution errors (function errors always handled; ignored with --no-execute)",
    )
    parser.add_argument(
        "--synonym-csv",
        default=None,
        help="Use synonym CSV instead of Oracle query (columns: owner,synonym_name,table_owner,table_name)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path",
    )
    args = parser.parse_args()

    no_execute = args.no_execute

    # -- Logging setup --
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    log.setLevel(log_level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    if not log.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(fmt)
        log.addHandler(handler)
    if args.log_file:
        fh = logging.FileHandler(args.log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)

    # -- Read view list --
    view_list = _read_view_list(args.view_list)
    if not view_list:
        print("Error: view list is empty.", file=sys.stderr, flush=True)
        sys.exit(1)
    print(f"Loaded {len(view_list)} view(s) from {args.view_list}", flush=True)

    # -- Credentials --
    ora_user = args.oracle_user or os.environ.get("ORACLE_USER") or ORACLE_USER
    ora_pass = args.oracle_password or os.environ.get("ORACLE_PASSWORD") or ORACLE_PASSWORD
    ora_dsn = args.oracle_dsn or os.environ.get("ORACLE_DSN", ORACLE_DSN)

    pg_host = args.pg_host or os.environ.get("PG_HOST", PG_HOST)
    pg_port = args.pg_port if args.pg_port is not None else int(os.environ.get("PG_PORT", PG_PORT))
    pg_database = args.pg_database or os.environ.get("PG_DATABASE", PG_DATABASE)
    pg_user = args.pg_user or os.environ.get("PG_USER", PG_USER)
    pg_password = args.pg_password or os.environ.get("PG_PASSWORD", PG_PASSWORD)

    if not ora_user or not ora_pass:
        print("Error: Oracle credentials required (--oracle-user / --oracle-password or env vars).",
              file=sys.stderr, flush=True)
        sys.exit(1)

    # -- Connect to Oracle --
    print(f"Connecting to Oracle ({ora_dsn})...", flush=True)
    try:
        oracle_conn = _oracle_connect_with_retry(ora_user, ora_pass, ora_dsn)
    except Exception as e:
        print(f"Error: cannot connect to Oracle: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    print("  Oracle connected.", flush=True)

    # -- Load synonym map --
    print("Loading synonym map...", flush=True)
    t0 = time.perf_counter()
    synonym_map: dict[str, str] = {}
    if args.synonym_csv:
        import csv
        csv_path = Path(args.synonym_csv)
        if csv_path.exists():
            with open(csv_path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    owner = (row.get("owner") or "").strip().upper()
                    syn = (row.get("synonym_name") or "").strip().upper()
                    t_owner = (row.get("table_owner") or "").strip().upper()
                    t_name = (row.get("table_name") or "").strip().upper()
                    if syn and t_owner and t_name:
                        target = f"{t_owner}.{t_name}"
                        if owner:
                            synonym_map[f"{owner}.{syn}"] = target
                        if owner == "PUBLIC" or not owner:
                            synonym_map[syn] = target
            print(f"  Loaded {len(synonym_map)} synonym mappings from CSV in {time.perf_counter()-t0:.1f}s", flush=True)
        else:
            print(f"  Warning: CSV not found: {args.synonym_csv}", file=sys.stderr, flush=True)
    else:
        try:
            synonym_map = get_synonym_map(oracle_conn)
            print(f"  Loaded {len(synonym_map)} synonym mappings from Oracle in {time.perf_counter()-t0:.1f}s", flush=True)
        except Exception as e:
            print(f"  Warning: synonym fetch failed: {e}", file=sys.stderr, flush=True)

    # -- Build schema objects cache --
    unique_schemas = {s.upper() for s, _ in view_list if s.strip()}
    schema_objects_cache: dict[str, dict[str, str]] = {}
    if unique_schemas:
        print(f"Building schema objects cache for {len(unique_schemas)} schema(s)...", flush=True)
        for su in unique_schemas:
            try:
                schema_objects_cache[su] = get_oracle_schema_objects(oracle_conn, su)
                log.info("  Schema %s: %d objects cached", su, len(schema_objects_cache[su]))
            except Exception as e:
                log.warning("  Schema %s: cache failed: %s", su, e)
        print(f"  Cached {len(schema_objects_cache)} schema(s).", flush=True)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    t_start = time.perf_counter()

    # ================================================================
    # --no-execute: convert only, write sequentially numbered SQL files
    # ================================================================
    if no_execute:
        print("\n=== Convert-only mode (--no-execute) ===", flush=True)
        success_dir = output_dir / "success"
        failed_dir = output_dir / "failed"
        success_dir.mkdir(parents=True, exist_ok=True)
        failed_dir.mkdir(parents=True, exist_ok=True)

        view_keys_lower: set[ViewKey] = {((s or "").lower(), (v or "").lower()) for s, v in view_list}
        view_keys_by_name: dict[str, list[ViewKey]] = {}
        for (s, v) in view_keys_lower:
            view_keys_by_name.setdefault(v, []).append((s, v))

        converted: list[tuple[str, str, str, Optional[str], list[str], set]] = []
        n_ok = 0
        n_fail = 0
        pad = max(3, len(str(len(view_list))))

        for i, (schema, view_name) in enumerate(view_list, 1):
            display = f"{schema}.{view_name}" if schema else view_name
            print(f"  [{i:>{pad}}/{len(view_list)}] Converting: {display}", end="", flush=True)

            ddl = _fetch_view_ddl_from_oracle(oracle_conn, schema, view_name)
            if not ddl:
                print(" FAIL (no DDL)", flush=True)
                converted.append((schema, view_name, display, None, ["No DDL found in Oracle"], set()))
                n_fail += 1
                continue

            step_sec = args.step_timeout or 0
            try:
                pg_sql, warns, err_type, refs = _convert_single_view(
                    schema, view_name, ddl, synonym_map,
                    True,  # apply_oracle_conversions
                    bool(oracle_conn) and not args.no_qualify,  # do_qualify
                    oracle_conn, schema_objects_cache, step_sec,
                    None,  # qualify_lock
                    False,  # skip_deps=False to get dependency info
                    view_keys_lower, view_keys_by_name,
                )
                if err_type:
                    print(f" FAIL ({err_type})", flush=True)
                    converted.append((schema, view_name, display, None, [w.strip() for w in warns] + [err_type], set()))
                    n_fail += 1
                else:
                    print(" OK", flush=True)
                    converted.append((schema, view_name, display, pg_sql, [w.strip() for w in warns], refs))
                    n_ok += 1
            except Exception as e:
                print(f" FAIL ({e})", flush=True)
                converted.append((schema, view_name, display, None, [str(e)], set()))
                n_fail += 1

        # Topological sort for dependency order
        views_for_sort = [(s, v) for s, v, *_ in converted]
        deps_for_sort = [c[5] for c in converted]
        order = topological_sort(views_for_sort, deps_for_sort)
        if order != list(range(len(view_list))):
            print("  Writing in dependency order.", flush=True)

        dep_pad = max(3, len(str(len(order))))
        error_log_lines: list[str] = []

        for rank, idx in enumerate(order):
            schema, view_name, display, pg_sql, warns, _ = converted[idx]
            prefix = f"{(rank + 1):0{dep_pad}d}_"
            safe_name = f"{schema}_{view_name}".replace(".", "_") if schema else view_name
            safe_name = re.sub(r'[<>:"/\\|?*]', "_", safe_name)

            if pg_sql:
                header = f"-- Source: {display}\n"
                for w in warns:
                    if w:
                        header += f"-- WARNING: {w}\n"
                out_path = success_dir / f"{prefix}{safe_name}.sql"
                out_path.write_text(header + "\n" + pg_sql + "\n", encoding="utf-8")
            else:
                header = f"-- Source: {display}\n"
                for w in warns:
                    header += f"-- ERROR: {w}\n"
                out_path = failed_dir / f"{prefix}{safe_name}.sql"
                ddl_fallback = _fetch_view_ddl_from_oracle(oracle_conn, schema, view_name) or ""
                out_path.write_text(header + "\n" + ddl_fallback + "\n", encoding="utf-8")
                error_log_lines.append(f"{display}\t{'; '.join(warns)[:500]}")

        if error_log_lines:
            log_path = failed_dir / "error_log.txt"
            log_path.write_text("view\terror_message\n" + "\n".join(error_log_lines) + "\n", encoding="utf-8")

        elapsed = time.perf_counter() - t_start
        print(f"\n{'='*60}", flush=True)
        print(f"Convert-only Summary (--no-execute)", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"  Total views       : {len(view_list)}", flush=True)
        print(f"  Converted OK      : {n_ok}", flush=True)
        print(f"  Failed            : {n_fail}", flush=True)
        print(f"  Success dir       : {success_dir}", flush=True)
        print(f"  Failed dir        : {failed_dir}", flush=True)
        print(f"  Total time        : {elapsed:.1f}s ({elapsed/60:.1f} min)", flush=True)
        print(f"{'='*60}", flush=True)

        try:
            oracle_conn.close()
        except Exception:
            pass
        if n_fail > 0:
            sys.exit(1)
        return

    # ================================================================
    # Execute mode (default): convert + execute on PG with dep resolution
    # ================================================================
    print(f"Connecting to PostgreSQL ({pg_host}:{pg_port}/{pg_database})...", flush=True)
    try:
        import psycopg2
        pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_database,
            user=pg_user,
            password=pg_password,
            options="-c statement_timeout=120000 -c lock_timeout=30000",
        )
    except Exception as e:
        print(f"Error: cannot connect to PostgreSQL: {e}", file=sys.stderr, flush=True)
        oracle_conn.close()
        sys.exit(1)
    print("  PostgreSQL connected.", flush=True)

    migrator = RecursiveViewMigrator(
        oracle_conn=oracle_conn,
        pg_conn=pg_conn,
        synonym_map=synonym_map,
        schema_objects_cache=schema_objects_cache,
        step_timeout=args.step_timeout,
        auto_remove_columns=args.auto_remove_columns,
        no_qualify=args.no_qualify,
        output_dir=output_dir,
    )

    migrator.migrate_all(view_list)
    elapsed = time.perf_counter() - t_start

    # -- Summary --
    migrator.print_summary()
    migrator.write_error_log()
    print(f"\nTotal time: {elapsed:.1f}s ({elapsed/60:.1f} min)", flush=True)

    # -- Cleanup --
    try:
        oracle_conn.close()
    except Exception:
        pass
    try:
        pg_conn.close()
    except Exception:
        pass

    if migrator.stats["fail"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

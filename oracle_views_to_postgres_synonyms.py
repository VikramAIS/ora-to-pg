#!/usr/bin/env python3
"""
Oracle views → PostgreSQL with synonym resolution and Oracle→PG conversions.

STANDALONE: This script has no dependency on any other project file. It only uses the Python
stdlib and optional pip packages: oracledb (Oracle), sqlglot (parse/transpile), psycopg2 (PostgreSQL).
Run: python oracle_views_to_postgres_synonyms.py view_list.txt [options]

- Reads view names from text file(s) (one view per line: schemaname.viewname or viewname).
- Fetches synonym map from Oracle (dba_synonyms or all_synonyms) or from --synonym-csv.
- Fetches each view DDL from Oracle (DBMS_METADATA.GET_DDL).
- Rewrites table references: synonyms → real schema.table using sqlglot.
- Transpiles Oracle DDL to PostgreSQL with sqlglot.
- Applies Oracle→PostgreSQL conversions in this script (ROWID→ctid, NVL/NVL2/DECODE, TO_*, USERENV,
  SYSDATE/SYSTIMESTAMP, TRUNC, sequence.NEXTVAL/CURRVAL, date/timestamp arithmetic, empty string as NULL,
  LISTAGG/REGEXP_LIKE/REGEXP_SUBSTR, BITAND/BITOR, etc.). Use --no-convert-oracle to skip body conversions.
- Executes each CREATE VIEW on PostgreSQL (optionally in dependency order).
- Writes successful view SQL to one folder, failed view SQL to another folder.

Limitations:
- If views depend on other views in the list, dependency ordering is used when executing (unless --no-dependency-order).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

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
    "czdvcons", "fnd_attachment_util_pkg", "gl_alloc_batches_pkg", "hr_chkfmt",
    "hr_general", "hr_gbbal", "mrp_launch_plan_pk", "oe_query", "pa_billing",
    "pa_budget_upgrade_pkg", "pa_get_resource", "pa_security", "pa_status",
    "pa_task_utils", "pa_utils", "po_vendors_ap_pkg", "shp_picking_headers_pkg",
})

# PostgreSQL reserved keywords (keep uppercase when lowercasing identifiers)
PG_RESERVED = frozenset({
    "all", "and", "any", "as", "asc", "between", "both", "by", "case", "cast",
    "collate", "coalesce", "cross", "current_date", "current_time", "current_timestamp",
    "current_user", "desc", "distinct", "else", "end", "except", "exists", "false",
    "for", "from", "full", "group", "having", "in", "inner", "intersect", "into",
    "join", "leading", "left", "like", "limit", "localtime", "localtimestamp",
    "natural", "not", "null", "offset", "on", "or", "order", "outer", "overlaps",
    "right", "select", "session_user", "similar", "some", "table", "then", "to",
    "trailing", "true", "union", "unique", "user", "using", "when", "where", "with",
})


def load_view_list(input_path: str) -> list[tuple[str, str]]:
    """Load view names from file. Each line: schemaname.viewname or viewname. Returns [(schema, view), ...]."""
    views = []
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "." in line:
            parts = line.split(".", 1)
            schema, view = parts[0].strip(), parts[1].strip()
            if view:
                views.append((schema, view))
        else:
            if line:
                views.append(("", line))
    return views


def load_view_list_from_files(paths: list[str]) -> list[tuple[str, str]]:
    """Load and merge view lists from multiple files. Deduplicates while preserving order."""
    seen: set[tuple[str, str]] = set()
    out = []
    for p in paths:
        for v in load_view_list(p):
            if v not in seen:
                seen.add(v)
                out.append(v)
    return out


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


def get_oracle_ddl(connection, schema: str, view_name: str) -> str:
    """Fetch view DDL from Oracle using DBMS_METADATA.GET_DDL."""
    cursor = connection.cursor()
    try:
        try:
            import oracledb
            def output_type_handler(cursor, name, default_type, size, precision, scale):
                if default_type == oracledb.DB_TYPE_CLOB:
                    return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
            cursor.outputtypehandler = output_type_handler
        except Exception:
            pass
        schema_param = schema or None
        if schema_param:
            cursor.execute(
                "SELECT DBMS_METADATA.GET_DDL('VIEW', :vname, :sname) FROM DUAL",
                {"vname": view_name.upper(), "sname": schema.upper()},
            )
        else:
            cursor.execute(
                "SELECT DBMS_METADATA.GET_DDL('VIEW', :vname) FROM DUAL",
                {"vname": view_name.upper()},
            )
        row = cursor.fetchone()
        if not row or not row[0]:
            display = f"{schema}.{view_name}" if schema else view_name
            raise ValueError(f"No DDL returned for {display}")
        return (row[0].read() if hasattr(row[0], "read") else row[0]) or ""
    finally:
        cursor.close()


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



def _first_statement_only(sql: str) -> str:
    """Return the first SQL statement (strip trailing semicolons/comments that can break parse_one)."""
    sql = sql.strip()
    # Remove trailing semicolon so parse_one sees one statement
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _remove_outer_join_plus(text: str) -> str:
    """Remove Oracle (+) outer join notation so parsers and PG don't error (semantics become inner join)."""
    return re.sub(r"\(\s*\+\s*\)", "", text, flags=re.IGNORECASE)


def _replace_to_func_for_oracle_parse(
    sql: str, func: str, cast_type: str
) -> str:
    """Replace TO_*(expr [, ...]) with CAST(expr AS type) in Oracle SQL so parser doesn't require format."""
    pattern = re.compile(r"\b" + re.escape(func) + r"\s*\(", re.IGNORECASE)
    while True:
        match = pattern.search(sql)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(sql, start_paren)
        if close is None:
            break
        inner = sql[start_paren + 1 : close].strip()
        first = inner.split(",", 1)[0].strip() if "," in inner else inner
        repl = f"CAST({first} AS {cast_type})"
        sql = sql[: match.start()] + repl + sql[close + 1 :]
    return sql


def _strip_oracle_hints(sql: str) -> str:
    """Remove Oracle optimizer hints so sqlglot does not emit 'Hints are not supported'. Hints: /*+ ... */ and --+ ."""
    # Block hints: /*+ ... */ (non-greedy to first */)
    sql = re.sub(r"/\*\+\s*.*?\*/", " ", sql, flags=re.DOTALL)
    # Single-line hint: --+ rest of line
    sql = re.sub(r"--\+[^\n]*", "", sql)
    return sql


def _preprocess_oracle_before_parse(sql: str) -> str:
    """Preprocess Oracle DDL so sqlglot can parse (strip hints, remove (+), fix TO_* that need format)."""
    sql = _strip_oracle_hints(sql)
    sql = _remove_outer_join_plus(sql)
    sql = _replace_to_func_for_oracle_parse(sql, "TO_NUMBER", "NUMBER")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_CHAR", "VARCHAR2(4000)")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_DATE", "DATE")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_TIMESTAMP", "TIMESTAMP")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_CLOB", "CLOB")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_BLOB", "BLOB")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_NCLOB", "NCLOB")
    return sql


def rewrite_sql_with_synonyms(
    sql: str,
    synonym_map: dict[str, str],
    view_schema: str = "",
) -> str:
    """
    Parse Oracle SQL with sqlglot, replace table references using synonym_map,
    then transpile to PostgreSQL.
    synonym_map: "TABLE" or "SCHEMA.TABLE" -> "REAL_SCHEMA.REAL_TABLE"
    view_schema: Oracle schema of the view; used to resolve private synonyms for unqualified table refs.
    """
    import sqlglot
    from sqlglot import exp

    sql = _first_statement_only(sql)
    sql = _preprocess_oracle_before_parse(sql)
    if not synonym_map:
        tree = sqlglot.parse_one(sql, read="oracle")
        return tree.sql(dialect="postgres")

    tree = sqlglot.parse_one(sql, read="oracle")
    view_schema_upper = (view_schema or "").upper()

    for table in tree.find_all(exp.Table):
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

    return tree.sql(dialect="postgres")


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
    """Replace Oracle EBS package references with apps. to fix 'package namespace does not exist'."""
    for pkg in ORACLE_EBS_PACKAGES:
        body = re.sub(r"\b" + re.escape(pkg) + r"\.", "apps.", body, flags=re.IGNORECASE)
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
                repl = f"({expr})::text"
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
    return body


def _replace_oracle_builtin_functions_in_body(body: str) -> str:
    """Replace Oracle built-in/custom functions not in PG: MONTHS_BETWEEN, ADD_MONTHS, ROUND(_,_), LPAD(_,_), ap_round_currency, cs_get_serviced_status."""
    def replace_func(body: str, func_name: str, replacer) -> str:
        pattern = re.compile(r"\b" + re.escape(func_name) + r"\s*\(", re.IGNORECASE)
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
            try:
                repl = replacer(args)
            except Exception:
                break
            if repl is None:
                break
            body = body[: match.start()] + repl + body[close + 1 :]
        return body

    # MONTHS_BETWEEN(ts1, ts2)
    body = replace_func(body, "MONTHS_BETWEEN", lambda a: f"((( {a[0].strip()} )::timestamp::date - ( {a[1].strip()} )::timestamp::date) / 30.0)" if len(a) >= 2 else None)
    # ADD_MONTHS(ts, n)
    body = replace_func(body, "ADD_MONTHS", lambda a: f"(( {a[0].strip()} )::timestamp + ( {a[1].strip()} )::int * INTERVAL '1 month')" if len(a) >= 2 else None)
    # ROUND(x, n) -> round((x)::numeric, (n)::int)
    body = replace_func(body, "ROUND", lambda a: f"round(( {a[0].strip()} )::numeric, ( {a[1].strip()} )::int)" if len(a) >= 2 else None)
    # LPAD(s, n) or LPAD(s, n, pad) -> lpad(::text, ::int [, ::text])
    body = replace_func(body, "LPAD", lambda a: (f"lpad(( {a[0].strip()} )::text, ( {a[1].strip()} )::int" + (f", ( {a[2].strip()} )::text)" if len(a) >= 3 else "") + ")") if len(a) >= 2 else None)
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


def _replace_trunc_in_body(body: str) -> str:
    """Oracle TRUNC(date [, format]) -> date_trunc; TRUNC(numeric, n) -> trunc(numeric, n)."""
    pattern = re.compile(r"\bTRUNC\s*\(", re.IGNORECASE)
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
        if not args:
            break
        expr = args[0].strip()
        if len(args) >= 2:
            second = args[1].strip()
            if second.isdigit() or (second.startswith("-") and second[1:].isdigit()):
                body = body[: match.start()] + f"trunc(({expr})::numeric, {second})" + body[close + 1 :]
                continue
            fmt = second.upper().strip("'\"")
            if fmt in ("DD", "D", "DAY", "DY"):
                body = body[: match.start()] + f"date_trunc('day', ({expr})::timestamp)" + body[close + 1 :]
            elif fmt in ("MM", "MON", "MONTH"):
                body = body[: match.start()] + f"date_trunc('month', ({expr})::timestamp)" + body[close + 1 :]
            elif fmt in ("YYYY", "YEAR", "YY", "Y"):
                body = body[: match.start()] + f"date_trunc('year', ({expr})::timestamp)" + body[close + 1 :]
            elif fmt in ("Q", "QUARTER"):
                body = body[: match.start()] + f"date_trunc('quarter', ({expr})::timestamp)" + body[close + 1 :]
            elif fmt in ("HH", "HH12", "HH24"):
                body = body[: match.start()] + f"date_trunc('hour', ({expr})::timestamp)" + body[close + 1 :]
            elif fmt in ("MI", "MINUTE"):
                body = body[: match.start()] + f"date_trunc('minute', ({expr})::timestamp)" + body[close + 1 :]
            else:
                body = body[: match.start()] + f"date_trunc('day', ({expr})::timestamp)" + body[close + 1 :]
        else:
            body = body[: match.start()] + f"date_trunc('day', ({expr})::timestamp)" + body[close + 1 :]
    return body


def _replace_oracle_misc_in_body(body: str) -> str:
    """Oracle→PG: SYSDATE/SYSTIMESTAMP, MINUS→EXCEPT, ROWNUM, INSTR(2-arg)→strpos, LENGTHB→octet_length, RPAD, sequences, etc."""
    body = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = re.sub(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = re.sub(r"\s+MINUS\s+", " EXCEPT ", body, flags=re.IGNORECASE)
    body = re.sub(r"\bROWNUM\b", "(ROW_NUMBER() OVER ())", body, flags=re.IGNORECASE)
    body = re.sub(r"\bLENGTHB\s*\(", "octet_length(", body, flags=re.IGNORECASE)
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
    pattern = re.compile(r"\bINSTR\s*\(", re.IGNORECASE)
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
            s, sub = args[0].strip(), args[1].strip()
            body = body[: match.start()] + f"strpos(( {s} )::text, ( {sub} )::text)" + body[close + 1 :]
        else:
            break
    # RPAD(s, n) or RPAD(s, n, pad) -> rpad(::text, ::int [, ::text])
    pattern = re.compile(r"\bRPAD\s*\(", re.IGNORECASE)
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
            s, n = args[0].strip(), args[1].strip()
            rpad_tail = f", ( {args[2].strip()} )::text)" if len(args) >= 3 else ")"
            body = body[: match.start()] + f"rpad(( {s} )::text, ( {n} )::int{rpad_tail}" + body[close + 1 :]
        else:
            break
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
    # NEXT_DAY(date, weekday) -> date + 7 days (stub)
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
        if args:
            d = args[0].strip()
            body = body[: match.start()] + f"(( {d} )::date + INTERVAL '7 days')" + body[close + 1 :]
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
    return body


def _replace_pg_unit_dd_and_substring(body: str) -> str:
    """Fix PG-incompatible patterns: date_trunc('dd',...) -> 'day'; INTERVAL 'N dd' -> 'N day'; substring(_,_,len) -> GREATEST(len,0)."""
    body = re.sub(r"date_trunc\s*\(\s*['\"]dd['\"]\s*,", "date_trunc('day',", body, flags=re.IGNORECASE)
    # PostgreSQL INTERVAL uses 'day' not 'dd' (unit "dd" not recognized for type timestamp with time zone)
    body = re.sub(r"INTERVAL\s*'\s*([\d.]+)\s*dd\s*'", r"INTERVAL '\1 day'", body, flags=re.IGNORECASE)
    body = re.sub(r"INTERVAL\s*\"\s*([\d.]+)\s*dd\s*\"", r"INTERVAL '\1 day'", body, flags=re.IGNORECASE)
    for func in ("substring", "substr"):
        pattern = re.compile(r"\b" + func + r"\s*\(", re.IGNORECASE)
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
            if len(args) < 3:
                break
            expr, start_arg, length_arg = args[0].strip(), args[1].strip(), args[2].strip()
            new_inner = f"{expr}, {start_arg}, GREATEST({length_arg}, 0)"
            body = body[: match.start()] + func + "(" + new_inner + ")" + body[close + 1 :]
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
    """Oracle: empty string '' equals NULL in comparisons. Convert column = '' -> column IS NULL (and <> '' -> IS NOT NULL)."""
    col_ref = r"\b(?:[a-zA-Z_][a-zA-Z0-9_]*)(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?"
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
            continue
        part = re.sub(rf"({col_ref})\s*=\s*''", r"\1 IS NULL", part, flags=re.IGNORECASE)
        part = re.sub(rf'({col_ref})\s*=\s*""', r"\1 IS NULL", part, flags=re.IGNORECASE)
        part = re.sub(rf"({col_ref})\s*<>\s*''", r"\1 IS NOT NULL", part, flags=re.IGNORECASE)
        part = re.sub(rf"({col_ref})\s*!=\s*''", r"\1 IS NOT NULL", part, flags=re.IGNORECASE)
        part = re.sub(rf'({col_ref})\s*<>\s*""', r"\1 IS NOT NULL", part, flags=re.IGNORECASE)
        part = re.sub(rf'({col_ref})\s*!=\s*""', r"\1 IS NOT NULL", part, flags=re.IGNORECASE)
        result.append(part)
    return "".join(result)


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
    """Remove double quotes from quoted identifiers; skip inside single-quoted strings."""
    def unquote(match):
        return match.group(1).replace('""', '"')
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
    """Ensure FROM, WHERE, GROUP, ORDER, etc. are preceded by space; GROUP BY / ORDER BY spaced."""
    keywords = r"(FROM|WHERE|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|EXCEPT|INTERSECT)(?=\W|$)"
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            part = re.sub(rf"([^\s])({keywords})", r"\1 \2", part, flags=re.IGNORECASE)
            part = re.sub(r"\bGROUP\s*BY\b", "GROUP BY", part, flags=re.IGNORECASE)
            part = re.sub(r"\bORDER\s*BY\b", "ORDER BY", part, flags=re.IGNORECASE)
            part = re.sub(r"(GROUP)(BY)(?=\W|$)", r"\1 \2", part, flags=re.IGNORECASE)
            part = re.sub(r"(ORDER)(BY)(?=\W|$)", r"\1 \2", part, flags=re.IGNORECASE)
            result.append(part)
    return "".join(result)


def _fix_timestamp_plus_integer(body: str) -> str:
    """Oracle: date/timestamp + number = add days. PG needs interval. Converts + N, + 1.5, + numeric_col to * INTERVAL '1 day'."""
    date_funcs = r"(?:CURRENT_TIMESTAMP|SYSDATE|LOCALTIMESTAMP|CURRENT_DATE|LOCALTIME|CURRENT_TIME)"
    # + integer or decimal literal
    body = re.sub(rf"({date_funcs})\s*\+\s*(-?\d+(?:\.\d*)?)\b", r"\1 + \2 * INTERVAL '1 day'", body, flags=re.IGNORECASE)
    body = re.sub(rf"({date_funcs})\s*-\s*(\d+(?:\.\d*)?)\b", r"\1 - (\2)::numeric * INTERVAL '1 day'", body, flags=re.IGNORECASE)
    # + numeric column/identifier (fixes 'operator does not exist: timestamp without time zone + numeric')
    def _replace_plus_id(m):
        if m.group(2).upper() == "INTERVAL":
            return m.group(0)
        return m.group(1) + " + (" + m.group(2) + ")::numeric * INTERVAL '1 day'"
    body = re.sub(
        rf"({date_funcs})\s*\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\*\s*INTERVAL)",
        _replace_plus_id,
        body,
        flags=re.IGNORECASE,
    )
    # date_trunc(...) + N / - N / + numeric_col
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
        plus_id = re.match(r"\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\*\s*INTERVAL)", after)
        minus_m = re.match(r"-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        span_start = match.start()
        def span_len(m):
            return (close + 1 - span_start) + (len(after_raw) - len(after)) + len(m.group(0))
        if plus_m:
            body = body[: span_start] + body[span_start : close + 1] + f" + {plus_m.group(1)} * INTERVAL '1 day'" + body[span_start + span_len(plus_m) :]
        elif plus_id and plus_id.group(1).upper() != "INTERVAL":
            body = body[: span_start] + body[span_start : close + 1] + f" + ({plus_id.group(1)})::numeric * INTERVAL '1 day'" + body[span_start + span_len(plus_id) :]
        elif minus_m:
            body = body[: span_start] + body[span_start : close + 1] + f" - ({minus_m.group(1)})::numeric * INTERVAL '1 day'" + body[span_start + span_len(minus_m) :]
        else:
            break
    # (expr)::date or ::timestamp + N / - N / + numeric_col
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
        plus_id = re.match(r"\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\*\s*INTERVAL)", after)
        minus_m = re.match(r"-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        if plus_m:
            span_len = match.end() - open_pos + (len(after_raw) - len(after)) + len(plus_m.group(0))
            body = body[: open_pos] + body[open_pos : match.end()] + f" + {plus_m.group(1)} * INTERVAL '1 day'" + body[open_pos + span_len :]
        elif plus_id and plus_id.group(1).upper() != "INTERVAL":
            span_len = match.end() - open_pos + (len(after_raw) - len(after)) + len(plus_id.group(0))
            body = body[: open_pos] + body[open_pos : match.end()] + f" + ({plus_id.group(1)})::numeric * INTERVAL '1 day'" + body[open_pos + span_len :]
        elif minus_m:
            span_len = match.end() - open_pos + (len(after_raw) - len(after)) + len(minus_m.group(0))
            body = body[: open_pos] + body[open_pos : match.end()] + f" - ({minus_m.group(1)})::numeric * INTERVAL '1 day'" + body[open_pos + span_len :]
        else:
            break
    # to_timestamp(...) + N / - N / + numeric_col
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
        plus_id = re.match(r"\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\*\s*INTERVAL)", after)
        minus_m = re.match(r"-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", after)
        if plus_m:
            span_len = (close + 1 - abs_start) + (len(after_raw) - len(after)) + len(plus_m.group(0))
            body = body[: abs_start] + body[abs_start : close + 1] + f" + {plus_m.group(1)} * INTERVAL '1 day'" + body[abs_start + span_len :]
        elif plus_id and plus_id.group(1).upper() != "INTERVAL":
            span_len = (close + 1 - abs_start) + (len(after_raw) - len(after)) + len(plus_id.group(0))
            body = body[: abs_start] + body[abs_start : close + 1] + f" + ({plus_id.group(1)})::numeric * INTERVAL '1 day'" + body[abs_start + span_len :]
        elif minus_m:
            span_len = (close + 1 - abs_start) + (len(after_raw) - len(after)) + len(minus_m.group(0))
            body = body[: abs_start] + body[abs_start : close + 1] + f" - ({minus_m.group(1)})::numeric * INTERVAL '1 day'" + body[abs_start + span_len :]
        else:
            break
    return body


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
            from_match = re.match(r"\bFROM\b", body[i:], re.IGNORECASE)
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
    alias_re = re.compile(r"\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$", re.IGNORECASE)
    implicit_alias_re = re.compile(r"\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$")
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
            alias = m.group(1)
            alias_lower = alias.lower()
            alias_count[alias_lower] = alias_count.get(alias_lower, 0) + 1
            count = alias_count[alias_lower]
            out_alias = _quote_alias_if_reserved(alias if count == 1 else f"{alias}_{count}")
            if count > 1:
                new_part = alias_re.sub(f" AS {out_alias}", part_stripped, count=1)
                new_parts.append(new_part)
            else:
                if alias != out_alias:
                    new_part = alias_re.sub(f" AS {out_alias}", part_stripped, count=1)
                    new_parts.append(new_part)
                else:
                    new_parts.append(part_stripped)
        else:
            # Only treat trailing identifier as implicit alias when part is not a subquery/expression
            # (e.g. "(SELECT 1 FROM t)" would otherwise get " t" -> " AS t" -> invalid "FROM AS t")
            imm = implicit_alias_re.search(part_stripped) if "(" not in part_stripped else None
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
                unnamed_col_index[0] += 1
                new_parts.append(part_stripped + f" AS col_{unnamed_col_index[0]}")
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


def normalize_view_script(pg_ddl: str, apply_oracle_conversions: bool = True) -> str:
    """
    Normalize view DDL to: DROP VIEW IF EXISTS name; CREATE OR REPLACE VIEW name AS body;
    (no FORCE). View name and column names are lowercased.
    If apply_oracle_conversions is True (default), apply all Oracle→PG body conversions.
    If False (e.g. --no-convert-oracle), only do structure and view name.
    """
    if not pg_ddl or not pg_ddl.strip():
        return pg_ddl
    text = pg_ddl.replace("\r\n", "\n").replace("\r", "\n").strip()
    text = re.sub(r"^\s*DROP\s+VIEW\s+IF\s+EXISTS\s+[^;]+;\s*", "", text, flags=re.IGNORECASE)
    text = text.strip()
    create_match = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+(.+?)\s+(?:\([^)]*\)\s+)?AS\s+(.*)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if not create_match:
        return pg_ddl
    raw_name = create_match.group(1).strip()
    body = create_match.group(2)
    view_name = raw_name.replace('"', "").strip()
    view_name = re.sub(r"\s*\.\s*", ".", view_name).lower()
    if not view_name:
        view_name = "view_name"
    body = re.sub(r"\s*BEQUEATH\s+(?:DEFINER|CURRENT_USER)\s*", " ", body, flags=re.IGNORECASE)
    body = re.sub(r"\s*WITH\s+READ\s+ONLY\s*;?\s*$", "", body, flags=re.IGNORECASE)
    body = body.rstrip().rstrip(";").strip()
    body = re.sub(r"\s+", " ", body).strip()
    if apply_oracle_conversions:
        try:
            body = _decode_html_entities_in_body(body)
            body = _repair_identifier_space_before_dot(body)
            body = _remove_outer_join_plus(body)
            body = _replace_nvl_nvl2_decode_in_body(body)
            body = _replace_oracle_to_functions_in_body(body)
            body = _replace_oracle_builtin_functions_in_body(body)
            body = _replace_trunc_in_body(body)
            body = _replace_pg_unit_dd_and_substring(body)
            body = _replace_oracle_misc_in_body(body)
            body = _replace_oracle_sequence_refs(body)
            body = _replace_rowid_to_ctid(body)
            body = _replace_userenv_to_postgres(body)
            body = _replace_oracle_package_schemas(body)
            body = _replace_package_in_from_clause(body)
            body = _replace_dual_with_pg(body)
            body = _convert_start_with_connect_by_to_recursive_cte(body)
            body = _fix_timestamp_plus_integer(body)
            body = _oracle_empty_string_as_null(body)
            body = _empty_string_to_null_for_datetime(body)
            body = _remove_quotes_from_columns(body)
            body = _deduplicate_select_aliases_in_body(body)
            body = _cast_numeric_string_literals_in_equality(body)
            body = _lowercase_body_identifiers(body)
            body = _ensure_space_before_keywords(body)
        except Exception:
            pass
    else:
        body = _lowercase_body_identifiers(body)
    return f"DROP VIEW IF EXISTS {view_name};\nCREATE OR REPLACE VIEW {view_name} AS\n{body}\n;"


# Type alias for (schema, view_name) for dependency tracking
ViewKey = tuple[str, str]


def _extract_view_refs_from_body(body: str, view_keys: set[ViewKey]) -> set[ViewKey]:
    """
    Find FROM/JOIN references in body that are in view_keys (our view list).
    body = SELECT part after AS. Returns set of (schema, view_name) that this view depends on.
    """
    refs: set[ViewKey] = set()
    # Match FROM ref, JOIN ref (optionally schema.table)
    pattern = re.compile(
        r"\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+"
        r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
        re.IGNORECASE,
    )
    for m in pattern.finditer(body):
        ref = m.group(1).strip()
        if "." in ref:
            s, v = ref.split(".", 1)
            key = (s.strip().lower(), v.strip().lower())
        else:
            key = ("", ref.strip().lower())
        if key in view_keys:
            refs.add(key)
        else:
            # Unqualified: match any view with same name
            for (s, v) in view_keys:
                if v == key[1]:
                    refs.add((s, v))
    return refs


def _body_from_create_view_ddl(ddl: str) -> str:
    """Return the SELECT body from CREATE VIEW ... AS <body>."""
    match = re.search(r"\bAS\s+(.*)\s*;?\s*$", ddl, re.IGNORECASE | re.DOTALL)
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


def _get_unqualified_table_refs(body: str) -> set[str]:
    """Return set of unqualified identifiers used as table/view in FROM and JOIN (no dot in name)."""
    refs: set[str] = set()
    pattern = re.compile(
        r"\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+"
        r"([a-zA-Z_][a-zA-Z0-9_]*)",
        re.IGNORECASE,
    )
    for m in pattern.finditer(body):
        name = m.group(1).strip()
        if "." not in name:
            refs.add(name)
    return refs


def _qualify_unqualified_refs_in_body(
    body: str,
    view_schema: str,
    connection,
    schema_objects_cache: Optional[dict[str, dict[str, str]]] = None,
) -> str:
    """
    Qualify unqualified table/view refs in body using Oracle object type and APPS/APPS2 rules.
    - If unqualified ref is a VIEW in Oracle (in view_schema): use view_schema.ref
    - If unqualified ref is a TABLE and view_schema is APPS: use prefix.ref where prefix = chars before first _
    - If unqualified ref is a TABLE and view_schema is APPS2: use prefix2.ref (prefix + '2')
    - Otherwise leave unqualified (or use view_schema if we can't determine).
    When schema_objects_cache is provided (schema_upper -> object_name_upper -> type), uses it instead of per-ref Oracle queries.
    """
    view_schema_upper = (view_schema or "").strip().upper()
    unqualified = _get_unqualified_table_refs(body)
    if not unqualified:
        return body

    schema_objs = (schema_objects_cache or {}).get(view_schema_upper)
    replacement_map: dict[str, str] = {}
    for ref in unqualified:
        ref_upper = (ref or "").upper()
        if schema_objs is not None:
            obj_type = schema_objs.get(ref_upper)
        else:
            obj_type = get_oracle_object_type(connection, view_schema_upper, ref_upper)
        if obj_type == "VIEW":
            qual = f"{view_schema_upper.lower()}.{ref}"
            replacement_map[ref] = qual
        elif obj_type == "TABLE":
            if view_schema_upper == "APPS":
                prefix = ref.split("_", 1)[0] if "_" in ref else ref
                qual = f"{prefix.lower()}.{ref}"
                replacement_map[ref] = qual
            elif view_schema_upper == "APPS2":
                prefix = ref.split("_", 1)[0] if "_" in ref else ref
                qual = f"{prefix.lower()}2.{ref}"
                replacement_map[ref] = qual
            # else: leave unqualified (no replacement)
        # else: not found or other type, leave unqualified

    if not replacement_map:
        return body

    # Replace only in FROM/JOIN position: (FROM|JOIN) + whitespace + identifier (case-insensitive match for ref)
    ref_lower_to_qual: dict[str, str] = {k.lower(): v for k, v in replacement_map.items()}

    def replacer(m: re.Match) -> str:
        ref = m.group(2)
        qual = ref_lower_to_qual.get(ref.lower()) or replacement_map.get(ref)
        if qual is not None:
            return m.group(1) + " " + qual
        return m.group(0)

    body = re.sub(
        r"(\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+)([a-zA-Z_][a-zA-Z0-9_]*)\b",
        replacer,
        body,
        flags=re.IGNORECASE,
    )
    return body


def apply_qualify_unqualified_refs(
    pg_sql: str,
    view_schema: str,
    connection,
    schema_objects_cache: Optional[dict[str, dict[str, str]]] = None,
) -> str:
    """
    Final step: qualify unqualified tables/views in the view body using Oracle object type and APPS/APPS2 rules.
    Returns updated pg_sql (DROP VIEW ...; CREATE OR REPLACE VIEW ... AS body).
    When schema_objects_cache is provided, qualification uses it (no per-ref Oracle queries).
    """
    view_name_match = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\S+)\s+AS", pg_sql, re.IGNORECASE)
    if not view_name_match:
        return pg_sql
    view_name_in_ddl = view_name_match.group(1)
    body = _body_from_create_view_ddl(pg_sql)
    body = _qualify_unqualified_refs_in_body(body, view_schema, connection, schema_objects_cache)
    return f"DROP VIEW IF EXISTS {view_name_in_ddl};\nCREATE OR REPLACE VIEW {view_name_in_ddl} AS\n{body}\n;"


def topological_sort(
    views: list[tuple[str, str]],
    deps: list[set[ViewKey]],
) -> list[int]:
    """
    Return indices so that dependencies come first.
    deps[i] = set of (schema, view_name) that view i depends on (from our view list).
    On cycle, append remaining in original order and warn.
    """
    n = len(views)
    dep_indices: list[set[int]] = [set() for _ in range(n)]
    for i in range(n):
        for (ds, dv) in deps[i]:
            for j in range(n):
                if i == j:
                    continue
                sj, vj = (views[j][0] or "").lower(), (views[j][1] or "").lower()
                if (sj, vj) == (ds, dv):
                    dep_indices[i].add(j)
                elif not ds and vj == dv:
                    dep_indices[i].add(j)
    in_degree = [len(dep_indices[i]) for i in range(n)]
    order: list[int] = []
    used = [False] * n
    while len(order) < n:
        candidates = [i for i in range(n) if not used[i] and in_degree[i] == 0]
        if not candidates:
            remaining = [i for i in range(n) if not used[i]]
            if remaining:
                order.extend(remaining)
                print("  Warning: circular dependency among views; order may be wrong.", file=sys.stderr, flush=True)
            break
        for i in candidates:
            used[i] = True
            order.append(i)
            for j in range(n):
                if i in dep_indices[j]:
                    in_degree[j] -= 1
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


def execute_view_on_postgres(connection, sql: str, view_schema: str, view_name: str) -> tuple[bool, str]:
    """
    Execute a single CREATE VIEW statement on PostgreSQL.
    Returns (success, message). message is empty on success or error text on failure.
    """
    try:
        import psycopg2
    except ImportError:
        return False, "psycopg2 not installed"

    sql = sql.strip()
    if not sql:
        return False, "Empty SQL"

    # Optionally qualify view name with schema if we want to create in a specific schema
    # Here we run the SQL as-is (view name is already in the CREATE VIEW from Oracle)
    try:
        with connection.cursor() as cur:
            cur.execute(sql)
        connection.commit()
        return True, ""
    except psycopg2.Error as e:
        connection.rollback()
        return False, str(e).strip()


def _first_error_line(full_sql: str, err_type: str) -> str:
    """Extract first comment line that looks like an error message from full_sql."""
    for line in full_sql.splitlines():
        s = line.strip()
        if s.startswith("-- ERROR") or s.startswith("-- Execution"):
            return s[2:].strip()[:500]  # drop "-- ", cap length
    return err_type


def _process_one_no_exec(
    no_exec_results: list,
    schema: str,
    view_name: str,
    display: str,
    safe_name: str,
    prefix: str,
    ddl: str,
    synonym_map: dict,
    view_keys_lower: set,
    block_lines: list,
    no_convert_oracle: bool,
    schema_objects_cache: dict,
    conn_o,
) -> None:
    """Rewrite, normalize, qualify one view DDL and append to no_exec_results."""
    try:
        pg_sql = rewrite_sql_with_synonyms(ddl, synonym_map, view_schema=schema)
    except Exception as e:
        block_lines.append(f"-- ERROR rewrite: {e}\n")
        block_lines.append(ddl)
        no_exec_results.append((schema, view_name, display, "\n".join(block_lines), safe_name, prefix, "rewrite", set()))
        return
    pg_sql = normalize_view_script(pg_sql, apply_oracle_conversions=not no_convert_oracle)
    try:
        pg_sql = apply_qualify_unqualified_refs(pg_sql, schema, conn_o, schema_objects_cache)
    except Exception as e:
        block_lines.append(f"-- WARNING: qualify unqualified refs skipped: {e}\n")
    body = _body_from_create_view_ddl(pg_sql)
    refs = _extract_view_refs_from_body(body, view_keys_lower)
    block_lines.append(pg_sql)
    if not pg_sql.endswith("\n"):
        block_lines.append("")
    full_sql = "\n".join(block_lines)
    no_exec_results.append((schema, view_name, display, full_sql, safe_name, prefix, None, refs))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Oracle views, resolve synonyms, transpile to PostgreSQL, execute and split success/failed.",
    )
    parser.add_argument(
        "input_files",
        nargs="+",
        help="Text file(s) with one view per line (schemaname.viewname or viewname)",
    )
    parser.add_argument(
        "--success-dir",
        default="postgres_views_success",
        help="Folder for SQL of views that executed successfully (default: postgres_views_success)",
    )
    parser.add_argument(
        "--failed-dir",
        default="postgres_views_failed",
        help="Folder for SQL of views that failed to execute (default: postgres_views_failed)",
    )
    parser.add_argument(
        "--converted-dir",
        default="postgres_views_converted",
        help="Folder to write converted (transpiled) PostgreSQL SQL before execution (default: postgres_views_converted)",
    )
    parser.add_argument("--oracle-user", default=None, help="Oracle user (or ORACLE_USER env)")
    parser.add_argument("--oracle-password", default=None, help="Oracle password (or ORACLE_PASSWORD env)")
    parser.add_argument("--oracle-dsn", default=None, help="Oracle DSN (or ORACLE_DSN env)")
    parser.add_argument("--pg-host", default=None, help="PostgreSQL host (or PG_HOST env)")
    parser.add_argument("--pg-port", type=int, default=None, help="PostgreSQL port (or PG_PORT env)")
    parser.add_argument("--pg-database", default=None, help="PostgreSQL database (or PG_DATABASE env)")
    parser.add_argument("--pg-user", default=None, help="PostgreSQL user (or PG_USER env)")
    parser.add_argument("--pg-password", default=None, help="PostgreSQL password (or PG_PASSWORD env)")
    parser.add_argument(
        "--no-execute",
        action="store_true",
        help="Only fetch, rewrite and write SQL to success dir (do not run on PostgreSQL)",
    )
    parser.add_argument(
        "--synonym-csv",
        default=None,
        help="Optional: use synonym CSV instead of Oracle (columns: owner,synonym_name,table_owner,table_name)",
    )
    parser.add_argument(
        "--no-dependency-order",
        action="store_true",
        help="Do not reorder views by dependency (default: when executing, create dependency views first)",
    )
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Create PostgreSQL schema (CREATE SCHEMA IF NOT EXISTS) before creating views in that schema",
    )
    parser.add_argument(
        "--no-convert-oracle",
        action="store_true",
        help="Skip Oracle→PG conversions (ROWID, NVL, TO_*, USERENV, SYSDATE, date/timestamp, etc.); use only sqlglot transpilation",
    )
    args = parser.parse_args()

    # Oracle credentials
    user = args.oracle_user or ORACLE_USER or os.environ.get("ORACLE_USER")
    password = args.oracle_password or ORACLE_PASSWORD or os.environ.get("ORACLE_PASSWORD")
    dsn = args.oracle_dsn or ORACLE_DSN or os.environ.get("ORACLE_DSN", "localhost:1521/ORCL")

    # PostgreSQL credentials
    pg_host = args.pg_host or os.environ.get("PG_HOST", PG_HOST)
    pg_port = args.pg_port if args.pg_port is not None else int(os.environ.get("PG_PORT", PG_PORT))
    pg_database = args.pg_database or os.environ.get("PG_DATABASE", PG_DATABASE)
    pg_user = args.pg_user or os.environ.get("PG_USER", PG_USER)
    pg_password = args.pg_password or os.environ.get("PG_PASSWORD", PG_PASSWORD)

    # Load view list
    try:
        views = load_view_list_from_files(args.input_files)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    if not views:
        print("No views found in input file(s).", file=sys.stderr, flush=True)
        sys.exit(1)
    print(f"Loaded {len(views)} view(s) from {args.input_files}", flush=True)

    # Dependencies
    try:
        import oracledb
    except ImportError:
        print("Error: oracledb required. pip install oracledb", file=sys.stderr, flush=True)
        sys.exit(1)
    try:
        import sqlglot
    except ImportError:
        print("Error: sqlglot required. pip install sqlglot", file=sys.stderr, flush=True)
        sys.exit(1)
    if not args.no_execute:
        try:
            import psycopg2
        except ImportError:
            print("Error: psycopg2 required. pip install psycopg2-binary", file=sys.stderr, flush=True)
            sys.exit(1)

    # Synonym map: from Oracle or CSV
    synonym_map: dict[str, str] = {}
    if args.synonym_csv:
        path = Path(args.synonym_csv)
        if path.exists():
            import csv
            with open(path, newline="", encoding="utf-8-sig") as f:
                r = csv.DictReader(f)
                for row in r:
                    owner = (row.get("owner") or "").strip().upper()
                    syn = (row.get("synonym_name") or "").strip().upper()
                    t_owner = (row.get("table_owner") or "").strip().upper()
                    t_name = (row.get("table_name") or "").strip().upper()
                    if syn and t_owner and t_name:
                        target = f"{t_owner}.{t_name}"
                        synonym_map[syn] = target
                        if owner:
                            synonym_map[f"{owner}.{syn}"] = target
            print(f"Loaded {len(synonym_map)} synonym mappings from {args.synonym_csv}", flush=True)
        else:
            print(f"Warning: synonym CSV not found: {args.synonym_csv}", file=sys.stderr, flush=True)
    else:
        try:
            conn_o = _oracle_connect_with_retry(user, password, dsn, max_attempts=3, delay_sec=2)
            try:
                synonym_map = get_synonym_map(conn_o)
                print(f"Fetched {len(synonym_map)} synonym mappings from Oracle", flush=True)
            finally:
                conn_o.close()
        except Exception as e:
            print(f"Warning: could not fetch synonyms from Oracle: {e}. Proceeding without synonym resolution.", file=sys.stderr, flush=True)

    if not user or not password:
        print("Error: Oracle credentials required to fetch view DDL. Set ORACLE_USER, ORACLE_PASSWORD (and optionally ORACLE_DSN).", file=sys.stderr, flush=True)
        sys.exit(1)

    # Output dirs
    success_dir = Path(args.success_dir)
    failed_dir = Path(args.failed_dir)
    converted_dir = Path(args.converted_dir)
    success_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)
    if not args.no_execute:
        converted_dir.mkdir(parents=True, exist_ok=True)

    # PG connection for execution
    pg_conn = None
    if not args.no_execute:
        try:
            pg_conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                dbname=pg_database,
                user=pg_user,
                password=pg_password,
            )
        except Exception as e:
            print(f"Error: cannot connect to PostgreSQL: {e}", file=sys.stderr, flush=True)
            sys.exit(1)

    n_ok = 0
    n_fail = 0
    pad = max(3, len(str(len(views))))
    view_keys_lower: set[ViewKey] = {((s or "").lower(), (v or "").lower()) for s, v in views}

    # Error log file (one line per failure) in failed_dir
    error_log_path = failed_dir / "error_log.txt"
    error_log_lines: list[str] = []

    # Build schema object cache on a temporary connection (then close). So the main DDL connection
    # is never used for schema queries — first use is get_oracle_ddl only, matching e2e3 and avoiding
    # session state that can cause GET_DDL to hang on some views.
    unique_schemas_upper: set[str] = set()
    for s, _ in views:
        su = (s or "").strip().upper()
        if su:
            unique_schemas_upper.add(su)
    schema_objects_cache: dict[str, dict[str, str]] = {}
    if unique_schemas_upper:
        print(f"Building schema object cache for {len(unique_schemas_upper)} schema(s) (temp connection)...", flush=True)
        try:
            conn_temp = _oracle_connect_with_retry(user, password, dsn, max_attempts=3, delay_sec=2)
            try:
                for su in unique_schemas_upper:
                    try:
                        schema_objects_cache[su] = get_oracle_schema_objects(conn_temp, su)
                        print(f"  Schema {su}: {len(schema_objects_cache[su])} objects (VIEW/TABLE)", flush=True)
                    except Exception as ex:
                        print(f"  Schema {su}: skip ({ex})", flush=True)
            finally:
                conn_temp.close()
            if schema_objects_cache:
                print(f"Cached object types for {len(schema_objects_cache)} schema(s).", flush=True)
        except Exception as e:
            print(f"Warning: could not build schema cache: {e}. Qualification may be slower.", file=sys.stderr, flush=True)

    # Single Oracle connection for DDL fetches only (one query per view, like e2e3)
    print("Connecting to Oracle for DDL fetches (up to 3 attempts, 2s between)...", flush=True)
    try:
        conn_o = _oracle_connect_with_retry(user, password, dsn, max_attempts=3, delay_sec=2)
    except Exception as e:
        print(f"Oracle connection failed: {e}", file=sys.stderr, flush=True)
        raise
    print("Connected to Oracle.", flush=True)
    try:
        if args.no_execute:
            print(f"Mode: export only (no PostgreSQL execution). Processing {len(views)} view(s).", flush=True)
        else:
            print(f"Mode: fetch + convert + execute on PostgreSQL. Processing {len(views)} view(s).", flush=True)
        print(f"Fetching DDL (one query per view, single connection)...", flush=True)

        if args.no_execute:
            # Two-pass when exporting: fetch+rewrite all, then write in dependency order (or input order if --no-dependency-order)
            no_exec_results: list[tuple[str, str, str, str, str, str, Optional[str], set[ViewKey]]] = []
            # Each: (schema, view_name, display, full_sql, safe_name, prefix, error_type, refs)

            for i, (schema, view_name) in enumerate(views):
                display = f"{schema}.{view_name}" if schema else view_name
                safe_name = f"{schema}_{view_name}".replace(".", "_") if schema else view_name.replace(".", "_")
                safe_name = re.sub(r'[<>:"/\\|?*]', "_", safe_name)
                prefix = f"{(i + 1):0{pad}d}_"
                block_lines = [f"-- Source: {display}\n"]
                err_type = None
                pg_sql = ""
                refs: set[ViewKey] = set()
                try:
                    ddl = get_oracle_ddl(conn_o, schema, view_name)
                except Exception as e:
                    err_type = "fetch"
                    block_lines.append(f"-- ERROR fetching DDL: {e}\n")
                    no_exec_results.append((schema, view_name, display, "\n".join(block_lines), safe_name, prefix, err_type, refs))
                    print(f"  [{i+1:>{pad}}/{len(views)}] FAIL (fetch): {display}", flush=True)
                    continue
                _process_one_no_exec(
                    no_exec_results, schema, view_name, display, safe_name, prefix,
                    ddl, synonym_map, view_keys_lower, block_lines, args.no_convert_oracle,
                    schema_objects_cache, conn_o,
                )
                print(f"  [{i+1:>{pad}}/{len(views)}] Fetched: {display}", flush=True)

            no_exec_deps_list = [r[7] for r in no_exec_results]
            no_exec_order = list(range(len(views))) if args.no_dependency_order else topological_sort(views, no_exec_deps_list)
            if no_exec_order != list(range(len(views))):
                print("  Exporting in dependency order.", flush=True)
            print(f"  Writing output files to {success_dir}/, {failed_dir}/...", flush=True)
            dep_pad_noexec = max(3, len(str(len(no_exec_order))))
            for rank, idx in enumerate(no_exec_order):
                schema, view_name, display, full_sql, safe_name, _prefix, err_type, _ = no_exec_results[idx]
                prefix = f"{(rank + 1):0{dep_pad_noexec}d}_" if no_exec_order != list(range(len(views))) else _prefix
                if err_type:
                    out_path = failed_dir / f"{prefix}{safe_name}.sql"
                    out_path.write_text(full_sql, encoding="utf-8")
                    error_log_lines.append(f"{display}\t{err_type}\t{_first_error_line(full_sql, err_type)}")
                    n_fail += 1
                    print(f"  [{rank+1:>{pad}}/{len(no_exec_order)}] (error) {display}", flush=True)
                else:
                    out_path = success_dir / f"{prefix}{safe_name}.sql"
                    out_path.write_text(full_sql, encoding="utf-8")
                    n_ok += 1
                    print(f"  [{rank+1:>{pad}}/{len(no_exec_order)}] OK (no-exec): {display}", flush=True)
        else:
            # Two-pass when executing: fetch+rewrite all, then execute in dependency order
            results: list[tuple[str, str, str, str, str, str, Optional[str], set[ViewKey]]] = []
            # Each: (schema, view_name, display, full_sql, pg_sql, safe_name, prefix, error_type, refs)

            for i, (schema, view_name) in enumerate(views):
                display = f"{schema}.{view_name}" if schema else view_name
                safe_name = f"{schema}_{view_name}".replace(".", "_") if schema else view_name.replace(".", "_")
                safe_name = re.sub(r'[<>:"/\\|?*]', "_", safe_name)
                prefix = f"{(i + 1):0{pad}d}_"
                block_lines = [f"-- Source: {display}\n"]
                err_type = None
                pg_sql = ""
                refs = set()
                try:
                    ddl = get_oracle_ddl(conn_o, schema, view_name)
                except Exception as e:
                    err_type = "fetch"
                    block_lines.append(f"-- ERROR fetching DDL: {e}\n")
                    results.append((schema, view_name, display, "\n".join(block_lines), pg_sql, safe_name, prefix, err_type, refs))
                    print(f"  [{i+1:>{pad}}/{len(views)}] FAIL (fetch): {display}", flush=True)
                    continue
                try:
                    pg_sql = rewrite_sql_with_synonyms(ddl, synonym_map, view_schema=schema)
                except Exception as e:
                    err_type = "rewrite"
                    block_lines.append(f"-- ERROR rewrite: {e}\n")
                    block_lines.append(ddl)
                    results.append((schema, view_name, display, "\n".join(block_lines), pg_sql, safe_name, prefix, err_type, refs))
                    print(f"  [{i+1:>{pad}}/{len(views)}] FAIL (rewrite): {display}", flush=True)
                    continue
                pg_sql = normalize_view_script(pg_sql, apply_oracle_conversions=not args.no_convert_oracle)
                try:
                    pg_sql = apply_qualify_unqualified_refs(pg_sql, schema, conn_o, schema_objects_cache)
                except Exception as e:
                    block_lines.append(f"-- WARNING: qualify unqualified refs skipped: {e}\n")
                body = _body_from_create_view_ddl(pg_sql)
                refs = _extract_view_refs_from_body(body, view_keys_lower)
                block_lines.append(pg_sql)
                if not pg_sql.endswith("\n"):
                    block_lines.append("")
                results.append((schema, view_name, display, "\n".join(block_lines), pg_sql, safe_name, prefix, None, refs))
                print(f"  [{i+1:>{pad}}/{len(views)}] Fetched: {display}", flush=True)

            deps_list = [r[8] for r in results]
            order = list(range(len(views))) if args.no_dependency_order else topological_sort(views, deps_list)
            if order != list(range(len(views))):
                print("  Executing in dependency order.", flush=True)
            # Write converted (transpiled) SQL to converted_dir before execution
            dep_pad = max(3, len(str(len(order))))
            n_converted = 0
            for rank, idx in enumerate(order):
                _s, _v, display, _full, pg_sql, safe_name, _pf, err_type, _ = results[idx]
                if not err_type and pg_sql:
                    p = f"{(rank + 1):0{dep_pad}d}_" if order != list(range(len(views))) else _pf
                    conv_path = converted_dir / f"{p}{safe_name}.sql"
                    body = pg_sql if pg_sql.strip().startswith("--") else f"-- Source: {display}\n\n{pg_sql}"
                    if not body.endswith("\n"):
                        body += "\n"
                    conv_path.write_text(body, encoding="utf-8")
                    n_converted += 1
            if n_converted:
                print(f"  Wrote {n_converted} converted view(s) to {converted_dir}/", flush=True)
            print(f"  Executing views on PostgreSQL ({len(order)} view(s))...", flush=True)

            schemas_ensured: set[str] = set()
            for rank, idx in enumerate(order):
                schema, view_name, display, full_sql, pg_sql, safe_name, _prefix, err_type, _ = results[idx]
                prefix = f"{(rank + 1):0{dep_pad}d}_" if order != list(range(len(views))) else _prefix
                if err_type:
                    out_path = failed_dir / f"{prefix}{safe_name}.sql"
                    out_path.write_text(full_sql, encoding="utf-8")
                    error_log_lines.append(f"{display}\t{err_type}\t{_first_error_line(full_sql, err_type)}")
                    n_fail += 1
                    print(f"  [{rank+1:>{pad}}/{len(order)}] (error) {display}", flush=True)
                    continue
                if args.ensure_schema and schema and schema.strip() and schema not in schemas_ensured:
                    ensure_pg_schema(pg_conn, schema)
                    schemas_ensured.add(schema)
                ok, err = execute_view_on_postgres(pg_conn, pg_sql, schema, view_name)
                if ok:
                    out_path = success_dir / f"{prefix}{safe_name}.sql"
                    out_path.write_text(full_sql, encoding="utf-8")
                    n_ok += 1
                    print(f"  [{rank+1:>{pad}}/{len(order)}] OK: {display}", flush=True)
                else:
                    block_lines = [f"-- Source: {display}\n", f"-- Execution error: {err}\n", pg_sql]
                    if not pg_sql.endswith("\n"):
                        block_lines.append("")
                    full_sql_fail = "\n".join(block_lines)
                    out_path = failed_dir / f"{prefix}{safe_name}.sql"
                    out_path.write_text(full_sql_fail, encoding="utf-8")
                    error_log_lines.append(f"{display}\texecute\t{err[:500]}")
                    n_fail += 1
                    print(f"  [{rank+1:>{pad}}/{len(order)}] FAIL (execute): {display} - {err[:80]}", flush=True)
    finally:
        conn_o.close()

    if pg_conn:
        pg_conn.close()

    # Write consolidated error log if any failures
    if error_log_lines:
        header = "view\terror_type\terror_message\n"
        error_log_path.write_text(header + "\n".join(error_log_lines), encoding="utf-8")
        print(f"Error log: {error_log_path}", flush=True)

    print(f"\nDone. Success: {success_dir}/ ({n_ok} files)", flush=True)
    print(f"Failed:  {failed_dir}/ ({n_fail} files)", flush=True)
    if not args.no_execute:
        print(f"Converted (pre-exec): {converted_dir}/", flush=True)
    if n_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()


# Multiple view list files, custom dirs, no execution (only fetch + rewrite + write SQL)
#python oracle_views_to_postgres_synonyms.py views_list.txt
#python oracle_views_to_postgres_synonyms.py v1.txt v2.txt --success-dir pg_ok --failed-dir pg_fail --no-execute

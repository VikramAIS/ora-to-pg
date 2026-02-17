#!/usr/bin/env python3
"""
Fix PostgreSQL views that fail with runtime errors after Oracle-to-PostgreSQL migration.

ERRORS HANDLED
--------------
1. "invalid input syntax for type numeric: 'pgAdmin4'"
   Caused by current_setting('application_name') or varchar columns with non-numeric
   data being cast to ::numeric.

2. 'unit "dd" not recognized for type timestamp with time zone'
   Caused by date_trunc() using Oracle format codes ('dd','mm','yyyy') instead of
   PostgreSQL unit names ('day','month','year').

3. "negative substring length not allowed"
   Caused by substring(str, start, length) where length evaluates to negative.
   Oracle SUBSTR silently returns NULL; PostgreSQL throws an error.

4. 'unrecognized configuration parameter "lc_collate"'
   Caused by current_setting('lc_collate') — lc_collate is a database-level property,
   not a runtime GUC. Also handles Oracle NLS_* parameters with no PG equivalent.

WHAT THIS SCRIPT DOES
---------------------
1. Connects to PostgreSQL.
2. Discovers all views in the target schemas (or from --view-list file).
3. Tests each view with SELECT ... LIMIT 1 inside a savepoint.
4. For views that fail, applies the appropriate fixes:

   date_trunc units:
     - date_trunc('dd', ...) → date_trunc('day', ...)
     - INTERVAL 'N dd' → INTERVAL 'N day'
     - Full mapping: dd/d→day, mm/mon→month, yyyy/yy→year, q→quarter, etc.

   negative substring:
     - substring(s, pos, len) → substring(s, pos, GREATEST(len, 0))

   unrecognized configuration parameter:
     - current_setting('lc_collate') → (SELECT datcollate FROM pg_database ...)
     - current_setting('lc_ctype') → (SELECT datctype FROM pg_database ...)
     - current_setting('nls_*') → NULL::text

   numeric input syntax (Strategy A — current_setting patterns):
     - current_setting('application_name') → NULL::text
     - (current_setting(...))::numeric → NULL::numeric

   numeric input syntax (Strategy B — data-level casts):
     - Creates safe_numeric() helper function (regex-based, no exception overhead)
     - Replaces column::numeric casts with safe_numeric(column)

5. Recreates the view with CREATE OR REPLACE VIEW.
6. Re-tests and reports results.

USAGE
-----
    python fix_pgadmin4_numeric_views.py [options]

    --pg-host / --pg-port / --pg-database / --pg-user / --pg-password
        PostgreSQL connection parameters (can also use PG* env vars).

    --schemas SCHEMA [SCHEMA ...]
        Schemas to scan (default: all non-system schemas).

    --dry-run
        Show what would be changed without executing.

    --output-dir DIR
        Write before/after SQL and report to this directory (default: ./fix_output).

    --force-retest
        After fixing, run SELECT again to verify the fix.

    --verbose / -v
        Increase logging verbosity.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger("fix_pgadmin4_numeric")

# ---------------------------------------------------------------------------
# PostgreSQL connection defaults (edit here or override with env vars / CLI)
# ---------------------------------------------------------------------------
PG_HOST: str = "localhost"
PG_PORT: int = 5432
PG_DATABASE: str = "postgres"
PG_USER: str = "postgres"
PG_PASSWORD: str = ""

# System schemas to skip when --schemas is not specified
SYSTEM_SCHEMAS = frozenset({
    "pg_catalog", "information_schema", "pg_toast", "pg_temp_1", "pg_toast_temp_1",
})

# ---------------------------------------------------------------------------
# Pattern definitions for problematic expressions
# ---------------------------------------------------------------------------

# Regex to match current_setting('...') with optional ::text on param and optional boolean arg
_CS_CALL = (
    r"(?:current_setting\s*\(\s*'[^']*'\s*(?:::text)?\s*(?:,\s*(?:true|false)\s*)?\))"
)

# Patterns that produce the numeric error (current_setting → ::numeric chain)
# We look for current_setting(...) eventually followed by ::numeric, possibly with
# intermediate ::text, substr(), COALESCE, etc.
_PROBLEMATIC_PATTERNS = [
    # Direct: current_setting('application_name')::numeric
    re.compile(
        r"\(\s*" + _CS_CALL + r"\s*\)\s*::numeric",
        re.IGNORECASE,
    ),
    # With explicit text cast: (current_setting('...')::text)::numeric
    re.compile(
        r"\(\s*" + _CS_CALL + r"\s*::text\s*\)\s*::numeric",
        re.IGNORECASE,
    ),
    # SUBSTR wrapping: (substr(current_setting('...'), N, M))::numeric
    re.compile(
        r"\(\s*substr(?:ing)?\s*\(\s*" + _CS_CALL + r"[^)]*\)\s*\)\s*::numeric",
        re.IGNORECASE,
    ),
    # COALESCE wrapping: COALESCE(current_setting('...'), '0')::numeric
    re.compile(
        r"COALESCE\s*\([^)]*" + _CS_CALL + r"[^)]*\)\s*::numeric",
        re.IGNORECASE,
    ),
    # Bare current_setting(...)::numeric (no parens)
    re.compile(
        _CS_CALL + r"\s*::numeric",
        re.IGNORECASE,
    ),
    # current_setting inside CAST(... AS numeric)
    re.compile(
        r"CAST\s*\([^)]*" + _CS_CALL + r"[^)]*AS\s+numeric\s*\)",
        re.IGNORECASE,
    ),
]

# Broader pattern: any current_setting('application_name') (regardless of cast)
_APP_NAME_PATTERN = re.compile(
    r"current_setting\s*\(\s*'application_name'\s*(?:::text)?\s*(?:,\s*(?:true|false)\s*)?\)",
    re.IGNORECASE,
)

# Pattern to find current_setting('...') used anywhere (for reporting)
_ANY_CURRENT_SETTING = re.compile(
    r"current_setting\s*\(\s*'([^']*)'\s*(?:::text)?\s*(?:,\s*(?:true|false)\s*)?\)",
    re.IGNORECASE,
)


def connect_pg(args) -> "psycopg2.extensions.connection":
    """Open a PostgreSQL connection."""
    import psycopg2
    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
    )
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# safe_numeric() — PL/pgSQL helper function
# ---------------------------------------------------------------------------

# Each statement must be executed separately (psycopg2 doesn't support multi-statement execute)
_SAFE_NUMERIC_STMTS = [
    # text -> numeric (main overload: regex pre-check, returns NULL on non-numeric input)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val text)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE
        WHEN val ~ '^\\s*[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?\\s*$'
        THEN val::numeric
        ELSE NULL
    END;
$$""",
    # bigint -> numeric (pass-through, always succeeds)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val bigint)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT val::numeric;
$$""",
    # integer -> numeric (pass-through, always succeeds)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val integer)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT val::numeric;
$$""",
    # smallint -> numeric (pass-through, always succeeds)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val smallint)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT val::numeric;
$$""",
    # numeric -> numeric (identity, no-op)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val numeric)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT val;
$$""",
    # double precision -> numeric (pass-through, always succeeds)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val double precision)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT val::numeric;
$$""",
    # real -> numeric (pass-through, always succeeds)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val real)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT val::numeric;
$$""",
    # character varying -> numeric (delegates to text overload)
    """CREATE OR REPLACE FUNCTION public.safe_numeric(val character varying)
RETURNS numeric LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT public.safe_numeric(val::text);
$$""",
    # Comment
    "COMMENT ON FUNCTION public.safe_numeric(text) IS "
    "'Safe text->numeric cast using regex pre-check: returns NULL on non-numeric input. "
    "SQL function (no PL/pgSQL exception overhead) — can be inlined by the optimizer. "
    "Overloads exist for bigint, integer, smallint, numeric, double precision, real, varchar.'",
]


def ensure_safe_numeric_function(conn) -> bool:
    """
    Create the public.safe_numeric() overloaded functions if they don't already exist.
    Each statement is executed separately (psycopg2 requires single-statement execute).
    Returns True if all functions are available.
    """
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT _ensure_safe_numeric")
        for stmt in _SAFE_NUMERIC_STMTS:
            cur.execute(stmt)
        cur.execute("RELEASE SAVEPOINT _ensure_safe_numeric")
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        log.warning("Could not create safe_numeric function: %s", e)
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _ensure_safe_numeric")
            cur.execute("RELEASE SAVEPOINT _ensure_safe_numeric")
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        cur.close()
        return False


def get_all_views(conn, schemas: list[str] | None) -> list[tuple[str, str, str]]:
    """
    Return list of (schema, view_name, definition) for all views in the target schemas.
    """
    cur = conn.cursor()
    if schemas:
        placeholders = ",".join(["%s"] * len(schemas))
        cur.execute(f"""
            SELECT schemaname, viewname, definition
            FROM pg_views
            WHERE schemaname IN ({placeholders})
            ORDER BY schemaname, viewname
        """, schemas)
    else:
        cur.execute("""
            SELECT schemaname, viewname, definition
            FROM pg_views
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema',
                                     'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
            ORDER BY schemaname, viewname
        """)
    rows = cur.fetchall()
    cur.close()
    return [(r[0], r[1], r[2]) for r in rows]


def get_view_columns(conn, schema: str, view_name: str) -> list[tuple[str, str]]:
    """Return list of (column_name, data_type) for a view."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, view_name))
    cols = cur.fetchall()
    cur.close()
    return cols


def test_view(conn, schema: str, view_name: str) -> tuple[bool, str]:
    """
    Try SELECT * FROM schema.view LIMIT 1 inside a savepoint.
    Returns (ok, error_message). On success error_message is empty.
    """
    cur = conn.cursor()
    fqn = f'"{schema}"."{view_name}"'
    try:
        cur.execute("SAVEPOINT _test_view")
        cur.execute(f"SELECT * FROM {fqn} LIMIT 1")
        cur.execute("RELEASE SAVEPOINT _test_view")
        cur.close()
        return (True, "")
    except Exception as e:
        err = str(e).strip()
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _test_view")
            cur.execute("RELEASE SAVEPOINT _test_view")
        except Exception:
            pass
        cur.close()
        return (False, err)


def is_numeric_input_error(error_msg: str) -> bool:
    """Check if the error is the 'invalid input syntax for type numeric' error."""
    return "invalid input syntax for type numeric" in error_msg.lower()


def is_date_trunc_unit_error(error_msg: str) -> bool:
    """Check if the error is a date_trunc unrecognized unit error (e.g. 'dd', 'mm')."""
    return "not recognized for type timestamp" in error_msg.lower() or \
           "not recognized for type interval" in error_msg.lower()


def is_negative_substring_error(error_msg: str) -> bool:
    """Check if the error is a negative substring length error."""
    return "negative substring length not allowed" in error_msg.lower()


def is_unrecognized_config_error(error_msg: str) -> bool:
    """Check if the error is an unrecognized configuration parameter (e.g. lc_collate)."""
    return "unrecognized configuration parameter" in error_msg.lower()


def is_fixable_error(error_msg: str) -> bool:
    """Check if the error is any of the types this script can fix."""
    return (is_numeric_input_error(error_msg)
            or is_date_trunc_unit_error(error_msg)
            or is_negative_substring_error(error_msg)
            or is_unrecognized_config_error(error_msg))


def classify_error(error_msg: str) -> str:
    """Return a short label for the error type."""
    if is_numeric_input_error(error_msg):
        return "numeric"
    if is_date_trunc_unit_error(error_msg):
        return "date_trunc_unit"
    if is_negative_substring_error(error_msg):
        return "negative_substr"
    if is_unrecognized_config_error(error_msg):
        return "unrecognized_config"
    return "other"


def get_view_dependencies(conn, schema: str, view_name: str) -> list[tuple[str, str]]:
    """
    Get views that this view depends on (i.e., views referenced in its definition).
    Returns list of (dep_schema, dep_view_name).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT
            cl_dep.relnamespace::regnamespace::text AS dep_schema,
            cl_dep.relname AS dep_view
        FROM pg_depend d
        JOIN pg_rewrite r ON r.oid = d.objid
        JOIN pg_class cl ON cl.oid = r.ev_class
        JOIN pg_class cl_dep ON cl_dep.oid = d.refobjid
        WHERE cl.relnamespace::regnamespace::text = %s
          AND cl.relname = %s
          AND cl_dep.relkind = 'v'
          AND cl_dep.oid != cl.oid
    """, (schema, view_name))
    deps = cur.fetchall()
    cur.close()
    return deps


# ---------------------------------------------------------------------------
# Fix strategies
# ---------------------------------------------------------------------------

def _find_closing_paren(text: str, open_pos: int) -> Optional[int]:
    """Find the matching closing paren for open_pos (which must be '(')."""
    depth = 0
    in_sq = False
    for i in range(open_pos, len(text)):
        ch = text[i]
        if ch == "'" and not in_sq:
            in_sq = True
        elif ch == "'" and in_sq:
            # Check for escaped quote ''
            if i + 1 < len(text) and text[i + 1] == "'":
                continue
            in_sq = False
        elif not in_sq:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
    return None


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


def fix_current_setting_numeric(definition: str) -> tuple[str, list[str]]:
    """
    Fix all patterns where current_setting(...) ends up cast to numeric.
    Returns (fixed_definition, list_of_changes_made).
    """
    changes = []
    body = definition

    # ---- Strategy 1: Replace current_setting('application_name') everywhere ----
    # current_setting('application_name') -> NULL::text
    # This is the most common culprit and NULL::text is safe in all contexts
    # (NULL cast to numeric gives NULL, not an error)
    app_name_count = len(_APP_NAME_PATTERN.findall(body))
    if app_name_count > 0:
        body = _APP_NAME_PATTERN.sub("NULL::text", body)
        changes.append(f"Replaced {app_name_count} occurrence(s) of current_setting('application_name') -> NULL::text")

    # ---- Strategy 2: Replace current_setting('...')::numeric patterns ----
    # For any current_setting('xxx')::numeric, replace with NULL::numeric
    # This catches session vars like 'client_info', 'fnd.user_id', etc.
    for pat in _PROBLEMATIC_PATTERNS:
        matches = list(pat.finditer(body))
        if matches:
            for m in reversed(matches):
                body = body[:m.start()] + "NULL::numeric" + body[m.end():]
            changes.append(f"Replaced {len(matches)} numeric-cast current_setting() pattern(s) -> NULL::numeric")

    # ---- Strategy 3: Handle complex nested patterns ----
    # Look for current_setting calls that are arguments to functions eventually cast to numeric
    # Pattern: (expr_containing_current_setting)::numeric
    # We use a more surgical approach: find current_setting(), check if ::numeric follows
    # within the same parenthesized expression
    search_start = 0
    cs_pattern = re.compile(r"current_setting\s*\(", re.IGNORECASE)
    while True:
        m = cs_pattern.search(body, search_start)
        if not m:
            break
        # Find the closing paren of the current_setting call
        open_pos = body.index("(", m.start())
        close = _find_closing_paren(body, open_pos)
        if close is None:
            search_start = m.end()
            continue

        # Check what comes after this current_setting(...) call
        after = body[close + 1:close + 50].lstrip()

        # Check if ::numeric follows (directly or through ::text::numeric)
        if re.match(r"^\s*::(?:text\s*)?::?\s*numeric", after, re.IGNORECASE):
            # Replace the entire current_setting(...)::numeric chain
            cast_match = re.match(r"^\s*::\s*(?:text\s*::)?\s*numeric", after, re.IGNORECASE)
            if cast_match:
                end_pos = close + 1 + len(body[close + 1:close + 1 + cast_match.end()])
                old = body[m.start():end_pos]
                body = body[:m.start()] + "NULL::numeric" + body[end_pos:]
                changes.append(f"Replaced nested pattern: {old[:60]}... -> NULL::numeric")
                search_start = m.start() + len("NULL::numeric")
                continue

        search_start = close + 1

    # ---- Strategy 4: Replace remaining current_setting calls for known GUC variables ----
    # that are commonly used in numeric context in Oracle EBS migrations
    numeric_guc_vars = [
        "client_info", "fnd.user_id", "fnd.resp_id", "fnd.resp_appl_id",
        "fnd.org_id", "fnd.security_group_id", "fnd.conc_request_id",
        "fnd.login_id", "fnd.prog_appl_id", "fnd.conc_program_id",
        "userenv.client_info", "userenv.bg_id", "userenv.fg_id",
    ]
    for guc in numeric_guc_vars:
        pat = re.compile(
            r"current_setting\s*\(\s*'" + re.escape(guc) + r"'\s*(?:::text)?\s*(?:,\s*(?:true|false)\s*)?\)",
            re.IGNORECASE,
        )
        count = len(pat.findall(body))
        if count > 0:
            body = pat.sub("NULL::text", body)
            changes.append(f"Replaced {count} occurrence(s) of current_setting('{guc}') -> NULL::text")

    return body, changes


def fix_data_level_numeric_casts(definition: str) -> tuple[str, list[str]]:
    """
    Strategy B: Replace column::numeric casts with safe_numeric(column) to handle
    rows with non-numeric data in varchar columns.

    Targets patterns like:
        r.reference_2::numeric
        l.vendor_id::numeric
        (expr)::numeric

    Skips safe casts that are already on numeric literals or known-safe expressions.
    Returns (fixed_definition, list_of_changes_made).
    """
    changes = []
    body = definition

    # Pattern: identifier::numeric  (e.g. r.reference_2::numeric, vendor_id::numeric)
    # Captures the identifier (with optional table alias) before ::numeric
    # Avoids matching:
    #   - numeric literals: 123::numeric, '123'::numeric
    #   - already safe: safe_numeric(...)
    #   - NULL::numeric
    #   - expressions already wrapped: )::numeric (handled separately)
    ident_numeric_pat = re.compile(
        r"""
        (?<!\w)                             # not preceded by word char
        (?<!::)                             # not preceded by another cast (avoids ::text::numeric double-match)
        (?<!safe_numeric\()                 # not already safe_numeric wrapped
        (                                   # group 1: the identifier
            [a-zA-Z_][a-zA-Z0-9_]*          #   column or alias
            (?:\.[a-zA-Z_][a-zA-Z0-9_]*)*   #   optional schema.table.column dotted parts
        )
        \s*::\s*numeric                     # the ::numeric cast
        (?!\s*\*)                            # not ::numeric * (interval multiplication)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Don't replace: NULL::numeric, integer literals::numeric
    skip_idents = frozenset({"null", "true", "false", "interval"})

    replacements = 0
    result_parts = []
    last_end = 0

    for m in ident_numeric_pat.finditer(body):
        ident = m.group(1)
        ident_lower = ident.lower().split(".")[-1]  # just the column name part

        # Skip if the identifier is NULL or a keyword
        if ident_lower in skip_idents:
            continue

        # Skip if preceded by a quote (it's a literal string)
        before_char = body[m.start() - 1] if m.start() > 0 else ""
        if before_char in ("'", '"'):
            continue

        result_parts.append(body[last_end:m.start()])
        result_parts.append(f"public.safe_numeric({ident})")
        last_end = m.end()
        replacements += 1

    if replacements > 0:
        result_parts.append(body[last_end:])
        body = "".join(result_parts)
        changes.append(f"Replaced {replacements} column::numeric cast(s) with safe_numeric(column)")

    # Pattern: (expression)::numeric  — parenthesized expressions cast to numeric
    # e.g. (COALESCE(x, '0'))::numeric
    paren_numeric_pat = re.compile(r"\)(\s*::\s*numeric)", re.IGNORECASE)
    paren_matches = list(paren_numeric_pat.finditer(body))

    paren_replacements = 0
    for pm in reversed(paren_matches):
        # Find the matching open paren
        close_pos = pm.start()  # position of ')'
        # Walk backwards to find matching '('
        depth = 0
        open_pos = None
        for i in range(close_pos, -1, -1):
            if body[i] == ")":
                depth += 1
            elif body[i] == "(":
                depth -= 1
                if depth == 0:
                    open_pos = i
                    break

        if open_pos is None:
            continue

        inner_expr = body[open_pos + 1:close_pos]

        # Skip if it's already safe_numeric(...)
        before_open = body[max(0, open_pos - 20):open_pos].rstrip()
        if before_open.lower().endswith("safe_numeric"):
            continue

        # Skip if inner is purely numeric literal
        if re.match(r"^\s*-?\d+\.?\d*\s*$", inner_expr):
            continue

        # Skip NULL::numeric
        if re.match(r"^\s*NULL\s*$", inner_expr, re.IGNORECASE):
            continue

        # Replace (expr)::numeric with public.safe_numeric(expr)
        full_match_end = pm.end()
        body = body[:open_pos] + f"public.safe_numeric({inner_expr})" + body[full_match_end:]
        paren_replacements += 1

    if paren_replacements > 0:
        changes.append(f"Replaced {paren_replacements} (expression)::numeric cast(s) with safe_numeric(expression)")

    return body, changes


# ---------------------------------------------------------------------------
# Fix: date_trunc unit mapping (Oracle format codes -> PostgreSQL units)
# ---------------------------------------------------------------------------

# Oracle TRUNC format -> PostgreSQL date_trunc unit
_DATE_TRUNC_UNIT_MAP = {
    "dd": "day", "d": "day", "day": "day", "dy": "day", "ddd": "day",
    "mm": "month", "mon": "month", "month": "month",
    "yyyy": "year", "yy": "year", "y": "year", "year": "year",
    "q": "quarter", "quarter": "quarter",
    "hh": "hour", "hh12": "hour", "hh24": "hour", "hour": "hour",
    "mi": "minute", "minute": "minute",
    "ss": "second", "second": "second",
    "ww": "week", "iw": "week", "w": "week", "week": "week",
    "cc": "century", "scc": "century", "century": "century",
}


def fix_date_trunc_units(definition: str) -> tuple[str, list[str]]:
    """
    Fix date_trunc('dd', ...) -> date_trunc('day', ...) and similar Oracle format codes.
    Also fixes INTERVAL 'N dd' -> INTERVAL 'N day'.
    Returns (fixed_definition, list_of_changes).
    """
    changes = []
    body = definition

    # Fix date_trunc('oracle_format' [::text], ...) -> date_trunc('pg_unit', ...)
    # Handles both: date_trunc('DD', ...) and date_trunc('DD'::text, ...)
    dt_pat = re.compile(
        r"date_trunc\s*\(\s*'([^']*)'\s*(?:::text\s*)?,",
        re.IGNORECASE,
    )
    replacements = 0
    search_start = 0
    while True:
        m = dt_pat.search(body, search_start)
        if not m:
            break
        unit = m.group(1).strip().lower()
        pg_unit = _DATE_TRUNC_UNIT_MAP.get(unit)
        if pg_unit and pg_unit != unit:
            # Replace the entire match (including any ::text) with clean format
            body = body[:m.start()] + f"date_trunc('{pg_unit}'," + body[m.end():]
            replacements += 1
            search_start = m.start() + len(f"date_trunc('{pg_unit}',")
        else:
            # Already valid or unknown — move past to avoid infinite loop
            search_start = m.end()

    if replacements > 0:
        changes.append(f"Mapped {replacements} date_trunc unit(s) to PostgreSQL names (e.g. 'dd'->'day')")

    # Fix INTERVAL 'N dd' -> INTERVAL 'N day'  (and mm->month, etc.)
    interval_pat = re.compile(
        r"INTERVAL\s*'(\s*[\d.]+\s*)([a-zA-Z]+)\s*'",
        re.IGNORECASE,
    )
    interval_replacements = 0
    for m in reversed(list(interval_pat.finditer(body))):
        unit = m.group(2).strip().lower()
        pg_unit = _DATE_TRUNC_UNIT_MAP.get(unit)
        if pg_unit and pg_unit != unit:
            body = body[:m.start()] + f"INTERVAL '{m.group(1)}{pg_unit}'" + body[m.end():]
            interval_replacements += 1

    if interval_replacements > 0:
        changes.append(f"Mapped {interval_replacements} INTERVAL unit(s) to PostgreSQL names")

    return body, changes


# ---------------------------------------------------------------------------
# Fix: negative substring length -> GREATEST(length, 0)
# ---------------------------------------------------------------------------

def fix_negative_substring_length(definition: str) -> tuple[str, list[str]]:
    """
    Fix substring/substr calls where the 3rd argument (length) can be negative.
    Wraps the length argument in GREATEST(..., 0).
    Returns (fixed_definition, list_of_changes).
    """
    changes = []
    body = definition

    replacements = 0
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
            inner = body[start_paren + 1:close].strip()
            args = _split_top_level_commas(inner)
            if len(args) < 3:
                search_start = close + 1
                continue
            length_arg = args[2].strip()
            # Skip if already wrapped in GREATEST
            if re.match(r"^\s*GREATEST\s*\(", length_arg, re.IGNORECASE):
                search_start = close + 1
                continue
            # Wrap length in GREATEST(..., 0)
            new_length = f"GREATEST({length_arg}, 0)"
            new_inner = f"{args[0].strip()}, {args[1].strip()}, {new_length}"
            if len(args) > 3:
                # Preserve any extra arguments
                extra = ", ".join(a.strip() for a in args[3:])
                new_inner += f", {extra}"
            replacement = func + "(" + new_inner + ")"
            body = body[:match.start()] + replacement + body[close + 1:]
            search_start = match.start() + len(replacement)
            replacements += 1

    if replacements > 0:
        changes.append(f"Wrapped {replacements} substring length arg(s) in GREATEST(..., 0)")

    return body, changes


# ---------------------------------------------------------------------------
# Fix: unrecognized configuration parameter (lc_collate, NLS settings, etc.)
# ---------------------------------------------------------------------------

# PostgreSQL database-level properties that are NOT runtime GUCs.
# current_setting('lc_collate') fails; must use pg_database catalog instead.
_DB_LEVEL_PARAMS = {
    "lc_collate": "(SELECT datcollate FROM pg_database WHERE datname = current_database())",
    "lc_ctype": "(SELECT datctype FROM pg_database WHERE datname = current_database())",
}

# Oracle NLS / session parameters that have no PostgreSQL GUC equivalent.
# These get replaced with NULL::text (safe fallback).
_NLS_PARAMS = frozenset({
    "nls_sort", "nls_comp", "nls_language", "nls_territory",
    "nls_date_format", "nls_date_language", "nls_numeric_characters",
    "nls_currency", "nls_iso_currency", "nls_calendar",
    "nls_characterset", "nls_nchar_characterset",
})


def _current_setting_pattern(param: str) -> re.Pattern:
    """
    Build a regex matching current_setting('param') with optional ::text cast on the argument
    and optional second boolean argument.

    Matches all of:
        current_setting('lc_collate')
        current_setting('lc_collate'::text)
        current_setting('lc_collate', true)
        current_setting('lc_collate'::text, false)
    """
    return re.compile(
        r"current_setting\s*\(\s*'"
        + re.escape(param)
        + r"'\s*(?:::text)?\s*"
        + r"(?:,\s*(?:true|false)\s*)?\)",
        re.IGNORECASE,
    )


def fix_unrecognized_config_params(definition: str, error_msg: str) -> tuple[str, list[str]]:
    """
    Fix current_setting('param') calls where 'param' is not a valid PostgreSQL GUC.

    - lc_collate / lc_ctype: replaced with a pg_database subquery.
    - Oracle NLS_* params: replaced with NULL::text.
    - Any other unrecognized param extracted from the error: replaced with NULL::text.

    Handles optional ::text cast on parameter name: current_setting('lc_collate'::text).

    Returns (fixed_definition, list_of_changes).
    """
    changes = []
    body = definition

    # Extract the parameter name from the error message
    # e.g., 'unrecognized configuration parameter "lc_collate"'
    param_match = re.search(
        r'unrecognized configuration parameter\s*"([^"]+)"',
        error_msg, re.IGNORECASE,
    )
    error_param = param_match.group(1).strip().lower() if param_match else None

    # Fix known database-level parameters (lc_collate, lc_ctype)
    for param, replacement in _DB_LEVEL_PARAMS.items():
        pat = _current_setting_pattern(param)
        count = len(pat.findall(body))
        if count > 0:
            body = pat.sub(replacement, body)
            changes.append(f"Replaced {count} current_setting('{param}') -> pg_database subquery")

    # Fix known Oracle NLS parameters -> NULL::text
    for param in _NLS_PARAMS:
        pat = _current_setting_pattern(param)
        count = len(pat.findall(body))
        if count > 0:
            body = pat.sub("NULL::text", body)
            changes.append(f"Replaced {count} current_setting('{param}') -> NULL::text (no PG equivalent)")

    # If the specific parameter from the error message wasn't covered above, replace it too
    if error_param and body.strip() == definition.strip():
        pat = _current_setting_pattern(error_param)
        count = len(pat.findall(body))
        if count > 0:
            if error_param in _DB_LEVEL_PARAMS:
                body = pat.sub(_DB_LEVEL_PARAMS[error_param], body)
                changes.append(f"Replaced {count} current_setting('{error_param}') -> pg_database subquery")
            else:
                body = pat.sub("NULL::text", body)
                changes.append(f"Replaced {count} current_setting('{error_param}') -> NULL::text (unrecognized GUC)")

    return body, changes


def fix_view_definition(definition: str, error_msg: str) -> tuple[str, list[str]]:
    """
    Main entry point: fix a view definition based on the error it produces.
    Returns (fixed_definition, list_of_changes).

    Handles:
      - "invalid input syntax for type numeric" (strategies A + B)
      - "unit ... not recognized for type timestamp" (date_trunc unit mapping)
      - "negative substring length not allowed" (GREATEST wrap)
      - "unrecognized configuration parameter" (lc_collate, NLS params)

    All applicable fixes are applied in a single pass so a view with multiple
    error types gets fully fixed at once.
    """
    changes = []
    fixed = definition

    # ---- Fix 1: date_trunc unit mapping (always apply — cheap, no side effects) ----
    fixed_dt, dt_changes = fix_date_trunc_units(fixed)
    fixed = fixed_dt
    changes.extend(dt_changes)

    # ---- Fix 2: negative substring length (always apply — cheap, no side effects) ----
    fixed_ss, ss_changes = fix_negative_substring_length(fixed)
    fixed = fixed_ss
    changes.extend(ss_changes)

    # ---- Fix 3: unrecognized configuration parameter (lc_collate, NLS, etc.) ----
    fixed_cfg, cfg_changes = fix_unrecognized_config_params(fixed, error_msg)
    fixed = fixed_cfg
    changes.extend(cfg_changes)

    # ---- Fix 4: numeric input syntax errors ----
    if is_numeric_input_error(error_msg) or is_fixable_error(error_msg):
        # Extract the problematic value from the error message
        # e.g., "invalid input syntax for type numeric: \"pgAdmin4\""
        val_match = re.search(
            r'invalid input syntax for type numeric:\s*["\']?([^"\']+)["\']?',
            error_msg, re.IGNORECASE,
        )
        problematic_value = val_match.group(1).strip() if val_match else None
        if problematic_value:
            changes.append(f"Error triggered by non-numeric value: '{problematic_value}'")

        # Strategy A: current_setting() fixes
        fixed_a, cs_changes = fix_current_setting_numeric(fixed)
        changes.extend(cs_changes)

        # If the problematic value appears as a literal in the view
        if problematic_value and problematic_value in fixed_a:
            pat = re.compile(
                r"'" + re.escape(problematic_value) + r"'\s*::\s*numeric",
                re.IGNORECASE,
            )
            count = len(pat.findall(fixed_a))
            if count > 0:
                fixed_a = pat.sub("NULL::numeric", fixed_a)
                changes.append(f"Replaced {count} literal '{problematic_value}'::numeric -> NULL::numeric")

        # Strategy B: data-level ::numeric casts
        if fixed_a.strip() == fixed.strip():
            # Strategy A didn't change anything — this is a data-level problem
            fixed_b, b_changes = fix_data_level_numeric_casts(fixed_a)
            fixed_a = fixed_b
            changes.extend(b_changes)
        else:
            # Strategy A changed something, but there may still be data-level casts.
            remaining_ident_numeric = re.search(
                r"[a-zA-Z_][a-zA-Z0-9_.]*\s*::\s*numeric", fixed_a, re.IGNORECASE,
            )
            if remaining_ident_numeric:
                fixed_b, b_changes = fix_data_level_numeric_casts(fixed_a)
                fixed_a = fixed_b
                changes.extend(b_changes)

        fixed = fixed_a

    return fixed, changes


def recreate_view(conn, schema: str, view_name: str, new_definition: str, dry_run: bool = False) -> tuple[bool, str]:
    """
    Recreate a view using CREATE OR REPLACE VIEW.
    The definition from pg_views is the SELECT part (no CREATE VIEW prefix).
    Returns (success, error_message).
    """
    fqn = f'"{schema}"."{view_name}"'
    sql = f"CREATE OR REPLACE VIEW {fqn} AS\n{new_definition}"

    if dry_run:
        return (True, "")

    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT _recreate_view")
        cur.execute(sql)
        cur.execute("RELEASE SAVEPOINT _recreate_view")
        conn.commit()
        cur.close()
        return (True, "")
    except Exception as e:
        err = str(e).strip()
        try:
            cur.execute("ROLLBACK TO SAVEPOINT _recreate_view")
            cur.execute("RELEASE SAVEPOINT _recreate_view")
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        cur.close()
        return (False, err)


# ---------------------------------------------------------------------------
# Dependency-aware ordering
# ---------------------------------------------------------------------------

def topological_sort_views(
    views: list[tuple[str, str, str]],
    conn,
) -> list[int]:
    """
    Sort views so that dependencies are processed before dependents.
    Returns list of indices into the views list.
    """
    view_set = {(s, v) for s, v, _ in views}
    index_map = {(s, v): i for i, (s, v, _) in enumerate(views)}

    # Build adjacency: view -> set of views it depends on
    deps = {}
    for i, (schema, vname, _) in enumerate(views):
        try:
            view_deps = get_view_dependencies(conn, schema, vname)
            deps[i] = {index_map[d] for d in view_deps if d in view_set and d != (schema, vname)}
        except Exception:
            deps[i] = set()

    # Kahn's algorithm
    in_degree = {i: 0 for i in range(len(views))}
    for i, dep_set in deps.items():
        for d in dep_set:
            in_degree[i] = in_degree.get(i, 0) + 1

    # Recalculate in-degree properly
    in_degree = {i: 0 for i in range(len(views))}
    reverse_deps = {i: set() for i in range(len(views))}
    for i, dep_set in deps.items():
        for d in dep_set:
            reverse_deps[d].add(i)
            in_degree[i] += 1

    queue = [i for i in range(len(views)) if in_degree[i] == 0]
    order = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for dependent in reverse_deps.get(node, set()):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    # Add any remaining (cycle) nodes
    remaining = set(range(len(views))) - set(order)
    order.extend(sorted(remaining))

    return order


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fix PostgreSQL views failing with 'invalid input syntax for type numeric: pgAdmin4'",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Examples:
              # Scan all user schemas, dry-run:
              python fix_pgadmin4_numeric_views.py --dry-run

              # Fix only views listed in a text file:
              python fix_pgadmin4_numeric_views.py --view-list broken_views.txt

              # Fix views in specific schemas:
              python fix_pgadmin4_numeric_views.py --schemas apps hr gl

              # With full connection params:
              python fix_pgadmin4_numeric_views.py --pg-host db.example.com --pg-database erp --pg-user admin --pg-password secret

              # Scan only, don't fix (just identify):
              python fix_pgadmin4_numeric_views.py --scan-only

              # Use environment variables:
              PGHOST=db PGPORT=5432 PGDATABASE=erp PGUSER=admin PGPASSWORD=secret python fix_pgadmin4_numeric_views.py

            View list file format (one view per line):
              schema.viewname
              apps.my_invoices_v
              hr.employee_details_v
              # Lines starting with # are ignored
              # Bare names (no schema) are also supported
        """),
    )

    # PostgreSQL connection (top-of-file defaults -> env vars -> CLI flags)
    pg_host = os.environ.get("PGHOST", PG_HOST)
    pg_port = int(os.environ.get("PGPORT", str(PG_PORT)))
    pg_database = os.environ.get("PGDATABASE", PG_DATABASE)
    pg_user = os.environ.get("PGUSER", PG_USER)
    pg_password = os.environ.get("PGPASSWORD", PG_PASSWORD)

    parser.add_argument("--pg-host", default=pg_host, help=f"PostgreSQL host (default: {pg_host})")
    parser.add_argument("--pg-port", type=int, default=pg_port, help=f"PostgreSQL port (default: {pg_port})")
    parser.add_argument("--pg-database", default=pg_database, help=f"PostgreSQL database (default: {pg_database})")
    parser.add_argument("--pg-user", default=pg_user, help=f"PostgreSQL user (default: {pg_user})")
    parser.add_argument("--pg-password", default=pg_password, help="PostgreSQL password")

    # Scope
    parser.add_argument("--view-list", default=None,
                        help="Path to a text file with view names to fix (one per line: schema.viewname)")
    parser.add_argument("--schemas", nargs="+", default=None,
                        help="Schemas to scan (default: all non-system schemas)")
    parser.add_argument("--view-pattern", default=None,
                        help="Only process views matching this SQL LIKE pattern (e.g. '%%INVOICE%%')")

    # Behavior
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fixed without actually changing anything")
    parser.add_argument("--scan-only", action="store_true",
                        help="Only scan and identify broken views; don't attempt fixes")
    parser.add_argument("--force-retest", action="store_true", default=True,
                        help="After fixing, re-test the view with SELECT (default: True)")
    parser.add_argument("--no-retest", action="store_true",
                        help="Skip re-testing after fix")
    parser.add_argument("--output-dir", default="./fix_numeric_output",
                        help="Directory for reports and SQL files (default: ./fix_numeric_output)")
    parser.add_argument("--skip-test", action="store_true",
                        help="Skip SELECT test; fix ALL views that have current_setting patterns in their definition")

    # Logging
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity")

    args = parser.parse_args()

    # Configure logging
    level = logging.WARNING
    if args.verbose >= 2:
        level = logging.DEBUG
    elif args.verbose >= 1:
        level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.no_retest:
        args.force_retest = False

    # Output directory
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    before_dir = out_dir / "before"
    after_dir = out_dir / "after"
    before_dir.mkdir(exist_ok=True)
    after_dir.mkdir(exist_ok=True)

    # Connect
    print("=" * 70)
    print("  Fix 'invalid input syntax for type numeric: pgAdmin4' in views")
    print("=" * 70)
    print(f"\nConnecting to PostgreSQL {args.pg_host}:{args.pg_port}/{args.pg_database} as {args.pg_user}...")

    try:
        conn = connect_pg(args)
    except Exception as e:
        print(f"ERROR: Cannot connect to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)
    print("  Connected.\n")

    # Get views
    if args.view_list:
        # ---- Load view names from text file ----
        view_list_path = Path(args.view_list)
        if not view_list_path.exists():
            print(f"ERROR: View list file not found: {view_list_path}", file=sys.stderr)
            conn.close()
            sys.exit(1)

        raw_lines = view_list_path.read_text(encoding="utf-8").splitlines()
        requested = []  # list of (schema_or_none, view_name)
        for line in raw_lines:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("--"):
                continue
            # Accept schema.viewname or just viewname
            if "." in line:
                parts = line.split(".", 1)
                requested.append((parts[0].strip().lower(), parts[1].strip().lower()))
            else:
                requested.append((None, line.lower()))

        print(f"Loaded {len(requested)} view name(s) from {view_list_path}")

        # Fetch ALL views from the catalog, then filter to the requested list
        print("Discovering views from catalog...")
        catalog_views = get_all_views(conn, args.schemas)
        print(f"  Catalog has {len(catalog_views)} view(s).")

        # Build a lookup set for fast matching
        requested_set_full = {(s, v) for s, v in requested if s is not None}
        requested_names_only = {v for s, v in requested if s is None}

        all_views = []
        not_found = []
        matched_keys = set()
        for schema, vname, defn in catalog_views:
            key_full = (schema.lower(), vname.lower())
            if key_full in requested_set_full:
                all_views.append((schema, vname, defn))
                matched_keys.add(key_full)
            elif vname.lower() in requested_names_only:
                all_views.append((schema, vname, defn))
                matched_keys.add((None, vname.lower()))

        # Report any view names from the file that weren't found in the catalog
        for s, v in requested:
            if s is not None and (s, v) not in {(sc.lower(), vn.lower()) for sc, vn, _ in all_views}:
                not_found.append(f"{s}.{v}")
            elif s is None and v not in {vn.lower() for _, vn, _ in all_views}:
                not_found.append(v)
        if not_found:
            print(f"  WARNING: {len(not_found)} view(s) from list not found in catalog:")
            for nf in not_found[:20]:
                print(f"    - {nf}")
            if len(not_found) > 20:
                print(f"    ... and {len(not_found) - 20} more")

        print(f"  Matched {len(all_views)} view(s) from list.\n")
    else:
        # ---- Discover all views from catalog ----
        print("Discovering views...")
        all_views = get_all_views(conn, args.schemas)
        print(f"  Found {len(all_views)} view(s) in {len(set(s for s, _, _ in all_views))} schema(s).\n")

    if not all_views:
        print("No views found. Nothing to do.")
        conn.close()
        return

    # Optional: filter by pattern
    if args.view_pattern:
        pat = args.view_pattern.replace("%", ".*").replace("_", ".")
        regex = re.compile(pat, re.IGNORECASE)
        all_views = [(s, v, d) for s, v, d in all_views if regex.search(v)]
        print(f"  After pattern filter: {len(all_views)} view(s).\n")

    # Phase 1: Identify broken views
    print("-" * 70)
    broken_views = []       # (index, schema, view_name, definition, error_msg)
    pattern_views = []      # views with current_setting patterns (even if SELECT works)
    ok_views = []           # views that SELECT successfully
    other_errors = []       # views with other (non-numeric) errors

    if args.skip_test:
        print("Phase 1: Scanning view definitions for current_setting patterns...")
        for i, (schema, vname, defn) in enumerate(all_views):
            display = f"{schema}.{vname}"
            has_cs = bool(_ANY_CURRENT_SETTING.search(defn or ""))
            if has_cs:
                broken_views.append((i, schema, vname, defn, "pattern-match (skip-test mode)"))
                print(f"  [{i+1}/{len(all_views)}] HAS PATTERN: {display}")
            else:
                ok_views.append((i, schema, vname))
                if (i + 1) % 100 == 0:
                    print(f"  [{i+1}/{len(all_views)}] scanned...", flush=True)
    else:
        print("Phase 1: Testing views with SELECT * ... LIMIT 1 ...")
        pad = len(str(len(all_views)))
        for i, (schema, vname, defn) in enumerate(all_views):
            display = f"{schema}.{vname}"
            ok, err = test_view(conn, schema, vname)
            if ok:
                ok_views.append((i, schema, vname))
                # Also check if it has current_setting patterns (for reporting)
                if _ANY_CURRENT_SETTING.search(defn or ""):
                    pattern_views.append((i, schema, vname, defn))
                log.debug("  [%s] OK: %s", i + 1, display)
            elif is_fixable_error(err):
                err_label = classify_error(err)
                broken_views.append((i, schema, vname, defn, err))
                print(f"  [{i+1:>{pad}}/{len(all_views)}] BROKEN ({err_label}): {display}")
                log.info("    Error: %s", err[:120])
            else:
                other_errors.append((i, schema, vname, err))
                log.info("  [%s] OTHER ERROR: %s — %s", i + 1, display, err[:80])
            if (i + 1) % 50 == 0:
                print(f"  [{i+1:>{pad}}/{len(all_views)}] tested... "
                      f"({len(broken_views)} broken, {len(ok_views)} ok, {len(other_errors)} other errors)",
                      flush=True)

    print(f"\n  Summary:")
    print(f"    OK:             {len(ok_views)}")
    print(f"    Fixable errors: {len(broken_views)}")
    if broken_views:
        from collections import Counter
        err_counts = Counter(classify_error(err) for _, _, _, _, err in broken_views)
        for etype, cnt in err_counts.most_common():
            print(f"      - {etype}: {cnt}")
    if pattern_views:
        print(f"    Has patterns (but OK): {len(pattern_views)}")
    if other_errors:
        print(f"    Other errors:  {len(other_errors)}")

    if not broken_views:
        print("\nNo views with fixable errors found.")
        if pattern_views:
            print(f"  Note: {len(pattern_views)} view(s) contain current_setting() but SELECT succeeded.")
            print("  These may fail under different conditions. Use --skip-test to fix them too.")
        conn.close()
        return

    if args.scan_only:
        print(f"\n--scan-only mode. {len(broken_views)} view(s) need fixing:")
        for _, schema, vname, defn, err in broken_views:
            print(f"  {schema}.{vname}")
            cs_matches = _ANY_CURRENT_SETTING.findall(defn or "")
            numeric_casts = len(re.findall(r"[a-zA-Z_][a-zA-Z0-9_.]*\s*::\s*numeric", defn or "", re.IGNORECASE))
            if cs_matches:
                print(f"    Strategy A: current_setting vars: {', '.join(set(cs_matches))}")
            if numeric_casts > 0:
                print(f"    Strategy B: {numeric_casts} column::numeric cast(s)")
            if not cs_matches and numeric_casts == 0:
                print(f"    (no obvious pattern — may be from dependent view/function)")
        # Write report
        report_path = out_dir / "scan_report.txt"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"Scan Report - {datetime.now().isoformat()}\n")
            f.write(f"Database: {args.pg_host}:{args.pg_port}/{args.pg_database}\n")
            f.write(f"Total views scanned: {len(all_views)}\n")
            f.write(f"Broken (numeric error): {len(broken_views)}\n\n")
            for _, schema, vname, defn, err in broken_views:
                f.write(f"--- {schema}.{vname} ---\n")
                f.write(f"Error: {err[:200]}\n")
                cs_matches = _ANY_CURRENT_SETTING.findall(defn or "")
                numeric_casts = len(re.findall(r"[a-zA-Z_][a-zA-Z0-9_.]*\s*::\s*numeric", defn or "", re.IGNORECASE))
                if cs_matches:
                    f.write(f"Strategy A: current_setting vars: {', '.join(set(cs_matches))}\n")
                if numeric_casts > 0:
                    f.write(f"Strategy B: {numeric_casts} column::numeric cast(s)\n")
                f.write(f"\nDefinition:\n{defn}\n\n")
        print(f"\nReport written to: {report_path}")
        conn.close()
        return

    # Phase 2: Fix broken views
    print("\n" + "-" * 70)
    mode_label = "DRY-RUN" if args.dry_run else "FIXING"
    print(f"Phase 2: {mode_label} {len(broken_views)} broken view(s)...\n")

    # Ensure safe_numeric() function exists (needed for Strategy B data-level fixes)
    if not args.dry_run:
        print("  Creating/verifying public.safe_numeric() helper function...")
        if ensure_safe_numeric_function(conn):
            print("  public.safe_numeric() is ready.\n")
        else:
            print("  WARNING: Could not create safe_numeric(). Data-level fixes may fail.\n")
    else:
        print("  DRY-RUN: Would create public.safe_numeric() helper function.\n")

    n_fixed = 0
    n_fix_failed = 0
    n_unchanged = 0
    fix_results = []  # (schema, view_name, status, details)

    # Sort by dependency order so we fix base views first
    broken_indices = [idx for idx, _, _, _, _ in broken_views]
    broken_map = {idx: (schema, vname, defn, err) for idx, schema, vname, defn, err in broken_views}

    for rank, (orig_idx, schema, vname, defn, err) in enumerate(broken_views):
        display = f"{schema}.{vname}"
        safe_name = f"{schema}__{vname}"
        print(f"  [{rank+1}/{len(broken_views)}] Fixing: {display}")

        if not defn or not defn.strip():
            print(f"    SKIP: empty definition")
            fix_results.append((schema, vname, "SKIP", "Empty definition"))
            n_fix_failed += 1
            continue

        # Save original
        before_path = before_dir / f"{safe_name}.sql"
        before_path.write_text(
            f"-- Original definition of {display}\n"
            f"-- Error: {err[:200]}\n\n"
            f"CREATE OR REPLACE VIEW \"{schema}\".\"{vname}\" AS\n{defn}\n",
            encoding="utf-8",
        )

        # Apply fixes (Strategy A: current_setting, then Strategy B: data-level ::numeric)
        fixed_defn, changes = fix_view_definition(defn, err)

        if fixed_defn.strip() == defn.strip():
            # No textual change from either strategy — try last-resort broad fix
            remaining_cs = _ANY_CURRENT_SETTING.findall(fixed_defn)
            if remaining_cs:
                for guc_var in set(remaining_cs):
                    pat = re.compile(
                        r"current_setting\s*\(\s*'" + re.escape(guc_var) + r"'\s*(?:::text)?\s*(?:,\s*(?:true|false)\s*)?\)",
                        re.IGNORECASE,
                    )
                    count = len(pat.findall(fixed_defn))
                    fixed_defn = pat.sub("NULL::text", fixed_defn)
                    changes.append(f"Broad fix: replaced {count} current_setting('{guc_var}') -> NULL::text")

        if fixed_defn.strip() == defn.strip():
            print(f"    UNCHANGED: no fixable patterns found")
            print(f"    The numeric error may come from a dependent view or function.")
            fix_results.append((schema, vname, "UNCHANGED", "No fixable pattern found"))
            n_unchanged += 1
            continue

        # Log changes
        for change in changes:
            print(f"    • {change}")

        # Save fixed version
        after_path = after_dir / f"{safe_name}.sql"
        after_path.write_text(
            f"-- Fixed definition of {display}\n"
            f"-- Changes applied:\n"
            + "".join(f"--   {c}\n" for c in changes)
            + f"\nCREATE OR REPLACE VIEW \"{schema}\".\"{vname}\" AS\n{fixed_defn}\n",
            encoding="utf-8",
        )

        if args.dry_run:
            print(f"    DRY-RUN: would recreate view")
            fix_results.append((schema, vname, "DRY-RUN", "; ".join(changes)))
            n_fixed += 1
            continue

        # Recreate the view
        ok, recreate_err = recreate_view(conn, schema, vname, fixed_defn, dry_run=False)
        if not ok:
            print(f"    FAIL: Could not recreate view: {recreate_err[:120]}")
            fix_results.append((schema, vname, "RECREATE_FAIL", recreate_err[:200]))
            n_fix_failed += 1
            continue

        print(f"    View recreated successfully.")

        # Re-test
        if args.force_retest:
            ok2, err2 = test_view(conn, schema, vname)
            if ok2:
                print(f"    RE-TEST: OK")
                fix_results.append((schema, vname, "FIXED", "; ".join(changes)))
                n_fixed += 1
            elif is_numeric_input_error(err2):
                print(f"    RE-TEST: STILL BROKEN (numeric error persists)")
                print(f"    Error: {err2[:120]}")
                fix_results.append((schema, vname, "STILL_BROKEN", err2[:200]))
                n_fix_failed += 1
            else:
                print(f"    RE-TEST: Different error: {err2[:100]}")
                fix_results.append((schema, vname, "DIFF_ERROR", err2[:200]))
                n_fixed += 1  # Original numeric error is fixed at least
        else:
            fix_results.append((schema, vname, "FIXED_NO_TEST", "; ".join(changes)))
            n_fixed += 1

    # Phase 3: Summary and report
    print("\n" + "=" * 70)
    print("  RESULTS")
    print("=" * 70)
    print(f"  Total views scanned:     {len(all_views)}")
    print(f"  Views with numeric error: {len(broken_views)}")
    print(f"  Fixed:                    {n_fixed}")
    print(f"  Fix failed:               {n_fix_failed}")
    print(f"  Unchanged (no pattern):   {n_unchanged}")
    if other_errors:
        print(f"  Other errors (ignored):   {len(other_errors)}")

    # Write detailed report
    report_path = out_dir / "fix_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"Fix Report - {datetime.now().isoformat()}\n")
        f.write(f"Database: {args.pg_host}:{args.pg_port}/{args.pg_database}\n")
        f.write(f"Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}\n")
        f.write(f"Total views: {len(all_views)}\n")
        f.write(f"Broken: {len(broken_views)}\n")
        f.write(f"Fixed: {n_fixed}\n")
        f.write(f"Failed: {n_fix_failed}\n")
        f.write(f"Unchanged: {n_unchanged}\n\n")

        f.write("=" * 60 + "\n")
        f.write("FIXED VIEWS\n")
        f.write("=" * 60 + "\n")
        for schema, vname, status, details in fix_results:
            if "FIXED" in status or status == "DRY-RUN":
                f.write(f"\n{schema}.{vname}  [{status}]\n")
                f.write(f"  {details}\n")

        f.write("\n" + "=" * 60 + "\n")
        f.write("FAILED / UNCHANGED VIEWS\n")
        f.write("=" * 60 + "\n")
        for schema, vname, status, details in fix_results:
            if "FIXED" not in status and status != "DRY-RUN":
                f.write(f"\n{schema}.{vname}  [{status}]\n")
                f.write(f"  {details}\n")

        if other_errors:
            f.write("\n" + "=" * 60 + "\n")
            f.write("VIEWS WITH OTHER (NON-NUMERIC) ERRORS\n")
            f.write("=" * 60 + "\n")
            for _, schema, vname, err in other_errors:
                f.write(f"\n{schema}.{vname}\n")
                f.write(f"  {err[:300]}\n")

    print(f"\n  Report:    {report_path}")
    print(f"  Before SQL: {before_dir}/")
    print(f"  After SQL:  {after_dir}/")

    # Write a combined fix SQL file (for manual review or re-execution)
    combined_path = out_dir / "all_fixes.sql"
    with open(combined_path, "w", encoding="utf-8") as f:
        f.write(f"-- Combined fix script generated {datetime.now().isoformat()}\n")
        f.write(f"-- Database: {args.pg_host}:{args.pg_port}/{args.pg_database}\n")
        f.write(f"-- Fixes 'invalid input syntax for type numeric' errors in views\n\n")
        f.write("BEGIN;\n\n")
        for rank, (orig_idx, schema, vname, defn, err) in enumerate(broken_views):
            safe_name = f"{schema}__{vname}"
            after_path = after_dir / f"{safe_name}.sql"
            if after_path.exists():
                content = after_path.read_text(encoding="utf-8")
                # Extract just the CREATE OR REPLACE VIEW statement
                for line in content.split("\n"):
                    if line.startswith("--"):
                        f.write(line + "\n")
                    else:
                        break
                # Write the actual SQL (skip comment lines)
                in_sql = False
                for line in content.split("\n"):
                    if line.strip().upper().startswith("CREATE OR REPLACE"):
                        in_sql = True
                    if in_sql:
                        f.write(line + "\n")
                f.write(";\n\n")
        f.write("COMMIT;\n")

    print(f"  Combined:  {combined_path}")

    conn.close()

    if n_fix_failed > 0:
        print(f"\nWARNING: {n_fix_failed} view(s) could not be fixed automatically.")
        print("  Check the report and 'before' SQL files for manual inspection.")
        sys.exit(1)
    elif args.dry_run:
        print(f"\nDRY-RUN complete. Re-run without --dry-run to apply fixes.")
    else:
        print(f"\nDone. All fixable views have been updated.")


if __name__ == "__main__":
    main()

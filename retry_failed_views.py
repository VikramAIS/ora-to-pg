#!/usr/bin/env python3
"""
Retry failed PostgreSQL views — resolve missing dependencies from Oracle, create stubs as fallback.

STANDALONE: This script has no dependency on any other project file. It uses the Python stdlib
and optional pip packages: oracledb (Oracle), psycopg2 (PostgreSQL).

Use Case:
    After migrating Oracle views to PostgreSQL (using oracle_sql_dir_to_postgres_synonyms.py),
    some views fail due to dependency issues — they reference tables/views that don't exist in
    PostgreSQL. For ARCHIVAL databases where the data is static, this script:

    Phase 1 — Analysis:
        Load failed .sql files, parse CREATE VIEW and dependency references, query the PostgreSQL
        catalog to find which dependencies are missing.

    Phase 2 — Resolve from Oracle:
        For each missing dependency, look it up in Oracle:
        - Resolve synonyms (ALL_SYNONYMS) to real owner.object_name.
        - Get object type (ALL_OBJECTS): TABLE or VIEW.
        - Fetch column structure (ALL_TAB_COLUMNS — works for both tables and views).
        - Map Oracle types to PostgreSQL types.
        - Generate CREATE TABLE in PostgreSQL (even for Oracle views — creating as a table avoids
          recursive dependency issues and is safe for archival data).
        If --with-data is specified, also copies row data from Oracle.

    Phase 3 — Stub fallback:
        For dependencies not found in Oracle either, create a stub:
          CREATE TABLE schema.name (dummy_id integer); -- so dependent views can at least parse.

    Phase 4 — Multi-pass retry:
        Retry all failed views on PostgreSQL in dependency order. Repeat passes until convergence
        (no new views resolve in a pass).

    Phase 5 — Report:
        Write resolved views, still-failed views, and a summary report.

Run:
    python retry_failed_views.py <failed_dir> [options]

    # Full pipeline: Oracle lookup + retry
    python retry_failed_views.py postgres_views_failed \\
        --oracle-user SYSTEM --oracle-password secret --oracle-dsn host:1521/ORCL \\
        --pg-host localhost --pg-database mydb --pg-user postgres --pg-password secret \\
        --ensure-schema

    # With data copy for missing tables (optional, can be slow for large tables):
    python retry_failed_views.py postgres_views_failed \\
        --oracle-user SYSTEM --oracle-password secret --oracle-dsn host:1521/ORCL \\
        --pg-host localhost --pg-database mydb --pg-user postgres --pg-password secret \\
        --with-data --data-row-limit 100000

    # No Oracle (stub-only fallback):
    python retry_failed_views.py postgres_views_failed \\
        --pg-host localhost --pg-database mydb --pg-user postgres --pg-password secret

    # Dry run (no connections needed):
    python retry_failed_views.py postgres_views_failed --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from collections import deque
from pathlib import Path
from typing import Optional

log = logging.getLogger("retry_failed_views")

# ---- Connection defaults (override with env or CLI) ----
ORACLE_USER: Optional[str] = None
ORACLE_PASSWORD: Optional[str] = None
ORACLE_DSN: str = "localhost:1521/ORCL"

PG_HOST: str = "localhost"
PG_PORT: int = 5432
PG_DATABASE: str = "postgres"
PG_USER: str = "postgres"
PG_PASSWORD: str = ""


# ---- Oracle type -> PostgreSQL type mapping ----
ORACLE_TO_PG_TYPE: dict[str, str] = {
    "VARCHAR2": "text",
    "NVARCHAR2": "text",
    "VARCHAR": "text",
    "CHAR": "character",
    "NCHAR": "character",
    "CLOB": "text",
    "NCLOB": "text",
    "BLOB": "bytea",
    "RAW": "bytea",
    "LONG": "text",
    "LONG RAW": "bytea",
    "NUMBER": "numeric",
    "FLOAT": "double precision",
    "BINARY_FLOAT": "real",
    "BINARY_DOUBLE": "double precision",
    "INTEGER": "integer",
    "SMALLINT": "smallint",
    "DATE": "timestamp",
    "TIMESTAMP(0)": "timestamp(0)",
    "TIMESTAMP(3)": "timestamp(3)",
    "TIMESTAMP(6)": "timestamp",
    "TIMESTAMP(9)": "timestamp",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp with time zone",
    "TIMESTAMP WITH LOCAL TIME ZONE": "timestamp with time zone",
    "INTERVAL YEAR TO MONTH": "interval",
    "INTERVAL DAY TO SECOND": "interval",
    "ROWID": "text",
    "UROWID": "text",
    "XMLTYPE": "xml",
    "SDO_GEOMETRY": "text",
    "BOOLEAN": "boolean",
    "BFILE": "text",
}


# ---- Regex patterns ----
_CREATE_VIEW_PATTERN = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+(\S+)\s+AS\b",
    re.IGNORECASE,
)

_TABLE_REF_PATTERN = re.compile(
    r"\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+"
    r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
    re.IGNORECASE,
)

_RELATION_NOT_EXIST_PATTERN = re.compile(
    r'relation\s+"?([a-zA-Z_][a-zA-Z0-9_.]*)"?\s+does\s+not\s+exist',
    re.IGNORECASE,
)

# SQL keywords that can appear after FROM/JOIN but are NOT table names
_SQL_KEYWORDS = frozenset({
    "select", "where", "on", "and", "or", "not", "in", "exists", "case", "when",
    "then", "else", "end", "as", "null", "true", "false", "is", "between", "like",
    "having", "group", "order", "by", "limit", "offset", "union", "intersect",
    "except", "all", "distinct", "lateral", "values", "set", "returning",
    "with", "recursive", "inner", "outer", "left", "right", "full", "cross",
    "natural", "join", "from", "(", ")", "dual",
})


# ---------------------------------------------------------------------------
#  Phase 1 helpers — Load & parse failed SQL
# ---------------------------------------------------------------------------

def parse_view_name(sql: str) -> tuple[str, str]:
    """Parse schema and view name from CREATE VIEW SQL."""
    match = _CREATE_VIEW_PATTERN.search(sql)
    if not match:
        return "", ""
    name_part = match.group(1).strip().strip('"').lower()
    if "." in name_part:
        parts = name_part.split(".", 1)
        return parts[0].strip().strip('"'), parts[1].strip().strip('"')
    return "", name_part


def extract_dependency_refs(sql: str) -> set[tuple[str, str]]:
    """Extract FROM/JOIN table/view references from the view body."""
    match = _CREATE_VIEW_PATTERN.search(sql)
    if not match:
        return set()
    body = sql[match.end():]
    refs: set[tuple[str, str]] = set()
    for m in _TABLE_REF_PATTERN.finditer(body):
        ref = m.group(1).strip().lower()
        if ref in _SQL_KEYWORDS:
            continue
        if "." in ref:
            s, v = ref.split(".", 1)
            s, v = s.strip(), v.strip()
            if v not in _SQL_KEYWORDS:
                refs.add((s, v))
        else:
            refs.add(("", ref.strip()))
    return refs


def load_sql_files(
    input_dir: str,
    pattern: str = "*.sql",
    recursive: bool = False,
) -> list[tuple[str, str, str, Path]]:
    """
    Load SQL files from directory.
    Returns list of (schema, view_name, sql_content, file_path).
    """
    path = Path(input_dir)
    if not path.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    sql_files = sorted(path.rglob(pattern) if recursive else path.glob(pattern))

    result: list[tuple[str, str, str, Path]] = []
    seen: set[tuple[str, str]] = set()
    skipped = 0

    for f in sql_files:
        if not f.is_file() or f.name == "error_log.txt":
            continue
        try:
            content = f.read_text(encoding="utf-8-sig", errors="replace").strip()
        except Exception as e:
            log.warning("Skip unreadable file %s: %s", f.name, e)
            skipped += 1
            continue
        if not content:
            skipped += 1
            continue
        schema, view_name = parse_view_name(content)
        if not view_name:
            log.warning("Skip file %s: no CREATE VIEW found", f.name)
            skipped += 1
            continue
        key = (schema.lower(), view_name.lower())
        if key in seen:
            skipped += 1
            continue
        seen.add(key)
        result.append((schema, view_name, content, f))

    if skipped:
        log.info("Skipped %d file(s)", skipped)
    return result


# ---------------------------------------------------------------------------
#  Phase 1 helpers — Query PostgreSQL catalog
# ---------------------------------------------------------------------------

def get_existing_pg_relations(pg_conn) -> set[tuple[str, str]]:
    """
    Query PostgreSQL catalog for all existing tables and views.
    Returns set of (schema_lower, name_lower).
    """
    relations: set[tuple[str, str]] = set()
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT schemaname, tablename FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT schemaname, viewname FROM pg_views
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT schemaname, matviewname FROM pg_matviews
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """)
        for row in cur.fetchall():
            relations.add((row[0].lower(), row[1].lower()))
    return relations


def find_missing_dependencies(
    views: list[tuple[str, str, str, Path]],
    existing_pg: set[tuple[str, str]],
) -> dict[tuple[str, str], set[tuple[str, str]]]:
    """
    Find dependencies referenced by failed views that don't exist in PostgreSQL.
    Returns dict: missing_dep (schema, name) -> set of (view_schema, view_name) that need it.
    """
    # Build set of view keys (these are about to be created, so don't count as missing)
    view_keys: set[tuple[str, str]] = set()
    for schema, view_name, _, _ in views:
        view_keys.add((schema.lower(), view_name.lower()))

    missing: dict[tuple[str, str], set[tuple[str, str]]] = {}

    for schema, view_name, sql, _ in views:
        refs = extract_dependency_refs(sql)
        for ref_key in refs:
            # Not in PG catalog and not one of the views we're retrying
            if ref_key not in existing_pg and ref_key not in view_keys:
                # Also try matching without schema (public schema)
                unqualified_match = False
                if ref_key[0] == "":
                    # Check if any schema has this name
                    for (es, en) in existing_pg:
                        if en == ref_key[1]:
                            unqualified_match = True
                            break
                    if not unqualified_match:
                        for (vs, vn) in view_keys:
                            if vn == ref_key[1]:
                                unqualified_match = True
                                break

                if not unqualified_match:
                    if ref_key not in missing:
                        missing[ref_key] = set()
                    missing[ref_key].add((schema.lower(), view_name.lower()))

    return missing


# ---------------------------------------------------------------------------
#  Phase 2 — Oracle lookup
# ---------------------------------------------------------------------------

def oracle_connect(user: str, password: str, dsn: str, max_attempts: int = 3):
    """Open Oracle connection with retries."""
    import oracledb
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return oracledb.connect(user=user, password=password, dsn=dsn)
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                time.sleep(2)
    raise last_err  # type: ignore[misc]


def resolve_oracle_synonym(ora_conn, name_upper: str, schema_upper: str = "") -> Optional[tuple[str, str]]:
    """
    Resolve an Oracle synonym to (real_owner, real_object_name).
    Tries: schema-qualified private synonym first, then PUBLIC synonym.
    Returns None if no synonym found.
    """
    cursor = ora_conn.cursor()
    try:
        # Try schema-qualified private synonym
        if schema_upper:
            cursor.execute("""
                SELECT table_owner, table_name FROM all_synonyms
                WHERE owner = :o AND synonym_name = :s
                AND table_owner IS NOT NULL AND table_name IS NOT NULL
                AND ROWNUM = 1
            """, {"o": schema_upper, "s": name_upper})
            row = cursor.fetchone()
            if row:
                return (row[0].upper(), row[1].upper())

        # Try PUBLIC synonym
        cursor.execute("""
            SELECT table_owner, table_name FROM all_synonyms
            WHERE owner = 'PUBLIC' AND synonym_name = :s
            AND table_owner IS NOT NULL AND table_name IS NOT NULL
            AND ROWNUM = 1
        """, {"s": name_upper})
        row = cursor.fetchone()
        if row:
            return (row[0].upper(), row[1].upper())

        return None
    finally:
        cursor.close()


def get_oracle_object_type(ora_conn, owner_upper: str, name_upper: str) -> Optional[str]:
    """Return Oracle object type: 'TABLE', 'VIEW', 'SYNONYM', or None."""
    cursor = ora_conn.cursor()
    try:
        cursor.execute("""
            SELECT object_type FROM all_objects
            WHERE owner = :o AND object_name = :n
            AND object_type IN ('TABLE', 'VIEW', 'SYNONYM', 'MATERIALIZED VIEW')
            AND ROWNUM = 1
        """, {"o": owner_upper, "n": name_upper})
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        cursor.close()


def get_oracle_columns(ora_conn, owner_upper: str, name_upper: str) -> list[tuple[str, str, int, int, str]]:
    """
    Fetch columns from ALL_TAB_COLUMNS (works for both tables and views).
    Returns list of (column_name, data_type, data_length, data_precision, nullable)
    ordered by column_id.
    """
    cursor = ora_conn.cursor()
    try:
        cursor.execute("""
            SELECT column_name, data_type, NVL(data_length, 0), NVL(data_precision, 0),
                   NVL(data_scale, 0), nullable
            FROM all_tab_columns
            WHERE owner = :o AND table_name = :t
            ORDER BY column_id
        """, {"o": owner_upper, "t": name_upper})
        rows = cursor.fetchall()
        result = []
        for col_name, data_type, data_length, data_precision, data_scale, nullable in rows:
            result.append((col_name, data_type, data_precision, data_scale, nullable))
        return result
    finally:
        cursor.close()


def map_oracle_type_to_pg(data_type: str, precision: int, scale: int) -> str:
    """Map an Oracle column data type to PostgreSQL."""
    dt_upper = (data_type or "").upper().strip()

    # Check exact match first
    pg_type = ORACLE_TO_PG_TYPE.get(dt_upper)
    if pg_type:
        # Refine NUMBER with precision/scale
        if dt_upper == "NUMBER":
            if precision > 0 and scale > 0:
                return f"numeric({precision},{scale})"
            if precision > 0 and scale == 0:
                if precision <= 4:
                    return "smallint"
                if precision <= 9:
                    return "integer"
                if precision <= 18:
                    return "bigint"
                return f"numeric({precision})"
            # NUMBER without precision -> numeric
            return "numeric"
        if dt_upper in ("CHAR", "NCHAR") and precision > 0:
            return f"character({precision})"
        return pg_type

    # TIMESTAMP variants
    if dt_upper.startswith("TIMESTAMP"):
        if "TIME ZONE" in dt_upper:
            return "timestamp with time zone"
        return "timestamp"

    # INTERVAL variants
    if dt_upper.startswith("INTERVAL"):
        return "interval"

    # Fallback
    log.debug("Unknown Oracle type '%s', defaulting to text", dt_upper)
    return "text"


def generate_create_table_ddl(
    pg_schema: str,
    pg_name: str,
    columns: list[tuple[str, str, int, int, str]],
) -> str:
    """
    Generate PostgreSQL CREATE TABLE DDL from Oracle column info.
    columns: [(col_name, data_type, precision, scale, nullable), ...]
    """
    qualified = f"{pg_schema}.{pg_name}" if pg_schema else pg_name
    if not columns:
        return f"CREATE TABLE IF NOT EXISTS {qualified} (dummy_id integer);"

    col_defs: list[str] = []
    for col_name, data_type, precision, scale, nullable in columns:
        pg_type = map_oracle_type_to_pg(data_type, precision, scale)
        col_lower = col_name.lower()
        null_clause = "" if nullable == "Y" else " NOT NULL"
        col_defs.append(f"    {col_lower} {pg_type}{null_clause}")

    cols_sql = ",\n".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {qualified} (\n{cols_sql}\n);"


def generate_stub_ddl(pg_schema: str, pg_name: str) -> str:
    """Generate a stub table DDL for a completely unknown dependency."""
    qualified = f"{pg_schema}.{pg_name}" if pg_schema else pg_name
    return (
        f"-- STUB: dependency not found in Oracle or PostgreSQL\n"
        f"CREATE TABLE IF NOT EXISTS {qualified} (dummy_id integer);"
    )


def resolve_single_dependency(
    ora_conn,
    dep_schema: str,
    dep_name: str,
) -> tuple[str, str, Optional[list[tuple[str, str, int, int, str]]], str]:
    """
    Resolve a single missing dependency from Oracle.
    Returns (resolved_owner, resolved_name, columns_or_None, resolution_type).
    resolution_type: 'table', 'view', 'synonym->table', 'synonym->view', 'stub'.
    """
    dep_schema_upper = dep_schema.upper() if dep_schema else ""
    dep_name_upper = dep_name.upper()

    # Step 1: Try direct lookup
    if dep_schema_upper:
        obj_type = get_oracle_object_type(ora_conn, dep_schema_upper, dep_name_upper)
        if obj_type in ("TABLE", "VIEW", "MATERIALIZED VIEW"):
            columns = get_oracle_columns(ora_conn, dep_schema_upper, dep_name_upper)
            if columns:
                res_type = "table" if obj_type == "TABLE" else "view"
                return dep_schema_upper, dep_name_upper, columns, res_type
            # Object exists but no columns (rare)
            return dep_schema_upper, dep_name_upper, None, "stub"

    # Step 2: Try synonym resolution
    resolved = resolve_oracle_synonym(ora_conn, dep_name_upper, dep_schema_upper)
    if resolved:
        real_owner, real_name = resolved
        obj_type = get_oracle_object_type(ora_conn, real_owner, real_name)
        if obj_type in ("TABLE", "VIEW", "MATERIALIZED VIEW"):
            columns = get_oracle_columns(ora_conn, real_owner, real_name)
            if columns:
                res_type = f"synonym->{obj_type.lower()}"
                return real_owner, real_name, columns, res_type

    # Step 3: Try common Oracle schemas (APPS, APPS2, HR, GL, AP, AR, etc.)
    if not dep_schema_upper:
        common_schemas = ["APPS", "APPS2", "PUBLIC"]
        for try_schema in common_schemas:
            obj_type = get_oracle_object_type(ora_conn, try_schema, dep_name_upper)
            if obj_type in ("TABLE", "VIEW", "MATERIALIZED VIEW"):
                columns = get_oracle_columns(ora_conn, try_schema, dep_name_upper)
                if columns:
                    res_type = f"found_in_{try_schema.lower()}"
                    return try_schema, dep_name_upper, columns, res_type

    # Step 4: Not found anywhere
    return dep_schema_upper or "public", dep_name_upper, None, "stub"


# ---------------------------------------------------------------------------
#  Phase 2b — Optional data copy
# ---------------------------------------------------------------------------

def copy_data_from_oracle(
    ora_conn,
    pg_conn,
    oracle_owner: str,
    oracle_name: str,
    pg_schema: str,
    pg_name: str,
    row_limit: int = 0,
    batch_size: int = 1000,
) -> tuple[bool, int, str]:
    """
    Copy rows from Oracle table/view to PostgreSQL table.
    Returns (success, rows_copied, error_message).
    """
    qualified_ora = f"{oracle_owner}.{oracle_name}"
    qualified_pg = f"{pg_schema}.{pg_name}" if pg_schema else pg_name

    try:
        ora_cur = ora_conn.cursor()
        limit_clause = f" WHERE ROWNUM <= {row_limit}" if row_limit > 0 else ""
        ora_cur.execute(f"SELECT * FROM {qualified_ora}{limit_clause}")

        col_names = [desc[0].lower() for desc in ora_cur.description]
        placeholders = ", ".join(["%s"] * len(col_names))
        col_list = ", ".join(col_names)
        insert_sql = f"INSERT INTO {qualified_pg} ({col_list}) VALUES ({placeholders})"

        rows_copied = 0
        pg_cur = pg_conn.cursor()

        while True:
            rows = ora_cur.fetchmany(batch_size)
            if not rows:
                break
            # Convert Oracle LOB objects to strings/bytes
            converted = []
            for row in rows:
                new_row = []
                for val in row:
                    if hasattr(val, "read"):
                        new_row.append(val.read())
                    else:
                        new_row.append(val)
                converted.append(tuple(new_row))

            pg_cur.executemany(insert_sql, converted)
            rows_copied += len(converted)

        pg_conn.commit()
        pg_cur.close()
        ora_cur.close()
        return True, rows_copied, ""
    except Exception as e:
        try:
            pg_conn.rollback()
        except Exception:
            pass
        return False, 0, str(e).strip()


# ---------------------------------------------------------------------------
#  Helpers — PostgreSQL execution
# ---------------------------------------------------------------------------

def ensure_pg_schema(pg_conn, schema: str) -> bool:
    """Create schema in PostgreSQL if it does not exist."""
    if not schema or not schema.strip():
        return True
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema.strip()}")
        pg_conn.commit()
        return True
    except Exception:
        pg_conn.rollback()
        return False


def execute_sql(pg_conn, sql: str) -> tuple[bool, str]:
    """Execute SQL on PostgreSQL. Returns (success, error_message)."""
    sql = sql.strip()
    if not sql:
        return False, "Empty SQL"
    try:
        with pg_conn.cursor() as cur:
            cur.execute(sql)
        pg_conn.commit()
        return True, ""
    except Exception as e:
        pg_conn.rollback()
        return False, str(e).strip()


def is_dependency_error(error_msg: str) -> bool:
    """Check if error is a dependency issue."""
    lower = (error_msg or "").lower()
    if "does not exist" in lower and ("relation" in lower or "schema" in lower or "type" in lower):
        return True
    return False


def extract_missing_relation(error_msg: str) -> Optional[str]:
    """Extract the missing relation name from a dependency error."""
    match = _RELATION_NOT_EXIST_PATTERN.search(error_msg or "")
    return match.group(1).lower() if match else None


# ---------------------------------------------------------------------------
#  Topological sort
# ---------------------------------------------------------------------------

def topological_sort_views(views: list[tuple[str, str, str, Path]]) -> list[int]:
    """Sort views so dependencies come first. Returns indices in order."""
    n = len(views)
    if n == 0:
        return []

    key_to_idx: dict[tuple[str, str], int] = {}
    name_to_idx: dict[str, list[int]] = {}
    for i, (schema, view_name, _, _) in enumerate(views):
        key = (schema.lower(), view_name.lower())
        key_to_idx[key] = i
        name_to_idx.setdefault(view_name.lower(), []).append(i)

    in_degree = [0] * n
    dependents: list[set[int]] = [set() for _ in range(n)]

    for i, (schema, view_name, sql, _) in enumerate(views):
        refs = extract_dependency_refs(sql)
        for (rs, rv) in refs:
            dep_idx = key_to_idx.get((rs, rv))
            if dep_idx is None:
                for idx in name_to_idx.get(rv, []):
                    if idx != i:
                        dep_idx = idx
                        break
            if dep_idx is not None and dep_idx != i:
                if i not in dependents[dep_idx]:
                    dependents[dep_idx].add(i)
                    in_degree[i] += 1

    queue = deque(i for i in range(n) if in_degree[i] == 0)
    order: list[int] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for dep in dependents[node]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)

    if len(order) < n:
        remaining = set(range(n)) - set(order)
        order.extend(sorted(remaining))
        log.warning("Circular dependency among %d view(s); order may be suboptimal.", len(remaining))

    return order


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Retry failed PostgreSQL views — resolve missing dependencies from Oracle, create stubs as fallback.\n\n"
            "Phases:\n"
            "  1. Load failed views + analyze what's missing in PostgreSQL\n"
            "  2. Resolve missing objects from Oracle (table structure -> CREATE TABLE in PG)\n"
            "  3. Create stubs for objects not found in Oracle\n"
            "  4. Multi-pass retry of failed views\n"
            "  5. Summary report"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input_dir", help="Directory with failed PostgreSQL CREATE VIEW SQL files")
    parser.add_argument("--pattern", default="*.sql", help="Glob pattern (default: *.sql)")
    parser.add_argument("--recursive", action="store_true", help="Search recursively")
    parser.add_argument("--success-dir", default="retry_resolved", help="Output: resolved views (default: retry_resolved)")
    parser.add_argument("--still-failed-dir", default="retry_still_failed", help="Output: still-failed views (default: retry_still_failed)")
    parser.add_argument("--max-passes", type=int, default=50, help="Max retry passes (default: 50)")
    parser.add_argument("--ensure-schema", action="store_true", help="CREATE SCHEMA IF NOT EXISTS before creating objects")
    parser.add_argument("--dry-run", action="store_true", help="Analyze and report only (no DB connections)")
    parser.add_argument("--drop-before-retry", action="store_true", help="DROP VIEW IF EXISTS before each retry attempt")

    # Oracle connection
    parser.add_argument("--oracle-user", default=None, help="Oracle user (or ORACLE_USER env)")
    parser.add_argument("--oracle-password", default=None, help="Oracle password (or ORACLE_PASSWORD env)")
    parser.add_argument("--oracle-dsn", default=None, help="Oracle DSN (or ORACLE_DSN env)")

    # PostgreSQL connection
    parser.add_argument("--pg-host", default=None, help="PostgreSQL host (or PG_HOST env)")
    parser.add_argument("--pg-port", type=int, default=None, help="PostgreSQL port (or PG_PORT env)")
    parser.add_argument("--pg-database", default=None, help="PostgreSQL database (or PG_DATABASE env)")
    parser.add_argument("--pg-user", default=None, help="PostgreSQL user (or PG_USER env)")
    parser.add_argument("--pg-password", default=None, help="PostgreSQL password (or PG_PASSWORD env)")

    # Data copy
    parser.add_argument("--with-data", action="store_true", help="Copy row data from Oracle for missing tables/views")
    parser.add_argument("--data-row-limit", type=int, default=0, help="Max rows to copy per table (0=all, default: 0)")

    # Logging
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    parser.add_argument("--log-file", default=None, metavar="PATH")

    args = parser.parse_args()

    # Configure logging
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    log.setLevel(log_level)
    if not log.handlers:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(fmt)
        log.addHandler(handler)
    if args.log_file:
        fh = logging.FileHandler(args.log_file, encoding="utf-8")
        fh.setFormatter(log.handlers[0].formatter)
        log.addHandler(fh)

    # ---- Resolve all credentials once (CLI > env var > module default) ----
    ora_user = args.oracle_user or ORACLE_USER or os.environ.get("ORACLE_USER")
    ora_password = args.oracle_password or ORACLE_PASSWORD or os.environ.get("ORACLE_PASSWORD")
    ora_dsn = args.oracle_dsn or os.environ.get("ORACLE_DSN", ORACLE_DSN)

    pg_host = args.pg_host or os.environ.get("PG_HOST", PG_HOST)
    pg_port = args.pg_port if args.pg_port is not None else int(os.environ.get("PG_PORT", str(PG_PORT)))
    pg_database = args.pg_database or os.environ.get("PG_DATABASE", PG_DATABASE)
    pg_user = args.pg_user or os.environ.get("PG_USER", PG_USER)
    pg_password = args.pg_password or os.environ.get("PG_PASSWORD", PG_PASSWORD)

    log.info("Oracle: user=%s dsn=%s", ora_user or "(not set)", ora_dsn)
    log.info("PostgreSQL: %s:%s/%s user=%s", pg_host, pg_port, pg_database, pg_user)

    # ===========================================================================
    #  PHASE 1: Load failed views & analyze dependencies
    # ===========================================================================
    print("=" * 65, flush=True)
    print("PHASE 1: Loading failed views and analyzing dependencies", flush=True)
    print("=" * 65, flush=True)

    try:
        views = load_sql_files(args.input_dir, pattern=args.pattern, recursive=args.recursive)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    if not views:
        print("No CREATE VIEW SQL files found.", flush=True)
        sys.exit(0)

    print(f"  Loaded {len(views)} failed view(s).", flush=True)

    # Gather all dependency refs
    all_deps: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for schema, view_name, sql, _ in views:
        refs = extract_dependency_refs(sql)
        all_deps[(schema, view_name)] = refs

    total_refs = sum(len(r) for r in all_deps.values())
    print(f"  Total dependency references: {total_refs}", flush=True)

    if args.dry_run:
        # Just show dependency analysis
        print("\n--- Dry Run: Dependency Analysis ---", flush=True)
        view_keys = {(s.lower(), v.lower()) for s, v, _, _ in views}
        for schema, view_name, sql, _ in views:
            display = f"{schema}.{view_name}" if schema else view_name
            refs = all_deps.get((schema, view_name), set())
            internal = [f"{s}.{v}" if s else v for s, v in refs if (s, v) in view_keys]
            external = [f"{s}.{v}" if s else v for s, v in refs if (s, v) not in view_keys]
            parts = []
            if internal:
                parts.append(f"internal deps: {', '.join(internal)}")
            if external:
                parts.append(f"external deps: {', '.join(external)}")
            info = "; ".join(parts) if parts else "no dependencies"
            print(f"  {display} — {info}", flush=True)
        print(f"\nDry run complete. {len(views)} view(s) analyzed.", flush=True)
        sys.exit(0)

    # ===========================================================================
    #  Connect to PostgreSQL
    # ===========================================================================
    print(f"\n  Connecting to PostgreSQL: {pg_host}:{pg_port}/{pg_database}...", flush=True)
    try:
        import psycopg2
        pg_conn = psycopg2.connect(host=pg_host, port=pg_port, database=pg_database,
                                   user=pg_user, password=pg_password)
        pg_conn.autocommit = False
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    # Query existing PG relations
    print("  Querying PostgreSQL catalog for existing relations...", flush=True)
    existing_pg = get_existing_pg_relations(pg_conn)
    print(f"  Found {len(existing_pg)} existing table(s)/view(s) in PostgreSQL.", flush=True)

    # Find what's missing
    missing_deps = find_missing_dependencies(views, existing_pg)
    print(f"  Missing dependencies: {len(missing_deps)} unique relation(s)", flush=True)

    if missing_deps and log.isEnabledFor(logging.DEBUG):
        for (ms, mn), needed_by in missing_deps.items():
            dep_display = f"{ms}.{mn}" if ms else mn
            log.debug("  Missing: %s (needed by %d view(s))", dep_display, len(needed_by))

    # ===========================================================================
    #  PHASE 2: Resolve missing dependencies from Oracle
    # ===========================================================================
    print(f"\n{'=' * 65}", flush=True)
    print("PHASE 2: Resolving missing dependencies from Oracle", flush=True)
    print("=" * 65, flush=True)

    ora_conn = None

    resolved_from_oracle: dict[tuple[str, str], tuple[str, str, str]] = {}  # dep_key -> (ddl, resolution_type, oracle_qualified)
    stubs: dict[tuple[str, str], str] = {}  # dep_key -> stub_ddl
    data_copy_targets: list[tuple[str, str, str, str, str, str]] = []  # (ora_owner, ora_name, pg_schema, pg_name, dep_key_s, dep_key_n)

    if missing_deps and ora_user and ora_password:
        print(f"  Connecting to Oracle: {ora_dsn}...", flush=True)
        try:
            ora_conn = oracle_connect(ora_user, ora_password, ora_dsn)
            print("  Oracle connection established.", flush=True)
        except Exception as e:
            print(f"  Warning: Oracle connection failed ({e}). Will use stubs for all missing deps.", flush=True)

    if ora_conn and missing_deps:
        sorted_deps = sorted(missing_deps.keys())
        pad = max(3, len(str(len(sorted_deps))))
        ora_resolved = 0
        ora_stub = 0

        for i, (dep_s, dep_n) in enumerate(sorted_deps):
            dep_display = f"{dep_s}.{dep_n}" if dep_s else dep_n
            needed_by = missing_deps[(dep_s, dep_n)]

            real_owner, real_name, columns, res_type = resolve_single_dependency(ora_conn, dep_s, dep_n)
            pg_schema = dep_s.lower() if dep_s else "public"
            pg_name = dep_n.lower()

            if columns:
                ddl = generate_create_table_ddl(pg_schema, pg_name, columns)
                ora_qualified = f"{real_owner}.{real_name}"
                resolved_from_oracle[(dep_s, dep_n)] = (ddl, res_type, ora_qualified)
                ora_resolved += 1
                print(f"  [{i+1:>{pad}}/{len(sorted_deps)}] RESOLVED: {dep_display} "
                      f"({res_type}, {len(columns)} col(s), from {ora_qualified}) "
                      f"— needed by {len(needed_by)} view(s)", flush=True)

                if args.with_data:
                    data_copy_targets.append((real_owner, real_name, pg_schema, pg_name, dep_s, dep_n))
            else:
                stub_ddl = generate_stub_ddl(pg_schema, pg_name)
                stubs[(dep_s, dep_n)] = stub_ddl
                ora_stub += 1
                print(f"  [{i+1:>{pad}}/{len(sorted_deps)}] STUB: {dep_display} "
                      f"(not found in Oracle) — needed by {len(needed_by)} view(s)", flush=True)

        print(f"\n  Oracle resolution: {ora_resolved} resolved, {ora_stub} stub(s)", flush=True)
    elif missing_deps:
        if not ora_user or not ora_password:
            print("  No Oracle credentials provided. Creating stubs for all missing dependencies.", flush=True)
        for dep_s, dep_n in sorted(missing_deps.keys()):
            pg_schema = dep_s.lower() if dep_s else "public"
            pg_name = dep_n.lower()
            stubs[(dep_s, dep_n)] = generate_stub_ddl(pg_schema, pg_name)
        print(f"  Created {len(stubs)} stub DDL(s).", flush=True)
    else:
        print("  No missing dependencies to resolve!", flush=True)

    # ===========================================================================
    #  PHASE 3: Create resolved objects + stubs in PostgreSQL
    # ===========================================================================
    print(f"\n{'=' * 65}", flush=True)
    print("PHASE 3: Creating missing objects in PostgreSQL", flush=True)
    print("=" * 65, flush=True)

    # Ensure schemas
    if args.ensure_schema:
        schemas_needed: set[str] = set()
        for schema, _, _, _ in views:
            if schema and schema.strip():
                schemas_needed.add(schema.strip().lower())
        for (dep_s, _) in list(resolved_from_oracle.keys()) + list(stubs.keys()):
            if dep_s and dep_s.strip():
                schemas_needed.add(dep_s.strip().lower())
        for s in sorted(schemas_needed):
            ensure_pg_schema(pg_conn, s)
        if schemas_needed:
            print(f"  Ensured {len(schemas_needed)} schema(s): {', '.join(sorted(schemas_needed))}", flush=True)

    created_count = 0
    create_failed_count = 0

    # Create objects resolved from Oracle
    for (dep_s, dep_n), (ddl, res_type, ora_qual) in sorted(resolved_from_oracle.items()):
        dep_display = f"{dep_s}.{dep_n}" if dep_s else dep_n
        ok, err = execute_sql(pg_conn, ddl)
        if ok:
            created_count += 1
            log.info("Created %s (from Oracle %s, type=%s)", dep_display, ora_qual, res_type)
        else:
            create_failed_count += 1
            log.warning("Failed to create %s: %s", dep_display, err[:200])
            print(f"  Warning: failed to create {dep_display}: {err[:120]}", flush=True)

    # Create stubs
    for (dep_s, dep_n), stub_ddl in sorted(stubs.items()):
        dep_display = f"{dep_s}.{dep_n}" if dep_s else dep_n
        ok, err = execute_sql(pg_conn, stub_ddl)
        if ok:
            created_count += 1
            log.info("Created stub for %s", dep_display)
        else:
            create_failed_count += 1
            log.warning("Failed to create stub %s: %s", dep_display, err[:200])
            print(f"  Warning: failed to create stub {dep_display}: {err[:120]}", flush=True)

    print(f"  Created {created_count} object(s) in PostgreSQL ({create_failed_count} failed).", flush=True)

    # Optional data copy
    if args.with_data and data_copy_targets and ora_conn:
        print(f"\n  Copying data for {len(data_copy_targets)} table(s) from Oracle...", flush=True)
        total_rows = 0
        for ora_owner, ora_name, pg_schema, pg_name, _, _ in data_copy_targets:
            qualified = f"{pg_schema}.{pg_name}"
            ok, rows, err = copy_data_from_oracle(
                ora_conn, pg_conn, ora_owner, ora_name, pg_schema, pg_name,
                row_limit=args.data_row_limit,
            )
            if ok:
                total_rows += rows
                print(f"    {qualified}: {rows} row(s) copied", flush=True)
            else:
                print(f"    {qualified}: data copy failed — {err[:120]}", flush=True)
        print(f"  Data copy complete: {total_rows} total row(s).", flush=True)

    # ===========================================================================
    #  PHASE 4: Multi-pass retry of failed views
    # ===========================================================================
    print(f"\n{'=' * 65}", flush=True)
    print("PHASE 4: Multi-pass retry of failed views", flush=True)
    print("=" * 65, flush=True)

    dep_order = topological_sort_views(views)
    pending = set(range(len(views)))
    resolved_views: dict[int, int] = {}  # index -> pass number
    last_errors: dict[int, str] = {}
    total_passes = 0

    t_start = time.perf_counter()

    for pass_num in range(1, args.max_passes + 1):
        total_passes = pass_num
        resolved_this_pass = 0
        ordered_pending = [i for i in dep_order if i in pending]

        for idx in ordered_pending:
            schema, view_name, sql, fpath = views[idx]
            display = f"{schema}.{view_name}" if schema else view_name
            qualified = f"{schema}.{view_name}" if schema else view_name

            if args.drop_before_retry:
                try:
                    with pg_conn.cursor() as cur:
                        cur.execute(f"DROP VIEW IF EXISTS {qualified} CASCADE")
                    pg_conn.commit()
                except Exception:
                    pg_conn.rollback()

            ok, err = execute_sql(pg_conn, sql)
            if ok:
                resolved_views[idx] = pass_num
                pending.discard(idx)
                resolved_this_pass += 1
                log.debug("Pass %d OK: %s", pass_num, display)
            else:
                last_errors[idx] = err
                log.debug("Pass %d FAIL: %s — %s", pass_num, display, err[:120])

        remaining = len(pending)
        print(f"  Pass {pass_num}: resolved {resolved_this_pass}, remaining {remaining}", flush=True)

        if resolved_this_pass == 0:
            print(f"  No progress — stopping.", flush=True)
            break
        if not pending:
            print("  All views resolved!", flush=True)
            break

    total_elapsed = time.perf_counter() - t_start

    # ===========================================================================
    #  PHASE 5: Write output & report
    # ===========================================================================
    print(f"\n{'=' * 65}", flush=True)
    print("PHASE 5: Writing results", flush=True)
    print("=" * 65, flush=True)

    success_dir = Path(args.success_dir)
    still_failed_dir = Path(args.still_failed_dir)
    success_dir.mkdir(parents=True, exist_ok=True)
    still_failed_dir.mkdir(parents=True, exist_ok=True)

    n_resolved = len(resolved_views)
    n_still_failed = len(pending)

    # Write resolved
    res_pad = max(3, len(str(max(n_resolved, 1))))
    for rank, (idx, pass_num) in enumerate(sorted(resolved_views.items())):
        schema, view_name, sql, fpath = views[idx]
        safe_name = f"{schema}_{view_name}".replace(".", "_") if schema else view_name.replace(".", "_")
        safe_name = re.sub(r'[<>:"/\\|?*]', "_", safe_name)
        display = f"{schema}.{view_name}" if schema else view_name
        out_path = success_dir / f"{(rank + 1):0{res_pad}d}_{safe_name}.sql"
        header = f"-- Resolved in pass {pass_num}: {display}\n-- Original file: {fpath.name}\n\n"
        out_path.write_text(header + sql + "\n", encoding="utf-8")

    # Write still-failed
    fail_pad = max(3, len(str(max(n_still_failed, 1))))
    dep_failed = 0
    other_failed = 0
    for rank, idx in enumerate(sorted(pending)):
        schema, view_name, sql, fpath = views[idx]
        safe_name = f"{schema}_{view_name}".replace(".", "_") if schema else view_name.replace(".", "_")
        safe_name = re.sub(r'[<>:"/\\|?*]', "_", safe_name)
        display = f"{schema}.{view_name}" if schema else view_name
        err = last_errors.get(idx, "unknown error")
        is_dep = is_dependency_error(err)
        err_type = "dependency" if is_dep else "other"
        if is_dep:
            dep_failed += 1
        else:
            other_failed += 1
        missing = extract_missing_relation(err)
        missing_info = f"\n-- Missing relation: {missing}" if missing else ""
        header = (
            f"-- STILL FAILED: {display}\n"
            f"-- Error type: {err_type}\n"
            f"-- Error: {err[:500]}{missing_info}\n"
            f"-- Original file: {fpath.name}\n\n"
        )
        out_path = still_failed_dir / f"{(rank + 1):0{fail_pad}d}_{safe_name}.sql"
        out_path.write_text(header + sql + "\n", encoding="utf-8")

    # Error log
    if pending:
        error_log_path = still_failed_dir / "error_log.txt"
        lines = ["view\terror_type\terror_message\tmissing_relation\n"]
        for idx in sorted(pending):
            schema, view_name, _, _ = views[idx]
            display = f"{schema}.{view_name}" if schema else view_name
            err = last_errors.get(idx, "unknown")
            err_type = "dependency" if is_dependency_error(err) else "other"
            missing = extract_missing_relation(err) or ""
            lines.append(f"{display}\t{err_type}\t{err[:500]}\t{missing}\n")
        error_log_path.write_text("".join(lines), encoding="utf-8")

    # Summary report
    report_path = Path(args.input_dir).parent / "retry_report.txt"
    r = [
        "=" * 72,
        "RETRY FAILED VIEWS — SUMMARY REPORT",
        "=" * 72,
        f"Input directory:          {args.input_dir}",
        f"Total views loaded:       {len(views)}",
        f"Missing dependencies:     {len(missing_deps)}",
        f"  Resolved from Oracle:   {len(resolved_from_oracle)}",
        f"  Stubs created:          {len(stubs)}",
        f"  Objects created in PG:  {created_count} ({create_failed_count} failed)",
        "",
        f"Retry passes:             {total_passes}",
        f"Elapsed:                  {total_elapsed:.1f}s",
        "",
        f"Views resolved:           {n_resolved}",
        f"Views still failed:       {n_still_failed}",
        f"  - Dependency errors:    {dep_failed}",
        f"  - Other errors:         {other_failed}",
        "",
        f"Output — resolved:        {success_dir}/",
        f"Output — still-failed:    {still_failed_dir}/",
        "",
    ]

    if resolved_from_oracle:
        r.append("--- Dependencies Resolved from Oracle ---")
        for (ds, dn), (_, res_type, ora_qual) in sorted(resolved_from_oracle.items()):
            dep_display = f"{ds}.{dn}" if ds else dn
            needed_count = len(missing_deps.get((ds, dn), set()))
            r.append(f"  {dep_display} <- {ora_qual} ({res_type}) — unblocks {needed_count} view(s)")
        r.append("")

    if stubs:
        r.append("--- Dependencies Created as Stubs ---")
        for (ds, dn) in sorted(stubs.keys()):
            dep_display = f"{ds}.{dn}" if ds else dn
            needed_count = len(missing_deps.get((ds, dn), set()))
            r.append(f"  {dep_display} (stub) — unblocks {needed_count} view(s)")
        r.append("")

    if resolved_views:
        r.append("--- Resolved Views (by pass) ---")
        for pass_num in sorted(set(resolved_views.values())):
            v_in_pass = [(idx, views[idx]) for idx in resolved_views if resolved_views[idx] == pass_num]
            r.append(f"  Pass {pass_num}: {len(v_in_pass)} view(s)")
            for _, (schema, view_name, _, _) in v_in_pass:
                display = f"{schema}.{view_name}" if schema else view_name
                r.append(f"    - {display}")
        r.append("")

    if pending:
        r.append("--- Still Failed Views ---")
        for idx in sorted(pending):
            schema, view_name, _, _ = views[idx]
            display = f"{schema}.{view_name}" if schema else view_name
            err = last_errors.get(idx, "unknown")
            err_type = "dependency" if is_dependency_error(err) else "other"
            missing = extract_missing_relation(err)
            suffix = f" [missing: {missing}]" if missing else ""
            r.append(f"  [{err_type}] {display}{suffix}")
            r.append(f"           {err[:200]}")
        r.append("")

    r.append("=" * 72)
    report_path.write_text("\n".join(r) + "\n", encoding="utf-8")

    # Console summary
    print(f"\n{'=' * 65}", flush=True)
    print(f"Done in {total_passes} pass(es), {total_elapsed:.1f}s.", flush=True)
    print(f"  Dependencies:  {len(missing_deps)} missing", flush=True)
    if resolved_from_oracle:
        print(f"    - {len(resolved_from_oracle)} resolved from Oracle (table structure created in PG)", flush=True)
    if stubs:
        print(f"    - {len(stubs)} created as stubs (not found in Oracle)", flush=True)
    print(f"  Views resolved:     {n_resolved} -> {success_dir}/", flush=True)
    print(f"  Views still failed: {n_still_failed} -> {still_failed_dir}/", flush=True)
    if dep_failed:
        print(f"    - {dep_failed} dependency error(s)", flush=True)
    if other_failed:
        print(f"    - {other_failed} non-dependency error(s)", flush=True)
    print(f"  Report: {report_path}", flush=True)
    print("=" * 65, flush=True)

    # Cleanup
    if ora_conn:
        try:
            ora_conn.close()
        except Exception:
            pass
    try:
        pg_conn.close()
    except Exception:
        pass

    if n_still_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

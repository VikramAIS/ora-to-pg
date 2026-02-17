#!/usr/bin/env python3
"""
Oracle-to-PostgreSQL Full DDL Migrator
======================================

Automatically discovers and migrates DDL objects from an Oracle database to PostgreSQL.

Migration order (dependency-aware):
  Phase  1: Schemas           — creates matching schemas in PG
  Phase  2: Sequences         — Oracle sequences → PG sequences
  Phase  3: User-Defined Types— Oracle OBJECT/COLLECTION/VARRAY → PG composite types
  Phase  4: Tables            — columns, types, PK, UK, CHECK, defaults; FKs in 2nd pass
  Phase  5: Partitions        — annotation for manual conversion to PG native partitioning
  Phase  6: Indexes           — B-tree, bitmap→GIN, function-based → expression indexes
  Phase  7: Synonyms          — Oracle synonyms → PostgreSQL views
  Phase  8: Functions/Procs   — Oracle PL/SQL → PG PL/pgSQL (best-effort + stubs)
  Phase  9: Packages          — Oracle packages → PG schema + functions
  Phase 10: Views             — full Oracle→PG transpilation with synonym resolution
  Phase 11: Materialized Views— Oracle MVs → PG materialized views
  Phase 12: Triggers          — Oracle triggers → PG trigger functions + triggers
  Phase 13: Grants            — object-level privileges

Input: Oracle connection details + PostgreSQL connection details (CLI args or env vars).
No input files required — the script discovers objects directly from Oracle.

All Oracle→PG SQL conversion rules are included inline (from oracle_sql_dir_to_postgres_synonyms.py):
  - ROWID→ctid, NVL/NVL2/DECODE→CASE/COALESCE, TO_*/TRUNC/USERENV/SYSDATE
  - (+) outer join removal, DUAL replacement, START WITH/CONNECT BY→recursive CTE
  - Date arithmetic, empty-string-as-NULL, identifier lowercasing, alias deduplication
  - Package namespace fixes, sequence refs, LISTAGG→string_agg, REGEXP_*, etc.

Requirements:
  pip install oracledb sqlglot psycopg2-binary

Usage:
  python oracle_to_pg_ddl_migrator.py \\
      --oracle-user SYSTEM --oracle-password pass --oracle-dsn localhost:1521/ORCL \\
      --pg-host localhost --pg-port 5432 --pg-database mydb --pg-user postgres --pg-password pass

  # Migrate only specific schemas:
  python oracle_to_pg_ddl_migrator.py ... --schemas APPS,APPS2,HR

  # Dry-run (no PG execution):
  python oracle_to_pg_ddl_migrator.py ... --no-execute

  # Skip specific object types:
  python oracle_to_pg_ddl_migrator.py ... --skip-triggers --skip-grants --skip-partitions
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import re
import sys
import textwrap
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# =============================================================================
# Module-level logger — configured in main()
# =============================================================================
log = logging.getLogger("oracle_to_pg_migrator")

# =============================================================================
# Default connection config (overridden by CLI / env vars)
# =============================================================================
ORACLE_USER: Optional[str] = None
ORACLE_PASSWORD: Optional[str] = None
ORACLE_DSN: str = "localhost:1521/ORCL"

PG_HOST: str = "localhost"
PG_PORT: int = 5432
PG_DATABASE: str = "postgres"
PG_USER: str = "postgres"
PG_PASSWORD: str = ""

# =============================================================================
# Known Oracle EBS / Noetix package names
# =============================================================================
ORACLE_EBS_PACKAGES = frozenset({
    "ap_auto_payment_pkg", "ap_invoices_pkg", "ap_utilities_pkg", "arp_addr_pkg",
    "budget_inquiry_pkg", "ce_auto_bank_match", "cs_incidents_pkg", "cs_std",
    "czdvcons", "fnd_attachment_util_pkg", "gl_alloc_batches_pkg",
    "glr03300_pkg", "hr_chkfmt",
    "hr_general", "hr_gbbal", "mrp_launch_plan_pk", "oe_query", "pa_billing",
    "pa_budget_upgrade_pkg", "pa_get_resource", "pa_security", "pa_status",
    "pa_task_utils", "pa_utils", "po_vendors_ap_pkg", "shp_picking_headers_pkg",
})

# =============================================================================
# PostgreSQL reserved keywords
# =============================================================================
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

# Oracle→PG date format mapping
ORACLE_TO_PG_DATE_FORMATS = {
    "YYYY": "YYYY", "YY": "YY", "RRRR": "YYYY", "RR": "YY",
    "MM": "MM", "MON": "Mon", "MONTH": "Month", "DD": "DD",
    "DY": "Dy", "DAY": "Day", "HH": "HH12", "HH12": "HH12", "HH24": "HH24",
    "MI": "MI", "SS": "SS", "SSSSS": "SSSS", "AM": "AM", "PM": "PM",
    "A.M.": "AM", "P.M.": "PM", "FF": "US", "FF3": "MS", "FF6": "US",
}

# Type alias
ViewKey = Tuple[str, str]

# =============================================================================
# Pre-compiled patterns (performance for large batches)
# =============================================================================
_to_func_pattern_cache: Dict[str, re.Pattern] = {}

_REWRITE_VIEW_NAME_PATTERN = re.compile(
    r"(CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+)(.+?)(\s+(?:\([^)]*\)\s+)?AS\s+)",
    re.IGNORECASE | re.DOTALL,
)
_NORMALIZE_DROP_VIEW_PATTERN = re.compile(
    r"^\s*DROP\s+VIEW\s+IF\s+EXISTS\s+[^;]+;\s*", re.IGNORECASE
)
_NORMALIZE_CREATE_VIEW_PATTERN = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+(.+?)\s+(?:\([^)]*\)\s+)?AS\s+(.*)",
    re.IGNORECASE | re.DOTALL,
)
_CREATE_VIEW_PATTERN = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:FORCE\s+)?VIEW\s+",
    re.IGNORECASE,
)
_CREATE_OR_REPLACE_VIEW_AS_PATTERN = re.compile(
    r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\S+)\s+AS", re.IGNORECASE
)
_BODY_FROM_DDL_PATTERN = re.compile(
    r"\bAS\s+(.*)\s*;?\s*$", re.IGNORECASE | re.DOTALL
)
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
_QUALIFY_FROM_JOIN_PATTERN = re.compile(
    r"(\b(?:FROM|(?:NATURAL\s+)?(?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+(?:OUTER\s+)?JOIN)\s+)([a-zA-Z_][a-zA-Z0-9_]*)\b",
    flags=re.IGNORECASE,
)
_TRUNC_DATE_INDICATORS = re.compile(
    r"\b(?:SYSDATE|CURRENT_DATE|CURRENT_TIMESTAMP|SYSTIMESTAMP|"
    r"pg_to_date__|pg_to_timestamp__|TO_DATE|TO_TIMESTAMP|ADD_MONTHS|"
    r"LAST_DAY|NEXT_DAY|MONTHS_BETWEEN|DATE_TRUNC|date_trunc)\b",
    re.IGNORECASE,
)
_TRUNC_DATE_COLUMN_SUFFIX = re.compile(
    r"(?:_date|_time|_timestamp|_dt|_ts)\s*$", re.IGNORECASE,
)
_EMPTY_STR_PLACEHOLDER = "__ORACLE_EMPTY_PH__"


# #############################################################################
#  SECTION 1: Oracle Connection Helpers
# #############################################################################

def oracle_connect_with_retry(user: str, password: str, dsn: str,
                              max_attempts: int = 3, delay_sec: float = 2):
    """Open Oracle connection with retries."""
    import oracledb
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            log.debug("Oracle connection opened (attempt %d)", attempt)
            return conn
        except Exception as e:
            last_err = e
            log.warning("Oracle connect attempt %d/%d failed: %s", attempt, max_attempts, e)
            if attempt < max_attempts:
                time.sleep(delay_sec)
    raise last_err


def pg_connect(host: str, port: int, database: str, user: str, password: str):
    """Open PostgreSQL connection."""
    import psycopg2
    log.debug("Connecting to PostgreSQL %s:%d/%s as %s", host, port, database, user)
    conn = psycopg2.connect(
        host=host, port=port, dbname=database, user=user, password=password,
        options="-c statement_timeout=120000 -c lock_timeout=30000",
    )
    log.debug("PostgreSQL connection established")
    return conn


# #############################################################################
#  SECTION 2: Oracle Discovery Functions
# #############################################################################

def discover_schemas(conn, schema_filter: Optional[List[str]] = None) -> List[str]:
    """Discover schemas from Oracle that have objects (views/tables/functions/packages)."""
    log.info("[DISCOVER] Discovering schemas...")
    cursor = conn.cursor()
    try:
        if schema_filter:
            placeholders = ",".join(f":{i}" for i in range(len(schema_filter)))
            params = {str(i): s.upper() for i, s in enumerate(schema_filter)}
            cursor.execute(f"""
                SELECT DISTINCT owner FROM all_objects
                WHERE owner IN ({placeholders})
                  AND object_type IN ('VIEW','TABLE','FUNCTION','PROCEDURE','PACKAGE','PACKAGE BODY','SYNONYM')
                ORDER BY owner
            """, params)
        else:
            cursor.execute("""
                SELECT DISTINCT owner FROM all_objects
                WHERE object_type IN ('VIEW','TABLE','FUNCTION','PROCEDURE','PACKAGE','PACKAGE BODY')
                  AND owner NOT IN ('SYS','SYSTEM','OUTLN','DBSNMP','WMSYS','EXFSYS','CTXSYS',
                                    'XDB','MDSYS','ORDSYS','ORDDATA','ORDPLUGINS','SI_INFORMTN_SCHEMA',
                                    'OLAPSYS','OWBSYS','OWBSYS_AUDIT','GSMADMIN_INTERNAL','APPQOSSYS',
                                    'ANONYMOUS','XS$NULL','OJVMSYS','DIP','ORACLE_OCM','LBACSYS',
                                    'DVSYS','DVF','AUDSYS','DBSFWUSER','REMOTE_SCHEDULER_AGENT',
                                    'SYSBACKUP','SYSDG','SYSKM','SYSRAC','SYS$UMF','GGSYS',
                                    'GSMCATUSER','GSMROOTUSER','GSMUSER','MDDATA','SPATIAL_CSW_ADMIN_USR',
                                    'SPATIAL_WFS_ADMIN_USR')
                ORDER BY owner
            """)
        schemas = [row[0] for row in cursor.fetchall()]
        log.info("[DISCOVER] Found %d schema(s): %s", len(schemas), ", ".join(schemas[:20]))
        return schemas
    finally:
        cursor.close()


def discover_tables(conn, schemas: List[str]) -> List[Dict]:
    """Discover tables with columns, constraints, and defaults.
    Returns list of dicts:
      {
        'owner': str, 'table_name': str,
        'columns': [(col_name, data_type, data_length, data_precision, data_scale, nullable, default_val), ...],
        'primary_key': {'name': str, 'columns': [str, ...]},
        'unique_keys': [{'name': str, 'columns': [str, ...]}, ...],
        'check_constraints': [{'name': str, 'condition': str}, ...],
        'foreign_keys': [{'name': str, 'columns': [str, ...], 'ref_owner': str, 'ref_table': str, 'ref_columns': [str, ...]}, ...],
      }
    """
    log.info("[DISCOVER] Discovering tables for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results: List[Dict] = []
    try:
        for schema in schemas:
            schema_upper = schema.upper()
            # Discover table names
            cursor.execute(
                "SELECT table_name FROM all_tables WHERE owner = :o AND temporary = 'N' "
                "AND table_name NOT LIKE 'BIN$%%' AND table_name NOT LIKE 'SYS_%%' "
                "ORDER BY table_name",
                {"o": schema_upper},
            )
            table_names = [r[0] for r in cursor.fetchall()]
            log.info("[DISCOVER]   Schema %s: %d table(s)", schema, len(table_names))

            for tbl_name in table_names:
                tbl_info: Dict = {
                    "owner": schema, "table_name": tbl_name,
                    "columns": [], "primary_key": None,
                    "unique_keys": [], "check_constraints": [], "foreign_keys": [],
                }

                # Columns
                cursor.execute("""
                    SELECT column_name, data_type, data_length, data_precision, data_scale,
                           nullable, data_default
                    FROM all_tab_columns
                    WHERE owner = :o AND table_name = :t
                    ORDER BY column_id
                """, {"o": schema_upper, "t": tbl_name})
                for row in cursor.fetchall():
                    col_name = row[0]
                    data_type = row[1]
                    data_length = row[2]
                    data_precision = row[3]
                    data_scale = row[4]
                    nullable = row[5]
                    data_default = row[6]
                    if data_default and hasattr(data_default, "read"):
                        data_default = data_default.read()
                    if data_default:
                        data_default = data_default.strip()
                    tbl_info["columns"].append(
                        (col_name, data_type, data_length, data_precision, data_scale, nullable, data_default)
                    )

                # Constraints: PK, UNIQUE, CHECK, FK
                cursor.execute("""
                    SELECT c.constraint_name, c.constraint_type, c.search_condition,
                           c.r_owner, c.r_constraint_name
                    FROM all_constraints c
                    WHERE c.owner = :o AND c.table_name = :t
                      AND c.constraint_type IN ('P','U','C','R')
                      AND c.status = 'ENABLED'
                    ORDER BY c.constraint_type, c.constraint_name
                """, {"o": schema_upper, "t": tbl_name})
                constraints_raw = cursor.fetchall()

                for con_name, con_type, search_cond, r_owner, r_con_name in constraints_raw:
                    # Get columns for this constraint
                    cursor.execute("""
                        SELECT column_name FROM all_cons_columns
                        WHERE owner = :o AND constraint_name = :c
                        ORDER BY position
                    """, {"o": schema_upper, "c": con_name})
                    con_cols = [r[0] for r in cursor.fetchall()]

                    if con_type == "P":
                        tbl_info["primary_key"] = {"name": con_name, "columns": con_cols}
                    elif con_type == "U":
                        tbl_info["unique_keys"].append({"name": con_name, "columns": con_cols})
                    elif con_type == "C":
                        cond = search_cond
                        if cond and hasattr(cond, "read"):
                            cond = cond.read()
                        if cond:
                            cond = cond.strip()
                            # Skip system-generated NOT NULL constraints
                            if re.match(r'^"?\w+"?\s+IS\s+NOT\s+NULL$', cond, re.IGNORECASE):
                                continue
                        tbl_info["check_constraints"].append({"name": con_name, "condition": cond or ""})
                    elif con_type == "R" and r_owner and r_con_name:
                        # FK: find referenced table and columns
                        cursor.execute("""
                            SELECT table_name FROM all_constraints
                            WHERE owner = :o AND constraint_name = :c
                        """, {"o": r_owner, "c": r_con_name})
                        ref_row = cursor.fetchone()
                        ref_table = ref_row[0] if ref_row else ""
                        cursor.execute("""
                            SELECT column_name FROM all_cons_columns
                            WHERE owner = :o AND constraint_name = :c
                            ORDER BY position
                        """, {"o": r_owner, "c": r_con_name})
                        ref_cols = [r[0] for r in cursor.fetchall()]
                        tbl_info["foreign_keys"].append({
                            "name": con_name, "columns": con_cols,
                            "ref_owner": r_owner, "ref_table": ref_table, "ref_columns": ref_cols,
                        })

                results.append(tbl_info)
        log.info("[DISCOVER] Found %d table(s) total", len(results))
    finally:
        cursor.close()
    return results


def discover_synonyms(conn, schemas: Optional[List[str]] = None) -> List[Tuple[str, str, str, str]]:
    """Discover synonyms. Returns [(owner, synonym_name, table_owner, table_name), ...]."""
    log.info("[DISCOVER] Discovering synonyms...")
    cursor = conn.cursor()
    try:
        try:
            if schemas:
                placeholders = ",".join(f":{i}" for i in range(len(schemas)))
                params = {str(i): s.upper() for i, s in enumerate(schemas)}
                cursor.execute(f"""
                    SELECT owner, synonym_name, table_owner, table_name
                    FROM dba_synonyms
                    WHERE (owner IN ({placeholders}) OR owner = 'PUBLIC')
                      AND table_owner IS NOT NULL AND table_name IS NOT NULL
                """, params)
            else:
                cursor.execute("""
                    SELECT owner, synonym_name, table_owner, table_name
                    FROM dba_synonyms
                    WHERE table_owner IS NOT NULL AND table_name IS NOT NULL
                """)
        except Exception:
            log.debug("[DISCOVER] dba_synonyms not accessible, falling back to all_synonyms")
            cursor.execute("""
                SELECT owner, synonym_name, table_owner, table_name
                FROM all_synonyms
                WHERE table_owner IS NOT NULL AND table_name IS NOT NULL
            """)
        rows = cursor.fetchall()
        log.info("[DISCOVER] Found %d synonym(s)", len(rows))
        return [(r[0], r[1], r[2], r[3]) for r in rows]
    finally:
        cursor.close()


def get_synonym_map(connection) -> Dict[str, str]:
    """Build synonym_name -> table_owner.table_name mapping."""
    log.info("[SYNONYMS] Building synonym map...")
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
    synonym_map: Dict[str, str] = {}
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
    log.info("[SYNONYMS] Built %d synonym mappings", len(synonym_map))
    return synonym_map


def discover_views(conn, schemas: List[str]) -> List[Tuple[str, str, str]]:
    """Discover views. Returns [(owner, view_name, text), ...]."""
    log.info("[DISCOVER] Discovering views for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results = []
    try:
        for schema in schemas:
            cursor.execute(
                "SELECT view_name, text FROM all_views WHERE owner = :o ORDER BY view_name",
                {"o": schema.upper()},
            )
            for row in cursor.fetchall():
                vname = row[0]
                text = row[1]
                if text and hasattr(text, "read"):
                    text = text.read()
                if text:
                    text = text.rstrip().rstrip(";").rstrip()
                    ddl = f"CREATE OR REPLACE VIEW {schema}.{vname} AS\n{text}"
                    results.append((schema, vname, ddl))
        log.info("[DISCOVER] Found %d view(s) total", len(results))
    finally:
        cursor.close()
    return results


def discover_functions(conn, schemas: List[str]) -> List[Tuple[str, str, str, str]]:
    """Discover standalone functions/procedures. Returns [(owner, name, type, source), ...]."""
    log.info("[DISCOVER] Discovering functions/procedures for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results = []
    try:
        for schema in schemas:
            cursor.execute("""
                SELECT object_name, object_type
                FROM all_objects
                WHERE owner = :o AND object_type IN ('FUNCTION','PROCEDURE')
                ORDER BY object_name
            """, {"o": schema.upper()})
            objects = cursor.fetchall()
            for obj_name, obj_type in objects:
                cursor.execute("""
                    SELECT text FROM all_source
                    WHERE owner = :o AND name = :n AND type = :t
                    ORDER BY line
                """, {"o": schema.upper(), "n": obj_name, "t": obj_type})
                lines = [r[0] for r in cursor.fetchall()]
                if lines:
                    source = "".join(lines)
                    results.append((schema, obj_name, obj_type, source))
        log.info("[DISCOVER] Found %d function(s)/procedure(s)", len(results))
    finally:
        cursor.close()
    return results


def discover_packages(conn, schemas: List[str]) -> List[Tuple[str, str, str, str]]:
    """Discover packages. Returns [(owner, package_name, spec_source, body_source), ...]."""
    log.info("[DISCOVER] Discovering packages for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results = []
    try:
        for schema in schemas:
            cursor.execute("""
                SELECT DISTINCT object_name
                FROM all_objects
                WHERE owner = :o AND object_type = 'PACKAGE'
                ORDER BY object_name
            """, {"o": schema.upper()})
            pkg_names = [r[0] for r in cursor.fetchall()]
            for pkg_name in pkg_names:
                cursor.execute("""
                    SELECT text FROM all_source
                    WHERE owner = :o AND name = :n AND type = 'PACKAGE'
                    ORDER BY line
                """, {"o": schema.upper(), "n": pkg_name})
                spec_lines = [r[0] for r in cursor.fetchall()]
                spec_source = "".join(spec_lines) if spec_lines else ""

                cursor.execute("""
                    SELECT text FROM all_source
                    WHERE owner = :o AND name = :n AND type = 'PACKAGE BODY'
                    ORDER BY line
                """, {"o": schema.upper(), "n": pkg_name})
                body_lines = [r[0] for r in cursor.fetchall()]
                body_source = "".join(body_lines) if body_lines else ""
                results.append((schema, pkg_name, spec_source, body_source))
        log.info("[DISCOVER] Found %d package(s)", len(results))
    finally:
        cursor.close()
    return results


def get_oracle_schema_objects(connection, owner: str) -> Dict[str, str]:
    """Return dict of object_name_upper -> 'VIEW'|'TABLE' for all VIEW/TABLE in given schema."""
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT object_name, object_type FROM all_objects WHERE owner = :owner AND object_type IN ('VIEW', 'TABLE')",
            {"owner": owner.upper()},
        )
        return {row[0].upper(): row[1] for row in cursor.fetchall()}
    finally:
        cursor.close()


def discover_sequences(conn, schemas: List[str]) -> List[Dict]:
    """Discover sequences. Returns list of dicts with sequence properties."""
    log.info("[DISCOVER] Discovering sequences for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results: List[Dict] = []
    try:
        for schema in schemas:
            cursor.execute("""
                SELECT sequence_name, min_value, max_value, increment_by,
                       cycle_flag, cache_size, last_number, order_flag
                FROM all_sequences
                WHERE sequence_owner = :o
                ORDER BY sequence_name
            """, {"o": schema.upper()})
            for row in cursor.fetchall():
                results.append({
                    "owner": schema, "sequence_name": row[0],
                    "min_value": row[1], "max_value": row[2],
                    "increment_by": row[3], "cycle_flag": row[4],
                    "cache_size": row[5], "last_number": row[6],
                    "order_flag": row[7],
                })
        log.info("[DISCOVER] Found %d sequence(s)", len(results))
    finally:
        cursor.close()
    return results


def discover_types(conn, schemas: List[str]) -> List[Dict]:
    """Discover user-defined types with their attributes.
    Returns list of dicts: {owner, type_name, typecode, attributes: [(attr_name, attr_type, length, precision, scale), ...]}
    """
    log.info("[DISCOVER] Discovering user-defined types for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results: List[Dict] = []
    try:
        for schema in schemas:
            cursor.execute("""
                SELECT type_name, typecode
                FROM all_types
                WHERE owner = :o
                  AND typecode IN ('OBJECT', 'COLLECTION', 'VARRAY')
                  AND type_name NOT LIKE 'SYS_%'
                ORDER BY type_name
            """, {"o": schema.upper()})
            types_raw = cursor.fetchall()
            for type_name, typecode in types_raw:
                attrs = []
                if typecode == "OBJECT":
                    cursor.execute("""
                        SELECT attr_name, attr_type_name, length, precision, scale
                        FROM all_type_attrs
                        WHERE owner = :o AND type_name = :t
                        ORDER BY attr_no
                    """, {"o": schema.upper(), "t": type_name})
                    attrs = [(r[0], r[1], r[2], r[3], r[4]) for r in cursor.fetchall()]
                results.append({
                    "owner": schema, "type_name": type_name,
                    "typecode": typecode, "attributes": attrs,
                })
        log.info("[DISCOVER] Found %d user-defined type(s)", len(results))
    finally:
        cursor.close()
    return results


def discover_indexes(conn, schemas: List[str]) -> List[Dict]:
    """Discover indexes (excluding PK/UK which are created with tables).
    Returns list of dicts with index properties.
    """
    log.info("[DISCOVER] Discovering indexes for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results: List[Dict] = []
    try:
        for schema in schemas:
            schema_upper = schema.upper()
            # Get constraint-related indexes to exclude
            cursor.execute("""
                SELECT index_name FROM all_constraints
                WHERE owner = :o AND constraint_type IN ('P', 'U')
                  AND index_name IS NOT NULL
            """, {"o": schema_upper})
            constraint_indexes = {r[0] for r in cursor.fetchall()}

            cursor.execute("""
                SELECT index_name, index_type, table_name, uniqueness,
                       tablespace_name, compression, prefix_length
                FROM all_indexes
                WHERE owner = :o
                  AND table_type = 'TABLE'
                  AND index_name NOT LIKE 'SYS_%'
                  AND index_name NOT LIKE 'BIN$%'
                  AND temporary = 'N'
                ORDER BY table_name, index_name
            """, {"o": schema_upper})
            indexes_raw = cursor.fetchall()

            for idx_name, idx_type, tbl_name, uniqueness, _tbs, compression, prefix_len in indexes_raw:
                if idx_name in constraint_indexes:
                    continue
                # Get columns
                cursor.execute("""
                    SELECT column_name, column_position, descend
                    FROM all_ind_columns
                    WHERE index_owner = :o AND index_name = :i
                    ORDER BY column_position
                """, {"o": schema_upper, "i": idx_name})
                columns = [(r[0], r[1], r[2]) for r in cursor.fetchall()]

                # Check for function-based expressions
                expressions = []
                if idx_type in ("FUNCTION-BASED NORMAL", "FUNCTION-BASED BITMAP"):
                    cursor.execute("""
                        SELECT column_expression, column_position
                        FROM all_ind_expressions
                        WHERE index_owner = :o AND index_name = :i
                        ORDER BY column_position
                    """, {"o": schema_upper, "i": idx_name})
                    for row in cursor.fetchall():
                        expr = row[0]
                        if expr and hasattr(expr, "read"):
                            expr = expr.read()
                        expressions.append((expr, row[1]))

                results.append({
                    "owner": schema, "index_name": idx_name,
                    "index_type": idx_type, "table_name": tbl_name,
                    "uniqueness": uniqueness, "compression": compression,
                    "prefix_length": prefix_len,
                    "columns": columns, "expressions": expressions,
                })
        log.info("[DISCOVER] Found %d index(es) (excluding PK/UK)", len(results))
    finally:
        cursor.close()
    return results


def discover_mviews(conn, schemas: List[str]) -> List[Tuple[str, str, str, str]]:
    """Discover materialized views.
    Returns [(owner, mview_name, query, rewrite_enabled), ...]
    """
    log.info("[DISCOVER] Discovering materialized views for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results = []
    try:
        for schema in schemas:
            cursor.execute("""
                SELECT mview_name, query, rewrite_enabled, refresh_mode, refresh_method,
                       build_mode
                FROM all_mviews
                WHERE owner = :o
                ORDER BY mview_name
            """, {"o": schema.upper()})
            for row in cursor.fetchall():
                mview_name = row[0]
                query = row[1]
                if query and hasattr(query, "read"):
                    query = query.read()
                results.append({
                    "owner": schema, "mview_name": mview_name,
                    "query": (query or "").strip().rstrip(";").rstrip(),
                    "rewrite_enabled": row[2],
                    "refresh_mode": row[3], "refresh_method": row[4],
                    "build_mode": row[5],
                })
        log.info("[DISCOVER] Found %d materialized view(s)", len(results))
    finally:
        cursor.close()
    return results


def discover_triggers(conn, schemas: List[str]) -> List[Dict]:
    """Discover triggers. Returns list of dicts with trigger properties."""
    log.info("[DISCOVER] Discovering triggers for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results: List[Dict] = []
    try:
        for schema in schemas:
            cursor.execute("""
                SELECT trigger_name, trigger_type, triggering_event, table_name,
                       referencing_names, when_clause, status, trigger_body
                FROM all_triggers
                WHERE owner = :o
                  AND base_object_type = 'TABLE'
                  AND status = 'ENABLED'
                ORDER BY table_name, trigger_name
            """, {"o": schema.upper()})
            for row in cursor.fetchall():
                trigger_body = row[7]
                if trigger_body and hasattr(trigger_body, "read"):
                    trigger_body = trigger_body.read()
                when_clause = row[5]
                if when_clause and hasattr(when_clause, "read"):
                    when_clause = when_clause.read()
                results.append({
                    "owner": schema, "trigger_name": row[0],
                    "trigger_type": row[1], "triggering_event": row[2],
                    "table_name": row[3], "referencing_names": row[4],
                    "when_clause": (when_clause or "").strip(),
                    "status": row[6], "trigger_body": (trigger_body or "").strip(),
                })
        log.info("[DISCOVER] Found %d trigger(s)", len(results))
    finally:
        cursor.close()
    return results


def discover_grants(conn, schemas: List[str]) -> List[Dict]:
    """Discover object-level grants/privileges.
    Returns list of dicts: {grantor, grantee, owner, table_name, privilege, grantable}
    """
    log.info("[DISCOVER] Discovering grants for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results: List[Dict] = []
    try:
        for schema in schemas:
            try:
                cursor.execute("""
                    SELECT grantor, grantee, owner, table_name, privilege, grantable, type
                    FROM dba_tab_privs
                    WHERE owner = :o
                      AND grantee NOT IN ('SYS','SYSTEM','PUBLIC')
                    ORDER BY table_name, grantee, privilege
                """, {"o": schema.upper()})
            except Exception:
                log.debug("[DISCOVER] dba_tab_privs not accessible, falling back to all_tab_privs")
                cursor.execute("""
                    SELECT grantor, grantee, table_schema, table_name, privilege, grantable, type
                    FROM all_tab_privs
                    WHERE table_schema = :o
                      AND grantee NOT IN ('SYS','SYSTEM','PUBLIC')
                    ORDER BY table_name, grantee, privilege
                """, {"o": schema.upper()})
            for row in cursor.fetchall():
                results.append({
                    "grantor": row[0], "grantee": row[1],
                    "owner": row[2], "table_name": row[3],
                    "privilege": row[4], "grantable": row[5],
                    "object_type": row[6] if len(row) > 6 else "",
                })
        log.info("[DISCOVER] Found %d grant(s)", len(results))
    finally:
        cursor.close()
    return results


def discover_partitions(conn, schemas: List[str]) -> List[Dict]:
    """Discover partitioned tables with partition details.
    Returns list of dicts with partition info per table.
    """
    log.info("[DISCOVER] Discovering partitions for %d schema(s)...", len(schemas))
    cursor = conn.cursor()
    results: List[Dict] = []
    try:
        for schema in schemas:
            schema_upper = schema.upper()
            cursor.execute("""
                SELECT table_name, partitioning_type, subpartitioning_type,
                       partition_count
                FROM all_part_tables
                WHERE owner = :o
                ORDER BY table_name
            """, {"o": schema_upper})
            part_tables = cursor.fetchall()

            for tbl_name, part_type, sub_type, part_count in part_tables:
                # Get partition key columns
                cursor.execute("""
                    SELECT column_name, column_position
                    FROM all_part_key_columns
                    WHERE owner = :o AND name = :t AND object_type = 'TABLE'
                    ORDER BY column_position
                """, {"o": schema_upper, "t": tbl_name})
                key_columns = [r[0] for r in cursor.fetchall()]

                # Get individual partitions
                cursor.execute("""
                    SELECT partition_name, high_value, partition_position, tablespace_name
                    FROM all_tab_partitions
                    WHERE table_owner = :o AND table_name = :t
                    ORDER BY partition_position
                """, {"o": schema_upper, "t": tbl_name})
                partitions = []
                for row in cursor.fetchall():
                    high_val = row[1]
                    if high_val and hasattr(high_val, "read"):
                        high_val = high_val.read()
                    partitions.append({
                        "partition_name": row[0],
                        "high_value": (high_val or "").strip(),
                        "position": row[2],
                    })

                # Get sub-partition key columns if applicable
                sub_key_columns = []
                if sub_type and sub_type != "NONE":
                    cursor.execute("""
                        SELECT column_name, column_position
                        FROM all_part_key_columns
                        WHERE owner = :o AND name = :t AND object_type = 'TABLE'
                        ORDER BY column_position
                    """, {"o": schema_upper, "t": tbl_name})
                    sub_key_columns = [r[0] for r in cursor.fetchall()]

                results.append({
                    "owner": schema, "table_name": tbl_name,
                    "partitioning_type": part_type,
                    "subpartitioning_type": sub_type,
                    "partition_count": part_count,
                    "key_columns": key_columns,
                    "sub_key_columns": sub_key_columns,
                    "partitions": partitions,
                })
        log.info("[DISCOVER] Found %d partitioned table(s)", len(results))
    finally:
        cursor.close()
    return results


# #############################################################################
#  SECTION 3: Oracle→PG SQL Conversion Rules
#  (Complete set from oracle_sql_dir_to_postgres_synonyms.py)
# #############################################################################

def _find_closing_paren(text: str, open_pos: int) -> Optional[int]:
    """Return index of ')' matching '(' at open_pos. Skips single-quoted strings."""
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


def _find_open_paren(text: str, close_pos: int) -> Optional[int]:
    """Return index of '(' matching ')' at close_pos."""
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
    if pos <= 0 or pos >= len(text):
        return False
    count = 0
    i = 0
    while i < pos:
        if text[i] == "'":
            if i + 1 < pos and text[i + 1] == "'":
                i += 2
                continue
            count += 1
        i += 1
    return count % 2 == 1


def _split_top_level_commas(s: str) -> List[str]:
    """Split by commas at depth 0 (ignore inside parentheses and single-quoted strings)."""
    parts: List[str] = []
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


def _sub_outside_strings(pattern: str, repl: str, body: str, flags: int = 0) -> str:
    """Run re.sub only on parts outside single-quoted strings."""
    parts = re.split(r"('(?:[^']|'')*')", body)
    compiled = re.compile(pattern, flags)
    for i in range(0, len(parts), 2):
        parts[i] = compiled.sub(repl, parts[i])
    return "".join(parts)


def _strip_sql_comments(content: str) -> str:
    """Remove -- line comments and /* */ block comments from SQL. Preserves strings."""
    result: List[str] = []
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


def _strip_oracle_hints(sql: str) -> str:
    """Remove Oracle optimizer hints: /*+ ... */ and --+ ."""
    parts = re.split(r"('(?:[^']|'')*')", sql)
    for i in range(0, len(parts), 2):
        parts[i] = re.sub(r"/\*\+\s*.*?\*/", " ", parts[i], flags=re.DOTALL)
        parts[i] = re.sub(r"--\+[^\n]*", "", parts[i])
    return "".join(parts)


def _remove_outer_join_plus(text: str) -> str:
    """Remove Oracle (+) outer join notation."""
    if re.search(r"\(\s*\+\s*\)", text):
        text = "/* WARNING: Oracle (+) outer-join syntax removed; review manually */\n" + text
    return re.sub(r"\(\s*\+\s*\)", "", text, flags=re.IGNORECASE)


def _first_statement_only(sql: str) -> str:
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def _replace_to_func_for_oracle_parse(sql: str, func: str, cast_type: str) -> str:
    """Replace TO_*(expr [, ...]) with CAST or placeholder for sqlglot parsing."""
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
        inner = sql[start_paren + 1:close].strip()
        top_args = _split_top_level_commas(inner)
        first = top_args[0].strip() if top_args else inner
        if func.upper() == "TO_CHAR":
            if len(top_args) >= 2 and top_args[1].strip():
                repl = f"pg_to_char__({first}, {top_args[1].strip()})"
            else:
                repl = f"CAST({first} AS {cast_type})"
        elif func.upper() == "TO_DATE" and len(top_args) >= 2 and top_args[1].strip():
            repl = f"pg_to_date__({first}, {top_args[1].strip()})"
        elif func.upper() == "TO_TIMESTAMP" and len(top_args) >= 2 and top_args[1].strip():
            repl = f"pg_to_timestamp__({first}, {top_args[1].strip()})"
        else:
            repl = f"CAST({first} AS {cast_type})"
        sql = sql[:match.start()] + repl + sql[close + 1:]
        search_start = match.start() + len(repl)
    return sql


def _replace_nvl_nvl2_decode_in_body(body: str) -> str:
    """NVL->COALESCE, NVL2->CASE, DECODE->CASE."""
    body = re.sub(r"\bNVL\s*\(", "COALESCE(", body, flags=re.IGNORECASE)
    while True:
        match = re.search(r"\bNVL2\s*\(", body, re.IGNORECASE)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1:close].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 3:
            break
        expr, if_not_null, if_null = args[0].strip(), args[1].strip(), args[2].strip()
        repl = f"CASE WHEN {expr} IS NOT NULL THEN {if_not_null} ELSE {if_null} END"
        body = body[:match.start()] + repl + body[close + 1:]
    while True:
        match = re.search(r"\bDECODE\s*\(", body, re.IGNORECASE)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        inner = body[start_paren + 1:close]
        case_expr = _decode_to_case(inner)
        body = body[:match.start()] + case_expr + body[close + 1:]
    return body


def _decode_to_case(inner: str) -> str:
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


def _preprocess_oracle_before_parse(sql: str) -> str:
    """Preprocess Oracle DDL so sqlglot can parse it."""
    sql = _strip_oracle_hints(sql)
    sql = _strip_sql_comments(sql)
    sql = _remove_outer_join_plus(sql)
    sql = _replace_to_func_for_oracle_parse(sql, "TO_NUMBER", "NUMBER")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_CHAR", "VARCHAR2(4000)")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_DATE", "DATE")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_TIMESTAMP", "TIMESTAMP")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_CLOB", "CLOB")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_BLOB", "BLOB")
    sql = _replace_to_func_for_oracle_parse(sql, "TO_NCLOB", "NCLOB")
    sql = _replace_nvl_nvl2_decode_in_body(sql)
    sql = re.sub(r"\bLIKE\s*\(\s*('[^']*')\s*\)", r"LIKE \1", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bLIKE\s*\(\s*('[^']*')\s*ESCAPE\s*('[^']*')\s*\)", r"LIKE \1 ESCAPE \2", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bTRUNC\s*\(", "pg_trunc__(", sql, flags=re.IGNORECASE)
    return sql


def rewrite_sql_with_synonyms(sql: str, synonym_map: Dict[str, str],
                               view_schema: str = "", view_name: str = "") -> str:
    """Parse Oracle SQL with sqlglot, replace table references using synonym_map, transpile to PG."""
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
            parent = getattr(table, "parent", None)
            if parent is not None and isinstance(parent, exp.Create) and getattr(parent, "this", None) is table:
                continue
            name = (table.name or "").upper()
            db = (table.db or "").upper() if table.db else ""
            if db:
                key = f"{db}.{name}"
            else:
                key = f"{view_schema_upper}.{name}" if view_schema_upper else name
            target = synonym_map.get(key) or synonym_map.get(name)
            if target:
                schema_part, real_name = target.split(".", 1)
                table.set("this", exp.Identifier(this=real_name))
                table.set("db", exp.Identifier(this=schema_part))
        out = tree.sql(dialect="postgres")
    if (view_schema or "").strip() and (view_name or "").strip():
        desired = f"{view_schema.strip()}.{view_name.strip()}".lower()
        out = _REWRITE_VIEW_NAME_PATTERN.sub(lambda m: m.group(1) + desired + m.group(3), out, count=1)
    return out


def _replace_oracle_to_functions_in_body(body: str) -> str:
    """Replace Oracle TO_NUMBER/TO_CHAR/TO_DATE/TO_TIMESTAMP with PostgreSQL equivalents."""
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
            inner = body[start_paren + 1:close].strip()
            args = _split_top_level_commas(inner)
            expr = args[0].strip() if args else ("''" if func != "TO_NUMBER" else "0")
            if func == "TO_NUMBER":
                repl = f"({expr})::numeric"
            elif func == "TO_CHAR":
                if len(args) >= 2 and args[1].strip():
                    repl = f"pg_to_char__({expr}, {args[1].strip()})"
                else:
                    repl = f"({expr})"
            elif func in ("TO_CLOB", "TO_NCLOB"):
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
            else:
                repl = f"({expr})::text"
            body = body[:match.start()] + repl + body[close + 1:]
    body = body.replace("pg_to_char__(", "to_char(")
    return body


def _replace_oracle_builtin_functions_in_body(body: str) -> str:
    """Replace Oracle built-in functions: MONTHS_BETWEEN, ADD_MONTHS, ROUND, LPAD, etc."""
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
            inner = body[start_paren + 1:close].strip()
            args = _split_top_level_commas(inner)
            try:
                repl = replacer(args)
            except Exception:
                break
            if repl is None:
                search_start = close + 1
                continue
            body = body[:match.start()] + repl + body[close + 1:]
            search_start = match.start() + len(repl)
        return body
    body = replace_func(body, "MONTHS_BETWEEN", lambda a: f"((( {a[0].strip()} )::timestamp::date - ( {a[1].strip()} )::timestamp::date) / 30.0)" if len(a) >= 2 else None)
    body = replace_func(body, "ADD_MONTHS", lambda a: f"(( {a[0].strip()} )::timestamp + ( {a[1].strip()} )::int * INTERVAL '1 month')" if len(a) >= 2 else None)
    body = replace_func(body, "ROUND", lambda a: f"round(( {a[0].strip()} )::numeric, ( {a[1].strip()} )::int)" if len(a) >= 2 else None)
    body = replace_func(body, "LPAD", lambda a: (f"lpad(( {a[0].strip()} )::text, ( {a[1].strip()} )::int" + (f", ( {a[2].strip()} )::text" if len(a) >= 3 else "") + ")") if len(a) >= 2 else None)
    body = replace_func(body, "apps.ap_round_currency", lambda a: f"round(( {a[0].strip()} )::numeric, 2)" if a else None)
    body = replace_func(body, "ap_round_currency", lambda a: f"round(( {a[0].strip()} )::numeric, 2)" if a else None)
    body = replace_func(body, "cs_get_serviced_status", lambda a: "NULL::text")
    body = replace_func(body, "apps.cs_get_serviced_status", lambda a: "NULL::text")
    return body


def _replace_oracle_sequence_refs(body: str) -> str:
    """Oracle sequence.NEXTVAL -> nextval('sequence')."""
    body = re.sub(r"\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\.NEXTVAL\b", r"nextval('\1')", body, flags=re.IGNORECASE)
    body = re.sub(r"\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\.CURRVAL\b", r"currval('\1')", body, flags=re.IGNORECASE)
    return body


def _is_trunc_expr_likely_date(expr: str) -> bool:
    e = expr.strip()
    if _TRUNC_DATE_INDICATORS.search(e):
        return True
    col_m = re.search(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*$", e)
    if col_m and _TRUNC_DATE_COLUMN_SUFFIX.search(col_m.group(1)):
        return True
    if re.search(r"\bAS\s+(?:DATE|TIMESTAMP)", e, re.IGNORECASE):
        return True
    return False


def _replace_trunc_in_body(body: str) -> str:
    """Oracle TRUNC -> date_trunc or trunc(numeric)."""
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
        inner = body[start_paren + 1:close].strip()
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
                fmt_map = {"DD": "day", "D": "day", "DAY": "day", "DY": "day",
                           "MM": "month", "MON": "month", "MONTH": "month",
                           "YYYY": "year", "YEAR": "year", "YY": "year", "Y": "year",
                           "Q": "quarter", "QUARTER": "quarter",
                           "HH": "hour", "HH12": "hour", "HH24": "hour",
                           "MI": "minute", "MINUTE": "minute"}
                pg_unit = fmt_map.get(fmt, "day")
                repl = f"date_trunc('{pg_unit}', ({expr})::timestamp)"
        else:
            if _is_trunc_expr_likely_date(expr):
                repl = f"date_trunc('day', ({expr})::timestamp)"
            else:
                repl = f"trunc(({expr})::numeric)"
        if repl is not None:
            body = body[:match.start()] + repl + body[close + 1:]
            search_start = match.start() + len(repl)
        else:
            search_start = close + 1
    return body


def _replace_oracle_misc_in_body(body: str) -> str:
    """Oracle→PG: SYSDATE, SYSTIMESTAMP, MINUS, ROWNUM, INSTR, RPAD, LAST_DAY, etc."""
    body = _sub_outside_strings(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\s+MINUS\s+", " EXCEPT ", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\bROWNUM\b", "(ROW_NUMBER() OVER ())", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(r"\bLENGTHB\s*\(", "octet_length(", body, flags=re.IGNORECASE)
    # ROWIDTOCHAR / CHARTOROWID
    for func_name in ("ROWIDTOCHAR", "CHARTOROWID"):
        pattern_f = re.compile(r"\b" + func_name + r"\s*\(", re.IGNORECASE)
        f_start = 0
        while True:
            match = pattern_f.search(body, f_start)
            if not match:
                break
            sp = match.end() - 1
            cl = _find_closing_paren(body, sp)
            if cl is None:
                break
            inner = body[sp + 1:cl].strip()
            replacement = f"({inner})::text"
            body = body[:match.start()] + replacement + body[cl + 1:]
            f_start = match.start() + len(replacement)
    # BITAND / BITOR
    for op_name, op_char in [("BITAND", "&"), ("BITOR", "|")]:
        pattern_b = re.compile(r"\b" + op_name + r"\s*\(", re.IGNORECASE)
        while True:
            match = pattern_b.search(body)
            if not match:
                break
            sp = match.end() - 1
            cl = _find_closing_paren(body, sp)
            if cl is None:
                break
            inner = body[sp + 1:cl].strip()
            args = _split_top_level_commas(inner)
            if len(args) >= 2:
                a, b = args[0].strip(), args[1].strip()
                body = body[:match.start()] + f"(( {a} )::bigint {op_char} ( {b} )::bigint)" + body[cl + 1:]
            else:
                break
    # INSTR
    pattern_instr = re.compile(r"\bINSTR\s*\(", re.IGNORECASE)
    instr_start = 0
    while True:
        match = pattern_instr.search(body, instr_start)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            s, sub = args[0].strip(), args[1].strip()
            repl = f"strpos(( {s} )::text, ( {sub} )::text)"
            body = body[:match.start()] + repl + body[cl + 1:]
            instr_start = match.start() + len(repl)
        else:
            instr_start = cl + 1
    # RPAD
    pattern_rpad = re.compile(r"\bRPAD\s*\(", re.IGNORECASE)
    rpad_start = 0
    while True:
        match = pattern_rpad.search(body, rpad_start)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if len(args) >= 2:
            s, n = args[0].strip(), args[1].strip()
            tail = f", ( {args[2].strip()} )::text)" if len(args) >= 3 else ")"
            replacement = f"rpad(( {s} )::text, ( {n} )::int{tail}"
            body = body[:match.start()] + replacement + body[cl + 1:]
            rpad_start = match.start() + len(replacement)
        else:
            rpad_start = cl + 1
    # LAST_DAY
    pattern_ld = re.compile(r"\bLAST_DAY\s*\(", re.IGNORECASE)
    while True:
        match = pattern_ld.search(body)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if args:
            d = args[0].strip()
            body = body[:match.start()] + f"((date_trunc('month', ( {d} )::timestamp) + INTERVAL '1 month - 1 day')::date)" + body[cl + 1:]
        else:
            break
    # NEXT_DAY
    pattern_nd = re.compile(r"\bNEXT_DAY\s*\(", re.IGNORECASE)
    while True:
        match = pattern_nd.search(body)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if args:
            d = args[0].strip()
            body = body[:match.start()] + f"(( {d} )::date + INTERVAL '7 days')" + body[cl + 1:]
        else:
            break
    # LISTAGG -> string_agg
    pattern_la = re.compile(r"\bLISTAGG\s*\(", re.IGNORECASE)
    while True:
        match = pattern_la.search(body)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 2:
            break
        expr, delim = args[0].strip(), args[1].strip()
        rest = body[cl + 1:]
        within = re.match(r"\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+(.+?)\)", rest, re.IGNORECASE | re.DOTALL)
        if within:
            order_part = within.group(1).strip()
            agg = f"string_agg(( {expr} )::text, ( {delim} )::text ORDER BY {order_part})"
            span_end = cl + 1 + within.end()
        else:
            agg = f"string_agg(( {expr} )::text, ( {delim} )::text)"
            span_end = cl + 1
        body = body[:match.start()] + agg + body[span_end:]
    # REGEXP_LIKE
    pattern_rl = re.compile(r"\bREGEXP_LIKE\s*\(", re.IGNORECASE)
    while True:
        match = pattern_rl.search(body)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 2:
            break
        s, pat = args[0].strip(), args[1].strip()
        case_ins = len(args) >= 3 and "I" in (args[2].strip().upper().strip("'\""))
        op = "~*" if case_ins else "~"
        body = body[:match.start()] + f"(( {s} )::text {op} ( {pat} )::text)" + body[cl + 1:]
    # SYS_GUID
    body = re.sub(r"\bSYS_GUID\s*\(\s*\)", "gen_random_uuid()::text", body, flags=re.IGNORECASE)
    # WM_CONCAT
    pattern_wm = re.compile(r"\bWM_CONCAT\s*\(", re.IGNORECASE)
    while True:
        match = pattern_wm.search(body)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if args:
            expr = args[0].strip()
            body = body[:match.start()] + f"string_agg(( {expr} )::text, ',')" + body[cl + 1:]
        else:
            break
    # REGEXP_SUBSTR
    pattern_rs = re.compile(r"\bREGEXP_SUBSTR\s*\(", re.IGNORECASE)
    while True:
        match = pattern_rs.search(body)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if len(args) < 2:
            break
        s, pat = args[0].strip(), args[1].strip()
        body = body[:match.start()] + f"(SELECT (r)[1] FROM regexp_matches(( {s} )::text, ( {pat} )::text) AS r LIMIT 1)" + body[cl + 1:]
    # REPLACE 2-arg -> 3-arg
    pattern_rp = re.compile(r"\bREPLACE\s*\(", re.IGNORECASE)
    rpl_start = 0
    while True:
        match = pattern_rp.search(body, rpl_start)
        if not match:
            break
        sp = match.end() - 1
        cl = _find_closing_paren(body, sp)
        if cl is None:
            break
        inner = body[sp + 1:cl].strip()
        args = _split_top_level_commas(inner)
        if len(args) == 2:
            s, old = args[0].strip(), args[1].strip()
            repl = f"REPLACE({s}, {old}, '')"
            body = body[:match.start()] + repl + body[cl + 1:]
            rpl_start = match.start() + len(repl)
        else:
            rpl_start = cl + 1
    return body


def _replace_pg_unit_dd_and_substring(body: str) -> str:
    body = re.sub(r"date_trunc\s*\(\s*['\"]dd['\"]\s*,", "date_trunc('day',", body, flags=re.IGNORECASE)
    body = re.sub(r"INTERVAL\s*'\s*([\d.]+)\s*dd\s*'", r"INTERVAL '\1 day'", body, flags=re.IGNORECASE)
    for func in ("substring", "substr"):
        pattern = re.compile(r"\b" + func + r"\s*\(", re.IGNORECASE)
        search_start = 0
        while True:
            match = pattern.search(body, search_start)
            if not match:
                break
            sp = match.end() - 1
            cl = _find_closing_paren(body, sp)
            if cl is None:
                break
            inner = body[sp + 1:cl].strip()
            args = _split_top_level_commas(inner)
            if len(args) < 3:
                search_start = cl + 1
                continue
            expr, start_arg, length_arg = args[0].strip(), args[1].strip(), args[2].strip()
            new_inner = f"{expr}, {start_arg}, GREATEST({length_arg}, 0)"
            replacement = func + "(" + new_inner + ")"
            body = body[:match.start()] + replacement + body[cl + 1:]
            search_start = match.start() + len(replacement)
    return body


def _replace_dual_with_pg(body: str) -> str:
    single_row = "(SELECT 1 AS dummy) AS dual"
    body = re.sub(r"\bFROM\s+sys\.dual\b", "FROM " + single_row, body, flags=re.IGNORECASE)
    body = re.sub(r"\bFROM\s+dual\b", "FROM " + single_row, body, flags=re.IGNORECASE)
    body = re.sub(r"\bJOIN\s+sys\.dual\b", "JOIN " + single_row, body, flags=re.IGNORECASE)
    body = re.sub(r"\bJOIN\s+dual\b", "JOIN " + single_row, body, flags=re.IGNORECASE)
    return body


def _convert_start_with_connect_by_to_recursive_cte(body: str) -> str:
    """Rewrite Oracle hierarchical queries to PostgreSQL recursive CTE."""
    start_with_m = re.search(r"\bSTART\s+WITH\s+", body, re.IGNORECASE)
    if not start_with_m:
        return body
    rest_after_start = body[start_with_m.end():]
    connect_m = re.search(r"\s+CONNECT\s+BY\s+", rest_after_start, re.IGNORECASE)
    if not connect_m:
        return body
    start_with_condition = rest_after_start[:connect_m.start()].strip()
    connect_by_rest = rest_after_start[connect_m.end():].strip()
    connect_by_rest = re.sub(r"^\s*NOCYCLE\s+", "", connect_by_rest, flags=re.IGNORECASE)
    order_sib = re.search(r"\s+ORDER\s+SIBLINGS\s+BY\s+", connect_by_rest, re.IGNORECASE)
    connect_by_condition = connect_by_rest[:order_sib.start()].strip() if order_sib else connect_by_rest.strip()
    if not start_with_condition or not connect_by_condition:
        return body
    main_query_orig = body[:start_with_m.start()].strip()
    main_query_orig = re.sub(r"[\s,]+$", "", main_query_orig)
    main_query_base = main_query_orig
    where_m = re.search(r"\bWHERE\s+", main_query_base, re.IGNORECASE)
    if where_m:
        main_query_base = main_query_base[:where_m.end()] + f" ( {start_with_condition} ) AND " + main_query_base[where_m.end():]
    else:
        order_m = re.search(r"\bORDER\s+BY\s+", main_query_base, re.IGNORECASE)
        insert_pos = order_m.start() if order_m else len(main_query_base)
        main_query_base = main_query_base[:insert_pos] + f" WHERE ( {start_with_condition} ) " + main_query_base[insert_pos:]
    from_m = re.search(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s*(?:([a-zA-Z_][a-zA-Z0-9_]*))?\s*(?:WHERE|JOIN|START|$)", main_query_orig, re.IGNORECASE)
    if not from_m:
        return body
    table_ref = from_m.group(1).strip()
    table_alias = (from_m.group(2) or table_ref).strip()
    join_cond = connect_by_condition
    join_cond = re.sub(r"\bPRIOR\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*)\b", r"r.\1 = rcte_t.\2", join_cond, flags=re.IGNORECASE)
    join_cond = re.sub(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*PRIOR\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", r"rcte_t.\1 = r.\2", join_cond, flags=re.IGNORECASE)
    base_sql = re.sub(r"\bLEVEL\b", "1 AS level", main_query_base, flags=re.IGNORECASE)
    rec_sql = re.sub(r"\bLEVEL\b", "(r.level + 1) AS level", main_query_orig, flags=re.IGNORECASE)
    rec_from = f" FROM {table_ref} rcte_t JOIN rcte r ON {join_cond}"
    rec_sql = re.sub(r"\bFROM\s+" + re.escape(table_ref) + r"\s*(?:" + re.escape(table_alias) + r")?\s*(?=\s+WHERE|\s+JOIN|$)", rec_from, rec_sql, count=1, flags=re.IGNORECASE)
    if " JOIN rcte r ON " not in rec_sql:
        rec_sql = re.sub(r"\bFROM\s+(\S+)\s*", rec_from + " ", rec_sql, count=1, flags=re.IGNORECASE)
    if " JOIN rcte r ON " not in rec_sql:
        return body
    cte_body = f"( {base_sql} UNION ALL {rec_sql} )"
    return f"WITH RECURSIVE rcte AS {cte_body} SELECT * FROM rcte"


def _oracle_empty_string_as_null(body: str) -> str:
    col_ref = r"\b(?:[a-zA-Z_][a-zA-Z0-9_]*)(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?"
    body = re.sub(rf"({col_ref})\s*=\s*''(?!')", r"\1 IS NULL", body, flags=re.IGNORECASE)
    body = re.sub(rf"({col_ref})\s*<>\s*''(?!')", r"\1 IS NOT NULL", body, flags=re.IGNORECASE)
    body = re.sub(rf"({col_ref})\s*!=\s*''(?!')", r"\1 IS NOT NULL", body, flags=re.IGNORECASE)
    body = re.sub(r"(\))\s*=\s*''(?!')", r"\1 IS NULL", body)
    body = re.sub(r"(\))\s*<>\s*''(?!')", r"\1 IS NOT NULL", body)
    body = re.sub(r"(\))\s*!=\s*''(?!')", r"\1 IS NOT NULL", body)
    body = re.sub(r"\bTHEN\s+''(?!')", "THEN NULL", body, flags=re.IGNORECASE)
    body = re.sub(r"\bELSE\s+''(?!')", "ELSE NULL", body, flags=re.IGNORECASE)
    body = re.sub(r",\s*''(?!')\s*(?=[),])", ", NULL", body)
    return body


def _empty_string_to_null_for_datetime(body: str) -> str:
    parts = re.split(r"('(?:[^']|'')*')", body)
    for i in range(1, len(parts), 2):
        if parts[i] == "''":
            parts[i] = _EMPTY_STR_PLACEHOLDER
    text = "".join(parts)
    text = re.sub(rf"\(\s*{re.escape(_EMPTY_STR_PLACEHOLDER)}\s*\)\s*::\s*(date|timestamp(?:\s+without\s+time\s+zone|\s+with\s+time\s+zone)?)\b", r"NULL::\1", text, flags=re.IGNORECASE)
    text = re.sub(rf"\(\s*{re.escape(_EMPTY_STR_PLACEHOLDER)}\s*\)\s*::\s*(numeric|integer|int|bigint|smallint)\b", r"NULL::\1", text, flags=re.IGNORECASE)
    while True:
        match = re.search(r"\bto_timestamp\s*\(\s*" + re.escape(_EMPTY_STR_PLACEHOLDER) + r"\s*,", text, re.IGNORECASE)
        if not match:
            break
        start_paren = text.index("(", match.start())
        close = _find_closing_paren(text, start_paren)
        if close is None:
            break
        text = text[:match.start()] + "NULL::timestamp" + text[close + 1:]
    text = text.replace(_EMPTY_STR_PLACEHOLDER, "''")
    return text


def _remove_quotes_from_columns(body: str) -> str:
    def unquote(match):
        ident = match.group(1).replace('""', '"')
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
            result.append(pattern.sub(unquote, part))
    return "".join(result)


def _ensure_space_before_keywords(body: str) -> str:
    keywords = r"(FROM|WHERE|GROUP|ORDER|HAVING|LIMIT|OFFSET|UNION|EXCEPT|INTERSECT)(?=\W|$)"
    parts = re.split(r"""('(?:[^']|'')*'|"(?:[^"]|"")*")""", body)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            part = re.sub(rf"([^\sa-zA-Z_])({keywords})", r"\1 \2", part, flags=re.IGNORECASE)
            part = re.sub(r"\bGROUP\s*BY\b", "GROUP BY", part, flags=re.IGNORECASE)
            part = re.sub(r"\bORDER\s*BY\b", "ORDER BY", part, flags=re.IGNORECASE)
            result.append(part)
    return "".join(result)


def _fix_timestamp_plus_integer(body: str) -> str:
    date_funcs = r"(?:CURRENT_TIMESTAMP|SYSDATE|LOCALTIMESTAMP|CURRENT_DATE|LOCALTIME|CURRENT_TIME)"
    body = _sub_outside_strings(rf"({date_funcs})\s*\+\s*(-?\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", r"\1 + \2 * INTERVAL '1 day'", body, flags=re.IGNORECASE)
    body = _sub_outside_strings(rf"({date_funcs})\s*-\s*(\d+(?:\.\d*)?)\b(?!\s*\*\s*INTERVAL)", r"\1 - (\2)::numeric * INTERVAL '1 day'", body, flags=re.IGNORECASE)
    def _replace_plus_id(m):
        if m.group(2).upper() == "INTERVAL":
            return m.group(0)
        return m.group(1) + " + (" + m.group(2) + ")::numeric * INTERVAL '1 day'"
    body = re.sub(rf"({date_funcs})\s*\+\s*([a-zA-Z_][a-zA-Z0-9_.]*)\b(?!\s*\(|\s*\*\s*INTERVAL)", _replace_plus_id, body, flags=re.IGNORECASE)
    return body


def _replace_oracle_package_schemas(body: str) -> str:
    for pkg in ORACLE_EBS_PACKAGES:
        esc = re.escape(pkg)
        body = re.sub(r"\b" + esc + r"\.([a-zA-Z_][a-zA-Z0-9_]*)\s*(\()", r"apps.\1\2", body, flags=re.IGNORECASE)
        body = re.sub(r"\b" + esc + r"\.[a-zA-Z_][a-zA-Z0-9_]*\b(?!\s*\()", "NULL", body, flags=re.IGNORECASE)
    return body


def _replace_package_in_from_clause(body: str) -> str:
    for pkg in ORACLE_EBS_PACKAGES:
        esc = re.escape(pkg)
        body = re.sub(r"\bFROM\s+" + esc + r"\b", "FROM (SELECT 1 AS dummy) AS " + pkg, body, count=0, flags=re.IGNORECASE)
        body = re.sub(r"\bJOIN\s+" + esc + r"\b", "JOIN (SELECT 1 AS dummy) AS " + pkg, body, count=0, flags=re.IGNORECASE)
    return body


def _cast_numeric_string_literals_in_equality(body: str) -> str:
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    pat = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*'(\d{1,5})'(?!\d)(?!\s*::)")
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            result.append(pat.sub(r"\1 = '\2'::numeric", part))
    return "".join(result)


def _replace_userenv_to_postgres(body: str) -> str:
    while True:
        match = re.search(r"\bUSERENV\s*\(", body, re.IGNORECASE)
        if not match:
            break
        start_paren = match.end() - 1
        close = _find_closing_paren(body, start_paren)
        if close is None:
            break
        arg = body[start_paren + 1:close].strip().strip("'\"").upper().replace(" ", "")
        repl = "current_user" if arg in ("USER", "SESSIONUSER", "SESSION_USER") else "NULL::text"
        body = body[:match.start()] + repl + body[close + 1:]
    return body


def _replace_rowid_to_ctid(body: str) -> str:
    body = re.sub(r"\bROW_ID\b", "ctid", body, flags=re.IGNORECASE)
    body = re.sub(r"\bROWID\b", "ctid", body, flags=re.IGNORECASE)
    return body


def _decode_html_entities_in_body(body: str) -> str:
    body = body.replace("&gt;", ">").replace("&lt;", "<")
    body = body.replace("&amp;", "&").replace("&quot;", '"')
    return body


def _repair_identifier_space_before_dot(body: str) -> str:
    parts = re.split(r"('(?:[^']|'')*')", body)
    result = []
    pat = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*)(\.)")
    def replace_if_not_keyword(m):
        if m.group(1).lower() in PG_RESERVED:
            return m.group(0)
        return f"{m.group(1)}_{m.group(2)}{m.group(3)}"
    for i, part in enumerate(parts):
        if i % 2 == 1:
            result.append(part)
        else:
            result.append(pat.sub(replace_if_not_keyword, part))
    return "".join(result)


def _lowercase_body_identifiers(body: str) -> str:
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


def _find_select_list_bounds(body: str) -> Optional[Tuple[int, int]]:
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
    if alias.lower() in PG_RESERVED:
        return f'"{alias}"'
    return alias


def _is_star_select_item(part: str) -> bool:
    p = part.strip()
    if p == "*":
        return True
    return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*\s*\.\s*\*\s*$", p))


def _deduplicate_select_aliases_in_body(body: str) -> str:
    bounds = _find_select_list_bounds(body)
    if not bounds:
        return body
    start, end = bounds
    select_list = body[start:end]
    select_list = re.sub(r",\s*$", "", select_list).strip()
    parts = _split_top_level_commas(select_list)
    alias_count: Dict[str, int] = {}
    unnamed_col_index = [0]
    new_parts: List[str] = []
    alias_re = re.compile(r'\s+AS\s+("?[a-zA-Z_$][a-zA-Z0-9_$]*"?)\s*$', re.IGNORECASE)
    implicit_alias_re = re.compile(r"\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*$")
    for part in parts:
        part_stripped = part.strip()
        if not part_stripped:
            unnamed_col_index[0] += 1
            new_parts.append(f"NULL AS empty_{unnamed_col_index[0]}")
            continue
        if _is_star_select_item(part_stripped):
            new_parts.append(part_stripped)
            continue
        m = alias_re.search(part_stripped)
        if m:
            alias_raw = m.group(1)
            is_quoted = alias_raw.startswith('"') and alias_raw.endswith('"')
            alias_bare = alias_raw[1:-1] if is_quoted else alias_raw
            alias_lower = alias_bare.lower()
            alias_count[alias_lower] = alias_count.get(alias_lower, 0) + 1
            count = alias_count[alias_lower]
            if count > 1:
                new_alias_bare = f"{alias_bare}_{count}"
                out_alias = f'"{new_alias_bare}"' if is_quoted else _quote_alias_if_reserved(new_alias_bare)
                new_parts.append(alias_re.sub(f" AS {out_alias}", part_stripped, count=1))
            else:
                out_alias = alias_raw if is_quoted else _quote_alias_if_reserved(alias_bare)
                if out_alias != alias_raw:
                    new_parts.append(alias_re.sub(f" AS {out_alias}", part_stripped, count=1))
                else:
                    new_parts.append(part_stripped)
        else:
            imm = implicit_alias_re.search(part_stripped) if "(" not in part_stripped else None
            if imm:
                before_text = part_stripped[:imm.start()].rstrip()
                if before_text.endswith(("||", "+", "-", "*", "/")):
                    imm = None
            if imm:
                candidate = imm.group(1)
                if candidate.upper() == "END" and re.search(r"\bCASE\b", part_stripped, re.IGNORECASE):
                    imm = None
                elif candidate.lower() in ("null", "true", "false"):
                    imm = None
            if imm:
                alias = imm.group(1)
                alias_lower = alias.lower()
                alias_count[alias_lower] = alias_count.get(alias_lower, 0) + 1
                count = alias_count[alias_lower]
                out_alias = _quote_alias_if_reserved(alias if count == 1 else f"{alias}_{count}")
                new_parts.append(implicit_alias_re.sub(f" AS {out_alias}", part_stripped, count=1))
            else:
                unnamed_col_index[0] += 1
                new_parts.append(part_stripped + f" AS col_{unnamed_col_index[0]}")
    new_list = ", ".join(new_parts)
    return body[:start] + new_list + body[end:]


# --- Normalize view script (complete pipeline) ---

def normalize_view_script(pg_ddl: str, apply_oracle_conversions: bool = True,
                          orig_schema: str = "", orig_view_name: str = "") -> str:
    """Normalize view DDL with full Oracle→PG body conversions."""
    if not pg_ddl or not pg_ddl.strip():
        return pg_ddl
    text = pg_ddl.replace("\r\n", "\n").replace("\r", "\n").strip()
    text = _NORMALIZE_DROP_VIEW_PATTERN.sub("", text).strip()
    create_match = _NORMALIZE_CREATE_VIEW_PATTERN.search(text)
    if not create_match:
        return pg_ddl
    raw_name = create_match.group(1).strip()
    body = create_match.group(2)
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
            body = _strip_sql_comments(body)
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
        except Exception as e:
            log.warning("[NORMALIZE] exception for %s: %s", view_name, e)
    else:
        body = _lowercase_body_identifiers(body)
    body = body.replace("pg_to_char__(", "to_char(")
    body = body.replace("pg_to_date__(", "to_date(")
    body = body.replace("pg_to_timestamp__(", "to_timestamp(")
    return f"CREATE OR REPLACE VIEW {view_name} AS\n{body}\n;"


# #############################################################################
#  SECTION 4: Dependency Resolution
# #############################################################################

def _body_from_create_view_ddl(ddl: str) -> str:
    match = _BODY_FROM_DDL_PATTERN.search(ddl)
    return match.group(1).strip() if match else ""


def _extract_view_refs_from_body(body: str, view_keys: Set[ViewKey],
                                  view_keys_by_name: Optional[Dict[str, List[ViewKey]]] = None) -> Set[ViewKey]:
    refs: Set[ViewKey] = set()
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


def topological_sort(views: List[Tuple[str, str]], deps: List[Set[ViewKey]]) -> List[int]:
    """Topological sort using Kahn's algorithm."""
    n = len(views)
    if n == 0:
        return []
    key_to_indices: Dict[Tuple[str, str], List[int]] = {}
    name_to_indices: Dict[str, List[int]] = {}
    for j in range(n):
        sj = (views[j][0] or "").lower()
        vj = (views[j][1] or "").lower()
        key_to_indices.setdefault((sj, vj), []).append(j)
        name_to_indices.setdefault(vj, []).append(j)
    dep_indices: List[Set[int]] = [set() for _ in range(n)]
    reverse_deps: List[Set[int]] = [set() for _ in range(n)]
    for i in range(n):
        for (ds, dv) in deps[i]:
            resolved = key_to_indices.get((ds, dv), [])
            if not resolved and not ds:
                resolved = name_to_indices.get(dv, [])
            for j in resolved:
                if j != i:
                    dep_indices[i].add(j)
                    reverse_deps[j].add(i)
    in_degree = [len(dep_indices[i]) for i in range(n)]
    queue_list = [i for i in range(n) if in_degree[i] == 0]
    order: List[int] = []
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
        log.warning("[TOPO] Circular dependency among %d view(s); order may be wrong", len(remaining))
    return order if order else list(range(n))


# #############################################################################
#  SECTION 5: PostgreSQL Execution Helpers
# #############################################################################

def ensure_pg_schema(conn, schema: str) -> bool:
    if not schema or not schema.strip():
        return True
    try:
        from psycopg2 import sql as psql
        with conn.cursor() as cur:
            cur.execute(psql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(psql.Identifier(schema.strip().lower())))
        conn.commit()
        log.debug("[PG] Schema ensured: %s", schema)
        return True
    except Exception as e:
        conn.rollback()
        log.warning("[PG] Failed to create schema %s: %s", schema, e)
        return False


def execute_ddl_on_pg(conn, ddl: str) -> Tuple[bool, str]:
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        return True, ""
    except Exception as e:
        conn.rollback()
        return False, str(e).strip()


# #############################################################################
#  SECTION 6: Migration Functions
# #############################################################################

def migrate_schemas(pg_conn, schemas: List[str], stats: Dict) -> None:
    """Create schemas in PostgreSQL."""
    log.info("=" * 72)
    log.info("PHASE 1: SCHEMA CREATION")
    log.info("=" * 72)
    for schema in schemas:
        pg_schema = schema.strip().lower()
        if not pg_schema:
            continue
        ok = ensure_pg_schema(pg_conn, pg_schema)
        if ok:
            stats["schemas_ok"] += 1
            log.info("  [SCHEMA] Created/exists: %s", pg_schema)
        else:
            stats["schemas_fail"] += 1
            stats["errors"].append(("schema", pg_schema, "", "Failed to create schema"))


def migrate_sequences(pg_conn, sequences: List[Dict], stats: Dict,
                      dry_run: bool = False) -> None:
    """Migrate Oracle sequences to PostgreSQL."""
    log.info("=" * 72)
    log.info("PHASE 2: SEQUENCE MIGRATION")
    log.info("=" * 72)
    log.info("  %d sequence(s) to migrate", len(sequences))
    stats["sequences_total"] = len(sequences)
    if not sequences:
        return

    schemas_ensured: Set[str] = set()
    for i, seq in enumerate(sequences):
        owner = seq["owner"]
        seq_name = seq["sequence_name"]
        schema_l = owner.strip().lower()
        seq_name_l = seq_name.strip().lower()
        display = f"{schema_l}.{seq_name_l}"

        if schema_l and schema_l not in schemas_ensured:
            if not dry_run:
                ensure_pg_schema(pg_conn, schema_l)
            schemas_ensured.add(schema_l)

        min_val = seq.get("min_value", 1)
        max_val = seq.get("max_value")
        inc_by = seq.get("increment_by", 1)
        cycle_flag = seq.get("cycle_flag", "N")
        cache_size = seq.get("cache_size", 20)
        last_number = seq.get("last_number", 1)

        parts = [f"CREATE SEQUENCE IF NOT EXISTS {schema_l}.{seq_name_l}"]
        parts.append(f"  INCREMENT BY {inc_by}")
        if min_val is not None:
            parts.append(f"  MINVALUE {min_val}")
        # Cap Oracle's enormous max to PG bigint max
        pg_max = 9223372036854775807
        if max_val is not None and max_val <= pg_max:
            parts.append(f"  MAXVALUE {max_val}")
        else:
            parts.append("  NO MAXVALUE")
        start_val = last_number if last_number is not None else (min_val or 1)
        parts.append(f"  START WITH {start_val}")
        if cache_size and cache_size > 1:
            parts.append(f"  CACHE {cache_size}")
        if cycle_flag == "Y":
            parts.append("  CYCLE")
        else:
            parts.append("  NO CYCLE")
        ddl = "\n".join(parts) + ";"

        if dry_run:
            stats["sequences_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, ddl)
        if ok:
            stats["sequences_ok"] += 1
            if (i + 1) % 200 == 0 or (i + 1) == len(sequences):
                log.info("  [SEQ] Progress: %d/%d", i + 1, len(sequences))
        else:
            stats["sequences_fail"] += 1
            stats["errors"].append(("sequence", display, "", err[:300]))
            log.debug("  [SEQ] FAIL %s: %s", display, err[:150])


def migrate_types(pg_conn, types_list: List[Dict], stats: Dict,
                  dry_run: bool = False) -> None:
    """Migrate Oracle user-defined types to PostgreSQL composite types."""
    log.info("=" * 72)
    log.info("PHASE 3: USER-DEFINED TYPE MIGRATION")
    log.info("=" * 72)
    log.info("  %d type(s) to migrate", len(types_list))
    stats["types_total"] = len(types_list)
    if not types_list:
        return

    schemas_ensured: Set[str] = set()
    for i, udt in enumerate(types_list):
        owner = udt["owner"]
        type_name = udt["type_name"]
        typecode = udt["typecode"]
        attrs = udt.get("attributes", [])
        schema_l = owner.strip().lower()
        type_name_l = type_name.strip().lower()
        display = f"{schema_l}.{type_name_l}"

        if schema_l and schema_l not in schemas_ensured:
            if not dry_run:
                ensure_pg_schema(pg_conn, schema_l)
            schemas_ensured.add(schema_l)

        if typecode == "OBJECT" and attrs:
            attr_defs = []
            for attr_name, attr_type, length, precision, scale in attrs:
                pg_type = _oracle_col_type_to_pg(attr_type, length, precision, scale)
                attr_defs.append(f"  {attr_name.lower()} {pg_type}")
            ddl = f"DO $$ BEGIN\n"
            ddl += f"  CREATE TYPE {schema_l}.{type_name_l} AS (\n"
            ddl += ",\n".join(attr_defs)
            ddl += "\n  );\n"
            ddl += f"EXCEPTION WHEN duplicate_object THEN NULL;\n"
            ddl += f"END $$;"
        elif typecode == "COLLECTION":
            ddl = (
                f"-- Oracle COLLECTION type {display}: no direct PG equivalent.\n"
                f"-- Consider using PostgreSQL arrays or composite types.\n"
                f"-- CREATE DOMAIN {schema_l}.{type_name_l} AS TEXT[];"
            )
        elif typecode == "VARRAY":
            ddl = (
                f"-- Oracle VARRAY type {display}: mapped to PG array.\n"
                f"CREATE DOMAIN IF NOT EXISTS {schema_l}.{type_name_l} AS TEXT[];"
            )
        else:
            ddl = f"-- Oracle type {display} ({typecode}): skipped (unsupported typecode)"

        if dry_run:
            stats["types_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, ddl)
        if ok:
            stats["types_ok"] += 1
        else:
            stats["types_fail"] += 1
            stats["errors"].append(("type", display, typecode, err[:300]))
            log.debug("  [TYPE] FAIL %s: %s", display, err[:150])


def _oracle_col_type_to_pg(data_type: str, data_length, data_precision, data_scale) -> str:
    """Convert an Oracle column data type to a PostgreSQL column data type."""
    dt = (data_type or "").upper().strip()

    # Direct mappings for common types
    direct = {
        "CHAR": f"CHAR({data_length or 1})",
        "NCHAR": f"CHAR({data_length or 1})",
        "NVARCHAR2": f"VARCHAR({data_length or 4000})",
        "CLOB": "TEXT",
        "NCLOB": "TEXT",
        "LONG": "TEXT",
        "BLOB": "BYTEA",
        "RAW": f"BYTEA",
        "LONG RAW": "BYTEA",
        "BFILE": "TEXT",
        "ROWID": "VARCHAR(18)",
        "UROWID": "VARCHAR(4000)",
        "BINARY_FLOAT": "REAL",
        "BINARY_DOUBLE": "DOUBLE PRECISION",
        "FLOAT": f"DOUBLE PRECISION",
        "REAL": "REAL",
        "XMLTYPE": "XML",
        "SDO_GEOMETRY": "GEOMETRY",
        "BOOLEAN": "BOOLEAN",
        "PLS_INTEGER": "INTEGER",
        "BINARY_INTEGER": "INTEGER",
        "INTEGER": "INTEGER",
        "SMALLINT": "SMALLINT",
    }
    if dt in direct:
        return direct[dt]

    # VARCHAR2
    if dt == "VARCHAR2":
        length = data_length or 4000
        return f"VARCHAR({length})"

    # NUMBER
    if dt == "NUMBER":
        if data_precision is not None and data_scale is not None:
            if data_scale == 0:
                if data_precision <= 4:
                    return "SMALLINT"
                elif data_precision <= 9:
                    return "INTEGER"
                elif data_precision <= 18:
                    return "BIGINT"
                else:
                    return f"NUMERIC({data_precision})"
            else:
                return f"NUMERIC({data_precision},{data_scale})"
        elif data_precision is not None and data_scale is None:
            return f"NUMERIC({data_precision})"
        else:
            return "NUMERIC"

    # DATE → TIMESTAMP (Oracle DATE has time component)
    if dt == "DATE":
        return "TIMESTAMP"

    # TIMESTAMP variants
    if dt.startswith("TIMESTAMP"):
        if "WITH LOCAL TIME ZONE" in dt:
            return "TIMESTAMPTZ"
        elif "WITH TIME ZONE" in dt:
            return "TIMESTAMPTZ"
        else:
            return "TIMESTAMP"

    # INTERVAL
    if dt.startswith("INTERVAL YEAR"):
        return "INTERVAL"
    if dt.startswith("INTERVAL DAY"):
        return "INTERVAL"

    # Fallback
    return dt if dt else "TEXT"


def _convert_oracle_default_to_pg(default_val: str) -> str:
    """Convert an Oracle column default value to PostgreSQL equivalent."""
    if not default_val:
        return ""
    val = default_val.strip()
    # SYSDATE / SYSTIMESTAMP
    val = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", val, flags=re.IGNORECASE)
    val = re.sub(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", val, flags=re.IGNORECASE)
    # SYS_GUID() → gen_random_uuid()
    val = re.sub(r"\bSYS_GUID\s*\(\s*\)", "gen_random_uuid()", val, flags=re.IGNORECASE)
    # user → CURRENT_USER
    val = re.sub(r"^USER$", "CURRENT_USER", val, flags=re.IGNORECASE)
    # USERENV('SESSIONID') etc.
    val = re.sub(r"\bUSERENV\s*\(\s*'[^']*'\s*\)", "current_setting('application_name')", val, flags=re.IGNORECASE)
    # empty string '' in Oracle is NULL, but as a DEFAULT it can stay ''
    return val


def _convert_check_condition_to_pg(condition: str) -> str:
    """Convert Oracle CHECK constraint condition to PostgreSQL."""
    if not condition:
        return condition
    cond = condition.strip()
    # Remove double-quoted identifiers and lowercase them
    cond = re.sub(r'"([^"]+)"', lambda m: m.group(1).lower(), cond)
    return cond


def migrate_tables(pg_conn, tables: List[Dict], stats: Dict, dry_run: bool = False) -> None:
    """Migrate Oracle tables to PostgreSQL with columns, constraints, and defaults."""
    log.info("=" * 72)
    log.info("PHASE 4: TABLE MIGRATION")
    log.info("=" * 72)
    log.info("  %d table(s) to migrate", len(tables))
    stats["tables_total"] = len(tables)
    if not tables:
        return

    # Sort tables: those without foreign keys first, then those with FKs
    # This is a simple heuristic; for deep FK chains, multiple passes may be needed
    tables_no_fk = [t for t in tables if not t.get("foreign_keys")]
    tables_with_fk = [t for t in tables if t.get("foreign_keys")]
    ordered_tables = tables_no_fk + tables_with_fk
    log.info("  %d table(s) without FK (first), %d with FK (after)", len(tables_no_fk), len(tables_with_fk))

    schemas_ensured: Set[str] = set()

    for idx, tbl in enumerate(ordered_tables):
        owner = tbl["owner"]
        tbl_name = tbl["table_name"]
        schema_l = owner.strip().lower()
        tbl_name_l = tbl_name.strip().lower()
        display = f"{schema_l}.{tbl_name_l}"

        if schema_l and schema_l not in schemas_ensured:
            if not dry_run:
                ensure_pg_schema(pg_conn, schema_l)
            schemas_ensured.add(schema_l)

        columns = tbl.get("columns", [])
        if not columns:
            stats["tables_fail"] += 1
            stats["errors"].append(("table", display, "", "No columns found"))
            log.debug("  [TABLE] SKIP %s: no columns", display)
            continue

        # Build CREATE TABLE DDL
        col_defs = []
        for col_name, data_type, data_length, data_precision, data_scale, nullable, default_val in columns:
            col_l = col_name.strip().lower()
            pg_type = _oracle_col_type_to_pg(data_type, data_length, data_precision, data_scale)

            parts = [col_l, pg_type]

            if default_val:
                pg_default = _convert_oracle_default_to_pg(default_val)
                if pg_default:
                    parts.append(f"DEFAULT {pg_default}")

            if nullable == "N":
                parts.append("NOT NULL")

            col_defs.append("  " + " ".join(parts))

        # Primary key
        pk = tbl.get("primary_key")
        if pk and pk.get("columns"):
            pk_cols = ", ".join(c.lower() for c in pk["columns"])
            col_defs.append(f"  CONSTRAINT {pk['name'].lower()} PRIMARY KEY ({pk_cols})")

        # Unique constraints
        for uq in tbl.get("unique_keys", []):
            if uq.get("columns"):
                uq_cols = ", ".join(c.lower() for c in uq["columns"])
                col_defs.append(f"  CONSTRAINT {uq['name'].lower()} UNIQUE ({uq_cols})")

        # Check constraints
        for ck in tbl.get("check_constraints", []):
            cond = _convert_check_condition_to_pg(ck.get("condition", ""))
            if cond:
                col_defs.append(f"  CONSTRAINT {ck['name'].lower()} CHECK ({cond})")

        ddl = f"CREATE TABLE IF NOT EXISTS {schema_l}.{tbl_name_l} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"

        if dry_run:
            stats["tables_ok"] += 1
            if (idx + 1) % 200 == 0 or (idx + 1) == len(ordered_tables):
                log.info("  [DRY-RUN TABLE] Progress: %d/%d", idx + 1, len(ordered_tables))
            continue

        ok, err = execute_ddl_on_pg(pg_conn, ddl)
        if ok:
            stats["tables_ok"] += 1
            if (idx + 1) % 200 == 0 or (idx + 1) == len(ordered_tables):
                log.info("  [TABLE] Progress: %d/%d", idx + 1, len(ordered_tables))
        else:
            stats["tables_fail"] += 1
            stats["errors"].append(("table", display, "", err[:300]))
            log.debug("  [TABLE] FAIL %s: %s", display, err[:150])

    # Second pass: add foreign keys (separate ALTER TABLE to avoid ordering issues)
    fk_total = 0
    fk_ok = 0
    fk_fail = 0
    for tbl in ordered_tables:
        for fk in tbl.get("foreign_keys", []):
            fk_total += 1
            owner = tbl["owner"]
            tbl_name = tbl["table_name"]
            schema_l = owner.strip().lower()
            tbl_name_l = tbl_name.strip().lower()
            fk_name = fk["name"].lower()
            fk_cols = ", ".join(c.lower() for c in fk["columns"])
            ref_owner_l = fk["ref_owner"].strip().lower()
            ref_table_l = fk["ref_table"].strip().lower()
            ref_cols = ", ".join(c.lower() for c in fk["ref_columns"])

            fk_ddl = (
                f"ALTER TABLE {schema_l}.{tbl_name_l} "
                f"ADD CONSTRAINT {fk_name} "
                f"FOREIGN KEY ({fk_cols}) "
                f"REFERENCES {ref_owner_l}.{ref_table_l} ({ref_cols});"
            )
            if dry_run:
                fk_ok += 1
                continue
            ok_fk, err_fk = execute_ddl_on_pg(pg_conn, fk_ddl)
            if ok_fk:
                fk_ok += 1
            else:
                fk_fail += 1
                display = f"{schema_l}.{tbl_name_l}"
                stats["errors"].append(("table_fk", display, fk_name, err_fk[:300]))
                log.debug("  [FK] FAIL %s.%s: %s", display, fk_name, err_fk[:150])

    stats["table_fk_total"] = fk_total
    stats["table_fk_ok"] = fk_ok
    stats["table_fk_fail"] = fk_fail
    if fk_total > 0:
        log.info("  Foreign keys: %d total, %d OK, %d fail", fk_total, fk_ok, fk_fail)
    log.info("  Tables complete: %d OK, %d fail out of %d",
             stats.get("tables_ok", 0), stats.get("tables_fail", 0), stats.get("tables_total", 0))


def migrate_indexes(pg_conn, indexes: List[Dict], stats: Dict,
                    dry_run: bool = False) -> None:
    """Migrate Oracle indexes to PostgreSQL (excludes PK/UK already created with tables)."""
    log.info("=" * 72)
    log.info("PHASE 6: INDEX MIGRATION")
    log.info("=" * 72)
    log.info("  %d index(es) to migrate", len(indexes))
    stats["indexes_total"] = len(indexes)
    if not indexes:
        return

    for i, idx in enumerate(indexes):
        owner = idx["owner"]
        idx_name = idx["index_name"]
        tbl_name = idx["table_name"]
        idx_type = idx.get("index_type", "NORMAL")
        uniqueness = idx.get("uniqueness", "NONUNIQUE")
        columns = idx.get("columns", [])
        expressions = idx.get("expressions", [])

        schema_l = owner.strip().lower()
        idx_name_l = idx_name.strip().lower()
        tbl_name_l = tbl_name.strip().lower()
        display = f"{schema_l}.{idx_name_l}"

        unique_kw = "UNIQUE " if uniqueness == "UNIQUE" else ""

        # Determine PG index method
        if "BITMAP" in idx_type:
            method = "USING gin"
        else:
            method = ""

        # Build column list
        if expressions:
            expr_by_pos = {pos: expr for expr, pos in expressions}
            col_parts = []
            for col_name, col_pos, descend in columns:
                if col_pos in expr_by_pos and expr_by_pos[col_pos]:
                    expr_str = expr_by_pos[col_pos].strip()
                    # Basic Oracle-to-PG expression conversion
                    expr_str = re.sub(r'"([^"]+)"', lambda m: m.group(1).lower(), expr_str)
                    expr_str = re.sub(r"\bNVL\s*\(", "COALESCE(", expr_str, flags=re.IGNORECASE)
                    expr_str = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", expr_str, flags=re.IGNORECASE)
                    col_parts.append(f"({expr_str})")
                else:
                    col_l = col_name.strip().lower()
                    desc_kw = " DESC" if descend == "DESC" else ""
                    col_parts.append(f"{col_l}{desc_kw}")
        else:
            col_parts = []
            for col_name, _col_pos, descend in columns:
                col_l = col_name.strip().lower()
                desc_kw = " DESC" if descend == "DESC" else ""
                col_parts.append(f"{col_l}{desc_kw}")

        if not col_parts:
            stats["indexes_fail"] += 1
            stats["errors"].append(("index", display, tbl_name_l, "No columns found"))
            continue

        col_list_str = ", ".join(col_parts)
        method_str = f" {method}" if method else ""
        ddl = (
            f"CREATE {unique_kw}INDEX IF NOT EXISTS {idx_name_l} "
            f"ON {schema_l}.{tbl_name_l}{method_str} ({col_list_str});"
        )

        if dry_run:
            stats["indexes_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, ddl)
        if ok:
            stats["indexes_ok"] += 1
            if (i + 1) % 200 == 0 or (i + 1) == len(indexes):
                log.info("  [IDX] Progress: %d/%d", i + 1, len(indexes))
        else:
            stats["indexes_fail"] += 1
            stats["errors"].append(("index", display, tbl_name_l, err[:300]))
            log.debug("  [IDX] FAIL %s: %s", display, err[:150])


def migrate_synonyms_as_views(pg_conn, synonyms: List[Tuple[str, str, str, str]],
                               schemas_filter: Optional[Set[str]], stats: Dict,
                               dry_run: bool = False) -> None:
    """Migrate Oracle synonyms as PostgreSQL views pointing to target tables."""
    log.info("=" * 72)
    log.info("PHASE 7: SYNONYM → VIEW MIGRATION")
    log.info("=" * 72)
    relevant = []
    for owner, syn_name, tbl_owner, tbl_name in synonyms:
        owner_u = (owner or "").upper()
        if schemas_filter and owner_u not in schemas_filter and owner_u != "PUBLIC":
            continue
        relevant.append((owner, syn_name, tbl_owner, tbl_name))
    log.info("  %d synonym(s) to migrate", len(relevant))
    stats["synonyms_total"] = len(relevant)
    for i, (owner, syn_name, tbl_owner, tbl_name) in enumerate(relevant):
        owner_l = owner.strip().lower()
        syn_l = syn_name.strip().lower()
        tbl_owner_l = tbl_owner.strip().lower()
        tbl_name_l = tbl_name.strip().lower()
        if owner_l == "public":
            view_schema = "public"
        else:
            view_schema = owner_l
        qualified_view = f"{view_schema}.{syn_l}"
        qualified_target = f"{tbl_owner_l}.{tbl_name_l}"
        ddl = f"CREATE OR REPLACE VIEW {qualified_view} AS SELECT * FROM {qualified_target};"
        if dry_run:
            stats["synonyms_ok"] += 1
            log.debug("  [DRY-RUN] %s -> %s", qualified_view, qualified_target)
            continue
        ok, err = execute_ddl_on_pg(pg_conn, ddl)
        if ok:
            stats["synonyms_ok"] += 1
            if (i + 1) % 500 == 0 or (i + 1) == len(relevant):
                log.info("  [SYNONYM] Progress: %d/%d", i + 1, len(relevant))
        else:
            stats["synonyms_fail"] += 1
            stats["errors"].append(("synonym", qualified_view, qualified_target, err[:200]))
            log.debug("  [SYNONYM] FAIL %s: %s", qualified_view, err[:100])


def migrate_functions(pg_conn, functions: List[Tuple[str, str, str, str]],
                      stats: Dict, dry_run: bool = False) -> None:
    """Migrate Oracle functions/procedures to PostgreSQL (best-effort PL/SQL→PL/pgSQL)."""
    log.info("=" * 72)
    log.info("PHASE 8: FUNCTION/PROCEDURE MIGRATION")
    log.info("=" * 72)
    log.info("  %d function(s)/procedure(s) to migrate", len(functions))
    stats["functions_total"] = len(functions)
    for i, (schema, name, obj_type, source) in enumerate(functions):
        schema_l = schema.strip().lower()
        name_l = name.strip().lower()
        display = f"{schema_l}.{name_l}"
        try:
            pg_sql = convert_oracle_function_to_pg(schema_l, name_l, obj_type, source)
        except Exception as e:
            stats["functions_fail"] += 1
            stats["errors"].append(("function", display, obj_type, str(e)[:200]))
            log.debug("  [FUNC] FAIL convert %s: %s", display, e)
            continue
        if dry_run:
            stats["functions_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, pg_sql)
        if ok:
            stats["functions_ok"] += 1
            if (i + 1) % 100 == 0 or (i + 1) == len(functions):
                log.info("  [FUNC] Progress: %d/%d", i + 1, len(functions))
        else:
            stats["functions_fail"] += 1
            stats["errors"].append(("function", display, obj_type, err[:200]))
            log.debug("  [FUNC] FAIL execute %s: %s", display, err[:100])


def convert_oracle_function_to_pg(schema: str, name: str, obj_type: str, source: str) -> str:
    """Best-effort conversion of Oracle PL/SQL function/procedure to PostgreSQL PL/pgSQL."""
    body = source.strip()
    body = re.sub(r"\bVARCHAR2\b", "VARCHAR", body, flags=re.IGNORECASE)
    body = re.sub(r"\bNUMBER\b", "NUMERIC", body, flags=re.IGNORECASE)
    body = re.sub(r"\bPLS_INTEGER\b", "INTEGER", body, flags=re.IGNORECASE)
    body = re.sub(r"\bBINARY_INTEGER\b", "INTEGER", body, flags=re.IGNORECASE)
    body = re.sub(r"\bBOOLEAN\b", "BOOLEAN", body, flags=re.IGNORECASE)
    body = re.sub(r"\bCLOB\b", "TEXT", body, flags=re.IGNORECASE)
    body = re.sub(r"\bBLOB\b", "BYTEA", body, flags=re.IGNORECASE)
    body = re.sub(r"\bRAW\s*\(\s*\d+\s*\)", "BYTEA", body, flags=re.IGNORECASE)
    body = re.sub(r"\bLONG\b", "TEXT", body, flags=re.IGNORECASE)
    body = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = re.sub(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", body, flags=re.IGNORECASE)
    body = re.sub(r"\bNVL\s*\(", "COALESCE(", body, flags=re.IGNORECASE)
    body = re.sub(r"\bDBMS_OUTPUT\.PUT_LINE\b", "RAISE NOTICE '%', ", body, flags=re.IGNORECASE)
    body = re.sub(r"\bRETURN\b(?!\s)", "RETURN ", body, flags=re.IGNORECASE)
    body = re.sub(r"%TYPE\b", "%TYPE", body, flags=re.IGNORECASE)
    body = re.sub(r"%ROWTYPE\b", "%ROWTYPE", body, flags=re.IGNORECASE)
    body = re.sub(r"\bEXCEPTION\b", "EXCEPTION", body, flags=re.IGNORECASE)
    body = re.sub(r"\bWHEN\s+OTHERS\s+THEN\b", "WHEN OTHERS THEN", body, flags=re.IGNORECASE)
    header_match = re.match(
        r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?(FUNCTION|PROCEDURE)\s+(\S+)",
        body, re.IGNORECASE
    )
    if not header_match:
        return f"-- WARNING: Could not parse {obj_type} {schema}.{name}\n-- Original source:\n/*\n{source}\n*/"
    qualified_name = f"{schema}.{name}"
    return f"CREATE OR REPLACE {obj_type} {qualified_name}\n" \
           f"-- WARNING: Auto-converted from Oracle PL/SQL; manual review required\n" \
           f"-- Original Oracle source follows, wrapped in PL/pgSQL:\n" \
           f"LANGUAGE plpgsql AS $$\n" \
           f"BEGIN\n" \
           f"  -- TODO: Complete manual conversion of Oracle PL/SQL to PL/pgSQL\n" \
           f"  RAISE NOTICE 'Stub for {qualified_name} - needs manual conversion';\n" \
           f"  RETURN NULL;\n" \
           f"END;\n$$;\n" \
           f"/* Original Oracle source:\n{source}\n*/"


def migrate_packages(pg_conn, packages: List[Tuple[str, str, str, str]],
                     stats: Dict, dry_run: bool = False) -> None:
    """Migrate Oracle packages to PostgreSQL.
    Strategy: Create a schema per package and individual functions within it."""
    log.info("=" * 72)
    log.info("PHASE 9: PACKAGE MIGRATION")
    log.info("=" * 72)
    log.info("  %d package(s) to migrate", len(packages))
    stats["packages_total"] = len(packages)
    for i, (schema, pkg_name, spec_source, body_source) in enumerate(packages):
        schema_l = schema.strip().lower()
        pkg_l = pkg_name.strip().lower()
        display = f"{schema_l}.{pkg_l}"
        pg_schema = f"{schema_l}_pkg_{pkg_l}"
        log.info("  [PKG %d/%d] %s -> schema %s", i + 1, len(packages), display, pg_schema)
        if not dry_run:
            ensure_pg_schema(pg_conn, pg_schema)
        func_defs = extract_functions_from_package_body(body_source or spec_source, schema_l, pkg_l, pg_schema)
        if not func_defs:
            comment_ddl = (
                f"-- Package {display}: spec and body extracted but no functions parsed.\n"
                f"-- Package spec ({len(spec_source)} chars), body ({len(body_source)} chars)\n"
                f"COMMENT ON SCHEMA {pg_schema} IS 'Oracle package {display} (stub)';\n"
            )
            if not dry_run:
                execute_ddl_on_pg(pg_conn, comment_ddl)
            stats["packages_ok"] += 1
            stats["pkg_functions_stub"] += 1
            continue
        pkg_ok = 0
        pkg_fail = 0
        for func_ddl in func_defs:
            if dry_run:
                pkg_ok += 1
                continue
            ok, err = execute_ddl_on_pg(pg_conn, func_ddl)
            if ok:
                pkg_ok += 1
            else:
                pkg_fail += 1
                stats["errors"].append(("package_func", display, "", err[:200]))
        if pkg_fail == 0:
            stats["packages_ok"] += 1
        else:
            stats["packages_fail"] += 1
        stats["pkg_functions_ok"] += pkg_ok
        stats["pkg_functions_fail"] += pkg_fail
        log.info("    Functions: %d OK, %d FAIL", pkg_ok, pkg_fail)


def extract_functions_from_package_body(source: str, oracle_schema: str,
                                         pkg_name: str, pg_schema: str) -> List[str]:
    """Extract function/procedure definitions from Oracle package body and convert to PG stubs."""
    if not source:
        return []
    results = []
    pattern = re.compile(
        r"\b(FUNCTION|PROCEDURE)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(\([^)]*\))?\s*(RETURN\s+\S+)?",
        re.IGNORECASE,
    )
    for m in pattern.finditer(source):
        kind = m.group(1).upper()
        fname = m.group(2).lower()
        params_raw = m.group(3) or "()"
        return_type_raw = m.group(4) or ""
        params_pg = convert_oracle_params_to_pg(params_raw)
        if kind == "FUNCTION":
            ret_type = convert_oracle_type_to_pg(return_type_raw.replace("RETURN", "").strip()) if return_type_raw else "TEXT"
            ddl = (
                f"CREATE OR REPLACE FUNCTION {pg_schema}.{fname}{params_pg}\n"
                f"RETURNS {ret_type} LANGUAGE plpgsql AS $$\n"
                f"BEGIN\n"
                f"  -- Stub: Oracle package {oracle_schema}.{pkg_name}.{fname}\n"
                f"  RETURN NULL;\n"
                f"END;\n$$;"
            )
        else:
            ddl = (
                f"CREATE OR REPLACE PROCEDURE {pg_schema}.{fname}{params_pg}\n"
                f"LANGUAGE plpgsql AS $$\n"
                f"BEGIN\n"
                f"  -- Stub: Oracle package {oracle_schema}.{pkg_name}.{fname}\n"
                f"  NULL;\n"
                f"END;\n$$;"
            )
        results.append(ddl)
    return results


def convert_oracle_params_to_pg(params_raw: str) -> str:
    """Convert Oracle parameter list to PostgreSQL format (best-effort)."""
    if not params_raw or params_raw.strip() in ("()", ""):
        return "()"
    inner = params_raw.strip().strip("()")
    if not inner.strip():
        return "()"
    parts = _split_top_level_commas(inner)
    pg_parts = []
    for part in parts:
        part = part.strip()
        part = re.sub(r"\bIN\s+OUT\b", "INOUT", part, flags=re.IGNORECASE)
        part = re.sub(r"\bOUT\b", "OUT", part, flags=re.IGNORECASE)
        part = re.sub(r"\bIN\b", "IN", part, flags=re.IGNORECASE)
        part = re.sub(r"\bDEFAULT\s+\S+", "", part, flags=re.IGNORECASE)
        part = re.sub(r":=\s*\S+", "", part, flags=re.IGNORECASE)
        part = convert_oracle_type_in_param(part)
        pg_parts.append(part.strip())
    return "(" + ", ".join(pg_parts) + ")"


def convert_oracle_type_in_param(param: str) -> str:
    """Convert Oracle types in a parameter declaration."""
    param = re.sub(r"\bVARCHAR2\s*(\([^)]*\))?", "VARCHAR", param, flags=re.IGNORECASE)
    param = re.sub(r"\bNUMBER\s*(\([^)]*\))?", "NUMERIC", param, flags=re.IGNORECASE)
    param = re.sub(r"\bPLS_INTEGER\b", "INTEGER", param, flags=re.IGNORECASE)
    param = re.sub(r"\bBINARY_INTEGER\b", "INTEGER", param, flags=re.IGNORECASE)
    param = re.sub(r"\bCLOB\b", "TEXT", param, flags=re.IGNORECASE)
    param = re.sub(r"\bBLOB\b", "BYTEA", param, flags=re.IGNORECASE)
    param = re.sub(r"\bLONG\b", "TEXT", param, flags=re.IGNORECASE)
    param = re.sub(r"\bRAW\s*\(\s*\d+\s*\)", "BYTEA", param, flags=re.IGNORECASE)
    return param


def convert_oracle_type_to_pg(oracle_type: str) -> str:
    """Convert an Oracle return type to PostgreSQL equivalent."""
    t = oracle_type.strip().upper()
    mapping = {
        "VARCHAR2": "VARCHAR", "NUMBER": "NUMERIC", "PLS_INTEGER": "INTEGER",
        "BINARY_INTEGER": "INTEGER", "CLOB": "TEXT", "BLOB": "BYTEA",
        "LONG": "TEXT", "BOOLEAN": "BOOLEAN", "DATE": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP", "RAW": "BYTEA", "INTEGER": "INTEGER",
        "FLOAT": "DOUBLE PRECISION", "REAL": "REAL",
    }
    base = re.sub(r"\s*\(.*\)", "", t)
    return mapping.get(base, oracle_type if oracle_type else "TEXT")


def migrate_views(pg_conn, _ora_conn, views: List[Tuple[str, str, str]],
                  synonym_map: Dict[str, str], _schema_objects_cache: Dict,
                  stats: Dict, dry_run: bool = False, workers: int = 8) -> None:
    """Migrate Oracle views to PostgreSQL with full conversion pipeline."""
    log.info("=" * 72)
    log.info("PHASE 10: VIEW MIGRATION")
    log.info("=" * 72)
    log.info("  %d view(s) to migrate", len(views))
    stats["views_total"] = len(views)
    if not views:
        return

    view_list = [(s, v) for s, v, _ in views]
    ddl_map = {(s, v): ddl for s, v, ddl in views}
    view_keys_lower: Set[ViewKey] = {((s or "").lower(), (v or "").lower()) for s, v in view_list}
    view_keys_by_name: Dict[str, List[ViewKey]] = {}
    for (s, v) in view_keys_lower:
        view_keys_by_name.setdefault(v, []).append((s, v))

    log.info("  Converting %d view(s) with %d worker(s)...", len(view_list), workers)
    t0 = time.perf_counter()

    def convert_one(item):
        idx, schema, vname = item
        display = f"{schema}.{vname}" if schema else vname
        ddl = ddl_map.get((schema, vname), "")
        if not ddl:
            return (idx, schema, vname, display, "", "no_ddl", set())
        try:
            pg_sql = rewrite_sql_with_synonyms(ddl, synonym_map, view_schema=schema, view_name=vname)
        except Exception as e:
            log.debug("[VIEW %s] sqlglot rewrite failed: %s, falling back to normalize-only", display, e)
            pg_sql = ddl
        pg_sql = normalize_view_script(pg_sql, apply_oracle_conversions=True, orig_schema=schema, orig_view_name=vname)
        body = _body_from_create_view_ddl(pg_sql)
        refs = _extract_view_refs_from_body(body, view_keys_lower, view_keys_by_name)
        return (idx, schema, vname, display, pg_sql, None, refs)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(convert_one, (i, s, v)): i for i, (s, v) in enumerate(view_list)}
        results_by_i = [None] * len(view_list)
        done_count = 0
        for fut in as_completed(futures):
            res = fut.result()
            results_by_i[res[0]] = res
            done_count += 1
            if done_count % 500 == 0 or done_count == len(view_list):
                elapsed = time.perf_counter() - t0
                log.info("  [CONVERT] %d/%d (%.1f%%) in %.1fs",
                         done_count, len(view_list), 100 * done_count / len(view_list), elapsed)

    elapsed = time.perf_counter() - t0
    log.info("  Conversion complete in %.1fs", elapsed)

    deps_list = [r[6] for r in results_by_i]
    order = topological_sort(view_list, deps_list)
    if order != list(range(len(view_list))):
        log.info("  Views reordered by dependency")

    log.info("  Executing views on PostgreSQL...")
    schemas_ensured: Set[str] = set()
    for rank, idx in enumerate(order):
        _, schema, _vname, display, pg_sql, err_type, _ = results_by_i[idx]
        if err_type:
            stats["views_fail"] += 1
            stats["errors"].append(("view", display, err_type, "No DDL or conversion error"))
            log.debug("  [VIEW %d/%d] FAIL (%s): %s", rank + 1, len(order), err_type, display)
            continue
        if schema and schema.strip() and schema not in schemas_ensured:
            ensure_pg_schema(pg_conn, schema.strip().lower())
            schemas_ensured.add(schema)
        if dry_run:
            stats["views_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, pg_sql)
        if ok:
            stats["views_ok"] += 1
            if (rank + 1) % 200 == 0 or (rank + 1) == len(order):
                log.info("  [VIEW] Progress: %d/%d executed", rank + 1, len(order))
        else:
            stats["views_fail"] += 1
            stats["errors"].append(("view", display, "execute", err[:300]))
            log.debug("  [VIEW %d/%d] FAIL: %s - %s", rank + 1, len(order), display, err[:100])


def migrate_mviews(pg_conn, mviews: List[Dict], synonym_map: Dict[str, str],
                   stats: Dict, dry_run: bool = False) -> None:
    """Migrate Oracle materialized views to PostgreSQL."""
    log.info("=" * 72)
    log.info("PHASE 11: MATERIALIZED VIEW MIGRATION")
    log.info("=" * 72)
    log.info("  %d materialized view(s) to migrate", len(mviews))
    stats["mviews_total"] = len(mviews)
    if not mviews:
        return

    schemas_ensured: Set[str] = set()
    for i, mv in enumerate(mviews):
        owner = mv["owner"]
        mview_name = mv["mview_name"]
        query = mv.get("query", "")
        schema_l = owner.strip().lower()
        mview_name_l = mview_name.strip().lower()
        display = f"{schema_l}.{mview_name_l}"

        if schema_l and schema_l not in schemas_ensured:
            if not dry_run:
                ensure_pg_schema(pg_conn, schema_l)
            schemas_ensured.add(schema_l)

        if not query:
            stats["mviews_fail"] += 1
            stats["errors"].append(("mview", display, "", "No query text"))
            continue

        # Apply Oracle→PG conversion rules to the query
        try:
            pg_query = rewrite_sql_with_synonyms(
                f"CREATE OR REPLACE VIEW tmp_mv AS\n{query}",
                synonym_map, view_schema=schema_l, view_name=mview_name_l,
            )
            # Strip the "CREATE OR REPLACE VIEW tmp_mv AS" wrapper
            as_match = re.search(r"\bAS\s+", pg_query, re.IGNORECASE)
            if as_match:
                pg_query = pg_query[as_match.end():]
            pg_query = normalize_view_script(
                f"CREATE OR REPLACE VIEW {display} AS\n{pg_query}",
                apply_oracle_conversions=True,
            )
            as_match2 = re.search(r"\bAS\s+", pg_query, re.IGNORECASE)
            if as_match2:
                pg_query = pg_query[as_match2.end():]
        except Exception as e:
            log.debug("[MVIEW %s] conversion failed: %s, using original query", display, e)
            pg_query = query

        ddl = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {schema_l}.{mview_name_l} AS\n{pg_query}\nWITH NO DATA;"

        if dry_run:
            stats["mviews_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, ddl)
        if ok:
            stats["mviews_ok"] += 1
            if (i + 1) % 100 == 0 or (i + 1) == len(mviews):
                log.info("  [MVIEW] Progress: %d/%d", i + 1, len(mviews))
        else:
            stats["mviews_fail"] += 1
            stats["errors"].append(("mview", display, "", err[:300]))
            log.debug("  [MVIEW] FAIL %s: %s", display, err[:150])


def migrate_triggers(pg_conn, triggers: List[Dict], stats: Dict,
                     dry_run: bool = False) -> None:
    """Migrate Oracle triggers to PostgreSQL (trigger function + trigger).

    Oracle triggers are converted to:
      1. A PL/pgSQL trigger function
      2. A CREATE TRIGGER statement that calls the function
    """
    log.info("=" * 72)
    log.info("PHASE 12: TRIGGER MIGRATION")
    log.info("=" * 72)
    log.info("  %d trigger(s) to migrate", len(triggers))
    stats["triggers_total"] = len(triggers)
    if not triggers:
        return

    for i, trg in enumerate(triggers):
        owner = trg["owner"]
        trg_name = trg["trigger_name"]
        trg_type = trg.get("trigger_type", "")
        event = trg.get("triggering_event", "")
        tbl_name = trg.get("table_name", "")
        when_clause = trg.get("when_clause", "")
        body = trg.get("trigger_body", "")

        schema_l = owner.strip().lower()
        trg_name_l = trg_name.strip().lower()
        tbl_name_l = tbl_name.strip().lower()
        display = f"{schema_l}.{trg_name_l}"

        if not body:
            stats["triggers_fail"] += 1
            stats["errors"].append(("trigger", display, tbl_name_l, "No trigger body"))
            continue

        # Determine timing: BEFORE / AFTER / INSTEAD OF
        timing = "BEFORE"
        if "BEFORE" in trg_type.upper():
            timing = "BEFORE"
        elif "AFTER" in trg_type.upper():
            timing = "AFTER"
        elif "INSTEAD OF" in trg_type.upper():
            timing = "INSTEAD OF"

        # Determine row/statement level
        for_each = "FOR EACH ROW" if "EACH ROW" in trg_type.upper() else "FOR EACH STATEMENT"

        # Convert Oracle trigger events (INSERT OR UPDATE OR DELETE)
        pg_event = event.strip().upper()
        pg_event = re.sub(r"\s+OR\s+", " OR ", pg_event, flags=re.IGNORECASE)

        # Convert trigger body: basic Oracle→PG transformations
        pg_body = body
        pg_body = re.sub(r":NEW\.", "NEW.", pg_body, flags=re.IGNORECASE)
        pg_body = re.sub(r":OLD\.", "OLD.", pg_body, flags=re.IGNORECASE)
        pg_body = re.sub(r"\bVARCHAR2\b", "VARCHAR", pg_body, flags=re.IGNORECASE)
        pg_body = re.sub(r"\bNUMBER\b", "NUMERIC", pg_body, flags=re.IGNORECASE)
        pg_body = re.sub(r"\bSYSDATE\b", "CURRENT_TIMESTAMP", pg_body, flags=re.IGNORECASE)
        pg_body = re.sub(r"\bSYSTIMESTAMP\b", "CURRENT_TIMESTAMP", pg_body, flags=re.IGNORECASE)
        pg_body = re.sub(r"\bNVL\s*\(", "COALESCE(", pg_body, flags=re.IGNORECASE)
        pg_body = re.sub(r"\bRAISE_APPLICATION_ERROR\s*\(([^,]+),\s*", r"RAISE EXCEPTION ", pg_body, flags=re.IGNORECASE)
        # Remove DECLARE keyword if present (PG uses body directly)
        pg_body = re.sub(r"^\s*DECLARE\b", "", pg_body, flags=re.IGNORECASE).strip()

        # Build trigger function
        func_name = f"{schema_l}.{trg_name_l}_func"
        when_sql = ""
        if when_clause:
            pg_when = when_clause
            pg_when = re.sub(r"\bNEW\.", "NEW.", pg_when, flags=re.IGNORECASE)
            pg_when = re.sub(r"\bOLD\.", "OLD.", pg_when, flags=re.IGNORECASE)
            when_sql = f"\n  WHEN ({pg_when})"

        # Ensure body ends with RETURN NEW/OLD/NULL for row triggers
        return_stmt = "RETURN NEW;" if "EACH ROW" in for_each else "RETURN NULL;"
        if not re.search(r"\bRETURN\s+(NEW|OLD|NULL)\s*;", pg_body, re.IGNORECASE):
            pg_body = pg_body.rstrip().rstrip(";") + ";\n  " + return_stmt

        func_ddl = (
            f"CREATE OR REPLACE FUNCTION {func_name}()\n"
            f"RETURNS TRIGGER LANGUAGE plpgsql AS $$\n"
            f"-- Auto-converted from Oracle trigger {display}; manual review required\n"
            f"BEGIN\n"
            f"  {pg_body}\n"
            f"END;\n$$;"
        )

        trg_ddl = (
            f"CREATE OR REPLACE TRIGGER {trg_name_l}\n"
            f"  {timing} {pg_event} ON {schema_l}.{tbl_name_l}\n"
            f"  {for_each}{when_sql}\n"
            f"  EXECUTE FUNCTION {func_name}();"
        )

        full_ddl = func_ddl + "\n\n" + trg_ddl

        if dry_run:
            stats["triggers_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, full_ddl)
        if ok:
            stats["triggers_ok"] += 1
            if (i + 1) % 100 == 0 or (i + 1) == len(triggers):
                log.info("  [TRIGGER] Progress: %d/%d", i + 1, len(triggers))
        else:
            stats["triggers_fail"] += 1
            stats["errors"].append(("trigger", display, tbl_name_l, err[:300]))
            log.debug("  [TRIGGER] FAIL %s: %s", display, err[:150])


def migrate_grants(pg_conn, grants: List[Dict], stats: Dict,
                   dry_run: bool = False) -> None:
    """Migrate Oracle grants/privileges to PostgreSQL."""
    log.info("=" * 72)
    log.info("PHASE 13: GRANT/PRIVILEGE MIGRATION")
    log.info("=" * 72)
    log.info("  %d grant(s) to migrate", len(grants))
    stats["grants_total"] = len(grants)
    if not grants:
        return

    # First, collect unique grantees and try to create roles
    grantees = {g["grantee"].strip().lower() for g in grants if g.get("grantee")}
    for grantee in sorted(grantees):
        if not dry_run:
            role_ddl = f"DO $$ BEGIN CREATE ROLE {grantee}; EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
            execute_ddl_on_pg(pg_conn, role_ddl)

    for i, grant in enumerate(grants):
        grantee = grant["grantee"].strip().lower()
        obj_owner = grant.get("owner", "").strip().lower()
        obj_name = grant.get("table_name", "").strip().lower()
        privilege = grant.get("privilege", "").strip().upper()
        grantable = grant.get("grantable", "NO")
        display = f"{privilege} ON {obj_owner}.{obj_name} TO {grantee}"

        if not obj_owner or not obj_name or not privilege or not grantee:
            stats["grants_fail"] += 1
            stats["errors"].append(("grant", display, "", "Incomplete grant info"))
            continue

        grant_option = " WITH GRANT OPTION" if grantable == "YES" else ""
        ddl = f"GRANT {privilege} ON {obj_owner}.{obj_name} TO {grantee}{grant_option};"

        if dry_run:
            stats["grants_ok"] += 1
            continue
        ok, err = execute_ddl_on_pg(pg_conn, ddl)
        if ok:
            stats["grants_ok"] += 1
            if (i + 1) % 500 == 0 or (i + 1) == len(grants):
                log.info("  [GRANT] Progress: %d/%d", i + 1, len(grants))
        else:
            stats["grants_fail"] += 1
            stats["errors"].append(("grant", display, "", err[:300]))
            log.debug("  [GRANT] FAIL %s: %s", display, err[:150])


def migrate_partitions(pg_conn, partitions: List[Dict], stats: Dict,
                       dry_run: bool = False) -> None:
    """Add partitioning to already-created tables or annotate for manual handling.

    PostgreSQL native partitioning requires the parent table to be created with PARTITION BY.
    Since tables are already created, we generate ALTER/recreation DDL or annotate.
    Strategy: Create partition children for tables already created as partitioned, or log
    instructions for manual conversion.
    """
    log.info("=" * 72)
    log.info("PHASE 5: PARTITION MIGRATION")
    log.info("=" * 72)
    log.info("  %d partitioned table(s) to process", len(partitions))
    stats["partitions_total"] = len(partitions)
    if not partitions:
        return

    for i, part_info in enumerate(partitions):
        owner = part_info["owner"]
        tbl_name = part_info["table_name"]
        part_type = part_info.get("partitioning_type", "")
        key_columns = part_info.get("key_columns", [])
        partition_list = part_info.get("partitions", [])
        schema_l = owner.strip().lower()
        tbl_name_l = tbl_name.strip().lower()
        display = f"{schema_l}.{tbl_name_l}"

        if not key_columns:
            stats["partitions_fail"] += 1
            stats["errors"].append(("partition", display, part_type, "No partition key columns"))
            continue

        key_cols_str = ", ".join(c.lower() for c in key_columns)

        # Map Oracle partition type to PG
        pg_part_type = part_type.upper()
        if pg_part_type not in ("RANGE", "LIST", "HASH"):
            stats["partitions_fail"] += 1
            stats["errors"].append(("partition", display, part_type, f"Unsupported partition type: {part_type}"))
            log.debug("  [PART] Unsupported type %s for %s", part_type, display)
            continue

        # Drop and recreate approach: we need to know the full table DDL.
        # Since the table already exists, we'll create partition children.
        # First: recreate parent as partitioned table (requires drop + recreate).
        # For safety, we generate DDL as comments with instructions.
        comment_lines = [
            f"-- PARTITION MIGRATION for {display}",
            f"-- Oracle partition type: {part_type}, key: ({key_cols_str})",
            f"-- To convert to PG native partitioning, the table must be recreated:",
            f"--   1. Rename existing table: ALTER TABLE {display} RENAME TO {tbl_name_l}_old;",
            f"--   2. Create partitioned parent:",
            f"--      CREATE TABLE {display} (LIKE {schema_l}.{tbl_name_l}_old INCLUDING ALL)",
            f"--      PARTITION BY {pg_part_type} ({key_cols_str});",
        ]

        # Generate CREATE TABLE ... PARTITION OF for each partition
        for p in partition_list:
            p_name = p.get("partition_name", "").lower()
            high_value = p.get("high_value", "")

            if pg_part_type == "RANGE":
                comment_lines.append(
                    f"--   CREATE TABLE {schema_l}.{tbl_name_l}_{p_name} "
                    f"PARTITION OF {display} FOR VALUES FROM (...) TO ({high_value});"
                )
            elif pg_part_type == "LIST":
                comment_lines.append(
                    f"--   CREATE TABLE {schema_l}.{tbl_name_l}_{p_name} "
                    f"PARTITION OF {display} FOR VALUES IN ({high_value});"
                )
            elif pg_part_type == "HASH":
                comment_lines.append(
                    f"--   CREATE TABLE {schema_l}.{tbl_name_l}_{p_name} "
                    f"PARTITION OF {display} FOR VALUES WITH (MODULUS {len(partition_list)}, "
                    f"REMAINDER {p.get('position', 0) - 1});"
                )

        comment_lines.append(f"--   3. Migrate data: INSERT INTO {display} SELECT * FROM {schema_l}.{tbl_name_l}_old;")
        comment_lines.append(f"--   4. Drop old table: DROP TABLE {schema_l}.{tbl_name_l}_old;")

        ddl = "\n".join(comment_lines)
        # Execute as a comment (COMMENT ON TABLE) so it's tracked
        comment_ddl = (
            f"COMMENT ON TABLE {schema_l}.{tbl_name_l} IS "
            f"'Partitioned in Oracle ({part_type} on {key_cols_str}, {len(partition_list)} partitions). "
            f"Manual conversion required for PG native partitioning.';"
        )

        if dry_run:
            stats["partitions_ok"] += 1
            log.debug("  [DRY-RUN PARTITION] %s: %s on (%s)", display, part_type, key_cols_str)
            continue
        ok, err = execute_ddl_on_pg(pg_conn, comment_ddl)
        if ok:
            stats["partitions_ok"] += 1
            log.info("  [PART %d/%d] %s: %s on (%s) — %d partitions (annotated for manual conversion)",
                     i + 1, len(partitions), display, part_type, key_cols_str, len(partition_list))
        else:
            stats["partitions_fail"] += 1
            stats["errors"].append(("partition", display, part_type, err[:300]))
            log.debug("  [PART] FAIL %s: %s", display, err[:150])

    log.info("  NOTE: Partition DDL generated as comments/annotations.")
    log.info("  PostgreSQL native partitioning requires table recreation.")
    log.info("  Review migration report for per-table partition instructions.")


def _write_ddl_to_file(output_dir: str, phase_name: str, ddl_statements: List[Tuple[str, str]]) -> Path:
    """Write DDL statements to a SQL file for review. Returns path to file."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    file_path = out_path / f"{phase_name}.sql"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"-- Oracle-to-PostgreSQL DDL Migration: {phase_name}\n")
        f.write(f"-- Generated: {datetime.datetime.now().isoformat()}\n")
        f.write(f"-- Objects: {len(ddl_statements)}\n\n")
        for obj_name, ddl in ddl_statements:
            f.write(f"-- Object: {obj_name}\n")
            f.write(ddl.rstrip())
            f.write("\n\n")
    log.info("  [FILE] Wrote %d statements to %s", len(ddl_statements), file_path)
    return file_path


# #############################################################################
#  SECTION 7: Reporting
# #############################################################################

def write_report(log_dir: str, stats: Dict, elapsed: float,
                 oracle_dsn: str, pg_info: str) -> Path:
    """Write a comprehensive migration report."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = Path(log_dir) / f"migration_report_{timestamp}.txt"

    lines = []
    lines.append("=" * 80)
    lines.append("ORACLE-TO-POSTGRESQL DDL MIGRATION REPORT")
    lines.append("=" * 80)
    lines.append(f"Timestamp       : {datetime.datetime.now().isoformat()}")
    lines.append(f"Oracle source   : {oracle_dsn}")
    lines.append(f"PostgreSQL target: {pg_info}")
    lines.append(f"Total elapsed   : {elapsed:.2f}s ({elapsed/60:.1f} min)")
    lines.append("")

    lines.append("-" * 80)
    lines.append("SUMMARY  (13 phases, dependency-ordered)")
    lines.append("-" * 80)
    lines.append(f"  1.  Schemas       : {stats.get('schemas_ok', 0)} created, {stats.get('schemas_fail', 0)} failed")
    lines.append(f"  2.  Sequences     : {stats.get('sequences_ok', 0)} OK / {stats.get('sequences_total', 0)} total, {stats.get('sequences_fail', 0)} failed")
    lines.append(f"  3.  Types         : {stats.get('types_ok', 0)} OK / {stats.get('types_total', 0)} total, {stats.get('types_fail', 0)} failed")
    lines.append(f"  4.  Tables        : {stats.get('tables_ok', 0)} OK / {stats.get('tables_total', 0)} total, {stats.get('tables_fail', 0)} failed")
    lines.append(f"      Foreign keys  : {stats.get('table_fk_ok', 0)} OK / {stats.get('table_fk_total', 0)} total, {stats.get('table_fk_fail', 0)} failed")
    lines.append(f"  5.  Partitions    : {stats.get('partitions_ok', 0)} OK / {stats.get('partitions_total', 0)} total, {stats.get('partitions_fail', 0)} failed (annotate only)")
    lines.append(f"  6.  Indexes       : {stats.get('indexes_ok', 0)} OK / {stats.get('indexes_total', 0)} total, {stats.get('indexes_fail', 0)} failed")
    lines.append(f"  7.  Synonyms      : {stats.get('synonyms_ok', 0)} OK / {stats.get('synonyms_total', 0)} total, {stats.get('synonyms_fail', 0)} failed")
    lines.append(f"  8.  Functions     : {stats.get('functions_ok', 0)} OK / {stats.get('functions_total', 0)} total, {stats.get('functions_fail', 0)} failed")
    lines.append(f"  9.  Packages      : {stats.get('packages_ok', 0)} OK / {stats.get('packages_total', 0)} total, {stats.get('packages_fail', 0)} failed")
    lines.append(f"      Pkg functions : {stats.get('pkg_functions_ok', 0)} OK, {stats.get('pkg_functions_fail', 0)} failed, {stats.get('pkg_functions_stub', 0)} stub-only")
    lines.append(f"  10. Views         : {stats.get('views_ok', 0)} OK / {stats.get('views_total', 0)} total, {stats.get('views_fail', 0)} failed")
    lines.append(f"  11. Mat. Views    : {stats.get('mviews_ok', 0)} OK / {stats.get('mviews_total', 0)} total, {stats.get('mviews_fail', 0)} failed")
    lines.append(f"  12. Triggers      : {stats.get('triggers_ok', 0)} OK / {stats.get('triggers_total', 0)} total, {stats.get('triggers_fail', 0)} failed")
    lines.append(f"  13. Grants        : {stats.get('grants_ok', 0)} OK / {stats.get('grants_total', 0)} total, {stats.get('grants_fail', 0)} failed")
    lines.append("")

    ok_keys = ("schemas_ok", "sequences_ok", "types_ok", "tables_ok", "indexes_ok",
               "synonyms_ok", "functions_ok", "packages_ok", "views_ok",
               "mviews_ok", "triggers_ok", "grants_ok", "partitions_ok")
    fail_keys = ("schemas_fail", "sequences_fail", "types_fail", "tables_fail", "indexes_fail",
                 "synonyms_fail", "functions_fail", "packages_fail", "views_fail",
                 "mviews_fail", "triggers_fail", "grants_fail", "partitions_fail")
    total_ok = sum(stats.get(k, 0) for k in ok_keys)
    total_fail = sum(stats.get(k, 0) for k in fail_keys)
    lines.append(f"  TOTAL OK      : {total_ok}")
    lines.append(f"  TOTAL FAILED  : {total_fail}")
    lines.append("")

    errors = stats.get("errors", [])
    if errors:
        lines.append("-" * 80)
        lines.append(f"ERRORS ({len(errors)} total)")
        lines.append("-" * 80)
        lines.append("type\tobject\tdetail\terror_message")
        for err_type, obj, detail, msg in errors[:5000]:
            safe_msg = msg.replace("\n", " | ").replace("\t", " ")
            lines.append(f"{err_type}\t{obj}\t{detail}\t{safe_msg}")
        if len(errors) > 5000:
            lines.append(f"... and {len(errors) - 5000} more errors (truncated)")
        lines.append("")
    else:
        lines.append("No errors. All objects migrated successfully.")
        lines.append("")

    lines.append("=" * 80)
    lines.append("END OF REPORT")
    lines.append("=" * 80)

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def setup_logging(log_dir: str, log_level: str = "INFO") -> Path:
    """Configure dual logging: console (INFO+) and file (DEBUG+)."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"migration_{timestamp}.log"

    log.setLevel(logging.DEBUG)
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    log.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    ch.setFormatter(logging.Formatter("%(levelname)-8s  %(message)s"))
    log.addHandler(ch)

    log.info("Log file: %s", log_file)
    return log_file


# #############################################################################
#  SECTION 8: Main
# #############################################################################

def main() -> None:
    run_start = time.time()

    parser = argparse.ArgumentParser(
        description="Oracle-to-PostgreSQL Full DDL Migrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Migrates all DDL objects from Oracle to PostgreSQL in dependency order:
              1.Schemas  2.Sequences  3.Types  4.Tables  5.Partitions(annotate)
              6.Indexes  7.Synonyms→Views  8.Functions  9.Packages
              10.Views  11.Materialized Views  12.Triggers  13.Grants

            Examples:
              python oracle_to_pg_ddl_migrator.py \\
                  --oracle-user APPS --oracle-password pass --oracle-dsn host:1521/ORCL \\
                  --pg-host localhost --pg-port 5432 --pg-database mydb \\
                  --pg-user postgres --pg-password pass

              # Migrate specific schemas only:
              python oracle_to_pg_ddl_migrator.py ... --schemas APPS,HR,FIN

              # Dry-run (convert but don't execute on PG):
              python oracle_to_pg_ddl_migrator.py ... --no-execute

              # Generate SQL files for review (in addition to execution):
              python oracle_to_pg_ddl_migrator.py ... --output-dir migration_output/
        """),
    )
    parser.add_argument("--oracle-user", default=None, help="Oracle user (or ORACLE_USER env)")
    parser.add_argument("--oracle-password", default=None, help="Oracle password (or ORACLE_PASSWORD env)")
    parser.add_argument("--oracle-dsn", default=None, help="Oracle DSN e.g. host:1521/SID (or ORACLE_DSN env)")
    parser.add_argument("--pg-host", default=None, help="PostgreSQL host (or PG_HOST env)")
    parser.add_argument("--pg-port", type=int, default=None, help="PostgreSQL port (or PG_PORT env)")
    parser.add_argument("--pg-database", default=None, help="PostgreSQL database (or PG_DATABASE env)")
    parser.add_argument("--pg-user", default=None, help="PostgreSQL user (or PG_USER env)")
    parser.add_argument("--pg-password", default=None, help="PostgreSQL password (or PG_PASSWORD env)")
    parser.add_argument("--schemas", default=None, help="Comma-separated list of Oracle schemas to migrate (default: auto-discover)")
    parser.add_argument("--no-execute", action="store_true", help="Convert DDL but do not execute on PostgreSQL")
    parser.add_argument("--output-dir", default=None, help="Write generated SQL files to this directory for review")
    parser.add_argument("--skip-sequences", action="store_true", help="Skip sequence migration")
    parser.add_argument("--skip-types", action="store_true", help="Skip user-defined type migration")
    parser.add_argument("--skip-tables", action="store_true", help="Skip table migration")
    parser.add_argument("--skip-indexes", action="store_true", help="Skip index migration")
    parser.add_argument("--skip-synonyms", action="store_true", help="Skip synonym migration")
    parser.add_argument("--skip-functions", action="store_true", help="Skip function/procedure migration")
    parser.add_argument("--skip-packages", action="store_true", help="Skip package migration")
    parser.add_argument("--skip-views", action="store_true", help="Skip view migration")
    parser.add_argument("--skip-mviews", action="store_true", help="Skip materialized view migration")
    parser.add_argument("--skip-triggers", action="store_true", help="Skip trigger migration")
    parser.add_argument("--skip-grants", action="store_true", help="Skip grant/privilege migration")
    parser.add_argument("--skip-partitions", action="store_true", help="Skip partition annotation")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers for view conversion (default: 8)")
    parser.add_argument("--log-dir", default="migration_logs", help="Directory for logs and reports (default: migration_logs)")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args()

    log_file = setup_logging(args.log_dir, args.log_level)

    log.info("=" * 72)
    log.info("ORACLE-TO-POSTGRESQL DDL MIGRATOR — STARTED")
    log.info("=" * 72)

    ora_user = args.oracle_user or ORACLE_USER or os.environ.get("ORACLE_USER")
    ora_password = args.oracle_password or ORACLE_PASSWORD or os.environ.get("ORACLE_PASSWORD")
    ora_dsn = args.oracle_dsn or os.environ.get("ORACLE_DSN", ORACLE_DSN)
    pg_host_val = args.pg_host or os.environ.get("PG_HOST", PG_HOST)
    pg_port_val = args.pg_port if args.pg_port is not None else int(os.environ.get("PG_PORT", PG_PORT))
    pg_database_val = args.pg_database or os.environ.get("PG_DATABASE", PG_DATABASE)
    pg_user_val = args.pg_user or os.environ.get("PG_USER", PG_USER)
    pg_password_val = args.pg_password or os.environ.get("PG_PASSWORD", PG_PASSWORD)

    if not ora_user or not ora_password:
        log.error("Oracle credentials required. Use --oracle-user/--oracle-password or env vars.")
        sys.exit(1)

    log.info("Oracle: %s@%s", ora_user, ora_dsn)
    log.info("PostgreSQL: %s@%s:%d/%s", pg_user_val, pg_host_val, pg_port_val, pg_database_val)

    stats: Dict = defaultdict(int)
    stats["errors"] = []

    try:
        import oracledb  # noqa: F401 – runtime availability check
        _ = oracledb
    except ImportError:
        log.error("oracledb required: pip install oracledb")
        sys.exit(1)
    try:
        import sqlglot  # noqa: F401 – runtime availability check
        _ = sqlglot
    except ImportError:
        log.error("sqlglot required: pip install sqlglot")
        sys.exit(1)
    if not args.no_execute:
        try:
            import psycopg2  # noqa: F401 – runtime availability check
            _ = psycopg2
        except ImportError:
            log.error("psycopg2 required: pip install psycopg2-binary")
            sys.exit(1)

    log.info("Connecting to Oracle...")
    try:
        ora_conn = oracle_connect_with_retry(ora_user, ora_password, ora_dsn)
    except Exception as e:
        log.error("Oracle connection failed: %s", e)
        sys.exit(1)
    log.info("Oracle connected.")

    pg_conn = None
    if not args.no_execute:
        log.info("Connecting to PostgreSQL...")
        try:
            pg_conn = pg_connect(pg_host_val, pg_port_val, pg_database_val, pg_user_val, pg_password_val)
        except Exception as e:
            log.error("PostgreSQL connection failed: %s", e)
            ora_conn.close()
            sys.exit(1)
        log.info("PostgreSQL connected.")

    if args.output_dir:
        os.makedirs(args.output_dir, exist_ok=True)
        log.info("SQL output directory: %s", args.output_dir)

    try:
        schema_filter = [s.strip().upper() for s in args.schemas.split(",")] if args.schemas else None
        schemas = discover_schemas(ora_conn, schema_filter)
        if not schemas:
            log.warning("No schemas found to migrate.")
        schemas_upper_set = {s.upper() for s in schemas}

        synonym_map = get_synonym_map(ora_conn)

        log.info("[CACHE] Building schema object cache...")
        schema_objects_cache: Dict[str, Dict[str, str]] = {}
        for s in schemas:
            try:
                schema_objects_cache[s.upper()] = get_oracle_schema_objects(ora_conn, s)
                log.debug("  Schema %s: %d objects", s, len(schema_objects_cache[s.upper()]))
            except Exception as e:
                log.warning("  Schema %s cache failed: %s", s, e)

        # =================================================================
        # DEPENDENCY-ORDERED MIGRATION PHASES
        #
        #  1. Schemas           — no deps
        #  2. Sequences         — depends on schemas
        #  3. User-Defined Types— depends on schemas
        #  4. Tables            — depends on schemas, sequences, types
        #       (FKs added in 2nd pass after all tables created)
        #  5. Partitions        — depends on tables (annotation only)
        #  6. Indexes           — depends on tables
        #  7. Synonyms → Views  — depends on tables
        #  8. Functions/Procs   — depends on schemas, types, tables
        #  9. Packages          — depends on schemas, types
        # 10. Views             — depends on tables, functions, synonyms
        # 11. Materialized Views— depends on tables, views, functions
        # 12. Triggers          — depends on tables, functions
        # 13. Grants            — depends on all objects
        # =================================================================

        # --- PHASE 1: Schemas ---
        if pg_conn and schemas:
            migrate_schemas(pg_conn, schemas, stats)
        elif args.no_execute:
            stats["schemas_ok"] = len(schemas)
            log.info("[DRY-RUN] Would create %d schema(s)", len(schemas))

        # --- PHASE 2: Sequences ---
        if not args.skip_sequences:
            sequences = discover_sequences(ora_conn, schemas)
            if pg_conn:
                migrate_sequences(pg_conn, sequences, stats, dry_run=args.no_execute)
            else:
                stats["sequences_total"] = len(sequences)
                log.info("[DRY-RUN] Would migrate %d sequence(s)", len(sequences))
        else:
            log.info("Skipping sequence migration (--skip-sequences)")

        # --- PHASE 3: User-Defined Types ---
        if not args.skip_types:
            user_types = discover_types(ora_conn, schemas)
            if pg_conn:
                migrate_types(pg_conn, user_types, stats, dry_run=args.no_execute)
            else:
                stats["types_total"] = len(user_types)
                log.info("[DRY-RUN] Would migrate %d type(s)", len(user_types))
        else:
            log.info("Skipping type migration (--skip-types)")

        # --- PHASE 4: Tables (columns + PK + UK + CHECK + defaults, then FKs in 2nd pass) ---
        if not args.skip_tables:
            tables = discover_tables(ora_conn, schemas)
            if pg_conn:
                migrate_tables(pg_conn, tables, stats, dry_run=args.no_execute)
            else:
                stats["tables_total"] = len(tables)
                log.info("[DRY-RUN] Would migrate %d table(s)", len(tables))
        else:
            log.info("Skipping table migration (--skip-tables)")

        # --- PHASE 5: Partitions (annotation/instructions, requires tables) ---
        if not args.skip_partitions:
            partition_info = discover_partitions(ora_conn, schemas)
            if pg_conn:
                migrate_partitions(pg_conn, partition_info, stats, dry_run=args.no_execute)
            else:
                stats["partitions_total"] = len(partition_info)
                log.info("[DRY-RUN] Would annotate %d partitioned table(s)", len(partition_info))
        else:
            log.info("Skipping partition annotation (--skip-partitions)")

        # --- PHASE 6: Indexes (requires tables) ---
        if not args.skip_indexes:
            indexes = discover_indexes(ora_conn, schemas)
            if pg_conn:
                migrate_indexes(pg_conn, indexes, stats, dry_run=args.no_execute)
            else:
                stats["indexes_total"] = len(indexes)
                log.info("[DRY-RUN] Would migrate %d index(es)", len(indexes))
        else:
            log.info("Skipping index migration (--skip-indexes)")

        # --- PHASE 7: Synonyms → Views ---
        if not args.skip_synonyms:
            synonyms = discover_synonyms(ora_conn, schemas if schema_filter else None)
            if pg_conn:
                migrate_synonyms_as_views(pg_conn, synonyms, schemas_upper_set if schema_filter else None, stats, dry_run=args.no_execute)
            else:
                stats["synonyms_total"] = len(synonyms)
                log.info("[DRY-RUN] Would migrate %d synonym(s)", len(synonyms))
        else:
            log.info("Skipping synonym migration (--skip-synonyms)")

        # --- PHASE 8: Functions/Procedures ---
        if not args.skip_functions:
            functions = discover_functions(ora_conn, schemas)
            if pg_conn:
                migrate_functions(pg_conn, functions, stats, dry_run=args.no_execute)
            else:
                stats["functions_total"] = len(functions)
                log.info("[DRY-RUN] Would migrate %d function(s)", len(functions))
        else:
            log.info("Skipping function migration (--skip-functions)")

        # --- PHASE 9: Packages ---
        if not args.skip_packages:
            packages = discover_packages(ora_conn, schemas)
            if pg_conn:
                migrate_packages(pg_conn, packages, stats, dry_run=args.no_execute)
            else:
                stats["packages_total"] = len(packages)
                log.info("[DRY-RUN] Would migrate %d package(s)", len(packages))
        else:
            log.info("Skipping package migration (--skip-packages)")

        # --- PHASE 10: Views ---
        if not args.skip_views:
            views = discover_views(ora_conn, schemas)
            if pg_conn:
                migrate_views(pg_conn, ora_conn, views, synonym_map, schema_objects_cache, stats,
                              dry_run=args.no_execute, workers=args.workers)
            else:
                stats["views_total"] = len(views)
                log.info("[DRY-RUN] Would migrate %d view(s)", len(views))
        else:
            log.info("Skipping view migration (--skip-views)")

        # --- PHASE 11: Materialized Views ---
        if not args.skip_mviews:
            mviews = discover_mviews(ora_conn, schemas)
            if pg_conn:
                migrate_mviews(pg_conn, mviews, synonym_map, stats, dry_run=args.no_execute)
            else:
                stats["mviews_total"] = len(mviews)
                log.info("[DRY-RUN] Would migrate %d materialized view(s)", len(mviews))
        else:
            log.info("Skipping materialized view migration (--skip-mviews)")

        # --- PHASE 12: Triggers ---
        if not args.skip_triggers:
            triggers = discover_triggers(ora_conn, schemas)
            if pg_conn:
                migrate_triggers(pg_conn, triggers, stats, dry_run=args.no_execute)
            else:
                stats["triggers_total"] = len(triggers)
                log.info("[DRY-RUN] Would migrate %d trigger(s)", len(triggers))
        else:
            log.info("Skipping trigger migration (--skip-triggers)")

        # --- PHASE 13: Grants/Privileges (last — all objects must exist) ---
        if not args.skip_grants:
            grants = discover_grants(ora_conn, schemas)
            if pg_conn:
                migrate_grants(pg_conn, grants, stats, dry_run=args.no_execute)
            else:
                stats["grants_total"] = len(grants)
                log.info("[DRY-RUN] Would migrate %d grant(s)", len(grants))
        else:
            log.info("Skipping grant migration (--skip-grants)")

    except Exception as e:
        log.error("FATAL ERROR: %s", e)
        log.debug("Traceback:\n%s", traceback.format_exc())
        stats["errors"].append(("fatal", "", "", str(e)))
    finally:
        try:
            ora_conn.close()
        except Exception:
            pass
        if pg_conn:
            try:
                pg_conn.close()
            except Exception:
                pass

    elapsed = time.time() - run_start

    log.info("")
    log.info("=" * 72)
    log.info("MIGRATION SUMMARY  (13 phases, dependency-ordered)")
    log.info("=" * 72)
    log.info("  1.  Schemas     : %d OK, %d fail", stats.get("schemas_ok", 0), stats.get("schemas_fail", 0))
    log.info("  2.  Sequences   : %d/%d OK, %d fail", stats.get("sequences_ok", 0), stats.get("sequences_total", 0), stats.get("sequences_fail", 0))
    log.info("  3.  Types       : %d/%d OK, %d fail", stats.get("types_ok", 0), stats.get("types_total", 0), stats.get("types_fail", 0))
    log.info("  4.  Tables      : %d/%d OK, %d fail (FK: %d OK, %d fail)",
             stats.get("tables_ok", 0), stats.get("tables_total", 0), stats.get("tables_fail", 0),
             stats.get("table_fk_ok", 0), stats.get("table_fk_fail", 0))
    log.info("  5.  Partitions  : %d/%d OK, %d fail", stats.get("partitions_ok", 0), stats.get("partitions_total", 0), stats.get("partitions_fail", 0))
    log.info("  6.  Indexes     : %d/%d OK, %d fail", stats.get("indexes_ok", 0), stats.get("indexes_total", 0), stats.get("indexes_fail", 0))
    log.info("  7.  Synonyms    : %d/%d OK, %d fail", stats.get("synonyms_ok", 0), stats.get("synonyms_total", 0), stats.get("synonyms_fail", 0))
    log.info("  8.  Functions   : %d/%d OK, %d fail", stats.get("functions_ok", 0), stats.get("functions_total", 0), stats.get("functions_fail", 0))
    log.info("  9.  Packages    : %d/%d OK, %d fail", stats.get("packages_ok", 0), stats.get("packages_total", 0), stats.get("packages_fail", 0))
    log.info("  10. Views       : %d/%d OK, %d fail", stats.get("views_ok", 0), stats.get("views_total", 0), stats.get("views_fail", 0))
    log.info("  11. Mat. Views  : %d/%d OK, %d fail", stats.get("mviews_ok", 0), stats.get("mviews_total", 0), stats.get("mviews_fail", 0))
    log.info("  12. Triggers    : %d/%d OK, %d fail", stats.get("triggers_ok", 0), stats.get("triggers_total", 0), stats.get("triggers_fail", 0))
    log.info("  13. Grants      : %d/%d OK, %d fail", stats.get("grants_ok", 0), stats.get("grants_total", 0), stats.get("grants_fail", 0))
    log.info("  -------")
    log.info("  Elapsed    : %.2fs (%.1f min)", elapsed, elapsed / 60)
    log.info("  Errors     : %d total", len(stats.get("errors", [])))

    pg_info = f"{pg_host_val}:{pg_port_val}/{pg_database_val}"
    report_path = write_report(args.log_dir, stats, elapsed, ora_dsn, pg_info)
    log.info("")
    log.info("  Log file   : %s", log_file)
    log.info("  Report     : %s", report_path)
    if args.output_dir:
        log.info("  SQL output : %s", args.output_dir)
    log.info("=" * 72)

    total_fail = sum(stats.get(k, 0) for k in (
        "schemas_fail", "sequences_fail", "types_fail", "tables_fail",
        "indexes_fail", "synonyms_fail", "functions_fail", "packages_fail",
        "views_fail", "mviews_fail", "triggers_fail", "grants_fail", "partitions_fail",
    ))
    if total_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

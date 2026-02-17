#!/usr/bin/env python3
"""
PostgreSQL View Migrator — migrate a list of views from one PG server to another.

Features:
  - Reads view definitions from the SOURCE PostgreSQL server.
  - Resolves dependency order (topological sort) so views that depend on other
    views are created AFTER their dependencies.
  - Creates views on the TARGET PostgreSQL server in the correct order.
  - Handles schema-qualified and unqualified view names.
  - Dry-run mode to inspect SQL without executing.
  - Extensive logging to console and log file.
  - Error log text file written at the end.

Requirements:
  pip install psycopg2-binary

Usage:
  python pg_view_migrator.py views.txt                 # read view list from file
  python pg_view_migrator.py views.txt --dry-run       # print SQL without executing
  python pg_view_migrator.py views.txt --drop-existing # DROP existing views on target before CREATE

Input file format (one view per line):
  public.vw_active_users
  public.vw_order_summary
  reporting.vw_monthly_revenue
  # lines starting with '#' and blank lines are ignored
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
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# =============================================================================
# SOURCE PostgreSQL credentials (the server you are copying FROM)
# =============================================================================
SOURCE_PG_HOST = "localhost"
SOURCE_PG_PORT = 5432
SOURCE_PG_DATABASE = "source_db"
SOURCE_PG_USER = "postgres"
SOURCE_PG_PASSWORD = "postgres"
SOURCE_PG_SCHEMA = "public"          # default schema for unqualified names

# =============================================================================
# TARGET PostgreSQL credentials (the server you are copying TO)
# =============================================================================
TARGET_PG_HOST = "localhost"
TARGET_PG_PORT = 5433
TARGET_PG_DATABASE = "target_db"
TARGET_PG_USER = "postgres"
TARGET_PG_PASSWORD = "postgres"
TARGET_PG_SCHEMA = "public"          # default schema for unqualified names

# =============================================================================
# Input file path for view list (overridden by CLI positional argument).
# Set to None to require the CLI argument.
# =============================================================================
DEFAULT_VIEW_LIST_FILE: Optional[str] = None   # e.g. "views.txt"

# =============================================================================
# Logging / output configuration
# =============================================================================
LOG_DIR = "logs"                     # directory for log files
LOG_LEVEL = logging.DEBUG            # file log level (DEBUG captures everything)
CONSOLE_LOG_LEVEL = logging.INFO     # console log level

# Module-level logger; configured in _setup_logging()
log = logging.getLogger("pg_view_migrator")

# =============================================================================
# Internals — no need to touch below this line for normal usage.
# =============================================================================


def _setup_logging(log_dir: str) -> Path:
    """
    Configure logging to both console (INFO+) and a timestamped log file (DEBUG+).
    Returns the Path to the log file.
    """
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"pg_view_migrator_{timestamp}.log"

    log.setLevel(logging.DEBUG)

    # File handler — captures everything (DEBUG and above)
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fh.setLevel(LOG_LEVEL)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    log.addHandler(fh)

    # Console handler — INFO and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(CONSOLE_LOG_LEVEL)
    ch.setFormatter(logging.Formatter("%(levelname)-8s  %(message)s"))
    log.addHandler(ch)

    log.info("Log file: %s", log_file)
    return log_file


def _write_error_log(
    log_dir: str,
    skipped: List[Tuple[str, str, str]],
    failures: List[Tuple[str, str, str, str]],
    total_requested: int,
    total_attempted: int,
    n_ok: int,
    n_fail: int,
    elapsed_secs: float,
    input_file: str,
) -> Path:
    """
    Write a tab-separated error log file.  Always written; contains a header
    section with run metadata and then per-view error rows.
    Returns the Path to the error log file.
    """
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    error_log_file = Path(log_dir) / f"pg_view_migrator_errors_{timestamp}.txt"

    lines: List[str] = []
    lines.append("=" * 80)
    lines.append("PG VIEW MIGRATOR — ERROR / STATUS LOG")
    lines.append("=" * 80)
    lines.append(f"Timestamp       : {datetime.datetime.now().isoformat()}")
    lines.append(f"Input file      : {input_file}")
    lines.append(f"Source server   : {SOURCE_PG_HOST}:{SOURCE_PG_PORT}/{SOURCE_PG_DATABASE}")
    lines.append(f"Target server   : {TARGET_PG_HOST}:{TARGET_PG_PORT}/{TARGET_PG_DATABASE}")
    lines.append(f"Total requested : {total_requested}")
    lines.append(f"Skipped (source): {len(skipped)}")
    lines.append(f"Attempted       : {total_attempted}")
    lines.append(f"Succeeded       : {n_ok}")
    lines.append(f"Failed          : {n_fail}")
    lines.append(f"Elapsed         : {elapsed_secs:.2f}s")
    lines.append("")

    if skipped:
        lines.append("-" * 80)
        lines.append("SKIPPED VIEWS (not found on source)")
        lines.append("-" * 80)
        lines.append("schema\tview_name\treason")
        for s, v, reason in skipped:
            lines.append(f"{s}\t{v}\t{reason}")
        lines.append("")

    if failures:
        lines.append("-" * 80)
        lines.append("FAILED VIEWS (execution errors on target)")
        lines.append("-" * 80)
        lines.append("schema\tview_name\terror_phase\terror_message")
        for s, v, phase, err in failures:
            safe_err = err.replace("\n", " | ").replace("\t", " ")
            lines.append(f"{s}\t{v}\t{phase}\t{safe_err}")
        lines.append("")

    if not skipped and not failures:
        lines.append("No errors. All views migrated successfully.")
        lines.append("")

    lines.append("=" * 80)
    lines.append("END OF ERROR LOG")
    lines.append("=" * 80)

    error_log_file.write_text("\n".join(lines), encoding="utf-8")
    return error_log_file


def _load_views_from_file(filepath: str) -> List[str]:
    """
    Read view names from a text file, one per line.
    - Blank lines are skipped.
    - Lines starting with '#' are treated as comments and skipped.
    - Inline comments after '#' are stripped.
    - Leading/trailing whitespace is stripped.
    """
    log.info("Reading view list from: %s", filepath)
    views: List[str] = []
    skipped_lines = 0
    with open(filepath, "r", encoding="utf-8") as fh:
        for lineno, raw_line in enumerate(fh, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                skipped_lines += 1
                log.debug("  Line %d: skipped (blank/comment): %r", lineno, raw_line.rstrip())
                continue
            # strip inline comment
            if "#" in line:
                line = line[:line.index("#")].strip()
            if not line:
                skipped_lines += 1
                continue
            log.debug("  Line %d: view name = %r", lineno, line)
            views.append(line)
    log.info("Parsed %d view name(s) from file (%d lines skipped)", len(views), skipped_lines)
    if not views:
        log.error("No view names found in '%s'. Exiting.", filepath)
        sys.exit(1)
    return views


def _parse_schema_view(name: str, default_schema: str) -> Tuple[str, str]:
    """Split 'schema.view' into (schema, view). Use *default_schema* when unqualified."""
    parts = name.strip().split(".", 1)
    if len(parts) == 2:
        return parts[0].strip().lower(), parts[1].strip().lower()
    return default_schema.lower(), parts[0].strip().lower()


def _connect(host: str, port: int, database: str, user: str, password: str):
    """Return a psycopg2 connection with autocommit enabled."""
    import psycopg2
    log.debug("Opening connection to %s:%d/%s as user=%s", host, port, database, user)
    t0 = time.time()
    conn = psycopg2.connect(
        host=host, port=port, database=database,
        user=user, password=password,
    )
    conn.autocommit = True
    elapsed = time.time() - t0
    log.debug("Connection established in %.3fs", elapsed)
    return conn


def _connect_source():
    log.info("Connecting to SOURCE: %s:%d/%s (user=%s)",
             SOURCE_PG_HOST, SOURCE_PG_PORT, SOURCE_PG_DATABASE, SOURCE_PG_USER)
    return _connect(
        SOURCE_PG_HOST, SOURCE_PG_PORT, SOURCE_PG_DATABASE,
        SOURCE_PG_USER, SOURCE_PG_PASSWORD,
    )


def _connect_target():
    log.info("Connecting to TARGET: %s:%d/%s (user=%s)",
             TARGET_PG_HOST, TARGET_PG_PORT, TARGET_PG_DATABASE, TARGET_PG_USER)
    return _connect(
        TARGET_PG_HOST, TARGET_PG_PORT, TARGET_PG_DATABASE,
        TARGET_PG_USER, TARGET_PG_PASSWORD,
    )


# ---- Fetch view definitions from source ----

def fetch_view_definition(conn, schema: str, view_name: str) -> Optional[str]:
    """
    Retrieve the CREATE OR REPLACE VIEW statement for *schema.view_name* from
    the source database.  Returns None if the view does not exist.
    """
    sql = """
        SELECT pg_get_viewdef(c.oid, true) AS view_def
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = %s
           AND c.relname = %s
           AND c.relkind IN ('v', 'm')   -- regular view or materialized view
    """
    log.debug("Fetching view definition for %s.%s", schema, view_name)
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql, (schema, view_name))
        row = cur.fetchone()
    elapsed = time.time() - t0
    if row is None:
        log.warning("View %s.%s NOT FOUND on source (%.3fs)", schema, view_name, elapsed)
        return None
    log.debug("Fetched %s.%s definition: %d chars (%.3fs)", schema, view_name, len(row[0]), elapsed)
    return row[0]


def fetch_view_columns(conn, schema: str, view_name: str) -> List[str]:
    """Return the ordered column names for a view (useful for explicit column list)."""
    sql = """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = %s AND table_name = %s
         ORDER BY ordinal_position
    """
    log.debug("Fetching columns for %s.%s", schema, view_name)
    with conn.cursor() as cur:
        cur.execute(sql, (schema, view_name))
        cols = [r[0] for r in cur.fetchall()]
    log.debug("  %s.%s has %d column(s): %s", schema, view_name, len(cols),
              ", ".join(cols[:10]) + ("..." if len(cols) > 10 else ""))
    return cols


def is_materialized_view(conn, schema: str, view_name: str) -> bool:
    """Check whether the relation is a materialized view."""
    sql = """
        SELECT 1
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'm'
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, view_name))
        result = cur.fetchone() is not None
    log.debug("  %s.%s is_materialized=%s", schema, view_name, result)
    return result


def build_create_statement(
    schema: str,
    view_name: str,
    view_def: str,
    columns: List[str],
    materialized: bool,
) -> str:
    """
    Build a full CREATE OR REPLACE VIEW (or CREATE MATERIALIZED VIEW) DDL.
    """
    qualified = f"{schema}.{view_name}"
    if materialized:
        # Materialized views don't support CREATE OR REPLACE in PG < 17
        col_list = ", ".join(columns) if columns else ""
        col_clause = f" ({col_list})" if col_list else ""
        ddl = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {qualified}{col_clause} AS\n{view_def};"
    else:
        col_list = ", ".join(columns) if columns else ""
        col_clause = f" ({col_list})" if col_list else ""
        ddl = f"CREATE OR REPLACE VIEW {qualified}{col_clause} AS\n{view_def};"
    log.debug("Built DDL for %s (%d chars, materialized=%s)", qualified, len(ddl), materialized)
    return ddl


# ---- Dependency resolution ----

def _extract_referenced_relations(view_def: str) -> Set[Tuple[str, str]]:
    """
    Very lightweight parser to find schema-qualified identifiers referenced in
    a view body.  Returns set of (schema, relation) tuples found.

    This intentionally casts a wide net: it finds identifiers of the form
        "schema"."name"  |  schema.name  |  schema."name"  |  "schema".name
    and returns them lowered.
    """
    refs: Set[Tuple[str, str]] = set()

    # Pattern: optional-quote schema optional-quote . optional-quote name optional-quote
    pattern = re.compile(
        r'"?(\w+)"?\s*\.\s*"?(\w+)"?',
        re.IGNORECASE,
    )
    for m in pattern.finditer(view_def):
        schema = m.group(1).lower()
        name = m.group(2).lower()
        # Skip common noise: pg_catalog, information_schema, function-call artifacts
        if schema in ("pg_catalog", "information_schema"):
            continue
        refs.add((schema, name))

    return refs


def resolve_dependency_order(
    view_defs: Dict[Tuple[str, str], str],
    all_views: Set[Tuple[str, str]],
) -> List[Tuple[str, str]]:
    """
    Topological sort of views based on their mutual dependencies.

    Parameters
    ----------
    view_defs : dict mapping (schema, view_name) -> view body SQL
    all_views : the full set of (schema, view_name) we want to migrate

    Returns
    -------
    Ordered list of (schema, view_name) — dependencies first.

    Raises
    ------
    RuntimeError if a dependency cycle is detected.
    """
    log.info("Resolving dependency order for %d view(s) ...", len(all_views))

    # adjacency: view -> set of views it depends on (that are in our list)
    deps: Dict[Tuple[str, str], Set[Tuple[str, str]]] = defaultdict(set)

    for view_key, body in view_defs.items():
        referenced = _extract_referenced_relations(body)
        for ref in referenced:
            if ref in all_views and ref != view_key:
                deps[view_key].add(ref)
                log.debug("  Dependency: %s.%s -> %s.%s", view_key[0], view_key[1], ref[0], ref[1])

    # Log views with no internal dependencies
    no_deps = [v for v in all_views if v not in deps or not deps[v]]
    log.debug("  %d view(s) have no dependencies within the migration set", len(no_deps))

    # Kahn's algorithm for topological sort
    in_degree: Dict[Tuple[str, str], int] = {v: 0 for v in all_views}
    reverse_adj: Dict[Tuple[str, str], Set[Tuple[str, str]]] = defaultdict(set)

    for view_key, dep_set in deps.items():
        for dep in dep_set:
            reverse_adj[dep].add(view_key)
            in_degree[view_key] = in_degree.get(view_key, 0)  # ensure key
            in_degree[dep] = in_degree.get(dep, 0)

    # Seed: views with zero in-degree (no dependencies within our list)
    q: deque[Tuple[str, str]] = deque()
    for v in all_views:
        if in_degree.get(v, 0) == 0:
            q.append(v)

    ordered: List[Tuple[str, str]] = []
    while q:
        v = q.popleft()
        ordered.append(v)
        for dependent in reverse_adj.get(v, set()):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                q.append(dependent)

    if len(ordered) != len(all_views):
        # Cycle detection — report the cycle members
        remaining = all_views - set(ordered)
        cycle_names = ", ".join(f"{s}.{n}" for s, n in sorted(remaining))
        log.error("Dependency cycle detected among views: %s", cycle_names)
        raise RuntimeError(
            f"Dependency cycle detected among views: {cycle_names}. "
            "Please resolve circular references before migrating."
        )

    log.info("Dependency order resolved successfully. Creation order:")
    for idx, (s, v) in enumerate(ordered, 1):
        dep_list = ", ".join(f"{ds}.{dv}" for ds, dv in sorted(deps.get((s, v), set())))
        dep_msg = f"  (depends on: {dep_list})" if dep_list else "  (no internal deps)"
        log.info("  %4d. %s.%s%s", idx, s, v, dep_msg)

    return ordered


# ---- Target-side helpers ----

def drop_view_on_target(conn, schema: str, view_name: str, materialized: bool) -> None:
    qualified = f"{schema}.{view_name}"
    if materialized:
        ddl = f"DROP MATERIALIZED VIEW IF EXISTS {qualified} CASCADE;"
    else:
        ddl = f"DROP VIEW IF EXISTS {qualified} CASCADE;"
    log.debug("Dropping existing view: %s", ddl)
    with conn.cursor() as cur:
        cur.execute(ddl)
    log.debug("  Dropped: %s", qualified)


def ensure_schema_exists(conn, schema: str) -> None:
    log.debug("Ensuring schema exists: %s", schema)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


def execute_ddl(conn, ddl: str) -> Tuple[bool, Optional[str]]:
    """Execute DDL on the target. Returns (success, error_message)."""
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        return True, None
    except Exception as exc:
        return False, str(exc).strip()


# ---- Main ----

def main() -> None:
    run_start = time.time()

    parser = argparse.ArgumentParser(
        description="Migrate PostgreSQL views from one server to another.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Provide a text file with one view name per line (schema.view_name or
            just view_name).  Blank lines and lines starting with '#' are ignored.

            Example views.txt:
              public.vw_active_users
              public.vw_order_summary
              reporting.vw_monthly_revenue
        """),
    )
    parser.add_argument(
        "view_list_file", nargs="?", default=None,
        help="Path to a text file containing view names (one per line). "
             "Falls back to DEFAULT_VIEW_LIST_FILE if not provided.",
    )
    parser.add_argument(
        "--no-execute", action="store_true",
        help="Skip target execution entirely. Fetches definitions from source, "
             "resolves dependency order, and writes DDL (use with --output-file). "
             "No connection to the target server is made.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the CREATE VIEW SQL statements without executing on target.",
    )
    parser.add_argument(
        "--drop-existing", action="store_true",
        help="DROP existing views on the target before creating them.",
    )
    parser.add_argument(
        "--continue-on-error", action="store_true",
        help="Continue migrating remaining views even if one fails.",
    )
    parser.add_argument(
        "--output-file", type=str, default=None,
        help="Write all generated DDL to this file (in dependency order).",
    )
    parser.add_argument(
        "--log-dir", type=str, default=LOG_DIR,
        help=f"Directory for log and error files (default: {LOG_DIR}).",
    )
    args = parser.parse_args()

    # --- Setup logging ---
    log_file = _setup_logging(args.log_dir)

    log.info("=" * 72)
    log.info("PG VIEW MIGRATOR — run started")
    log.info("=" * 72)
    log.info("Arguments: %s", vars(args))
    log.info("Source: %s:%d/%s  (user=%s, default_schema=%s)",
             SOURCE_PG_HOST, SOURCE_PG_PORT, SOURCE_PG_DATABASE, SOURCE_PG_USER, SOURCE_PG_SCHEMA)
    log.info("Target: %s:%d/%s  (user=%s, default_schema=%s)",
             TARGET_PG_HOST, TARGET_PG_PORT, TARGET_PG_DATABASE, TARGET_PG_USER, TARGET_PG_SCHEMA)

    # --- Determine input file ---
    input_file = args.view_list_file or DEFAULT_VIEW_LIST_FILE
    if not input_file:
        log.error("No view list file provided.")
        parser.error(
            "A view list file is required. Provide it as a positional argument:\n"
            "  python pg_view_migrator.py views.txt [options]\n"
            "Or set DEFAULT_VIEW_LIST_FILE at the top of the script."
        )

    if not os.path.isfile(input_file):
        log.error("Input file not found: %s", input_file)
        sys.exit(1)

    log.info("Input file: %s", input_file)

    # --- Load and parse view names from file ---
    raw_views = _load_views_from_file(input_file)

    parsed_views: List[Tuple[str, str]] = []
    for raw_name in raw_views:
        schema, vname = _parse_schema_view(raw_name, SOURCE_PG_SCHEMA)
        parsed_views.append((schema, vname))
        log.debug("  Parsed: %r -> schema=%s, view=%s", raw_name, schema, vname)

    all_views_set: Set[Tuple[str, str]] = set(parsed_views)
    if len(all_views_set) != len(parsed_views):
        dup_count = len(parsed_views) - len(all_views_set)
        log.warning("Duplicate view names detected in input file; %d duplicate(s) removed.", dup_count)
        parsed_views = list(all_views_set)

    log.info("Total unique views to migrate: %d", len(parsed_views))

    # --- Connect to source and fetch definitions ---
    try:
        src_conn = _connect_source()
        log.info("Source connection established.")
    except Exception as exc:
        log.error("Failed to connect to SOURCE: %s", exc)
        log.debug("Traceback:\n%s", traceback.format_exc())
        sys.exit(1)

    view_defs: Dict[Tuple[str, str], str] = {}
    view_columns: Dict[Tuple[str, str], List[str]] = {}
    view_is_mat: Dict[Tuple[str, str], bool] = {}
    skipped: List[Tuple[str, str, str]] = []   # (schema, name, reason)

    log.info("Fetching view definitions from source (%d views) ...", len(parsed_views))
    fetch_start = time.time()
    for i, (schema, vname) in enumerate(parsed_views, 1):
        display = f"{schema}.{vname}"
        try:
            body = fetch_view_definition(src_conn, schema, vname)
        except Exception as exc:
            reason = f"error fetching definition: {exc}"
            skipped.append((schema, vname, reason))
            log.error("  [%d/%d] ERROR fetching %s: %s", i, len(parsed_views), display, exc)
            log.debug("Traceback:\n%s", traceback.format_exc())
            continue

        if body is None:
            reason = "view not found on source"
            skipped.append((schema, vname, reason))
            log.warning("  [%d/%d] SKIP  %s — %s", i, len(parsed_views), display, reason)
            continue

        view_defs[(schema, vname)] = body
        try:
            view_columns[(schema, vname)] = fetch_view_columns(src_conn, schema, vname)
        except Exception as exc:
            log.warning("  Could not fetch columns for %s: %s (will use empty list)", display, exc)
            view_columns[(schema, vname)] = []

        try:
            view_is_mat[(schema, vname)] = is_materialized_view(src_conn, schema, vname)
        except Exception as exc:
            log.warning("  Could not check materialized status for %s: %s (assuming regular)", display, exc)
            view_is_mat[(schema, vname)] = False

        kind = "materialized" if view_is_mat.get((schema, vname), False) else "regular"
        n_cols = len(view_columns.get((schema, vname), []))
        log.info("  [%d/%d] OK    %s  (%d chars, %d cols, %s)",
                 i, len(parsed_views), display, len(body), n_cols, kind)

    fetch_elapsed = time.time() - fetch_start
    log.info("Fetch phase complete: %d fetched, %d skipped (%.2fs)",
             len(view_defs), len(skipped), fetch_elapsed)

    try:
        src_conn.close()
        log.debug("Source connection closed.")
    except Exception:
        pass

    if not view_defs:
        log.error("No view definitions fetched. Nothing to migrate.")
        elapsed = time.time() - run_start
        error_log = _write_error_log(
            args.log_dir, skipped, [], len(parsed_views), 0, 0, 0, elapsed, input_file)
        log.info("Error log written: %s", error_log)
        sys.exit(1)

    # --- Resolve dependency order ---
    migratable_set = set(view_defs.keys())
    try:
        ordered = resolve_dependency_order(view_defs, migratable_set)
    except RuntimeError as exc:
        log.error("Dependency resolution failed: %s", exc)
        elapsed = time.time() - run_start
        error_log = _write_error_log(
            args.log_dir, skipped, [], len(parsed_views), 0, 0, 0, elapsed, input_file)
        log.info("Error log written: %s", error_log)
        sys.exit(1)

    # --- Build DDL statements ---
    log.info("Building DDL statements ...")
    ddl_statements: List[Tuple[str, str, str]] = []   # (schema, view, ddl)
    for schema, vname in ordered:
        body = view_defs[(schema, vname)]
        cols = view_columns.get((schema, vname), [])
        mat = view_is_mat.get((schema, vname), False)
        ddl = build_create_statement(schema, vname, body, cols, mat)
        ddl_statements.append((schema, vname, ddl))
    log.info("Built %d DDL statement(s).", len(ddl_statements))

    # --- Optional: write DDL to file ---
    if args.output_file:
        log.info("Writing DDL to output file: %s", args.output_file)
        with open(args.output_file, "w", encoding="utf-8") as fh:
            fh.write("-- Auto-generated by pg_view_migrator.py\n")
            fh.write(f"-- Timestamp: {datetime.datetime.now().isoformat()}\n")
            fh.write(f"-- Source: {SOURCE_PG_HOST}:{SOURCE_PG_PORT}/{SOURCE_PG_DATABASE}\n")
            fh.write(f"-- Views: {len(ddl_statements)}\n\n")
            for schema, vname, ddl in ddl_statements:
                fh.write(f"-- ========== {schema}.{vname} ==========\n")
                fh.write(ddl)
                fh.write("\n\n")
        log.info("DDL written to: %s (%d views)", args.output_file, len(ddl_statements))

    # --- No-execute: skip target entirely ---
    if args.no_execute:
        log.info("=" * 72)
        log.info("NO-EXECUTE mode — target server will NOT be contacted.")
        log.info("=" * 72)
        if not args.output_file:
            for schema, vname, ddl in ddl_statements:
                print(f"\n-- {schema}.{vname}")
                print(ddl)
            print()
        log.info("Total: %d view DDL(s) generated (dependency-ordered).", len(ddl_statements))
        if skipped:
            log.info("Skipped: %d (not found on source).", len(skipped))
        if args.output_file:
            log.info("DDL saved to: %s", args.output_file)
        elapsed = time.time() - run_start
        error_log = _write_error_log(
            args.log_dir, skipped, [], len(parsed_views), 0, 0, 0, elapsed, input_file)
        log.info("Error log written: %s", error_log)
        log.info("Total elapsed: %.2fs", elapsed)
        sys.exit(0)

    # --- Dry-run: print but don't execute ---
    if args.dry_run:
        log.info("=" * 72)
        log.info("DRY RUN — SQL that WOULD be executed on target (in order):")
        log.info("=" * 72)
        for schema, vname, ddl in ddl_statements:
            print(f"\n-- {schema}.{vname}")
            if args.drop_existing:
                mat = view_is_mat.get((schema, vname), False)
                qual = f"{schema}.{vname}"
                kw = "MATERIALIZED VIEW" if mat else "VIEW"
                print(f"DROP {kw} IF EXISTS {qual} CASCADE;")
            print(ddl)
        print()
        log.info("Total: %d views would be created.", len(ddl_statements))
        if skipped:
            log.info("Skipped: %d (not found on source).", len(skipped))
        elapsed = time.time() - run_start
        error_log = _write_error_log(
            args.log_dir, skipped, [], len(parsed_views), len(ddl_statements), 0, 0, elapsed, input_file)
        log.info("Error log written: %s", error_log)
        log.info("Total elapsed: %.2fs", elapsed)
        sys.exit(0)

    # --- Execute on target ---
    try:
        tgt_conn = _connect_target()
        log.info("Target connection established.")
    except Exception as exc:
        log.error("Failed to connect to TARGET: %s", exc)
        log.debug("Traceback:\n%s", traceback.format_exc())
        elapsed = time.time() - run_start
        error_log = _write_error_log(
            args.log_dir, skipped, [], len(parsed_views), 0, 0, 0, elapsed, input_file)
        log.info("Error log written: %s", error_log)
        sys.exit(1)

    # Ensure schemas exist
    schemas_needed: Set[str] = {s for s, _ in ordered}
    for s in schemas_needed:
        try:
            ensure_schema_exists(tgt_conn, s)
            log.debug("Schema ensured: %s", s)
        except Exception as exc:
            log.warning("Failed to ensure schema %s: %s", s, exc)

    n_ok = 0
    n_fail = 0
    failures: List[Tuple[str, str, str, str]] = []   # (schema, view, phase, error)
    pad = len(str(len(ddl_statements)))

    log.info("Executing %d views on target ...", len(ddl_statements))
    exec_start = time.time()

    for idx, (schema, vname, ddl) in enumerate(ddl_statements, 1):
        display = f"{schema}.{vname}"
        view_start = time.time()

        # Optionally drop first
        if args.drop_existing:
            mat = view_is_mat.get((schema, vname), False)
            try:
                drop_view_on_target(tgt_conn, schema, vname, mat)
                log.debug("  Dropped existing: %s", display)
            except Exception as exc:
                log.warning("  Failed to drop %s (continuing): %s", display, exc)

        ok, err = execute_ddl(tgt_conn, ddl)
        view_elapsed = time.time() - view_start

        if ok:
            n_ok += 1
            log.info("  [%*d/%d] OK    %s  (%.3fs)", pad, idx, len(ddl_statements), display, view_elapsed)
        else:
            n_fail += 1
            failures.append((schema, vname, "execute", err or "unknown error"))
            log.error("  [%*d/%d] FAIL  %s  (%.3fs)", pad, idx, len(ddl_statements), display, view_elapsed)
            log.error("    Error: %s", err)
            log.debug("    DDL was:\n%s", ddl)
            if not args.continue_on_error:
                log.error("Stopping due to error. Use --continue-on-error to keep going.")
                break

    exec_elapsed = time.time() - exec_start
    log.info("Execution phase complete: %d OK, %d FAIL (%.2fs)", n_ok, n_fail, exec_elapsed)

    try:
        tgt_conn.close()
        log.debug("Target connection closed.")
    except Exception:
        pass

    # --- Summary ---
    elapsed = time.time() - run_start
    log.info("=" * 72)
    log.info("MIGRATION SUMMARY")
    log.info("=" * 72)
    log.info("  Total requested : %d", len(parsed_views))
    log.info("  Skipped (source): %d", len(skipped))
    log.info("  Attempted       : %d", len(ddl_statements))
    log.info("  Succeeded       : %d", n_ok)
    log.info("  Failed          : %d", n_fail)
    log.info("  Elapsed (total) : %.2fs", elapsed)
    log.info("  Elapsed (fetch) : %.2fs", fetch_elapsed)
    log.info("  Elapsed (exec)  : %.2fs", exec_elapsed)

    if skipped:
        log.info("")
        log.info("  Skipped views:")
        for s, v, reason in skipped:
            log.info("    - %s.%s: %s", s, v, reason)

    if failures:
        log.info("")
        log.info("  Failed views:")
        for s, v, phase, err in failures:
            log.info("    - %s.%s [%s]:", s, v, phase)
            for line in err.splitlines():
                log.info("        %s", line)

    # --- Write error log file ---
    error_log = _write_error_log(
        args.log_dir, skipped, failures,
        len(parsed_views), len(ddl_statements), n_ok, n_fail, elapsed, input_file,
    )
    log.info("")
    log.info("Log file    : %s", log_file)
    log.info("Error log   : %s", error_log)
    log.info("=" * 72)

    if n_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

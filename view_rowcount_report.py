#!/usr/bin/env python3
"""
View Row Count Report: Compare row counts between Oracle (source) and PostgreSQL (target).

Reads view names from one or more input files, converts to lowercase for PG,
executes SELECT COUNT(*) on both databases with a 30-second timeout per query,
and generates a CSV report.

Features:
  - Parallel processing (configurable workers) for large view lists (e.g. 20K views)
  - 30-second timeout per query; status set to "timeout" if exceeded
  - Supports multiple input files
  - Output: CSV with view_name, view_name_lower, oracle_row_count, oracle_status,
    pg_row_count, pg_status

Usage:
  python view_rowcount_report.py --view-list views.txt [options]
  python view_rowcount_report.py --view-list v1.txt --view-list v2.txt -o report.csv
  python view_rowcount_report.py --view-list views.txt --workers 100 --timeout 30

Input format (one view per line):
  SCHEMA.VIEW_NAME
  ANOTHER_VIEW
  # comments and blank lines are skipped
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

# -----------------------------------------------------------------------------
# Default connection settings (override with env or CLI)
# -----------------------------------------------------------------------------
ORACLE_USER: Optional[str] = None
ORACLE_PASSWORD: Optional[str] = None
ORACLE_DSN: str = "localhost:1521/ORCL"

PG_HOST: str = "localhost"
PG_PORT: int = 5432
PG_DATABASE: str = "postgres"
PG_USER: str = "postgres"
PG_PASSWORD: str = ""


def _run_with_timeout(seconds: float, func, *args, **kwargs) -> tuple[bool, Optional[object]]:
    """Run func in a daemon thread. Return (True, result) if finished within timeout, else (False, None)."""
    if seconds <= 0:
        return (True, func(*args, **kwargs))
    result: list = [None]
    exc: list[Optional[BaseException]] = [None]

    def run() -> None:
        try:
            result[0] = func(*args, **kwargs)
        except BaseException as e:
            exc[0] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout=seconds)
    if t.is_alive():
        return (False, None)
    if exc[0] is not None:
        raise exc[0]
    return (True, result[0])


def _read_view_list(paths: list[str]) -> list[tuple[str, str]]:
    """Read view names from text files. One per line: SCHEMA.VIEW_NAME or VIEW_NAME. Returns (schema, view_name) tuples."""
    entries: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for path in paths:
        p = Path(path)
        if not p.exists():
            print(f"Warning: view list file not found: {path}", file=sys.stderr, flush=True)
            continue
        with open(p, encoding="utf-8-sig") as f:
            for ln in f:
                ln = ln.strip()
                if not ln or ln.startswith("#"):
                    continue
                if "." in ln:
                    parts = ln.split(".", 1)
                    schema, view_name = parts[0].strip(), parts[1].strip()
                else:
                    schema, view_name = "", ln.strip()
                if not view_name:
                    continue
                key = (schema, view_name)
                if key not in seen:
                    seen.add(key)
                    entries.append(key)
    return entries


def _get_oracle_count(conn, schema: str, view_name: str, timeout_sec: float) -> tuple[Optional[int], str]:
    """Run SELECT COUNT(*) on Oracle. Returns (count, status) where status is 'ok', 'timeout', or 'error'."""
    cursor = conn.cursor()
    try:
        # Oracle: unquoted identifiers resolve to uppercase
        if schema:
            full_name = f"{schema.upper()}.{view_name.upper()}"
        else:
            full_name = view_name.upper()
        sql = f"SELECT COUNT(*) FROM {full_name}"

        def run() -> int:
            cursor.execute(sql)
            row = cursor.fetchone()
            return int(row[0]) if row else 0

        ok, result = _run_with_timeout(timeout_sec, run)
        if ok:
            return (result, "ok")
        return (None, "timeout")
    except Exception as e:
        return (None, f"error:{str(e)[:100]}")
    finally:
        cursor.close()


def _get_pg_count(conn, schema: str, view_name: str, timeout_sec: float) -> tuple[Optional[int], str]:
    """Run SELECT COUNT(*) on PostgreSQL. Uses lowercase identifiers. Returns (count, status)."""
    try:
        schema_lower = schema.lower() if schema else "public"
        view_lower = view_name.lower()
        full_name = f'"{schema_lower}"."{view_lower}"'
        sql = f"SELECT COUNT(*) FROM {full_name}"

        def run() -> int:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return int(row[0]) if row else 0

        ok, result = _run_with_timeout(timeout_sec, run)
        if ok:
            return (result, "ok")
        return (None, "timeout")
    except Exception as e:
        return (None, f"error:{str(e)[:100]}")


# Thread-local storage for database connections
_tls = threading.local()


def _get_oracle_conn(user: str, password: str, dsn: str):
    """Get or create thread-local Oracle connection."""
    if not hasattr(_tls, "oracle_conn") or _tls.oracle_conn is None:
        import oracledb
        _tls.oracle_conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return _tls.oracle_conn


def _get_pg_conn(host: str, port: int, dbname: str, user: str, password: str, timeout_ms: int):
    """Get or create thread-local PostgreSQL connection."""
    if not hasattr(_tls, "pg_conn") or _tls.pg_conn is None:
        import psycopg2
        _tls.pg_conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            options=f"-c statement_timeout={timeout_ms}",
        )
    return _tls.pg_conn


def _process_view(
    schema: str,
    view_name: str,
    ora_user: str,
    ora_pass: str,
    ora_dsn: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    pg_user: str,
    pg_pass: str,
    timeout_sec: float,
) -> dict:
    """Process a single view: get row counts from Oracle and PG. Returns a row dict for CSV."""
    view_display = f"{schema}.{view_name}" if schema else view_name
    schema_lower = schema.lower() if schema else ""
    view_lower = view_name.lower()
    view_lower_display = f"{schema_lower}.{view_lower}" if schema_lower else view_lower

    row_data = {
        "view_name": view_display,
        "view_name_lower": view_lower_display,
        "oracle_row_count": "",
        "oracle_status": "",
        "pg_row_count": "",
        "pg_status": "",
    }

    timeout_ms = int(timeout_sec * 1000)

    try:
        ora_conn = _get_oracle_conn(ora_user, ora_pass, ora_dsn)
        ora_count, ora_status = _get_oracle_count(ora_conn, schema, view_name, timeout_sec)
        row_data["oracle_row_count"] = str(ora_count) if ora_count is not None else ""
        row_data["oracle_status"] = ora_status

        pg_conn = _get_pg_conn(pg_host, pg_port, pg_db, pg_user, pg_pass, timeout_ms)
        pg_count, pg_status = _get_pg_count(pg_conn, schema, view_name, timeout_sec)
        row_data["pg_row_count"] = str(pg_count) if pg_count is not None else ""
        row_data["pg_status"] = pg_status
    except Exception as e:
        row_data["oracle_status"] = row_data["oracle_status"] or f"error:{str(e)[:80]}"
        row_data["pg_status"] = row_data["pg_status"] or f"error:{str(e)[:80]}"

    return row_data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare row counts between Oracle (source) and PostgreSQL (target).",
    )
    parser.add_argument(
        "--view-list",
        action="append",
        required=True,
        metavar="FILE",
        help="View list file(s) - one view per line (SCHEMA.VIEW_NAME). Can be repeated.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="view_rowcount_report.csv",
        help="Output CSV path (default: view_rowcount_report.csv)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Number of parallel workers (default: 50)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Query timeout in seconds (default: 30)",
    )
    parser.add_argument("--oracle-user", default=None, help="Oracle user (or ORACLE_USER env)")
    parser.add_argument("--oracle-password", default=None, help="Oracle password (or ORACLE_PASSWORD env)")
    parser.add_argument("--oracle-dsn", default=None, help="Oracle DSN (or ORACLE_DSN env)")
    parser.add_argument("--pg-host", default=None, help="PostgreSQL host (or PG_HOST env)")
    parser.add_argument("--pg-port", type=int, default=None, help="PostgreSQL port (or PG_PORT env)")
    parser.add_argument("--pg-database", default=None, help="PostgreSQL database (or PG_DATABASE env)")
    parser.add_argument("--pg-user", default=None, help="PostgreSQL user (or PG_USER env)")
    parser.add_argument("--pg-password", default=None, help="PostgreSQL password (or PG_PASSWORD env)")
    args = parser.parse_args()

    view_list = _read_view_list(args.view_list)
    if not view_list:
        print("Error: no views found in input files.", file=sys.stderr, flush=True)
        sys.exit(1)

    ora_user = args.oracle_user or os.environ.get("ORACLE_USER") or ORACLE_USER
    ora_pass = args.oracle_password or os.environ.get("ORACLE_PASSWORD") or ORACLE_PASSWORD
    ora_dsn = args.oracle_dsn or os.environ.get("ORACLE_DSN", ORACLE_DSN)
    pg_host = args.pg_host or os.environ.get("PG_HOST", PG_HOST)
    pg_port = args.pg_port if args.pg_port is not None else int(os.environ.get("PG_PORT", PG_PORT))
    pg_db = args.pg_database or os.environ.get("PG_DATABASE", PG_DATABASE)
    pg_user = args.pg_user or os.environ.get("PG_USER", PG_USER)
    pg_pass = args.pg_password or os.environ.get("PG_PASSWORD", PG_PASSWORD)

    if not ora_user or not ora_pass:
        print("Error: Oracle credentials required (--oracle-user / --oracle-password or env).", file=sys.stderr, flush=True)
        sys.exit(1)

    print(f"Loaded {len(view_list)} view(s) from input file(s).", flush=True)
    print(f"Workers: {args.workers}, Timeout: {args.timeout}s", flush=True)
    print("Processing...", flush=True)

    start = time.perf_counter()
    rows: list[dict] = []
    done = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                _process_view,
                schema,
                view_name,
                ora_user,
                ora_pass,
                ora_dsn,
                pg_host,
                pg_port,
                pg_db,
                pg_user,
                pg_pass,
                float(args.timeout),
            ): (schema, view_name)
            for schema, view_name in view_list
        }
        for fut in as_completed(futures):
            try:
                row = fut.result()
                rows.append(row)
            except Exception as e:
                schema, view_name = futures[fut]
                view_display = f"{schema}.{view_name}" if schema else view_name
                rows.append({
                    "view_name": view_display,
                    "view_name_lower": view_name.lower(),
                    "oracle_row_count": "",
                    "oracle_status": f"error:{str(e)[:100]}",
                    "pg_row_count": "",
                    "pg_status": "",
                })
            done += 1
            if done % 500 == 0 or done == len(view_list):
                print(f"  {done}/{len(view_list)} views processed", flush=True)

    elapsed = time.perf_counter() - start

    # Preserve order from input
    view_order = {f"{s}.{v}" if s else v: i for i, (s, v) in enumerate(view_list)}
    rows.sort(key=lambda r: view_order.get(r["view_name"], 999999))

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["view_name", "view_name_lower", "oracle_row_count", "oracle_status", "pg_row_count", "pg_status"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    timeout_oracle = sum(1 for r in rows if r["oracle_status"] == "timeout")
    timeout_pg = sum(1 for r in rows if r["pg_status"] == "timeout")
    error_oracle = sum(1 for r in rows if r["oracle_status"] and r["oracle_status"] != "ok" and r["oracle_status"] != "timeout")
    error_pg = sum(1 for r in rows if r["pg_status"] and r["pg_status"] != "ok" and r["pg_status"] != "timeout")
    ok_oracle = sum(1 for r in rows if r["oracle_status"] == "ok")
    ok_pg = sum(1 for r in rows if r["pg_status"] == "ok")

    print(f"\nCompleted in {elapsed:.1f}s ({elapsed/60:.1f} min)", flush=True)
    print(f"Report written to: {out_path}", flush=True)
    print(f"Summary:", flush=True)
    print(f"  Oracle: ok={ok_oracle}, timeout={timeout_oracle}, error={error_oracle}", flush=True)
    print(f"  PG:     ok={ok_pg}, timeout={timeout_pg}, error={error_pg}", flush=True)


if __name__ == "__main__":
    main()

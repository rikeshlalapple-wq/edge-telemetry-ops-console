import argparse
import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import alert_rules as ar
import db_resilience as dbr
import field_dashboard as fd

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")


def query_rows(
    db_path: str = DB_PATH,
    unit: str | None = None,
    metric: str | None = None,
    severity: str | None = None,
    min_id: int | None = None,
    limit: int = 25,
    order_desc: bool = True,
) -> list[dict]:
    if not os.path.exists(db_path):
        return []

    clauses: list[str] = []
    params: list[object] = []

    if unit:
        clauses.append("LOWER(COALESCE(unit_id, '')) LIKE ?")
        params.append(f"%{unit.strip().lower()}%")
    if metric:
        clauses.append("LOWER(COALESCE(metric_name, '')) LIKE ?")
        params.append(f"%{metric.strip().lower()}%")
    if severity:
        clauses.append("UPPER(COALESCE(severity, '')) = ?")
        params.append(severity.strip().upper())
    if min_id is not None:
        clauses.append("log_id > ?")
        params.append(int(min_id))

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    order_sql = "ORDER BY log_id DESC" if order_desc else "ORDER BY log_id ASC"
    limit_sql = f"LIMIT {max(1, int(limit))}" if limit is not None else ""

    sql = f"SELECT * FROM received_logs {where_sql} {order_sql} {limit_sql}"

    conn = dbr.connect_db(db_path)
    try:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        dbr.checkpoint_and_close(conn)

    return rows


def render_rows(rows: list[dict], title: str = "TRIAGE QUERY RESULTS") -> None:
    print(f"\n=== {title} (matches={len(rows)}) ===")
    if not rows:
        print("  (No matching rows.)")
        return

    alerts = ar.scan_recent_rows(rows, emit_log=False)
    if alerts:
        print("--- MATCHED ALERTS ---")
        for a in alerts[:5]:
            print(f"  [!] {a}")

    print("--- ROWS ---")
    for r in rows:
        ver = r.get("schema_version", 1)
        gauge = fd.format_gauge(r.get("metric_name", ""), r.get("metric_value"))
        print(
            f"  #{r['log_id']} (v{ver}) | {r['unit_id']} | "
            f"{r['metric_name']}={r['metric_value']} {gauge} | {r['severity']} | "
            f"ts={r['timestamp']}"
        )


def watch_stream(
    db_path: str = DB_PATH,
    unit: str | None = None,
    metric: str | None = None,
    severity: str | None = None,
    interval: float = 2.0,
) -> None:
    if not os.path.exists(db_path):
        print(f"[WATCH] Database not found: {db_path}")
        return

    existing = query_rows(
        db_path=db_path,
        unit=unit,
        metric=metric,
        severity=severity,
        limit=1,
        order_desc=True,
    )
    last_seen_id = int(existing[0]["log_id"]) if existing else 0

    filters_desc = []
    if unit:
        filters_desc.append(f"unit~{unit}")
    if metric:
        filters_desc.append(f"metric~{metric}")
    if severity:
        filters_desc.append(f"severity={severity.upper()}")
    f_str = ", ".join(filters_desc) if filters_desc else "ALL"

    print(f"\n=== LIVE TRIAGE WATCH | filters=[{f_str}] | poll={interval:.1f}s | last_id={last_seen_id} ===")
    print("Waiting for new rows... Press Ctrl+C to stop.")

    try:
        while True:
            new_rows = query_rows(
                db_path=db_path,
                unit=unit,
                metric=metric,
                severity=severity,
                min_id=last_seen_id,
                limit=100,
                order_desc=False,
            )
            if new_rows:
                for r in new_rows:
                    last_seen_id = max(last_seen_id, int(r["log_id"]))
                    ver = r.get("schema_version", 1)
                    gauge = fd.format_gauge(r.get("metric_name", ""), r.get("metric_value"))
                    print(
                        f"  [NEW #{r['log_id']}] (v{ver}) {r['unit_id']} | "
                        f"{r['metric_name']}={r['metric_value']} {gauge} | {r['severity']} | rx={r['received_at']}"
                    )
                row_alerts = ar.scan_recent_rows(new_rows, emit_log=False)
                for a in row_alerts:
                    print(f"    [ALERT] {a}")
            time.sleep(max(0.2, float(interval)))
    except KeyboardInterrupt:
        print("\n[WATCH] Stopped live triage stream.")


def interactive_filter_prompt() -> None:
    print("\n=== INTERACTIVE TRIAGE FILTER ===")
    unit = input("Unit filter (blank for any): ").strip() or None
    metric = input("Metric filter (blank for any): ").strip() or None
    severity = input("Severity exact match e.g. HIGH/INFO (blank for any): ").strip() or None
    lim_raw = input("Max rows [default 20]: ").strip()
    limit = int(lim_raw) if lim_raw.isdigit() else 20

    rows = query_rows(unit=unit, metric=metric, severity=severity, limit=limit, order_desc=True)
    render_rows(rows, title="FILTERED TRIAGE VIEW")

    watch_ans = input("Enter live watch mode for these filters? (y/N): ").strip().lower()
    if watch_ans == "y":
        watch_stream(unit=unit, metric=metric, severity=severity, interval=2.0)


def main():
    parser = argparse.ArgumentParser(description="Live Watch & Filterable Telemetry Triage")
    parser.add_argument("--unit", type=str, default=None, help="Filter by unit_id substring")
    parser.add_argument("--metric", type=str, default=None, help="Filter by metric_name substring")
    parser.add_argument("--severity", type=str, default=None, help="Filter by exact severity (e.g., HIGH, INFO)")
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to display (default: 20)")
    parser.add_argument("--watch", action="store_true", help="Stream newly ingested rows live")
    parser.add_argument("--interval", type=float, default=2.0, help="Watch poll interval in seconds (default: 2.0)")
    args = parser.parse_args()

    if args.watch:
        watch_stream(
            unit=args.unit,
            metric=args.metric,
            severity=args.severity,
            interval=args.interval,
        )
    else:
        rows = query_rows(
            unit=args.unit,
            metric=args.metric,
            severity=args.severity,
            limit=args.limit,
            order_desc=True,
        )
        render_rows(rows)


if __name__ == "__main__":
    main()

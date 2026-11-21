import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db_resilience as dbr

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")

CREATE_VIEWS_SQL = """
DROP VIEW IF EXISTS v_metric_health_summary;
DROP VIEW IF EXISTS v_telemetry_window;

CREATE VIEW v_telemetry_window AS
SELECT
    log_id,
    schema_version,
    unit_id,
    metric_name,
    metric_value,
    severity,
    timestamp,
    received_at,
    LAG(metric_value) OVER (
        PARTITION BY COALESCE(unit_id, 'unknown'), COALESCE(metric_name, 'unknown')
        ORDER BY log_id ASC
    ) AS prev_metric_value,
    CASE
        WHEN metric_value IS NOT NULL AND LAG(metric_value) OVER (
            PARTITION BY COALESCE(unit_id, 'unknown'), COALESCE(metric_name, 'unknown')
            ORDER BY log_id ASC
        ) IS NOT NULL
        THEN ROUND(
            metric_value - LAG(metric_value) OVER (
                PARTITION BY COALESCE(unit_id, 'unknown'), COALESCE(metric_name, 'unknown')
                ORDER BY log_id ASC
            ),
            3
        )
        ELSE NULL
    END AS delta_from_prev,
    ROUND(
        AVG(metric_value) OVER (
            PARTITION BY COALESCE(unit_id, 'unknown'), COALESCE(metric_name, 'unknown')
            ORDER BY log_id ASC
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ),
        3
    ) AS rolling_avg_5,
    ROW_NUMBER() OVER (
        PARTITION BY COALESCE(unit_id, 'unknown'), COALESCE(metric_name, 'unknown')
        ORDER BY log_id DESC
    ) AS recency_rank
FROM received_logs;

CREATE VIEW v_metric_health_summary AS
SELECT
    COALESCE(unit_id, 'unknown') AS unit_id,
    COALESCE(metric_name, 'unknown') AS metric_name,
    COUNT(*) AS sample_count,
    ROUND(MIN(metric_value), 3) AS min_val,
    ROUND(AVG(metric_value), 3) AS avg_val,
    ROUND(MAX(metric_value), 3) AS max_val,
    MAX(CASE WHEN recency_rank = 1 THEN metric_value END) AS latest_val,
    MAX(CASE WHEN recency_rank = 1 THEN delta_from_prev END) AS latest_delta,
    MAX(CASE WHEN recency_rank = 1 THEN rolling_avg_5 END) AS latest_rolling_avg_5,
    SUM(CASE WHEN UPPER(COALESCE(severity, '')) IN ('HIGH', 'CRITICAL', 'CRIT') THEN 1 ELSE 0 END) AS high_crit_count
FROM v_telemetry_window
GROUP BY COALESCE(unit_id, 'unknown'), COALESCE(metric_name, 'unknown');
"""


def ensure_views(db_path: str = DB_PATH) -> bool:
    if not os.path.exists(db_path):
        return False
    conn = dbr.connect_db(db_path)
    try:
        conn.executescript(CREATE_VIEWS_SQL)
        conn.commit()
    finally:
        dbr.checkpoint_and_close(conn)
    return True


def check_integrity(db_path: str = DB_PATH) -> str:
    if not os.path.exists(db_path):
        return "missing_db"
    conn = dbr.connect_db(db_path)
    try:
        row = conn.execute("PRAGMA integrity_check;").fetchone()
        return str(row[0]) if row else "unknown"
    finally:
        dbr.checkpoint_and_close(conn)


def run_window_report(db_path: str = DB_PATH, limit: int = 12) -> None:
    print("\n=== SQLITE WINDOW FUNCTIONS, VIEWS & INTEGRITY REPORT ===")
    if not os.path.exists(db_path):
        print(f"[SQL-VIEWS] Database not found: {db_path}")
        return

    ensure_views(db_path)
    integrity = check_integrity(db_path)
    print(f"[INTEGRITY] PRAGMA integrity_check -> {integrity}")

    conn = dbr.connect_db(db_path)
    try:
        summary_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT *
                FROM v_metric_health_summary
                ORDER BY unit_id ASC, metric_name ASC
                """
            ).fetchall()
        ]
        window_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT log_id, unit_id, metric_name, metric_value, prev_metric_value,
                       delta_from_prev, rolling_avg_5, severity
                FROM v_telemetry_window
                ORDER BY log_id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        ]
    finally:
        dbr.checkpoint_and_close(conn)

    print("\n--- VIEW: v_metric_health_summary ---")
    if not summary_rows:
        print("  (No summary rows.)")
    else:
        for s in summary_rows:
            latest_d = f"{s['latest_delta']:+.3f}" if s["latest_delta"] is not None else "n/a"
            roll5 = f"{s['latest_rolling_avg_5']:.3f}" if s["latest_rolling_avg_5"] is not None else "n/a"
            print(
                f"  {s['unit_id']} :: {s['metric_name']:<15} | n={s['sample_count']:<2} | "
                f"min={s['min_val']} avg={s['avg_val']} max={s['max_val']} | "
                f"latest={s['latest_val']} (Δ={latest_d}, avg5={roll5}) | hi/crit={s['high_crit_count']}"
            )

    print(f"\n--- VIEW: v_telemetry_window (LATEST {len(window_rows)}) ---")
    for w in window_rows:
        prev_v = f"{w['prev_metric_value']}" if w["prev_metric_value"] is not None else "n/a"
        delta_v = f"{w['delta_from_prev']:+.3f}" if w["delta_from_prev"] is not None else "n/a"
        roll5_v = f"{w['rolling_avg_5']:.3f}" if w["rolling_avg_5"] is not None else "n/a"
        print(
            f"  #{w['log_id']:<2} | {w['unit_id']} | {w['metric_name']:<15} | "
            f"val={w['metric_value']} (prev={prev_v}, Δ={delta_v}, avg5={roll5_v}) | {w['severity']}"
        )


if __name__ == "__main__":
    run_window_report()

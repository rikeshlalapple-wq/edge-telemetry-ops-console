import os
import sys
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import alert_rules as ar
import db_resilience as dbr

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")
SPARK_CHARS = "▁▂▃▄▅▆▇█"


def sparkline(values: list[float]) -> str:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return "n/a"
    if len(clean) == 1:
        return SPARK_CHARS[len(SPARK_CHARS) // 2]
    lo = min(clean)
    hi = max(clean)
    if hi == lo:
        return SPARK_CHARS[len(SPARK_CHARS) // 2] * len(clean)
    out = []
    for val in clean:
        norm = (val - lo) / (hi - lo)
        idx = min(len(SPARK_CHARS) - 1, max(0, int(norm * (len(SPARK_CHARS) - 1))))
        out.append(SPARK_CHARS[idx])
    return "".join(out)


def heat_bar(ratio: float, width: int = 12) -> str:
    clamped = max(0.0, min(1.0, ratio))
    filled = int(round(clamped * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def format_gauge(metric_name: str, value: float | None) -> str:
    if value is None:
        return "[------------] n/a"
    v = float(value)
    m = (metric_name or "").lower()

    if "battery" in m:
        bar = heat_bar(v / 100.0)
        tag = "LOW" if v < 15.0 else "OK"
        return f"{bar} {v:5.1f}% ({tag})"

    if "disk" in m:
        bar = heat_bar(v / 100.0)
        tag = "LOW" if v < 15.0 else "OK"
        return f"{bar} {v:5.1f}% ({tag})"

    if "rssi" in m or "wifi" in m:
        # Map -90 dBm (0.0) to -30 dBm (1.0)
        ratio = (v - (-90.0)) / 60.0
        bar = heat_bar(ratio)
        tag = "WEAK" if v < -75.0 else "GOOD"
        return f"{bar} {v:5.1f}dBm ({tag})"

    if "rtt" in m or "ping" in m:
        if v < 0:
            return "[XXXXXXXXXXXX] UNREACHABLE"
        ratio = v / 150.0
        bar = heat_bar(ratio)
        tag = "HIGH" if v > 150.0 else "OK"
        return f"{bar} {v:6.1f}ms ({tag})"

    ratio = max(0.0, min(1.0, abs(v) / 100.0))
    return f"{heat_bar(ratio)} {v:.2f}"


def main():
    if not os.path.exists(DB_PATH):
        print(f"[DASHBOARD] Database not found: {DB_PATH}")
        return

    conn = dbr.connect_db(DB_PATH)
    cur = conn.cursor()

    try:
        total = int(cur.execute("SELECT COUNT(*) FROM received_logs").fetchone()[0])
        high_cnt = int(
            cur.execute(
                "SELECT COUNT(*) FROM received_logs WHERE UPPER(COALESCE(severity, '')) IN ('HIGH', 'CRITICAL', 'CRIT')"
            ).fetchone()[0]
        )
        rows = [
            dict(r)
            for r in cur.execute("SELECT * FROM received_logs ORDER BY log_id DESC LIMIT 20").fetchall()
        ]
        series_rows = [
            dict(r)
            for r in cur.execute(
                "SELECT metric_name, metric_value, log_id FROM received_logs WHERE metric_value IS NOT NULL ORDER BY log_id ASC"
            ).fetchall()
        ]
    except Exception as exc:
        print(f"[DASHBOARD] DB read error: {exc}")
        dbr.checkpoint_and_close(conn)
        return

    dbr.checkpoint_and_close(conn)

    metric_series: dict[str, list[float]] = defaultdict(list)
    for item in series_rows:
        m_name = item.get("metric_name") or "unknown"
        metric_series[m_name].append(float(item["metric_value"]))

    alerts = ar.scan_recent_rows(rows, emit_log=False)

    print(f"=== WORKSTATION TELEMETRY DASHBOARD | Total: {total} | HIGH/CRIT: {high_cnt} | Alerts(20): {len(alerts)} ===")

    if metric_series:
        print("--- METRIC SPARKLINES & THRESHOLD GAUGES ---")
        for m_name in sorted(metric_series.keys()):
            hist = metric_series[m_name][-12:]
            latest_val = hist[-1]
            sp = sparkline(hist)
            gauge = format_gauge(m_name, latest_val)
            print(f"  {m_name:<16} | {gauge:<28} | trend: {sp}")

    if alerts:
        print("--- ACTIVE ALERTS (LAST 20 ROWS) ---")
        for a in alerts[:5]:
            print(f"  [!] {a}")

    print("--- RECENT INGESTED ROWS ---")
    for r in rows:
        ver = r.get("schema_version", 1)
        gauge = format_gauge(r.get("metric_name", ""), r.get("metric_value"))
        print(
            f"  #{r['log_id']} (v{ver}) | {r['unit_id']} | "
            f"{r['metric_name']}={r['metric_value']} {gauge} | {r['severity']} | "
            f"ts={r['timestamp']} | rx={r['received_at']}"
        )


if __name__ == "__main__":
    main()

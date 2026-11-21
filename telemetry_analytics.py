import math
import os
import statistics
import sys
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db_resilience as dbr

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")


def _median_abs_deviation(vals: list[float], med: float) -> float:
    devs = [abs(v - med) for v in vals]
    return statistics.median(devs) if devs else 0.0


def _iqr_bounds(vals: list[float]) -> tuple[float, float] | None:
    if len(vals) < 4:
        return None
    s = sorted(vals)
    q1 = statistics.median(s[: len(s) // 2])
    upper_half = s[len(s) // 2 :] if len(s) % 2 == 0 else s[len(s) // 2 + 1 :]
    q3 = statistics.median(upper_half)
    iqr = q3 - q1
    if iqr == 0:
        return None
    return (q1 - 1.5 * iqr, q3 + 1.5 * iqr)


def analyze_series(values: list[float]) -> dict:
    clean = [float(v) for v in values if v is not None and not math.isnan(float(v))]
    n = len(clean)
    if n == 0:
        return {
            "count": 0,
            "min": None,
            "avg": None,
            "max": None,
            "latest": None,
            "last_delta": None,
            "total_drift": None,
            "half_drift": 0.0,
            "drift_label": "NONE",
            "anomalies": [],
        }

    lo = min(clean)
    hi = max(clean)
    avg = sum(clean) / n
    latest = clean[-1]
    last_delta = (clean[-1] - clean[-2]) if n >= 2 else 0.0
    total_drift = clean[-1] - clean[0]

    # Compare second-half mean vs first-half mean for trend drift
    mid = max(1, n // 2)
    first_half = clean[:mid]
    second_half = clean[mid:] if len(clean) > 1 else clean
    half_drift = (sum(second_half) / len(second_half)) - (sum(first_half) / len(first_half))

    scale = max(abs(avg), 1.0)
    rel_drift = half_drift / scale
    if n >= 3 and abs(rel_drift) >= 0.08:
        drift_label = "DRIFT_UP" if half_drift > 0 else "DRIFT_DOWN"
    else:
        drift_label = "STABLE"

    anomalies: list[str] = []
    med = statistics.median(clean)
    mad = _median_abs_deviation(clean, med)
    iqr_bounds = _iqr_bounds(clean)

    for idx, val in enumerate(clean):
        reasons = []
        if val < 0 and ("rtt" in str(idx) or idx >= 0):
            pass
        if mad > 0:
            # 0.6745 * (x - median) / MAD is the standard robust Z-score equivalent
            mod_z = abs(0.6745 * (val - med) / mad)
            if mod_z >= 3.5:
                reasons.append(f"z={mod_z:.1f}")
        if iqr_bounds is not None:
            low_b, high_b = iqr_bounds
            if val < low_b or val > high_b:
                reasons.append("outside_1.5xIQR")

        if reasons:
            anomalies.append(f"sample#{idx + 1}={val:.2f} ({','.join(reasons)})")

    return {
        "count": n,
        "min": lo,
        "avg": avg,
        "max": hi,
        "latest": latest,
        "last_delta": last_delta,
        "total_drift": total_drift,
        "half_drift": half_drift,
        "drift_label": drift_label,
        "anomalies": anomalies,
    }


def run_analytics(db_path: str = DB_PATH) -> None:
    print("\n=== TELEMETRY ANOMALY & DRIFT ANALYTICS ===")
    if not os.path.exists(db_path):
        print(f"[ANALYTICS] Database not found: {db_path}")
        return

    conn = dbr.connect_db(db_path)
    cur = conn.cursor()
    rows = [
        dict(r)
        for r in cur.execute(
            """
            SELECT log_id, unit_id, metric_name, metric_value, timestamp
            FROM received_logs
            WHERE metric_value IS NOT NULL
            ORDER BY log_id ASC
            """
        ).fetchall()
    ]
    dbr.checkpoint_and_close(conn)

    if not rows:
        print("[ANALYTICS] No numeric telemetry rows available for analysis.")
        return

    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in rows:
        key = (r.get("unit_id") or "unknown_unit", r.get("metric_name") or "unknown_metric")
        grouped[key].append(float(r["metric_value"]))

    all_flags: list[str] = []
    for (unit_id, metric_name) in sorted(grouped.keys()):
        stats = analyze_series(grouped[(unit_id, metric_name)])
        print(
            f"\n[{unit_id} :: {metric_name}] n={stats['count']} | "
            f"min={stats['min']:.2f} avg={stats['avg']:.2f} max={stats['max']:.2f} | "
            f"latest={stats['latest']:.2f} (Δ={stats['last_delta']:+.2f}, drift={stats['drift_label']} {stats['half_drift']:+.2f})"
        )
        if stats["anomalies"]:
            for flag in stats["anomalies"]:
                msg = f"{unit_id}/{metric_name}: {flag}"
                all_flags.append(msg)
                print(f"  [OUTLIER] {flag}")
        else:
            print("  [OUTLIER] None detected")

    print("\n--- ANALYTICS SUMMARY ---")
    print(f"Series analyzed: {len(grouped)} | Outlier flags: {len(all_flags)}")


if __name__ == "__main__":
    run_analytics()

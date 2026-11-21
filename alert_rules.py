import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import log_resilience as lr

CFG_PATH = os.path.join(BASE_DIR, "ops_config.json")
ALERT_LOG_PATH = os.path.join(BASE_DIR, "logs", "alerts.log")
alert_logger = lr.get_logger("alert_rules", ALERT_LOG_PATH)

DEFAULT_THRESHOLDS = {
    "battery_low_pct": 15.0,
    "free_disk_low_pct": 15.0,
    "cpu_load_high_1m": 4.0,
    "wifi_weak_dbm": -75.0,
    "ping_high_ms": 150.0,
}


def load_thresholds() -> dict:
    thresholds = dict(DEFAULT_THRESHOLDS)
    if os.path.exists(CFG_PATH):
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            if isinstance(cfg.get("thresholds"), dict):
                thresholds.update(cfg["thresholds"])
        except Exception as exc:
            alert_logger.warning(f"Failed to load thresholds from {CFG_PATH}: {exc}")
    return thresholds


def evaluate_record(row: dict, thresholds: dict | None = None) -> list[str]:
    th = thresholds or load_thresholds()
    alerts = []

    log_id = row.get("log_id")
    unit_id = row.get("unit_id", "UNKNOWN")
    metric_name = str(row.get("metric_name", ""))
    severity = str(row.get("severity", "INFO")).upper()
    raw_val = row.get("metric_value")

    try:
        val = float(raw_val) if raw_val is not None else None
    except (TypeError, ValueError):
        val = None

    if severity in {"HIGH", "CRITICAL"}:
        alerts.append(f"#{log_id} [{unit_id}] {metric_name}={val} flagged severity={severity}")

    if val is not None:
        if metric_name == "battery_pct" and val <= float(th["battery_low_pct"]):
            alerts.append(f"#{log_id} [{unit_id}] Low battery: {val}% <= {th['battery_low_pct']}%")
        elif metric_name == "free_disk_pct" and val <= float(th["free_disk_low_pct"]):
            alerts.append(f"#{log_id} [{unit_id}] Low free disk: {val}% <= {th['free_disk_low_pct']}%")
        elif metric_name == "cpu_load_1m" and val >= float(th["cpu_load_high_1m"]):
            alerts.append(f"#{log_id} [{unit_id}] High CPU 1m load: {val} >= {th['cpu_load_high_1m']}")
        elif metric_name == "wifi_rssi_dbm" and val <= float(th["wifi_weak_dbm"]):
            alerts.append(f"#{log_id} [{unit_id}] Weak Wi-Fi signal: {val} dBm <= {th['wifi_weak_dbm']} dBm")
        elif metric_name == "ping_rtt_ms" and (val < 0 or val >= float(th["ping_high_ms"])):
            alerts.append(f"#{log_id} [{unit_id}] Ping issue/high RTT: {val} ms (threshold={th['ping_high_ms']} ms)")

    return alerts


def scan_recent_rows(rows: list[dict], emit_log: bool = False) -> list[str]:
    th = load_thresholds()
    triggered = []
    for row in rows:
        for msg in evaluate_record(row, th):
            triggered.append(msg)
            if emit_log:
                alert_logger.warning(f"ALERT | {msg}")
    return triggered

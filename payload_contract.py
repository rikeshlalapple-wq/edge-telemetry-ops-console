SCHEMA_VERSION = 1


def validate_record(item: dict) -> tuple[bool, str, dict]:
    if not isinstance(item, dict):
        return False, "record must be a JSON object", {}

    if "log_id" not in item:
        return False, "missing required field: log_id", {}

    try:
        log_id = int(item["log_id"])
    except (TypeError, ValueError):
        return False, "log_id must be an integer", {}

    raw_val = item.get("metric_value")
    if raw_val is None:
        return False, "missing required field: metric_value", {}
    try:
        metric_value = float(raw_val)
    except (TypeError, ValueError):
        return False, "metric_value must be numeric", {}

    severity = str(item.get("severity", "INFO")).upper().strip()
    if severity not in {"INFO", "WARN", "WARNING", "HIGH", "CRITICAL"}:
        severity = "INFO"

    version = item.get("schema_version", SCHEMA_VERSION)
    try:
        version = int(version)
    except (TypeError, ValueError):
        version = SCHEMA_VERSION

    normalized = {
        "schema_version": version,
        "log_id": log_id,
        "timestamp": str(item.get("timestamp", "")).strip(),
        "unit_id": str(item.get("unit_id", "UNKNOWN")).strip() or "UNKNOWN",
        "metric_name": str(item.get("metric_name", "telemetry")).strip() or "telemetry",
        "metric_value": metric_value,
        "severity": severity,
        "notes": str(item.get("notes", "")),
    }
    return True, "ok", normalized

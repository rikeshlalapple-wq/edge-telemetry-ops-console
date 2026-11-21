import json
import os
import sys
import urllib.error
import urllib.request
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import alert_rules as ar
import db_resilience as dbr
import ops_health as oh
import telemetry_analytics as ta

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")
CFG_PATH = os.path.join(BASE_DIR, "ops_config.json")
OLLAMA_URL = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
PREFERRED_MODELS = ["qwen2.5:1.5b", "llama3.2:1b", "apex-12"]


def detect_ollama_model() -> str | None:
    env_model = os.environ.get("TELEMETRY_LLM_MODEL")
    if env_model:
        return env_model.strip()

    if os.path.exists(CFG_PATH):
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            if isinstance(cfg, dict):
                llm_cfg = cfg.get("llm", {})
                if isinstance(llm_cfg, dict) and llm_cfg.get("model"):
                    return str(llm_cfg["model"]).strip()
        except Exception:
            pass

    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags", method="GET")
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        installed = [m.get("name") for m in data.get("models", []) if m.get("name")]
        for pref in PREFERRED_MODELS:
            if pref in installed:
                return pref
        return installed[0] if installed else None
    except Exception:
        return None


def build_rule_digest(db_path: str = DB_PATH) -> dict:
    if not os.path.exists(db_path):
        return {
            "total_rows": 0,
            "alerts": [],
            "outliers": [],
            "drifts": [],
            "latest_by_metric": {},
            "rejected_count": 0,
        }

    conn = dbr.connect_db(db_path)
    try:
        total_rows = int(conn.execute("SELECT COUNT(*) FROM received_logs").fetchone()[0])
        recent = [
            dict(r)
            for r in conn.execute("SELECT * FROM received_logs ORDER BY log_id DESC LIMIT 20").fetchall()
        ]
        series_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT log_id, unit_id, metric_name, metric_value
                FROM received_logs
                WHERE metric_value IS NOT NULL
                ORDER BY log_id ASC
                """
            ).fetchall()
        ]
    finally:
        dbr.checkpoint_and_close(conn)

    alerts = ar.scan_recent_rows(recent, emit_log=False)

    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    latest_by_metric: dict[str, str] = {}
    for r in series_rows:
        u = r.get("unit_id") or "unknown"
        m = r.get("metric_name") or "unknown"
        val = float(r["metric_value"])
        grouped[(u, m)].append(val)
        latest_by_metric[f"{u}:{m}"] = f"{val:.2f} (#{r['log_id']})"

    outliers: list[str] = []
    drifts: list[str] = []
    for (u, m), vals in sorted(grouped.items()):
        st = ta.analyze_series(vals)
        if st["anomalies"]:
            outliers.append(f"{u}/{m} -> {', '.join(st['anomalies'])}")
        if st["drift_label"] in ("DRIFT_UP", "DRIFT_DOWN"):
            drifts.append(f"{u}/{m}: {st['drift_label']} ({st['half_drift']:+.2f})")

    rej_count, _ = oh.summarize_rejections(oh.RECEIVER_LOG_PATH)

    return {
        "total_rows": total_rows,
        "alerts": alerts,
        "outliers": outliers,
        "drifts": drifts,
        "latest_by_metric": latest_by_metric,
        "rejected_count": rej_count,
    }


def format_rule_briefing(digest: dict) -> str:
    lines = [
        "=== RULE-BASED INCIDENT & FIELD BRIEFING ===",
        f"Active Rows: {digest['total_rows']} | Alerts(last 20): {len(digest['alerts'])} | "
        f"Outliers: {len(digest['outliers'])} | Drift Series: {len(digest['drifts'])} | "
        f"Rejected Payloads: {digest['rejected_count']}",
    ]

    if digest["latest_by_metric"]:
        latest_str = ", ".join(f"{k}={v}" for k, v in sorted(digest["latest_by_metric"].items()))
        lines.append(f"Latest Readings: {latest_str}")

    if digest["alerts"]:
        lines.append("Triggered Rules:")
        for a in digest["alerts"][:5]:
            lines.append(f"  - {a}")
    else:
        lines.append("Triggered Rules: None")

    if digest["outliers"]:
        lines.append("Statistical Outliers:")
        for o in digest["outliers"][:5]:
            lines.append(f"  - {o}")

    if digest["drifts"]:
        lines.append("Trend Drifts:")
        for d in digest["drifts"][:5]:
            lines.append(f"  - {d}")

    return "\n".join(lines)


def generate_llm_briefing(digest: dict, model: str | None = None, timeout_sec: float = 12.0) -> tuple[str | None, str]:
    target_model = model or detect_ollama_model()
    if not target_model:
        return None, "no_local_model_detected"

    fact_block = format_rule_briefing(digest)
    prompt = (
        "You are a concise telemetry operations assistant. Summarize ONLY the facts provided below "
        "in 3 to 4 crisp bullet points for a field operator. Do not invent numbers or causes not "
        "present in the facts.\n\n"
        f"{fact_block}"
    )

    payload = json.dumps(
        {
            "model": target_model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2},
        }
    ).encode("utf-8")

    try:
        req = urllib.request.Request(
            f"{OLLAMA_URL}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        text = (body.get("response") or "").strip()
        if text:
            return text, f"ollama:{target_model}"
        return None, f"ollama:{target_model}:empty_response"
    except (urllib.error.URLError, TimeoutError, Exception) as exc:
        return None, f"ollama:{target_model}:unavailable ({type(exc).__name__})"


def run_briefing(use_llm: bool = True, model: str | None = None) -> None:
    digest = build_rule_digest(DB_PATH)
    rule_text = format_rule_briefing(digest)
    print(f"\n{rule_text}")

    if not use_llm:
        print("\n[LLM] Skipped (--no-llm).")
        return

    llm_text, status = generate_llm_briefing(digest, model=model)
    print(f"\n--- HYBRID OPERATOR BRIEFING ({status}) ---")
    if llm_text:
        print(llm_text)
    else:
        print("[FALLBACK] Local LLM not active; deterministic rule digest above is authoritative.")


if __name__ == "__main__":
    no_llm = "--no-llm" in sys.argv
    custom_model = None
    for idx, arg in enumerate(sys.argv[1:], start=1):
        if arg == "--model" and idx < len(sys.argv):
            custom_model = sys.argv[idx]
    run_briefing(use_llm=not no_llm, model=custom_model)

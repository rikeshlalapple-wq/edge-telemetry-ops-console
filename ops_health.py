import os
import re
import socket
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import alert_rules as ar
import db_resilience as dbr

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")
LOG_DIR = os.path.join(BASE_DIR, "logs")
RECEIVER_LOG_PATH = os.path.join(LOG_DIR, "http_receiver.log")


def check_tcp_port(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def summarize_schema_versions(cur) -> str:
    cols = {row[1] for row in cur.execute("PRAGMA table_info(received_logs)").fetchall()}
    if "schema_version" not in cols:
        return "none (column missing)"
    rows = cur.execute(
        "SELECT COALESCE(schema_version, 1) AS ver, COUNT(*) AS cnt FROM received_logs GROUP BY ver ORDER BY ver ASC"
    ).fetchall()
    if not rows:
        return "none (empty table)"
    return ", ".join(f"v{r['ver']}: {r['cnt']}" for r in rows)


def summarize_rejections(log_path: str = RECEIVER_LOG_PATH) -> tuple[int, list[str]]:
    if not os.path.exists(log_path):
        return 0, []

    total_rejected = 0
    recent_events: list[str] = []
    rej_pattern = re.compile(r"rejected=(\d+)")

    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                match = rej_pattern.search(line)
                if match:
                    cnt = int(match.group(1))
                    if cnt > 0:
                        total_rejected += cnt
                        recent_events.append(line)
                elif "INVALID_PAYLOAD" in line or "rejected" in line.lower():
                    recent_events.append(line)
    except OSError:
        return 0, []

    return total_rejected, recent_events[-5:]


def main():
    print("=== WORKSTATION OPS HEALTH CHECK ===")
    port_8090 = check_tcp_port("127.0.0.1", 8090)
    print(f"[NET] Receiver 127.0.0.1:8090 reachable: {'YES' if port_8090 else 'NO'}")

    db_exists = os.path.exists(DB_PATH)
    print(f"[DB] ingested_telemetry.db exists: {'YES' if db_exists else 'NO'}")

    if db_exists:
        conn = dbr.connect_db(DB_PATH)
        cur = conn.cursor()
        journal = cur.execute("PRAGMA journal_mode;").fetchone()[0]
        busy_ms = cur.execute("PRAGMA busy_timeout;").fetchone()[0]
        total_rows = cur.execute("SELECT COUNT(*) FROM received_logs").fetchone()[0]
        schema_summary = summarize_schema_versions(cur)
        recent = [
            dict(r)
            for r in cur.execute("SELECT * FROM received_logs ORDER BY log_id DESC LIMIT 20").fetchall()
        ]
        dbr.checkpoint_and_close(conn)

        alerts = ar.scan_recent_rows(recent, emit_log=False)
        print(
            f"[DB] journal_mode={journal} | busy_timeout={busy_ms}ms | "
            f"total_rows={total_rows} | schemas=[{schema_summary}]"
        )
        print(f"[ALERTS] Triggered in last 20 rows: {len(alerts)}")
        for a in alerts[:5]:
            print(f"  ! {a}")

    rej_count, rej_events = summarize_rejections(RECEIVER_LOG_PATH)
    print(f"[REJECTIONS] Rejected payloads recorded: {rej_count}")
    for ev in rej_events:
        print(f"  x {ev}")

    backups = sorted(os.listdir(BACKUP_DIR)) if os.path.isdir(BACKUP_DIR) else []
    logs = sorted(os.listdir(LOG_DIR)) if os.path.isdir(LOG_DIR) else []
    print(f"[BACKUPS] Count: {len(backups)} | Latest: {backups[-1] if backups else 'none'}")
    print(f"[LOGS] Count: {len(logs)} | Files: {', '.join(logs) if logs else 'none'}")


if __name__ == "__main__":
    main()

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db_resilience as dbr
import log_resilience as lr

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")
PRE_ARCHIVE_DIR = os.path.join(BACKUP_DIR, "pre_archive")
ARCHIVE_DIR = os.path.join(BACKUP_DIR, "archives")
LOG_PATH = os.path.join(BASE_DIR, "logs", "retention_archive.log")

logger = lr.get_logger("retention_archive", LOG_PATH)


def create_snapshot(db_path: str = DB_PATH, backup_dir: str = BACKUP_DIR, prefix_tag: str = "ingested") -> str:
    os.makedirs(backup_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    prefix = os.path.join(backup_dir, f"{prefix_tag}_{stamp}")

    src = dbr.connect_db(db_path)
    dst = dbr.connect_db(f"{prefix}.db")
    src.backup(dst)
    dbr.checkpoint_and_close(dst)

    rows = [dict(r) for r in src.execute("SELECT * FROM received_logs ORDER BY log_id ASC").fetchall()]
    dbr.checkpoint_and_close(src)

    with open(f"{prefix}.json", "w", encoding="utf-8") as jf:
        json.dump(rows, jf, indent=2)

    fieldnames = list(rows[0].keys()) if rows else ["log_id"]
    with open(f"{prefix}.csv", "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Snapshot created: {prefix}.db ({len(rows)} rows) + .json + .csv")
    return prefix


def archive_rows(
    older_than_days: int | None = None,
    keep_latest: int | None = None,
    dry_run: bool = False,
    safety_snapshot: bool = True,
) -> int:
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    conn = dbr.connect_db(DB_PATH)
    cur = conn.cursor()

    if keep_latest is not None:
        cutoff_row = cur.execute(
            "SELECT log_id FROM received_logs ORDER BY log_id DESC LIMIT 1 OFFSET ?",
            (max(0, keep_latest - 1),),
        ).fetchone()
        if not cutoff_row:
            dbr.checkpoint_and_close(conn)
            logger.info("Nothing to archive (fewer rows than keep_latest).")
            return 0
        min_keep_id = cutoff_row["log_id"]
        candidates = [
            dict(r)
            for r in cur.execute(
                "SELECT * FROM received_logs WHERE log_id < ? ORDER BY log_id ASC",
                (min_keep_id,),
            ).fetchall()
        ]
    else:
        days = 30 if older_than_days is None else older_than_days
        cutoff_ts = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        candidates = [
            dict(r)
            for r in cur.execute(
                "SELECT * FROM received_logs WHERE COALESCE(received_at, timestamp, '') < ? ORDER BY log_id ASC",
                (cutoff_ts,),
            ).fetchall()
        ]

    if not candidates:
        dbr.checkpoint_and_close(conn)
        logger.info("No rows matched retention archive criteria.")
        return 0

    if dry_run:
        dbr.checkpoint_and_close(conn)
        logger.info(f"[DRY-RUN] {len(candidates)} row(s) eligible for archiving.")
        return len(candidates)

    if safety_snapshot:
        snap_prefix = create_snapshot(DB_PATH, PRE_ARCHIVE_DIR, prefix_tag="pre_archive")
        logger.info(f"Pre-archive safety snapshot saved at {snap_prefix}.*")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_db = os.path.join(ARCHIVE_DIR, f"archive_{stamp}.db")
    arc_conn = dbr.connect_db(archive_db)
    arc_cur = arc_conn.cursor()
    arc_cur.execute("""
        CREATE TABLE IF NOT EXISTS archived_logs (
            log_id INTEGER PRIMARY KEY,
            schema_version INTEGER,
            timestamp TEXT,
            unit_id TEXT,
            metric_name TEXT,
            metric_value REAL,
            severity TEXT,
            notes TEXT,
            raw_json TEXT,
            received_at TEXT,
            archived_at TEXT
        )
    """)

    archived_at = datetime.now(timezone.utc).isoformat()
    ids_to_delete = []
    for r in candidates:
        arc_cur.execute(
            """
            INSERT OR IGNORE INTO archived_logs
            (log_id, schema_version, timestamp, unit_id, metric_name, metric_value, severity, notes, raw_json, received_at, archived_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r.get("log_id"),
                r.get("schema_version", 1),
                r.get("timestamp"),
                r.get("unit_id"),
                r.get("metric_name"),
                r.get("metric_value"),
                r.get("severity"),
                r.get("notes"),
                r.get("raw_json"),
                r.get("received_at"),
                archived_at,
            ),
        )
        ids_to_delete.append(r["log_id"])

    arc_conn.commit()
    dbr.checkpoint_and_close(arc_conn)

    placeholders = ",".join("?" for _ in ids_to_delete)
    cur.execute(f"DELETE FROM received_logs WHERE log_id IN ({placeholders})", ids_to_delete)
    conn.commit()
    dbr.checkpoint_and_close(conn)

    logger.info(f"Archived {len(ids_to_delete)} row(s) into {archive_db} and pruned from active DB.")
    return len(ids_to_delete)


def main():
    parser = argparse.ArgumentParser(description="Telemetry Backup & Retention Archiver")
    parser.add_argument("--snapshot", action="store_true", help="Create timestamped .db, .json, and .csv backup")
    parser.add_argument("--days", type=int, default=30, help="Archive records older than N days (default: 30)")
    parser.add_argument("--keep-latest", type=int, default=None, help="Keep only the newest N rows in active DB")
    parser.add_argument("--dry-run", action="store_true", help="Preview matching rows without modifying DB")
    parser.add_argument("--no-safety-snapshot", action="store_true", help="Skip automatic pre-archive snapshot")
    args = parser.parse_args()

    if args.snapshot:
        create_snapshot()
    if args.keep_latest is not None or args.dry_run:
        archive_rows(
            older_than_days=args.days,
            keep_latest=args.keep_latest,
            dry_run=args.dry_run,
            safety_snapshot=not args.no_safety_snapshot,
        )


if __name__ == "__main__":
    main()

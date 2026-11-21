# Runbook 02: SQLite WAL & Backup Integrity

## Symptom
`database is locked`, large `.db-wal` files, or post-disconnect checks.

## Fix
1. `db_resilience.py` sets `WAL` + `busy_timeout=5000`; `http_receiver.py` runs `PRAGMA wal_checkpoint(TRUNCATE)` on exit.
2. Run `python3 sql_window_views.py` (Option `[12]`) to verify `PRAGMA integrity_check`.
3. Run `python3 native_ops_tools.py` (Option `[11]`) to check `backups/SHA256SUMS.txt`.

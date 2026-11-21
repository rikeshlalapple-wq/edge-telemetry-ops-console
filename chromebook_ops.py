import argparse
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db_resilience as dbr
import field_dashboard as fd
import incident_briefing as ib
import native_ops_tools as notools
import ops_health as oh
import retention_archive as ra
import sql_window_views as swv
import telemetry_analytics as ta
import term_ui as tui
import triage_stream as ts
import tunnel_resilience as tr


def show_schema_and_rejections():
    print(tui.banner("SCHEMA & REJECTION AUDIT"))
    if os.path.exists(oh.DB_PATH):
        conn = dbr.connect_db(oh.DB_PATH)
        cur = conn.cursor()
        schema_summary = oh.summarize_schema_versions(cur)
        total_rows = cur.execute("SELECT COUNT(*) FROM received_logs").fetchone()[0]
        dbr.checkpoint_and_close(conn)
        print(f"[{tui.paint('SCHEMA', tui.BOLD, tui.CYAN)}] Active rows: {total_rows} | Distribution: {schema_summary}")
    else:
        print(f"[{tui.severity_badge('WARN')}] ingested_telemetry.db not found.")

    rej_count, rej_events = oh.summarize_rejections(oh.RECEIVER_LOG_PATH)
    rej_tag = tui.severity_badge("HIGH") if rej_count > 0 else tui.severity_badge("OK")
    print(f"[{rej_tag}] Total rejected records logged: {rej_count}")
    if rej_events:
        print(tui.section("RECENT REJECTION / VALIDATION LOG LINES"))
        for line in rej_events:
            print(f"  {tui.paint('x', tui.RED)} {line}")
    else:
        print(tui.paint("  (No rejected payloads logged.)", tui.DIM))


def run_console(use_alt_screen: bool = False):
    with tui.AltScreen(enabled=use_alt_screen):
        try:
            while True:
                print(tui.banner("CHROMEBOOK TELEMETRY & OPS CONSOLE"))
                print("[1] Telemetry Dashboard & Active Alerts")
                print("[2] Workstation Ops Health Check")
                print("[3] Create Full Backup Snapshot (.db/.json/.csv)")
                print("[4] Dry-Run Archive (older than 30 days)")
                print("[5] Execute Archive (older than 30 days)")
                print("[6] Resilient Sync Edge Telemetry")
                print("[7] Schema & Rejection Audit")
                print("[8] Anomaly & Drift Analytics")
                print("[9] Filter & Live Triage Stream")
                print("[10] Rule-Hybrid Incident Briefing")
                print("[11] Native Linux & Checksum Audit")
                print("[12] SQLite Window Views & Integrity")
                print("[Q] Quit")
                choice = input(tui.paint("Select> ", tui.BOLD, tui.GREEN)).strip().lower()

                if choice == "1":
                    fd.main()
                elif choice == "2":
                    oh.main()
                elif choice == "3":
                    ra.create_snapshot()
                elif choice == "4":
                    ra.archive_rows(older_than_days=30, dry_run=True)
                elif choice == "5":
                    confirm = input("Archive rows older than 30 days into ~/backups/archives? (y/N): ").strip().lower()
                    if confirm == "y":
                        ra.archive_rows(older_than_days=30, dry_run=False)
                    else:
                        print(f"[{tui.severity_badge('WARN')}] Archive not executed.")
                elif choice == "6":
                    tr.run_resilient_sync("sync")
                elif choice == "7":
                    show_schema_and_rejections()
                elif choice == "8":
                    ta.run_analytics()
                elif choice == "9":
                    ts.interactive_filter_prompt()
                elif choice == "10":
                    ib.run_briefing(use_llm=True)
                elif choice == "11":
                    notools.run_native_audit()
                elif choice == "12":
                    swv.run_window_report()
                elif choice == "q":
                    print(f"[{tui.severity_badge('OK')}] Exiting Chromebook Ops Console.")
                    break
                else:
                    print(f"[{tui.severity_badge('WARN')}] Invalid option.")
        except KeyboardInterrupt:
            print(f"\n[{tui.severity_badge('OK')}] Interrupted cleanly — exiting console.")


def main():
    parser = argparse.ArgumentParser(description="Chromebook Telemetry & Ops Console")
    parser.add_argument("--alt-screen", action="store_true", help="Run inside alternate terminal screen buffer")
    args = parser.parse_args()
    env_alt = os.environ.get("TELEMETRY_ALT_SCREEN", "").lower() in ("1", "true", "yes")
    run_console(use_alt_screen=args.alt_screen or env_alt)


if __name__ == "__main__":
    main()

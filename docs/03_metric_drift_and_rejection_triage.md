# Runbook 03: Drift & Rejection Triage

## Symptom
High `REJECTED` counts or `DRIFT_UP` / `OUTLIER` flags.

## Fix
1. Check schema/logs (Option `[7]`) for malformed payloads.
2. Filter live events: `python3 triage_stream.py --severity HIGH --watch`
3. Check rolling median/IQR/MAD drift: `python3 telemetry_analytics.py`
4. Generate handoff summary: `python3 incident_briefing.py`

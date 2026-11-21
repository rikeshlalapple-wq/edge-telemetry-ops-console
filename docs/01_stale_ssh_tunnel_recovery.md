# Runbook 01: Stale SSH Tunnel Recovery

## Symptom
Sync fails with `Address already in use` on `8090` or `Connection refused`.

## Fix
1. Start `python3 http_receiver.py` if port `8090` is not listening (`ss -tlnp | grep 8090`).
2. Clear stale forwards and resync: `python3 tunnel_resilience.py sync`
3. Verify rows in Console `[1]` or `[2]`.

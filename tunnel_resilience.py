import json
import os
import subprocess
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(BASE_DIR, "ops_config.json")


def load_sync_config() -> dict:
    defaults = {
        "chromebook_host": "127.0.0.1",
        "receiver_port": 8090,
        "termux_ssh_port": 8022,
        "termux_primary_ip": "192.0.2.10",
        "termux_mesh_ip": "100.64.0.10",
        "termux_user": "edge_operator",
    }
    if os.path.exists(CFG_PATH):
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            cb = cfg.get("chromebook", {})
            tx = cfg.get("termux", {})
            defaults["chromebook_host"] = cb.get("host", defaults["chromebook_host"])
            defaults["receiver_port"] = int(cb.get("port", defaults["receiver_port"]))
            defaults["termux_ssh_port"] = int(tx.get("ssh_port", defaults["termux_ssh_port"]))
            defaults["termux_primary_ip"] = tx.get("primary_ip", defaults["termux_primary_ip"])
            defaults["termux_mesh_ip"] = tx.get("mesh_ip", defaults["termux_mesh_ip"])
        except Exception:
            pass
    return defaults


def clear_stale_forward(port: int) -> None:
    patterns = [
        f"{port}:127.0.0.1:{port}",
        f"{port}:localhost:{port}",
    ]
    for pat in patterns:
        subprocess.run(["pkill", "-f", pat], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    time.sleep(0.25)


def select_termux_target(cfg: dict) -> str:
    candidates = [cfg["termux_primary_ip"], cfg["termux_mesh_ip"]]
    for ip in candidates:
        if not ip:
            continue
        res = subprocess.run(
            ["ping", "-c", "1", "-W", "1", ip],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if res.returncode == 0:
            return ip
    return cfg["termux_primary_ip"]


def run_resilient_sync(sync_cmd: str = "sync") -> int:
    cfg = load_sync_config()
    port = cfg["receiver_port"]
    clear_stale_forward(port)

    cmd = f"source ~/.bashrc 2>/dev/null || true; {sync_cmd}"
    proc = subprocess.run(["bash", "-lc", cmd], text=True, capture_output=True, check=False)

    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="")

    if proc.returncode != 0 and "remote port forwarding failed" in (proc.stdout + proc.stderr).lower():
        print(f"[TUNNEL] Port {port} collision detected — clearing stale forward and retrying once...")
        clear_stale_forward(port)
        retry = subprocess.run(["bash", "-lc", cmd], text=True, capture_output=True, check=False)
        if retry.stdout:
            print(retry.stdout, end="")
        if retry.stderr:
            print(retry.stderr, end="")
        return retry.returncode

    return proc.returncode

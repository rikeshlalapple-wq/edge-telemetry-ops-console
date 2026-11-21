import hashlib
import os
import shutil
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(BASE_DIR, "backups")


def _run_cmd(args: list[str], timeout: float = 4.0) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            args,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        combined = out if out else err
        return proc.returncode, combined
    except FileNotFoundError:
        return 127, f"command_not_found:{args[0]}"
    except subprocess.TimeoutExpired:
        return 124, f"timeout:{' '.join(args)}"
    except Exception as exc:
        return 1, f"error:{exc}"


def get_disk_summary(path: str = BASE_DIR) -> str:
    if shutil.which("df"):
        rc, out = _run_cmd(["df", "-h", path])
        if rc == 0 and out:
            lines = out.splitlines()
            return lines[-1] if len(lines) > 1 else lines[0]
    usage = shutil.disk_usage(path)
    used_pct = (usage.used / usage.total * 100.0) if usage.total else 0.0
    free_gb = usage.free / (1024**3)
    return f"disk {path}: {free_gb:.1f}G free ({used_pct:.1f}% used)"


def get_uptime_summary() -> str:
    if shutil.which("uptime"):
        rc, out = _run_cmd(["uptime", "-p"])
        if rc == 0 and out:
            return out
        rc2, out2 = _run_cmd(["uptime"])
        if rc2 == 0 and out2:
            return out2.strip()
    if os.path.exists("/proc/uptime"):
        with open("/proc/uptime", "r", encoding="utf-8") as f:
            secs = float(f.read().split()[0])
        mins = int(secs // 60)
        hrs, rem_m = divmod(mins, 60)
        days, rem_h = divmod(hrs, 24)
        return f"up {days}d {rem_h}h {rem_m}m"
    return "uptime unavailable"


def check_listener_8090() -> str:
    if shutil.which("ss"):
        rc, out = _run_cmd(["ss", "-tln"])
        if rc == 0 and ":8090" in out:
            return "active (:8090 listening via ss)"
    if os.path.exists("/proc/net/tcp"):
        target_hex = f"{8090:04X}".upper()
        try:
            with open("/proc/net/tcp", "r", encoding="utf-8") as f:
                for line in f.readlines()[1:]:
                    parts = line.split()
                    if len(parts) >= 4 and parts[3] == "0A":
                        local_port_hex = parts[1].split(":")[1].upper()
                        if local_port_hex == target_hex:
                            return "active (:8090 LISTEN in /proc/net/tcp)"
        except OSError:
            pass
    return "not_listening (:8090)"


def compute_sha256(file_path: str) -> str | None:
    if not os.path.isfile(file_path):
        return None
    if shutil.which("sha256sum"):
        rc, out = _run_cmd(["sha256sum", file_path], timeout=10.0)
        if rc == 0 and out:
            return out.split()[0]
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def write_backup_checksums(backup_dir: str = BACKUP_DIR) -> str | None:
    if not os.path.isdir(backup_dir):
        return None
    manifest_path = os.path.join(backup_dir, "SHA256SUMS.txt")
    entries: list[str] = []
    for fname in sorted(os.listdir(backup_dir)):
        if fname == "SHA256SUMS.txt":
            continue
        fpath = os.path.join(backup_dir, fname)
        if os.path.isfile(fpath) and fname.endswith((".db", ".json", ".csv", ".gz")):
            digest = compute_sha256(fpath)
            if digest:
                entries.append(f"{digest}  {fname}")
    with open(manifest_path, "w", encoding="utf-8") as mf:
        mf.write("\n".join(entries) + ("\n" if entries else ""))
    return manifest_path


def run_native_audit() -> None:
    print("\n=== NATIVE LINUX & SHELL DIAGNOSTICS ===")
    print(f"[UPTIME]  {get_uptime_summary()}")
    print(f"[DISK]    {get_disk_summary(BASE_DIR)}")
    print(f"[SOCKET]  {check_listener_8090()}")
    manifest = write_backup_checksums(BACKUP_DIR)
    if manifest:
        print(f"[SHA256]  Manifest updated -> {manifest}")


if __name__ == "__main__":
    run_native_audit()

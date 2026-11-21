import os
import shutil
import sys

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"

ALT_SCREEN_ENTER = "\033[?1049h\033[H\033[2J"
ALT_SCREEN_EXIT = "\033[?1049l"


def color_enabled() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("TELEMETRY_NO_COLOR", "").lower() in ("1", "true", "yes"):
        return False
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def paint(text: str, *codes: str) -> str:
    if not color_enabled() or not codes:
        return text
    return "".join(codes) + text + RESET


def severity_badge(severity: str | None) -> str:
    sev = (severity or "INFO").upper()
    if sev in ("CRITICAL", "CRIT", "HIGH", "ERROR"):
        return paint(sev, BOLD, RED)
    if sev in ("WARN", "WARNING", "DRIFT_UP", "DRIFT_DOWN", "LOW", "WEAK"):
        return paint(sev, BOLD, YELLOW)
    if sev in ("OK", "GOOD", "HEALTHY", "ACTIVE", "YES", "STABLE"):
        return paint(sev, GREEN)
    return paint(sev, CYAN)


def banner(title: str, char: str = "=") -> str:
    width = shutil.get_terminal_size((88, 24)).columns
    span = max(24, min(width - 2, 100))
    line = char * span
    return f"\n{paint(line, DIM, CYAN)}\n{paint(title, BOLD, CYAN)}\n{paint(line, DIM, CYAN)}"


def section(title: str) -> str:
    return paint(f"\n--- {title} ---", BOLD, BLUE)


class AltScreen:
    """Context manager for optional alternate-screen buffer control."""

    def __init__(self, enabled: bool = False):
        self.enabled = bool(enabled and color_enabled())
        self._entered = False

    def __enter__(self):
        if self.enabled:
            sys.stdout.write(ALT_SCREEN_ENTER)
            sys.stdout.flush()
            self._entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._entered:
            sys.stdout.write(ALT_SCREEN_EXIT)
            sys.stdout.flush()
            self._entered = False
        return False

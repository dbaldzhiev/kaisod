"""Utility to perform a single scan (for timers/systemd)."""
from __future__ import annotations

from .app import run_scan


def main() -> None:
    run_scan()


if __name__ == "__main__":
    main()

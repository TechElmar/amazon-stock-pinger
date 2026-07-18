"""Headless runner — runs the stock monitor without the PySide6 GUI.

Use this when you want to run the bot on a server / VPS / Raspberry Pi
where you don't want (or can't have) a desktop window. Reads the same
amazon_monitor_pro.db file the GUI uses, so you can configure products
and settings via the GUI on your PC, then sync the .db file to the
server.

Run:
    py headless.py

Stops on Ctrl+C cleanly.

Environment overrides:
    AMP_SECONDARY_WEBHOOK   optional secondary Discord webhook URL
"""

import asyncio
import signal
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from database import Database
from monitor import MonitorWorker


EASTERN_TZ = ZoneInfo("America/New_York")


def ts() -> str:
    return datetime.now(EASTERN_TZ).strftime("%H:%M:%S")


class FakeSignal:
    """Drop-in for PySide6 Signal — calls a plain Python callback on
    .emit(). Lets MonitorWorker run outside Qt's event loop."""

    def __init__(self, callback=None):
        self._cb = callback

    def emit(self, *args):
        if self._cb:
            try:
                self._cb(*args)
            except Exception as e:
                print(f"[{ts()}] [signal-error] {e}", file=sys.stderr)

    def connect(self, _slot):
        pass  # no-op for headless

    def disconnect(self, _slot=None):
        pass


def main():
    db = Database()
    worker = MonitorWorker(db)

    # Replace the QSignal class attributes with plain callable shims
    # so worker.log.emit(...) etc. route to stdout instead of Qt.
    worker.log = FakeSignal(lambda msg: print(f"[{ts()}] {msg}"))
    worker.status = FakeSignal(lambda s: print(f"[{ts()}] [STATUS] {s}"))
    worker.product_updated = FakeSignal()  # silent
    worker.hit_counted = FakeSignal(
        lambda asin, price: print(f"[{ts()}] [HIT] {asin} @ ${price}")
    )

    # SIGINT/SIGTERM → graceful stop (asyncio loop exits cleanly).
    def _stop(*_):
        print(f"\n[{ts()}] Shutdown signal received — stopping monitor...")
        worker.running = False

    signal.signal(signal.SIGINT, _stop)
    try:
        signal.signal(signal.SIGTERM, _stop)
    except (AttributeError, ValueError):
        # SIGTERM not available on Windows in all contexts.
        pass

    print(f"[{ts()}] Amazon Stock Pinger (headless) starting...")
    print(f"[{ts()}] DB: {db.path}")

    try:
        asyncio.run(worker.run_async())
    except KeyboardInterrupt:
        pass

    print(f"[{ts()}] Monitor stopped.")


if __name__ == "__main__":
    main()

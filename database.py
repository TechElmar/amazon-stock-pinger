import sqlite3
from pathlib import Path
from typing import Tuple

DB_PATH = Path("amazon_monitor_pro.db")
WATCHLIST_PATH = Path("watchlist.txt")

class Database:
    def __init__(self, path=DB_PATH):
        self.path = path
        self.init_db()

    def connect(self):
        return sqlite3.connect(self.path)

    def init_db(self):
        with self.connect() as con:
            cur = con.cursor()
            # target_price: the @everyone ping threshold. A target-hit
            # ping fires when the Amazon price is at/below this. 0 means
            # "never ping — daily digest only".
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    asin TEXT PRIMARY KEY,
                    target_price REAL NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    title TEXT DEFAULT '',
                    last_price TEXT DEFAULT '',
                    last_price_number REAL,
                    stock TEXT DEFAULT '',
                    last_checked TEXT DEFAULT ''
                )
            """)
            # Idempotent column adds for ping-state persistence. SQLite
            # raises OperationalError if the column already exists, which
            # is fine — we just want to ensure it's there for both fresh
            # and existing DBs.
            for col, decl in [
                ("last_pinged_price", "REAL"),
                ("last_pinged_at", "TEXT"),
                ("effective_state", "TEXT"),  # 'in_stock' | 'oos' | NULL
                ("oos_since", "REAL"),        # epoch seconds OOS run began
                ("alert_state", "TEXT"),      # 'armed' | 'fired' | NULL
            ]:
                try:
                    cur.execute(f"ALTER TABLE products ADD COLUMN {col} {decl}")
                except sqlite3.OperationalError:
                    pass
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            defaults = {
                "amazon_domain": "https://www.amazon.ca",
                "discord_webhook": "",
                "check_interval": "1.0",
            }
            for key, value in defaults.items():
                cur.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value))
            con.commit()

    def get_products(self):
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            return [dict(row) for row in con.execute("SELECT * FROM products ORDER BY asin")]

    def update_product_enabled(self, asin, enabled):
        with self.connect() as con:
            con.execute(
                "UPDATE products SET enabled = ? WHERE asin = ?",
                (enabled, asin),
            )
            con.commit()

    def upsert_product(self, asin, target_price=0.0, enabled=1):
        with self.connect() as con:
            con.execute("""
                INSERT INTO products(asin, target_price, enabled)
                VALUES(?, ?, ?)
                ON CONFLICT(asin) DO UPDATE SET
                    target_price=excluded.target_price,
                    enabled=excluded.enabled
            """, (asin, target_price, enabled))
            con.commit()

    def delete_product(self, asin):
        with self.connect() as con:
            con.execute("DELETE FROM products WHERE asin=?", (asin,))
            con.commit()

    def save_ping(self, asin: str, price: float, ts: str) -> None:
        """Persist the most recent ping for this ASIN. Survives restarts
        so the bot doesn't re-ping items already pinged in a previous
        session. Called from MonitorWorker._maybe_announce."""
        with self.connect() as con:
            con.execute(
                "UPDATE products SET last_pinged_price=?, last_pinged_at=? WHERE asin=?",
                (price, ts, asin),
            )
            con.commit()

    def save_effective_state(self, asin: str, state) -> None:
        """Persist the hysteresis-confirmed stock state ('in_stock' or
        'oos'). Loaded on startup so restock detection works correctly
        across bot restarts — without this, an item that went OOS while
        the bot was down would re-ping as "first in-stock" on next run."""
        with self.connect() as con:
            con.execute(
                "UPDATE products SET effective_state=? WHERE asin=?",
                (state, asin),
            )
            con.commit()

    def save_alert_state(self, asin: str, state) -> None:
        """Persist the target-alert latch: 'armed' (will @everyone-ping
        when the item is confirmed at/below target) or 'fired' (already
        pinged; silent until re-armed by a 1h+ OOS comeback or the
        price leaving the target zone). Persisting this is what stops
        always-at-target items from re-pinging on every restart."""
        with self.connect() as con:
            con.execute(
                "UPDATE products SET alert_state=? WHERE asin=?",
                (state, asin),
            )
            con.commit()

    def save_oos_since(self, asin: str, oos_since) -> None:
        """Persist the timestamp (epoch seconds) when the current OOS
        run started, or None to clear it. This is what makes the
        '1 hour OOS' restock rule survive restarts — if an item went
        OOS an hour before the bot was stopped and comes back after
        restart, the persisted oos_since still reflects the real
        duration."""
        with self.connect() as con:
            con.execute(
                "UPDATE products SET oos_since=? WHERE asin=?",
                (oos_since, asin),
            )
            con.commit()

    def update_result(self, asin, title, price, price_number, stock, last_checked):
        with self.connect() as con:
            con.execute("""
                UPDATE products
                SET title=?, last_price=?, last_price_number=?, stock=?, last_checked=?
                WHERE asin=?
            """, (title, price, price_number, stock, last_checked, asin))
            con.commit()

    def get_setting(self, key):
        with self.connect() as con:
            row = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            return row[0] if row else ""

    def set_setting(self, key, value):
        with self.connect() as con:
            con.execute("""
                INSERT INTO settings(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, str(value)))
            con.commit()

    def get_settings(self):
        with self.connect() as con:
            return {key: value for key, value in con.execute("SELECT key, value FROM settings")}

    # ------------------------------------------------------------------
    # watchlist.txt sync — plaintext bulk editor for the products list
    # ------------------------------------------------------------------

    def sync_from_watchlist(self, path: Path = WATCHLIST_PATH) -> Tuple[int, int, int]:
        """Read a watchlist.txt file and make the products table match.

        File format (one product per line):
            ASIN              → tracked, digest-only (no ping target)
            ASIN 64.99        → tracked, @everyone pings at ≤ $64.99
            # anything        → comment, ignored
            (blank line)      → ignored

        Behavior:
          - ASINs in the file but not the DB → inserted (enabled=1)
          - ASINs in the DB but not in the file → DELETED
          - ASINs in both → target updated to the file's value

        Returns (added, removed, updated). Returns (0, 0, 0) with no DB
        changes if the file is missing — treats DB as source of truth
        when there's no watchlist file yet.
        """
        if not path.exists():
            return 0, 0, 0

        file_products: dict = {}
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            asin = parts[0].strip().upper()
            if len(asin) != 10 or not asin.isalnum():
                continue
            target = 0.0
            if len(parts) >= 2:
                try:
                    target = float(parts[1].lstrip("$"))
                except ValueError:
                    target = 0.0
            file_products[asin] = target

        existing = {p["asin"]: p for p in self.get_products()}

        added = removed = updated = 0
        for asin, target in file_products.items():
            prev = existing.get(asin)
            if prev is None:
                self.upsert_product(asin, target, 1)
                added += 1
            elif float(prev.get("target_price") or 0) != target:
                self.upsert_product(asin, target, int(prev.get("enabled", 1)))
                updated += 1
        for asin in existing.keys() - file_products.keys():
            self.delete_product(asin)
            removed += 1

        return added, removed, updated

    def export_watchlist(self, path: Path = WATCHLIST_PATH) -> int:
        """Write the current products table to a watchlist.txt file.
        Useful for seeding the file from an existing DB. Returns the
        number of lines written."""
        lines = [
            "# Amazon Stock Pinger — watchlist",
            "# Format: ASIN [ping_target]",
            "#   ASIN alone      → daily-digest only, never @everyone-pings",
            "#   ASIN 64.99      → @everyone ping when Amazon price ≤ $64.99",
            "# Lines starting with # are ignored. Edit while the bot is",
            "# stopped, then restart — the products table syncs to match",
            "# this file (additions, removals, target changes).",
            "",
        ]
        products = sorted(self.get_products(), key=lambda p: p["asin"])
        for p in products:
            target = float(p.get("target_price") or 0)
            if target > 0:
                lines.append(f"{p['asin']} {target:.2f}")
            else:
                lines.append(p["asin"])
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return len(products)

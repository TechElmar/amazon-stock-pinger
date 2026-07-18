import sys
import json
import urllib.request
from datetime import datetime
from threading import Thread
from zoneinfo import ZoneInfo
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QDoubleSpinBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, QTabWidget,
    QTextEdit, QFrame, QSizePolicy, QAbstractItemView
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont, QColor
from database import Database
from monitor import MonitorWorker

# Eastern timezone (auto-handles EST/EDT daylight saving).
EASTERN_TZ = ZoneInfo("America/New_York")
# Max log lines kept in the buffer.
LOG_BUFFER_CAP = 10000

# Optional: hardcoded Discord log dump webhook. Leave empty to disable.
# When set, the last DISCORD_LOG_LINES log lines get posted to this
# webhook every DISCORD_LOG_INTERVAL_MS milliseconds.
DISCORD_LOG_WEBHOOK = ""
DISCORD_LOG_INTERVAL_MS = 30 * 60 * 1000
DISCORD_LOG_LINES = 50
DISCORD_MSG_CHAR_LIMIT = 1900


STYLE = """
QMainWindow, QDialog { background: #0a0e1a; }

QWidget {
    color: #e2e8f0;
    font-family: 'Segoe UI', sans-serif;
    font-size: 13px;
}

QLabel { background: transparent; }

QLabel#title {
    font-size: 28px;
    font-weight: 800;
    color: #f8fafc;
    letter-spacing: -0.5px;
}

QLabel#subtitle {
    font-size: 12px;
    color: #64748b;
    font-weight: 500;
}

QLabel#statValue {
    font-size: 30px;
    font-weight: 800;
    color: #f1f5f9;
}

QLabel#statLabel {
    font-size: 10px;
    color: #94a3b8;
    font-weight: 700;
    letter-spacing: 1.2px;
}

QLabel#sectionLabel {
    font-size: 11px;
    color: #94a3b8;
    font-weight: 700;
    letter-spacing: 1.2px;
    padding: 4px 0;
}

QLabel#fieldLabel {
    color: #94a3b8;
    font-weight: 600;
    font-size: 12px;
}

QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {
    background: #111827;
    border: 1px solid #1e293b;
    border-radius: 10px;
    padding: 9px 12px;
    color: #f1f5f9;
    selection-background-color: #2563eb;
}

QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {
    border-color: #3b82f6;
}

QPushButton {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #3b82f6, stop:1 #2563eb);
    border: none;
    border-radius: 10px;
    padding: 11px 20px;
    color: white;
    font-weight: 600;
}

QPushButton:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #60a5fa, stop:1 #3b82f6);
}

QPushButton:pressed { background: #1d4ed8; }

QPushButton#danger {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #ef4444, stop:1 #dc2626);
}

QPushButton#danger:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #f87171, stop:1 #ef4444);
}

QPushButton#secondary {
    background: #1e293b;
    color: #cbd5e1;
    padding: 8px 14px;
}

QPushButton#secondary:hover { background: #334155; }

QPushButton#rowDelete {
    background: transparent;
    border: 1px solid #7f1d1d;
    color: #fca5a5;
    padding: 5px 10px;
    border-radius: 6px;
    font-size: 11px;
    font-weight: 600;
}

QPushButton#rowDelete:hover {
    background: rgba(220, 38, 38, 0.2);
    border-color: #dc2626;
    color: #fecaca;
}

QPushButton#toggle {
    background: #334155;
    border: 1px solid #475569;
    border-radius: 12px;
    padding: 5px 14px;
    color: #94a3b8;
    font-weight: 700;
    font-size: 10px;
    letter-spacing: 1px;
}

QPushButton#toggle:checked {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
        stop:0 #22c55e, stop:1 #16a34a);
    border-color: #16a34a;
    color: white;
}

QPushButton#toggle:hover { border-color: #64748b; }
QPushButton#toggle:checked:hover { border-color: #22c55e; }

QTableWidget {
    background: #0f172a;
    border: 1px solid #1e293b;
    border-radius: 12px;
    gridline-color: transparent;
    color: #e2e8f0;
}

QTableWidget::item {
    padding: 4px;
    border-bottom: 1px solid #1e293b;
}

QTableWidget::item:selected {
    background: rgba(59, 130, 246, 0.12);
    color: #f1f5f9;
}

QHeaderView::section {
    background: #1e293b;
    color: #94a3b8;
    padding: 10px 8px;
    border: none;
    font-weight: 700;
    font-size: 10px;
    letter-spacing: 1px;
}

QFrame#card {
    background: #0f172a;
    border: 1px solid #1e293b;
    border-radius: 14px;
}

QFrame#statCard {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 #131826, stop:1 #0f172a);
    border: 1px solid #1e293b;
    border-radius: 14px;
}

QFrame#runningBadge {
    background: rgba(34, 197, 94, 0.15);
    border: 1px solid rgba(34, 197, 94, 0.4);
    border-radius: 12px;
}

QFrame#stoppedBadge {
    background: rgba(100, 116, 139, 0.15);
    border: 1px solid rgba(100, 116, 139, 0.3);
    border-radius: 12px;
}

QLabel#runningText { color: #4ade80; font-weight: 600; font-size: 12px; }
QLabel#stoppedText { color: #94a3b8; font-weight: 600; font-size: 12px; }

QTabWidget::pane { border: none; background: transparent; }

QTabBar::tab {
    background: transparent;
    color: #64748b;
    padding: 10px 24px;
    border-radius: 10px;
    margin: 2px;
    font-weight: 600;
}

QTabBar::tab:hover {
    background: #1e293b;
    color: #cbd5e1;
}

QTabBar::tab:selected {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
        stop:0 #3b82f6, stop:1 #2563eb);
    color: white;
}

QTextEdit {
    background: #0a0e1a;
    border: 1px solid #1e293b;
    border-radius: 12px;
    padding: 12px;
    font-family: 'Consolas', monospace;
    font-size: 12px;
    color: #cbd5e1;
}

QScrollBar:vertical {
    background: transparent;
    width: 8px;
    margin: 0;
}

QScrollBar::handle:vertical {
    background: #334155;
    border-radius: 4px;
    min-height: 30px;
}

QScrollBar::handle:vertical:hover { background: #475569; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background: transparent; }
"""


def stock_color(stock):
    s = (stock or "").lower()
    if "out of stock" in s:
        return "#94a3b8"
    if "in stock" in s:
        return "#4ade80"
    if "blocked" in s or "captcha" in s:
        return "#fb923c"
    if "http" in s or "error" in s:
        return "#f87171"
    if "likely" in s or "pre-order" in s:
        return "#fde047"
    return "#cbd5e1"


def make_card():
    f = QFrame()
    f.setObjectName("card")
    return f


class StatCard(QFrame):
    def __init__(self, label, value="0"):
        super().__init__()
        self.setObjectName("statCard")
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 14, 20, 14)
        lay.setSpacing(2)
        self.value_label = QLabel(value)
        self.value_label.setObjectName("statValue")
        self.label_label = QLabel(label)
        self.label_label.setObjectName("statLabel")
        lay.addWidget(self.value_label)
        lay.addWidget(self.label_label)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

    def set_value(self, v):
        self.value_label.setText(str(v))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = Database()
        self.worker = None
        self._loading = False
        self._row_widgets = {}
        self._log_buffer: list[str] = []
        self.setWindowTitle("Amazon Stock Pinger")
        self.resize(1200, 820)
        self.setStyleSheet(STYLE)
        self.build_ui()
        self.load_settings()
        self.refresh_products()

        # Optional Discord log dump timer — only runs when webhook set.
        if DISCORD_LOG_WEBHOOK:
            self._discord_log_timer = QTimer(self)
            self._discord_log_timer.timeout.connect(self._post_logs_to_discord)
            self._discord_log_timer.start(DISCORD_LOG_INTERVAL_MS)
            startup_ts = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")
            self._send_to_log_webhook(
                f"🟢 **Amazon Stock Pinger started** — {startup_ts} EST\n"
                f"Log dumps will post here every "
                f"{DISCORD_LOG_INTERVAL_MS // 60000} minutes."
            )

    def _send_to_log_webhook(self, messages):
        if isinstance(messages, str):
            messages = [messages]
        if not messages or not DISCORD_LOG_WEBHOOK:
            return

        def _send():
            for content in messages:
                if not content:
                    continue
                try:
                    data = json.dumps({"content": content}).encode("utf-8")
                    # Cloudflare 403's requests without a UA.
                    req = urllib.request.Request(
                        DISCORD_LOG_WEBHOOK,
                        data=data,
                        headers={
                            "Content-Type": "application/json",
                            "User-Agent": "AmazonStockPinger/1.0",
                        },
                        method="POST",
                    )
                    urllib.request.urlopen(req, timeout=10).close()
                except Exception:
                    pass

        Thread(target=_send, daemon=True).start()

    def _post_logs_to_discord(self):
        if not self._log_buffer:
            return
        lines = list(self._log_buffer[-DISCORD_LOG_LINES:])
        now = datetime.now(EASTERN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        header = f"📋 **Log dump** — {now} EST ({len(lines)} recent lines)"

        chunks: list[str] = []
        current: list[str] = []
        current_size = 0
        for line in lines:
            line_len = len(line) + 1
            if current_size + line_len > DISCORD_MSG_CHAR_LIMIT and current:
                chunks.append("\n".join(current))
                current = []
                current_size = 0
            current.append(line)
            current_size += line_len
        if current:
            chunks.append("\n".join(current))

        messages = [header] + [f"```\n{chunk}\n```" for chunk in chunks]
        self._send_to_log_webhook(messages)

    def build_ui(self):
        root = QWidget()
        layout = QVBoxLayout(root)
        layout.setContentsMargins(22, 22, 22, 22)
        layout.setSpacing(16)

        header = QHBoxLayout()
        title_block = QVBoxLayout()
        title_block.setSpacing(2)
        title = QLabel("Amazon Stock Pinger")
        title.setObjectName("title")
        subtitle = QLabel("Real-time stock + price monitoring with Discord alerts")
        subtitle.setObjectName("subtitle")
        title_block.addWidget(title)
        title_block.addWidget(subtitle)
        header.addLayout(title_block)
        header.addStretch()

        self.status_badge = QFrame()
        self.status_badge.setObjectName("stoppedBadge")
        badge_layout = QHBoxLayout(self.status_badge)
        badge_layout.setContentsMargins(14, 6, 14, 6)
        self.status_label = QLabel("● Stopped")
        self.status_label.setObjectName("stoppedText")
        badge_layout.addWidget(self.status_label)
        header.addWidget(self.status_badge)
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.addTab(self.dashboard_tab(), "Dashboard")
        self.tabs.addTab(self.products_tab(), "Products")
        self.tabs.addTab(self.settings_tab(), "Settings")
        self.tabs.addTab(self.logs_tab(), "Logs")
        layout.addWidget(self.tabs)

        self.setCentralWidget(root)

    def dashboard_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(16)

        stats_row = QHBoxLayout()
        stats_row.setSpacing(12)
        self.stat_total = StatCard("TOTAL PRODUCTS", "0")
        self.stat_enabled = StatCard("ACTIVE", "0")
        self.stat_disabled = StatCard("PAUSED", "0")
        self.stat_hits = StatCard("TARGET HITS THIS SESSION", "0")
        for s in (self.stat_total, self.stat_enabled, self.stat_disabled, self.stat_hits):
            stats_row.addWidget(s, 1)
        layout.addLayout(stats_row)

        ctrl_card = make_card()
        ctrl_layout = QVBoxLayout(ctrl_card)
        ctrl_layout.setContentsMargins(20, 18, 20, 18)
        ctrl_layout.setSpacing(12)

        section = QLabel("MONITOR CONTROL")
        section.setObjectName("sectionLabel")
        ctrl_layout.addWidget(section)

        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("▶  Start Monitoring")
        self.stop_btn = QPushButton("◼  Stop")
        self.stop_btn.setObjectName("danger")
        self.start_btn.clicked.connect(self.start_monitoring)
        self.stop_btn.clicked.connect(self.stop_monitoring)
        btn_row.addWidget(self.start_btn)
        btn_row.addWidget(self.stop_btn)
        btn_row.addStretch()
        ctrl_layout.addLayout(btn_row)
        layout.addWidget(ctrl_card)

        layout.addStretch()
        return tab

    def products_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(14)

        add_card = make_card()
        add_layout = QGridLayout(add_card)
        add_layout.setContentsMargins(18, 16, 18, 16)
        add_layout.setHorizontalSpacing(10)
        add_layout.setVerticalSpacing(8)

        section = QLabel("ADD PRODUCT")
        section.setObjectName("sectionLabel")
        add_layout.addWidget(section, 0, 0, 1, 4)

        self.asin_input = QLineEdit()
        self.asin_input.setPlaceholderText("ASIN  (e.g. B0G3CY83L5)")
        self.asin_input.returnPressed.connect(self.add_product)
        self.target_input = QDoubleSpinBox()
        self.target_input.setMaximum(99999)
        self.target_input.setDecimals(2)
        self.target_input.setPrefix("$ ")
        self.target_input.setValue(0)
        self.target_input.setToolTip(
            "@everyone ping target: fires when the Amazon price is at or "
            "below this. $0 = never ping — item appears in the daily "
            "digest only."
        )

        add_layout.addWidget(self.asin_input, 1, 0, 1, 2)
        add_layout.addWidget(self.target_input, 1, 2)

        add_btn = QPushButton("+ Add Product")
        add_btn.clicked.connect(self.add_product)
        add_layout.addWidget(add_btn, 1, 3)

        add_layout.setColumnStretch(0, 1)
        add_layout.setColumnStretch(1, 1)
        layout.addWidget(add_card)

        # Table — inline-editable ping target, toggle + delete per row.
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels([
            "ON / OFF", "ASIN", "TITLE", "PRICE", "STOCK",
            "PING TARGET", "LAST CHECK", ""
        ])
        h = self.table.horizontalHeader()
        h.setSectionResizeMode(QHeaderView.ResizeToContents)
        h.setSectionResizeMode(2, QHeaderView.Stretch)  # title stretches
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(48)
        layout.addWidget(self.table)

        return tab

    def settings_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(14)

        card = make_card()
        grid = QGridLayout(card)
        grid.setContentsMargins(20, 20, 20, 20)
        grid.setSpacing(12)

        title = QLabel("APPLICATION SETTINGS")
        title.setObjectName("sectionLabel")
        grid.addWidget(title, 0, 0, 1, 2)

        self.webhook_input = QLineEdit()
        self.webhook_input.setPlaceholderText("https://discord.com/api/webhooks/...")
        self.interval_input = QDoubleSpinBox()
        self.interval_input.setMinimum(0.1)
        self.interval_input.setMaximum(3600)
        self.interval_input.setDecimals(2)
        self.interval_input.setSingleStep(0.1)
        self.interval_input.setSuffix("  sec")
        self.domain_input = QLineEdit()
        self.wishlist_input = QLineEdit()
        self.wishlist_input.setPlaceholderText(
            "Optional — public Amazon wishlist ID (part after /ls/ in URL) "
            "for sub-second restock detection via HTTP fan-out"
        )

        row = 1
        for lbl, widget in [
            ("Discord Webhook", self.webhook_input),
            ("Check Interval", self.interval_input),
            ("Amazon Domain", self.domain_input),
            ("Wishlist ID", self.wishlist_input),
        ]:
            ql = QLabel(lbl)
            ql.setObjectName("fieldLabel")
            grid.addWidget(ql, row, 0)
            grid.addWidget(widget, row, 1)
            row += 1

        grid.setColumnStretch(1, 1)

        save_btn = QPushButton("Save Settings")
        save_btn.clicked.connect(self.save_settings)
        grid.addWidget(save_btn, row, 1)

        layout.addWidget(card)
        layout.addStretch()
        return tab

    def logs_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        ctrl = QHBoxLayout()
        self.log_search = QLineEdit()
        self.log_search.setPlaceholderText(
            "Search logs (case-insensitive — filters live as you type)..."
        )
        self.log_search.textChanged.connect(self._on_log_search_changed)
        self.log_match_label = QLabel("")
        self.log_match_label.setStyleSheet("color: #94a3b8; font-size: 11px;")
        clear_btn = QPushButton("Clear")
        clear_btn.setObjectName("secondary")
        clear_btn.clicked.connect(self._clear_logs)
        ctrl.addWidget(self.log_search, 1)
        ctrl.addWidget(self.log_match_label)
        ctrl.addWidget(clear_btn)
        layout.addLayout(ctrl)

        self.logs = QTextEdit()
        self.logs.setReadOnly(True)
        layout.addWidget(self.logs)
        return tab

    def _clear_logs(self):
        self._log_buffer.clear()
        self.logs.clear()
        self._update_log_match_count()

    def _on_log_search_changed(self):
        self._refresh_log_view()

    def _refresh_log_view(self):
        query = self.log_search.text().strip().lower() if hasattr(self, "log_search") else ""
        if query:
            matched = [ln for ln in self._log_buffer if query in ln.lower()]
        else:
            matched = self._log_buffer
        self.logs.setPlainText("\n".join(matched))
        cursor = self.logs.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.logs.setTextCursor(cursor)
        self._update_log_match_count()

    def _update_log_match_count(self):
        total = len(self._log_buffer)
        query = self.log_search.text().strip().lower() if hasattr(self, "log_search") else ""
        if query:
            shown = sum(1 for ln in self._log_buffer if query in ln.lower())
            self.log_match_label.setText(f"{shown} / {total} match")
        else:
            self.log_match_label.setText(f"{total} lines")

    def load_settings(self):
        s = self.db.get_settings()
        self.webhook_input.setText(s.get("discord_webhook", ""))
        try:
            self.interval_input.setValue(float(s.get("check_interval", "1.0")))
        except (TypeError, ValueError):
            self.interval_input.setValue(1.0)
        self.domain_input.setText(s.get("amazon_domain", "https://www.amazon.ca"))
        self.wishlist_input.setText(s.get("wishlist_id", ""))

    def save_settings(self):
        self.db.set_setting("discord_webhook", self.webhook_input.text().strip())
        self.db.set_setting("check_interval", self.interval_input.value())
        self.db.set_setting("amazon_domain", self.domain_input.text().strip())
        self.db.set_setting("wishlist_id", self.wishlist_input.text().strip())
        QMessageBox.information(self, "Saved", "Settings saved.")

    def _mirror_watchlist(self):
        """Export the current products table to watchlist.txt so the
        two stay in sync. Critical: on startup the monitor calls
        sync_from_watchlist(), which DELETES any DB product missing
        from the file. Without mirroring after each GUI edit, a
        product added/edited in the app would be wiped on the next
        launch because it was never written to the file."""
        try:
            self.db.export_watchlist()
        except Exception as e:
            self.log(f"Could not update watchlist.txt: {e}")

    def add_product(self):
        asin = self.asin_input.text().strip().upper()
        if not asin:
            QMessageBox.warning(self, "Missing ASIN", "Enter an ASIN/SKU.")
            return
        self.db.upsert_product(asin, self.target_input.value(), 1)
        self.asin_input.clear()
        self.refresh_products()
        self._mirror_watchlist()

    def refresh_products(self):
        self._loading = True
        try:
            products = self.db.get_products()
            self.table.setRowCount(0)
            self._row_widgets.clear()
            enabled = 0
            for p in products:
                self._append_product_row(p)
                if p["enabled"]:
                    enabled += 1
            self.stat_total.set_value(len(products))
            self.stat_enabled.set_value(enabled)
            self.stat_disabled.set_value(len(products) - enabled)
        finally:
            self._loading = False

    def _append_product_row(self, p):
        row = self.table.rowCount()
        self.table.insertRow(row)
        asin = p["asin"]

        toggle = QPushButton("ON" if p["enabled"] else "OFF")
        toggle.setObjectName("toggle")
        toggle.setCheckable(True)
        toggle.setChecked(bool(p["enabled"]))
        toggle.clicked.connect(
            lambda checked, a=asin, b=toggle: self._on_toggle(a, b, checked)
        )
        self._wrap_cell(row, 0, toggle, center=True)

        asin_item = QTableWidgetItem(asin)
        asin_item.setFont(QFont("Consolas", 11))
        asin_item.setForeground(QColor("#cbd5e1"))
        self.table.setItem(row, 1, asin_item)

        title_item = QTableWidgetItem(p.get("title") or "—")
        title_item.setForeground(QColor("#e2e8f0"))
        self.table.setItem(row, 2, title_item)

        price_item = QTableWidgetItem(p.get("last_price") or "—")
        self._style_price_item(
            price_item, p.get("last_price_number"), p.get("target_price")
        )
        self.table.setItem(row, 3, price_item)

        stock_text = p.get("stock") or "—"
        stock_item = QTableWidgetItem(stock_text)
        stock_item.setForeground(QColor(stock_color(stock_text)))
        bold = QFont(); bold.setBold(True)
        stock_item.setFont(bold)
        self.table.setItem(row, 4, stock_item)

        tgt = QDoubleSpinBox()
        tgt.setMaximum(99999); tgt.setDecimals(2); tgt.setPrefix("$ ")
        tgt.setValue(float(p.get("target_price") or 0))
        tgt.setToolTip(
            "@everyone ping target: fires when the Amazon price is at "
            "or below this. $0 = never ping — daily digest only."
        )
        tgt.valueChanged.connect(lambda _v, a=asin: self._on_target_change(a))
        self._wrap_cell(row, 5, tgt)

        lc_item = QTableWidgetItem(p.get("last_checked") or "—")
        lc_item.setForeground(QColor("#64748b"))
        self.table.setItem(row, 6, lc_item)

        del_btn = QPushButton("Delete")
        del_btn.setObjectName("rowDelete")
        del_btn.clicked.connect(lambda _, a=asin: self._on_delete(a))
        self._wrap_cell(row, 7, del_btn, center=True)

        self._row_widgets[asin] = {
            "row": row,
            "toggle": toggle, "target": tgt,
            "title_item": title_item, "price_item": price_item,
            "stock_item": stock_item, "lc_item": lc_item,
        }

    def _wrap_cell(self, row, col, widget, center=False):
        wrap = QWidget()
        lay = QHBoxLayout(wrap)
        lay.setContentsMargins(6, 4, 6, 4)
        lay.addWidget(widget)
        if center:
            lay.setAlignment(Qt.AlignCenter)
        self.table.setCellWidget(row, col, wrap)

    def _style_price_item(self, item, price_num=None, target=None):
        """Green-bold when the price is at/below a set ping target."""
        try:
            if (
                price_num is not None
                and target is not None
                and float(target) > 0
                and float(price_num) <= float(target)
            ):
                item.setForeground(QColor("#4ade80"))
                f = QFont(); f.setBold(True); item.setFont(f)
                return
        except (TypeError, ValueError):
            pass
        item.setForeground(QColor("#cbd5e1"))
        item.setFont(QFont())

    def _on_toggle(self, asin, btn, checked):
        if self._loading:
            return
        btn.setText("ON" if checked else "OFF")
        self.db.update_product_enabled(asin, 1 if checked else 0)
        products = self.db.get_products()
        enabled = sum(1 for p in products if p["enabled"])
        self.stat_enabled.set_value(enabled)
        self.stat_disabled.set_value(len(products) - enabled)

    def _on_target_change(self, asin):
        if self._loading:
            return
        w = self._row_widgets.get(asin)
        if not w:
            return
        self.db.upsert_product(
            asin,
            w["target"].value(),
            1 if w["toggle"].isChecked() else 0,
        )
        self._mirror_watchlist()

    def _on_delete(self, asin):
        if QMessageBox.question(
            self, "Delete?", f"Remove {asin} from the list?",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return
        self.db.delete_product(asin)
        self.refresh_products()
        self._mirror_watchlist()

    def log(self, message):
        ts = datetime.now(EASTERN_TZ).strftime("%H:%M:%S")
        line = f"[{ts}] {message}"
        self._log_buffer.append(line)

        if len(self._log_buffer) > LOG_BUFFER_CAP:
            trim_to = int(LOG_BUFFER_CAP * 0.8)
            self._log_buffer = self._log_buffer[-trim_to:]
            self._refresh_log_view()
            return

        query = (
            self.log_search.text().strip().lower()
            if hasattr(self, "log_search")
            else ""
        )
        if not query or query in line.lower():
            self.logs.append(line)
        if hasattr(self, "log_match_label"):
            self._update_log_match_count()

    def on_hit_counted(self, asin: str, price: float):
        """Worker emitted a dedup'd stock-ping. Bump the dashboard
        counter ONCE per detected restock. The dedup happens in
        MonitorWorker via the drop_announced window."""
        try:
            current = int(self.stat_hits.value_label.text())
        except (TypeError, ValueError):
            current = 0
        self.stat_hits.set_value(current + 1)

    def update_product_row(self, payload):
        asin = payload.get("asin")
        w = self._row_widgets.get(asin)
        if not w:
            self.refresh_products()
            return

        self._loading = True
        try:
            stock_text = payload.get("stock") or "—"
            stock_lower = stock_text.lower()
            is_error_state = (
                "timeout" in stock_lower
                or "network error" in stock_lower
                or "connection error" in stock_lower
                or "blocked" in stock_lower
                or "captcha" in stock_lower
                or stock_lower.startswith("http")
                or "all sources cooling down" in stock_lower
            )

            if not is_error_state:
                w["title_item"].setText(payload.get("title") or "—")
                price_text = payload.get("last_price") or payload.get("price") or "—"
                w["price_item"].setText(price_text)
                self._style_price_item(
                    w["price_item"],
                    payload.get("last_price_number") or payload.get("price_number"),
                    payload.get("target_price"),
                )
                w["stock_item"].setText(stock_text)
                w["stock_item"].setForeground(QColor(stock_color(stock_text)))
                bold = QFont(); bold.setBold(True)
                w["stock_item"].setFont(bold)

            w["lc_item"].setText(
                payload.get("last_checked") or payload.get("timestamp") or "—"
            )
        finally:
            self._loading = False

    def set_status(self, text):
        self.status_label.setText(f"● {text}")
        running = text.lower() in ("running", "started")
        self.status_badge.setObjectName("runningBadge" if running else "stoppedBadge")
        self.status_label.setObjectName("runningText" if running else "stoppedText")
        self.status_badge.style().unpolish(self.status_badge)
        self.status_badge.style().polish(self.status_badge)
        self.status_label.style().unpolish(self.status_label)
        self.status_label.style().polish(self.status_label)

    def start_monitoring(self):
        self.save_settings()
        if self.worker and self.worker.isRunning():
            return
        self.worker = MonitorWorker(self.db)
        self.worker.log.connect(self.log)
        self.worker.status.connect(self.set_status)
        self.worker.product_updated.connect(self.update_product_row)
        self.worker.hit_counted.connect(self.on_hit_counted)
        self.worker.start()
        self.log("Monitor started.")

    def stop_monitoring(self):
        if self.worker:
            self.worker.stop()
        self.log("Stopping monitor...")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

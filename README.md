# Amazon Stock Pinger

Fast Amazon stock + price monitor that fires Discord webhooks on real
stock events: first in-stock detection, confirmed restocks, and price
drops. HTTP-only — no browser, no Playwright, no purchase logic. Pure
pinging.

![python](https://img.shields.io/badge/python-3.10+-blue) ![license](https://img.shields.io/badge/license-MIT-green)

## Features

- **HTTP-only scanning** with rotating Chrome/Safari/Firefox TLS
  impersonation via [curl_cffi](https://github.com/lexiforest/curl_cffi).
  Each request looks like a real browser to Amazon's WAF.
- **Public-wishlist HTTP fan-out** — a single wishlist GET returns
  stock for every item in it. Fanning out across the proxy pool with
  cache-busting query params drives sub-second restock detection
  because different CloudFront edges return fresh state.
- **Startup sweep + stock list** — on every start the bot silently
  sweeps the whole watchlist (no pings possible during the sweep),
  then posts one "Amazon Stock Watchlist" message: every purchasable
  item with a 🟢/🔴 price vs the last list, `(was $X)`, ASIN, product
  image, and a LINK button (PokeWatch-style layout, split into
  Part k/n beyond 6 items). Repeats every 24h. No @everyone. Items
  on the list at/below their target are latched — being listed IS
  their announcement, so they can't ping afterward.
- **Target-price pings with an anti-spam latch** — @everyone fires
  ONLY when an item is confirmed in stock, sold by Amazon, at/below
  its per-item target price — and the message never reveals the
  target (reason just says "Target Price Reached"). Each item then
  latches "fired" and stays silent while it just sits at target (no
  more constant pings for always-in-stock items). It re-arms only
  when the item genuinely leaves deal territory — out of stock for
  ≥1 hour, or price rising >5% above target — plus a hard 3h minimum
  between pings per item. The latch persists to SQLite, so restarts
  never replay pings. Items with no target ($0) never ping; they're
  list-only.
- **Amazon-seller gate** — only pings stock sold by Amazon itself
  (Amazon.ca / Amazon.com). Third-party sellers holding the buy box
  (usually at scalper prices) never ping; they count as "Amazon has
  no stock", so when Amazon retakes the buy box after 1h+ that fires
  a restock alert. Seller is read from the wishlist's Shipper/Seller
  line or the /dp/ buy-box merchant info, with a proxy-racing /dp/
  verification fetch as fallback. Undetermined seller = no ping
  (never guesses).
- **Discord embeds** with product thumbnail, color-coded by event
  type (green = in stock, blue = restock, amber = price drop), and a
  📓 Product page link button.
- **Proxy cooldown** — bad proxies (high block / error rate) cool
  down automatically; recovery is automatic.
- **Per-ASIN cursed detection** — high HTTP fail-rate ASINs get a
  parallel race across N proxies per scan.
- **PySide6 desktop GUI** for managing the product list and viewing
  live stats, plus a **headless runner** for VPS / Raspberry Pi
  24/7 deployment.

## Install

```bash
pip install -r requirements.txt
```

## Configure

1. **Discord webhook** — create one in your Discord server's channel
   settings → Integrations → Webhooks. Paste the URL into Settings
   tab in the GUI, or directly into `amazon_monitor_pro.db` via the
   GUI.
2. **Proxies (optional but strongly recommended)** — copy
   `proxies.txt.example` to `proxies.txt` and paste your proxies, one
   per line. Datacenter proxies from providers like
   [Webshare](https://www.webshare.io/) work well; residential is
   better but pricier.
3. **Wishlist (recommended for speed)** — create a public Amazon
   wishlist, add the products you want to monitor, copy the wishlist
   ID (the part after `/ls/` in the URL), paste into Settings tab.
4. **Products** — add via the GUI's Products tab, or bulk-manage by
   copying `watchlist.txt.example` to `watchlist.txt` and editing.
   One ASIN per line, optionally followed by a ping target (e.g.
   `B0G3CV6Z9D 65.00` = @everyone when Amazon price ≤ $65; ASIN
   alone = daily-digest only). The bot reads `watchlist.txt` on
   startup and makes the products table match (additions, removals,
   target changes).

## Run (desktop GUI)

```bash
py main.py
```

Add products in the Products tab → flip them ON → hit ▶ Start
Monitoring in the Dashboard.

## Run (headless / 24/7)

For VPS, Raspberry Pi, or any always-on Linux box:

```bash
py headless.py
```

Reads the same `amazon_monitor_pro.db` file the GUI uses. Configure
on your desktop with the GUI, copy the `.db` to your server, run
`headless.py` there. Stops cleanly on Ctrl+C / SIGTERM.

## Settings

| Setting             | Description                                          |
|---------------------|------------------------------------------------------|
| `discord_webhook`   | Primary stock-alert webhook (required)               |
| `check_interval`    | Per-product `/dp/` scan cadence in seconds           |
| `amazon_domain`     | e.g. `https://www.amazon.ca` or `.com`               |
| `wishlist_id`       | Public wishlist ID for HTTP fan-out (optional)       |

### Environment variables

| Variable                  | Description                                  |
|---------------------------|----------------------------------------------|
| `AMP_SECONDARY_WEBHOOK`   | Optional second Discord webhook (mirrors all pings) |

## Tuning

Edit the constants at the top of `monitor.py`:

- `HTTP_CONCURRENCY` — global request concurrency cap (default 80)
- `WISHLIST_HTTP_STAGGER_SECONDS` — fan-out interval per proxy
  (default 0.075 = ~13 probes/sec across pool). Raise to 0.15 if you
  see Blocked/Captcha rates climb. Drop to 0.05 for aggressive speed.
- `PROFILE_ROTATION_ENABLED` — adaptive TLS-profile rotation (off
  by default; turn on if Amazon WAF starts profiling your fingerprint)
- `IN_STOCK_CONFIRM_SCANS` / `OOS_CONFIRM_SCANS` — asymmetric
  hysteresis. The confirmed stock state only flips after this many
  consecutive consistent observations. In-stock confirms fast
  (default 2); out-of-stock needs many more (default 6) because a
  false OOS is the costly mistake — it's what would eventually fire a
  bogus "back in stock" ping. Raise `OOS_CONFIRM_SCANS` if you ever
  see a false restock.
- `MIN_RESTOCK_OOS_SECONDS` — how long an item must have no Amazon
  stock before a comeback re-arms its target alert. Default 3600 =
  1 hour. Brief OOS blips never re-arm.
- `TARGET_REARM_MARGIN` — price must rise above target × (1 + this)
  to re-arm a fired alert (default 0.05 = 5%). Stops boundary wobble
  from re-arming.
- `MIN_TARGET_REPING_SECONDS` — hard minimum between @everyone pings
  per item (default 10800 = 3h), even across re-arms.
- `DIGEST_INTERVAL_SECONDS` — daily digest cadence (default 86400).
- `SELLER_CLASS_TTL` — how long a resolved seller classification
  stays fresh before re-verifying via /dp/ (amazon 600s /
  third_party 120s / unknown 45s). Only matters when the wishlist
  HTML doesn't carry the Shipper/Seller line.
- `MIN_PRICE_DROP_PCT` — minimum % drop to fire a price-drop ping
  (default 1% = 0.01)

## Architecture

```
        ┌─────────────────────────────────────────────────┐
        │              MonitorWorker (asyncio)            │
        ├─────────────────────────────────────────────────┤
 GUI    │  scan_product_forever (per ASIN, /dp/ + AOD)    │
 ◄──── │  wishlist_http_scanner_forever (proxy fan-out)  │
        │  keepalive_loop · cookie_reset · stats_logger   │
        └─────────────────────────────────────────────────┘
                              │
                              ▼ HTTP via curl_cffi (rotating TLS)
                       ┌────────────────┐
                       │  Proxy Pool    │
                       └────────────────┘
                              │
                              ▼
                       ┌────────────────┐
                       │   Amazon CDN   │
                       └────────────────┘
                              │
              ┌───────────────┴────────────────┐
              ▼                                ▼
       state-change detector           ping_state machine
       (per-ASIN OOS → IS)             (event-based dedup)
              │                                │
              └──────────────┬─────────────────┘
                             ▼
                       Discord webhook (embed + button)
```

## License

MIT

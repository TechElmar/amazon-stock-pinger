import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlparse

import aiohttp
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from PySide6.QtCore import QThread, Signal

from notifier import DiscordNotifier

# lxml parses Amazon's ~0.5MB HTML 5-10x faster than the pure-Python
# html.parser. At wishlist fan-out rates (~13 parses/sec) that's the
# single biggest CPU saving in the app. Fall back gracefully if lxml
# isn't installed.
try:
    import lxml  # noqa: F401
    SOUP_PARSER = "lxml"
except ImportError:
    SOUP_PARSER = "html.parser"


PROXIES_FILE = Path("proxies.txt")

HTTP_TIMEOUT_SECONDS = 3
HTTP_CONCURRENCY = 80

# Optional secondary Discord webhook. Leave empty to disable. Posts
# the SAME stock-ping payload as the primary — useful for routing
# alerts to both a personal channel and a team channel.
#
# Do NOT commit a real webhook URL here. For deployments where you
# want a secondary channel, set the AMP_SECONDARY_WEBHOOK environment
# variable instead.
DISCORD_SECONDARY_WEBHOOK = os.environ.get("AMP_SECONDARY_WEBHOOK", "")

CHECK_INTERVAL_FALLBACK_SECONDS = 1.0

# Proxy cooldown / stats.
PROXY_MIN_CHECKS_BEFORE_COOLDOWN = 50
PROXY_BLOCK_RATE_COOLDOWN_THRESHOLD = 0.70
PROXY_ERROR_RATE_COOLDOWN_THRESHOLD = 0.50
PROXY_BLOCK_COOLDOWN_MINUTES = 15
PROXY_ERROR_COOLDOWN_MINUTES = 10

# Per-ASIN cursed detection — high HTTP fail-rate ASINs get a parallel
# race across multiple proxies on every scan to maximize the chance one
# slips past Amazon's WAF.
ASIN_FALLBACK_MIN_CHECKS = 30
ASIN_FALLBACK_FAIL_RATE = 0.55
ASIN_FALLBACK_RECOVER_RATE = 0.30
CURSED_PARALLEL_PROXIES = 3
CURSED_SCAN_INTERVAL = 0.5

# TLS impersonation profile.
# When PROFILE_ROTATION_ENABLED is False, use STATIC_IMPERSONATE_PROFILE
# for the whole session lifetime. When True, cycle through
# IMPERSONATE_PROFILES adaptively based on health.
PROFILE_ROTATION_ENABLED = False
STATIC_IMPERSONATE_PROFILE = "chrome146"

ROTATION_FAIL_THRESHOLD = 0.35
ROTATION_COOLDOWN_SECONDS = 60
ROTATION_MIN_SAMPLES = 30
ROTATION_MAX_PROFILE_SECONDS = 1800
ROTATION_WINDOW_SECONDS = 120
ROTATION_CHECK_INTERVAL = 15

# Wishlist HTTP fan-out. A single GET of /hz/wishlist/ls/{id} returns
# price/stock/title for every item in the list. Wishlist URLs have much
# lighter WAF protection than /dp/ because they're designed to be shared
# publicly. Fan out across the proxy pool with cache-busting query
# params so different CloudFront edges return fresh stock state.
WISHLIST_HTTP_STAGGER_SECONDS = 0.075

# How long a wishlist item's last-seen data stays valid in the merged
# view. Amazon renders wishlist items in chunks, so any single fetch
# may return only a SUBSET of the list. Rather than replacing the
# whole snapshot per fetch (which would momentarily drop the missing
# items and force them back onto the heavily-WAF'd /dp/ path), we MERGE
# each fetch into a rolling view and keep an item as long as it was
# seen within this window. A genuinely-removed wishlist item ages out
# after the TTL and then falls back to /dp/, which is correct.
WISHLIST_MERGE_TTL_SECONDS = 90

# Wishlist PAGINATION. Amazon server-renders only the first ~10 items of
# a wishlist; the rest load via a "show more" endpoint
# (/hz/wishlist/slv/items?...&paginationToken=<lek>). The scanner walks
# that chain each cycle so ALL items are covered, not just the first
# page. WISHLIST_MAX_PAGES caps the walk (the token wraps and re-serves
# once exhausted, so we also stop as soon as a page adds no new ASINs).
# Each page gets a few proxy retries before we give up on it for the
# cycle — the merge+TTL keeps its last-known data alive meanwhile.
WISHLIST_MAX_PAGES = 8
WISHLIST_PAGE_RETRIES = 4
# Target time for one full-list crawl cycle; real cycles run longer due
# to per-page fetch latency (~1-3s for a 24-item / 3-page list), which
# is plenty fast for restock detection and far lighter on the proxies
# than the old 13-fetch/sec single-page firehose.
WISHLIST_CYCLE_SECONDS = 0.5
WISHLIST_CYCLE_MIN_SLEEP = 0.2

# BURST-CONFIRM: the instant a targeted, armed item first reads in stock
# at/below its target, the crawler drops into a short high-rate window so
# the SECOND (confirming) read lands in a fraction of a second instead of
# waiting out drop-time block storms. The 2-read false-ping guard stays
# fully intact — this only makes confirm #2 fast, never skips it.
WISHLIST_BURST_SECONDS = 3.0    # how long one candidate keeps the crawl hot
WISHLIST_BURST_SLEEP = 0.1      # inter-cycle gap while bursting

# The pagination tokens are chained (page N's URL comes from page N-1's
# response), so the FIRST crawl must walk sequentially. But the page
# URLs stay valid as long as the list's contents/order don't change, so
# subsequent cycles fetch every known page CONCURRENTLY (one proxy each)
# — cutting a full sweep from ~3s to ~1s. We re-walk sequentially every
# WISHLIST_REDISCOVER_SECONDS to refresh the chain, and immediately if a
# parallel cycle's coverage collapses (a sign the tokens went stale).
WISHLIST_REDISCOVER_SECONDS = 60
# How often to emit the coverage log line (the crawl runs many cycles
# per second; we don't want a log line every cycle).
WISHLIST_CRAWL_LOG_SECONDS = 30

# Extracts the "show more" pagination URL from a wishlist page/fragment.
_SHOW_MORE_RE = re.compile(r'"showMoreUrl"\s*:\s*"([^"]+)"')


def extract_show_more_url(html: str) -> str:
    """Return the relative 'load more items' URL embedded in a wishlist
    page, or '' if this is the last page. Handles JSON-escaped and
    HTML-attribute-escaped forms."""
    m = _SHOW_MORE_RE.search(html)
    if not m:
        m = re.search(r'id="showMoreUrl"[^>]*value="([^"]+)"', html)
    if not m:
        return ""
    return (
        m.group(1)
        .replace("&amp;", "&")
        .replace("\\u0026", "&")
        .replace("\\/", "/")
    )

# ASYMMETRIC hysteresis. Flipping the confirmed stock state requires
# this many CONSECUTIVE consistent observations. The two directions
# use different thresholds ON PURPOSE:
#
#   IN_STOCK_CONFIRM_SCANS — small, so genuine restocks are caught
#     fast (an item with a price reads in-stock on nearly every probe).
#
#   OOS_CONFIRM_SCANS — large, because a FALSE out-of-stock is the
#     expensive mistake: it starts the restock clock and eventually
#     fires a bogus "back in stock" ping. Requiring many consecutive
#     OOS reads means a real OOS (every probe shows no price) confirms
#     quickly, while occasional parse noise (one bad probe among good
#     ones) never accumulates enough in a row to flip the state.
#
# An "unknown" observation neither advances nor resets the counters.
IN_STOCK_CONFIRM_SCANS = 2
OOS_CONFIRM_SCANS = 6

# ===================== NOTIFICATION MODEL =====================
# Two kinds of Discord messages:
#
#   1. DAILY DIGEST — one message every DIGEST_INTERVAL_SECONDS titled
#      "Amazon Stock Watchlist": every tracked item currently in stock
#      (sold by Amazon), with price/picture/link. NO @everyone.
#
#   2. TARGET ALERT — @everyone, fired ONLY when an item is confirmed
#      in stock, sold by Amazon, at/below its target_price. Governed
#      by a per-item armed/fired latch (see below) so items that just
#      SIT at target price (the greninja-box problem) ping exactly
#      once and then stay silent.
#
# THE LATCH. Each targeted item is "armed" or "fired":
#   - armed → ping when confirmed at/below target → becomes fired.
#   - fired → silent. Re-arms ONLY when the item durably leaves deal
#     territory:
#       * confirmed OOS for ≥ MIN_RESTOCK_OOS_SECONDS, then back, OR
#       * Amazon price rises above target * (1 + TARGET_REARM_MARGIN)
#         — the margin stops prices wobbling right at the target line
#         from re-arming and re-firing over and over.
#   - Additionally MIN_TARGET_REPING_SECONDS must have passed since
#     the item's last ping (hard per-item rate cap, belt & braces).
# The latch persists to the DB (products.alert_state) so restarts
# never replay pings for items already announced.
MIN_RESTOCK_OOS_SECONDS = 60 * 60          # OOS run needed to re-arm
TARGET_REARM_MARGIN = 0.05                 # price must exceed target by 5%
MIN_TARGET_REPING_SECONDS = 3 * 60 * 60    # ≥3h between pings per item

# SAME-PRICE SUPPRESSION — the anti-nag rule.
#
# An item that permanently sits at/below target (common: a $39.95 item
# with a $40 target) used to re-ping every few hours forever. Amazon's
# listing flickers out of stock for an hour or more — sometimes real,
# sometimes just Amazon showing a temporary "unavailable" — the restock
# rule re-arms it, and it re-fires at the exact same price. The channel
# reads it as spam, because nothing actually changed.
#
# The only genuinely new things worth an @role are: the price DROPPED
# below what we last announced, or the item was truly gone long enough
# that its return is real news. So a re-ping at the SAME (or higher)
# price than the last announced one needs a much longer quiet period;
# any real price drop bypasses it instantly.
SAME_PRICE_REPING_SECONDS = 24 * 60 * 60   # ≥24h between identical pings
PRICE_DROP_EPSILON = 0.01                  # cents of noise = not a drop

DIGEST_INTERVAL_SECONDS = 24 * 60 * 60     # one digest per day
# Don't send an overdue digest until the scanners have had time to
# establish real stock state after boot — otherwise the first digest
# after a restart would claim nothing is in stock.
DIGEST_STARTUP_WARMUP_SECONDS = 180

# Amazon Associates affiliate tag appended to every product link the
# bot posts to Discord (stock-list buttons + target-alert buttons).
# Set to "" to disable. Amazon's `tag=` param credits this associate
# for any resulting purchase. Not a secret — it appears in every public
# link — so it lives in code rather than an env var.
AFFILIATE_TAG = "shaunms-20"


def with_affiliate_tag(url: str) -> str:
    """Append the Associates tag to a product URL (no-op if the tag is
    empty, the URL is blank, or it already carries a tag)."""
    if not AFFILIATE_TAG or not url or "tag=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}tag={AFFILIATE_TAG}"


# ===================== AMAZON-SELLER GATE =====================
# Pings only fire for stock that is SOLD BY AMAZON (Amazon.ca /
# Amazon.com retail). Third-party sellers holding the buy box —
# usually at scalper prices — count as "Amazon has no stock" for
# ping purposes: no ping, and when Amazon later takes the buy box
# back after a 1h+ absence, that's a restock ping.
#
# Seller resolution TTLs (seconds) — how long a cached seller
# classification stays fresh before we re-verify with a /dp/ fetch.
# Only matters when the wishlist HTML doesn't carry seller info;
# when it does, the cache refreshes on every scan for free.
#   amazon:      long TTL, Amazon rarely loses the buy box silently
#   third_party: shorter, so we notice Amazon retaking the box fast
#   unknown:     short retry window for undetermined pages
SELLER_CLASS_TTL = {
    "amazon": 600,
    "third_party": 120,
    "unknown": 45,
}

# Amazon retail seller names. Matched case-insensitively after
# whitespace-normalization and trailing-dot strip. The regex covers
# amazon / amazon.ca / amazon.com / amazon.com.ca etc.; the set covers
# legal-entity spellings Amazon uses in merchant-info. Deliberately
# EXACT matching — a third-party store named "Amazonia Cards" must
# NOT classify as Amazon.
_AMAZON_SELLER_RE = re.compile(r"^amazon(\.[a-z]{2,3}){0,3}$")
_AMAZON_SELLER_KNOWN = {
    "amazon.com.ca ulc",
    "amazon.com services llc",
    "amazon.com services, inc",
    "amazon warehouse",
    "amazon resale",
}


def seller_class(seller: str) -> str:
    """Classify a seller string: "amazon" | "third_party" | "unknown".
    Empty/whitespace → unknown (we never guess)."""
    s = re.sub(r"\s+", " ", (seller or "")).strip().lower().rstrip(".")
    if not s:
        return "unknown"
    if _AMAZON_SELLER_RE.match(s) or s in _AMAZON_SELLER_KNOWN:
        return "amazon"
    return "third_party"

# Keepalive interval — how often the background pinger hits amazon's
# robots.txt to keep an HTTP/2 (or HTTP/3) connection pre-established
# on the scan_session. curl_cffi pools connections per-session; an
# idle pool eventually times out at the TCP/QUIC layer, forcing a
# fresh TLS handshake on the next request. That handshake costs
# ~50-150ms on the critical path. A 30s keepalive ping keeps the
# connection warm enough that real scans always reuse it.
KEEPALIVE_INTERVAL_SECONDS = 30

# Periodic cookie reset cadence. Clears the scanning session's
# accumulated cookies so Amazon's WAF doesn't fingerprint us as a
# long-running session bouncing between IPs.
COOKIE_RESET_SECONDS = 600

# Periodic stats log cadence.
STATS_LOG_SECONDS = 30

IMPERSONATE_PROFILES = [
    "safari260",
    "firefox147",
    "chrome145",
    "safari184",
    "chrome142",
    "firefox144",
    "chrome136",
    "safari180",
    "chrome133a",
    "safari170",
    "chrome131",
    "chrome124",
    "chrome120",
    "chrome146",
]

PRICE_SELECTOR_UNION = ", ".join([
    "#apex-pricetopay-accessibility-label",
    "#corePrice_feature_div .a-offscreen",
    "#corePriceDisplay_desktop_feature_div .a-offscreen",
    "#apex_desktop .a-offscreen",
    "#price_inside_buybox",
    ".reinventPricePriceToPayMargin .a-offscreen",
    "#tp_price_block_total_price_ww .a-offscreen",
    "#sns-base-price",
    "#newAccordionRow .a-offscreen",
    "#buyNewSection .a-offscreen",
    ".offer-price",
])


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


def extract_price_number(price_text):
    if not price_text:
        return None
    match = re.search(r"[\d,.]+", price_text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group())
    except ValueError:
        return None


def load_proxies() -> List[Optional[str]]:
    """Load proxies from PROXIES_FILE, handling multiple line formats:

    1. Already URL-formatted: `http://user:pass@host:port` → used as-is
    2. IPRoyal / provider format: `host:port:user:pass` → converted to URL
    3. Plain `host:port` (no auth) → prefixed with http://
    4. `DIRECT` → None (uses local IP, NOT recommended)
    """
    proxies: List[Optional[str]] = []

    if PROXIES_FILE.exists():
        for line in PROXIES_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.upper() == "DIRECT":
                proxies.append(None)
                continue
            if line.startswith("http://") or line.startswith("https://") or line.startswith("socks"):
                proxies.append(line)
                continue
            parts = line.split(":")
            if len(parts) == 4:
                host, port, user, password = parts
                proxies.append(f"http://{user}:{password}@{host}:{port}")
                continue
            if len(parts) == 2:
                proxies.append(f"http://{line}")
                continue
            proxies.append(line)

    if not proxies:
        proxies.append(None)

    return proxies


def proxy_label_from_url(proxy: Optional[str]) -> str:
    """Short display label for a proxy URL. For provider-style URLs
    where many proxies share the same host:port but differ by a
    session/residential ID embedded in the username (e.g. Webshare's
    "user-session-1234" or "userresidential-19912"), extract that
    trailing ID so each distinct proxy shows up separately in stats.

    Without this, a rotating-residential list where every entry shares
    one gateway host:port (but has thousands of unique IDs) would
    collapse into a SINGLE label — meaning a cooldown triggered by a
    handful of bad IPs would incorrectly blacklist the entire pool.
    The ID is matched positionally (last hyphen-segment of the
    username, right before the password) rather than requiring the
    literal word "session", so it works across naming conventions.
    """
    if not proxy:
        return "direct"
    label = proxy.split("@")[-1]
    m = re.search(r"-([A-Za-z0-9]+)(?=:[^@]*@)", proxy)
    if m:
        label = f"{label}/{m.group(1)}"
    return label


class HTTPAmazonChecker:
    def __init__(self, amazon_domain: str, semaphore: asyncio.Semaphore):
        self.amazon_domain = amazon_domain.rstrip("/")
        self.semaphore = semaphore

    def product_url(self, asin: str) -> str:
        return f"{self.amazon_domain}/dp/{asin}"

    def parse_html(self, asin: str, html: str, proxy_label: str) -> Dict[str, Any]:
        """Parse an Amazon /dp/{asin} page into a result dict.

        DETECTION PHILOSOPHY (after the false-OOS bug): we only ever
        declare "Out of stock" on DEFINITIVE proof. Anything we're not
        sure about returns "Unknown", which the hysteresis layer treats
        as a non-observation. A false OOS is the costly mistake — it
        starts the restock clock and eventually fires a bogus ping — so
        we refuse to infer OOS from weak/ambiguous signals.

        IN STOCK (both required):
          - a REAL Add-to-Cart / Buy-Now / Pre-order button element
            (not a JS string — an actual form input/button), AND
          - a parseable price.

        OUT OF STOCK (definitive proof only — any one):
          - #outOfStock element present, OR
          - the #availability text explicitly says "currently
            unavailable" / "temporarily out of stock" / "out of stock",
            OR
          - the page says "we don't know when or if this item will be
            back in stock".
        NOTE: "See All Buying Options" links and "Consider these
        alternative items" are NOT treated as OOS — Amazon shows both
        on plenty of in-stock multi-seller pages. Treating them as OOS
        was the bug that made in-stock items flap.

        Everything else → "Unknown".

        Also extracts the buy-box SELLER (result["seller"]) from the
        structured merchant locations only — #merchant-info, the
        tabular buy box, the seller-profile link, and the new
        offer-display feature spans. Deliberately NO page-wide "sold
        by" regex: the "Other Sellers on Amazon" section would poison
        it with third-party names even when Amazon holds the buy box.
        """
        url = self.product_url(asin)
        soup = BeautifulSoup(html, SOUP_PARSER)

        title = ""
        title_el = soup.select_one("#productTitle")
        if title_el:
            title = clean(title_el.get_text(" "))

        # Product thumbnail for Discord embed.
        image_url = ""
        img_el = soup.select_one("#landingImage")
        if img_el:
            image_url = (
                img_el.get("data-old-hires")
                or img_el.get("src")
                or ""
            )
        if not image_url:
            m = re.search(
                r'"hiRes"\s*:\s*"(https://m\.media-amazon\.com/images/I/[^"]+)"',
                html,
            )
            if m:
                image_url = m.group(1)
        if not image_url:
            m = re.search(
                r'"large"\s*:\s*"(https://m\.media-amazon\.com/images/I/[^"]+)"',
                html,
            )
            if m:
                image_url = m.group(1)

        availability_text = ""
        avail_el = soup.select_one("#availability")
        if avail_el:
            availability_text = clean(avail_el.get_text(" ")).lower()

        price = ""
        for el in soup.select(PRICE_SELECTOR_UNION):
            candidate = clean(el.get_text(" "))
            if "$" in candidate:
                price = candidate
                break

        # ============ SELLER (buy-box merchant) ============
        # Structured locations only, in reliability order.
        seller = ""

        # 1. Tabular buy box — "Sold by" row (modern desktop layout).
        row = soup.select_one('[tabular-attribute-name="Sold by"]')
        if row:
            t = clean(row.get_text(" "))
            t = re.sub(r"^sold by:?\s*", "", t, flags=re.I)
            if t:
                seller = t

        # 2. New offer-display feature span.
        if not seller:
            el = soup.select_one(
                '[offer-display-feature-name="desktop-merchant-info"] '
                '.offer-display-feature-text-message'
            )
            if el:
                seller = clean(el.get_text(" "))

        # 3. Third-party seller profile link (only present for 3P).
        if not seller:
            el = soup.select_one("#sellerProfileTriggerId")
            if el:
                seller = clean(el.get_text(" "))

        # 4. Classic #merchant-info sentence. Handles:
        #    "Ships from and sold by Amazon.ca."        → Amazon.ca
        #    "Sold by Eternal Emporium and Fulfilled by Amazon." → 3P!
        if not seller:
            el = soup.select_one("#merchant-info")
            if el:
                t = clean(el.get_text(" "))
                # Boundary: " and fulfilled/ships...", or a sentence-
                # ending dot (dot followed by space or end). A bare
                # `\.` boundary would truncate "Amazon.ca" to "Amazon"
                # because the lazy capture stops at the internal dot.
                m = re.search(
                    r"sold by\s+(.+?)(?:\s+and\s+fulfilled|\s+and\s+ships|\.\s|\.$|$)",
                    t,
                    flags=re.I,
                )
                if m:
                    seller = m.group(1).strip()

        html_lower = html.lower()

        # ============ DEFINITIVE OOS PROOF ============
        definitive_oos = bool(
            soup.select_one("#outOfStock")
            or "currently unavailable" in availability_text
            or "temporarily out of stock" in availability_text
            or "out of stock" in availability_text
            or (
                "we don't know when or if this item will be back"
                in html_lower
            )
        )

        # ============ IN-STOCK BUTTON (real element, not JS) ============
        has_add_to_cart = bool(soup.select_one(
            '#add-to-cart-button, '
            'input[name="submit.add-to-cart"], '
            'input[name="submit.addToCart"]'
        ))
        has_buy_now = bool(soup.select_one(
            '#buy-now-button, '
            'input[name="submit.buy-now"], '
            'input[name="submit.buyNow"]'
        ))
        has_preorder = bool(
            soup.select_one(
                '#preorder-button, '
                '#placePreOrderButton, '
                'input[name="submit.preorder"], '
                'input[name="submit.preorder-update"], '
                'input[name="submit.pre-order"]'
            )
            or re.search(r"pre-?order now", html_lower)
        )
        has_buyable_button = has_add_to_cart or has_buy_now or has_preorder

        # ============ DECISION ============
        # Definitive OOS wins outright. Otherwise we need a real buy
        # button AND a price to call it in stock. Everything else is
        # Unknown — we do NOT guess OOS from the absence of a button.
        if definitive_oos:
            stock = "Out of stock"
            price = ""
        elif has_buyable_button and price:
            if has_preorder and not (has_add_to_cart or has_buy_now):
                stock = "Pre-order"
            else:
                stock = "In stock"
        else:
            stock = "Unknown"

        return {
            "asin": asin,
            "title": title,
            "price": price,
            "price_number": extract_price_number(price),
            "stock": stock,
            "url": url,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": proxy_label,
            "image_url": image_url,
            "seller": seller,
        }

    async def fetch_product(
        self,
        session,
        asin: str,
        proxy: Optional[str],
    ) -> Dict[str, Any]:
        url = self.product_url(asin)
        proxy_label = proxy_label_from_url(proxy)

        # Range header — we only need the first ~150KB of HTML: the
        # buy box (price, buttons, AND the Sold-by merchant info) all
        # live in that window. Still cuts bandwidth ~5x vs full page.
        # Referer pretends user clicked through from search results,
        # which Amazon's WAF scrutinizes less than direct URL hits.
        headers = {
            "Accept-Language": "en-CA,en;q=0.9,en-US;q=0.8",
            "Cache-Control": "no-cache",
            "Referer": f"{self.amazon_domain}/s?k={asin}",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Upgrade-Insecure-Requests": "1",
            "Range": "bytes=0-150000",
        }

        async with self.semaphore:
            try:
                r = await session.get(
                    url,
                    headers=headers,
                    proxy=proxy,
                    timeout=HTTP_TIMEOUT_SECONDS,
                    allow_redirects=True,
                )

                if r.status_code >= 400:
                    return self.empty_result(asin, f"HTTP {r.status_code}", proxy_label)

                html = r.text
                html_lower = html.lower()

                if "captcha" in html_lower or "robot check" in html_lower:
                    return self.empty_result(asin, "Blocked/Captcha", proxy_label)

                return self.parse_html(asin, html, proxy_label)

            except Exception as e:
                err = str(e).lower()
                if "timed out" in err or "timeout" in err:
                    tag = "Timeout"
                elif (
                    "could not resolve" in err
                    or "couldn't connect" in err
                    or "connection" in err
                ):
                    tag = "Connection error"
                else:
                    tag = "Network error"
                return self.empty_result(asin, tag, proxy_label)

    def empty_result(self, asin: str, stock: str, proxy_label: str) -> Dict[str, Any]:
        return {
            "asin": asin,
            "title": "",
            "price": "",
            "price_number": None,
            "stock": stock,
            "url": self.product_url(asin),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": proxy_label,
            "image_url": "",
            "seller": "",
        }

    async def check_product_single(
        self,
        session,
        asin: str,
        proxies: List[Optional[str]],
        is_proxy_available,
        proxy_index: int,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], int]:
        """Pick ONE live proxy via round-robin and fetch. On HTTP error
        or captcha, retry ONCE with the next live proxy. Returns the
        result, the raw results (for stats), and the new index to persist."""

        live = [
            (i, p) for i, p in enumerate(proxies)
            if is_proxy_available(proxy_label_from_url(p))
        ]
        if not live:
            return self.empty_result(asin, "All sources cooling down", "none"), [], proxy_index

        start = proxy_index % len(live)
        order = live[start:] + live[:start]

        raw: List[Dict[str, Any]] = []
        for orig_idx, proxy in order[:2]:
            r = await self.fetch_product(session, asin, proxy)
            raw.append(r)
            stock = (r.get("stock") or "").lower()
            bad = (
                "blocked" in stock
                or "captcha" in stock
                or "http 5" in stock
                or "http 4" in stock
                or "error" in stock
                or "timeout" in stock
            )
            if not bad:
                return r, raw, (orig_idx + 1) % len(proxies)

        return raw[-1], raw, (order[1][0] + 1) % len(proxies) if len(order) > 1 else proxy_index

    async def check_product_parallel(
        self,
        session,
        asin: str,
        proxies: List[Optional[str]],
        is_proxy_available,
        proxy_index: int,
        n_parallel: int = 3,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], int]:
        """For cursed ASINs: race N proxies in parallel. First useful
        response wins, the rest get cancelled."""

        live = [
            (i, p) for i, p in enumerate(proxies)
            if is_proxy_available(proxy_label_from_url(p))
        ]
        if not live:
            return self.empty_result(asin, "All sources cooling down", "none"), [], proxy_index

        start = proxy_index % len(live)
        rotated = live[start:] + live[:start]
        selected = rotated[: max(1, n_parallel)]

        raw: List[Dict[str, Any]] = []
        tasks = [
            asyncio.create_task(self.fetch_product(session, asin, proxy))
            for _, proxy in selected
        ]

        winner = None
        pending = set(tasks)
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    try:
                        r = task.result()
                    except Exception:
                        continue
                    raw.append(r)
                    stock = (r.get("stock") or "").lower()
                    bad = (
                        "blocked" in stock
                        or "captcha" in stock
                        or "http 5" in stock
                        or "http 4" in stock
                        or "error" in stock
                        or "timeout" in stock
                    )
                    if not bad and winner is None:
                        winner = r
                        for t in pending:
                            t.cancel()
                        pending = set()
                        break

            next_idx = (selected[-1][0] + 1) % len(proxies) if proxies else 0
            return winner or (raw[-1] if raw else self.empty_result(asin, "All failed", "none")), raw, next_idx
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()


class WishlistScanner:
    """Scrapes a public Amazon wishlist page to extract stock/price/title
    for every item in one HTTP request.

    Why this works where /dp/ and /gp/aod/ don't: public wishlist URLs
    are designed to be shared with non-customers, so Amazon's WAF is
    far more lenient on them. They serve the same product data for the
    items they contain, but without the per-ASIN content protection
    that blocks direct /dp/ scrapes.
    """

    def __init__(self, amazon_domain: str, wishlist_id: str):
        self.amazon_domain = amazon_domain.rstrip("/")
        self.wishlist_id = (wishlist_id or "").strip()

    def wishlist_url(self) -> str:
        return f"{self.amazon_domain}/hz/wishlist/ls/{self.wishlist_id}"

    def parse_wishlist(self, html: str) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        if not html or len(html) < 1000:
            return results

        soup = BeautifulSoup(html, SOUP_PARSER)

        item_elements = (
            soup.select('li[data-itemtype="wl-list-item"]')
            or soup.select(".g-item-sortable")
            or soup.select("li[data-asin]")
            or soup.select(".a-list-item[data-itemId]")
        )

        if not item_elements:
            item_elements = soup.find_all(
                lambda t: (
                    t.name in ("li", "div")
                    and (t.get("data-asin") or t.get("data-itemId"))
                )
            )

        for el in item_elements:
            asin = (
                el.get("data-asin")
                or el.get("data-product-asin")
                or ""
            )
            if not asin:
                link = el.select_one('a[href*="/dp/"]')
                if link:
                    href = link.get("href", "")
                    m = re.search(r"/dp/([A-Z0-9]{10})", href)
                    if m:
                        asin = m.group(1)
            if not asin or len(asin) != 10:
                continue

            # Product thumbnail — wishlist HTML embeds a small image
            # per item. The CDN URL works directly in Discord embeds.
            image_url = ""
            img_el = el.select_one('img[src*="media-amazon.com/images/I/"]')
            if img_el:
                image_url = img_el.get("src", "") or img_el.get("data-src", "")

            title = ""
            for sel in (
                'a[id*="itemName"]',
                'h2 a',
                'a[href*="/dp/"]',
                ".a-link-normal[title]",
            ):
                title_el = el.select_one(sel)
                if title_el:
                    title = clean(title_el.get_text(" ")) or title_el.get("title", "")
                    if title:
                        break

            price = ""
            for sel in (
                ".a-price .a-offscreen",
                "span[id*='itemPrice']",
                ".a-color-price",
                ".a-price-whole",
            ):
                price_el = el.select_one(sel)
                if price_el:
                    candidate = clean(price_el.get_text(" "))
                    if "$" in candidate:
                        price = candidate
                        break

            # DETECTION (definitive-proof-only, mirrors parse_html):
            #
            # On a public wishlist, a PRICE is the single most reliable,
            # most consistently-parseable signal that an item is
            # purchasable. If a price is present, the item is in stock
            # (or pre-order). We do NOT downgrade a priced item to OOS
            # just because a "See All Buying Options" link is also
            # present — Amazon shows that link on in-stock multi-seller
            # items all the time, and treating it as OOS was exactly
            # the bug that made in-stock items flap in/out.
            #
            # OUT OF STOCK requires DEFINITIVE proof: explicit
            # unavailable text, OR a complete absence of price combined
            # with a see-all-buying-options link (the buy box is gone).
            el_raw = str(el)
            el_html_lower = el_raw.lower()

            has_preorder = bool(
                el.select_one('input[name="submit.preorder"]')
                or el.select_one('input[name="submit.preorder-update"]')
                or el.select_one('input[name="submit.pre-order"]')
                or el.select_one('#preorder-button')
                or re.search(r"pre-?order now", el_html_lower)
                or 'data-action="preorder' in el_html_lower
            )
            has_see_all = bool(
                el.select_one(f'a[href*="/gp/offer-listing/{asin}"]')
                or el.select_one('a[title="See All Buying Options"]')
            )
            explicit_unavailable = (
                "currently unavailable" in el_html_lower
                or "temporarily out of stock" in el_html_lower
                or "out of stock" in el_html_lower
                or "no longer available" in el_html_lower
            )

            # SELLER — wishlist items carry a "Shipper / Seller" line
            # (e.g. "Shipper / Seller  Amazon.ca" for first-party vs
            # "Shipper / Seller  Eternal Emporium" for scalpers). The
            # regex is scoped to THIS item's HTML only, skips any tags
            # between the label and the value, and captures the first
            # text run. Falls back to a "Sold by X" pattern. Empty if
            # neither matches — the seller gate treats that as unknown
            # and verifies via a /dp/ fetch instead of guessing.
            seller = ""
            m = re.search(
                r"shipper\s*/?\s*seller\s*:?\s*(?:<[^>]+>\s*)*([^<]{1,80})",
                el_raw,
                flags=re.I,
            )
            if m:
                seller = clean(m.group(1))
            if not seller:
                m = re.search(
                    r"sold by\s*:?\s*(?:<[^>]+>\s*)*([^<]{1,80})",
                    el_raw,
                    flags=re.I,
                )
                if m:
                    seller = clean(m.group(1))

            if explicit_unavailable or (not price and has_see_all):
                # Definitive OOS: stated unavailable, or no price at all
                # and the buy box has been replaced by a see-all link.
                stock = "Out of stock"
                price = ""
            elif price:
                # Price present → purchasable → in stock (pre-order if a
                # pre-order affordance is present).
                stock = "Pre-order" if has_preorder else "In stock"
            else:
                # No price and no definitive OOS marker → can't tell.
                # Hysteresis treats Unknown as a non-observation.
                stock = "Unknown"

            results[asin] = {
                "asin": asin,
                "title": title,
                "price": price,
                "price_number": extract_price_number(price),
                "stock": stock,
                "url": f"{self.amazon_domain}/dp/{asin}",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "wishlist",
                "image_url": image_url,
                "seller": seller,
            }

        return results


class MonitorWorker(QThread):
    log = Signal(str)
    product_updated = Signal(dict)
    status = Signal(str)
    # Fires ONCE per @everyone target-price ping (deduped via the
    # armed/fired latch in _maybe_announce). The dashboard counter
    # listens to THIS instead of counting from the per-scan
    # product_updated stream, which would count every flicker as a hit.
    hit_counted = Signal(str, float)

    def __init__(self, db):
        super().__init__()
        self.db = db
        self.running = False

        self.proxy_stats: Dict[str, Dict[str, int]] = {}
        self.proxy_cooldowns: Dict[str, datetime] = {}

        self.asin_stats: Dict[str, Dict[str, int]] = {}
        # ASINs flagged as cursed (high HTTP fail rate). Get the
        # parallel-race scan path instead of single-proxy.
        self.cursed_asins: set = set()

        # Per-ASIN dedup of unchanged DB writes / UI emits.
        self.last_results: Dict[str, Tuple[str, str, str]] = {}

        # Fire-and-forget background tasks — kept strong-referenced so
        # asyncio doesn't GC them mid-flight.
        self.background_tasks: set = set()

        # Round-robin proxy index per ASIN.
        self.proxy_indexes: Dict[str, int] = {}

        # Live curl_cffi scanning session, rotated by the profile
        # rotation loop. Scan tasks read this attribute on every
        # iteration so they auto pick up the new session after rotation.
        self.scan_session = None
        self._profile_index = 0

        # Wishlist scanner state.
        self.wishlist_scanner: Optional["WishlistScanner"] = None
        self.wishlist_results: Dict[str, Dict[str, Any]] = {}
        self.previous_wishlist_results: Dict[str, Dict[str, Any]] = {}
        # Rolling merged wishlist view: asin -> (result, monotonic_ts).
        # Each partial fetch updates the entries it saw; wishlist_results
        # is rebuilt from everything seen within WISHLIST_MERGE_TTL_SECONDS
        # so a chunked/partial fetch can't drop items back to /dp/.
        self._wishlist_seen: Dict[str, Tuple[Dict[str, Any], float]] = {}
        # Diagnostics for the paginated crawler (last completed cycle).
        self._last_crawl_pages: int = 0
        self._last_crawl_items: int = 0
        # Cached page-URL chain for parallel fetching, discovered by the
        # periodic sequential walk. Empty forces a fresh discovery.
        self._wl_page_urls: List[str] = []
        # Burst-confirm deadline (monotonic). While now < this, the
        # wishlist crawler runs at WISHLIST_BURST_SLEEP cadence to race
        # the confirming read of a targeted item that just went in stock.
        self._wishlist_burst_until: float = 0.0

        # Shared refs set in run_async.
        # Optional Discord bot. It is an ADDITIONAL delivery target that
        # posts only to AMP_ALERT_CHANNEL_ID; the webhooks below keep
        # firing to their own destinations untouched either way. Stays
        # None without AMP_BOT_TOKEN.
        self.bot = None
        self.notifier: Optional["DiscordNotifier"] = None
        self.secondary_notifier: Optional["DiscordNotifier"] = (
            DiscordNotifier(DISCORD_SECONDARY_WEBHOOK)
            if DISCORD_SECONDARY_WEBHOOK
            else None
        )
        self.discord_session = None
        self.checker: Optional["HTTPAmazonChecker"] = None

        # Target-alert latch per ASIN: "armed" | "fired". Missing key
        # = armed (new items ping on their first confirmed target hit).
        # Persisted to products.alert_state — survives restarts, which
        # is what stops always-at-target items from re-pinging on boot.
        self.alert_state: Dict[str, str] = {}
        # Epoch of the last @everyone target ping per ASIN. Enforces
        # MIN_TARGET_REPING_SECONDS even across re-arms. Rehydrated
        # from products.last_pinged_at at startup.
        self.last_target_ping_at: Dict[str, float] = {}
        # Price we last ANNOUNCED per ASIN (ping or stock-list latch).
        # Drives SAME_PRICE_REPING_SECONDS: re-announcing the same price
        # is nagging, a lower price is real news. Rehydrated from
        # products.last_pinged_price at startup.
        self.last_pinged_price: Dict[str, float] = {}
        # ASINs whose target hit is being held by the re-ping cooldown
        # (used to log the suppression exactly once, not per scan).
        self._cooldown_logged: set = set()
        # ASINs whose repeat ping is held by same-price suppression
        # (logged once per hold, not per scan).
        self._same_price_logged: set = set()

        # STARTUP SWEEP GUARD. False until the boot stock-list message
        # has been sent. While False, NO target ping can fire — the
        # first sweep only records what's in stock. Items on that boot
        # list that sit at/below target get latched "fired" when the
        # list is sent, so they can't ping afterwards either. Only
        # genuinely NEW events after the sweep can ping.
        self._startup_sweep_done: bool = False
        # ASINs whose target hit was held during the sweep (log once).
        self._sweep_held_logged: set = set()

        # Latest seller-confirmed in-stock scan result per ASIN. The
        # stock-list builder reads this so it always has a live price/
        # title/image even for /dp/-scanned items whose DB row hasn't
        # been written yet.
        self.latest_result: Dict[str, dict] = {}

        # Hysteresis state machine. Each ASIN has:
        #   consec_in_stock[asin]  — consecutive "in_stock" observations
        #   consec_oos[asin]       — consecutive "oos" observations
        #   effective_state[asin]  — confirmed state: "in_stock" | "oos"
        #                            (None until enough scans agree)
        # An "Unknown" observation (parse failure, ambiguous page)
        # leaves counters unchanged — it's a non-event. In-stock flips
        # after IN_STOCK_CONFIRM_SCANS, OOS after OOS_CONFIRM_SCANS.
        self.consec_in_stock: Dict[str, int] = {}
        self.consec_oos: Dict[str, int] = {}
        self.effective_state: Dict[str, Optional[str]] = {}

        # Per-ASIN epoch timestamp marking when the current confirmed-OOS
        # run began. Set when effective_state flips to "oos", cleared
        # when it flips back to "in_stock". The restock rule compares
        # (now - oos_since) against MIN_RESTOCK_OOS_SECONDS. Persisted
        # to products.oos_since so the 1-hour clock keeps ticking across
        # bot restarts.
        self.oos_since: Dict[str, float] = {}

        # Seller resolution cache: asin -> {"seller", "class", "ts"}.
        # class is "amazon" | "third_party" | "unknown". Refreshed for
        # free whenever a scan result carries seller info; otherwise a
        # throttled /dp/ verification fetch fills it in. In-memory only
        # — rebuilds within seconds of startup.
        self.seller_cache: Dict[str, Dict[str, Any]] = {}
        # ASINs with an in-flight seller verification fetch (dedup).
        self._verifying: set = set()
        # Proxy list snapshot for verification fetches (set in run_async).
        self.scan_proxies: List[Optional[str]] = []

        # Perf caches — avoid hammering SQLite from the hot paths.
        # (The wishlist processor used to call db.get_products() ~13x/sec
        # and every product loop hit db.get_setting twice per second.)
        self._enabled_products_cache: Dict[str, dict] = {}
        self._enabled_products_ts: float = 0.0
        self._interval_cache: Optional[float] = None
        self._interval_cache_ts: float = 0.0
        # Per-ASIN scan-log throttle: (last_logged_key, last_logged_at).
        self._scan_log_state: Dict[str, Tuple[tuple, float]] = {}

        # Rolling per-scan health events for adaptive profile rotation.
        self.health_events: list = []

    def stop(self):
        self.running = False

    def run(self):
        asyncio.run(self.run_async())

    # ------------------------------------------------------------------
    # Proxy / ASIN health
    # ------------------------------------------------------------------

    def update_proxy_stats(self, proxy_label: str, stock: str):
        if not proxy_label:
            proxy_label = "direct"

        if proxy_label not in self.proxy_stats:
            self.proxy_stats[proxy_label] = {
                "hits": 0, "blocks": 0, "errors": 0, "total": 0,
            }

        self.proxy_stats[proxy_label]["total"] += 1
        stock_lower = (stock or "").lower()

        if "blocked" in stock_lower or "captcha" in stock_lower:
            self.proxy_stats[proxy_label]["blocks"] += 1
        elif "http" in stock_lower or "error" in stock_lower:
            self.proxy_stats[proxy_label]["errors"] += 1
        else:
            self.proxy_stats[proxy_label]["hits"] += 1

    def _record_health(self, success: bool) -> None:
        now = time.monotonic()
        self.health_events.append((now, success))
        cutoff = now - ROTATION_WINDOW_SECONDS
        while self.health_events and self.health_events[0][0] < cutoff:
            self.health_events.pop(0)

    def _health_stats(self) -> Tuple[int, float]:
        if not self.health_events:
            return 0, 0.0
        total = len(self.health_events)
        fails = sum(1 for _, s in self.health_events if not s)
        return total, fails / total

    def _is_healthy_response(self, result: Dict[str, Any]) -> bool:
        if not (result.get("title") or result.get("price")):
            return False
        stock = (result.get("stock") or "").lower()
        bad_markers = (
            "timeout", "blocked", "captcha", "network error",
            "connection error",
        )
        if any(m in stock for m in bad_markers):
            return False
        if stock.startswith("http"):
            return False
        return True

    def is_proxy_available(self, proxy_label: str) -> bool:
        cooldown_until = self.proxy_cooldowns.get(proxy_label)
        if not cooldown_until:
            return True
        if cooldown_until <= datetime.now():
            self.proxy_cooldowns.pop(proxy_label, None)
            self.log.emit(f"{proxy_label} cooldown finished.")
            return True
        return False

    def evaluate_proxy_cooldown(self, proxy_label: str):
        if not proxy_label:
            proxy_label = "direct"
        if proxy_label == "direct":
            return

        stats = self.proxy_stats.get(proxy_label)
        if not stats:
            return

        total = stats.get("total", 0)
        blocks = stats.get("blocks", 0)
        errors = stats.get("errors", 0)
        hits = stats.get("hits", 0)

        if total < PROXY_MIN_CHECKS_BEFORE_COOLDOWN:
            return

        block_rate = blocks / total if total else 0
        error_rate = errors / total if total else 0
        now = datetime.now()

        if block_rate >= PROXY_BLOCK_RATE_COOLDOWN_THRESHOLD:
            self.proxy_cooldowns[proxy_label] = (
                now + timedelta(minutes=PROXY_BLOCK_COOLDOWN_MINUTES)
            )
            self.log.emit(
                f"{proxy_label} cooled down for {PROXY_BLOCK_COOLDOWN_MINUTES} minutes "
                f"(block rate {round(block_rate * 100, 1)}%, sample {total})."
            )
            self.proxy_stats[proxy_label] = {
                "hits": 0, "blocks": 0, "errors": 0, "total": 0,
            }
            return

        if error_rate >= 0.75 and hits < errors:
            self.proxy_cooldowns[proxy_label] = (
                now + timedelta(minutes=PROXY_ERROR_COOLDOWN_MINUTES)
            )
            self.log.emit(
                f"{proxy_label} cooled down for {PROXY_ERROR_COOLDOWN_MINUTES} minutes "
                f"(error rate {round(error_rate * 100, 1)}%, sample {total})."
            )
            self.proxy_stats[proxy_label] = {
                "hits": 0, "blocks": 0, "errors": 0, "total": 0,
            }

    def log_proxy_stats(self):
        if not self.proxy_stats:
            return
        now = datetime.now()
        self.log.emit("===== PROXY STATS =====")
        for proxy, stats in sorted(self.proxy_stats.items()):
            total = stats["total"]
            block_rate = round((stats["blocks"] / total) * 100, 1) if total else 0
            error_rate = round((stats["errors"] / total) * 100, 1) if total else 0
            cooldown_text = ""
            cooldown_until = self.proxy_cooldowns.get(proxy)
            if cooldown_until and cooldown_until > now:
                remaining = int((cooldown_until - now).total_seconds())
                cooldown_text = f" | COOLDOWN: {remaining}s left"
            self.log.emit(
                f"{proxy} | Hits: {stats['hits']} | Blocks: {stats['blocks']} | "
                f"Errors: {stats['errors']} | Block Rate: {block_rate}% | "
                f"Error Rate: {error_rate}%{cooldown_text}"
            )

    def update_asin_stats(self, asin: str, raw_results: List[Dict[str, Any]]):
        if asin not in self.asin_stats:
            self.asin_stats[asin] = {"total": 0, "errors": 0, "good": 0}
        for r in raw_results:
            stock = r.get("stock", "").lower()
            self.asin_stats[asin]["total"] += 1
            if "http 5" in stock:
                self.asin_stats[asin]["errors"] += 1
            elif r.get("title") or r.get("price"):
                self.asin_stats[asin]["good"] += 1

    def evaluate_asin_cursed(self, asin: str):
        """Flag ASINs with high HTTP fail rate as cursed → parallel scan path."""
        stats = self.asin_stats.get(asin)
        if not stats:
            return
        total = stats["total"]
        good = stats["good"]
        if total < ASIN_FALLBACK_MIN_CHECKS:
            return
        fail_rate = 1 - (good / total) if total else 0
        if fail_rate >= ASIN_FALLBACK_FAIL_RATE and asin not in self.cursed_asins:
            self.cursed_asins.add(asin)
            self.log.emit(
                f"ASIN {asin} flagged as CURSED "
                f"(fail rate {round(fail_rate * 100, 1)}%, {total} checks) "
                f"→ parallel x{CURSED_PARALLEL_PROXIES} per scan."
            )
        elif fail_rate < ASIN_FALLBACK_RECOVER_RATE and asin in self.cursed_asins:
            self.cursed_asins.discard(asin)
            self.log.emit(
                f"ASIN {asin} recovered "
                f"(fail rate improved to {round(fail_rate * 100, 1)}%)."
            )

    def log_asin_stats(self):
        if not self.asin_stats:
            return
        self.log.emit("===== ASIN STATS =====")
        for asin, stats in sorted(self.asin_stats.items()):
            total = stats["total"]
            good = stats["good"]
            errors = stats["errors"]
            error_rate = round((errors / total) * 100, 1) if total else 0
            fail_rate = round((1 - good / total) * 100, 1) if total else 0
            flag = f" [CURSED — parallel x{CURSED_PARALLEL_PROXIES}]" if asin in self.cursed_asins else ""
            self.log.emit(
                f"{asin} | Total: {total} | Good: {good} | Errors: {errors} | "
                f"Error Rate: {error_rate}% | Fail Rate: {fail_rate}%{flag}"
            )

    # ------------------------------------------------------------------
    # Settings / pings
    # ------------------------------------------------------------------

    def _read_interval(self) -> float:
        """Check interval from settings, cached for 10s — this is
        called by every product loop on every iteration, and hitting
        SQLite dozens of times per second for a value that changes
        maybe once a session is wasted I/O."""
        now = time.monotonic()
        if (
            self._interval_cache is not None
            and now - self._interval_cache_ts < 10
        ):
            return self._interval_cache
        try:
            raw = self.db.get_setting("check_interval") or str(CHECK_INTERVAL_FALLBACK_SECONDS)
            val = max(0.1, float(raw))
        except (ValueError, TypeError):
            val = CHECK_INTERVAL_FALLBACK_SECONDS
        self._interval_cache = val
        self._interval_cache_ts = now
        return val

    def _enabled_products(self) -> Dict[str, dict]:
        """Enabled-products snapshot keyed by ASIN, cached for 5s.
        The wishlist processor consults this on every probe (~13x/sec);
        a fresh SQLite query each time was the app's biggest hidden
        I/O cost. UI toggles take effect within 5s — fine."""
        now = time.monotonic()
        if now - self._enabled_products_ts > 5:
            self._enabled_products_cache = {
                p["asin"]: p
                for p in self.db.get_products()
                if int(p.get("enabled", 1)) == 1
            }
            self._enabled_products_ts = now
        return self._enabled_products_cache

    def _spawn_send(self, coro_factory, label: str) -> None:
        """Schedule a Discord send as a background task. coro_factory
        is a zero-arg callable returning the awaitable (so we can build
        one per webhook). Strong-ref the task in self.background_tasks
        so asyncio cannot GC it mid-flight."""

        async def _send():
            try:
                ok, msg = await coro_factory()
                self.log.emit(msg)
            except Exception as e:
                self.log.emit(f"Discord {label} error: {e}")

        task = asyncio.create_task(_send())
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    def _fire_target_pings(
        self,
        *,
        product: dict,
        result: dict,
        reason: str,
    ) -> None:
        """Send the @everyone target alert to primary + optional
        secondary webhooks. The target price itself is never included
        in the message."""
        if self.discord_session is None:
            return
        asin = product["asin"]
        # Seller for the embed — from this scan if it carried one,
        # else the cache (pings only fire seller-confirmed, so one of
        # these is always the Amazon entity name).
        seller = clean(result.get("seller") or "") or (
            (self.seller_cache.get(asin) or {}).get("seller", "")
        )
        kwargs = {
            "title": result.get("title", ""),
            "asin": asin,
            "price": result.get("price", ""),
            "reason": reason,
            "url": with_affiliate_tag(result.get("url", "")),
            "image_url": result.get("image_url", ""),
            "seller": seller,
        }
        for n in (self.notifier, self.secondary_notifier, self.bot):
            if n is None:
                continue
            self._spawn_send(
                lambda n=n: n.send_target_alert(self.discord_session, **kwargs),
                "target alert",
            )

    def _set_alert_state(self, asin: str, state: str) -> None:
        """Flip the armed/fired latch and persist it. Logs transitions."""
        prev = self.alert_state.get(asin, "armed")
        if prev == state:
            return
        self.alert_state[asin] = state
        try:
            self.db.save_alert_state(asin, state)
        except Exception as e:
            self.log.emit(f"DB save_alert_state error: {e}")
        self.log.emit(f"🔫 {asin} alert latch: {prev} → {state}")

    def _categorize(self, asin: str, result: dict) -> str:
        """Categorize a scan result into the state-machine observation:

            "in_stock" — definitively in stock AND sold by Amazon
            "oos"      — definitively out of stock, OR in stock but
                         held by a third-party seller. From the ping
                         perspective these are the same thing: Amazon
                         has no stock. When Amazon later takes the buy
                         box back after 1h+, that's a restock ping.
            "unknown"  — ambiguous stock, network error, OR seller
                         undetermined. The hysteresis layer ignores
                         these — we never guess in either direction.

        Seller resolution order: the scan result's own seller field
        (wishlist "Shipper / Seller" line or /dp/ merchant info) →
        fresh cache entry → throttled /dp/ verification fetch (result
        lands in the cache for the next scan to use).
        """
        stock_text = (result.get("stock") or "").lower()
        if not stock_text:
            return "unknown"
        # Errors / proxy issues — neither signal.
        for bad in (
            "blocked", "captcha", "timeout", "network error",
            "connection error", "all sources cooling down",
        ):
            if bad in stock_text:
                return "unknown"
        if stock_text.startswith("http"):
            return "unknown"
        # Definitive OOS.
        if "out of stock" in stock_text:
            return "oos"
        # In-stock variants require a real parsed price; without one
        # we can't trust the signal and shouldn't risk a wrong ping.
        if result.get("price_number") is None:
            return "unknown"
        if not (
            "in stock" in stock_text
            or "likely" in stock_text
            or "pre-order" in stock_text
        ):
            return "unknown"

        # In stock — now the AMAZON-SELLER gate.
        sclass = self._resolve_seller(asin, result)
        if sclass == "amazon":
            return "in_stock"
        if sclass == "third_party":
            return "oos"
        return "unknown"

    def _resolve_seller(self, asin: str, result: dict) -> str:
        """Resolve the seller class for this scan: "amazon" |
        "third_party" | "unknown".

        Priority: seller string in the scan result itself (free, most
        current) → fresh cache entry → spawn a throttled /dp/
        verification fetch and return "unknown" for now (the cache
        fills in for subsequent scans, which arrive every ~75ms on
        wishlist items)."""
        seller = clean(result.get("seller") or "")
        if seller:
            cls = seller_class(seller)
            prev = (self.seller_cache.get(asin) or {}).get("class")
            self.seller_cache[asin] = {
                "seller": seller, "class": cls, "ts": time.time(),
            }
            if cls != prev:
                self.log.emit(f"🏷️ {asin} seller: {seller} [{cls}]")
            return cls

        entry = self.seller_cache.get(asin)
        if entry:
            age = time.time() - entry["ts"]
            if age <= SELLER_CLASS_TTL.get(entry["class"], 45):
                return entry["class"]

        # No usable info — verify in the background, stay unknown now.
        self._request_seller_verification(asin)
        return "unknown"

    def _request_seller_verification(self, asin: str) -> None:
        """Spawn a background /dp/ fetch to determine the buy-box
        seller. Deduped via self._verifying; throttled by the cache
        TTLs in _resolve_seller (this is only reached when the cache
        is empty or stale)."""
        if self.checker is None or self.scan_session is None:
            return
        if asin in self._verifying:
            return
        self._verifying.add(asin)
        try:
            task = asyncio.create_task(self._verify_seller(asin))
        except RuntimeError:
            # No running event loop (unit tests) — skip.
            self._verifying.discard(asin)
            return
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    async def _verify_seller(self, asin: str) -> None:
        """Background /dp/ fetch to extract the buy-box seller, cached
        for SELLER_CLASS_TTL. This is the fallback for wishlist renders
        that don't carry the Shipper/Seller line — the /dp/ buy box
        always states the merchant when the item is buyable.

        Races several proxies in parallel (same trick as cursed-ASIN
        scanning): /dp/ pages on hot products are WAF-blocked at high
        rates, and a stalled seller resolution would delay the ping on
        a real drop. First non-blocked response wins."""
        try:
            proxies = self.scan_proxies or [None]
            key = f"__verify_{asin}"
            idx = self.proxy_indexes.get(key, 0)

            result, _raw, next_idx = await self.checker.check_product_parallel(
                self.scan_session,
                asin,
                proxies,
                self.is_proxy_available,
                idx,
                n_parallel=CURSED_PARALLEL_PROXIES,
            )
            self.proxy_indexes[key] = next_idx
            seller = clean(result.get("seller") or "")
            cls = seller_class(seller)
            prev = (self.seller_cache.get(asin) or {}).get("class")
            self.seller_cache[asin] = {
                "seller": seller, "class": cls, "ts": time.time(),
            }
            if cls != prev:
                self.log.emit(
                    f"🏷️ {asin} seller verified via /dp/: "
                    f"{seller or 'undetermined'} [{cls}]"
                )
        except Exception as e:
            self.log.emit(f"Seller verify error {asin}: {e}")
            self.seller_cache[asin] = {
                "seller": "", "class": "unknown", "ts": time.time(),
            }
        finally:
            self._verifying.discard(asin)

    def _observe(
        self, asin: str, observation: str
    ) -> Tuple[Optional[str], Optional[str], Optional[float]]:
        """Apply one observation to the hysteresis counters and return
        (previous_effective_state, new_effective_state, ended_oos_secs).

        observation: "in_stock" | "oos" | "unknown"

        State flips only after the direction's confirmation threshold
        (IN_STOCK_CONFIRM_SCANS / OOS_CONFIRM_SCANS) of consecutive
        same observations. "unknown" observations are no-ops — they
        don't advance OR reset the counters.

        ended_oos_secs is non-None ONLY on the scan where state flips
        oos → in_stock: it's the duration (seconds) of the OOS run that
        just ended. _should_alert uses it to decide restock pings. This
        method fully owns the oos_since lifecycle (set on flip-to-oos,
        clear on flip-to-in_stock), so there's no way for a stale
        timestamp to leak into a later OOS run.

        All transitions persist to DB so the rules survive restarts.
        """
        if observation == "in_stock":
            self.consec_in_stock[asin] = self.consec_in_stock.get(asin, 0) + 1
            self.consec_oos[asin] = 0
        elif observation == "oos":
            self.consec_oos[asin] = self.consec_oos.get(asin, 0) + 1
            self.consec_in_stock[asin] = 0
        # "unknown": leave counters untouched

        prev = self.effective_state.get(asin)
        new = prev
        # Asymmetric thresholds: in-stock confirms fast, OOS needs many
        # more consecutive reads (false OOS is the costly mistake).
        if self.consec_in_stock.get(asin, 0) >= IN_STOCK_CONFIRM_SCANS:
            new = "in_stock"
        elif self.consec_oos.get(asin, 0) >= OOS_CONFIRM_SCANS:
            new = "oos"

        ended_oos_secs: Optional[float] = None

        if new != prev:
            self.effective_state[asin] = new
            try:
                self.db.save_effective_state(asin, new)
            except Exception as e:
                self.log.emit(f"DB save_effective_state error: {e}")

            if new == "oos":
                # Just confirmed OOS — start the clock (only if not
                # already running, so an existing run's start time is
                # preserved across the flip).
                if asin not in self.oos_since:
                    now_oos = time.time()
                    self.oos_since[asin] = now_oos
                    try:
                        self.db.save_oos_since(asin, now_oos)
                    except Exception as e:
                        self.log.emit(f"DB save_oos_since error: {e}")
            elif new == "in_stock":
                # Just confirmed back in stock — close out the OOS run.
                # Compute the duration for the restock decision, then
                # clear the clock (memory + DB) so a future OOS run
                # starts fresh.
                started = self.oos_since.pop(asin, None)
                if started is not None:
                    ended_oos_secs = time.time() - started
                    try:
                        self.db.save_oos_since(asin, None)
                    except Exception as e:
                        self.log.emit(f"DB save_oos_since(clear) error: {e}")

            self.log.emit(
                f"📊 {asin} state {prev or 'unknown'} → {new}"
            )
        return prev, new, ended_oos_secs

    def _should_alert(
        self,
        asin: str,
        result: dict,
        target: float,
    ) -> Tuple[bool, str]:
        """Decide whether the current scan fires an @everyone target
        alert. Returns (should_ping, reason).

        THE RULE:
          - @everyone ONLY when: confirmed in stock, sold by Amazon,
            price at/below target, latch is armed, and the per-item
            re-ping cooldown has passed.
          - target == 0 → digest-only item, never pings.
          - Once fired, the latch stays fired while the item just sits
            at target (the greninja-box fix). Re-arms only when the
            item durably leaves deal territory: a ≥1h confirmed OOS
            run, or price above target * (1 + TARGET_REARM_MARGIN).

        This method ALSO drives the stock state machine on every scan
        (stock state feeds the daily digest), so it must be called for
        digest-only items too.
        """
        observation = self._categorize(asin, result)
        prev_eff, new_eff, ended_oos_secs = self._observe(asin, observation)

        # Remember the freshest confirmed-Amazon in-stock read — the
        # stock-list builder uses it for live price/title/image.
        if observation == "in_stock":
            self.latest_result[asin] = dict(result)

        # RE-ARM #1: item came back from a real (≥1h) OOS run. This is
        # delivered exactly once, on the comeback scan — which is also
        # the scan that may immediately re-fire below if it came back
        # at/below target.
        restock_comeback = (
            ended_oos_secs is not None
            and ended_oos_secs >= MIN_RESTOCK_OOS_SECONDS
        )
        if restock_comeback and self.alert_state.get(asin) == "fired":
            self._set_alert_state(asin, "armed")
            hours = int(ended_oos_secs // 3600)
            mins = int((ended_oos_secs % 3600) // 60)
            self.log.emit(
                f"🔄 {asin} re-armed — back in stock after "
                f"{hours}h {mins}m without Amazon stock."
            )

        # BURST-CONFIRM: this scan saw the item in stock, but effective
        # state hasn't flipped yet (the confirm streak is mid-way). If
        # it's a real, armed ping candidate at/below target, kick the
        # wishlist crawler into a short high-rate burst so the confirming
        # read lands fast — before a drop-time block storm can stall it.
        # This never lowers IN_STOCK_CONFIRM_SCANS; it only speeds the
        # arrival of confirm #2.
        if observation == "in_stock" and new_eff != "in_stock":
            pn = result.get("price_number")
            if (
                pn is not None
                and target and target > 0
                and pn <= target
                and self.alert_state.get(asin, "armed") == "armed"
            ):
                self._wishlist_burst_until = (
                    time.monotonic() + WISHLIST_BURST_SECONDS
                )

        # HARD GATE: a ping can only fire off a scan that is itself a
        # seller-confirmed Amazon in-stock read, with the confirmed
        # effective state agreeing. (3P / unknown-seller / error scans
        # can never fire.)
        if observation != "in_stock" or new_eff != "in_stock":
            return False, ""

        price_number = result.get("price_number")
        if price_number is None:
            return False, ""

        # Digest-only items (no target) never ping.
        if not target or target <= 0:
            return False, ""

        # RE-ARM #2: price rose clearly above the target zone. The 5%
        # margin means wobble right at the target line can't re-arm —
        # only a real move out of deal territory does.
        if (
            self.alert_state.get(asin) == "fired"
            and price_number > target * (1 + TARGET_REARM_MARGIN)
        ):
            self._set_alert_state(asin, "armed")
            self.log.emit(
                f"🔄 {asin} re-armed — price ${price_number:.2f} left the "
                f"target zone (${target:.2f} +{int(TARGET_REARM_MARGIN*100)}%)."
            )

        # Not at target → nothing to fire.
        if price_number > target:
            return False, ""

        # At/below target: latch + cooldown decide.
        if self.alert_state.get(asin, "armed") != "armed":
            return False, ""

        last_ping = self.last_target_ping_at.get(asin, 0.0)
        if time.time() - last_ping < MIN_TARGET_REPING_SECONDS:
            if asin not in self._cooldown_logged:
                self._cooldown_logged.add(asin)
                remaining = int(
                    (MIN_TARGET_REPING_SECONDS - (time.time() - last_ping)) / 60
                )
                self.log.emit(
                    f"⏸️ {asin} at target but re-ping cooldown active "
                    f"(~{remaining}m left) — holding armed, will fire "
                    f"when cooldown expires if still at target."
                )
            return False, ""

        # SAME-PRICE SUPPRESSION (anti-nag). We already announced this
        # item at this price (or cheaper). A flickery listing that keeps
        # bouncing OOS→in-stock would otherwise re-fire the identical
        # ping every few hours. Only a real price DROP, or a full
        # SAME_PRICE_REPING_SECONDS of quiet, gets to speak again.
        prev_announced = self.last_pinged_price.get(asin)
        if (
            prev_announced is not None
            and price_number >= prev_announced - PRICE_DROP_EPSILON
            and time.time() - last_ping < SAME_PRICE_REPING_SECONDS
        ):
            if asin not in self._same_price_logged:
                self._same_price_logged.add(asin)
                hrs = (SAME_PRICE_REPING_SECONDS - (time.time() - last_ping)) / 3600
                self.log.emit(
                    f"🔁 {asin} at target but already announced at "
                    f"${prev_announced:.2f} — same price, no re-ping "
                    f"(~{hrs:.1f}h left, or any drop below "
                    f"${prev_announced:.2f} fires immediately)."
                )
            return False, ""

        # Reason strings deliberately never reveal the target price.
        if restock_comeback:
            reason = "Restocked — Target Price Reached"
        else:
            reason = "Target Price Reached"
        return True, reason

    def _maybe_announce(
        self,
        product: dict,
        result: dict,
        source: str,
    ) -> bool:
        """Centralized ping gate. Flipping the latch to 'fired' BEFORE
        any await is the atomic claim that stops concurrent /dp/ +
        wishlist scans from double-firing."""
        asin = product["asin"]
        try:
            target = float(product.get("target_price") or 0)
        except (TypeError, ValueError):
            target = 0.0

        should, reason = self._should_alert(asin, result, target)
        if not should:
            return False

        # STARTUP SWEEP: no pings until the boot stock list has gone
        # out. The item stays armed; if it's still at target when the
        # list is built, the list latches it "fired" (it's visible on
        # today's list — that IS its announcement).
        if not self._startup_sweep_done:
            if asin not in self._sweep_held_logged:
                self._sweep_held_logged.add(asin)
                self.log.emit(
                    f"🧹 {asin} at target during startup sweep — held. "
                    f"It will appear in the stock list instead (no ping)."
                )
            return False

        # Atomic claim: fired latch + cooldown stamp, both sync.
        self._set_alert_state(asin, "fired")
        now_epoch = time.time()
        self.last_target_ping_at[asin] = now_epoch
        self._cooldown_logged.discard(asin)

        price_number = float(result.get("price_number") or 0)
        # Remember what we announced — a later ping at this same price
        # is nagging, one at a lower price is real news.
        self.last_pinged_price[asin] = price_number
        self._same_price_logged.discard(asin)
        ts_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.db.save_ping(asin, price_number, ts_iso)
        except Exception as e:
            self.log.emit(f"DB save_ping error: {e}")

        self.log.emit(
            f"🎯 TARGET HIT | {asin} @ {result.get('price') or '-'} "
            f"(target ${target:.2f}) — {reason} (via {source})"
        )
        try:
            self.hit_counted.emit(asin, price_number)
        except Exception:
            pass

        self._fire_target_pings(
            product=product,
            result=result,
            reason=reason,
        )
        return True

    # ------------------------------------------------------------------
    # Per-product /dp/ scan loop
    # ------------------------------------------------------------------

    async def scan_product_forever(
        self,
        product,
        checker,
        proxies,
    ):
        """One product, one loop, forever. Each product gets its own
        asyncio task — a slow ASIN's timeout doesn't block any other.

        Reads self.scan_session on every iteration so the profile
        rotation loop can swap the underlying session and have every
        running scan task pick up the new session immediately.
        """
        asin = product["asin"]
        # Burst mode: when a product flips OOS → in stock, drop the
        # scan interval to 100ms for 30s to keep stock state fresh
        # in case the user wants to act fast.
        last_stock_oos = False
        burst_until = 0.0

        while self.running:
            try:
                # Source selection: prefer wishlist data when available
                # (faster + lighter on Amazon's WAF), fall back to /dp/.
                has_wishlist_data = asin in self.wishlist_results

                if has_wishlist_data:
                    result = dict(self.wishlist_results[asin])
                    proxy_source = result.get("source", "wishlist")
                    raw_results: List[Dict[str, Any]] = []
                else:
                    idx = self.proxy_indexes.get(asin, 0)
                    if asin in self.cursed_asins:
                        result, raw_results, new_idx = (
                            await checker.check_product_parallel(
                                self.scan_session,
                                asin,
                                proxies,
                                self.is_proxy_available,
                                idx,
                                n_parallel=CURSED_PARALLEL_PROXIES,
                            )
                        )
                    else:
                        result, raw_results, new_idx = (
                            await checker.check_product_single(
                                self.scan_session,
                                asin,
                                proxies,
                                self.is_proxy_available,
                                idx,
                            )
                        )
                    self.proxy_indexes[asin] = new_idx
                    proxy_source = result.get("source", "direct")

                    for r in raw_results:
                        src = r.get("source", "direct")
                        self.update_proxy_stats(src, r["stock"])
                        self.evaluate_proxy_cooldown(src)

                self.update_asin_stats(asin, [result] if raw_results == [] else raw_results)
                self.evaluate_asin_cursed(asin)

                if not has_wishlist_data:
                    self._record_health(self._is_healthy_response(result))

                # Skip DB write + UI emit when nothing changed. Skip
                # DB writes on transient error states so the table
                # keeps showing last known good values.
                result_stock_lower = (result.get("stock") or "").lower()
                is_error_state = (
                    "timeout" in result_stock_lower
                    or "network error" in result_stock_lower
                    or "connection error" in result_stock_lower
                    or "blocked" in result_stock_lower
                    or "captcha" in result_stock_lower
                    or result_stock_lower.startswith("http")
                    or "all sources cooling down" in result_stock_lower
                )

                result_key = (
                    result.get("title", ""),
                    result.get("price", ""),
                    result.get("stock", ""),
                )
                if self.last_results.get(asin) != result_key:
                    if not is_error_state:
                        self.last_results[asin] = result_key
                        self.db.update_result(
                            asin,
                            result["title"],
                            result["price"],
                            result["price_number"],
                            result["stock"],
                            result["timestamp"],
                        )
                    self.product_updated.emit({**product, **result})

                # Per-scan log line, throttled: always log when the
                # result changed, otherwise at most once per 30s per
                # ASIN. Unthrottled, 20+ products at sub-second
                # intervals push 40+ identical lines/sec through the
                # Qt log widget for zero information.
                log_key = (result.get("price", ""), result.get("stock", ""))
                now_mono = time.monotonic()
                prev_log = self._scan_log_state.get(asin)
                if (
                    prev_log is None
                    or prev_log[0] != log_key
                    or now_mono - prev_log[1] >= 30
                ):
                    self._scan_log_state[asin] = (log_key, now_mono)
                    self.log.emit(
                        f"{asin} | {result['price'] or '-'} | "
                        f"{result['stock']} | {proxy_source} | "
                        f"{result['title'][:60]}"
                    )

                # Burst mode trigger — uses raw observation, not the
                # hysteresis-confirmed effective state, so a single
                # OOS → in_stock flip fires burst mode immediately.
                obs_category = self._categorize(asin, result)
                is_real_in_stock = obs_category == "in_stock"
                if last_stock_oos and is_real_in_stock:
                    burst_until = time.monotonic() + 30
                    self.log.emit(f"BURST MODE on {asin} for 30s (OOS → In Stock)")
                last_stock_oos = obs_category == "oos"

                # Stock-ping decision: when wishlist data is available
                # the wishlist scanner is the canonical source — it
                # already calls _maybe_announce per probe. Calling
                # again from here would just double the hysteresis
                # counter increments without adding signal. So we only
                # announce from here for non-wishlist ASINs.
                if not has_wishlist_data:
                    self._maybe_announce(
                        product, result, source=f"dp/{proxy_source}"
                    )

            except Exception as e:
                self.log.emit(f"ERROR {asin}: {e}")

            if time.monotonic() < burst_until:
                await asyncio.sleep(0.1)
            elif asin in self.cursed_asins:
                await asyncio.sleep(CURSED_SCAN_INTERVAL)
            else:
                await asyncio.sleep(self._read_interval())

    # ------------------------------------------------------------------
    # Wishlist HTTP scanner
    # ------------------------------------------------------------------

    def _process_wishlist_results(
        self,
        results: Dict[str, Dict[str, Any]],
        source: str,
    ) -> None:
        """Shared post-processing for the wishlist HTTP fan-out.
        Runs _maybe_announce for every enabled tracked ASIN — the gate
        + state machine handle dedup, so it's safe to call on every
        result from every probe.

        Safe to call concurrently from multiple tasks — only mutations
        are dict updates (atomic in CPython single-thread async) and
        task spawns.
        """
        if not results:
            return

        if self.notifier is not None and self.discord_session is not None:
            # Cached enabled-products snapshot (5s TTL) — this method
            # runs on every wishlist probe (~13x/sec); a fresh SQLite
            # query each time was pure waste.
            enabled_by_asin = self._enabled_products()
            for ai, dat in results.items():
                prod = enabled_by_asin.get(ai)
                if prod is None:
                    continue
                self._maybe_announce(
                    prod, dict(dat), source=f"wishlist/{source}"
                )

        # MERGE this fetch into the rolling view instead of replacing.
        # Amazon may return only a subset of the list per fetch; a plain
        # assignment would momentarily drop the missing items and shove
        # them back onto the WAF-blocked /dp/ path. Keep every item seen
        # within WISHLIST_MERGE_TTL_SECONDS.
        now_mono = time.monotonic()
        for ai, dat in results.items():
            self._wishlist_seen[ai] = (dict(dat), now_mono)
        cutoff = now_mono - WISHLIST_MERGE_TTL_SECONDS
        self._wishlist_seen = {
            ai: (dat, ts)
            for ai, (dat, ts) in self._wishlist_seen.items()
            if ts >= cutoff
        }
        self.previous_wishlist_results = self.wishlist_results
        self.wishlist_results = {
            ai: dat for ai, (dat, ts) in self._wishlist_seen.items()
        }

    async def _fetch_wishlist_url(
        self,
        url: str,
        proxy: Optional[str],
    ) -> Tuple[Dict[str, Dict[str, Any]], str, str]:
        """Low-level wishlist GET to an arbitrary URL (so we can pass a
        cache-busted variant and the paginated 'show more' URLs).
        Returns (results_dict, status_message, next_page_url). The last
        value is the relative 'load more' URL for the next page, or ''
        if this is the final page.
        """
        if self.scan_session is None or self.wishlist_scanner is None:
            return {}, "no session", ""

        # The paginated /slv/items fragments are an XHR endpoint; the
        # main list page is a normal navigation. Send the headers each
        # one expects.
        is_more = "/slv/items" in url
        headers = {
            "Accept": (
                "text/html,*/*;q=0.9" if is_more else
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-CA,en;q=0.9,en-US;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": self.wishlist_scanner.wishlist_url(),
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors" if is_more else "navigate",
            "Sec-Fetch-Dest": "empty" if is_more else "document",
            "DNT": "1",
        }
        if is_more:
            headers["X-Requested-With"] = "XMLHttpRequest"
        else:
            headers["Sec-Fetch-User"] = "?1"
            headers["Upgrade-Insecure-Requests"] = "1"
        try:
            r = await self.scan_session.get(
                url,
                headers=headers,
                proxy=proxy,
                timeout=HTTP_TIMEOUT_SECONDS + 4,
                allow_redirects=True,
            )
            if r.status_code >= 400:
                return {}, f"HTTP {r.status_code}", ""
            html = r.text or ""
            html_lower = html.lower()
            if "captcha" in html_lower or "robot check" in html_lower:
                return {}, "Blocked/Captcha", ""
            return (
                self.wishlist_scanner.parse_wishlist(html),
                "",
                extract_show_more_url(html),
            )
        except Exception as e:
            err = str(e).lower()
            if "timed out" in err or "timeout" in err:
                return {}, "Timeout", ""
            if "could not resolve" in err or "connection" in err:
                return {}, "Connection error", ""
            return {}, "Network error", ""

    async def wishlist_http_scanner_forever(
        self,
        proxies: List[Optional[str]],
    ) -> None:
        """Paginated HTTP wishlist crawler — the primary stock detector.

        Amazon only server-renders the first ~10 items of a wishlist and
        lazy-loads the rest, so a single fetch misses everything past the
        first page. Each cycle this walks the full "show more" chain
        (page 1 → page 2 → ... via the paginationToken) through the
        rotating proxy pool, cache-busting every request, and merges each
        page into the rolling view (WISHLIST_MERGE_TTL_SECONDS) so a
        page that gets momentarily blocked keeps its last-known data.

        Stops each crawl as soon as a page adds no new ASINs (the token
        wraps and re-serves once the list is exhausted) or the page cap
        is hit. Every page gets a few proxy retries before we skip it
        for the cycle.

        Wishlist must be public for this to work (no auth headers).
        """
        if self.wishlist_scanner is None:
            return

        rotation: List[Optional[str]] = list(proxies) if proxies else [None]
        if not rotation:
            rotation = [None]

        self.log.emit(
            f"HTTP wishlist crawler launching: {len(rotation)} proxies, "
            f"paginated full-list walk (up to {WISHLIST_MAX_PAGES} pages/cycle)."
        )

        domain = self.wishlist_scanner.amazon_domain
        proxy_idx = 0

        def next_proxy() -> Optional[str]:
            nonlocal proxy_idx
            for _ in range(len(rotation)):
                p = rotation[proxy_idx % len(rotation)]
                proxy_idx += 1
                if p is None or self.is_proxy_available(proxy_label_from_url(p)):
                    return p
            # everything cooling down — just return the next one
            p = rotation[proxy_idx % len(rotation)]
            proxy_idx += 1
            return p

        async def fetch_page(page_url: str):
            """Fetch one wishlist page, retrying across a few proxies.
            Returns (parsed_items | None, next_page_url)."""
            for _try in range(WISHLIST_PAGE_RETRIES):
                if not self.running:
                    return None, ""
                proxy = next_proxy()
                cb = int(time.time() * 1000)
                sep = "&" if "?" in page_url else "?"
                parsed, status, more = await self._fetch_wishlist_url(
                    f"{page_url}{sep}_={cb}", proxy
                )
                if not status:
                    return parsed, more
            return None, ""  # blocked after retries — merge+TTL covers it

        async def discover():
            """Sequential walk of the show-more chain. Returns the list
            of page URLs that contributed items + the set of ASINs seen."""
            url = self.wishlist_scanner.wishlist_url()
            page_urls: List[str] = []
            seen: set = set()
            for _page in range(WISHLIST_MAX_PAGES):
                parsed, more = await fetch_page(url)
                if parsed is None:
                    break
                self._process_wishlist_results(parsed, source="crawl")
                new = set(parsed.keys()) - seen
                if new:
                    page_urls.append(url)
                seen |= set(parsed.keys())
                if not more or not new:
                    break
                url = (domain + more) if more.startswith("/") else more
            return page_urls, seen

        async def fetch_parallel(page_urls: List[str]):
            """Fetch all known pages concurrently (one proxy each), merge."""
            async def one(u):
                parsed, _more = await fetch_page(u)
                if parsed:
                    self._process_wishlist_results(parsed, source="crawl-par")
                    return set(parsed.keys())
                return set()
            got = await asyncio.gather(*[one(u) for u in page_urls])
            seen: set = set()
            for s in got:
                seen |= s
            return seen

        last_discovery = 0.0
        while self.running:
            cycle_start = time.monotonic()
            try:
                need_discovery = (
                    not self._wl_page_urls
                    or cycle_start - last_discovery >= WISHLIST_REDISCOVER_SECONDS
                )
                if need_discovery:
                    page_urls, seen = await discover()
                    if page_urls:
                        self._wl_page_urls = page_urls
                        # Baseline coverage from the authoritative walk.
                        self._last_crawl_items = len(seen)
                    last_discovery = cycle_start
                    mode = "seq"
                else:
                    seen = await fetch_parallel(self._wl_page_urls)
                    mode = "par"
                    # Coverage collapsed → tokens likely went stale; force a
                    # fresh sequential discovery on the next cycle.
                    if (
                        self._last_crawl_items
                        and len(seen) < self._last_crawl_items * 0.5
                    ):
                        self._wl_page_urls = []

                self._last_crawl_pages = len(self._wl_page_urls)
                if seen:
                    now_mono = time.monotonic()
                    if now_mono - getattr(self, "_last_crawl_log", 0) >= WISHLIST_CRAWL_LOG_SECONDS:
                        self._last_crawl_log = now_mono
                        self.log.emit(
                            f"🧭 Wishlist crawl [{mode}]: {len(seen)} items "
                            f"across {self._last_crawl_pages} page(s)."
                        )
            except Exception as e:
                self.log.emit(f"Wishlist crawl error: {e}")

            elapsed = time.monotonic() - cycle_start
            if time.monotonic() < self._wishlist_burst_until:
                # A targeted item is one confirming read away — crawl hard
                # so effective state flips (and the ping fires) fast.
                await asyncio.sleep(WISHLIST_BURST_SLEEP)
            else:
                await asyncio.sleep(
                    max(WISHLIST_CYCLE_MIN_SLEEP, WISHLIST_CYCLE_SECONDS - elapsed)
                )

    # ------------------------------------------------------------------
    # Background helpers
    # ------------------------------------------------------------------

    async def keepalive_loop(self, domain: str) -> None:
        """Background heartbeat to keep an HTTP/2 (or HTTP/3) connection
        to amazon's edge pre-established on self.scan_session.

        curl_cffi pools connections within a session. An idle pool
        eventually times out at the TCP/QUIC layer, forcing a fresh
        TLS handshake on the next request — ~50-150ms on the critical
        path of a hit ping. A periodic lightweight GET keeps the pool
        warm so real scans always reuse it.
        """
        await asyncio.sleep(10)
        while self.running:
            try:
                if self.scan_session is None:
                    await asyncio.sleep(KEEPALIVE_INTERVAL_SECONDS)
                    continue
                try:
                    await self.scan_session.get(
                        f"{domain.rstrip('/')}/robots.txt",
                        timeout=5,
                        headers={
                            "Accept": "text/plain,*/*;q=0.8",
                            "Alt-Used": urlparse(domain).hostname or "www.amazon.com",
                        },
                        allow_redirects=False,
                    )
                except Exception:
                    pass
            except Exception as e:
                self.log.emit(f"Keepalive error: {e}")
            await asyncio.sleep(KEEPALIVE_INTERVAL_SECONDS)

    async def stats_logger(self):
        while self.running:
            await asyncio.sleep(STATS_LOG_SECONDS)
            self.log_proxy_stats()
            self.log_asin_stats()

    # ------------------------------------------------------------------
    # Daily digest
    # ------------------------------------------------------------------

    def _build_digest_items(self) -> list:
        """Collect every enabled watchlist item whose hysteresis-
        confirmed state is in-stock (sold by Amazon). Freshest data
        wins: live wishlist result first, DB row as fallback."""
        items = []
        for prod in self.db.get_products():
            if not int(prod.get("enabled", 1)):
                continue
            asin = prod["asin"]
            if self.effective_state.get(asin) != "in_stock":
                continue
            live = (
                self.wishlist_results.get(asin)
                or self.latest_result.get(asin)
                or {}
            )
            url = live.get("url") or (
                self.checker.product_url(asin)
                if self.checker
                else f"https://www.amazon.ca/dp/{asin}"
            )
            if live:
                price = live.get("price") or ""
                price_number = live.get("price_number")
            else:
                price = prod.get("last_price") or ""
                price_number = prod.get("last_price_number")
            items.append({
                "asin": asin,
                "title": live.get("title") or prod.get("title") or asin,
                "price": price,
                "price_number": (
                    float(price_number) if price_number is not None else None
                ),
                "url": with_affiliate_tag(url),
                "image_url": live.get("image_url") or "",
                "target": float(prod.get("target_price") or 0),
            })
        items.sort(key=lambda it: (it["title"] or "").lower())
        return items

    def _latch_at_target(self, items: list) -> int:
        """Latch every at-target in-stock item as 'fired' so it can't
        ping — an item already in stock at/below target isn't a fresh
        drop worth an @role. Genuinely new events (a ≥1h OOS comeback,
        or the price re-entering the target zone from >5% above) re-arm
        it as usual. Returns how many were newly latched."""
        latched = 0
        for it in items:
            target = it.get("target") or 0
            pn = it.get("price_number")
            if (
                target > 0
                and pn is not None
                and pn <= target
                and self.alert_state.get(it["asin"]) != "fired"
            ):
                self._set_alert_state(it["asin"], "fired")
                # Appearing on the stock list IS this item's
                # announcement — record price + time so the same-price
                # rule treats it exactly like a ping. A later drop
                # below this price still fires immediately.
                asin = it["asin"]
                self.last_pinged_price[asin] = float(pn)
                if asin not in self.last_target_ping_at:
                    self.last_target_ping_at[asin] = time.time()
                # PERSIST it: in-memory only would mean the next restart
                # forgets what we announced and re-opens the nag window.
                try:
                    self.db.save_ping(
                        asin,
                        float(pn),
                        datetime.fromtimestamp(
                            self.last_target_ping_at[asin]
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                    )
                except Exception as e:
                    self.log.emit(f"DB latch-announce save error: {e}")
                latched += 1
        return latched

    def _send_digest(self) -> None:
        """Build + send the stock-list message (the 24h list), latch
        every at-target item on it, persist price history for the
        (was $X) markers, and stamp last_digest_at."""
        items = self._build_digest_items()
        latched = self._latch_at_target(items)

        # Price history from the previous list → (was $X).
        try:
            prev_prices = {
                k: float(v)
                for k, v in json.loads(
                    self.db.get_setting("digest_prices") or "{}"
                ).items()
            }
        except Exception:
            prev_prices = {}

        if self.discord_session is not None:
            for n in (self.notifier, self.secondary_notifier, self.bot):
                if n is None:
                    continue
                self._spawn_send(
                    lambda n=n, i=[dict(x) for x in items], p=dict(prev_prices):
                        n.send_digest(self.discord_session, items=i, prev_prices=p),
                    "stock list",
                )

        try:
            self.db.set_setting("digest_prices", json.dumps({
                it["asin"]: it["price_number"]
                for it in items
                if it.get("price_number") is not None
            }))
        except Exception as e:
            self.log.emit(f"Digest price-history save error: {e}")
        self.db.set_setting("last_digest_at", str(time.time()))

        # Safety: keep the gate open (startup opens it; this is a no-op
        # once live, but guards a fresh-install first-send path).
        self._startup_sweep_done = True
        self.log.emit(
            f"📦 Stock list sent (24h) — {len(items)} purchasable "
            f"item(s), {latched} at-target item(s) latched (no ping)."
        )

    async def daily_digest_loop(self) -> None:
        """Startup housekeeping → 24h stock list.

        Phase 1 (every boot): wait DIGEST_STARTUP_WARMUP_SECONDS while
        the scanners do a full pass, then latch at-target in-stock items
        (so they can't false-ping) and open the ping gate — WITHOUT
        sending a Discord message. This is why restarting to deploy
        updates no longer spams the channel with a fresh stock list.

        Phase 2: send the stock list only when DIGEST_INTERVAL_SECONDS
        has elapsed since the last one (persisted as last_digest_at), so
        the list is a true once-per-24h post, independent of restarts.
        A fresh install (no last_digest_at) posts one shortly after boot
        to establish the baseline.
        """
        started = time.monotonic()
        while self.running and (
            time.monotonic() - started < DIGEST_STARTUP_WARMUP_SECONDS
        ):
            await asyncio.sleep(1)
        if not self.running:
            return

        # Startup housekeeping: latch + open the gate, but do NOT send.
        try:
            items = self._build_digest_items()
            latched = self._latch_at_target(items)
            self.log.emit(
                f"🧹 Startup sweep complete — {len(items)} item(s) in "
                f"stock, {latched} at-target latched (no ping). Boot "
                f"stock-list suppressed; it posts on the 24h schedule."
            )
        except Exception as e:
            self.log.emit(f"Startup latch error: {e}")
        finally:
            # Always open the gate so target hunting goes live even if
            # the latch pass hit an error.
            self._startup_sweep_done = True

        while self.running:
            await asyncio.sleep(60)
            try:
                try:
                    last = float(self.db.get_setting("last_digest_at") or 0)
                except (TypeError, ValueError):
                    last = 0.0
                if time.time() - last >= DIGEST_INTERVAL_SECONDS:
                    self._send_digest()
            except Exception as e:
                self.log.emit(f"Digest loop error: {e}")

    async def cookie_reset_loop(self):
        """Clear the scan session's accumulated cookies periodically.
        Prevents Amazon's WAF from fingerprinting us as a long-running
        session bouncing between IPs."""
        while self.running:
            await asyncio.sleep(COOKIE_RESET_SECONDS)
            if not self.running:
                return
            try:
                if self.scan_session is not None:
                    self.scan_session.cookies.clear()
                    self.log.emit("Cookies cleared — session looks fresh to WAF.")
            except Exception as e:
                self.log.emit(f"Cookie clear failed: {e}")

    async def profile_rotation_loop(self):
        """Adaptive TLS-profile rotation. Rotates when healthy-ASIN
        fail rate degrades past ROTATION_FAIL_THRESHOLD over a rolling
        window, or after ROTATION_MAX_PROFILE_SECONDS as a safety net."""
        last_rotation_ts = time.monotonic()
        while self.running:
            await asyncio.sleep(ROTATION_CHECK_INTERVAL)
            if not self.running:
                return

            time_on_profile = time.monotonic() - last_rotation_ts
            if time_on_profile < ROTATION_COOLDOWN_SECONDS:
                continue

            total, fail_rate = self._health_stats()
            should_rotate = False
            reason = ""
            if total >= ROTATION_MIN_SAMPLES and fail_rate >= ROTATION_FAIL_THRESHOLD:
                should_rotate = True
                reason = (
                    f"healthy-ASIN fail rate {int(fail_rate * 100)}% "
                    f"over {total} recent scans"
                )
            elif time_on_profile >= ROTATION_MAX_PROFILE_SECONDS:
                should_rotate = True
                reason = f"max profile duration ({int(time_on_profile)}s)"

            if not should_rotate:
                continue

            try:
                next_idx = (self._profile_index + 1) % len(IMPERSONATE_PROFILES)
                next_profile = IMPERSONATE_PROFILES[next_idx]
                current_profile = IMPERSONATE_PROFILES[self._profile_index]
                self.log.emit(
                    f"Rotating TLS profile: {current_profile} → {next_profile} ({reason})"
                )

                new_session = AsyncSession(
                    impersonate=next_profile,
                    max_clients=HTTP_CONCURRENCY,
                )
                await new_session.__aenter__()
                old_session = self.scan_session
                self.scan_session = new_session
                self._profile_index = next_idx

                # Drain window — let in-flight requests on the old
                # session finish before closing it.
                await asyncio.sleep(5)
                if old_session is not None:
                    try:
                        await old_session.__aexit__(None, None, None)
                    except Exception:
                        pass

                self.health_events.clear()
                last_rotation_ts = time.monotonic()
            except Exception as e:
                self.log.emit(f"Profile rotation error: {e}")

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run_async(self):
        self.running = True
        self.status.emit("Running")

        # Sync the products table to watchlist.txt if the file exists.
        # Lets users manage the product list via a plaintext editor
        # in addition to the GUI — edits take effect on next start.
        try:
            added, removed, updated = self.db.sync_from_watchlist()
            if added or removed or updated:
                self.log.emit(
                    f"watchlist.txt sync: +{added} added, "
                    f"-{removed} removed, ~{updated} target updated"
                )
        except Exception as e:
            self.log.emit(f"watchlist.txt sync error: {e}")

        # ==============================================================
        # STARTUP STATE REHYDRATION — the anti-restart-spam guarantee.
        #
        # Rule: after a restart, NOTHING pings just because the bot came
        # back up. The STARTUP SWEEP is the primary guard: pings are
        # impossible until the boot stock list is sent, and everything
        # at target on that list gets latched from LIVE data (see
        # _send_digest). Rehydration only restores:
        #   1. Persisted armed/fired latches (products.alert_state).
        #   2. The last ping timestamp (products.last_pinged_at) so the
        #      re-ping cooldown spans restarts.
        #   3. A clean slate for the stock-state machine: we do NOT
        #      reload effective_state, and we wipe persisted oos_since
        #      — a restock re-arm requires a real ≥1h OOS run observed
        #      entirely AFTER startup.
        # ==============================================================
        try:
            products = self.db.get_products()
            restored = 0
            cleared_clocks = 0
            for prod in products:
                asin = prod["asin"]

                latch = prod.get("alert_state")
                if latch in ("armed", "fired"):
                    self.alert_state[asin] = latch
                    restored += 1

                # Last announced price — without this the same-price
                # rule would forget across restarts and every deploy
                # would re-open the nag window.
                lpp = prod.get("last_pinged_price")
                if lpp is not None:
                    try:
                        self.last_pinged_price[asin] = float(lpp)
                    except (TypeError, ValueError):
                        pass

                lpa = prod.get("last_pinged_at")
                if lpa:
                    try:
                        self.last_target_ping_at[asin] = datetime.strptime(
                            lpa, "%Y-%m-%d %H:%M:%S"
                        ).timestamp()
                    except (ValueError, TypeError):
                        pass

                if prod.get("oos_since") is not None:
                    try:
                        self.db.save_oos_since(asin, None)
                    except Exception as e:
                        self.log.emit(f"DB clear oos_since error: {e}")
                    cleared_clocks += 1

            self.log.emit(
                f"State rehydrated: {restored} alert latches restored, "
                f"{cleared_clocks} OOS clocks reset. Startup sweep begins "
                f"— no pings until the boot stock list is sent."
            )
        except Exception as e:
            self.log.emit(f"DB state rehydration error: {e}")

        try:
            settings = self.db.get_settings()
            domain = settings.get("amazon_domain", "https://www.amazon.ca")
            notifier = DiscordNotifier(settings.get("discord_webhook", ""))

            proxies = load_proxies()
            # Snapshot for the seller-verification fetches, which run
            # outside the per-product loops and need their own rotation.
            self.scan_proxies = list(proxies)
            self.log.emit(f"HTTP checker loaded {len(proxies)} source(s).")

            semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)
            checker = HTTPAmazonChecker(domain, semaphore)
            self.checker = checker
            self.notifier = notifier

            # Optional bot, purely additive. Any failure here is swallowed
            # so webhook delivery is never affected by it.
            try:
                from bot import StockPingerBot, DISCORD_AVAILABLE, BOT_TOKEN
                if BOT_TOKEN and not DISCORD_AVAILABLE:
                    self.log.emit(
                        "🤖 AMP_BOT_TOKEN set but discord.py is missing. "
                        "Run: venv/bin/pip install -U discord.py"
                    )
                elif BOT_TOKEN:
                    self.bot = StockPingerBot(self.db, worker=self, log=self.log)
                    self.log.emit(
                        "🤖 Bot token found, connecting. Webhooks continue "
                        "unchanged regardless of how this goes."
                    )
                else:
                    self.log.emit("🤖 No AMP_BOT_TOKEN, webhook-only mode.")
            except Exception as e:
                self.bot = None
                self.log.emit(f"🤖 Bot init failed, webhooks unaffected: {e}")

            async with aiohttp.ClientSession() as discord_session:
                self.discord_session = discord_session

                # Pick initial TLS profile.
                if PROFILE_ROTATION_ENABLED:
                    self._profile_index = 0
                    initial_profile = IMPERSONATE_PROFILES[0]
                else:
                    self._profile_index = 0
                    initial_profile = STATIC_IMPERSONATE_PROFILE

                self.scan_session = AsyncSession(
                    impersonate=initial_profile,
                    max_clients=HTTP_CONCURRENCY,
                )
                await self.scan_session.__aenter__()
                self.log.emit(
                    f"Scan session opened with impersonate={initial_profile}. "
                    f"Profile rotation {'ENABLED' if PROFILE_ROTATION_ENABLED else 'DISABLED'}."
                )

                try:
                    products = [
                        p for p in self.db.get_products()
                        if int(p.get("enabled", 1)) == 1
                    ]
                    if not products:
                        self.log.emit("No enabled products to check.")

                    tasks = [
                        asyncio.create_task(
                            self.scan_product_forever(p, checker, proxies)
                        )
                        for p in products
                    ]
                    self.log.emit(
                        f"Spawned {len(tasks)} product scan tasks. "
                        f"Each rotates through {len(proxies)} proxies."
                    )

                    # Wishlist HTTP fan-out — the fast stock detector.
                    wishlist_id = (self.db.get_setting("wishlist_id") or "").strip()
                    if wishlist_id:
                        self.wishlist_scanner = WishlistScanner(domain, wishlist_id)
                        self.log.emit(
                            f"Wishlist scanner enabled: HTTP fan-out @ "
                            f"{WISHLIST_HTTP_STAGGER_SECONDS}s per-proxy stagger. "
                            f"id={wishlist_id}"
                        )
                        tasks.append(asyncio.create_task(
                            self.wishlist_http_scanner_forever(proxies)
                        ))
                    else:
                        self.log.emit(
                            "Wishlist scanner DISABLED (no wishlist_id in Settings). "
                            "Add a public wishlist ID for sub-second restock detection."
                        )

                    if self.bot is not None and self.bot.enabled:
                        tasks.append(asyncio.create_task(self.bot.start()))

                    tasks.append(asyncio.create_task(self.keepalive_loop(domain)))
                    tasks.append(asyncio.create_task(self.stats_logger()))
                    tasks.append(asyncio.create_task(self.cookie_reset_loop()))
                    tasks.append(asyncio.create_task(self.daily_digest_loop()))
                    if PROFILE_ROTATION_ENABLED:
                        tasks.append(asyncio.create_task(self.profile_rotation_loop()))

                    self.status.emit("Running")

                    try:
                        while self.running:
                            await asyncio.sleep(0.25)
                    finally:
                        # Close the gateway cleanly rather than having it
                        # cancelled mid-frame.
                        if self.bot is not None:
                            try:
                                await self.bot.close()
                            except Exception:
                                pass
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                finally:
                    if self.scan_session is not None:
                        try:
                            await self.scan_session.__aexit__(None, None, None)
                        except Exception:
                            pass
                        self.scan_session = None

        except Exception as e:
            self.log.emit(f"FATAL ERROR: {e}")
            import traceback
            self.log.emit(traceback.format_exc())

        finally:
            # Let in-flight Discord pings finish before tearing down.
            if self.background_tasks:
                try:
                    await asyncio.wait(list(self.background_tasks), timeout=5)
                except Exception:
                    pass
            self.status.emit("Stopped")

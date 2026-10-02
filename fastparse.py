"""Regex-only product page extraction, scoped to anchored windows.

Why this exists: building a BeautifulSoup DOM to read six fields costs
382-463ms of CPU per page, measured on the live box. The bot runs one
event loop on one core, so at ~4 in-stock reads/second that single call
was eating most of the core and every hot item queued behind it. The
per-product interval sat at 4.9s no matter what the rate limits said.

TWO RULES LEARNED THE HARD WAY, both from validation failures:

1. SCOPE EVERY SEARCH. The DOM parser reads price only from specific
   containers (#corePriceDisplay_desktop_feature_div, #apex_desktop,
   #price_inside_buybox ...). A page-wide hunt for `a-offscreen` found
   $22.98 on B0GYTRYV7P where the DOM correctly found nothing: a price
   from an unrelated block. A wrong price is a wrong ping, so each field
   is located by a literal anchor first and read only from a small window
   after it.

2. NEVER TOUCH THE WHOLE STRING. `html.lower()` and case-insensitive
   regex over 450KB were most of the 130ms first attempt. Anchors are
   found with str.find (plain C scan), and only the few-KB window is
   lowered or regexed.

Validated field-for-field against _parse_dom by validate_fastparse.py.
Returns None whenever it cannot decide, and the DOM then answers.
"""
import re
from datetime import datetime
from typing import Any, Dict, Optional

# Price containers, in the same order as PRICE_SELECTOR_UNION in
# monitor.py, so the first hit is the one the DOM would have chosen.
_PRICE_ANCHORS = (
    'id="apex-pricetopay-accessibility-label"',
    'id="corePriceDisplay_desktop_feature_div"',
    'id="corePrice_feature_div"',
    'id="apex_desktop"',
    'id="price_inside_buybox"',
    'id="tp_price_block_total_price_ww"',
    'id="sns-base-price"',
    'id="newAccordionRow"',
    'id="buyNewSection"',
    'class="reinventPricePriceToPayMargin',
    'class="offer-price',
)
_PRICE_WINDOW = 8000

_RE_OFFSCREEN = re.compile(r'class="a-offscreen"\s*>\s*(\$[\d,]+\.\d{2})')
_RE_DOLLARS = re.compile(r'(\$[\d,]+\.\d{2})')
_RE_TAGS = re.compile(r"<[^>]+>")
_RE_TITLE_TEXT = re.compile(r'>\s*([^<]{1,400})')

_OOS_PHRASES = ("currently unavailable", "temporarily out of stock",
                "out of stock")
_OOS_BACK = "we don't know when or if this item will be back"

_BTN_LITERALS = ('id="add-to-cart-button"', 'name="submit.add-to-cart"',
                 'name="submit.addToCart"', 'id="buy-now-button"',
                 'name="submit.buy-now"', 'name="submit.buyNow"')
# Amazon's "See All Buying Options" box. It renders inside #buybox in
# place of the cart and buy-now buttons when no offer holds the buy box.
_NO_BUY_BOX = 'id="unqualifiedBuyBox_feature_div"'

_PRE_LITERALS = ('id="preorder-button"', 'id="placePreOrderButton"',
                 'name="submit.preorder"', 'name="submit.preorder-update"',
                 'name="submit.pre-order"')

# Words that are labels, never a merchant name.
_SELLER_LABELS = ("sold by", "ships from", "shipper / seller", "shipper",
                  "seller", "sold and shipped by", "dispatches from",
                  "visit the store", "other sellers on amazon")

_SELLER_ANCHORS = (
    'id="sellerProfileTriggerId"',
    'tabular-attribute-name="Sold by"',
    'offer-display-feature-name="desktop-merchant-info"',
    'id="merchant-info"',
)
_SELLER_WINDOW = 2500

_IMAGE_ANCHORS = ('id="landingImage"',)
_RE_IMG_HIRES = re.compile(r'data-old-hires="([^"]+)"')
_RE_IMG_SRC = re.compile(r'\bsrc="(https://m\.media-amazon\.com/images/[^"]+)"')
_RE_JSON_HIRES = re.compile(
    r'"hiRes"\s*:\s*"(https://m\.media-amazon\.com/images/I/[^"]+)"')
_RE_JSON_LARGE = re.compile(
    r'"large"\s*:\s*"(https://m\.media-amazon\.com/images/I/[^"]+)"')

_ENT = (("&amp;", "&"), ("&#39;", "'"), ("&apos;", "'"), ("&quot;", '"'),
        ("&nbsp;", " "), ("&lt;", "<"), ("&gt;", ">"), ("&reg;", ""),
        ("&trade;", ""))


def _clean(s: str) -> str:
    s = _RE_TAGS.sub(" ", s or "")
    # A window sliced mid-tag leaves an unterminated "<div id=..." that the
    # tag pattern cannot match. Everything from a lone '<' on is markup.
    cut = s.find("<")
    if cut >= 0:
        s = s[:cut]
    for a, b in _ENT:
        s = s.replace(a, b)
    return " ".join(s.split()).strip()


def extract_price_number(price: str) -> Optional[float]:
    if not price:
        return None
    m = _RE_DOLLARS.search(price)
    if not m:
        return None
    try:
        return float(m.group(1).lstrip("$").replace(",", ""))
    except ValueError:
        return None


def _window(html: str, anchor: str, size: int, start: int = 0) -> Optional[str]:
    i = html.find(anchor, start)
    if i < 0:
        return None
    return html[i:i + size]


def _tag_of(html: str, anchor: str, cap: int = 9000) -> Optional[str]:
    """The anchor's own tag, from the anchor to the closing '>'.

    Needed because landingImage carries a multi-KB data-a-dynamic-image
    blob before its real src, so a fixed-size window truncated the tag and
    picked up a URL from the blob instead of the one the DOM reads.
    """
    i = html.find(anchor)
    if i < 0:
        return None
    end = html.find(">", i)
    if end < 0 or end - i > cap:
        end = min(i + cap, len(html))
    return html[i:end + 1]


def _after_tag(html: str, anchor: str, size: int,
               start: int = 0) -> Optional[str]:
    """Text following the anchor's tag, i.e. that element's content."""
    i = html.find(anchor, start)
    if i < 0:
        return None
    end = html.find(">", i)
    if end < 0:
        return None
    return html[end + 1:end + 1 + size]


def _price(html: str) -> str:
    """First price from the same containers the DOM reads, in DOM order."""
    for anchor in _PRICE_ANCHORS:
        win = _window(html, anchor, _PRICE_WINDOW)
        if win is None:
            continue
        m = _RE_OFFSCREEN.search(win)
        if m:
            return m.group(1)
        # #apex-pricetopay-accessibility-label and #price_inside_buybox
        # carry the price as plain text rather than an a-offscreen span.
        m = _RE_DOLLARS.search(win)
        if m:
            return m.group(1)
    return ""


def _title(html: str) -> str:
    win = _window(html, 'id="productTitle"', 1200)
    if win:
        m = _RE_TITLE_TEXT.search(win[len('id="productTitle"'):])
        if m:
            got = _clean(m.group(1))
            if got:
                return got
    win = _window(html, '<meta name="title"', 600)
    if win:
        m = re.search(r'content="([^"]{1,400})"', win)
        if m:
            return _clean(m.group(1)).replace(" : Amazon.ca", "")
    return ""


def _availability_text(html: str) -> str:
    win = _window(html, 'id="availability"', 900)
    return _clean(win).lower() if win else ""


def _seller(html: str) -> str:
    """Merchant name from the same structured spots the DOM reads.

    The offer-display block appears TWICE with the same attribute: the
    first carries role="heading" and the literal text "Shipper / Seller",
    the second (class="offer-display-feature-text") carries the name.
    Reading the first is what produced "Shipper / Seller" as a merchant.
    """
    # 1. the seller-profile link: its text IS the name
    got = _first_name(_after_tag(html, 'id="sellerProfileTriggerId"', 300))
    if got:
        return got

    # 2. tabular buy box "Sold by" row
    got = _first_name(_after_tag(html, 'tabular-attribute-name="Sold by"', 1400))
    if got:
        return got

    # 3. offer-display: skip the heading block, read the text block
    anchor = 'offer-display-feature-name="desktop-merchant-info"'
    pos = 0
    for _ in range(6):
        i = html.find(anchor, pos)
        if i < 0:
            break
        pos = i + 1
        head = html[max(0, i - 200):i + 260]
        if 'role="heading"' in head:
            continue          # the label block, not the value
        got = _first_name(_after_tag(html, anchor, 1400, start=i))
        if got:
            return got

    # 4. classic #merchant-info sentence
    win = _after_tag(html, 'id="merchant-info"', 1400)
    if win:
        flat = _clean(win)
        m = re.search(r"sold by\s*:?\s*([^.|]{2,80})", flat, re.I)
        if m:
            got = _strip_label(_clean_text(m.group(1)))
            if got:
                return got
        got = _first_name(win)
        if got:
            return got
    return ""


_RE_TEXT_NODE = re.compile(r">([^<>]{2,80})<")


def _first_name(win: Optional[str]) -> str:
    """First real text node in the window, label-stripped.

    Cleaning the entire window concatenated every element inside it, so a
    single merchant read came back as "Amazon.ca Amazon.ca Shipper / S".
    One element's text is what is wanted, so walk text nodes in order and
    take the first that is not a label.
    """
    if not win:
        return ""
    for m in _RE_TEXT_NODE.finditer(">" + win):
        got = _strip_label(_clean_text(m.group(1)))
        if got:
            return got
    return ""


def _clean_text(s: str) -> str:
    """Entity-decode a text node. No tag stripping: there are no tags in
    a text node, and _clean's cut-at-'<' would empty it."""
    for a, b in _ENT:
        s = s.replace(a, b)
    return " ".join(s.split()).strip()


def _strip_label(text: str) -> str:
    """Drop a leading label and return the name, or '' if only a label."""
    if not text:
        return ""
    low = text.lower()
    for lab in _SELLER_LABELS:
        if low.startswith(lab):
            text = text[len(lab):].lstrip(" :-").strip()
            low = text.lower()
            break
    text = text.split("  ")[0].strip(" :.|")
    if not text or len(text) > 80:
        return ""
    if text.lower() in _SELLER_LABELS:
        return ""
    # Markup leaking through is a bug, not a merchant name.
    if any(ch in text for ch in "<>=\"") or "data-" in text:
        return ""
    return text


def _image(html: str) -> str:
    """Same precedence as the DOM: data-old-hires, then src, then the
    JSON blobs. Read from the landingImage TAG, not a fixed window, so
    the data-a-dynamic-image blob cannot supply the answer instead."""
    tag = _tag_of(html, 'id="landingImage"')
    if tag:
        m = _RE_IMG_HIRES.search(tag)
        if m:
            return m.group(1)
        m = _RE_IMG_SRC.search(tag)
        if m:
            return m.group(1)
    m = _RE_JSON_HIRES.search(html) or _RE_JSON_LARGE.search(html)
    return m.group(1) if m else ""


def fast_parse(asin: str, html: str, proxy_label: str,
               url: str) -> Optional[Dict[str, Any]]:
    """Product page fields by regex, or None when the DOM should decide.

    Detection rules are identical to _parse_dom:
      in stock      = a real buy control AND a parseable price
      out of stock  = #outOfStock, an explicit phrase in #availability, or
                      the "don't know when or if" sentence
      anything else = Unknown
    """
    avail = _availability_text(html)
    definitive_oos = (
        'id="outOfStock"' in html
        or any(p in avail for p in _OOS_PHRASES)
    )
    if not definitive_oos:
        # Scope the expensive phrase check to where it can appear.
        win = _window(html, 'id="availability"', 4000) or ""
        if _OOS_BACK in win.lower():
            definitive_oos = True

    price = _price(html)
    has_btn = any(b in html for b in _BTN_LITERALS)
    has_pre = any(b in html for b in _PRE_LITERALS)
    if not has_pre:
        win = _window(html, 'id="buybox', 20000) or ""
        has_pre = "pre-order now" in win.lower() or "Pre-order Now" in win
    has_buyable_button = has_btn or has_pre

    if definitive_oos:
        stock, price = "Out of stock", ""
    elif has_buyable_button and price:
        stock = "Pre-order" if has_pre and not has_btn else "In stock"
    elif not has_buyable_button and _NO_BUY_BOX in html:
        # NO BUY BUTTON IS PROOF OF OUT OF STOCK. This page was
        # "Unknown", which the state machine ignores, so three hours of
        # it never re-armed B0H783FY5Z and its 2:51 AM drop on
        # 2026-10-02 went unannounced. Nothing here can be bought from
        # the page, which is the owner's own definition of gone.
        stock, price = "Out of stock (no buy button)", ""
    else:
        stock = "Unknown"

    title = _title(html)

    # Nothing recognisable: let the DOM try rather than guess.
    if stock == "Unknown" and not title and not price:
        return None

    return {
        "asin": asin,
        "title": title,
        "price": price,
        "price_number": extract_price_number(price),
        "stock": stock,
        "url": url,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": proxy_label,
        "image_url": _image(html),
        "seller": _seller(html),
        "buyable": bool(has_buyable_button and price),
    }

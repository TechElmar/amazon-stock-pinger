"""Read Amazon's offer list (the "See All Buying Options" panel).

WHY, 2026-10-03: the product page only shows the ONE offer holding the
buy box. When a reseller holds it, or nobody does ("See All Buying
Options"), Amazon's own offer is only visible in this list. That hid four
drops in one day that a competitor caught: the Mini Tin at $18.99 twice
behind a $129.99 reseller, and the Booster Bundle twice behind "See All
Buying Options". The list is also 15KB on the wire against ~115KB for a
product page read, and it stayed loadable (11 of 12) while product pages
on the same IPs were 12 of 12 captcha.

THE RULE (owner, 2026-10-03): any new-condition offer at or below the
target with an Add to Cart button means in stock at that price. No such
offer means sold out.

Every offer that can be added to the cart carries the seller and price in
its button label: aria-label="Add to Cart from seller X and price $Y".
That label is the whole signal; it only exists where the button does.
"""
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

URL = ("https://www.amazon.ca/gp/product/ajax/aodAjaxMain/ref=dp_aod_ALL_mbc"
       "?asin={asin}&pc=dp&experienceId=aodAjaxMain")

_RE_ATC = re.compile(
    r'aria-label="Add to Cart from seller (.+?) and price \$([\d,]+\.\d\d)\s*"')
_RE_COND = re.compile(r'id="aod-offer-heading".{0,400}?a-text-bold">\s*([^<]+?)\s*<', re.S)
_RE_TEXT = re.compile(r">\s*([^<>]{2,300}?)\s*<")
_RE_IMG = re.compile(r'id="pinned-image-id".{0,800}?src="(https://m\.media-amazon\.com/images/[^"]+)"', re.S)
_ENT = (("&amp;", "&"), ("&#39;", "'"), ("&quot;", '"'), ("&nbsp;", " "))


def _unent(s: str) -> str:
    for a, b in _ENT:
        s = s.replace(a, b)
    return " ".join(s.split()).strip()


def offers(html: str) -> Optional[List[Dict[str, Any]]]:
    """Every offer block, pinned first. None when this is not an offer list."""
    if 'id="aod-pinned-offer"' not in html and 'id="aod-container"' not in html:
        return None
    starts = [m.start() for m in re.finditer(r'id="aod-(?:pinned-offer|offer)"', html)]
    out = []
    for k, s in enumerate(starts):
        blk = html[s:starts[k + 1] if k + 1 < len(starts) else len(html)]
        atc = _RE_ATC.search(blk)
        cond = _RE_COND.search(blk)
        out.append({
            "pinned": blk.startswith('id="aod-pinned-offer"'),
            "seller": _unent(atc.group(1)) if atc else "",
            "price_number": float(atc.group(2).replace(",", "")) if atc else None,
            "atc": bool(atc),
            # A block with no heading is taken as new: the list defaults to
            # new offers and every block measured had one.
            "condition": _unent(cond.group(1)) if cond else "",
            "new": (not cond) or _unent(cond.group(1)).lower().startswith("new"),
        })
    return out


def title(html: str) -> str:
    i = html.find('id="aod-asin-title-text"')
    if i < 0:
        return ""
    m = _RE_TEXT.search(html, html.find(">", i))
    return _unent(m.group(1)) if m else ""


def image(html: str) -> str:
    m = _RE_IMG.search(html)
    return m.group(1) if m else ""


def verdict(asin: str, html: str, target: float, source: str) -> Optional[Dict[str, Any]]:
    """A result dict the monitor understands, or None if html is not an
    offer list (blocked, captcha, error page)."""
    offs = offers(html)
    if offs is None:
        return None
    buyable = [o for o in offs if o["atc"] and o["new"] and o["price_number"] is not None]
    hits = [o for o in buyable if target and o["price_number"] <= target]
    best = min(hits, key=lambda o: o["price_number"]) if hits else None
    lowest = min(buyable, key=lambda o: o["price_number"]) if buyable else None
    res = {
        "asin": asin,
        "title": title(html),
        "url": "https://www.amazon.ca/dp/" + asin,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
        "image_url": image(html),
        "aod_offers": len(buyable),
        "aod_lowest": ("$%.2f %s" % (lowest["price_number"], lowest["seller"])) if lowest else "",
    }
    if best:
        res.update(stock="In stock", price="$%.2f" % best["price_number"],
                   price_number=best["price_number"], seller=best["seller"],
                   buyable=True)
    else:
        res.update(stock="Out of stock (no offer at target)", price="",
                   price_number=None, seller="", buyable=False)
    return res

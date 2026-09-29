"""Prove fast_parse agrees with the real DOM parser, on real pages.

Any disagreement on a field that drives a ping (stock, price_number,
buyable) is a hard failure. Title and image differences are reported but
tolerated: the ping already falls back to the stored title, and the image
is cosmetic. Seller differences are reported because seller no longer
gates a ping, but a systematic difference would still be worth knowing.
"""
import asyncio
import sys
import time

sys.path.insert(0, "/opt/stock-pinger")
import monitor
import fastparse

H = {"Accept-Language": "en-CA,en;q=0.9,en-US;q=0.8"}
# A spread: in stock cheap, in stock expensive, out of stock, reseller-held,
# and the two that have actually been dropping.
ASINS = ["B0D7MB41BD", "B0GG11K25W", "B0H78BB9TY", "B0H783FY5Z",
         "B0H7FDBNSB", "B0GRCDKMSW", "B0H7818RCM", "B0GYTRYV7P"]
CAP = 450 * 1024

HARD = ("stock", "price_number", "buyable", "seller")
SOFT = ("title", "image_url")

fails = []
soft_diffs = []


async def grab(asin, px):
    async with monitor.AsyncSession(
            impersonate=monitor.STATIC_IMPERSONATE_PROFILE,
            max_clients=1) as s:
        r = await s.get(f"https://www.amazon.ca/dp/{asin}", headers=H,
                        proxy=px, timeout=25, stream=True)
        buf = bytearray()
        async for c in r.aiter_content():
            buf.extend(c)
            if len(buf) >= CAP:
                break
        await r.aclose()
    return bytes(buf).decode("utf-8", "replace")


async def main():
    rot = monitor.load_rotating_proxies()
    px = rot[0] if rot else None
    ck = monitor.HTTPAmazonChecker("https://www.amazon.ca", asyncio.Semaphore(4))
    t_dom = t_fast = 0.0
    n = 0

    for asin in ASINS:
        try:
            html = await grab(asin, px)
        except Exception as e:
            print(f"{asin}: fetch failed ({type(e).__name__}), skipped")
            continue
        if monitor.is_shell_page(html):
            print(f"{asin}: shell page, skipped (not a parser case)")
            continue

        t0 = time.perf_counter()
        dom = ck.parse_html(asin, html, "probe")
        t_dom += time.perf_counter() - t0

        t0 = time.perf_counter()
        fast = fastparse.fast_parse(asin, html, "probe",
                                    ck.product_url(asin))
        t_fast += time.perf_counter() - t0
        n += 1

        if fast is None:
            print(f"{asin}: fast_parse declined (falls back to DOM) "
                  f"- dom said stock={dom.get('stock')!r}")
            if dom.get("stock") in ("In stock", "Pre-order", "Out of stock"):
                fails.append(f"{asin}: declined but DOM decided "
                             f"{dom.get('stock')!r}")
            continue

        print(f"\n{asin}  {len(html)/1024:.0f}KB")
        for k in HARD:
            a, b = dom.get(k), fast.get(k)
            flag = "OK " if a == b else "MISMATCH"
            print(f"   [{flag}] {k:<13} dom={a!r:<14} fast={b!r}")
            if a != b:
                fails.append(f"{asin}.{k}: dom={a!r} fast={b!r}")
        for k in SOFT:
            a, b = (dom.get(k) or ""), (fast.get(k) or "")
            if a != b:
                soft_diffs.append(f"{asin}.{k}: dom={a[:40]!r} fast={b[:40]!r}")
                print(f"   [soft] {k:<13} dom={a[:34]!r} fast={b[:34]!r}")

    print("\n" + "=" * 66)
    if n:
        print(f"CPU per page: DOM {1000*t_dom/n:.1f}ms  "
              f"fast {1000*t_fast/n:.1f}ms  "
              f"({t_dom/max(t_fast,1e-9):.0f}x cheaper)")
    print(f"pages compared: {n}")
    if soft_diffs:
        print(f"\nsoft differences ({len(soft_diffs)}), tolerated:")
        for d in soft_diffs:
            print("   " + d)
    if fails:
        print(f"\nHARD FAILURES ({len(fails)}):")
        for f in fails:
            print("   " + f)
        sys.exit(1)
    print("\nALL HARD FIELDS AGREE")

asyncio.run(main())

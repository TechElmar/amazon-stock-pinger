"""Passive Pokemon Center CA queue watcher. LOGGING ONLY: no pings.

Pokemon Center sits behind DataDome + Imperva; every page answers us
with a 403 JavaScript challenge. This never tries to pass it. It records
what the site's front door sends back, so that when a real queue goes up
we can see which signal moves, the same kind of side signal competitor
bots call a "Potential queue detected".

Every POLL_SECONDS, through one IPRoyal US line (proxyon and the Decodo
Canadian lines refuse this site):
  home    https://www.pokemoncenter.com/en-ca   (no redirects followed)
  robots  https://www.pokemoncenter.com/robots.txt
recording status, size, timing, redirect target, cookie names, edge
headers, a body signature with the random script path removed, and any
queue-like words. DNS for www.pokemoncenter.com every DNS_SECONDS.

One JSON object per line in LOG. Lines with "change" mark where a URL's
signature, status or size class differs from its previous poll.
Credentials never reach the log, only the proxy's IP.
"""
import asyncio
import hashlib
import json
import os
import re
import socket
import time
from datetime import datetime

from curl_cffi.requests import AsyncSession

POOL_FILE = "/opt/stock-pinger/proxies_offers.txt"
LOG = "/var/log/pc-queue-watch.log"
POLL_SECONDS = 20
DNS_SECONDS = 300
URLS = {
    "home": "https://www.pokemoncenter.com/en-ca",
    "robots": "https://www.pokemoncenter.com/robots.txt",
}
WORDS = ("queue", "waiting room", "in line", "high traffic", "virtual", "wait time", "your turn")
EDGE = ("server", "x-cache", "x-cdn", "x-datadome", "x-iinfo", "location", "retry-after", "cache-control")


def us_lines():
    out = []
    for line in open(POOL_FILE, encoding="utf-8"):
        line = line.strip()
        if line.startswith(("81.90.", "89.47.")):
            h, p, u, pw = line.split(":")
            out.append((h, "http://%s:%s@%s:%s" % (u, pw, h, p)))
    return out


_RE_DD = re.compile(r"var dd=\{(.*?)\}", re.S)


def dd_info(text):
    """DataDome's challenge object, minus its per-request tokens (cid, e,
    cookie). rt is the challenge type: 'i' invisible device check, 'c'
    visible puzzle, 'b' hard block."""
    m = _RE_DD.search(text)
    if not m:
        return {}
    pairs = dict(re.findall(r"'(\w+)':'?([^,']*)'?", m.group(1)))
    return {k: pairs.get(k, "") for k in ("rt", "hsh", "b", "s", "host")}


def signature(text):
    dd = dd_info(text)
    if dd:
        t = "dd|" + "|".join(dd[k] for k in ("rt", "hsh", "b", "s", "host"))
    else:
        t = re.sub(r'src="/[^"]*"', 'src=""', text)
        t = re.sub(r"[A-Za-z0-9+/=~_-]{24,}", "X", t)
        t = re.sub(r"\s+", " ", t)[:4000]
    return hashlib.sha1(t.encode("utf-8", "replace")).hexdigest()[:10]


def size_class(n):
    return "<1K" if n < 1024 else "<4K" if n < 4096 else "<32K" if n < 32768 else ">=32K"


async def probe(name, url, ip, proxy):
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "url": name, "ip": ip}
    t0 = time.monotonic()
    try:
        async with AsyncSession(impersonate="chrome146", proxy=proxy, timeout=20) as s:
            r = await s.get(url, headers={"Accept-Language": "en-CA,en;q=0.9"},
                            allow_redirects=False)
            names = sorted(s.cookies.keys())
        text = r.content[:300000].decode("utf-8", "replace")
        title = re.search(r"<title[^>]*>([^<]{0,120})", text, re.I)
        low = text.lower()
        rec.update(
            status=r.status_code,
            bytes=len(r.content),
            ms=int((time.monotonic() - t0) * 1000),
            sig=signature(text),
            dd=dd_info(text).get("rt", ""),
            title=title.group(1).strip() if title else "",
            cookies=names,
            edge={k: r.headers.get(k, "")[:120] for k in EDGE if r.headers.get(k)},
            words=[w for w in WORDS if w in low],
        )
    except Exception as e:
        rec.update(error="%s: %s" % (type(e).__name__, str(e)[:120]),
                   ms=int((time.monotonic() - t0) * 1000))
    return rec


def dns_record():
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "url": "dns"}
    try:
        host, aliases, ips = socket.gethostbyname_ex("www.pokemoncenter.com")
        rec.update(host=host, aliases=aliases, ips=sorted(ips))
    except Exception as e:
        rec.update(error=str(e)[:120])
    return rec


async def main():
    lines = us_lines()
    if not lines:
        raise SystemExit("no IPRoyal US lines in " + POOL_FILE)
    prev = {}
    k = 0
    next_dns = 0.0
    once = os.environ.get("ONCE") == "1"
    while True:
        started = time.monotonic()
        ip, proxy = lines[k % len(lines)]
        k += 1
        recs = await asyncio.gather(*(probe(n, u, ip, proxy) for n, u in URLS.items()))
        if time.monotonic() >= next_dns:
            recs.append(dns_record())
            next_dns = time.monotonic() + DNS_SECONDS
        with open(LOG, "a", encoding="utf-8") as f:
            for rec in recs:
                key = (rec.get("status"), rec.get("sig"), size_class(rec.get("bytes", 0)),
                       tuple(rec.get("ips", [])), bool(rec.get("error")))
                if rec["url"] in prev and prev[rec["url"]] != key:
                    rec["change"] = True
                prev[rec["url"]] = key
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                if once:
                    print(json.dumps(rec, ensure_ascii=False))
        if once:
            return
        await asyncio.sleep(max(1.0, POLL_SECONDS - (time.monotonic() - started)))

asyncio.run(main())

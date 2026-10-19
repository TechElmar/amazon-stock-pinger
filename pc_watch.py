"""Pokemon Center CA queue watcher: passive signals, "Potential queue" ping.

Pokemon Center sits behind DataDome + Imperva; every page answers us
with a 403 JavaScript challenge. This never tries to pass it. It records
what the site's front door sends back and pings when that changes the
way it did during a real queue.

THE SIGNAL, learned from the 2026-10-15 queue (a competitor pinged
"Potential queue detected" at 12:39 Toronto). Over six days the home
page and robots.txt answered with the ~850-byte 403 challenge, except:
  12:35:39  200, 212 bytes      never seen before, held 3.5 minutes
  12:39:39  200, ~1,050 bytes   on robots.txt too (site-wide), until ~16:03
plus timeouts while the site strained. Neither small 200 page appeared
at any other time; a ~6KB 200 page overnight 10-13/14 and a 503 on
robots.txt at 03:00 10-14 did, which is why the rule is specific.

RULE: QUEUE_CONFIRM_POLLS polls in a row where home or robots returns
200 with QUEUE_MIN_BYTES..QUEUE_MAX_BYTES -> ping once; re-arm after
REARM_SECONDS with no such response.

Every POLL_SECONDS, through one IPRoyal US line (proxyon and the Decodo
Canadian lines refuse this site): home + robots, recording status, size,
timing, redirect, cookie names, edge headers, DataDome challenge type and
a token-free signature; DNS every DNS_SECONDS. JSON lines in LOG. The
body of any response shape not seen before is saved to SAMPLE_DIR.

Pings go to the webhooks in ALERT_CONFIG (chmod 600, never committed):
{"webhooks": [...], "role_id": "..."}. No file = log only.
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

import aiohttp
from curl_cffi.requests import AsyncSession

POOL_FILE = "/opt/stock-pinger/proxies_offers.txt"
LOG = "/var/log/pc-queue-watch.log"
SAMPLE_DIR = "/opt/stock-pinger/pc_samples"
ALERT_CONFIG = "/opt/stock-pinger/pc_alert.json"
POLL_SECONDS = 10
DNS_SECONDS = 300
QUEUE_MIN_BYTES = 150
QUEUE_MAX_BYTES = 1500
QUEUE_CONFIRM_POLLS = 2
REARM_SECONDS = 30 * 60
STORE_URL = "https://www.pokemoncenter.com/en-ca"
URLS = {
    "home": STORE_URL,
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


def queue_like(rec):
    """A small 200 page from home or robots that is not robots.txt itself
    (the real file is a ~800-byte 200 too, and the 2026-10-13/14 replay
    fired on it twice before this check)."""
    return (rec.get("status") == 200
            and QUEUE_MIN_BYTES <= (rec.get("bytes") or 0) <= QUEUE_MAX_BYTES
            and rec["url"] in ("home", "robots")
            and not rec.get("robots_file"))


async def probe(name, url, ip, proxy):
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "url": name, "ip": ip}
    t0 = time.monotonic()
    text = ""
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
            robots_file=text.lstrip()[:11].lower() == "user-agent:",
        )
    except Exception as e:
        rec.update(error="%s: %s" % (type(e).__name__, str(e)[:120]),
                   ms=int((time.monotonic() - t0) * 1000))
    return rec, text


def save_sample(rec, text, seen):
    """Keep the body of each response shape we have not seen before, so a
    queue page can be read afterwards instead of guessed at."""
    if rec.get("error") or not text:
        return
    normal = (rec.get("status") == 403 and 800 <= rec.get("bytes", 0) <= 900) or \
             (rec["url"] == "robots" and rec.get("status") == 200 and 780 <= rec.get("bytes", 0) <= 820)
    key = (rec["url"], rec.get("status"), rec.get("sig"))
    if normal or key in seen:
        return
    seen.add(key)
    os.makedirs(SAMPLE_DIR, exist_ok=True)
    if len(os.listdir(SAMPLE_DIR)) >= 200:
        return
    name = "%s-%s-%s-%s.html" % (rec["ts"].replace(" ", "_").replace(":", ""), rec["url"],
                                 rec.get("status"), rec.get("bytes"))
    with open(os.path.join(SAMPLE_DIR, name), "w", encoding="utf-8") as f:
        f.write(text[:200000])


def dns_record():
    rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "url": "dns"}
    try:
        host, aliases, ips = socket.gethostbyname_ex("www.pokemoncenter.com")
        rec.update(host=host, aliases=aliases, ips=sorted(ips))
    except Exception as e:
        rec.update(error=str(e)[:120])
    return rec


def alert_payload(role_id, test=False):
    mention = "<@&%s> " % role_id if role_id else ""
    text = ("%s🚨 Potential queue detected!\nStart navigating Pokemon Center CA now." % mention)
    if test:
        text += "\n(TEST, ignore)"
    payload = {
        "content": text,
        "components": [{"type": 1, "components": [{
            "type": 2, "style": 5, "label": "Pokemon Center CA", "url": STORE_URL}]}],
        "allowed_mentions": {"roles": [role_id] if role_id else []},
    }
    return payload


async def send_alert(test=False):
    """POST the ping to every configured webhook. Returns a list of
    (webhook index, status). Plain text first so phone notifications
    show it; a link button under it (with_components=true)."""
    try:
        cfg = json.load(open(ALERT_CONFIG, encoding="utf-8"))
    except Exception:
        return [("no config", None)]
    payload = alert_payload(cfg.get("role_id", ""), test=test)
    out = []
    async with aiohttp.ClientSession() as s:
        for i, hook in enumerate(cfg.get("webhooks", [])):
            url = hook + ("&" if "?" in hook else "?") + "with_components=true"
            try:
                async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    out.append((i, r.status))
            except Exception as e:
                out.append((i, type(e).__name__))
    return out


def log_line(obj):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


async def main():
    if os.environ.get("TEST_ALERT") == "1":
        print("test alert:", await send_alert(test=True))
        return
    lines = us_lines()
    if not lines:
        raise SystemExit("no IPRoyal US lines in " + POOL_FILE)
    prev, seen = {}, set()
    k = 0
    next_dns = 0.0
    streak = 0
    armed = True
    last_queue = 0.0
    once = os.environ.get("ONCE") == "1"
    while True:
        started = time.monotonic()
        ip, proxy = lines[k % len(lines)]
        k += 1
        results = await asyncio.gather(*(probe(n, u, ip, proxy) for n, u in URLS.items()))
        recs = [r for r, _ in results]
        for rec, text in results:
            save_sample(rec, text, seen)
        if time.monotonic() >= next_dns:
            recs.append(dns_record())
            next_dns = time.monotonic() + DNS_SECONDS
        for rec in recs:
            key = (rec.get("status"), rec.get("sig"), size_class(rec.get("bytes", 0)),
                   tuple(rec.get("ips", [])), bool(rec.get("error")))
            if rec["url"] in prev and prev[rec["url"]] != key:
                rec["change"] = True
            prev[rec["url"]] = key
            log_line(rec)
            if once:
                print(json.dumps(rec, ensure_ascii=False))

        # Queue detector.
        now = time.monotonic()
        pages = [r for r in recs if r["url"] in ("home", "robots")]
        if any(queue_like(r) for r in pages):
            streak += 1
            last_queue = now
        elif all(r.get("error") for r in pages):
            pass        # timeouts are common mid-queue: neither proof nor disproof
        else:
            streak = 0
        if not armed and now - last_queue > REARM_SECONDS:
            armed = True
            log_line({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "url": "detector",
                      "event": "re-armed"})
        if armed and streak >= QUEUE_CONFIRM_POLLS:
            armed = False
            sent = await send_alert()
            log_line({"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "url": "detector",
                      "event": "POTENTIAL QUEUE", "sent": sent})

        if once:
            return
        await asyncio.sleep(max(1.0, POLL_SECONDS - (time.monotonic() - started)))

asyncio.run(main())

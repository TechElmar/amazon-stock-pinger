"""Pokemon Center CA queue watcher: passive signals, "Potential queue" ping.

Pokemon Center sits behind DataDome + Imperva; every page answers us
with a 403 JavaScript challenge. This never tries to pass it. It records
what the site's front door sends back and pings when that changes the
way it does when a queue goes up.

THE SIGNAL. Normally home and robots.txt return the ~850-byte 403
challenge. When a queue starts, Imperva escalates: first a 200 of 212
bytes (an "_Incapsula_Resource" JS challenge), then a ~1,050-byte 200
("Request unsuccessful. Incapsula incident ID"), site-wide, plus
timeouts while the site strains. Neither page appeared at any other time.
  2026-10-15  first sign 12:35:39, competitor ping 12:39
  2026-10-21  fired 11:04:51
  2026-10-27  fired 11:50:09, competitors 11:52
A ~6KB 200 page overnight 10-13/14, a 503 on robots.txt at 03:00 10-14
and the real robots.txt (a ~800-byte 200) did not fire, on purpose.

RULE: QUEUE_CONFIRM_POLLS polls in a row where home or robots returns a
200 of QUEUE_MIN_BYTES..QUEUE_MAX_BYTES that is not robots.txt itself
-> ping once; polls where every request errored neither count nor
reset; re-arm after REARM_SECONDS with no such response.

BLIND: if every request fails for BLIND_SECONDS (proxies lapsed, line
changed), the ops targets get one notice, and another on recovery, so
the watcher can never go quiet without anyone knowing.

Proxies: lines from PC_POOL_FILE if it exists, else the IPRoyal lines in
proxies_offers.txt (proxyon and the Decodo lines refuse this site).
Re-read every POOL_RELOAD_SECONDS, so a renewal needs no restart.

Targets come from ALERT_CONFIG (chmod 600, never committed):
  {"targets": [{"name": "...", "webhook": "...", "role_id": "...",
                "alerts": true, "ops": false}, ...]}
Each target pings its OWN server's role. The older
{"webhooks": [...], "role_id": "..."} shape is still read.

One JSON object per line in LOG; bodies of new response shapes go to
SAMPLE_DIR. Credentials never reach the log, only the proxy's IP.
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

OFFER_POOL_FILE = "/opt/stock-pinger/proxies_offers.txt"
PC_POOL_FILE = "/opt/stock-pinger/proxies_pc.txt"
LOG = "/var/log/pc-queue-watch.log"
SAMPLE_DIR = "/opt/stock-pinger/pc_samples"
ALERT_CONFIG = "/opt/stock-pinger/pc_alert.json"
POLL_SECONDS = 10
DNS_SECONDS = 300
POOL_RELOAD_SECONDS = 600
QUEUE_MIN_BYTES = 150
QUEUE_MAX_BYTES = 1500
QUEUE_CONFIRM_POLLS = 2
REARM_SECONDS = 30 * 60
BLIND_SECONDS = 10 * 60
STORE_URL = "https://www.pokemoncenter.com/en-ca"
CREDIT = "Bot developed by @elmar"
URLS = {
    "home": STORE_URL,
    "robots": "https://www.pokemoncenter.com/robots.txt",
}
WORDS = ("queue", "waiting room", "in line", "high traffic", "virtual", "wait time", "your turn")
EDGE = ("server", "x-cache", "x-cdn", "x-datadome", "x-iinfo", "location", "retry-after", "cache-control")


def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def pool_lines():
    """(ip, proxy url) pairs. PC_POOL_FILE wins when present; otherwise the
    IPRoyal lines (81.90.x, 89.47.x) from the offer pool."""
    out = []
    if os.path.exists(PC_POOL_FILE):
        src, keep = PC_POOL_FILE, (lambda l: True)
    else:
        src, keep = OFFER_POOL_FILE, (lambda l: l.startswith(("81.90.", "89.47.")))
    try:
        for line in open(src, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#") or not keep(line):
                continue
            parts = line.split(":")
            if len(parts) == 4:
                h, p, u, pw = parts
                out.append((h, "http://%s:%s@%s:%s" % (u, pw, h, p)))
    except OSError:
        pass
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
    """A small 200 page from home or robots that is not robots.txt itself."""
    return (rec.get("status") == 200
            and QUEUE_MIN_BYTES <= (rec.get("bytes") or 0) <= QUEUE_MAX_BYTES
            and rec["url"] in ("home", "robots")
            and not rec.get("robots_file"))


async def probe(name, url, ip, proxy):
    rec = {"ts": now_ts(), "url": name, "ip": ip}
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
    """Keep the body of each response shape not seen before."""
    if rec.get("error") or not text:
        return
    normal = (rec.get("status") == 403 and 800 <= rec.get("bytes", 0) <= 900) or rec.get("robots_file")
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
    rec = {"ts": now_ts(), "url": "dns"}
    try:
        host, aliases, ips = socket.gethostbyname_ex("www.pokemoncenter.com")
        rec.update(host=host, aliases=aliases, ips=sorted(ips))
    except Exception as e:
        rec.update(error=str(e)[:120])
    return rec


def load_targets():
    """Targets from ALERT_CONFIG; [] when there is no usable config."""
    try:
        cfg = json.load(open(ALERT_CONFIG, encoding="utf-8"))
    except Exception:
        return []
    if "targets" in cfg:
        out = []
        for t in cfg["targets"]:
            if t.get("webhook"):
                out.append({"name": t.get("name", "?"), "webhook": t["webhook"],
                            "role_id": str(t.get("role_id") or ""),
                            "alerts": t.get("alerts", True), "ops": t.get("ops", False)})
        return out
    # older shape: one role for every webhook, ops notices to all of them
    return [{"name": "webhook%d" % i, "webhook": h, "role_id": str(cfg.get("role_id") or ""),
             "alerts": True, "ops": True} for i, h in enumerate(cfg.get("webhooks", []))]


def queue_payload(role_id, test=False):
    """Plain text first (phone notifications show only that), the target
    server's OWN role, and a link button underneath."""
    mention = "<@&%s> " % role_id if role_id else ""
    text = "%s🚨 Potential queue detected!\nStart navigating Pokemon Center CA now." % mention
    if test:
        text += "\n(TEST, ignore)"
    text += "\n-# %s" % CREDIT
    return {
        "content": text,
        "components": [{"type": 1, "components": [{
            "type": 2, "style": 5, "label": "Pokemon Center CA", "url": STORE_URL}]}],
        "allowed_mentions": {"roles": [role_id] if role_id else []},
    }


def ops_payload(text):
    """Watcher health notice: no mentions at all."""
    return {"content": text, "allowed_mentions": {"parse": []}}


async def post(targets, make_payload):
    out = []
    async with aiohttp.ClientSession() as s:
        for t in targets:
            url = t["webhook"] + ("&" if "?" in t["webhook"] else "?") + "with_components=true"
            try:
                async with s.post(url, json=make_payload(t),
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    out.append((t["name"], r.status))
            except Exception as e:
                out.append((t["name"], type(e).__name__))
    return out


async def send_queue_alert(test=False, only=None):
    targets = [t for t in load_targets() if t["alerts"] and (only is None or t["name"] == only)]
    if not targets:
        return [("no targets", None)]
    return await post(targets, lambda t: queue_payload(t["role_id"], test=test))


async def send_ops(text):
    targets = [t for t in load_targets() if t["ops"]]
    if not targets:
        return [("no ops targets", None)]
    return await post(targets, lambda t: ops_payload(text))


def log_line(obj):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


async def main():
    if os.environ.get("TEST_ALERT"):
        # TEST_ALERT=<target name>: one test ping to that target only.
        print("test alert:", await send_queue_alert(test=True, only=os.environ["TEST_ALERT"]))
        return
    lines = pool_lines()
    next_pool = time.monotonic() + POOL_RELOAD_SECONDS
    prev, seen = {}, set()
    k = 0
    next_dns = 0.0
    streak = 0
    armed = True
    last_queue = 0.0
    last_ok = time.monotonic()
    blind_noticed = False
    once = os.environ.get("ONCE") == "1"
    while True:
        started = time.monotonic()
        if not lines or started >= next_pool:
            lines = pool_lines() or lines
            next_pool = started + POOL_RELOAD_SECONDS
        if not lines:
            log_line({"ts": now_ts(), "url": "detector", "event": "no proxy lines"})
            if not blind_noticed and started - last_ok >= BLIND_SECONDS:
                blind_noticed = True
                await send_ops("⚠️ Pokemon Center watcher has no proxy lines to use. "
                               "Queue pings are paused until it does.")
            await asyncio.sleep(60)
            continue
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

        now = time.monotonic()
        pages = [r for r in recs if r["url"] in ("home", "robots")]

        # Blind check: any answer at all from the site counts as sight.
        if any(r.get("status") for r in pages):
            last_ok = now
            if blind_noticed:
                blind_noticed = False
                sent = await send_ops("✅ Pokemon Center watcher can see the site again. "
                                      "Queue pings are back on.")
                log_line({"ts": now_ts(), "url": "detector", "event": "recovered", "sent": sent})
        elif not blind_noticed and now - last_ok >= BLIND_SECONDS:
            blind_noticed = True
            sent = await send_ops("⚠️ Pokemon Center watcher is blind: every check has failed for "
                                  "%d minutes (proxy lines?). Queue pings cannot fire until it "
                                  "recovers." % (BLIND_SECONDS // 60))
            log_line({"ts": now_ts(), "url": "detector", "event": "blind", "sent": sent})

        # Queue detector.
        if any(queue_like(r) for r in pages):
            streak += 1
            last_queue = now
        elif all(r.get("error") for r in pages):
            pass        # timeouts are common mid-queue: neither proof nor disproof
        else:
            streak = 0
        if not armed and now - last_queue > REARM_SECONDS:
            armed = True
            log_line({"ts": now_ts(), "url": "detector", "event": "re-armed"})
        if armed and streak >= QUEUE_CONFIRM_POLLS:
            armed = False
            sent = await send_queue_alert()
            log_line({"ts": now_ts(), "url": "detector", "event": "POTENTIAL QUEUE", "sent": sent})

        if once:
            return
        await asyncio.sleep(max(1.0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    asyncio.run(main())

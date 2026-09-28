"""Health check for the stock pinger, oriented around one question:
if a product dropped RIGHT NOW, would it ping?

Checks the whole path, not just whether the process is up:
  detection coverage -> state machine -> ping gate -> Discord delivery
Anything that would silently swallow a ping is called out as a FAIL.
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, "/opt/stock-pinger")
LOG = "/var/log/stock-pinger.log"
HOT_FILE = "/opt/stock-pinger/hot.txt"


def hot_asins():
    """The hot tier, so this script judges the same products the bot does."""
    out = set()
    try:
        with open(HOT_FILE, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tok = line.split()[0].upper()
                if len(tok) == 10 and tok.isalnum():
                    out.add(tok)
    except OSError:
        pass
    return out


HOT = hot_asins()

OK, WARN, FAIL = [], [], []


def ok(m):
    OK.append(m)
    print(f"  [OK]   {m}")


def warn(m):
    WARN.append(m)
    print(f"  [WARN] {m}")


def fail(m):
    FAIL.append(m)
    print(f"  [FAIL] {m}")


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True,
                              text=True, timeout=30).stdout.strip()
    except Exception:
        return ""


def tail(n=60000):
    try:
        with open(LOG, errors="replace") as fh:
            return fh.readlines()[-n:]
    except Exception:
        return []


print("=" * 70)
print(f"STOCK PINGER HEALTH CHECK  {datetime.now():%Y-%m-%d %H:%M:%S}")
print("=" * 70)

# ---------------------------------------------------------------- 1
print("\n1. PROCESS AND HOST")
active = sh("systemctl is-active stock-pinger")
ok(f"service active ({sh('systemctl show -p ActiveEnterTimestamp --value stock-pinger')})") \
    if active == "active" else fail(f"service is {active!r}")

load = os.getloadavg()[0]
cores = os.cpu_count() or 1
(ok if load < cores * 1.5 else warn)(
    f"load {load:.2f} on {cores} core(s)")

mem = sh("free -m | awk '/Mem:/{print $3\" \"$2}'").split()
if len(mem) == 2:
    used, total = int(mem[0]), int(mem[1])
    (ok if used < total * 0.85 else warn)(
        f"memory {used}MB / {total}MB ({used*100//total}%)")

disk = sh("df -h / | awk 'NR==2{print $5\" \"$4}'").split()
if disk:
    pct = int(disk[0].rstrip("%"))
    (ok if pct < 85 else warn)(f"disk {disk[0]} used, {disk[1]} free")

logsz = sh(f"du -m {LOG} 2>/dev/null | cut -f1")
if logsz.isdigit():
    (ok if int(logsz) < 200 else warn)(f"log file {logsz}MB")
rot = sh("ls /etc/logrotate.d/ | grep -c stock")
(ok if rot != "0" else warn)("logrotate configured" if rot != "0"
                             else "NO logrotate rule (log can grow unbounded)")

# ---------------------------------------------------------------- 2
print("")
print("1b. TIERS")
if HOT:
    print(f"         hot.txt lists {len(HOT)} hot ASIN(s). Every")
    print("         freshness number below is measured over those")
    print("         only; cold products are idle on purpose.")
else:
    print("         no hot.txt - every product treated as hot")

print("\n2. DETECTION HEALTH (last heartbeats)")
lines = tail()
hb = [l for l in lines if "⚡" in l][-6:]
if not hb:
    fail("no heartbeat lines found at all")
else:
    for h in hb[-3:]:
        print(f"         {h.strip()[:150]}")
    last = hb[-1]
    m = re.search(r"checked every ([\d.]+)s", last)
    if m:
        cad = float(m.group(1))
        tier = "hot " if HOT else ""
        (ok if cad <= 3 else warn)(f"median {tier}refresh {cad}s")
    m = re.search(r"worst (\d+)s", last)
    if m:
        w = int(m.group(1))
        tier = "hot " if HOT else ""
        # Only the hot tier is judged here. Cold products sit 90s stale by
        # design, and failing on them buried the number that matters.
        (ok if w <= 20 else (warn if w <= 60 else fail))(
            f"worst {tier}product last seen {w}s ago")
    m = re.search(r"(\d+)% blocked", last)
    if m:
        b = int(m.group(1))
        (ok if b <= 15 else (warn if b <= 40 else fail))(f"/dp/ blocked {b}%")
    m = re.search(r"([\d.]+)% stale", last)
    if m:
        s = float(m.group(1))
        (ok if s <= 5 else (warn if s <= 20 else fail))(f"stale {s}%")
    if "⚠" in last:
        frag = last[last.index("⚠"):].split("|")[0].strip()[:60]
        fail(f"heartbeat is flagging starving HOT products: {frag}")
    else:
        ok("no hot product is behind" if HOT
           else "no products flagged as behind")
    m = re.search(r"[+](\d+) cold", last)
    if m:
        ok(f"{m.group(1)} cold product(s) idling by design, not a fault")

cyc = [l for l in lines if "Wishlist crawl" in l][-3:]
for c in cyc[-1:]:
    print(f"         {c.strip()[:150]}")
m = re.search(r"cycle (\d+)ms", cyc[-1]) if cyc else None
if m:
    cm = int(m.group(1))
    (ok if cm <= 3000 else warn)(f"crawl cycle {cm}ms (worst-case detect)")
else:
    warn("no crawl cycle measurement yet")

# ---------------------------------------------------------------- 3
print("\n3. ERRORS")
errs = defaultdict(int)
for l in lines:
    m = re.search(r"(ERROR [A-Z0-9]{10}|Instant confirm error|"
                  r"Seller verify error|Wishlist crawl error|"
                  r"Scan-rate log error|DB save_ping error)", l)
    if m:
        errs[m.group(1).split()[0] if "ERROR" in m.group(1)
             else m.group(1)] += 1
if not errs:
    ok("no errors in recent log")
else:
    for k, v in sorted(errs.items(), key=lambda x: -x[1]):
        (warn if v < 20 else fail)(f"{k}: {v} occurrences")

# ---------------------------------------------------------------- 4
print("\n4. PROXY POOL")
per = defaultdict(lambda: [0, 0])
recent = lines[-15000:]
for l in recent:
    p = l.split("|")
    if len(p) >= 4:
        px = p[3].strip()
        if re.match(r"^\d+\.\d+\.\d+\.\d+:\d+$", px):
            per[px][0] += 1
            if "Captcha" in p[2] or "Blocked" in p[2]:
                per[px][1] += 1
if not per:
    warn("no per-proxy data in recent log")
else:
    bad = []
    for px, (t, b) in sorted(per.items(), key=lambda x: -x[1][1] / max(x[1][0], 1)):
        rate = b / t * 100 if t else 0
        if rate >= 50 and t >= 5:
            bad.append(f"{px} {rate:.0f}% blocked ({b}/{t})")
    ok(f"{len(per)} proxies seen in recent traffic")
    if bad:
        for b in bad[:5]:
            warn(f"underperforming: {b}")
    else:
        ok("no proxy above 50% block rate")

# ---------------------------------------------------------------- 5
print("\n5. PER-PRODUCT COVERAGE (can every item still be seen?)")
seen_recent = defaultdict(lambda: {"n": 0, "unknown": 0, "last": None})
for l in recent:
    m = re.match(r"\[(\d\d:\d\d:\d\d)\] (B0[0-9A-Z]{8}) \| ([^|]*)\| ([^|]*)\|", l)
    if m:
        d = seen_recent[m.group(2)]
        d["n"] += 1
        d["last"] = m.group(1)
        if "Unknown" in m.group(3) or "Unknown" in l.split("|")[2]:
            d["unknown"] += 1
try:
    from database import Database
    db = Database()
    prods = [p for p in db.get_products() if int(p.get("enabled", 1) or 0) == 1]
except Exception as e:
    prods = []
    fail(f"cannot read database: {e}")

if prods:
    ok(f"{len(prods)} enabled products in DB")
    missing = [p["asin"] for p in prods if p["asin"] not in seen_recent]
    if missing:
        warn(f"{len(missing)} product(s) absent from recent log "
             f"(may just be quiet): {', '.join(missing[:6])}")
    else:
        ok("every enabled product appears in recent traffic")
    stuck = [a for a, d in seen_recent.items()
             if d["n"] >= 8 and d["unknown"] == d["n"]]
    if stuck:
        fail(f"{len(stuck)} product(s) ONLY ever read as Unknown "
             f"(cannot ever ping): {', '.join(stuck[:6])}")
    else:
        ok("no product is permanently stuck at Unknown")

# ---------------------------------------------------------------- 6
print("\n6. PING GATE STATE (would a drop actually fire?)")
if prods:
    fired = [p for p in prods if (p.get("alert_state") or "") == "fired"]
    armed = [p for p in prods if (p.get("alert_state") or "armed") == "armed"]
    ok(f"{len(armed)} armed, {len(fired)} latched 'fired'")
    now = time.time()
    blocked_by_cd = []
    for p in prods:
        lp = p.get("last_pinged_at")
        if not lp:
            continue
        try:
            t = datetime.strptime(lp, "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:
            continue
        if now - t < 3600 and (p.get("alert_state") or "") == "fired":
            blocked_by_cd.append(f"{p['asin']} ({int((now-t)/60)}m ago)")
    # NOT a warning any more. The 3h per-item cooldown was removed from the
    # bot (MIN_TARGET_REPING_SECONDS is 0), so this is just "pinged
    # recently and latched": it re-arms on a confirmed sellout, whenever
    # that happens, with no clock involved. Only the 5-minute runaway cap
    # remains and nothing sensible trips it.
    if blocked_by_cd:
        ok(f"{len(blocked_by_cd)} item(s) pinged within the hour and "
           f"latched: {', '.join(blocked_by_cd[:4])}")
        print("         (re-arms on a confirmed sellout, no cooldown)")
    else:
        ok("no item pinged in the last hour")

    no_target = [p["asin"] for p in prods
                 if not float(p.get("target_price") or 0)]
    if no_target:
        warn(f"{len(no_target)} product(s) have no target and can never ping")
    else:
        ok("every product has a ping target")

# ---------------------------------------------------------------- 7
print("\n7. DELIVERY (Discord)")
recent_sends = [l for l in lines if "alert sent" in l]
hits = [l for l in lines if "TARGET HIT" in l]
ok(f"{len(hits)} TARGET HIT(s) and {len(recent_sends)} send(s) in current log")
if hits and not recent_sends:
    fail("hits recorded but NO sends: delivery is broken")
dropcfg = sh("ls /etc/systemd/system/stock-pinger.service.d/ 2>/dev/null")
for need in ("webhook.conf", "bot.conf"):
    (ok if need in dropcfg else warn)(
        f"{need} present" if need in dropcfg else f"{need} MISSING")
botline = [l for l in lines if "Bot target alert sent" in l]
(ok if botline else warn)(
    "discord bot has delivered alerts" if botline
    else "no bot deliveries seen in current log")

# ---------------------------------------------------------------- 8
print("\n8. PUBLIC STATS FEED (rasho.dev)")
try:
    with open("/opt/stock-pinger/public/stats.json") as fh:
        st = json.load(fh)
    age = (datetime.now() - datetime.strptime(
        st["generated_at"], "%Y-%m-%d %H:%M:%S")).total_seconds()
    (ok if age < 900 else warn)(f"stats.json {int(age)}s old")
    live = st.get("live", {})
    cad = live.get("cadence_s")
    (ok if cad else fail)(f"cadence_s = {cad}, scans_per_s = "
                          f"{live.get('scans_per_s')}")
    if cad in (None, 0, 0.0):
        fail("portfolio cadence is empty: the log format likely changed")
except Exception as e:
    warn(f"stats.json unreadable: {e}")

# ---------------------------------------------------------------- 9
print("\n" + "=" * 70)
print(f"SUMMARY: {len(OK)} ok, {len(WARN)} warnings, {len(FAIL)} failures")
print("=" * 70)
if FAIL:
    print("\nFAILURES:")
    for f_ in FAIL:
        print(f"  - {f_}")
if WARN:
    print("\nWARNINGS:")
    for w_ in WARN:
        print(f"  - {w_}")
sys.exit(1 if FAIL else 0)

"""Real Discord bot for Amazon Stock Pinger.

Replaces the webhook "automated messenger" with an actual Discord
application you own:

  * Its own identity — name, avatar, member-list presence, APP badge, and
    a token that belongs to your developer account (webhooks are
    anonymous URLs that anyone holding the link can post to).
  * Slash commands — /track, /untrack, /settarget, /status, /watchlist,
    /instock, /check. Because MonitorWorker re-reads the enabled-products
    table every 5s, /track takes effect WITHOUT a restart or redeploy.
  * Live presence — "Watching N products".
  * Richer messages — real embeds, link buttons, and the ability to EDIT
    a message later (a live stock board that updates in place instead of
    posting a fresh wall of text every cycle).

Drop-in compatible with DiscordNotifier: send_target_alert() and
send_digest() take the same arguments (including the leading aiohttp
session, which this class ignores — discord.py owns its own transport)
and return the same (ok, message) tuple, so monitor.py can fan out to
webhooks and the bot through the exact same code path.

Everything here degrades gracefully: no AMP_BOT_TOKEN (or no discord.py
installed) means the bot simply never starts and the webhooks keep
working exactly as before.
"""

import asyncio
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import discord
    from discord import app_commands
    DISCORD_AVAILABLE = True
except ImportError:  # library not installed — webhook-only mode
    DISCORD_AVAILABLE = False
    discord = None  # type: ignore
    app_commands = None  # type: ignore


BOT_TOKEN = os.environ.get("AMP_BOT_TOKEN", "").strip()
# Channel the target-price pings go to. Digest falls back to the same one.
ALERT_CHANNEL_ID = int(os.environ.get("AMP_ALERT_CHANNEL_ID", "").strip() or 0)
# NB: os.environ.get(..., "0") returns the STRING "0", which is truthy —
# `or ALERT_CHANNEL_ID` would never fire. Compare after int().
_digest_env = os.environ.get("AMP_DIGEST_CHANNEL_ID", "").strip()
DIGEST_CHANNEL_ID = (int(_digest_env) if _digest_env else 0) or ALERT_CHANNEL_ID
# Syncing commands to one guild is INSTANT; global sync can take ~1h to
# propagate. Set this to your server id while developing.
GUILD_ID = int(os.environ.get("AMP_GUILD_ID", "0") or 0)

ALERT_COLOR = 0xF59E0B     # amber — "act now"
DIGEST_COLOR = 0x3B82F6    # blue — informational
OK_COLOR = 0x22C55E
ERR_COLOR = 0xEF4444

DIGEST_ITEMS_PER_EMBED = 8   # fields per digest embed
MAX_BUTTONS = 25             # Discord hard cap: 5 rows x 5 buttons


def _short(text: str, limit: int = 150) -> str:
    text = (text or "").strip()
    return text[: limit - 3].rstrip() + "..." if len(text) > limit else text


def _valid_asin(asin: str) -> bool:
    asin = (asin or "").strip().upper()
    return len(asin) == 10 and asin.isalnum()


class StockPingerBot:
    """Owns the discord.py client and exposes the DiscordNotifier API."""

    def __init__(self, db, worker=None, log=None):
        self.db = db
        self.worker = worker           # MonitorWorker, for live stats
        self._log = log
        self.started_at = time.time()
        self.client = None
        self.tree = None
        self.ready = asyncio.Event()
        self._presence_task = None

        if not DISCORD_AVAILABLE or not BOT_TOKEN:
            return

        intents = discord.Intents.default()   # no privileged intents needed
        self.client = discord.Client(intents=intents)
        self.tree = app_commands.CommandTree(self.client)
        self._register_events()
        self._register_commands()

    # ------------------------------------------------------------------
    # plumbing
    # ------------------------------------------------------------------

    def log(self, msg: str) -> None:
        if self._log is not None:
            try:
                self._log.emit(msg)
                return
            except Exception:
                pass
        print(msg, flush=True)

    @property
    def enabled(self) -> bool:
        return self.client is not None

    def _register_events(self) -> None:
        @self.client.event
        async def on_ready():
            who = f"{self.client.user} (id {self.client.user.id})"
            self.log(f"🤖 Discord bot online as {who}")
            try:
                if GUILD_ID:
                    guild = discord.Object(id=GUILD_ID)
                    self.tree.copy_global_to(guild=guild)
                    synced = await self.tree.sync(guild=guild)
                    self.log(
                        f"🤖 Synced {len(synced)} slash command(s) to guild "
                        f"{GUILD_ID} (instant)."
                    )
                else:
                    synced = await self.tree.sync()
                    self.log(
                        f"🤖 Synced {len(synced)} slash command(s) globally "
                        f"(may take up to 1h to appear)."
                    )
            except Exception as e:
                self.log(f"🤖 Slash command sync failed: {e}")
            self.ready.set()
            if self._presence_task is None:
                self._presence_task = asyncio.create_task(self._presence_loop())

    async def _presence_loop(self) -> None:
        """Show live coverage in the member list — 'Watching N products'."""
        while True:
            try:
                n = len([
                    p for p in self.db.get_products()
                    if int(p.get("enabled", 1)) == 1
                ])
                in_stock = 0
                if self.worker is not None:
                    in_stock = sum(
                        1 for v in self.worker.effective_state.values()
                        if v == "in_stock"
                    )
                await self.client.change_presence(
                    activity=discord.Activity(
                        type=discord.ActivityType.watching,
                        name=f"{n} products · {in_stock} in stock",
                    )
                )
            except Exception:
                pass
            await asyncio.sleep(300)

    async def _channel(self, cid: int):
        if not cid:
            return None
        ch = self.client.get_channel(cid)
        if ch is None:
            try:
                ch = await self.client.fetch_channel(cid)
            except Exception as e:
                self.log(f"🤖 Cannot access channel {cid}: {e}")
                return None
        return ch

    async def start(self) -> None:
        """Run the client. Spawn as an asyncio task next to the scanners."""
        if not self.enabled:
            return
        try:
            await self.client.start(BOT_TOKEN)
        except Exception as e:
            self.log(f"🤖 Discord bot failed to start: {e}")

    async def close(self) -> None:
        if self.client is not None and not self.client.is_closed():
            try:
                await self.client.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # DiscordNotifier-compatible send API
    # ------------------------------------------------------------------

    async def send_target_alert(
        self,
        session=None,          # ignored: discord.py owns its transport
        *,
        title,
        asin,
        price,
        reason,
        url,
        image_url="",
        seller="",
        mention=None,
    ):
        if not self.enabled:
            return False, "Bot disabled."
        if not self.ready.is_set():
            return False, "Bot not ready yet."
        ch = await self._channel(ALERT_CHANNEL_ID)
        if ch is None:
            return False, "No alert channel configured."

        embed = discord.Embed(
            title=_short(title, 200) or "Amazon Product",
            url=url or None,
            color=ALERT_COLOR,
            description=f"🎯 **{price or '—'}** · `{asin}`",
            timestamp=datetime.now().astimezone(),
        )
        embed.set_author(name="amazon.ca Price Alert")
        embed.add_field(
            name="Reason", value=reason or "Target Price Reached", inline=True
        )
        if seller:
            embed.add_field(name="Seller", value=seller, inline=True)
        if image_url:
            embed.set_thumbnail(url=image_url)
        embed.set_footer(text="Amazon Stock Pinger")

        view = discord.ui.View(timeout=None)
        if url:
            view.add_item(discord.ui.Button(
                style=discord.ButtonStyle.link, label="Product page",
                url=url, emoji="🛒",
            ))

        # Ping the opt-in role, and ONLY that role.
        from notifier import PING_ROLE_ID
        content = mention or ""
        if PING_ROLE_ID:
            content = f"<@&{PING_ROLE_ID}> 🎯 **{_short(title, 120)}** @ {price or '—'}"
            allowed = discord.AllowedMentions(
                everyone=False, users=False,
                roles=[discord.Object(id=int(PING_ROLE_ID))],
            )
        else:
            allowed = discord.AllowedMentions.none()

        try:
            await ch.send(content=content or None, embed=embed, view=view,
                          allowed_mentions=allowed)
            return True, "Bot target alert sent."
        except Exception as e:
            return False, f"Bot target alert failed: {e}"

    async def send_digest(self, session=None, *, items, prev_prices=None):
        """The stock list. No ping."""
        if not self.enabled:
            return False, "Bot disabled."
        if not self.ready.is_set():
            return False, "Bot not ready yet."
        ch = await self._channel(DIGEST_CHANNEL_ID)
        if ch is None:
            return False, "No digest channel configured."

        prev_prices = prev_prices or {}
        chunks = [
            items[i:i + DIGEST_ITEMS_PER_EMBED]
            for i in range(0, len(items), DIGEST_ITEMS_PER_EMBED)
        ] or [[]]

        try:
            for k, chunk in enumerate(chunks, 1):
                embed = discord.Embed(
                    title="📦 Amazon Stock Watchlist",
                    color=DIGEST_COLOR,
                    description=(
                        f"{len(items)} purchasable item"
                        f"{'s' if len(items) != 1 else ''}"
                        + (f" · Part {k}/{len(chunks)}" if len(chunks) > 1 else "")
                    ),
                    timestamp=datetime.now().astimezone(),
                )
                for it in chunk:
                    embed.add_field(
                        name=_short(it.get("title", ""), 240) or it["asin"],
                        value=self._price_line(it, prev_prices)
                              + f"\n[View on Amazon]({it['url']})",
                        inline=False,
                    )
                embed.set_footer(text="Amazon Stock Pinger · updates daily")

                view = discord.ui.View(timeout=None)
                for it in chunk[:5]:
                    view.add_item(discord.ui.Button(
                        style=discord.ButtonStyle.link,
                        label=_short(it.get("title", it["asin"]), 40),
                        url=it["url"],
                    ))
                await ch.send(embed=embed, view=view if len(view.children) else None)
            return True, f"Bot stock list sent — {len(items)} item(s)."
        except Exception as e:
            return False, f"Bot digest failed: {e}"

    @staticmethod
    def _price_line(item, prev_prices) -> str:
        """Always-green dot: everything on this list is in stock."""
        price = item.get("price") or "—"
        pn = item.get("price_number")
        prev = (prev_prices or {}).get(item["asin"])
        line = f"🟢 **{price}**"
        if pn is not None and prev is not None and abs(pn - prev) >= 0.01:
            line += f" (was ${prev:.2f})"
        return line + f" · `{item['asin']}`"

    # ------------------------------------------------------------------
    # Slash commands
    # ------------------------------------------------------------------

    def _register_commands(self) -> None:
        tree, db = self.tree, self.db

        @tree.command(name="status",
                      description="Bot health: uptime, coverage, proxies")
        async def status(inter: "discord.Interaction"):
            await inter.response.defer(ephemeral=True)
            prods = db.get_products()
            enabled = [p for p in prods if int(p.get("enabled", 1)) == 1]
            targeted = [p for p in enabled if (p.get("target_price") or 0) > 0]
            up = int(time.time() - self.started_at)
            h, m = up // 3600, (up % 3600) // 60

            e = discord.Embed(title="🤖 Stock Pinger Status",
                              color=OK_COLOR,
                              timestamp=datetime.now().astimezone())
            e.add_field(name="Uptime", value=f"{h}h {m}m", inline=True)
            e.add_field(name="Tracked", value=str(len(enabled)), inline=True)
            e.add_field(name="With targets", value=str(len(targeted)), inline=True)

            if self.worker is not None:
                w = self.worker
                in_stock = sum(1 for v in w.effective_state.values()
                               if v == "in_stock")
                e.add_field(name="In stock now", value=str(in_stock), inline=True)
                e.add_field(
                    name="Crawl coverage",
                    value=f"{getattr(w, '_last_crawl_items', 0)} items / "
                          f"{getattr(w, '_last_crawl_pages', 0)} pages",
                    inline=True,
                )
                stats = getattr(w, "proxy_stats", {}) or {}
                hits = sum(s.get("hits", 0) for s in stats.values())
                blocks = sum(s.get("blocks", 0) for s in stats.values())
                rate = (blocks / hits * 100) if hits else 0.0
                e.add_field(name="Proxy block rate",
                            value=f"{rate:.1f}% ({len(stats)} proxies)",
                            inline=True)
            await inter.followup.send(embed=e, ephemeral=True)

        @tree.command(name="instock",
                      description="What's purchasable right now")
        async def instock(inter: "discord.Interaction"):
            await inter.response.defer()
            if self.worker is None:
                await inter.followup.send("Monitor not attached.")
                return
            rows = []
            for p in db.get_products():
                if int(p.get("enabled", 1)) != 1:
                    continue
                if self.worker.effective_state.get(p["asin"]) != "in_stock":
                    continue
                rows.append(
                    f"🟢 **{p.get('last_price') or '—'}** · "
                    f"`{p['asin']}` {_short(p.get('title', ''), 60)}"
                )
            e = discord.Embed(
                title="📦 In Stock Right Now",
                description="\n".join(rows[:40]) or "Nothing in stock.",
                color=DIGEST_COLOR,
                timestamp=datetime.now().astimezone(),
            )
            await inter.followup.send(embed=e)

        @tree.command(name="watchlist",
                      description="Every tracked product and its target")
        async def watchlist(inter: "discord.Interaction"):
            await inter.response.defer(ephemeral=True)
            lines = []
            for p in db.get_products():
                if int(p.get("enabled", 1)) != 1:
                    continue
                tgt = p.get("target_price") or 0
                state = (p.get("effective_state") or "?")
                dot = "🟢" if state == "in_stock" else "⚫"
                tgt_s = f"≤${tgt:.2f}" if tgt > 0 else "digest-only"
                lines.append(f"{dot} `{p['asin']}` {tgt_s} · "
                             f"{_short(p.get('title', ''), 45)}")
            body = "\n".join(lines) or "Watchlist is empty."
            for i in range(0, len(body), 3900):
                await inter.followup.send(
                    embed=discord.Embed(
                        title="👁️ Watchlist" if i == 0 else "👁️ Watchlist (cont.)",
                        description=body[i:i + 3900], color=DIGEST_COLOR,
                    ),
                    ephemeral=True,
                )

        @tree.command(name="check", description="Latest data for one ASIN")
        @app_commands.describe(asin="10-character Amazon ASIN")
        async def check(inter: "discord.Interaction", asin: str):
            asin = asin.strip().upper()
            row = next((p for p in db.get_products() if p["asin"] == asin), None)
            if row is None:
                await inter.response.send_message(
                    f"`{asin}` is not tracked.", ephemeral=True)
                return
            tgt = row.get("target_price") or 0
            e = discord.Embed(
                title=_short(row.get("title", "") or asin, 200),
                color=DIGEST_COLOR, timestamp=datetime.now().astimezone(),
            )
            e.add_field(name="Price", value=row.get("last_price") or "—", inline=True)
            e.add_field(name="Target",
                        value=f"${tgt:.2f}" if tgt > 0 else "digest-only",
                        inline=True)
            e.add_field(name="State",
                        value=row.get("effective_state") or "unknown", inline=True)
            lpp = row.get("last_pinged_price")
            e.add_field(name="Last announced",
                        value=f"${lpp:.2f}" if lpp is not None else "never",
                        inline=True)
            e.add_field(name="Latest scan",
                        value=row.get("last_checked") or "—", inline=True)
            e.set_footer(text=asin)
            await inter.response.send_message(embed=e, ephemeral=True)

        # ---- mutating commands: server managers only -------------------

        @tree.command(name="track",
                      description="Track a product (takes effect in ~5s, no restart)")
        @app_commands.describe(asin="10-character ASIN",
                               target="Ping at or below this price (0 = digest only)")
        @app_commands.default_permissions(manage_guild=True)
        async def track(inter: "discord.Interaction", asin: str,
                        target: float = 0.0):
            asin = asin.strip().upper()
            if not _valid_asin(asin):
                await inter.response.send_message(
                    f"`{asin}` is not a valid 10-character ASIN.", ephemeral=True)
                return
            if target < 0:
                await inter.response.send_message(
                    "Target cannot be negative.", ephemeral=True)
                return
            try:
                db.upsert_product(asin, target_price=float(target), enabled=1)
            except Exception as e:
                await inter.response.send_message(f"DB error: {e}", ephemeral=True)
                return
            tgt_s = f"at or below **${target:.2f}**" if target > 0 else "**digest-only** (never pings)"
            await inter.response.send_message(
                embed=discord.Embed(
                    title="✅ Now tracking",
                    description=f"`{asin}` — {tgt_s}\n\n"
                                f"Live within ~5s. No restart needed.",
                    color=OK_COLOR,
                )
            )
            self.log(f"🤖 /track {asin} target={target} by {inter.user}")

        @tree.command(name="untrack", description="Stop tracking a product")
        @app_commands.describe(asin="10-character ASIN")
        @app_commands.default_permissions(manage_guild=True)
        async def untrack(inter: "discord.Interaction", asin: str):
            asin = asin.strip().upper()
            if not any(p["asin"] == asin for p in db.get_products()):
                await inter.response.send_message(
                    f"`{asin}` is not tracked.", ephemeral=True)
                return
            db.update_product_enabled(asin, 0)
            await inter.response.send_message(
                embed=discord.Embed(
                    title="🛑 Stopped tracking",
                    description=f"`{asin}` disabled (row kept, so its "
                                f"ping history survives).",
                    color=ERR_COLOR,
                )
            )
            self.log(f"🤖 /untrack {asin} by {inter.user}")

        @tree.command(name="settarget", description="Change a product's target price")
        @app_commands.describe(asin="10-character ASIN", target="New target price")
        @app_commands.default_permissions(manage_guild=True)
        async def settarget(inter: "discord.Interaction", asin: str, target: float):
            asin = asin.strip().upper()
            row = next((p for p in db.get_products() if p["asin"] == asin), None)
            if row is None:
                await inter.response.send_message(
                    f"`{asin}` is not tracked — use `/track` first.",
                    ephemeral=True)
                return
            old = row.get("target_price") or 0
            db.upsert_product(asin, target_price=float(target),
                              enabled=int(row.get("enabled", 1)))
            await inter.response.send_message(
                embed=discord.Embed(
                    title="🎯 Target updated",
                    description=f"`{asin}`  ${old:.2f} → **${target:.2f}**\n\n"
                                f"Live within ~5s.",
                    color=OK_COLOR,
                )
            )
            self.log(f"🤖 /settarget {asin} {old} -> {target} by {inter.user}")

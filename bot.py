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
from typing import Any, Dict, List, Optional, Union

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

DIGEST_ITEMS_PER_EMBED = 8   # fields per digest embed (fallback path)
# Components V2 caps a message at ~40 components. Each product block
# costs 3 (section + action row + separator), so 6 per message is safe.
DIGEST_ITEMS_PER_PART = 6


# Components V2 ("LayoutView") lands in discord.py 2.6+. It's what
# gives headings, sections with a side thumbnail, separators and
# stacked button rows — the modern look. Falls back to classic embeds
# on older libraries.
LAYOUT_V2 = DISCORD_AVAILABLE and hasattr(discord.ui, "LayoutView")

# Quantity shortcuts on each alert. Amazon's remote "add to cart"
# endpoint drops the item straight into the cart at this quantity,
# carrying the affiliate tag through the purchase.
ATC_QUANTITIES = (1, 2, 3)


def _short(text: str, limit: int = 150) -> str:
    text = (text or "").strip()
    return text[: limit - 3].rstrip() + "..." if len(text) > limit else text


def _credit_text() -> str:
    """Attribution line, single source of truth in notifier.py."""
    try:
        from notifier import CREDIT as _c
        return _c
    except Exception:
        return "Bot developed by @elmar"


CREDIT = _credit_text()


def _affiliate_tag() -> str:
    """Single source of truth lives in monitor.py; imported lazily so
    bot.py has no import-time dependency on it."""
    try:
        from monitor import AFFILIATE_TAG
        return AFFILIATE_TAG or ""
    except Exception:
        return ""


def _atc_url(asin: str, qty: int, domain: str = "https://www.amazon.ca") -> str:
    """Amazon's remote add-to-cart link — one click puts `qty` in the
    cart instead of making people click through the listing first."""
    url = (
        f"{domain}/gp/aws/cart/add.html?ASIN.1={asin}&Quantity.1={qty}"
    )
    tag = _affiliate_tag()
    return f"{url}&AssociateTag={tag}" if tag else url


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

        # Ping the opt-in role, and ONLY that role.
        from notifier import PING_ROLE_ID
        if PING_ROLE_ID:
            allowed = discord.AllowedMentions(
                everyone=False, users=False,
                roles=[discord.Object(id=int(PING_ROLE_ID))],
            )
        else:
            allowed = discord.AllowedMentions.none()

        try:
            content, view = self._alert_message(
                title=title, asin=asin, price=price, reason=reason,
                url=url, seller=seller, role_id=PING_ROLE_ID,
            )
            await ch.send(content=content, view=view,
                          allowed_mentions=allowed)
            return True, "Bot target alert sent."
        except Exception as e:
            return False, f"Bot target alert failed: {e}"

    # -- alert rendering ------------------------------------------------

    def _alert_message(self, *, title, asin, price, reason, url,
                       seller, role_id, mention=None):
        """Plain message text plus real buttons. Returns (content, view).

        NOT Components V2, on purpose. Discord builds the phone
        notification from a message's `content`, and the V2 API forbids
        `content` entirely, so a V2 alert can only ever push a useless
        "Bot sent a message". Everything here is ordinary message text,
        so the notification names the product and the price, which is
        the whole point of getting pinged.

        Buttons still work because a bot (unlike a plain webhook) may
        attach action rows to a normal message. The cost is the side
        thumbnail: an image needs either V2 or an embed, and an embed
        brings back the boxed frame. Text beats picture on a phone.
        """
        ping = mention if mention is not None else (
            f"<@&{role_id}>" if role_id else "")
        short_title = _short(title, 120) or "Amazon Product"

        # Lead line carries the product and price so a truncated
        # notification preview still says what restocked and for how much.
        lines = [f"{ping} 🎯 **{short_title}** @ {price or '—'}".lstrip()]
        lines.append("")
        lines.append("# 🎯 Amazon.ca Restock Alert")
        lines.append(f"**{_short(title, 180) or 'Amazon Product'}**")
        lines.append(f"🎯 **{price or '—'}** · `{asin}`")
        lines.append("")
        lines.append(f"Event: {reason or 'Target Price Reached'}")
        lines.append("Reason: Item is in stock at or below target")
        if seller:
            lines.append(f"Seller: {seller}")
        lines.append("")
        lines.append(f"-# {CREDIT}")
        content = "\n".join(lines)

        view = discord.ui.View(timeout=None)
        for q in ATC_QUANTITIES:
            view.add_item(discord.ui.Button(
                style=discord.ButtonStyle.link, label=f"ATC {q}",
                url=_atc_url(asin, q), emoji="🛒", row=0,
            ))
        if url:
            view.add_item(discord.ui.Button(
                style=discord.ButtonStyle.link, label="Listing",
                url=url, emoji="📄", row=1,
            ))
        return content, view

    def _alert_layout(self, *, title, asin, price, reason, url,
                      image_url, seller, role_id, mention=None):
        """Flat Components V2 alert: mention, heading, thumbnailed
        product section, add-to-cart rows.

        Deliberately NOT wrapped in a Container. A Container draws the
        bordered, accent-barred box around everything, which is the
        embed-looking frame we are trying to get away from. Adding the
        pieces straight to the view renders them as plain message
        content: cleaner on desktop, and the text is real message text
        so a phone notification can actually show what restocked
        instead of a generic "sent a message".

        `mention` overrides the usual role ping, so /preview can aim a
        test ping at one person or role.
        """
        ui = discord.ui
        view = ui.LayoutView(timeout=None)

        ping = mention if mention is not None else (
            f"<@&{role_id}>" if role_id else "")
        if ping:
            view.add_item(ui.TextDisplay(ping))
        view.add_item(ui.TextDisplay("# 🎯 Amazon.ca Restock Alert"))

        # Divider under the heading, so the alert reads as a header and
        # then a product rather than one run-on block.
        view.add_item(ui.Separator())

        # What the item IS (title, price, code) is separated from the
        # supporting detail by a blank line: the eye lands on the price
        # first, which is the part that decides whether to click.
        body = (
            f"**{_short(title, 180) or 'Amazon Product'}**\n"
            f"🎯 **{price or '—'}** · `{asin}`\n"
            f"\n"
            f"Event: {reason or 'Target Price Reached'}\n"
            f"Reason: Item is in stock at or below target"
        )
        if seller:
            body += f"\nSeller: {seller}"

        if image_url:
            view.add_item(ui.Section(
                ui.TextDisplay(body),
                accessory=ui.Thumbnail(media=image_url),
            ))
        else:
            view.add_item(ui.TextDisplay(body))

        # Divider before the actions, so the buttons read as a distinct
        # "do something" zone instead of crowding the detail lines.
        view.add_item(ui.Separator())

        # Quick add-to-cart row (qty 1/2/3), then the listing.
        atc = ui.ActionRow()
        for q in ATC_QUANTITIES:
            atc.add_item(ui.Button(
                style=discord.ButtonStyle.link, label=f"ATC {q}",
                url=_atc_url(asin, q), emoji="🛒",
            ))
        view.add_item(atc)

        if url:
            row = ui.ActionRow()
            row.add_item(ui.Button(
                style=discord.ButtonStyle.link, label="Listing",
                url=url, emoji="📄",
            ))
            view.add_item(row)

        # Attribution, small and dim so it reads as a footer. '-# ' is
        # Discord's subtext markdown.
        view.add_item(ui.Separator())
        view.add_item(ui.TextDisplay(f"-# {CREDIT}"))

        return view

    def _alert_embed_fallback(self, title, asin, price, reason, url,
                              image_url, seller, role_id):
        """Classic embed, used only on discord.py < 2.6."""
        embed = discord.Embed(
            title=_short(title, 200) or "Amazon Product",
            url=url or None, color=ALERT_COLOR,
            description=f"🎯 **{price or '—'}** · `{asin}`",
            timestamp=datetime.now().astimezone(),
        )
        embed.set_author(name="amazon.ca Price Alert")
        embed.add_field(name="Reason",
                        value=reason or "Target Price Reached", inline=True)
        if seller:
            embed.add_field(name="Seller", value=seller, inline=True)
        if image_url:
            embed.set_thumbnail(url=image_url)
        view = discord.ui.View(timeout=None)
        for q in ATC_QUANTITIES:
            view.add_item(discord.ui.Button(
                style=discord.ButtonStyle.link, label=f"ATC {q}",
                url=_atc_url(asin, q), emoji="🛒"))
        if url:
            view.add_item(discord.ui.Button(
                style=discord.ButtonStyle.link, label="Listing",
                url=url, emoji="📄"))
        content = (
            f"<@&{role_id}> 🎯 **{_short(title, 120)}** @ {price or '—'}"
            if role_id else ""
        )
        return content, embed, view

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
        per = DIGEST_ITEMS_PER_PART if LAYOUT_V2 else DIGEST_ITEMS_PER_EMBED
        chunks = [
            items[i:i + per] for i in range(0, len(items), per)
        ] or [[]]

        try:
            for k, chunk in enumerate(chunks, 1):
                if LAYOUT_V2:
                    await ch.send(view=self._digest_layout(
                        chunk, k, len(chunks), len(items), prev_prices))
                else:
                    embed = discord.Embed(
                        title="📦 Amazon Stock Watchlist", color=DIGEST_COLOR,
                        description=(
                            f"{len(items)} purchasable item"
                            f"{'s' if len(items) != 1 else ''}"
                            + (f" · Part {k}/{len(chunks)}"
                               if len(chunks) > 1 else "")
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
                    await ch.send(embed=embed)
            return True, f"Bot stock list sent — {len(items)} item(s)."
        except Exception as e:
            return False, f"Bot digest failed: {e}"

    def _digest_layout(self, chunk, k, n, total, prev_prices):
        """Components V2 stock list: heading, then one thumbnailed
        section + listing button per product, separated by dividers."""
        ui = discord.ui
        c = ui.Container(accent_colour=discord.Colour(DIGEST_COLOR))
        header = (
            "# 📦 Amazon Stock Watchlist\n"
            f"Updated: {datetime.now().strftime('%B %d, %Y')}\n"
            f"{total} purchasable item{'s' if total != 1 else ''}"
        )
        if n > 1:
            header += f" · Part {k}/{n}"
        c.add_item(ui.TextDisplay(header))
        c.add_item(ui.Separator())

        for i, it in enumerate(chunk):
            body = (f"**{_short(it.get('title', ''), 150) or it['asin']}**\n"
                    f"{self._price_line(it, prev_prices)}")
            if it.get("image_url"):
                c.add_item(ui.Section(
                    ui.TextDisplay(body),
                    accessory=ui.Thumbnail(media=it["image_url"]),
                ))
            else:
                c.add_item(ui.TextDisplay(body))
            row = ui.ActionRow()
            row.add_item(ui.Button(
                style=discord.ButtonStyle.link, label="Listing",
                url=it["url"], emoji="📄",
            ))
            c.add_item(row)
            if i < len(chunk) - 1:
                c.add_item(ui.Separator())

        # One credit for the whole list, not one per product.
        c.add_item(ui.Separator())
        c.add_item(ui.TextDisplay(f"-# {CREDIT}"))

        view = ui.LayoutView(timeout=None)
        view.add_item(c)
        return view

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

        @tree.command(
            name="preview",
            description="Preview a message layout — only you see it, pings nobody",
        )
        @app_commands.describe(
            kind="Which message to preview",
            asin="Optional: render with a real tracked product's live data",
            ping="Optional: really ping this role or person as a live test",
        )
        @app_commands.choices(kind=[
            app_commands.Choice(name="Target alert", value="alert"),
            app_commands.Choice(name="Stock list", value="stocklist"),
        ])
        @app_commands.default_permissions(manage_guild=True)
        async def preview(inter: "discord.Interaction",
                          kind: "app_commands.Choice[str]",
                          asin: Optional[str] = None,
                          ping: Optional[
                              "Union[discord.Role, discord.Member]"] = None):
            """Safe dry-run of the real renderers.

            Three guarantees, because this bot lives in a large server:
              1. ephemeral=True  — nobody but the invoker can see it.
              2. AllowedMentions.none() — the role pill still RENDERS
                 (so the preview is visually accurate) but Discord sends
                 zero notifications.
              3. Read-only — builds a view and returns; touches no
                 latch, no DB row, no ping state.
            """
            sample = {
                "title": "Pokémon TCG: Mega Evolution—Pitch Black Booster Bundle",
                "asin": "B0GYTRYV7P", "price": "$49.95",
                "price_number": 49.95, "seller": "Amazon.ca",
                "url": "https://www.amazon.ca/dp/B0GYTRYV7P",
                "image_url": "",
            }

            # Prefer real live data when an ASIN is given.
            if asin:
                a = asin.strip().upper()
                row = next((p for p in db.get_products() if p["asin"] == a), None)
                if row is None:
                    await inter.response.send_message(
                        f"`{a}` is not tracked — omit the asin to use sample "
                        f"data, or `/track` it first.", ephemeral=True)
                    return
                live = {}
                if self.worker is not None:
                    live = dict(self.worker.latest_result.get(a) or {})
                sample = {
                    "title": live.get("title") or row.get("title") or a,
                    "asin": a,
                    "price": live.get("price") or row.get("last_price") or "—",
                    "price_number": live.get("price_number")
                                    or row.get("last_price_number"),
                    "seller": live.get("seller") or "Amazon.ca",
                    "url": live.get("url")
                           or f"https://www.amazon.ca/dp/{a}",
                    "image_url": live.get("image_url") or "",
                }
            try:
                from monitor import with_affiliate_tag
                sample["url"] = with_affiliate_tag(sample["url"])
            except Exception:
                pass

            from notifier import PING_ROLE_ID
            try:
                content = None
                if kind.value == "alert":
                    content, view = self._alert_message(
                        title=sample["title"], asin=sample["asin"],
                        price=sample["price"], reason="Restock Alert",
                        url=sample["url"], seller=sample["seller"],
                        role_id=PING_ROLE_ID,
                        mention=(ping.mention if ping else None),
                    )
                else:
                    view = self._digest_layout(
                        [sample], 1, 1, 1, {}) if LAYOUT_V2 else None
                if view is None:
                    await inter.response.send_message(
                        "Components V2 unavailable (discord.py < 2.6) — "
                        "alerts fall back to classic embeds.", ephemeral=True)
                    return

                if ping is None:
                    # Silent preview: invisible to others, notifies nobody.
                    await inter.response.send_message(
                        content=content, view=view, ephemeral=True,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
                    await inter.followup.send(
                        f"☝️ Preview of the **{kind.name}** layout.\n"
                        f"• Visible only to you (ephemeral)\n"
                        f"• **0 notifications sent** — the mention renders "
                        f"but pings nobody\n"
                        f"• Nothing was written to the database\n"
                        f"• Add `ping:` to send a real test ping",
                        ephemeral=True,
                    )
                    return

                # LIVE TEST PING. Posts publicly and really notifies the
                # chosen target, because an ephemeral message cannot
                # notify anyone but the person who ran the command.
                is_role = isinstance(ping, discord.Role)
                allowed = discord.AllowedMentions(
                    everyone=False,
                    roles=[ping] if is_role else False,
                    users=False if is_role else [ping],
                )
                await inter.response.send_message(
                    content=content, view=view, allowed_mentions=allowed)
                await inter.followup.send(
                    f"✅ Test ping sent to **{ping}** "
                    f"({'role' if is_role else 'user'}).\n"
                    f"• This one was **public and really notified them**\n"
                    f"• Nothing was written to the database",
                    ephemeral=True,
                )
                self.log(f"🤖 /preview test ping -> {ping} by {inter.user}")
            except Exception as e:
                # Never let a preview bug surface as a broken interaction.
                msg = f"Preview failed to render: `{e}`"
                if inter.response.is_done():
                    await inter.followup.send(msg, ephemeral=True)
                else:
                    await inter.response.send_message(msg, ephemeral=True)
                self.log(f"🤖 /preview error: {e}")

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

import aiohttp
from datetime import datetime
from urllib.parse import urlparse


TARGET_ALERT_COLOR = 0xf59e0b   # amber — "act now" signal
DIGEST_COLOR = 0x3b82f6         # calm blue — informational (fallback embeds)

# Target alerts ping this Discord role (opt-in "Amazon Notifications"
# role) instead of @everyone. Discord pings roles by numeric ID, not
# name, so we emit <@&ID> and whitelist the role in allowed_mentions
# (which makes it ping even if the role isn't marked "mentionable").
# Role IDs are not secret. Set to "" to fall back to @everyone.
PING_ROLE_ID = "1502822024358264843"
PING_MENTION = f"<@&{PING_ROLE_ID}>" if PING_ROLE_ID else "@everyone"

# Stock-list layout (Discord Components V2). Each product block costs
# 6 components (section + text + thumbnail + action row + button +
# separator) and Discord caps a message at ~40 components, so 6
# products per message part is the safe ceiling. Longer lists split
# into "Part k/n" messages, PokeWatch-style.
DIGEST_ITEMS_PER_PART = 6

# IS_COMPONENTS_V2 message flag — switches the message to the new
# layout system (text displays, sections with thumbnail accessories,
# separators). content/embeds must be absent when this flag is set.
FLAG_COMPONENTS_V2 = 1 << 15


# Quantity shortcuts on each alert. Amazon's remote add-to-cart endpoint
# drops the item straight into the cart, carrying the affiliate tag.
ATC_QUANTITIES = (1, 2, 3)


# Attribution line on every alert. A Discord handle rather than a URL:
# the server approved a credit, not a link out of the server.
#
# Deliberately plain text, not a real <@id> mention, so it renders as a
# credit and never pings anyone.
CREDIT = "Bot developed by @elmar"


def _credit_blocks():
    """Separator plus a small grey credit line, for Components V2 messages.

    '-# ' is Discord's subtext markdown, which renders smaller and dimmer
    than body text, so the credit reads as a footer rather than content.
    """
    return [
        {"type": 14, "divider": True, "spacing": 1},
        {"type": 10, "content": f"-# {CREDIT}"},
    ]


def _short(text, limit=150):
    text = (text or "").strip()
    if len(text) > limit:
        return text[: limit - 3].rstrip() + "..."
    return text


def _atc_url(asin, qty, domain="https://www.amazon.ca"):
    """One-click add-to-cart at a given quantity. The affiliate tag lives
    in monitor.py; imported lazily because monitor imports THIS module
    (a top-level import would be circular)."""
    url = f"{domain}/gp/aws/cart/add.html?ASIN.1={asin}&Quantity.1={qty}"
    try:
        from monitor import AFFILIATE_TAG
        if AFFILIATE_TAG:
            url += f"&AssociateTag={AFFILIATE_TAG}"
    except Exception:
        pass
    return url


class DiscordNotifier:
    """Async, non-blocking Discord webhook.

    Two message types:
      - send_target_alert: @everyone ping when a tracked item is
        confirmed in stock, sold by Amazon, at/below its target price.
        The target price itself is never shown.
      - send_digest: the "Amazon Stock Watchlist" list of everything
        currently purchasable. NO ping — informational. Styled with
        Components V2 (bold title, 🟢/🔴 price + (was $X), `ASIN`,
        product image, LINK button, separators); falls back to classic
        embeds if the webhook rejects the new layout.

    Reuses the caller's aiohttp.ClientSession so a slow webhook can
    never stall the asyncio event loop.
    """

    def __init__(self, webhook_url):
        self.webhook_url = (webhook_url or "").strip()
        # None = untested, True = components v2 works, False = this
        # webhook strips components (plain incoming webhook).
        self._supports_components = None

    async def _post_raw(self, session: aiohttp.ClientSession, payload: dict):
        """POST and return (status, body_text). status None on network
        error."""
        try:
            async with session.post(
                self.webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                body = "" if r.status in (200, 204) else await r.text()
                return r.status, body
        except Exception as e:
            return None, str(e)

    async def _post(self, session: aiohttp.ClientSession, payload: dict, label: str):
        if not self.webhook_url:
            return False, "Webhook is empty."
        status, body = await self._post_raw(session, payload)
        if status in (200, 204):
            return True, f"Discord {label} sent."
        return False, f"Discord {label} failed: {status} {body[:150]}"

    # ------------------------------------------------------------------
    # Target alert (@everyone)
    # ------------------------------------------------------------------

    async def send_target_alert(
        self,
        session: aiohttp.ClientSession,
        *,
        title,
        asin,
        price,
        reason,
        url,
        image_url="",
        seller="",
        mention=PING_MENTION,
    ):
        """Role ping (Amazon Notifications) when an item hits its target
        price (Amazon-sold, confirmed in stock). Deliberately does NOT
        reveal what the target price is — the armed/fired latch in the
        monitor guarantees this doesn't repeat while the item sits at
        target."""
        if not self.webhook_url:
            return False, "Webhook is empty."

        # Components V2 gives the nicest layout, but PLAIN incoming
        # webhooks cannot carry components at all: Discord strips them
        # and then rejects the message as empty (50006). Only an
        # application-owned webhook or a bot can send them. So try V2
        # once, remember the answer, and never pay for that round trip
        # again on a webhook that cannot do it.
        if self._supports_components is not False:
            payload = self._alert_v2_payload(
                title=title, asin=asin, price=price, reason=reason,
                url=url, image_url=image_url, seller=seller,
            )
            status, body = await self._post_raw(session, payload)
            if status in (200, 204):
                self._supports_components = True
                return True, "Discord target alert sent (components v2)."
            if status != 400:
                return False, (
                    f"Discord target alert failed: {status} {str(body)[:150]}"
                )
            self._supports_components = False

        fb = self._alert_embed_payload(
            title=title, asin=asin, price=price, reason=reason,
            url=url, image_url=image_url, seller=seller, mention=mention,
        )
        return await self._post(session, fb, "target alert")

    def _alert_v2_payload(self, *, title, asin, price, reason, url,
                          image_url, seller):
        """Container → role pill → H1 heading → section w/ thumbnail →
        separators → ATC quantity row → Listing row."""
        blocks = []
        if PING_ROLE_ID:
            blocks.append({"type": 10, "content": PING_MENTION})
        blocks.append({
            "type": 10,
            "content": "# 🎯 Amazon.ca Restock Alert",
        })
        blocks.append({"type": 14, "divider": True, "spacing": 1})

        body = (
            f"**{_short(title, 180) or 'Amazon Product'}**\n"
            f"🎯 **{price or '—'}** · `{asin}`\n"
            f"Event: {reason or 'Target Price Reached'}\n"
            f"Reason: Item is in stock at or below target"
        )
        if seller:
            body += f"\nSeller: {seller}"

        if image_url:
            blocks.append({
                "type": 9,
                "components": [{"type": 10, "content": body}],
                "accessory": {"type": 11, "media": {"url": image_url}},
            })
        else:
            blocks.append({"type": 10, "content": body})

        blocks.append({"type": 14, "divider": True, "spacing": 1})
        blocks.append({
            "type": 1,
            "components": [
                {"type": 2, "style": 5, "label": f"ATC {q}",
                 "emoji": {"name": "🛒"}, "url": _atc_url(asin, q)}
                for q in ATC_QUANTITIES
            ],
        })
        if url:
            blocks.append({
                "type": 1,
                "components": [{
                    "type": 2, "style": 5, "label": "Listing",
                    "emoji": {"name": "📄"}, "url": url,
                }],
            })
        blocks.extend(_credit_blocks())

        payload = {
            "flags": FLAG_COMPONENTS_V2,
            "components": [{
                "type": 17,
                "accent_color": TARGET_ALERT_COLOR,
                "components": blocks,
            }],
        }
        # Whitelist ONLY the notify role, so the pill actually pings even
        # when the role isn't "mentionable", and nothing else can.
        if PING_ROLE_ID:
            payload["allowed_mentions"] = {"roles": [PING_ROLE_ID]}
        return payload

    def _alert_embed_payload(self, *, title, asin, price, reason, url,
                             image_url, seller, mention):
        """Embed alert. This is the LIVE path for plain incoming
        webhooks, which cannot render components, so it has to carry the
        full design on its own: heading, spaced detail lines, thumbnail,
        credit footer, and markdown add-to-cart links."""
        try:
            host = (urlparse(url).netloc or "Amazon").replace("www.", "")
        except Exception:
            host = "Amazon"

        # Plain incoming webhooks strip message components, so the
        # quantity shortcuts ship as bold markdown links instead of
        # buttons. Same one click behaviour, no button chrome.
        actions = "  ·  ".join(
            f"[**ATC {q}**]({_atc_url(asin, q)})" for q in ATC_QUANTITIES
        )
        body = [
            f"🎯 **{price or '—'}**  ·  `{asin}`",
            "",
            f"**Event:** {reason or 'Target Price Reached'}",
            "**Reason:** Item is in stock at or below target",
        ]
        if seller:
            body.append(f"**Seller:** {seller}")
        body += ["", f"🛒 {actions}"]
        if url:
            body.append(f"📄 [**View Listing**]({url})")

        embed = {
            "author": {"name": f"{host} Restock Alert"},
            "title": _short(title, 200) or "Amazon Product",
            "url": url or None,
            "color": TARGET_ALERT_COLOR,
            "description": "\n".join(body),
            "footer": {"text": CREDIT},
        }
        if image_url:
            embed["thumbnail"] = {"url": image_url}

        payload = {
            "content": f"{mention} 🎯 **{_short(title, 120)}** @ {price or '—'}",
            "embeds": [embed],
        }
        if PING_ROLE_ID:
            payload["allowed_mentions"] = {"roles": [PING_ROLE_ID]}
        return payload

    # ------------------------------------------------------------------
    # Stock-list digest (no ping)
    # ------------------------------------------------------------------

    @staticmethod
    def _price_line(item, prev_prices):
        """'🟢 **$49.98** (was $59.95) · `ASIN`'. The circle is ALWAYS
        green — everything on this list is in stock, so a green dot is
        just the in-stock indicator (no red/absent variants, which read
        as confusing). The '(was $X)' note still shows when the price
        moved since the previous list."""
        price = item.get("price") or "—"
        pn = item.get("price_number")
        prev = (prev_prices or {}).get(item["asin"])
        parts = ["🟢", f"**{price}**"]
        if pn is not None and prev is not None and abs(pn - prev) >= 0.01:
            parts.append(f"(was ${prev:.2f})")
        return " ".join(parts) + f" · `{item['asin']}`"

    def _digest_header(self, k, n, total_items):
        # Date only — no clock time. The list refreshes daily and viewers
        # span multiple timezones, so a wall-clock time is just noise.
        today = datetime.now().strftime("%B %d, %Y")
        lines = [
            "# Amazon Stock Watchlist",
            f"Updated: {today}",
            f"{total_items} purchasable item{'s' if total_items != 1 else ''}",
        ]
        if n > 1:
            lines.append(f"Part {k}/{n}")
        return "\n".join(lines)

    def _digest_v2_payload(self, chunk, k, n, total_items, prev_prices):
        """Build one Components-V2 message part: header text, then per
        product a section (bold plain-text title + price line, product
        image as the side thumbnail), a LINK button, and a separator."""
        components = [
            {"type": 10, "content": self._digest_header(k, n, total_items)},
            {"type": 14, "divider": True, "spacing": 2},
        ]
        for i, it in enumerate(chunk):
            text = f"**{_short(it['title'])}**\n{self._price_line(it, prev_prices)}"
            if it.get("image_url"):
                components.append({
                    "type": 9,
                    "components": [{"type": 10, "content": text}],
                    "accessory": {
                        "type": 11,
                        "media": {"url": it["image_url"]},
                    },
                })
            else:
                components.append({"type": 10, "content": text})
            components.append({
                "type": 1,
                "components": [{
                    "type": 2, "style": 5, "label": "LINK", "url": it["url"],
                }],
            })
            if i < len(chunk) - 1:
                components.append({"type": 14, "divider": True, "spacing": 1})
        # Only on the final part. A digest split across three messages does
        # not need the credit repeated three times.
        if k == n:
            components.extend(_credit_blocks())
        return {"flags": FLAG_COMPONENTS_V2, "components": components}

    def _digest_embed_payload(self, chunk, k, n, total_items, prev_prices):
        """Classic-embed fallback, used only if the webhook rejects the
        V2 layout (400). Same information, plainer look."""
        header = {
            "title": "📦 Amazon Stock Watchlist",
            "color": DIGEST_COLOR,
            "description": self._digest_header(k, n, total_items).replace(
                "# Amazon Stock Watchlist\n", ""
            ),
        }
        embeds = [header]
        for it in chunk:
            e = {
                "title": _short(it["title"]),
                "color": DIGEST_COLOR,
                "description": (
                    f"{self._price_line(it, prev_prices)}\n[LINK]({it['url']})"
                ),
            }
            if it.get("image_url"):
                e["thumbnail"] = {"url": it["image_url"]}
            embeds.append(e)
        out = embeds[:10]
        # Footer goes on the last embed that actually survives the cap, and
        # only on the final part, matching the V2 layout above.
        if k == n and out:
            out[-1]["footer"] = {"text": CREDIT}
        return {"embeds": out}

    async def send_digest(
        self,
        session: aiohttp.ClientSession,
        *,
        items,              # [{asin,title,price,price_number,url,image_url,target}]
        prev_prices=None,   # {asin: float} — prices from the previous list
    ):
        """The stock-list message. One message (or Part k/n series for
        >6 items), never a ping."""
        if not self.webhook_url:
            return False, "Webhook is empty."

        chunks = [
            items[i:i + DIGEST_ITEMS_PER_PART]
            for i in range(0, len(items), DIGEST_ITEMS_PER_PART)
        ] or [[]]
        n = len(chunks)

        for k, chunk in enumerate(chunks, 1):
            # Same capability memory as the alert path: a plain incoming
            # webhook strips components, so once we have learned that,
            # skip straight to embeds instead of burning a rejected
            # round trip per chunk on every single digest.
            if self._supports_components is not False:
                payload = self._digest_v2_payload(
                    chunk, k, n, len(items), prev_prices)
                status, body = await self._post_raw(session, payload)
                if status in (200, 204):
                    self._supports_components = True
                    continue
                if status != 400:
                    return False, (
                        f"Discord digest failed: {status} {str(body)[:150]}"
                    )
                self._supports_components = False

            fb = self._digest_embed_payload(
                chunk, k, n, len(items), prev_prices)
            status2, body2 = await self._post_raw(session, fb)
            if status2 not in (200, 204):
                return False, (
                    f"Discord digest failed: {status2} {str(body2)[:150]}"
                )

        return True, (
            f"Discord stock list sent — {len(items)} item(s)"
            + (f" in {n} parts." if n > 1 else ".")
        )

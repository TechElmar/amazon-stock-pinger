import aiohttp
from datetime import datetime
from urllib.parse import urlparse


TARGET_ALERT_COLOR = 0xf59e0b   # amber — "act now" signal
DIGEST_COLOR = 0x3b82f6         # calm blue — informational (fallback embeds)

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


def _short(text, limit=150):
    text = (text or "").strip()
    if len(text) > limit:
        return text[: limit - 3].rstrip() + "..."
    return text


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
        mention="@everyone",
    ):
        """@everyone ping: item hit its target price (Amazon-sold,
        confirmed in stock). Deliberately does NOT reveal what the
        target price is — the armed/fired latch in the monitor
        guarantees this doesn't repeat while the item sits at target."""
        if not self.webhook_url:
            return False, "Webhook is empty."

        try:
            host = (urlparse(url).netloc or "Amazon").replace("www.", "")
        except Exception:
            host = "Amazon"

        content = f"{mention} 🎯 **{_short(title, 120)}** @ {price or '—'}"

        embed = {
            "author": {"name": f"{host} Price Alert"},
            "title": _short(title, 200) or "Amazon Product",
            "url": url,
            "color": TARGET_ALERT_COLOR,
            "description": f"🎯 **{price or '—'}** · `{asin}`",
            "fields": [
                {"name": "Reason", "value": reason or "Target Price Reached", "inline": True},
            ],
        }
        if seller:
            embed["fields"].append(
                {"name": "Seller", "value": seller, "inline": True}
            )
        if image_url:
            embed["thumbnail"] = {"url": image_url}

        payload = {
            "content": content,
            "embeds": [embed],
            "components": [
                {
                    "type": 1,
                    "components": [
                        {
                            "type": 2,
                            "style": 5,
                            "label": "Product page",
                            "emoji": {"name": "📓"},
                            "url": url,
                        }
                    ],
                }
            ],
        }
        return await self._post(session, payload, "target alert")

    # ------------------------------------------------------------------
    # Stock-list digest (no ping)
    # ------------------------------------------------------------------

    @staticmethod
    def _price_line(item, prev_prices):
        """'🟢 **$49.98** (was $59.95) · `ASIN`' — green when the price
        held or dropped since the last list, red when it rose, no
        circle when there's no history."""
        price = item.get("price") or "—"
        pn = item.get("price_number")
        prev = (prev_prices or {}).get(item["asin"])
        parts = []
        if pn is not None and prev is not None:
            parts.append("🟢" if pn <= prev else "🔴")
        parts.append(f"**{price}**")
        if pn is not None and prev is not None and abs(pn - prev) >= 0.01:
            parts.append(f"(was ${prev:.2f})")
        return " ".join(parts) + f" · `{item['asin']}`"

    def _digest_header(self, k, n, total_items):
        now = datetime.now().strftime("%Y-%m-%d %I:%M %p").replace(" 0", " ")
        lines = [
            "# Amazon Stock Watchlist",
            f"Updated: {now}",
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
        return {"embeds": embeds[:10]}

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
            payload = self._digest_v2_payload(chunk, k, n, len(items), prev_prices)
            status, body = await self._post_raw(session, payload)
            if status in (200, 204):
                continue
            if status == 400:
                # Webhook rejected the V2 layout — retry as embeds.
                fb = self._digest_embed_payload(chunk, k, n, len(items), prev_prices)
                status2, body2 = await self._post_raw(session, fb)
                if status2 in (200, 204):
                    continue
                return False, f"Discord digest failed: {status2} {str(body2)[:150]}"
            return False, f"Discord digest failed: {status} {str(body)[:150]}"

        return True, (
            f"Discord stock list sent — {len(items)} item(s)"
            + (f" in {n} parts." if n > 1 else ".")
        )

from __future__ import annotations
import aiohttp
import asyncio
import os
from typing import Dict, Any, Optional, Tuple

class Notifier:
    """
    - kind: "discord" (webhook) or "ntfy".
    - Secrets:
        * Discord: webhook URL only via ENV (default: DISCORD_WEBHOOK_URL).
          -> opt cfg: notifier.discord_webhook_env to change variable name (ex: "MY_WEBHOOK").
          -> (tolerated) notifier.webhook_url if you know what you are doing, BUT not recommended.
        * ntfy: topic/token in clear OK (no critical action).

    - API:
        await notify(title, message)   # asynchrone
        notify_sync(title, message)    # fallback si on n'a pas d'event loop
    """

    DEFAULT_USER_AGENT = "arb-bot/1.2"

    def __init__(
        self,
        cfg: Optional[Dict[str, Any]] = None,
        *,
        kind: Optional[str] = None,
        ntfy_server: Optional[str] = None,
        ntfy_topic: Optional[str] = None,
        ntfy_token: Optional[str] = None,
        discord_webhook_env: Optional[str] = None,
        discord_webhook_url: Optional[str] = None,
        username: Optional[str] = None,
        avatar_url: Optional[str] = None,
    ) -> None:

        base = cfg or {}
        # Allows either a "venues_cfg" dict that contains "notifier", or the already extracted sub-dict.
        if isinstance(base, dict) and "notifier" in base:
            n = base.get("notifier") or {}
        else:
            n = base if isinstance(base, dict) else {}

        # Normalisation
        self.kind = (kind or n.get("kind") or "ntfy").strip().lower()

        # ----- Discord -----
        self.discord_webhook_env = (discord_webhook_env or n.get("discord_webhook_env") or "DISCORD_WEBHOOK_URL").strip()
        # Security principle: read from ENV first, then tolerate explicit YAML field (not recommended).
        self._discord_webhook_url = (
            discord_webhook_url
            or os.environ.get(self.discord_webhook_env, "").strip()
            or (n.get("webhook_url", "").strip() if isinstance(n.get("webhook_url"), str) else "")
        )

        self.username = (username or n.get("username") or "arb-bot").strip()
        self.avatar_url = (avatar_url or n.get("avatar_url") or "").strip()

        # ----- ntfy -----
        self.ntfy_server = (ntfy_server or n.get("ntfy_server") or "https://ntfy.sh").rstrip("/")
        self.ntfy_topic  = (ntfy_topic  or n.get("ntfy_topic")  or "bot_arbitrage_bouix").strip()
        self.ntfy_token  = (ntfy_token  or n.get("ntfy_token")  or "").strip()

        self._base_headers = {"User-Agent": self.DEFAULT_USER_AGENT}
        if self.ntfy_token:
            self._base_headers["Authorization"] = f"Bearer {self.ntfy_token}"

        # Simple anti-flood (optional and light)
        self._last_send_ts: float = 0.0
        self._min_interval_s: float = float(n.get("min_interval_s", 0.0))  # default no throttle

    # --------------- Public ---------------

    async def notify(self, title: str, message: str) -> None:
        """ Async send. Silent on network error. """
        if not message:
            return
        if self._min_interval_s > 0:
            now = asyncio.get_running_loop().time()
            if now - self._last_send_ts < self._min_interval_s:
                return
            self._last_send_ts = now

        try:
            if self.kind == "discord":
                await self._send_discord(title or "Arb Bot", message)
            elif self.kind == "ntfy":
                await self._send_ntfy(title or "Arb Bot", message)
            else:
                # inconnu -> no-op
                return
        except Exception:
            # do not crash strategy on notifier failure
            return

    def notify_sync(self, title: str, message: str) -> None:
        """ Fallback for sync code if no loop is active. """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.notify(title, message))
        else:
            loop.create_task(self.notify(title, message))

    # --------------- Discord ---------------

    async def _send_discord(self, title: str, message: str) -> None:
        if not self._discord_webhook_url:
            # No webhook -> nothing to do
            return

        # Discord limite content à ~2000 chars. On coupe si besoin.
        MAX = 1900
        content = f"**{title}**\n{message}"
        chunks = [content[i:i+MAX] for i in range(0, len(content), MAX)] or ["(vide)"]

        timeout = aiohttp.ClientTimeout(total=8)
        headers = {"User-Agent": self.DEFAULT_USER_AGENT, "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as sess:
            for part in chunks:
                payload = {
                    "content": part,
                    "username": self.username or "arb-bot",
                }
                if self.avatar_url:
                    payload["avatar_url"] = self.avatar_url
                try:
                    async with sess.post(self._discord_webhook_url, json=payload) as r:
                        if r.status in (200, 204):
                            continue
                        # 4xx -> useless to retry
                        if 400 <= r.status < 500:
                            return
                        # 5xx -> light retry
                        await asyncio.sleep(0.6)
                        async with sess.post(self._discord_webhook_url, json=payload) as r2:
                            # whatever the result after retry, return
                            return
                except Exception:
                    # silent network error
                    return

    # --------------- ntfy ---------------

    async def _send_ntfy(self, title: str, message: str) -> None:
        url = f"{self.ntfy_server}/{self.ntfy_topic}"
        data = (message or "").encode("utf-8")
        headers = {
            **self._base_headers,
            "Title": title or "Arb Bot",
            "X-Title": title or "Arb Bot",
            "Content-Type": "text/plain; charset=utf-8",
        }
        backoff = 0.6
        for _ in range(3):
            try:
                timeout = aiohttp.ClientTimeout(total=7)
                async with aiohttp.ClientSession(timeout=timeout) as sess:
                    async with sess.post(url, data=data, headers=headers) as r:
                        if 200 <= r.status < 300:
                            return
                        if r.status in (401, 403):
                            return
            except Exception:
                pass
            await asyncio.sleep(backoff)
            backoff = min(4.0, backoff * 1.8)

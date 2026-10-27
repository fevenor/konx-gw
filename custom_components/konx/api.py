"""HTTP/WS client for the konx-gw REST API (aiohttp only, no extra deps)."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

import aiohttp

from .const import API_STATE, WS_PATH

_LOGGER = logging.getLogger(__name__)


class KonxClient:
    """Small async client with reconnecting websocket."""

    def __init__(self, host: str, port: int, token: str) -> None:
        self.base = f"http://{host}:{port}"
        self.headers = {"Authorization": f"Bearer {token}"}
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def state(self) -> dict[str, Any]:
        session = await self._ensure_session()
        async with session.get(self.base + API_STATE, headers=self.headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def snapshot_jpeg(self, device_id: str, stream: str = "main") -> bytes | None:
        session = await self._ensure_session()
        try:
            async with session.get(
                self.base + f"/api/devices/{device_id}/snapshot",
                headers=self.headers,
                params={"stream": stream},
                timeout=aiohttp.ClientTimeout(total=25),
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.read()
        except aiohttp.ClientError:
            return None

    async def talk_start(self, device_id: str) -> bool:
        session = await self._ensure_session()
        async with session.post(
            self.base + f"/api/devices/{device_id}/talk/start", headers=self.headers
        ) as resp:
            return resp.status == 200

    async def talk_stop(self, device_id: str) -> None:
        session = await self._ensure_session()
        try:
            async with session.post(
                self.base + f"/api/devices/{device_id}/talk/stop", headers=self.headers
            ):
                pass
        except aiohttp.ClientError:
            pass

    async def talk_audio(self, device_id: str, pcm16_8k: bytes) -> bool:
        session = await self._ensure_session()
        async with session.post(
            self.base + f"/api/devices/{device_id}/talk/audio",
            headers={**self.headers, "Content-Type": "audio/L16"},
            data=pcm16_8k,
        ) as resp:
            return resp.status == 200

    async def listen_events(self, on_event: Callable[[dict], None]) -> None:
        """Long-lived websocket with exponential backoff reconnect."""
        backoff = 2
        while True:
            try:
                session = await self._ensure_session()
                ws_url = self.base.replace("http", "ws") + WS_PATH
                async with session.ws_connect(ws_url, headers=self.headers) as ws:
                    _LOGGER.debug("konx-gw websocket connected")
                    backoff = 2
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                on_event(json.loads(msg.data))
                            except ValueError:
                                continue
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except aiohttp.ClientError as err:
                _LOGGER.debug("konx-gw ws error: %s", err)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

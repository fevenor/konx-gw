"""DataUpdateCoordinator for the konx gateway."""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import timedelta
from typing import Any

from homeassistant.const import CONF_HOST, CONF_PORT, CONF_TOKEN
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import KonxClient
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = 30

# 去重窗口容量：REST 补拉与 WS 实时可能重叠送达同一事件，
# 按 id 去重且只保留最近若干条，防止长运行内存无界增长。
_SEEN_WINDOW = 512


class KonxCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Pull /api/state; realtime events arrive via the websocket.

    事件可靠性：WS 断线期间的事件经两条通道补齐——
    1. WS 重连带 ?since=<游标>，网关回放缺漏区间；
    2. 30s 轮询兜底从 REST /api/events?since= 补拉（覆盖重连失败场景）。
    游标为最后收到的事件 id，与 REST /api/events 的 since 同语义。
    """

    def __init__(self, hass: HomeAssistant, entry_data: dict) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=SCAN_INTERVAL),
        )
        self.client = KonxClient(
            entry_data[CONF_HOST], entry_data[CONF_PORT], entry_data[CONF_TOKEN]
        )
        self.ws_handlers: list = []
        self._ws_task: asyncio.Task | None = None
        # 事件补拉游标（最后收到的事件 id；HA 重启后从 0 重新对齐）
        self.last_event_id: int = 0
        # 已路由事件 id 的有界去重集合
        self._seen_ids: set[int] = set()
        self._seen_order: deque[int] = deque(maxlen=_SEEN_WINDOW)

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            state = await self.client.state()
        except Exception as err:  # noqa: BLE001
            raise UpdateFailed(f"gateway unreachable: {err}") from err
        # WS 断线期间的事件补拉兜底：失败不影响状态轮询本身
        await self._poll_missed_events()
        return state

    async def _poll_missed_events(self) -> None:
        """轮询兜底：从 REST /api/events 补齐游标之后的事件。

        服务端返回最新在前，倒序（旧→新）逐条走同一路由管线；
        与 WS 实时流重叠的事件按 id 去重。
        """
        try:
            missed = await self.client.events(since=self.last_event_id, limit=200)
        except Exception:  # noqa: BLE001
            _LOGGER.debug("事件补拉失败（等待下次轮询重试）")
            return
        for ev in reversed(missed):
            self.on_ws_message({"type": "event.new", "data": ev})

    def _advance_cursor(self, ev: dict) -> None:
        try:
            eid = int(ev.get("id", 0))
        except (TypeError, ValueError):
            return
        if eid > self.last_event_id:
            self.last_event_id = eid

    def _mark_seen(self, ev: dict) -> bool:
        """记录事件 id；返回 False 表示重复事件应跳过。"""
        try:
            eid = int(ev.get("id", 0))
        except (TypeError, ValueError):
            return True  # 无 id 的事件不去重，按新事件处理
        if eid in self._seen_ids:
            return False
        # 窗口满时先显式淘汰最老 id，再插入，保证 deque 与 set 严格同步
        if len(self._seen_order) >= _SEEN_WINDOW:
            self._seen_ids.discard(self._seen_order.popleft())
        self._seen_ids.add(eid)
        self._seen_order.append(eid)
        return True

    def on_ws_message(self, msg: dict) -> None:
        if msg.get("type") != "event.new":
            for handler in self.ws_handlers:
                handler(msg)
            return
        ev = msg.get("data", {})
        if not self._mark_seen(ev):
            return  # REST 补拉与 WS 实时重叠，已路由过
        self._advance_cursor(ev)
        for handler in self.ws_handlers:
            handler(msg)

    async def start_ws(self) -> None:
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = self.hass.async_create_task(
                self.client.listen_events(self.on_ws_message, since=lambda: self.last_event_id)
            )

    async def stop(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        await self.client.close()

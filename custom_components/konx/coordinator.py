"""DataUpdateCoordinator for the konx gateway."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from homeassistant.const import CONF_HOST, CONF_PORT, CONF_TOKEN
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import KonxClient
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = 30


class KonxCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Pull /api/state; realtime events arrive via the websocket."""

    def __init__(self, hass: HomeAssistant, entry_data: dict) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=asyncio.timedelta(seconds=SCAN_INTERVAL),
        )
        self.client = KonxClient(
            entry_data[CONF_HOST], entry_data[CONF_PORT], entry_data[CONF_TOKEN]
        )
        self.ws_handlers: list = []
        self._ws_task: asyncio.Task | None = None

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            return await self.client.state()
        except Exception as err:  # noqa: BLE001
            raise UpdateFailed(f"gateway unreachable: {err}") from err

    def on_ws_message(self, msg: dict) -> None:
        for handler in self.ws_handlers:
            handler(msg)

    async def start_ws(self) -> None:
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = self.hass.async_create_task(
                self.client.listen_events(self.on_ws_message)
            )

    async def stop(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        await self.client.close()

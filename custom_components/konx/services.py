"""Services for the konx integration."""
from __future__ import annotations

import logging

import voluptuous as vol

from homeassistant.const import CONF_DEVICE_ID
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

ANNOUNCE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_DEVICE_ID): cv.string,
        vol.Optional("text"): cv.string,
        vol.Optional("url"): cv.string,
    }
)


async def async_setup_services(hass: HomeAssistant) -> None:
    async def handle_announce(call: ServiceCall) -> None:
        device_id = call.data[CONF_DEVICE_ID]
        entry_ids = list(hass.data.get(DOMAIN, {}).keys())
        if not entry_ids:
            raise HomeAssistantError("konx gateway not configured")
        from .coordinator import KonxCoordinator

        coord: KonxCoordinator = hass.data[DOMAIN][entry_ids[0]]
        ok = await coord.client.talk_start(device_id)
        if not ok:
            raise HomeAssistantError(
                "对讲不可用：网关尚在等待 p2pv3 设备会话模块（M5）；或会话被占用"
            )
        try:
            if call.data.get("url"):
                import aiohttp

                async with aiohttp.ClientSession() as session:
                    async with session.get(call.data["url"]) as resp:
                        data = await resp.read()
                # WAV: strip 44-byte header
                if data[:4] == b"RIFF":
                    data = data[44:]
                await coord.client.talk_audio(device_id, data)
            elif call.data.get("text"):
                raise HomeAssistantError(
                    "text→语音 需在自动化中先用 TTS 生成 WAV，再通过 url 传入"
                )
        finally:
            await coord.client.talk_stop(device_id)

    hass.services.async_register(DOMAIN, "announce", handle_announce, schema=ANNOUNCE_SCHEMA)

"""Services for the konx integration."""
from __future__ import annotations

import logging

import voluptuous as vol

from homeassistant.const import CONF_DEVICE_ID, CONF_HOST
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

ANNOUNCE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_DEVICE_ID): cv.string,
        vol.Optional("text"): cv.string,
        vol.Optional("url"): cv.string,
        vol.Optional("gateway"): cv.string,
    }
)


async def async_setup_services(hass: HomeAssistant) -> None:
    async def handle_announce(call: ServiceCall) -> None:
        device_id = call.data[CONF_DEVICE_ID]
        coord = _pick_coordinator(hass, device_id, call.data.get("gateway"))
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
                # WAV 头的剥离由网关侧完成（RIFF/WAVE chunk 遍历定位
                # data 块，兼容非 44 字节头的容器）；客户端不再自行硬剥
                # 44 字节，避免把带附加块（LIST/_fact）WAV 的有效数据
                # 一起切掉。
                await coord.client.talk_audio(device_id, data)
            elif call.data.get("text"):
                raise HomeAssistantError(
                    "text→语音 需在自动化中先用 TTS 生成 WAV，再通过 url 传入"
                )
        finally:
            await coord.client.talk_stop(device_id)

    hass.services.async_register(DOMAIN, "announce", handle_announce, schema=ANNOUNCE_SCHEMA)


def _pick_coordinator(hass: HomeAssistant, device_id: str, gateway: str | None):
    """选择承载该设备的网关协调器（多网关部署）。

    选择顺序（最小改动方案，M8）：
    1. 显式指定 gateway（HA 配置项 entry ID 或主机地址）→ 精确匹配；
    2. 未指定 gateway → 按 device_id 归属自动匹配（遍历各网关的设备
       列表）；
    3. 都匹配不到 → 回退第一个配置的网关（保持单网关部署的旧行为）。
    """
    data = hass.data.get(DOMAIN, {})
    if not data:
        raise HomeAssistantError("konx gateway not configured")
    fallback = None
    for entry_id, coord in data.items():
        if fallback is None:
            fallback = coord
        if gateway:
            entry = hass.config_entries.async_get_entry(entry_id)
            if entry and gateway in (entry_id, entry.data.get(CONF_HOST)):
                return coord
            continue
        for dev in (coord.data or {}).get("devices", []):
            if dev.get("id") == device_id:
                return coord
    if gateway and fallback is not None:
        raise HomeAssistantError(f"未找到网关 {gateway}，请检查 gateway 参数")
    return fallback

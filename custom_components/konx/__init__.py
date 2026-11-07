"""The Konx gateway integration."""
from __future__ import annotations

import logging

from homeassistant.const import CONF_HOST, CONF_PORT, CONF_TOKEN
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType

from .const import DOMAIN
from .coordinator import KonxCoordinator
from .services import async_setup_services

_LOGGER = logging.getLogger(__name__)

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

PLATFORMS = ["binary_sensor", "camera", "event", "sensor"]



async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    hass.data.setdefault(DOMAIN, {})
    await async_setup_services(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    coord = KonxCoordinator(hass, entry.data)
    try:
        await coord.async_config_entry_first_refresh()
        hass.data[DOMAIN][entry.entry_id] = coord
        # 先注册实体的事件处理器，再接收实时消息。
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
        await coord.start_ws()
    except Exception:
        await coord.stop()
        hass.data[DOMAIN].pop(entry.entry_id, None)
        raise
    entry.async_on_unload(entry.add_update_listener(_async_reload))
    return True


async def _async_reload(hass, entry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry) -> bool:
    coord: KonxCoordinator | None = hass.data[DOMAIN].get(entry.entry_id)
    unloaded = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unloaded:
        if coord:
            await coord.stop()
        hass.data[DOMAIN].pop(entry.entry_id)
    return unloaded

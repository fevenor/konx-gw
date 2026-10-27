"""Doorbell event entities."""
from __future__ import annotations

import logging

from homeassistant.components.event import EventEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import ATTR_MSG_ID, DOMAIN
from .coordinator import KonxCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    coord: KonxCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities = [KonxDoorbellEvent(coord, dev) for dev in coord.data.get("devices", [])]
    async_add_entities(entities)
    coord.ws_handlers.append(coord_ws_router(entities))


def coord_ws_router(entities):
    @callback
    def _route(msg: dict) -> None:
        if msg.get("type") != "event.new":
            return
        ev = msg.get("data", {})
        for ent in entities:
            if ent.matches(ev):
                ent.fire(ev)
    return _route


class KonxDoorbellEvent(EventEntity):
    """Fires on doorbell.press events from the gateway."""

    _attr_event_types = ["press"]

    def __init__(self, coord: KonxCoordinator, dev: dict) -> None:
        self._coord = coord
        self._dev = dev
        self._attr_unique_id = f"konx_{dev['id']}_doorbell"
        self._attr_name = f"{dev.get('name', dev['id'])} 门铃"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"konx_{dev['id']}")},
            name=dev.get("name", dev["id"]),
            manufacturer="Konx",
            model="KW05 via konx-gw",
        )

    def matches(self, ev: dict) -> bool:
        return ev.get("type") == "doorbell.press" and ev.get("device_id") == self._dev["id"]

    def fire(self, ev: dict) -> None:
        self._trigger_event(
            "press",
            {ATTR_MSG_ID: ev.get(ATTR_MSG_ID), "ts": ev.get("ts_recv")},
        )
        self.async_write_ha_state()

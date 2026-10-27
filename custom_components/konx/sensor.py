"""Sensors: connected clients per kind, last doorbell time."""
from __future__ import annotations

from datetime import datetime

from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import KonxCoordinator


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    coord: KonxCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([KonxClientsSensor(coord, entry)] + [
        KonxLastDoorbell(coord, dev) for dev in coord.data.get("devices", [])
    ])
    coord.ws_handlers.append(_make_client_refresh(coord))


def _make_client_refresh(coord):
    @callback
    def _refresh(msg):
        if msg.get("type") == "client.changed":
            coord.async_set_updated_data(coord.data)
    return _refresh


class KonxClientsSensor(CoordinatorEntity[KonxCoordinator], SensorEntity):
    def __init__(self, coord, entry) -> None:
        super().__init__(coord)
        self._attr_unique_id = "konx_gw_clients"
        self._attr_name = "网关客户端数"
        self._attr_native_unit_of_measurement = "clients"
        self._attr_state_class = "measurement"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"konx-gw-{entry.data.get('host', 'gw')}")},
            name="Konx Gateway",
            manufacturer="Konx",
            model="konx-gw",
        )

    @property
    def native_value(self) -> int:
        clients = (self.coordinator.data or {}).get("clients", {})
        return sum(clients.values())

    @property
    def extra_state_attributes(self):
        return (self.coordinator.data or {}).get("clients", {})


class KonxLastDoorbell(CoordinatorEntity[KonxCoordinator], SensorEntity):
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, coord, dev: dict) -> None:
        super().__init__(coord)
        self._dev = dev
        self._last: datetime | None = None
        self._attr_unique_id = f"konx_{dev['id']}_last_doorbell"
        self._attr_name = f"{dev.get('name', dev['id'])} 最后门铃"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"konx_{dev['id']}")},
            name=dev.get("name", dev["id"]),
            manufacturer="Konx",
            model="KW05 via konx-gw",
        )
        coord.ws_handlers.append(self._on_ws)

    @callback
    def _on_ws(self, msg):
        ev = msg.get("data", {})
        if msg.get("type") == "event.new" and ev.get("type") == "doorbell.press" and ev.get("device_id") == self._dev["id"]:
            self._last = datetime.fromtimestamp(ev.get("ts_recv", 0))
            self.async_write_ha_state()

    @property
    def native_value(self) -> datetime | None:
        return self._last

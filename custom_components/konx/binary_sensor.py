"""Binary sensors: device online, cloud session, p2p session."""
from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
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
    entities: list[BinarySensorEntity] = [
        KonxGatewaySensor(coord, entry, "cloud", "云登录", BinarySensorDeviceClass.CONNECTIVITY),
        KonxGatewaySensor(coord, entry, "p2p", "推送会话", BinarySensorDeviceClass.CONNECTIVITY),
    ]
    for dev in coord.data.get("devices", []):
        entities.append(KonxDeviceOnline(coord, dev))
    async_add_entities(entities)


class KonxGatewaySensor(CoordinatorEntity[KonxCoordinator], BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY

    def __init__(self, coord, entry, key: str, name: str, dc) -> None:
        super().__init__(coord)
        self._key = key
        self._attr_unique_id = f"konx_gw_{key}"
        self._attr_name = f"网关 {name}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"konx-gw-{entry.data.get('host', 'gw')}")},
            name="Konx Gateway",
            manufacturer="Konx",
            model="konx-gw",
        )

    @property
    def is_on(self) -> bool:
        state = self.coordinator.data or {}
        if self._key == "cloud":
            return state.get("cloud") == "up"
        return state.get("p2p") == "ready"

    @property
    def extra_state_attributes(self):
        state = self.coordinator.data or {}
        if self._key == "p2p":
            return {"server": state.get("p2p_server"), "rtt_ms": state.get("p2p_rtt_ms")}
        return {}


class KonxDeviceOnline(CoordinatorEntity[KonxCoordinator], BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY

    def __init__(self, coord, dev: dict) -> None:
        super().__init__(coord)
        self._dev = dev
        self._attr_unique_id = f"konx_{dev['id']}_online"
        self._attr_name = f"{dev.get('name', dev['id'])} 在线"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"konx_{dev['id']}")},
            name=dev.get("name", dev["id"]),
            manufacturer="Konx",
            model="KW05 via konx-gw",
        )

    @property
    def is_on(self) -> bool:
        for dev in (self.coordinator.data or {}).get("devices", []):
            if dev["id"] == self._dev["id"]:
                return bool(dev.get("online"))
        return False

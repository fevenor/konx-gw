"""Camera entities backed by the gateway RTSP egress (AAC pipeline)."""
from __future__ import annotations

import asyncio
import logging
from urllib.parse import quote

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.const import CONF_HOST
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    CONF_RTSP_PASSWORD,
    CONF_RTSP_PORT,
    CONF_RTSP_USER,
    DEFAULT_RTSP_PORT,
    DEFAULT_RTSP_USER,
    DOMAIN,
)
from .coordinator import KonxCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant, entry, async_add_entities: AddEntitiesCallback
) -> None:
    coord: KonxCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities = []
    for dev in coord.data.get("devices", []):
        entities.append(KonxCamera(coord, entry, dev))
    async_add_entities(entities)


class KonxCamera(Camera):
    """One camera per gateway device; stream via HA stream component."""

    _attr_supported_features = CameraEntityFeature.STREAM
    _attr_brand = "Konx"

    def __init__(self, coord: KonxCoordinator, entry, dev: dict) -> None:
        super().__init__()
        self._coord = coord
        self._dev = dev
        host = entry.data[CONF_HOST]
        rtsp_port = entry.data.get(CONF_RTSP_PORT, DEFAULT_RTSP_PORT)
        # RTSP 出口强制 HTTP Basic 鉴权（用户默认 nvr，密码见网关首次启动
        # 日志）：凭据以内嵌 user:pass 形态写入 URL，特殊字符需 URL 编码。
        # RTSP 出口监听独立端口（默认 8554），与 API 端口无关。
        # ?audio=aac 为音频转码提示后缀（网关侧忽略不影响取流）。
        user = quote(str(entry.data.get(CONF_RTSP_USER, DEFAULT_RTSP_USER)), safe="")
        password = quote(str(entry.data.get(CONF_RTSP_PASSWORD, "")), safe="")
        self._rtsp = (
            f"rtsp://{user}:{password}@{host}:{rtsp_port}"
            f"/konx/{dev['id']}/main?audio=aac"
        )
        self._attr_unique_id = f"konx_{dev['id']}_camera"
        self._attr_name = f"{dev.get('name', dev['id'])} 摄像头"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"konx_{dev['id']}")},
            name=dev.get("name", dev["id"]),
            manufacturer="Konx",
            model="KW05 via konx-gw",
            via_device=(DOMAIN, f"konx-gw-{entry.data[CONF_HOST]}"),
        )

    @property
    def stream_source(self) -> str | None:
        return self._rtsp

    async def async_camera_image(self, width: int | None = None, height: int | None = None) -> bytes | None:
        return await self._coord.client.snapshot_jpeg(self._dev["id"])

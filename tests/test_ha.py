"""以最小 HA 接口替身测试集成契约；HTTP 客户端使用真实 aiohttp 类型。"""
import asyncio
import importlib
import json
import sys
import types
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, Mock, patch

import aiohttp


def module(name, **attrs):
    value = types.ModuleType(name)
    value.__dict__.update(attrs)
    sys.modules[name] = value
    return value


class Coordinator:
    def __class_getitem__(cls, item):
        return cls

    def __init__(self, hass, logger, *, name, update_interval):
        self.hass = hass
        self.update_interval = update_interval


# 仅提供被测集成声明的 HA 接口，错误的方法名不会被通配 Mock 接受。
module("homeassistant")
module("homeassistant.const", CONF_HOST="host", CONF_PORT="port", CONF_TOKEN="token")
module("homeassistant.core", HomeAssistant=object)
module("homeassistant.helpers")
module("homeassistant.helpers.config_validation", config_entry_only_config_schema=lambda domain: {})
module("homeassistant.helpers.typing", ConfigType=dict)
module("homeassistant.helpers.update_coordinator", DataUpdateCoordinator=Coordinator, UpdateFailed=RuntimeError)
module("homeassistant.helpers.device_registry", DeviceInfo=dict)
module("homeassistant.helpers.entity_platform", AddEntitiesCallback=object)
module("homeassistant.components")
module("homeassistant.components.camera", Camera=object, CameraEntityFeature=types.SimpleNamespace(STREAM=1))
module("custom_components.konx.services", async_setup_services=AsyncMock())

integration = importlib.import_module("custom_components.konx")
api = importlib.import_module("custom_components.konx.api")
camera = importlib.import_module("custom_components.konx.camera")
coord_module = importlib.import_module("custom_components.konx.coordinator")


class FakeWS:
    def __init__(self, events):
        self.events = iter(events)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            event = next(self.events)
        except StopIteration:
            raise StopAsyncIteration
        return types.SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(event))


class TestHA(unittest.IsolatedAsyncioTestCase):
    def coordinator(self):
        return coord_module.KonxCoordinator(object(), {"host": "demo.invalid", "port": 8080, "token": "demo"})

    async def test_coordinator_and_camera(self):
        coord = self.coordinator()
        self.assertEqual(coord.update_interval, timedelta(seconds=30))
        entry = types.SimpleNamespace(data={"host": "demo.invalid", "rtsp_password": "demo"})
        entity = camera.KonxCamera(coord, entry, {"id": "10000001"})
        self.assertIn("/konx/10000001/main", await entity.stream_source())

    async def test_rest_events_and_empty_database(self):
        client = api.KonxClient("demo.invalid", 8080, "demo")
        response = types.SimpleNamespace(raise_for_status=Mock(), json=AsyncMock(return_value={"events": None}))
        context = AsyncMock()
        context.__aenter__.return_value = response
        session = types.SimpleNamespace(closed=False, get=Mock(return_value=context))
        client._session = session
        self.assertEqual(await client.events(since=12), [])
        self.assertEqual(session.get.call_args.args[0], "http://demo.invalid:8080/api/events")
        self.assertEqual(session.get.call_args.kwargs["params"]["since"], "12")

    async def test_simulated_doorbell_poll_and_ws_dedup(self):
        coord = self.coordinator()
        delivered = []
        coord.ws_handlers.append(delivered.append)
        event = {"id": 7, "type": "doorbell.press", "device_id": "10000001"}
        coord.client.events = AsyncMock(return_value=[event])
        await coord._poll_missed_events()
        coord.on_ws_message({"type": "event.new", "data": event})
        self.assertEqual(len(delivered), 1)
        self.assertEqual(coord.last_event_id, 7)
        await coord._poll_missed_events()
        self.assertEqual(len(delivered), 1)
        coord.client.events.assert_awaited_with(since=7, limit=200)

    async def test_ws_reconnect_uses_received_cursor(self):
        client = api.KonxClient("demo.invalid", 8080, "demo")
        event = {"type": "event.new", "data": {"id": 501}}
        session = types.SimpleNamespace(closed=False, ws_connect=Mock(side_effect=[FakeWS([event]), asyncio.CancelledError()]))
        client._session = session
        with patch.object(api.asyncio, "sleep", new=AsyncMock()):
            with self.assertRaises(asyncio.CancelledError):
                await client.listen_events(lambda event: None, since=10)
        self.assertTrue(session.ws_connect.call_args_list[1].args[0].endswith("?since=501"))

    async def test_ws_reconnect_uses_poll_cursor(self):
        client = api.KonxClient("demo.invalid", 8080, "demo")
        cursor = [10]
        def polling_finished(event):
            cursor[0] = 900
        session = types.SimpleNamespace(closed=False, ws_connect=Mock(side_effect=[FakeWS([{"type": "state.changed"}]), asyncio.CancelledError()]))
        client._session = session
        with patch.object(api.asyncio, "sleep", new=AsyncMock()):
            with self.assertRaises(asyncio.CancelledError):
                await client.listen_events(polling_finished, since=lambda: cursor[0])
        self.assertTrue(session.ws_connect.call_args_list[1].args[0].endswith("?since=900"))

    async def test_setup_registers_entities_before_ws(self):
        calls = []
        coord = types.SimpleNamespace(
            async_config_entry_first_refresh=AsyncMock(side_effect=lambda: calls.append("refresh")),
            start_ws=AsyncMock(side_effect=lambda: calls.append("ws")), stop=AsyncMock())
        hass = types.SimpleNamespace(data={"konx": {}}, config_entries=types.SimpleNamespace(
            async_forward_entry_setups=AsyncMock(side_effect=lambda *args: calls.append("platforms"))))
        entry = types.SimpleNamespace(data={}, entry_id="demo", async_on_unload=Mock(), add_update_listener=Mock())
        with patch.object(integration, "KonxCoordinator", return_value=coord):
            self.assertTrue(await integration.async_setup_entry(hass, entry))
        self.assertEqual(calls, ["refresh", "platforms", "ws"])

    async def test_setup_failure_closes_client(self):
        coord = types.SimpleNamespace(async_config_entry_first_refresh=AsyncMock(side_effect=RuntimeError("离线")), stop=AsyncMock())
        hass = types.SimpleNamespace(data={"konx": {}})
        entry = types.SimpleNamespace(data={}, entry_id="demo")
        with patch.object(integration, "KonxCoordinator", return_value=coord):
            with self.assertRaises(RuntimeError):
                await integration.async_setup_entry(hass, entry)
        coord.stop.assert_awaited_once()
        self.assertNotIn("demo", hass.data["konx"])


if __name__ == "__main__":
    unittest.main()

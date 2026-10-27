"""Config flow for the Konx gateway."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_HOST, CONF_PORT, CONF_TOKEN
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError

from .const import (
    CONF_RTSP_PASSWORD,
    CONF_RTSP_PORT,
    CONF_RTSP_USER,
    DEFAULT_PORT,
    DEFAULT_RTSP_PORT,
    DEFAULT_RTSP_USER,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

_PORT = vol.All(vol.Coerce(int), vol.Range(min=1, max=65535))

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): str,
        vol.Optional(CONF_PORT, default=DEFAULT_PORT): _PORT,
        # RTSP 出口运行在独立端口（:8554），与 API 端口无关
        vol.Optional(CONF_RTSP_PORT, default=DEFAULT_RTSP_PORT): _PORT,
        # RTSP 出口 HTTP Basic 凭据（默认用户 nvr，密码见网关首次启动日志）
        vol.Optional(CONF_RTSP_USER, default=DEFAULT_RTSP_USER): str,
        vol.Required(CONF_RTSP_PASSWORD): str,
        vol.Required(CONF_TOKEN): str,
    }
)


async def _validate(hass: HomeAssistant, host: str, port: int, token: str) -> dict:
    """Probe the gateway /api/state."""
    url = f"http://{host}:{port}/api/state"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status == 401:
                    raise InvalidAuth
                if resp.status != 200:
                    raise CannotConnect(f"status {resp.status}")
                return await resp.json()
    except asyncio.TimeoutError as err:
        raise CannotConnect("timeout") from err
    except aiohttp.ClientError as err:
        raise CannotConnect(str(err)) from err


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the konx gateway config flow."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if user_input is not None:
            try:
                await _validate(
                    self.hass,
                    user_input[CONF_HOST],
                    user_input[CONF_PORT],
                    user_input[CONF_TOKEN],
                )
            except CannotConnect as err:
                return self.async_show_form(
                    step_id="user",
                    data_schema=STEP_USER_SCHEMA,
                    errors={"base": "cannot_connect"},
                    description_placeholders={"detail": str(err)},
                )
            except InvalidAuth:
                return self.async_show_form(
                    step_id="user", data_schema=STEP_USER_SCHEMA, errors={"base": "invalid_auth"}
                )
            # unique_id 只锚定网关地址：不含版本号，避免网关升级后
            # 同一实例被识别成新网关（重复建条目）。
            await self.async_set_unique_id(f"konx-gw-{user_input[CONF_HOST]}")
            self._abort_if_unique_id_configured()
            return self.async_create_entry(
                title=f"Konx Gateway ({user_input[CONF_HOST]})",
                data=user_input,
            )
        return self.async_show_form(step_id="user", data_schema=STEP_USER_SCHEMA)

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return OptionsFlowHandler()


class OptionsFlowHandler(config_entries.OptionsFlow):
    """Options are informational; the gateway owns the configuration."""

    async def async_step_init(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        return self.async_show_form(step_id="init", data_schema=vol.Schema({}))


class CannotConnect(HomeAssistantError):
    """Cannot connect to the gateway."""


class InvalidAuth(HomeAssistantError):
    """Invalid token."""

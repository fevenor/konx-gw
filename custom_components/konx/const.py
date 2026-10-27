"""Constants for the Konx gateway integration."""
DOMAIN = "konx"

CONF_HOST = "host"
CONF_PORT = "port"
CONF_TOKEN = "token"
CONF_RTSP_PORT = "rtsp_port"
# RTSP 出口走 HTTP Basic 鉴权（realm=konx-gw）：默认用户 nvr，
# 密码在网关首次启动时生成并打印一次（存 kv 表 nvr_password）。
CONF_RTSP_USER = "rtsp_user"
CONF_RTSP_PASSWORD = "rtsp_password"

DEFAULT_PORT = 8080
DEFAULT_RTSP_PORT = 8554
DEFAULT_RTSP_USER = "nvr"

API_STATE = "/api/state"
API_EVENTS = "/api/events"
WS_PATH = "/api/ws"

ATTR_DEVICE_ID = "device_id"
ATTR_MSG_ID = "msg_id"

EVENT_DOORBELL_PRESS = f"{DOMAIN}_doorbell_press"

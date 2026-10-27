# konx-gw —— konx 门铃本地网关

将 **Konx KW05 可视门铃**接入家庭本地网络的网关服务（Go 实现）：维持云端登录与服务器推送长连接，接收门铃事件；并通过 **P2P v2 设备会话直接取流**，以标准协议（RTSP / ONVIF / REST+WS / MQTT）向 **Home Assistant**、NVR、Frigate、Blue Iris 等任意本地客户端再分发——无需云端中转、无需官方 App 常驻。

协议适配基于官方开源仓库 [GWP2PSDK](https://github.com/GWTimes/GWP2PSDK) 与 [GwellDemo](https://github.com/jwkj/GwellDemo) 提供的 SDK 与示例工程二次开发。

```text
                    云端（konx 云平台）                             家庭内网
  ┌──────────────────────────────────────┐      ┌──────────────── 容器宿主/主机 ────────────────┐
  │ api1.cloudlinks.cn:443   云登录        │      │              konx-gw (Go)                    │
  │ 列表服务器 :51701/tcp+udp 设备列表      │◀────▶│  cloud   登录/重登/LoadDeviceInfo 设备列表    │
  │ P2P 服务器 :udp 动态高位端口            │◀────▶│  p2p     gute 帧/RC5/推送会话/AV 呼叫取流    │
  └──────────────────────────────────────┘      │  events  告警解码(13=门铃)/去重/SQLite/总线   │
                                                │  media   p2p-av 源管理/H264 RTP/快照/转码     │
        ┌─────────────────────────┐             │  rtspout :8554 出口（HTTP Basic，用户 nvr）  │
        │ 门铃 KW05 ◀═ P2P v2 呼叫═┼════════════▶│  onvif   :8000 Device/Media/Events          │
        │ （H264 主码流，无需 NVR） │             │  api+web :8080 REST/WS/内嵌管理页           │
        └─────────────────────────┘             │  mqttx   可选 MQTT 发布 + HA discovery      │
                                                └──────┬──────────────┬──────────────┬────────┘
                                                       ▼              ▼              ▼
                                              Home Assistant    Frigate/Blue    MQTT broker
                                              custom_components  Iris/NVR       (可选)
                                              /konx (本仓库)     (RTSP/ONVIF)
```

- 设计文档：[docs/DESIGN.md](docs/DESIGN.md)
- 部署文档（含 systemd/Docker）：[deploy/README.md](deploy/README.md)
- P2P 协议技术档案另行内部维护

## 功能矩阵

| 功能 | 状态 | 说明 |
|---|---|---|
| 云登录 + 推送长连接 | ✅ | RC5/gute 帧字节级实现（与官方 SDK 参考实现一致）；断线重连、被踢重登、心跳保活 |
| 设备列表 | ✅ | `LoadDeviceInfo`（管道串解析：设备 ID、`deviceCate=501` 即门铃、secretKey、备注名）；失败/空时自动降级旧端点 `SearchBindDev` |
| 门铃事件 | ✅ | 服务器推送 `ALARM_MSG_FROM_SRV`：`alarm_type=13` = 门铃按键（2=移动侦测、14=紧急按钮）；u64 MsgID 滑窗去重；SQLite 落库（原始载荷保真留档） |
| P2P v2 取流（`media.source=p2p-av`） | ✅ **最终方案** | 经 P2P 服务器撮合直连设备取 H264 流（真机实测验证）；treated 密码凭据自动解析（`LoadDeviceInfo → secretKey → gwpass 解密`），失败可 `media.p2p.treated_password` 手动兜底 |
| RTSP 出口 | ✅ | `:8554`，路径 `/konx/{设备ID}/main`（`/sub` 请求自动回退 main——设备仅一路主码流）；HTTP Basic 鉴权（默认用户 `nvr`）；DESCRIBE 早到等待与懒预热 |
| ONVIF 服务 | ✅ | `:8000` Device/Media/Events(PullPoint)，WS-UsernameToken 鉴权，门铃主题 `tns1:Doorbell/Stimulus` |
| Web 管理 + REST/WS API | ✅ | `:8080` 状态/事件/实时画面(MJPEG)/客户端视图，内嵌无 CDN；Bearer Token（read/talk/admin scope） |
| Home Assistant 集成 | ✅ | `custom_components/konx`：摄像头、门铃事件实体、在线/会话传感器，WS 即时推送 + 轮询兜底 |
| MQTT | ✅ 可选 | 状态/事件发布 + HA discovery（未配 broker 自动关闭） |
| 对讲（网关→门铃放音） | ⏳ | 会话模型与 G.711 编码就绪，设备侧 Sink 待实现；当前 API 明确返回 501 |

## 快速开始

### 方式一：Docker Compose（推荐）

```bash
cd deploy
cp config.example.yaml config.yaml       # 编辑：devices 设备列表、advertise.host、media.source
export KONX_CLOUD_USER="<账号>"           # 云端账号（即 App 账号）
export KONX_CLOUD_PASS_MD5=$(printf '%s' '<密码>' | md5sum | cut -d' ' -f1)
docker compose up -d --build
```

> 也可直接 `export KONX_CLOUD_PASS='<明文密码>'`，网关仅在内存中转 MD5，不落盘。

**首次启动日志会打印两个一次性凭据，请立即保存：**

1. Web 管理密码（admin 登录 `http://<主机>:8080/`）
2. RTSP/ONVIF 用户 `nvr` 的密码（拉流与 HA 配置都要用）

### 方式二：裸机 / systemd

```bash
go build -o konx-gw ./cmd/konx-gw        # 需要 Go 1.26+；快照/HLS/MJPEG 需主机安装 ffmpeg
```

systemd 单元示例与逐行注释见 [deploy/README.md](deploy/README.md#裸机-systemd-部署)。

### 最小可用配置

```yaml
gw_id: front-gate
devices:
  - id: "<设备ID>"         # 设备 ID（数字，见 App 或 LoadDeviceInfo）
    name: 前门门铃
media:
  source: p2p-av          # 最终方案：P2P 直连设备取流
advertise:
  host: 192.168.1.10      # 宿主机 IP（HA/NVR 从这里拉流）
```

云端凭据经环境变量注入（见上文），不出现在配置文件中。

## 配置项总表

以下与 `internal/config` 实现逐一对应（默认值均为代码内置缺省）。

### 顶层

| 键 | 默认 | 说明 |
|---|---|---|
| `gw_id` | `front-gate` | 实例标识（MQTT/发现前缀用） |

### cloud —— 云端会话

| 键 | 默认 | 说明 |
|---|---|---|
| `cloud.user` | — | 云端账号（建议用环境变量 `KONX_CLOUD_USER`） |
| `cloud.pass_md5` | — | 密码 MD5（建议 `KONX_CLOUD_PASS_MD5`，或明文走 `KONX_CLOUD_PASS` 自动转 MD5 不落盘） |
| `cloud.api_host` | `api1.cloudlinks.cn` | 云 API 端点 |
| `cloud.device_list` | `auto` | 设备列表端点：`auto`（LoadDeviceInfo 失败/空降级 SearchBindDev）\| `loaddeviceinfo` \| `searchbinddev` |
| `cloud.app_id` | demo 内置 | 可选覆盖 LoginCheck/设备列表请求的应用身份 AppID（32 位小写 hex；env `KONX_CLOUD_APP_ID`）。云端对 demo 身份有呼叫频率门控（15-30 分钟惩罚窗），生产 App 身份不受限；身份参数由部署方自备（由部署方自行持有） |
| `cloud.app_token` | demo 内置 | 可选覆盖应用身份 AppToken（64 位小写 hex；env `KONX_CLOUD_APP_TOKEN`） |
| `cloud.app_version` | `"1"` | 可选覆盖登录参数 AppVersion（纯数字；env `KONX_CLOUD_APP_VERSION`）。生产 App 发送位压缩版本号（由部署方自备） |
| `cloud.app_package` | `com.gwelldemo` | 可选覆盖登录参数 AppName/PackageName（env `KONX_CLOUD_APP_PACKAGE`）。生产 App 两者均为自身包名；身份参数由部署方自备（由部署方自行持有） |

### p2p —— 推送会话

| 键 | 默认 | 说明 |
|---|---|---|
| `p2p.trace` | `false` | 协议调试开关：全帧落盘 `/data/trace/` |
| `p2p.heartbeat` | `5s` | 推送通道心跳间隔 |
| `p2p.reconnect.min` / `.max` | `5s` / `60s` | 重连退避区间 |

### devices —— 设备列表

| 键 | 说明 |
|---|---|
| `devices[].id` | 设备 ID（**数字字符串**；`p2p-av` 强制校验） |
| `devices[].name` | 展示名（HA 实体名沿用） |
| `devices[].ip` | 设备内网 IP（仅 `device-rtsp` 源需要） |
| `devices[].rtsp.{transport,user,pass}` | 设备侧 RTSP 参数（`pass: auto` = MD5(设备ID)；仅 `device-rtsp` 源使用） |

### media —— 媒体源（核心）

| 键 | 默认 | 说明 |
|---|---|---|
| `media.source` | `external-rtsp` | **显式单选，无自动回退**：`p2p-av`（最终方案，P2P 直连设备）\| `external-rtsp`（上游 NVR 转发流）\| `device-rtsp`（直连设备 RTSP，需独占确认）\| `off` |
| `media.external_url` | — | `source=external-rtsp` 必填；`{profile}/{device}` 可作占位符 |
| `media.device_rtsp_exclusive_ack` | `false` | `source=device-rtsp` 必须显式 `true`：设备 RTSP 并发=1，NVR 在用时会挤掉 NVR 甚至致设备重启 |
| `media.event_stream_hold` | `15s` | 门铃事件后预热保流时长（按键后立刻可看） |
| `media.idle_stop` | `60s` | 通用源空闲挂断 |
| `media.p2p.treated_password` | `0` | 手动兜底直配设备 treated 密码（数字）；`0` = 取流启动时凭据自动解析（LoadDeviceInfo → secretKey → gwpass 解密），解析失败时再以此项兜底 |
| `media.p2p.hard_idle` | `15m` | **p2p-av 专用**空闲硬挂断。P2P 呼叫重建成本高且频繁拆挂有节流黑名单风险，**勿设短** |
| `media.ffmpeg_path` | `/usr/bin/ffmpeg` | 快照/HLS/MJPEG 与音频转码用 |

> `session_hold`（取流结束后不拆 P2P 链、观众重连仅重发取流命令的常驻会话模式）正在开发中，落地后可彻底规避呼叫节流窗，届时此处将补充对应配置项。

### rtsp —— RTSP 出口

| 键 | 默认 | 说明 |
|---|---|---|
| `rtsp.listen` | `:8554` | 监听地址（内部快照/HLS 管道走 `127.0.0.1:8554` 同一出口） |
| `rtsp.describe_wait` | `10s` | DESCRIBE/SETUP 早到等待：源已发起但描述未就绪（p2p-av 呼叫建立实测 ~5-8s）时最多等这么久；无源槽位立即 404 |

### onvif / advertise / mqtt / web / events / log

| 键 | 默认 | 说明 |
|---|---|---|
| `onvif.enable` / `.listen` | `true` / `:8000` | ONVIF 服务开关与端口 |
| `onvif.allow_anonymous` | `true` | 匿名访问（仅建议可信 VLAN） |
| `onvif.topics` | `["tns1:Doorbell/Stimulus"]` | 事件主题 |
| `advertise.host` | — | 对外公告地址（HA/NVR 拉流用宿主机 IP） |
| `advertise.rtsp_port` / `.http_port` | `8554` / `8080` | 端口映射与监听不同时必改 |
| `mqtt.enable` / `.broker` / `.user` / `.pass` / `.discovery` | `false` / — / — / — / `true` | 可选 MQTT 发布 |
| `web.listen` / `.data_dir` | `:8080` / `/data` | Web/API 端口与数据目录（SQLite/trace/HLS） |
| `events.retention_days` / `.max_rows` / `.keep_raw` | `90` / `100000` / `true` | 事件库保留策略；`keep_raw` 保留原始载荷便于排查 |
| `log.level` / `.format` | `info` / `json` | 日志 |

### 环境变量（优先于配置文件）

| 变量 | 说明 |
|---|---|
| `KONX_CLOUD_USER` / `KONX_CLOUD_PASS_MD5` / `KONX_CLOUD_PASS` | 云端账号 / 密码 MD5 / 明文密码（内存转 MD5） |
| `KONX_CLOUD_DEVICE_LIST` | 覆盖 `cloud.device_list` |
| `KONX_MEDIA_SOURCE` / `KONX_MEDIA_EXTERNAL_URL` | 覆盖媒体源 / 上游地址 |
| `KONX_MQTT_BROKER` | 设置即启用 MQTT |
| `KONX_WEB_LISTEN` / `KONX_DATA_DIR` / `KONX_LOG_LEVEL` | Web 端口 / 数据目录 / 日志级别 |

## 媒体源与 P2P 节流（重要）

**`media.source=p2p-av` 是最终方案**：网关像官方 App 一样经 P2P 服务器撮合与门铃建立 v2 设备会话，直接收取 H264 主码流并打包为 RTP 从 RTSP 出口分发。设备侧不占 RTSP 名额，与既有 NVR 直连互不干扰；下游多客户端由网关统一分发，并发不再受设备限制。

拉流地址（HTTP Basic，用户 `nvr`）：

```text
rtsp://nvr:<nvr密码>@<网关主机>:8554/konx/<设备ID>/main
```

**P2P 呼叫节流规律**（真机实测）：

- 完整拆链后 **15–30 分钟内**重新呼叫会被服务器拉黑——表现为登录后零推送、无 CALLING（礼貌挂断也不能避免，纯时间衰减）。
- 对策一：**session_hold 常驻会话模式**（开发中）——取流结束不拆链，观众重连只重发取流命令，仅长时间空闲才拆。
- 对策二（当前可用）：**避免频繁观看**。默认 `hard_idle=15m` 已尽量长保链路；网关对无凭据请求直接 401（不触发 P2P 呼叫、不烧节流窗），DESCRIBE 404 触发的预热呼叫有每设备 30s 冷却。

## 接入 Home Assistant

```bash
# 方式一：手动安装
cp -r custom_components/konx /config/custom_components/
# 方式二：HACS → 自定义仓库指向本仓库（含 custom_components/hacs.json）
```

重启 HA → **设置 → 设备与服务 → 添加集成 → "Konx Gateway"**，填写：

| 字段 | 说明 |
|---|---|
| 主机 / 端口 | 网关地址与 Web 端口（默认 8080） |
| RTSP 端口 | 默认 8554（仅端口映射不同时才改） |
| RTSP 用户名 / 密码 | 默认 `nvr` / 网关首次启动日志打印的密码 |
| API Token | 网关 Web 页创建（建议 read+talk scope） |

自动生成的实体（每台门铃）：

| 实体 | 说明 |
|---|---|
| 摄像头 | HA `stream` 组件经 RTSP 出口拉流；快照走网关 JPEG 管道 |
| 门铃（事件实体） | 有人按铃即触发，可驱动自动化/通知 |
| 在线 / 最后门铃 | 设备在线状态与最近按键时间 |
| 网关云登录 / 推送会话 / 客户端数 | 网关侧健康状态 |

另有 `konx.announce` 服务（经网关向门铃放音）；**对讲设备侧链路未完成，网关当前返回 501**，请勿依赖。

## 接入 NVR / Frigate / Blue Iris

| 协议 | 地址 |
|---|---|
| RTSP | `rtsp://nvr:<nvr密码>@<网关主机>:8554/konx/<设备ID>/main` |
| ONVIF | `http://<网关主机>:8000/onvif/device_service`（门铃事件主题 `tns1:Doorbell/Stimulus`） |

## 安全须知

- **云端凭据**：密码只以 MD5 形态参与登录协议；推荐 `KONX_CLOUD_USER` + `KONX_CLOUD_PASS_MD5` 环境变量注入，明文入口 `KONX_CLOUD_PASS` 仅内存转换、从不落盘。日志全量脱敏（MD5、sid、code、token）。
- **RTSP/ONVIF 凭据**：`nvr` 密码由网关首次启动随机生成并**仅打印一次**，明文存于数据目录 SQLite `kv` 表（`nvr_password`）。
- **Web 管理**：bcrypt 口令（首启随机生成打印一次），登录限速 5 次/分钟/IP。
- **API**：Bearer Token，scope=read/talk/admin，可吊销；HA 用只读+talk 即可。
- 与手机 App 共用账号会互踢；建议为网关单独注册账号。
- 定位家庭内网服务，不做公网暴露；TLS 交由可选反代。

## 故障排查

| 症状 | 原因与处理 |
|---|---|
| 登录成功但**零推送**（无门铃事件） | P2P 呼叫节流黑名单：完整拆链后 15–30 分钟内重呼会被拉黑。等 30 分钟再试；长期对策为 session_hold（开发中）或减少频繁观看 |
| p2p-av 呼叫被设备 REJECT | treated 密码链失败：凭据自动解析（`LoadDeviceInfo → secretKey → gwpass 解密`）不成功时，手动在 `media.p2p.treated_password` 直配兜底 |
| RTSP 拉流 401 | Basic 凭据错误：核对 `nvr` 用户与首启日志密码（忘记可删数据目录 `kv` 表记录或整体重置后重启重新生成） |
| RTSP 拉流偶发 404 / 起播慢 | p2p-av 呼叫建立需 ~5-8s：客户端首次 DESCRIBE 可能 404，重试即可；必要时调大 `rtsp.describe_wait` |
| 设备列表为空 / 登录 404 | 官方云旧端点已下线：`cloud.device_list: auto` 会自动从 SearchBindDev 降级到 LoadDeviceInfo；仍失败检查账号与网络出口 |
| 需要看协议交互细节 | 配置 `p2p.trace: true`，全帧落盘 `/data/trace/` 后分析（协议技术档案另行内部维护） |
| 对讲 501 | 预期行为：设备侧对讲链路待实现，当前明确返回 501 而非假装成功 |

## 开发

```bash
go test ./...          # 单元 + golden 比对 + mock P2P 服务器（无需外网/凭据；E2E 缺省 SKIP，KONX_E2E=1 启用）
go run ./cmd/realcheck # 真实列表服务 + 设备 ONVIF 一次性验证
```

```text
cmd/konx-gw/            入口（-config 默认 /etc/konx-gw/config.yaml；healthcheck 子命令）
cmd/realcheck/          真实端点一次性验证工具
cmd/bindcheck/          账号绑定设备一次性查询工具
internal/cloud/         登录 + 设备列表（LoadDeviceInfo / SearchBindDev）
internal/p2p/           rc5 / frame / list / session / av（协议层与 AV 取流）
internal/events/        告警解码 / 去重 / SQLite / 总线
internal/media/         源管理（p2p-av/external-rtsp/device-rtsp）/ ffmpeg 管道
internal/rtspout/       RTSP 出口（:8554，Basic 鉴权，懒预热）
internal/onvif/         ONVIF Device/Media/Events（:8000）
internal/api/           REST + WS + metrics + 内嵌 Web
internal/auth/          bcrypt / token / 限速 / RTSP 凭据表
internal/talk/          对讲会话 + G.711（设备侧 Sink 待实现）
internal/mqttx/         MQTT 发布 + HA discovery
custom_components/konx/ Home Assistant 集成
deploy/                 Dockerfile / compose / 配置示例 / systemd
docs/                   设计与协议文档
```

更多文档见 [docs/README.md](docs/README.md)。

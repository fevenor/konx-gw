# konx-gw 完整设计

版本：1.0（2026-10-24）
状态：设计评审稿
协议适配依据：gwelldemo 官方示例工程与官方 SDK 资料；P2P 协议技术档案另行内部维护

---

## 1. 背景与事实基础

### 1.1 已验证事实（设计依赖）

| # | 事实 | 证据 |
|---|---|---|
| F1 | 云登录配方端到端可用：`POST https://api1.cloudlinks.cn/Users/LoginCheck.ashx`，固定参数（AppVersion 必须纯数字），返回 `UserID`（signed u32，高位恒置位）、`P2PVerifyCode1/2`（R1/R2）、`SessionID/SessionID2` | 实测 error_code=0 |
| F2 | 同账号新登录会踢旧会话（与手机 App 互踢） | 登录层实测 |
| F3 | P2P 服务器列表：HTTP/UDP `:51701`（GetServiceList 路径与官方 SDK 参考实现一致），7F2B/36B 加密帧 → 7F2C/92B，条目端口为**大端** | 实测 5 台列表服务器全通 |
| F4 | 推送通道端到端打通：detect(7F01)→certify(7F03)→initInfo(7F81)→heart(7F21,5s)→CertifyResp ACK，随后 NOTIFY 推送(0x7F35)到达；最小握手不需要 UpdateEncKeyReq | 实测 60s 收 30 条 |
| F5 | 加密体系全实现：RC5-32/6/12（默认密钥见 `rc5.DefaultKey`）、帧内密钥、chkval 求和、RC5-16 派生 sid2、hash_key=0xD4D79C1A（空 pubkey） | 加密互通实测 |
| F6 | 门铃/告警事件（`ALARM_MSG_FROM_SRV`/`CALLING`，u64 MsgID 去重）经**服务器推送通道**下发，无需设备 P2P 会话 | 协议分析确认；载荷结构待真机按键验证 |
| F7 | 服务器推送 CheckLogin（~2s 重发直到应用层应答）；帧级 ACK 不足以止发 | 实测 |
| F8 | 设备 RTSP `:554`，`/onvif1`=1080p、`/onvif2`=180p，H264+G.711；**并发能力=1**，双客户端拉流会导致设备重启 | 用户实测 + ONVIF 摸底 |
| F9 | RTSP Digest，realm=`HIipCamera`，用户 `admin`，密码=`MD5(设备ID)`；裸栈 DESCRIBE 会 400（设备解析器挑剔），NVR 客户端栈可用 | 实测 + NVR 验证 |
| F10 | 设备 ONVIF `:5000`（gSOAP/2.8，无鉴权）：device/media/ptz/deviceio 四服务；**无 Events 服务**、无 WS-Discovery 应答；SerialNumber=设备ID | 实测 |
| F11 | App 原生音视频与对讲走 **p2pv3 设备会话**（avctl_*/AVSTREAMCTL 消息族），不占 RTSP 名额；该通路**尚未完整实现** | 官方 SDK 资料分析 + App 关 RTSP 仍可看实测 |
| F12 | 参考实现行为经真机验证形成 golden 测试向量，支撑 Go 实现比对 | 已建立 |

### 1.2 待决项（不阻塞架构，阻塞对应模块）

| # | 待决项 | 影响 | 计划 |
|---|---|---|---|
| U1 | 门铃按键推送的精确载荷结构 | 事件解码器 | M2 真机按铃闭环验证；解码器做成可插拔 + 原始帧留档 |
| U2 | CheckLogin 的应用层应答消息 | 推送通道安静度 | M1 复跑参考实现观察应答帧；过渡期容忍 2s 重发 |
| U3 | 设备 p2pv3 会话（0x50 通道握手 + AVSTREAMCTL 取流 + 对讲回传） | 首选音视频源、对讲（期间由 external-rtsp/受控 device-rtsp 回退保底） | M5 继续按既有路径分析 |
| U4 | 对讲话音编码（推断 G.711，未证） | 对讲模块 | 随 U3 一并确认 |

---

## 2. 目标与非目标

### 2.1 目标

1. **G1 账号会话维护**：网关长期维持云登录 + P2P 服务器推送长连接，掉线自愈，成为"常驻客户端"。
2. **G2 标准化音视频出口**：对外提供**带鉴权**的 ONVIF（Device/Media/Events）+ RTSP 再分发服务，任意标准客户端（NVR/Frigate/Blue Iris/HA）可接入，并发不再受设备"RTSP=1"限制。
3. **G3 对讲**：通过网关可向门铃发送语音（麦克风对讲），入口含 Web 页与 REST；ONVIF backchannel 为扩展项。
4. **G4 门铃事件**：网关本地落库记录；ONVIF 客户端经 PullPoint 标准获取；配置 MQTT 后同步发布。
5. **G5 简单 Web 页面**：登录/连接状态、事件记录、实时音视频、已连接客户端。
6. **G6 Home Assistant 接入**：配套 `custom_components/konx`，不依赖 MQTT 即可工作（直连网关 API）。
7. **G7 部署形态**：常规 Docker（compose），运行于既有容器宿主，非 HA Add-on。

### 2.2 非目标

- 不重写完整 P2P 媒体通路（NAT 多路径/uTCP/KCP 全栈）——仅实现"事件子集"（已验证）与"设备会话子集"（M5 按需分析，走服务器撮合，不做全 NAT 穿透矩阵）。
- 不做云存储回放、AI 事件、固件升级等 openapi 功能。
- 不做多租户/公网暴露设计（定位家庭内网服务；TLS 由可选反代承担）。
- 不提供 WS-Discovery 组播应答；不做媒体源自动回退链（2026-10-24 复杂度裁剪）。
- 不修改设备固件。

---

## 3. 总体架构

### 3.1 拓扑

```text
            云端（konx 云平台）                       家庭内网
  ┌────────────────────────────┐        ┌────────────────────────── 容器宿主 ──────────────────────────┐
  │ api1.cloudlinks.cn:443     │        │  ┌──────────────────── konx-gw (Go) ─────────────────────┐   │
  │ 列表服务器 :51701/udp,tcp  │◀──────▶│  │ cloud   登录/重登/凭据管理                              │   │
  │ P2P 服务器 :udp 动态端口   │◀──────▶│  │ p2p     gute帧/RC5/会话状态机/推送接收 ←─(F3,F4,F5)     │   │
  └────────────────────────────┘        │  │ events  解码→去重→SQLite→总线→{WS, ONVIF, MQTT, Hook} │   │
                                        │  │ media   源管理(p2p-av首选+分级回退)→再分发→快照→转码  │   │
  ┌────────────────────────────┐        │  │ device  设备注册表 + p2pv3设备会话(首选A/V源+对讲)   │   │
  │ 门铃 KW05 (Technology IPC) │        │  │ onvif   Device/Media/Events + WS-Discovery + 鉴权   │   │
  │ ◀═ p2pv3 会话(经云撮合,M5)══┼═══════▶│  │ rtsp    :8554 出口服务 (digest/basic 鉴权)          │   │
  │   A/V→网关 · 对讲←网关 首选  │        │  │ api+web :8080 REST/WS + 内嵌页面 (管理鉴权)         │   │
  │  RTSP :554 (备选*,并发=1)───┼──────▶ │  │ mqttx   可选 MQTT 发布 + HA discovery               │   │
  │  ONVIF :5000 (信息) ───────┼───────▶│  └─────────────────────────────────────────────────────┘   │
  └──────────────┬─────────────┘        │      ▲ HTTP/WS(token)        ▲ RTSP/ONVIF        ▲ MQTT      │
                 │ RTSP 直连·保持现状    │  ┌──┴──────────┐   ┌─────────┴────────┐   ┌──────┴──────┐    │
                 ▼                       │  │ Home Assist.│   │ Frigate / Blue   │   │ MQTT broker │    │
  ┌───────────────────────────┐          │  │ custom_comp │   │ Iris 等标准客户端 │   │ (可选)      │    │
  │ NVR（不迁移，继续独占      │          │  │ `konx`      │   │ (ONVIF/RTSP)     │   └─────────────┘    │
  │ 设备 RTSP 名额）          │          │  └─────────────┘   └──────────────────┘                      │
  └───────────────────────────┘          └───────────────────────────────────────────────────────────────┘
```

> \* `device-rtsp` 备选源受**独占确认**保护：仅当 NVR 让位（或分时窗口）且 `device_rtsp_exclusive_ack: true` 时网关才直连设备 RTSP（F8：并发=1，第二客户端会致设备重启）。常态下 NVR 保持直连不动；网关默认 p2pv3（M5 前以 external-rtsp 显式供源），均不与 NVR 竞争。

### 3.2 关键架构决策

| 决策 | 理由 |
|---|---|
| **AD1 音视频首选 p2pv3 设备会话，设备 RTSP 仅作参数化备选；NVR 保持直连不动** | F11：App 原生 A/V 走 p2pv3、不占 RTSP 名额，与 NVR 直连天然共存；F8：设备 RTSP 并发=1，任何第二路直连（含网关）都会触发设备重启——因此网关的 `device-rtsp` 源采用**显式独占确认制**，M5 前以 `external-rtsp`（NVR 转发流）显式供源。下游多客户端由网关出口统一分发，出口并发不受设备限制。 |
| **AD2 事件与媒体分级**：事件走服务器长连接（已验证 F4/F6，M2 即交付）；音视频**首选**设备 p2pv3 会话（M5 交付，此前显式配置 external-rtsp/device-rtsp 供流），对讲同路 | 先交付已验证通路的价值；p2pv3 与 RTSP 备选的切换对下游完全透明（AD4）。 |
| **AD3 事件源不依赖媒体源**：门铃事件即使 RTSP 关闭也必须触达 | F6：事件走服务器推送，与设备 RTSP 无关。 |
| **AD4 媒体源抽象接口 + 显式单选**（`p2p-av` / `external-rtsp` / `device-rtsp`(独占确认) / `off`；**无自动回退链**） | 2026-10-24 决策：砍掉回退逻辑以降复杂度——M5 前配 external-rtsp（或受控 device-rtsp），M5 后手动切 p2p-av；源切换不破坏下游契约与 HA 实体。 |
| **AD5 出口协议选 ONVIF + RTSP 双轨** | 用户点名 ONVIF；RTSP 是 ONVIF GetStreamUri 的载体且事实标准。ONVIF Events 弥补设备自身无 Events（F10）——**网关作为"虚拟 NVR/门禁"补齐事件面**。 |
| **AD6 直通优先，转码按需**（ffmpeg 子进程仅用于 G.711→AAC、快照、MJPEG） | H.264 直接 passthrough（零 CPU）；G.711 浏览器/HA Stream 不友好（F8 编码事实），最小转码点收敛在音频。 |
| **AD7 云账号密码只存 MD5** | 登录只需 MD5(F1)；配置可注入 `KONX_CLOUD_PASS_MD5`，明文不落盘。 |
| **AD8 HA 集成直连网关 API（REST+WS）**，MQTT 是可选旁路 | G6；避免强依赖 broker；MQTT discovery 作为无自定义组件时的兼容出口。 |

---

## 4. 协议适配层（internal/cloud, internal/p2p）

### 4.1 Go 移植范围

按协议规范（内部技术档案）字节级移植，纯标准库可完成（RC5/MD5/自定义 hash 均自实现，无 cgo）：

| 包 | 内容 |
|---|---|
| `p2p/rc5` | RC5-32/6/12（P=0xB7E15163, Q=0x9E3779B9, LE, ECB 8B 块）、RC5-16（P16=0xB7E1/Q16=0x9E37）、`chkval`（u32 求和 @0x00/04/08/0C/14，自字段置零）、`hash_key`（init 0x4E67C6A7） |
| `p2p/frame` | gute 帧编解码（class/type/len/term/seq/flags）、加密布局（恒加密区 [04,0C) 默认密钥；帧内密钥={头4B+flags4B} 加密 [0C,14) 与 [18,18+n*8)；尾部明文）、detect/certify/initInfo/heart/ACK/列表帧构造器 |
| `p2p/list` | 列表服务客户端（HTTP POST body=原始帧 或 UDP），响应条目解析（**端口 BE**），DNS（内嵌 8.8.8.8/223.5.5.5/114.114.114.114 兜底） |
| `p2p/session` | 会话状态机（见 4.2） |
| `p2p/push` | 0x7F35 NOTIFY 解析：msg_id/src/dst + 消息名 + JSON 体；帧级 ACK 回发 |
| `cloud/login` | LoginCheck.ashx 客户端（§2.1 参数表全量）、错误码映射（10901021/50/26/20）、`UserID` signed↔u32 转换、后续 `.ashx` 需要的 `"0"+低31位` 形式工具 |

### 4.2 推送会话状态机

```text
Idle → Login(cloud) → ListQuery → DetectAll(并发×全部服务器, 800ms 窗)
     → Select(RTT+应答评分) → Certify → InitInfo → Heart(5s 周期) → CertifyRespACK
     ── Ready ──┬─ 接收 NOTIFY → 帧ACK → 事件解码器 → 总线
                ├─ CheckLogin → (U2 未决：暂仅帧ACK；实现应用层应答后止发)
                └─ 心跳丢失×3 / 5s 无 ACK → Reconnect(服务器轮换, 指数退避 5s→60s + 抖动)
```

- 每次进入 Reconnect 先**重新登录**（F2：凭据可能被 App 踢掉）；检测到 10901050/凭据失效则告警（Web/HA 可见）。
- 账号策略（2026-10-24 确认）：开发/验证期与手机 App 共用现有账号（接受互踢、重登自愈）；**验证通过后申请独立云账号并授权访问设备**，届时仅改 `cloud.user/pass_md5`，网关自动重登，互踢消除。
- 列表结果缓存，服务器连续 2 次失败后强制刷新列表。
- 本地 UDP 绑定 51880 起（与参考实现一致）。
- **协议调试模式**（config `p2p.trace: true`）：全帧十六进制落盘 `data/trace/`（固定格式），供 U1/U2/U3 分析期间比对。

### 4.3 事件解码器（可插拔）

```go
type PushDecoder interface {
    Name() string                       // "checklogin" / "alarm" / "calling" / ...
    Decode(msgName string, body []byte) ([]Event, error)
}
```

- 内置：`checklogin`（会话状态事件）、`alarm-generic`（按实测结构 `{dev_id u64, alarm_type u32, data}` 解析）、`calling`。
- **未知消息不丢弃**：原始 hex + 消息名入库 `events(raw)`，`type=unknown`，Web 页标注"待识别"——U1 真机验证的数据直接从库里来。
- 去重：`MsgID u64` 主去重键 + 3s 滑窗二次去重（防服务器重发）。

---

## 5. 设备接入与媒体层（internal/device, internal/media）

### 5.1 设备注册表

```yaml
devices:
  - id: "<设备ID>"           # 设备ID（=SerialNumber, F10），RTSP 密码派生输入
    name: "前门门铃"
    ip: "192.168.1.100"        # 可选；不填则依赖云推送事件中的寻址
```

- 设备能力探测（启动时温和执行，复用实测结论）：ONVIF :5000 `GetDeviceInformation`/`GetProfiles`（单连接、间隔≥1.5s，F10 设备脆弱性）。
- RTSP 凭据：`admin` / `MD5(设备ID)` 自动派生（F9），允许覆盖。

### 5.2 源管理器（显式单选 + 再分发）

> 2026-10-24 决策修订：**音视频源为配置显式单选，不做自动回退**（降低复杂度）；M5 后首选 p2p-av，此前用 external-rtsp 或受控 device-rtsp；NVR 保持设备直连不动。

```text
StreamSource 接口:
  Start(ctx, profile Main|Sub) error     // 惰性启动
  Stop(profile)                          // 引用计数归零 + hold 计时器后停止
  RTPStreams(profile) (video, audio)     // 供出口 passthrough
  Events() <-chan SourceState            // started/stopped/error
```

源选择（`media.source` 显式单选，换源＝改配置后重载）：

| 实现 | 说明 |
|---|---|
| `p2p-av`（M5 后推荐） | 设备 p2pv3 会话 + AVSTREAMCTL（U3）。与 App 同路（F11），**不占设备 RTSP 名额**，与 NVR 直连天然共存；对讲同路。M5 前选择该项 → 启动报 `source_unavailable` 并提示可选源。 |
| `external-rtsp`（M5 前推荐） | 从 NVR 等已持有流的上游转发地址拉流（`external_url`+凭据），网关不碰设备 RTSP。 |
| `device-rtsp`（独占确认制） | gortsplib 客户端拉 `/onvif1`(main) 或 `/onvif2`(sub)。仅 `device_rtsp_exclusive_ack: true` 时可用（前提：NVR 让位或分时窗口；F8 并发=1 会致设备重启），否则拒绝启动（`rtsp_locked`）。裸栈 400（F9）→ NVR 行为对齐参数（Interleaved TCP、UA 伪装、URI 规范化）+ `pull_with_ffmpeg` 兜底；main/sub 切换串行化并间隔≥1.5s。 |
| `off` | 只做事件网关。 |

- 无自动回退：源失败即置错误态（state/事件/metrics 标注原因），不静默换源；恢复由人工或配置重载触发。

- **引用计数 + hold**：`event_stream_hold: 15s`——门铃事件到达时自动预启 main 源（下游想看立即有流），无消费者持续 hold 时长后停源，保护设备。
- 出口：RTSP 服务 `:8554`，路径 `/konx/{device}/{main|sub}`（+查询参数变体 `?audio=aac` 走转码管道）。
- 快照：源运行中从视频轨取 SPS/PPS+IDR → ffmpeg 单帧 JPEG（`/api/devices/{id}/snapshot`）；源未运行时按策略（默认触发短拉，受互斥锁保护）。

### 5.3 转码与 Web 播放

| 管道 | 实现 | 用途 |
|---|---|---|
| passthrough | gortsplib 服务端转发 H264 + G.711 (PCMA) | NVR/Frigate/标准客户端 |
| audio-aac | ffmpeg: `-c:v copy -c:a pcm_alaw→aac` | HA Stream、Web HLS（G.711 不可直放） |
| mjpeg | ffmpeg 解码 sub 流 → multipart/x-mixed-replace | Web 低延迟简览（180p，CPU 低） |
| hls | ffmpeg aac 管道 → fMP4 分片（2s） | Web 主看板 |

ffmpeg 为镜像内置静态二进制——**仅 `full` 一档镜像**（2026-10-24 决策），AAC/HLS/MJPEG/快照默认可用。

---

## 6. 对讲设计（internal/device/talk）

**上行输入（客户端→网关），三条入口共用一个 TalkSession：**

| 入口 | 协议 | 说明 |
|---|---|---|
| Web 页 | WS `/api/devices/{id}/talk/ws` | 浏览器 `getUserMedia` → WebAudio 重采样 PCM16/8kHz 分帧下发；网关侧做 G.711 编码（纯 Go） |
| REST | `POST /api/devices/{id}/talk/audio`（audio/L16 或 wav 分块） | HA 自动化/TTS 播报到门口（`konx.announce` 服务） |
| ONVIF backchannel（扩展） | RTSP 双向（a=sendonly 反向轨） | 标准但对讲客户端支持稀少，M5 后评估；非交付承诺 |

**下行输出（网关→设备）：** `TalkSink` 接口；M3 期间唯一实现是 `UnavailableSink`（明确报错"等待设备会话模块"），M5 实现 `P2pAvTalkSink`（avctl 对讲通道，编码按 U4 确认，推断 G.711 A-law/u-law）。

- TalkSession 全局每设备一条：独占（对讲进行中拒绝第二路，排队或忙音提示）；心跳超时 5s 自动释放。
- 权限：`talk` 独立 scope（见 §12）。

---

## 7. ONVIF 服务（internal/onvif）

网关对外呈现为一台 **Konx 虚拟 NVR/门铃设备**（Manufacturer=`Konx`，Model=`konx-gw`，FW=网关版本），补齐设备自身缺失的事件面。

### 7.1 服务清单

| 服务 | 端点 | 操作（v1） |
|---|---|---|
| Device | `/onvif/device_service` | GetServices, GetServiceCapabilities, GetDeviceInformation, GetCapabilities, GetSystemDateAndTime, GetHostname, GetUsers(本地用户表) |
| Media | `/onvif/media_service` | GetProfiles（每设备 2 profile：主/子，H264+G.711 或 +AAC 变体）, GetProfile, GetStreamUri(rtsp://gw:8554/...), GetVideoEncoderConfigurations, GetSnapshotUri(http://gw:8080/api/.../snapshot) |
| Events | `/onvif/events_service` | GetServiceCapabilities(PersistentPullPoint), CreatePullPointSubscription, PullMessages, Unsubscribe, Subscribe（Basic，HTTP 反向通知，v2 可选） |
> WS-Discovery（组播自动发现，UDP 3702）**不提供**（2026-10-24 决策：部署环境网络复杂，采用容器端口映射）。客户端在 NVR/Frigate/Blue Iris 中手动添加 `http://<宿主机IP>:8000/onvif/device_service` 即可，仅损失"自动扫描"便利，其余功能不受影响。

### 7.2 门铃事件主题

- 缺省主题 **`tns1:Doorbell/Stimulus`**（boolean, Source=设备ID）。
- 兼容别名（可配置数组）：`tns1:Device/Trigger/DigitalInput` 等——不同 NVR 识别方言不一，双发由 PullPoint 响应按订阅者过滤。
- 事件同时置 `Data`（SimpleItem: msg_id/ts/type）。

### 7.3 鉴权

- WS-UsernameToken（PasswordDigest + Nonce + Created），用户表与 RTSP 凭据同源（§12）。
- `onvif.allow_anonymous: true`（默认，内网）时跳过校验但仍在能力里如实通告。

---

## 8. 事件引擎（internal/events）

```text
p2p push → Decoder → Dedup(MsgID + 3s窗) → Store(SQLite) → Bus → Fan-out:
                                                                  ├─ WS/SSE (web + HA 组件)
                                                                  ├─ ONVIF PullPoint 队列(每订阅者)
                                                                  ├─ MQTT publisher
                                                                  └─ Webhook(可选列表)
```

### 8.1 事件类型

| type | 触发 |
|---|---|
| `doorbell.press` | U1 解出的按键告警（含 `unknown` 降级路径） |
| `device.alarm` | 其他 ALARM_MSG_FROM_SRV |
| `device.calling` | CALLING |
| `device.online / device.offline` | 推送推理 + RTSP/ONVIF 探测交叉 |
| `cloud.session` | 登录成功/被踢/凭据失效 |
| `p2p.session` | 推送通道 up/down/服务器切换 |
| `source.*` | 媒体源启停与错误 |
| `gw.system` | 启动、配置重载、内部错误 |
| `unknown` | 未识别推送（原始载荷入库，服务 U1） |

### 8.2 存储 DDL（SQLite /data/konx-gw.db，WAL）

```sql
CREATE TABLE events(
  id INTEGER PRIMARY KEY,
  msg_id TEXT,                -- 云端 u64 MsgID（可空）
  device_id TEXT NOT NULL,
  type TEXT NOT NULL,
  ts_event INTEGER,           -- 事件自带时间（可空）
  ts_recv INTEGER NOT NULL,   -- 网关接收单调时间
  payload TEXT,               -- 规范化 JSON
  raw BLOB                    -- 原始帧载荷（unknown/调试保留, 可关）
);
CREATE INDEX idx_events_ts ON events(ts_recv);
CREATE INDEX idx_events_dev ON events(device_id, ts_recv);

CREATE TABLE clients(
  id TEXT PRIMARY KEY,        -- uuid
  kind TEXT NOT NULL,         -- onvif|rtsp|api|web|mqtt
  peer TEXT, user TEXT,
  connected_at INTEGER NOT NULL,
  last_active INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT);   -- 会话状态/登录历史/迁移版本
```

- 保留策略：`events.retention_days`（默认 90）+ 上限条数，后台每日清理。
- 客户端注册表由各出口模块登记心跳（ONVIF PullMessage、RTSP 会话、WS、Web），驱动 Web 页"已连接客户端"与 HA 传感器。

---

## 9. MQTT（internal/mqttx）

- 可选模块（未配置则完全关闭）。
- 客户端：paho.mqtt.golang；LWT `konx-gw/{gw_id}/status = offline`，连接后 `online`（retain）。

**Topic 与载荷：**

```text
konx-gw/{gw_id}/status                                  {"ver","uptime","cloud":"up|down","p2p":"up|down"}
konx-gw/{gw_id}/device/{dev}/availability               online|offline (retain)
konx-gw/{gw_id}/device/{dev}/event/doorbell             {"msg_id","ts","type":"press"} (非retain)
konx-gw/{gw_id}/device/{dev}/event/#                    其他事件类型
konx-gw/{gw_id}/device/{dev}/clients                    {"rtsp":n,"onvif":n,"web":n}
homeassistant/event/konx_{dev}_doorbell/config           （discovery: true 时, HA event 实体）
homeassistant/binary_sensor/konx_{dev}_online/config
homeassistant/camera/konx_{dev}/config                  （stream_source 指回网关 RTSP）
```

- 双轨说明：HA 自定义组件为主路径；MQTT discovery 供"不想装组件"的场景（camera 走 generic RTSP）。

---

## 10. Web UI 与 API（internal/api, internal/web）

- 内嵌静态资源（go:embed），零外部 CDN（hls.js 本地打包）；管理鉴权：登录换 HttpOnly Cookie（bcrypt 口令）；API 走 `Authorization: Bearer <token>`（scope：`read`/`talk`/`admin`）。

### 10.1 REST（v1 前缀 /api）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | /api/state | 汇总：cloud/p2p 会话、设备列表与可达性、媒体源状态、客户端计数、版本 |
| GET | /api/events?limit=&since=&device=&type= | 事件查询 |
| DELETE | /api/events?before= | 清理（admin） |
| GET | /api/devices | 设备+能力+源状态 |
| GET | /api/devices/{id}/snapshot?stream=main\|sub | JPEG |
| POST | /api/devices/{id}/talk/start \| /talk/stop | 对讲会话（M5 前返回 501+原因） |
| POST | /api/devices/{id}/talk/audio | L16/WAV 分块 |
| GET | /api/clients | 活动客户端明细 |
| POST/DELETE | /api/tokens, /api/tokens/{id} | token 管理（admin） |
| GET | /healthz, /metrics | 存活 / Prometheus |

### 10.2 WebSocket `/api/ws`

服务端推送（带序号，断线以 `since` 补拉）：`state.changed`、`event.new`、`client.changed`、`talk.state`。HA 组件与 Web 页共用。

### 10.3 页面（单页，四个视图）

1. **状态**：云登录（账号脱敏、最近被踢时间）、p2p 会话（服务器、RTT、心跳）、设备（在线、源状态）；失败诊断文案（如"凭据被踢：手机 App 与网关互踢，建议专用账号"）。
2. **事件**：时间线表格（过滤/分页），unknown 事件高亮并附原始载荷查看（服务 U1）。
3. **实时**：设备选择 → HLS/MJPEG 播放 + 音量（AAC）；对讲按钮（按住说话，M5 前置灰并提示依赖）。
4. **客户端**：RTSP/ONVIF/API/Web/MQTT 会话表（身份、IP、速率、时长），一键断开（admin）。

---

## 11. Home Assistant 集成（custom_components/konx）

### 11.1 结构

```text
custom_components/konx/
  manifest.json        domain: konx, requirements: 无第三方纯库优先(aiohttp 内置), issue_tracker
  config_flow.py       单网关实体: host, port(8080), token(默认 read scope)
  coordinator.py       DataUpdateCoordinator: 30s GET /api/state + WS /api/ws 即时事件
  api.py               aiohttp 客户端 + WS 重连(指数退避)
  camera.py            CameraEntity: stream_source=rtsp://gw:8554/konx/{dev}/main?audio=aac
                       still_image_url=/api/devices/{id}/snapshot (token 参数化头)
  event.py             EventEntity: doorbell_press (device_class 无标准门铃, 自定义)
  binary_sensor.py     设备在线; 网关云会话; 网关 p2p 会话
  sensor.py            每设备客户端数; 最后事件时间
  services.yaml        konx.announce (text/url→网关 talk, M5), konx.snapshot
  translations/{zh-Hans,en}.json, icons.json, strings.json
```

### 11.2 行为

- 配置流校验 token（GET /api/state 401→错误提示）；Options 流选设备、启停实体。
- 摄像头实体开启 `front_end` 流走 HA `stream` 组件（AAC 管道解决 G.711，AD6）；对讲在 HA 前端无标准 UI，提供 `konx.announce` 服务（TTS→talk REST）覆盖自动化场景。
- WS 断线降级为轮询（事件仍准实时≤30s）。
- 设备模型：一台"Konx Gateway"Hub 设备 + 每门铃子设备（门铃事件/摄像头/传感器挂子设备）。

---

## 12. 安全设计

| 面 | 机制 |
|---|---|
| 云凭据 | 密码仅存 MD5（AD7）；`KONX_CLOUD_USER`/`KONX_CLOUD_PASS_MD5` 环境变量优先于配置文件；日志全量脱敏（MD5、sid、code1/2、token） |
| Web/管理 | bcrypt 口令（首次启动生成随机口令打印一次）；会话 Cookie（HttpOnly+SameSite=Lax）；登录限速（5 次/分钟/IP） |
| API token | 随机 32B，库存 SHA-256，scope=read/talk/admin，可吊销；HA 用只读+talk token |
| RTSP/ONVIF | 同一凭据表（每用户独立，digest 与 basic 并存）；默认拒绝；`allow_anonymous` 开关仅建议可信 VLAN |
| 网络 | 默认监听内网地址；TLS 交由可选反代（文档给 Caddy/nginx 片段）；不做公网暴露 |
| 审计 | token/客户端管理操作入 `gw.system` 事件 |
| 设备保护 | 所有对设备探测遵守"单连接+≥1.5s 间隔"（F10）；RTSP 互斥锁（AD1） |

---

## 13. 配置设计（config.yaml）

```yaml
gw_id: front-gate                  # MQTT/发现用实例标识
cloud:
  user: ""                         # 或 env KONX_CLOUD_USER
  pass_md5: ""                     # 或 env KONX_CLOUD_PASS_MD5（二选一，勿配明文）
  device_list: auto                # 设备列表端点: auto(LoadDeviceInfo, 失败/空降级 SearchBindDev) | loaddeviceinfo | searchbinddev
  app_id: ""                       # 可选覆盖应用身份 AppID（32 hex；env KONX_CLOUD_APP_ID），缺省 GwellDemo 内置值
  app_token: ""                    # 可选覆盖应用身份 AppToken（64 hex；env KONX_CLOUD_APP_TOKEN），缺省 GwellDemo 内置值
  app_version: ""                  # 可选覆盖 AppVersion（纯数字；env KONX_CLOUD_APP_VERSION），缺省 "1"（生产为位压缩版本号，由部署方自备）
  app_package: ""                  # 可选覆盖 AppName/PackageName（env KONX_CLOUD_APP_PACKAGE），缺省 com.gwelldemo（生产 App 为自身包名）
  app: { appid: "<GwellDemo>", package: com.gwelldemo, app_version: "1" }  # 固定三元组（appid 优先级低于 app_id 覆盖）
p2p:
  trace: false                     # 协议调试: 全帧落盘
  heartbeat: 5s
  reconnect: { min: 5s, max: 60s }
devices:
  - id: "<设备ID>"
    name: 前门门铃
    ip: 192.168.1.100
    rtsp: { transport: tcp, user: admin, pass: auto }   # pass:auto=MD5(id)
media:
  source: external-rtsp            # 显式单选, 无自动回退: p2p-av(M5后推荐) | external-rtsp(M5前推荐) | device-rtsp | off
  external_url: ""                 # source=external-rtsp 必填(NVR 等上游转发流地址)
  device_rtsp_exclusive_ack: false # source=device-rtsp 必须显式 true(NVR 直连在用时并发=1 会致设备重启)
  pull_with_ffmpeg: false
  event_stream_hold: 15s
  idle_stop: 60s
  ffmpeg_path: /usr/bin/ffmpeg     # full 镜像内置
rtsp:
  listen: ":8554"
onvif:
  enable: true
  listen: ":8000"
  allow_anonymous: true
  topics: ["tns1:Doorbell/Stimulus"]      # 追加别名在此
advertise:                          # 桥接+端口映射: 对外公告地址(ONVIF GetStreamUri/快照/HA 用); 留空取请求 Host
  host: ""                          # 宿主机 IP, 如 192.168.1.10
  rtsp_port: 8554                   # 宿主机映射端口(与 listen 不同时必改)
  http_port: 8080
mqtt:
  enable: false
  broker: ""                        # tcp://host:1883
  user: "", pass: ""
  discovery: true
web:
  listen: ":8080"
  # admin_password_hash 由首次启动生成写入 data/
events:
  retention_days: 90
  max_rows: 100000
  keep_raw: true                    # U1 落地后可关
storage:
  data_dir: /data                   # sqlite + trace + hls 分片
log: { level: info, format: json }
```

环境变量覆盖规则：`KONX_` 前缀 + 下划线路径（`KONX_MQTT_BROKER`）；配置热重载（SIGHUP）仅限非会话项。

---

## 14. 部署设计

```yaml
# deploy/docker-compose.yml
services:
  konx-gw:
    image: konx-gw:full              # linux/amd64 + arm64, 内置 ffmpeg(唯一档位)
    restart: unless-stopped
    ports:                           # 桥接网络 + 端口映射(最终部署形态, 无组播需求)
      - "8080:8080"                  # Web/REST/WS
      - "8554:8554"                  # RTSP(下游客户端)
      - "8000:8000"                  # ONVIF
    environment:
      - KONX_CLOUD_USER=${KONX_CLOUD_USER}
      - KONX_CLOUD_PASS_MD5=${KONX_CLOUD_PASS_MD5}
      - TZ=Asia/Shanghai
    volumes:
      - ./data:/data
    healthcheck:
      test: ["CMD", "/usr/local/bin/konx-gw", "healthcheck"]
      interval: 30s
```

- 镜像：多阶段构建（golang:1.23 builder → distroless + ffmpeg 静态二进制）；**仅 `full` 一档**（2026-10-24 决策）。
- 出网需求（防火墙清单）：`api*.cloudlinks.cn:443/tcp`、列表服务器 `:51701/tcp+udp`、P2P 服务器 `udp 高位动态`；入网映射 8080/8554/8000，无组播（WS-Discovery 不提供）。
- 桥接映射下，ONVIF GetStreamUri/快照/RTSP 地址经 `advertise` 配置公告（宿主 IP + 映射端口；端口一一映射时只需填 host）。
- 升级：单一二进制+静态资源，SQLite 迁移向前兼容；数据卷仅 `/data`。
- HA 侧：`configuration.yaml` 无需改动，组件放入 `custom_components/konx/` 重启即可。

---

## 15. 可观测性

- 结构化日志（slog JSON），模块前缀（`p2p`/`media`/`onvif`...），敏感字段 redact 中间件。
- `/metrics`（Prometheus，可选暴露）：`konx_p2p_up`、`konx_cloud_login_total{result}`、`konx_push_messages_total{type}`、`konx_events_total{type}`、`konx_source_state{device,profile}`、`konx_clients{kind}`、`konx_rtsp_sessions`。
- `p2p.trace` 调试模式（§4.2）与 `events.keep_raw` 共同支撑 U1–U4 的剩余分析。

---

## 16. 代码结构

```text
konx-gw/
  cmd/konx-gw/            # main + healthcheck 子命令
  internal/
    cloud/  p2p/  device/  media/  onvif/  events/  mqttx/  api/  web/  auth/  config/
  custom_components/konx/ # §11
  deploy/                 # Dockerfile, Dockerfile.full, docker-compose.yml, Caddy 片段
  docs/                   # DESIGN.md(本文档)
  test/                   # golden 帧, mock 服务器, e2e
```

依赖（全 MIT/Apache-2.0）：`bluenviron/gortsplib/v4`、`eclipse/paho.mqtt.golang`、`modernc.org/sqlite`、`gorilla/websocket`、`golang.org/x/crypto`、`gopkg.in/yaml.v3`。ONVIF 服务端手写 SOAP handler（无成熟 Go 服务端库，模板化 XML）。

---

## 17. 测试策略

| 层 | 方法 |
|---|---|
| 单元 | RC5/RC5-16/chkval/hash_key 用逐字节 golden 用例；帧编解码往返；事件解码器表驱动 |
| 协议 | **Go mock P2P 服务器**（讲 7F2B/2C/01/03/81/21/35 完整方言）驱动会话状态机全路径（重连/踢线/服务器轮换），CI 无外网运行 |
| 媒体 | gortsplib loopback（自建 RTSP 源模拟设备，注入"并发=1"行为断言互斥锁）；转码管道用固定 H264+PCMA 样片 |
| ONVIF | SOAP 用例集比对 XML；对 HA ONVIF 集成与 Frigate 做容器化 E2E（discovery→GetStreamUri→拉流→PullPoint 收门铃） |
| HA 组件 | HA dev 容器 + 网关 mock API；config_flow/实体/服务快照测试 |
| 硬件在环（手动清单） | 真机按铃（U1）、NVR 共存迁移、设备重启恢复、长稳 72h（内存/会话泄漏）、App 与网关互踢观察 |

---

## 18. 里程碑与验收

| 里程碑 | 内容 | 验收标准 |
|---|---|---|
| M0 骨架（0.5w） | 仓库/CI/Docker/config/slog/healthz | compose 起容器，/healthz 绿 |
| M1 协议层（1w） | p2p+cloud Go 移植、mock 服务器、trace 模式 | golden 全过；真账号实测推送通道 Ready 且收 CheckLogin；U2 若解则含应答 |
| M2 事件与最小可用（1w） | 事件引擎+SQLite+WS+Web(状态/事件)+HA 组件 v1（事件/传感器实体） | 真机按铃 → Web/HA 实时出现事件；**U1 在此闭环**（含 unknown 降级路径） |
| M3 媒体（1.5w） | 源框架+RTSP 出口+快照+AAC 管道+Web 实时+HA camera（联调供源：external-rtsp 为主，或与 NVR 分时窗口的 device-rtsp） | HA 前端流畅看直播（联调源供流）；出口多客户端 72h 稳定；**NVR 不迁移** |
| M4 标准面（1w） | ONVIF Device/Media/Events+WS-Discovery+MQTT+客户端注册表 | Frigate 经 ONVIF 发现并拉流、收到门铃事件；MQTT 侧 HA discovery 生效 |
| M5 设备会话与对讲（2w+，含协议分析） | U3/U4 协议攻坚 → p2p-av 源（此后默认首选）+ TalkSink + Web/REST 对讲 + `konx.announce` | Web 按住说话门铃出声；p2p-av 与 NVR 直连**共存** 72h（不占 RTSP 名额、设备不重启） |
| M6 加固（1w） | 鉴权完备、限速、metrics、文档、备份指南 | 安全清单过检；72h 长稳通过；发布 v1.0 |

依赖关系：M2 只依赖 M1；M3/M4 并行；M5 依赖 U3 攻坚（风险最高）。v1.0 = M0–M4+M6：媒体显式配置 external-rtsp（或受控 device-rtsp）供源、对讲暂缺；v1.1 = M5：交付 p2p-av 源 + 对讲，配置手动切换。全程 NVR 保持直连不动。

---

## 19. 风险登记

| 风险 | 等级 | 缓解 |
|---|---|---|
| U1 门铃载荷未验证 | 中 | unknown 事件保真入库；M2 真机闭环；解码器热插拔（配置映射表） |
| U2 CheckLogin 止发应答缺失 | 低 | 容忍 2s 重发（量小）；M1 复跑参考实现找应答帧 |
| U3 p2pv3 设备会话分析超期 | 高（对讲 + 首选音视频源） | 显式切换 external-rtsp 保底，v1.0 媒体不受阻；分析路径可行；NVR 保持直连，设备侧无第二 RTSP 消费者 |
| 账号互踢（App vs 网关） | 中→低 | 开发期共用现有账号：重登退避 + `cloud.session` 事件可见；**已确认：验证通过后申请独立云账号并授权访问设备**，仅改 cloud.user/pass_md5 即切换，届时风险消除 |
| 设备 RTSP 裸栈 400（F9） | 中 | NVR 行为对齐参数 + `pull_with_ffmpeg` 兜底；独占消费降低频率 |
| 设备脆弱性（探测风暴致 60s 自愈/重启） | 中 | 全部设备探测走限速器；互斥锁；断路器（连续失败熔断 5min） |
| konx 云平台长期存续 | 低-中（数年维度） | 事件链路留原始 trace；`external-rtsp` 模式保留纯本地降级（事件除外） |
| 云端风控（非官方客户端特征） | 低 | 流量保真模拟 App（UA/参数/心跳节奏）；慢速重试；可观测告警 |

---

## 20. 附录

- 端口总表：设备侧 554/tcp(RTSP)、5000/tcp(ONVIF)；网关映射入网 8080(web/api)、8554(rtsp)、8000(onvif)——无 3702/组播；网关出网 443/tcp、51701/tcp+udp、动态 udp。
- 命名：HA domain `konx`；MQTT 前缀 `konx-gw/`；RTSP 路径 `/konx/{dev}/{main|sub}`；ONVIF Manufacturer `Konx`。
- 协议技术档案另行内部维护。
- 许可：网关代码与组件私有；依赖均 MIT/Apache-2.0；ffmpeg LGPL 静态二进制以独立进程调用（无链接污染）。

---

## 21. 附录 B：关键交互时序与数据契约

### B.1 门铃事件端到端

```text
[按键] 门铃 → 云端 → P2P服务器
  └─NOTIFY(0x7F35, ALARM/CALLING)─▶ konx-gw p2p/push
       ├─ 帧ACK(0x7F35/24B) 立即回发
       ├─ Decoder: alarm→doorbell.press (未识别→type=unknown+raw入库)
       ├─ Dedup: MsgID主键 + 3s滑窗
       ├─ Store: events表
       └─ Bus fan-out:
            ├─ WS event.new ──────────▶ Web页 / HA组件 (即时)
            ├─ PullPoint队列置位 ─────▶ ONVIF客户端下次PullMessages立即返回
            ├─ MQTT publish ─────────▶ konx-gw/{gw}/device/{dev}/event/doorbell
            └─ media: 源预热(event_stream_hold, 有观看者则立即出流)
```

### B.2 ONVIF/NVR 客户端接入取流

```text
客户端手动添加 http://<宿主IP>:8000/onvif/device_service (无 WS-Discovery)
GetServices → GetCapabilities → GetProfiles(每设备 main/sub 两profile)
GetStreamUri → rtsp://gw:8554/konx/{dev}/main[?audio=aac]
RTSP PLAY → 出口会话计数+1 → 源管理器惰性启动(media.source 显式单选)
  ├─ p2p-av: p2pv3 设备会话取流(不占设备RTSP名额, 与NVR直连共存)
  └─ external-rtsp(NVR转发) / device-rtsp(需独占确认)
       → H264 passthrough / G.711(或转AAC)
TEARDOWN → 计数-1 → 归零且超 idle_stop → 停源释放设备
```

### B.3 HA 组件接入

```text
config_flow: host+token → GET /api/state(401即拒) → 建Hub+子设备实体
运行期: WS /api/ws 长连(断线指数退避, 降级30s轮询)
  门铃: event.new(doorbell.press) → EventEntity 触发
  直播: CameraEntity.stream_source = rtsp://gw:8554/...?audio=aac (HA stream组件)
  传感器: state.changed → 设备在线/云会话/p2p会话/客户端数
```

### B.4 对讲（M5 后）

```text
Web按住说话: getUserMedia → WebAudio重采样PCM16/8k → WS /talk/ws
  → TalkSession(每设备独占, 5s心跳超时释放) → G.711编码 → P2pAvTalkSink → 设备
HA自动化: konx.announce(text) → 网关 /talk/audio (L16/WAV)
ONVIF backchannel(扩展): RTSP SETUP反向音频轨, 同一TalkSession
M3~M4期间: UnavailableSink → 501 + 明确错误"等待设备会话模块(M5)"
```

### B.5 WS /api/ws 消息契约

```jsonc
// 服务端→客户端, 单调递增 seq, 客户端断线以 ?since=seq 补拉
{"seq": 142, "type": "event.new", "data": {
  "id": 1032, "msg_id": "180123456789", "device_id": "<设备ID>",
  "type": "doorbell.press", "ts_event": 1761291600, "ts_recv": 1761291601 }}
{"seq": 143, "type": "state.changed", "data": {
  "cloud": "up", "p2p": "up", "p2p_server": "<P2P服务器>:8000",
  "devices": [{"id": "<设备ID>", "online": true, "source": "main:active"}],
  "clients": {"rtsp": 2, "onvif": 1, "web": 1, "api": 1} }}
{"seq": 144, "type": "client.changed", "data": {...}}
{"seq": 145, "type": "talk.state", "data": {"device": "<设备ID>", "state": "active", "by": "web"}}
```

### B.6 REST 错误契约

```jsonc
// 统一: {"error": {"code": "token_invalid|scope_denied|device_offline|
//   rtsp_locked|talk_unavailable|not_found|rate_limited", "message": "..."}}
// 501 talk_unavailable 附 "reason":"awaiting_p2p_av_module"
```

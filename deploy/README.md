# konx-gw 部署

支持 **Docker Compose**（推荐）与**裸机 systemd** 两种形态。配置文件统一为 YAML（示例见同目录 `config.example.yaml`，完整字段说明见根目录 [README.md](../README.md#配置项总表) 与 [docs/DESIGN.md](../docs/DESIGN.md) §13）。

## 快速开始（Docker Compose）

```bash
cd deploy
cp config.example.yaml config.yaml     # 编辑：devices 设备列表、advertise.host、media.source
export KONX_CLOUD_USER="86-<手机号>"
export KONX_CLOUD_PASS_MD5=$(printf '%s' '<密码>' | md5sum | cut -d' ' -f1)
docker compose up -d --build
```

可直接设置明文环境变量 `KONX_ADMIN_PASSWORD` 和 `KONX_NVR_PASSWORD` 固定两个网关密码，无长度、复杂度要求，也无需自行哈希或加密。它们在每次启动时优先于数据库，修改后重新创建容器即可生效；不会改写数据库或打印配置的密码。显式空字符串也是有效密码，未设置才沿用数据库或首次随机生成。

未设置对应环境变量且数据库没有凭据时，首次启动日志会打印两个随机凭据：

- **admin 管理密码** —— Web 登录 `http://<宿主机>:8080/`
- **RTSP/ONVIF 用户 `nvr` 的密码** —— 拉流与 HA 配置使用

### bridge 网络

不要求 host 网络。使用 `media.source: p2p-av` 时，除 Web、RTSP、ONVIF 的 TCP 端口外，还需同端口映射 IPv4 UDP `51880`，见 Compose 示例。网关使用临时端口探测服务器，选择后在固定端口建立正式会话；设备向该会话地址发送握手与媒体包。仅发布三个 TCP 端口时，可能出现云端已登录但视频建链超时。此项为宿主机到容器的映射，不要求额外配置路由器端口转发，也无需映射整个 UDP 范围。

HTTP 与 RTSP 对外端口不同时，在 `advertise.host/http_port/rtsp_port` 中填写客户端实际访问的宿主机地址和端口。例如 `18080:8080` 对应 `web.listen: :8080`、`advertise.http_port: 18080`。下游 RTSP 使用 TCP；ONVIF 使用服务地址手动添加，不提供组播自动发现。

容器端口映射（见 `docker-compose.yml`）：

| 端口 | 用途 |
|---|---|
| 8080 | Web 管理页 / REST / WebSocket |
| 8554 | RTSP 出口（下游客户端拉流） |
| 8000 | ONVIF |

数据持久化在 `./data`（SQLite 事件库、kv 凭据表、trace、HLS 分片）；与监听端口不一致时记得同步修改 `advertise.rtsp_port` / `advertise.http_port`。

## 裸机 systemd 部署

前置：Go 1.26+ 编译（或取已编译二进制）；快照/HLS/MJPEG 功能需主机安装 `ffmpeg`。

```bash
go build -o /usr/local/bin/konx-gw ./cmd/konx-gw
install -d /etc/konx-gw /var/lib/konx-gw
cp deploy/config.example.yaml /etc/konx-gw/config.yaml   # 按需编辑
```

`/etc/systemd/system/konx-gw.service`：

```ini
[Unit]
# 门铃网关：云登录 + P2P 取流 + RTSP/ONVIF 出口
Description=konx-gw (konx doorbell gateway)
# 等网络就绪再启动（登录云端/P2P 服务器依赖出网）
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
# 二进制路径与配置文件（-config 缺省即 /etc/konx-gw/config.yaml，显式写出更清晰）
ExecStart=/usr/local/bin/konx-gw -config /etc/konx-gw/config.yaml
# 云端凭据经环境变量注入，不写入配置文件：
#   账号即 App 手机号（带国家码前缀 86-）
Environment=KONX_CLOUD_USER=86-13800000000
#   密码二选一：预生成 MD5（推荐，避免明文出现在 unit 文件）
Environment=KONX_CLOUD_PASS_MD5=0123456789abcdef0123456789abcdef
#   或明文（网关仅内存转 MD5，不落盘；unit 文件权限务必 600）
#Environment=KONX_CLOUD_PASS=明文密码
# 可选：固定网关密码，直接填明文
#Environment=KONX_ADMIN_PASSWORD=管理密码
#Environment=KONX_NVR_PASSWORD=媒体密码

# 崩溃自动拉起（推送会话具备断线自愈，重启无害）
Restart=on-failure
RestartSec=5

# 日志走 journald（容器外不用 json 格式时可在 config 里改 log.format=text）
Environment=KONX_LOG_LEVEL=info

# 数据目录：与 config 的 web.data_dir 保持一致
# （SQLite 事件库 / kv 凭据表 / trace / HLS 分片都在这里）
StateDirectory=konx-gw
# 轻量加固
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/konx-gw
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
chmod 600 /etc/systemd/system/konx-gw.service   # 含凭据时务必收紧权限
systemctl daemon-reload
systemctl enable --now konx-gw
journalctl -u konx-gw -f                        # 首启打印 admin/nvr 两个一次性密码，立即保存
```

> config 中对应调整：`web.data_dir: /var/lib/konx-gw`（与 `StateDirectory` 一致）、`rtsp.listen/onvif.listen/web.listen` 保持缺省即可；`advertise.host` 填本机 IP 供 HA/NVR 拉流。

## 接入

| 消费者 | 地址 |
|---|---|
| Web 管理页 | `http://<宿主机>:8080/` |
| Home Assistant | 安装 `custom_components/konx`（步骤见根 README），配置 host+port+RTSP 凭据+API token |
| RTSP 拉流 | `rtsp://nvr:<密码>@<宿主机>:8554/konx/<设备ID>/main`（`/sub` 自动回退 main） |
| ONVIF（NVR/Frigate/Blue Iris 手动添加） | `http://<宿主机>:8000/onvif/device_service` |
| MQTT | `konx-gw/<gw_id>/#`（配置后） |

## 出网放行

`api*.cloudlinks.cn:443/tcp`、列表服务 `:51701/tcp+udp`、P2P 服务器 `udp 动态高位端口`。

## 注意

- **P2P 呼叫节流**：完整拆链后 15–30 分钟内重呼会被服务器拉黑（登录后零推送）。保持 `media.p2p.hard_idle` ≥15m、避免频繁观看；`session_hold` 常驻会话模式开发中。详见根 README「媒体源与 P2P 节流」。
- 设备 RTSP 并发=1：`media.source=device-rtsp` 需显式 `device_rtsp_exclusive_ack: true`，否则拒绝启动（保护 NVR 与设备）。`p2p-av` 不受此限。
- 与手机 App 共用账号会互踢；验证通过后建议切换独立账号（仅改 env）。
- 忘记 `nvr`/admin 密码：设置 `KONX_NVR_PASSWORD` / `KONX_ADMIN_PASSWORD` 并重新创建容器即可覆盖，保留原数据库。

## 相关文档

- 配置项总表与故障排查：[../README.md](../README.md)
- 设计文档：[../docs/DESIGN.md](../docs/DESIGN.md)
- P2P 协议技术档案另行内部维护

## GitHub Actions 镜像

仓库的 `.github/workflows/images.yml` 使用两个原生 runner 构建 amd64 和 arm64，沿用本 Dockerfile。两种架构均成功后合并为多架构镜像，拉取时 Docker 自动选择主机架构。

工作流仅接受手动触发，push、PR 和推送标签均不会自动运行。每次执行先运行 Go 构建、静态检查、测试及 HA 接口回归测试，再构建并发布镜像。

在 GitHub 仓库的 **Actions → 检查与双架构镜像 → Run workflow** 选择分支即可运行；也可使用 GitHub CLI：`gh workflow run images.yml --ref main`。

- 选择 `main`：发布 `ghcr.io/<仓库所有者>/konx-gw:latest` 和 `sha-<完整提交号>`。
- 选择其他分支：只发布提交标签。
- 通过 CLI 选择版本标签（如 `--ref v1.0.0`）：同时发布同名版本标签。

工作流使用内置 `GITHUB_TOKEN` 登录 GHCR，需允许该仓库写入 Packages。仓库名称会自动转换为小写；Fork 后镜像路径随仓库名称变化。每次构建的中间架构标签以 `build-` 开头。

首次发布后可用以下命令拉取；若包保持私有，需先登录 GHCR：

```bash
docker pull ghcr.io/<仓库所有者>/konx-gw:latest
docker buildx imagetools inspect ghcr.io/<仓库所有者>/konx-gw:latest
```

ONVIF 返回的快照 URL 支持网关已有的 nvr Basic 凭据；原有 API read 令牌也可使用。媒体凭据仅增加快照读取权限。MQTT 的“媒体连接”实体表示取流连接状态，空闲未取流时为关闭，不能作为设备物理离线的判据。

本地 HA 接口回归测试使用最小 HA 接口替身及模拟事件，可运行 `uv run --no-project --with aiohttp python -m unittest discover -s tests -v`（在仓库根目录）。它验证初始化、直播地址接口、补拉及去重，不替代真实 Home Assistant 和门铃按键验收。

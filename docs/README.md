# konx-gw 文档索引

| 文档 | 内容 |
|---|---|
| [../README.md](../README.md) | 项目定位、功能矩阵、快速开始、**配置项总表**、HA 接入、安全须知、故障排查 |
| [DESIGN.md](DESIGN.md) | 完整设计：架构决策（AD1–ADn）、模块设计、REST/WS 契约、配置设计（§13）、里程碑 |
| [../deploy/README.md](../deploy/README.md) | 部署：Docker Compose、**systemd 单元示例（中文注释）**、端口/防火墙、一次性凭据说明 |
| [../deploy/config.example.yaml](../deploy/config.example.yaml) | 带注释的配置示例 |

## P2P 协议资料

网关协议适配基于 gwelldemo 官方示例工程与官方 SDK 资料；P2P 协议技术档案另行内部维护。

## 排障速查

- **登录后零推送 / P2P 呼叫全平** → 节流黑名单（15–30 分钟窗）：等待或启用 session_hold（开发中），见根 README「媒体源与 P2P 节流」。
- **呼叫被设备 REJECT** → treated 密码链失败：凭据自动解析不成功时，手动计算并在 `media.p2p.treated_password` 直配兜底。
- **协议交互细节** → 网关开 `p2p.trace: true`（全帧落盘 `/data/trace/`）后分析（协议技术档案另行内部维护）。

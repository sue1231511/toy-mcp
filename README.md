# toy-mcp

让 AI 通过 MCP 远程控制情趣玩具。支持 [Intiface Central](https://intiface.com/central/)（Buttplug 协议 v4）能连上的玩具，已在 Lovense Velvo 和 KissToy 上实测。

```
AI 客户端 --SSE--> 云端中转 (main.py) <--反向 WebSocket-- 手机小桥 (bridge/bridge.py, Termux) --ws://127.0.0.1:12345--> Intiface Central --蓝牙--> 玩具
```

- **云端中转**：一个小 Python 服务，对外提供 MCP 工具，等手机小桥连上来。
- **手机小桥**：跑在安卓手机的 Termux 里，一头连云端，一头连同一台手机上的 Intiface Central。
- 所有定时、节奏都在手机上执行，网络卡一下不影响正在跑的指令；每条指令都有时长上限，到点自动停。
- Intiface Central 同一时间只接受一个客户端，所以一座小桥管所有玩具。

---

## 一、部署云端中转

任何能跑 Python、对外开放 HTTPS 的地方都可以。仓库根目录就是中转服务：

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080 --no-access-log
```

用 [Zeabur](https://zeabur.com) 的话，直接从 GitHub 导入这个仓库，`zbpack.json` 已经写好构建和启动命令，端口 8080，绑一个域名就行。

| 环境变量 | 说明 |
|---|---|
| `BRIDGE_TOKEN` | **必填**。手机小桥连上来用的密钥，两边填一样的 |
| `MCP_KEY` | **强烈建议必填**。设了以后连 MCP 和看 `/status` 都要带这把钥匙（带法见第三节）。**不设的话，任何知道你地址的人都能控制你的玩具** |
| `MAX_COMMAND_SECONDS` | 每条指令最长多少秒，默认 `600` |

密钥用足够长的随机字符串，比如 `openssl rand -hex 32` 生成。

端点：

- MCP：`/mcp/sse`（SSE 传输）
- 小桥：`/ws`
- 健康检查：`/ping`、`/status`

`/ping` 不需要钥匙，只回一个 ok，给平台做健康检查用。`/status` 会显示小桥在不在线，等于暴露你什么时候在用，所以设了 `MCP_KEY` 以后它也要钥匙。

---

## 二、手机小桥（安卓 + Termux）

### 第一次安装

1. 装 **Intiface Central** 和 **Termux**（Termux 建议从 F-Droid 或 GitHub 下载）。
2. Termux 里：

```bash
pkg install python git
git clone https://github.com/<你的用户名>/toy-mcp ~/toy-mcp
cd ~/toy-mcp/bridge
pip install -r requirements.txt
cp bridge.env.example bridge.env
```

3. 编辑 `bridge.env`（比如 `nano bridge.env`），填两项：

```
BRIDGE_CLOUD_URL=wss://你的中转域名/ws
BRIDGE_TOKEN=和云端一样的 BRIDGE_TOKEN
```

4. `start.sh` 启动时会用 `termux-wake-lock` 申请唤醒锁，减少手机息屏后被冻住；另外最好给 Termux 关掉电池优化。

### 每次使用

1. 打开玩具（开几个都行），**关掉玩具自家的 App**，不然 Intiface 连不上它。
2. 打开 Intiface Central，点紫色播放键，等它显示服务在运行。
3. Termux 里：

```bash
sh ~/toy-mcp/bridge/start.sh
```

看到这几行就是通了：

- `Intiface ready`：连上 Intiface
- `Cloud link up`：连上云端
- `Device online hint=...`：玩具被认出来了

用完在 Termux 里按 `CTRL` + `c`，小桥会先把所有玩具停下再退出。

浏览器打开 `https://你的中转域名/status?key=<你的 MCP_KEY>`，`"bridge_connected": true` 就是小桥在线。看完把这个地址从浏览器历史里删掉，别让钥匙留在里面。

### 小桥的设置（`bridge.env`）

| 变量 | 默认 | 说明 |
|---|---|---|
| `BRIDGE_CLOUD_URL` | 必填 | `wss://<中转域名>/ws` |
| `BRIDGE_TOKEN` | 必填 | 和云端一样 |
| `INTIFACE_URL` | `ws://127.0.0.1:12345` | Intiface Central 的地址 |
| `TOY_DEVICE_HINTS` | `velvo,kisstoy` | 要管哪些玩具：设备名里包含的一段字，不分大小写，逗号分隔。每段对应一个玩具 |
| `CHANNEL_ALIASES` | `kisstoy:vibrate1=震动,vibrate2=吮吸` | 给通道起名字，格式 `玩具:通道=名字,...`，多个玩具用 `;` 隔开。设成空就不用别名 |
| `VELVO_DEVICE_HINT` | `velvo` | 旧版单设备写法，只在没设 `TOY_DEVICE_HINTS` 时生效 |
| `MAX_COMMAND_SECONDS` | `600` | 手机这边也会再限一次 |
| `RAMP_TICK_SECONDS` | `0.5` | 节奏里渐变的步长 |
| `REASSERT_ON_CHANGE` | `true` | 改一个通道时把其他通道也重发一次。部分 Lovense 带旋转的设备需要；设 `false` 只发改动的那个 |
| `SCAN_RETRY_SECONDS` | `10` | 玩具没连上时多久重新扫一次 |
| `BRIDGE_LOG_FILE` | 无 | 想存日志文件就填路径 |

### 加一个新玩具

1. 开机后在 Intiface 的 **Devices** 里能看到它，就说明 Intiface 支持。看不到基本就是不支持。
2. 记下它的名字（比如 `Kisstoy Lost (Sucking)`），把其中一段加进 `TOY_DEVICE_HINTS`，比如 `velvo,kisstoy,lush`。
3. 点进设备看 Device Controls：每个滑条就是一个通道。同类型只有一个时叫 `vibrate`、`rotate` 这样；有好几个时按顺序叫 `vibrate1`、`vibrate2`……顺序和 Intiface 里滑条从上到下一致。
4. 想用中文名，就在 `CHANNEL_ALIASES` 里加，比如 `lush:vibrate=震动`。
5. 重启小桥。

---

## 三、接到 AI 客户端

MCP 地址：`https://你的中转域名/mcp/sse`，传输方式选 **SSE**，认证选**不需要登录**。

钥匙三种带法，中转都认，按客户端支持的选，**能用请求头就用请求头**：

| 带法 | 写法 |
|---|---|
| 请求头 | `X-API-Key: <你的 MCP_KEY>` |
| 请求头 | `Authorization: Bearer <你的 MCP_KEY>` |
| 地址参数 | `https://你的中转域名/mcp/sse?key=<你的 MCP_KEY>` |

钥匙放在地址里，可能被平台的反向代理原样记进访问日志，只在客户端填不了请求头时用。

支持 JSON 配置的客户端：

```json
{
  "mcpServers": {
    "toy": {
      "type": "sse",
      "url": "https://你的中转域名/mcp/sse",
      "headers": { "X-API-Key": "<你的 MCP_KEY>" }
    }
  }
}
```

### 工具

任意玩具：

- `toy_list(battery=False)`：列出配置的玩具、是否在线、每个通道的名字/别名/范围、当前强度、电量
- `toy_set(device, levels, seconds)`：设强度，比如 `toy_set("kisstoy", {"吮吸": 30}, 60)`；0 是停
- `toy_pattern(device, steps, repeat)`：下发一段节奏，每步 `{"levels": {...}, "duration": 秒, "ramp": true/false}`
- `toy_stop(device=None)`：停一个玩具；**不填 device 就是全部停下**

Lovense Velvo 专用（老接口，保留兼容）：`velvo_status`、`velvo_vibrate`、`velvo_rotate`、`velvo_set`、`velvo_pattern`、`velvo_stop`。`velvo_stop` 同样会停下**所有**玩具。

建议你和你的 AI 约定一个安全词，说出来就调 `toy_stop()` 全部停止。

---

## 四、出问题的时候

| 看到什么 | 原因 | 怎么办 |
|---|---|---|
| 客户端连不上，返回 `401` | MCP 钥匙没带上或者不对 | 中转日志里那行 `refused` 后面写着这次带了哪种钥匙（`has_x_api_key` / `has_bearer` / `has_query_key`），对照着查 |
| 小桥日志 `HTTP 403` | 手机和云端的 `BRIDGE_TOKEN` 不一样 | 两边改成一样；平台环境变量的值只填等号后面的部分 |
| 一直 `Reconnecting to Intiface` | Intiface 服务没开 | 打开 Intiface 点播放键 |
| 没有 `Device online` | 玩具没开机，或者被它自家 App 连着 | 打开玩具，关掉其他连着它的 App |
| `matches none of hints` | 这个玩具的名字不在 `TOY_DEVICE_HINTS` 里 | 把它名字里的一段加进去 |
| 两个通道效果反了 | 别名和实际滑条对不上 | 在 `CHANNEL_ALIASES` 里对调 |
| 小桥过一会儿自己断了 | 手机把 Termux 冻住了 | 给 Termux 和 Intiface 关掉电池优化、允许后台运行，最近任务里锁住它们 |
| `git pull` 要用户名密码 | 仓库是你自己的私有 fork | 用户名填 GitHub 用户名，密码填 GitHub token（不是登录密码） |

---

## 许可

MIT，见 [LICENSE](LICENSE)。

只在双方都成年、都同意的情况下使用。

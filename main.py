"""
Toy Remote MCP - cloud relay (Velvo + KissToy, any toy the phone bridge is configured for)
- WebSocket endpoint /ws : the phone bridge (Termux) connects here (reverse connection)
- MCP tools via FastMCP, SSE at /mcp/sse (same layout as the phone bridge repo)
- HTTP: /ping, /status
- Deploy on Zeabur, port 8080

Env:
  BRIDGE_TOKEN         required. Shared secret, the bridge sends it in header X-Bridge-Token
  MCP_KEY              optional. When set, /mcp/sse requires ?key=<MCP_KEY>
  MAX_COMMAND_SECONDS  optional. Per-command duration ceiling in seconds (default 600)
"""

import asyncio
import hmac
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import parse_qs

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s.%(funcName)s:%(lineno)d %(message)s",
)
log = logging.getLogger("velvo.cloud")
logging.getLogger("mcp").setLevel(logging.WARNING)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        log.exception("Invalid number in env %s=%r", name, raw)
        raise
    if value <= 0:
        log.error("Env %s must be > 0, got %r", name, raw)
        raise ValueError("env {} must be > 0".format(name))
    return value


BRIDGE_TOKEN = os.getenv("BRIDGE_TOKEN", "")
MCP_KEY = os.getenv("MCP_KEY", "")
MAX_COMMAND_SECONDS = _env_float("MAX_COMMAND_SECONDS", 600.0)

# Device ranges read from Intiface Central (Lovense Velvo): Vibrate 0..20, Rotate -20..20.
# The bridge re-validates against the ranges the device really advertises.
VIBRATE_MIN, VIBRATE_MAX = 0, 20
ROTATE_MIN, ROTATE_MAX = -20, 20
MAX_PATTERN_STEPS = 200
COMMAND_TIMEOUT = 15.0
BRIDGE_REPLACED_CLOSE_CODE = 4001

bridge_ws: Optional[WebSocket] = None
bridge_hello: dict = {}
pending: dict = {}
_send_lock = asyncio.Lock()


class BridgeUnavailable(RuntimeError):
    pass


class BridgeTimeout(TimeoutError):
    pass


class BridgeCommandError(RuntimeError):
    pass


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if not BRIDGE_TOKEN:
        log.error("BRIDGE_TOKEN is not set: every /ws connection will be refused.")
    if not MCP_KEY:
        log.warning("MCP_KEY is not set: /mcp/sse is reachable by anyone who knows the URL.")
    log.info("Server starting up. MAX_COMMAND_SECONDS=%s", MAX_COMMAND_SECONDS)
    yield
    log.info("Server shutting down...")
    await _cleanup_bridge(bridge_ws)


app = FastAPI(title="Velvo Remote Relay", lifespan=lifespan)
mcp = FastMCP(
    name="velvo",
    instructions=(
        "Remote control for toys (Lovense Velvo, KissToy, ...) through one phone bridge. "
        "velvo_* tools control the Velvo; toy_* tools control any toy by name (call toy_list first to see "
        "connected toys and their channels). Every command has a duration ceiling. "
        "velvo_stop and toy_stop() without a device stop EVERY toy at once."
    ),
)


def _fail_pending(reason: str) -> None:
    for fut in list(pending.values()):
        if not fut.done():
            fut.set_exception(ConnectionError(reason))
    pending.clear()


async def _cleanup_bridge(ws: Optional[WebSocket]) -> None:
    global bridge_ws, bridge_hello
    if ws is None or bridge_ws is not ws:
        return
    bridge_ws = None
    bridge_hello = {}
    _fail_pending("Bridge disconnected")
    log.warning("Bridge disconnected - pending commands cancelled.")


async def send_command(cmd: str, params: Optional[dict] = None, timeout: float = COMMAND_TIMEOUT) -> Any:
    ws = bridge_ws
    if ws is None:
        raise BridgeUnavailable("手机小桥未连接（手机上的 bridge 没在运行或网络断了）")

    cid = uuid.uuid4().hex
    fut = asyncio.get_running_loop().create_future()
    pending[cid] = fut
    payload = json.dumps({"id": cid, "cmd": cmd, "params": params or {}}, ensure_ascii=False)
    try:
        async with _send_lock:
            await ws.send_text(payload)
        msg = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError as exc:
        log.error("Command timed out cmd=%s params=%r timeout=%ss", cmd, params, timeout)
        raise BridgeTimeout("指令 {} 在 {} 秒内没有收到小桥回复".format(cmd, timeout)) from exc
    finally:
        pending.pop(cid, None)

    if not msg.get("ok"):
        raise BridgeCommandError(str(msg.get("error", "unknown bridge error")))
    return msg.get("result")


def _check_level(name: str, value: Optional[int], low: int, high: int) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("{} 必须是整数，收到 {!r}".format(name, value))
    if value < low or value > high:
        raise ValueError("{}={} 超出范围 [{}, {}]".format(name, value, low, high))
    return value


def _check_seconds(seconds: Optional[float]) -> float:
    if seconds is None:
        return MAX_COMMAND_SECONDS
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise ValueError("seconds 必须是数字，收到 {!r}".format(seconds))
    if seconds <= 0:
        raise ValueError("seconds 必须大于 0，收到 {}".format(seconds))
    if seconds > MAX_COMMAND_SECONDS:
        raise ValueError("seconds={} 超过单条指令上限 {} 秒".format(seconds, MAX_COMMAND_SECONDS))
    return float(seconds)


def _ok(result: Any) -> str:
    return json.dumps({"ok": True, "result": result}, ensure_ascii=False)


def _err(message: str) -> str:
    return json.dumps({"ok": False, "error": message}, ensure_ascii=False)


async def _run(tool: str, cmd: str, params: dict, timeout: float = COMMAND_TIMEOUT) -> str:
    try:
        return _ok(await send_command(cmd, params, timeout))
    except (BridgeUnavailable, BridgeTimeout, BridgeCommandError) as exc:
        log.warning("tool=%s cmd=%s params=%r failed: %s", tool, cmd, params, exc, exc_info=True)
        return _err(str(exc))
    except ConnectionError as exc:
        log.warning("tool=%s cmd=%s params=%r bridge dropped: %s", tool, cmd, params, exc, exc_info=True)
        return _err("小桥在指令执行期间断开：{}".format(exc))
    except Exception as exc:
        log.exception("tool=%s cmd=%s params=%r unexpected error", tool, cmd, params)
        return _err("内部错误：{!r}".format(exc))


@mcp.tool()
async def velvo_status(battery: bool = True) -> str:
    """查看 Velvo 当前状态：小桥是否在线、Intiface 与设备是否连上、当前档位、是否有定时/节奏在运行、剩余秒数、电量。"""
    if bridge_ws is None:
        return _ok({"bridge_connected": False})
    return await _run("velvo_status", "status", {"battery": bool(battery)})


@mcp.tool()
async def velvo_vibrate(level: int, seconds: Optional[float] = None) -> str:
    """设置兔耳朵（外部）震动档位，0~20，0 为停。滚珠保持当前档位不变。
    seconds: 持续秒数，不填则使用上限；到时自动全部停止。新指令会取代正在运行的指令和定时。"""
    try:
        params = {"vibrate": _check_level("level", level, VIBRATE_MIN, VIBRATE_MAX), "seconds": _check_seconds(seconds)}
    except ValueError as exc:
        log.warning("velvo_vibrate invalid args level=%r seconds=%r: %s", level, seconds, exc, exc_info=True)
        return _err(str(exc))
    return await _run("velvo_vibrate", "set", params)


@mcp.tool()
async def velvo_rotate(level: int, seconds: Optional[float] = None) -> str:
    """设置滚珠（内部）转速，-20~20，正数与负数方向相反，0 为停。兔耳朵保持当前档位不变。
    seconds: 持续秒数，不填则使用上限；到时自动全部停止。新指令会取代正在运行的指令和定时。"""
    try:
        params = {"rotate": _check_level("level", level, ROTATE_MIN, ROTATE_MAX), "seconds": _check_seconds(seconds)}
    except ValueError as exc:
        log.warning("velvo_rotate invalid args level=%r seconds=%r: %s", level, seconds, exc, exc_info=True)
        return _err(str(exc))
    return await _run("velvo_rotate", "set", params)


@mcp.tool()
async def velvo_set(
    vibrate: Optional[int] = None,
    rotate: Optional[int] = None,
    seconds: Optional[float] = None,
) -> str:
    """同时设置兔耳朵震动（vibrate 0~20）和滚珠（rotate -20~20，负数反向）。留空的一项保持当前档位不变。
    seconds: 持续秒数，不填则使用上限；到时自动全部停止。新指令会取代正在运行的指令和定时。"""
    try:
        params = {
            "vibrate": _check_level("vibrate", vibrate, VIBRATE_MIN, VIBRATE_MAX),
            "rotate": _check_level("rotate", rotate, ROTATE_MIN, ROTATE_MAX),
            "seconds": _check_seconds(seconds),
        }
        if params["vibrate"] is None and params["rotate"] is None:
            raise ValueError("vibrate 和 rotate 至少要指定一个")
    except ValueError as exc:
        log.warning(
            "velvo_set invalid args vibrate=%r rotate=%r seconds=%r: %s", vibrate, rotate, seconds, exc, exc_info=True
        )
        return _err(str(exc))
    return await _run("velvo_set", "set", params)


@mcp.tool()
async def velvo_pattern(steps: list[dict], repeat: int = 1) -> str:
    """下发一段节奏，由手机小桥在本地按时间执行（不受网络卡顿影响）。
    steps: 步骤列表，每步是对象：
      {"vibrate": 0~20, "rotate": -20~20, "duration": 秒数(>0), "ramp": true/false}
      vibrate/rotate 省略表示该通道保持上一步的档位；ramp 为 true 时，档位在本步 duration 内从上一步线性过渡到本步目标值。
    repeat: 整段重复次数。整段总时长（所有 duration 之和 × repeat）不得超过单条指令上限。
    节奏播放完毕自动全部停止。新指令会取代正在运行的节奏。"""
    try:
        if not isinstance(steps, list) or not steps:
            raise ValueError("steps 必须是非空列表")
        if len(steps) > MAX_PATTERN_STEPS:
            raise ValueError("steps 最多 {} 步，收到 {} 步".format(MAX_PATTERN_STEPS, len(steps)))
        if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
            raise ValueError("repeat 必须是 >= 1 的整数，收到 {!r}".format(repeat))
        clean = []
        total = 0.0
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                raise ValueError("steps[{}] 必须是对象".format(i))
            unknown = set(step) - {"vibrate", "rotate", "duration", "ramp"}
            if unknown:
                raise ValueError("steps[{}] 含未知字段 {}".format(i, sorted(unknown)))
            duration = step.get("duration")
            if isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration <= 0:
                raise ValueError("steps[{}].duration 必须是大于 0 的数字，收到 {!r}".format(i, duration))
            ramp = step.get("ramp", False)
            if not isinstance(ramp, bool):
                raise ValueError("steps[{}].ramp 必须是布尔值，收到 {!r}".format(i, ramp))
            clean.append(
                {
                    "vibrate": _check_level("steps[{}].vibrate".format(i), step.get("vibrate"), VIBRATE_MIN, VIBRATE_MAX),
                    "rotate": _check_level("steps[{}].rotate".format(i), step.get("rotate"), ROTATE_MIN, ROTATE_MAX),
                    "duration": float(duration),
                    "ramp": ramp,
                }
            )
            total += float(duration)
        total *= repeat
        if total > MAX_COMMAND_SECONDS:
            raise ValueError("整段节奏总时长 {:.1f} 秒超过单条指令上限 {} 秒".format(total, MAX_COMMAND_SECONDS))
    except ValueError as exc:
        log.warning("velvo_pattern invalid args steps=%r repeat=%r: %s", steps, repeat, exc, exc_info=True)
        return _err(str(exc))
    return await _run("velvo_pattern", "pattern", {"steps": clean, "repeat": repeat})


@mcp.tool()
async def velvo_stop() -> str:
    """立刻停止所有玩具（不只是 Velvo），并取消所有正在运行的定时和节奏。安全词“病危”对应的第一动作。"""
    return await _run("velvo_stop", "stop", {"all": True})


# ---- multi-toy tools --------------------------------------------------------------------------------


def _check_device(device: Any) -> str:
    if not isinstance(device, str) or not device.strip():
        raise ValueError("device 必须是非空字符串，例如 \"kisstoy\" 或 \"velvo\"，收到 {!r}".format(device))
    return device.strip().lower()


def _check_levels(label: str, levels: Any) -> dict:
    if not isinstance(levels, dict) or not levels:
        raise ValueError("{} 必须是非空对象，例如 {{\"吮吸\": 60}}".format(label))
    clean = {}
    for key, value in levels.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("{} 的通道名必须是非空字符串，收到 {!r}".format(label, key))
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("{}.{} 必须是整数，收到 {!r}".format(label, key, value))
        clean[key.strip()] = value
    return clean


@mcp.tool()
async def toy_list(battery: bool = False) -> str:
    """列出小桥配置的所有玩具：是否连上、每个玩具有哪些通道（名字、别名、类型、强度范围）、当前强度、是否在跑定时/节奏。
    battery=true 时顺便读电量（会稍慢）。用 toy_set 之前先看这里，通道名和范围以这里为准。"""
    if bridge_ws is None:
        return _ok({"bridge_connected": False})
    return await _run("toy_list", "devices", {"battery": bool(battery)})


@mcp.tool()
async def toy_set(device: str, levels: dict[str, int], seconds: Optional[float] = None) -> str:
    """设置某个玩具一个或多个通道的强度，0 为停。没写到的通道保持当前强度。
    device: 玩具名，如 "kisstoy"、"velvo"（见 toy_list）。
    levels: {通道: 强度}。通道可用别名（KissToy："震动"、"吮吸"）、通道名（"vibrate1"）、或类型名（"vibrate" 表示该类型全部通道）。
            强度范围以 toy_list 里每个通道的 range 为准（KissToy 两个通道都是 0~100）。
    seconds: 持续秒数，不填则使用上限；到时该玩具自动全部停止。新指令会取代这个玩具正在运行的指令，不影响其他玩具。"""
    try:
        params = {"device": _check_device(device), "levels": _check_levels("levels", levels), "seconds": _check_seconds(seconds)}
    except ValueError as exc:
        log.warning("toy_set invalid args device=%r levels=%r seconds=%r: %s", device, levels, seconds, exc, exc_info=True)
        return _err(str(exc))
    return await _run("toy_set", "set", params)


@mcp.tool()
async def toy_pattern(device: str, steps: list[dict], repeat: int = 1) -> str:
    """给某个玩具下发一段节奏，由手机小桥在本地按时间执行（不受网络卡顿影响），不影响其他玩具。
    steps: 步骤列表，每步：{"levels": {通道: 强度}, "duration": 秒数(>0), "ramp": true/false}
      levels 里没写的通道保持上一步的强度；levels 可省略表示整步保持不变；
      ramp 为 true 时在本步 duration 内从上一步线性过渡到本步目标。通道写法同 toy_set。
    repeat: 整段重复次数。整段总时长（所有 duration 之和 × repeat）不得超过单条指令上限。
    播放完毕该玩具自动停止。"""
    try:
        params_device = _check_device(device)
        if not isinstance(steps, list) or not steps:
            raise ValueError("steps 必须是非空列表")
        if len(steps) > MAX_PATTERN_STEPS:
            raise ValueError("steps 最多 {} 步，收到 {} 步".format(MAX_PATTERN_STEPS, len(steps)))
        if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
            raise ValueError("repeat 必须是 >= 1 的整数，收到 {!r}".format(repeat))
        clean = []
        total = 0.0
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                raise ValueError("steps[{}] 必须是对象".format(i))
            unknown = set(step) - {"levels", "duration", "ramp"}
            if unknown:
                raise ValueError("steps[{}] 含未知字段 {}".format(i, sorted(unknown)))
            duration = step.get("duration")
            if isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration <= 0:
                raise ValueError("steps[{}].duration 必须是大于 0 的数字，收到 {!r}".format(i, duration))
            ramp = step.get("ramp", False)
            if not isinstance(ramp, bool):
                raise ValueError("steps[{}].ramp 必须是布尔值，收到 {!r}".format(i, ramp))
            levels = step.get("levels")
            item = {"duration": float(duration), "ramp": ramp}
            if levels:
                item["levels"] = _check_levels("steps[{}].levels".format(i), levels)
            clean.append(item)
            total += float(duration)
        total *= repeat
        if total > MAX_COMMAND_SECONDS:
            raise ValueError("整段节奏总时长 {:.1f} 秒超过单条指令上限 {} 秒".format(total, MAX_COMMAND_SECONDS))
    except ValueError as exc:
        log.warning("toy_pattern invalid args device=%r steps=%r repeat=%r: %s", device, steps, repeat, exc, exc_info=True)
        return _err(str(exc))
    return await _run("toy_pattern", "pattern", {"device": params_device, "steps": clean, "repeat": repeat})


@mcp.tool()
async def toy_stop(device: Optional[str] = None) -> str:
    """停止玩具并取消它的定时和节奏。device 不填 = 立刻停止所有玩具（安全词“病危”用这个）；填了只停那一个。"""
    if device is None:
        return await _run("toy_stop", "stop", {"all": True})
    try:
        params = {"device": _check_device(device)}
    except ValueError as exc:
        log.warning("toy_stop invalid args device=%r: %s", device, exc, exc_info=True)
        return _err(str(exc))
    return await _run("toy_stop", "stop", params)


@app.get("/ping")
async def ping():
    return {"status": "ok"}


@app.get("/status")
async def status():
    return {"bridge_connected": bridge_ws is not None, "bridge_token_configured": bool(BRIDGE_TOKEN)}


def _token_ok(candidate: str, expected: str) -> bool:
    if not expected:
        return False
    return hmac.compare_digest(candidate.encode("utf-8"), expected.encode("utf-8"))


async def _reader(ws: WebSocket) -> None:
    global bridge_hello
    try:
        while True:
            data = await ws.receive()
            if data.get("type") == "websocket.disconnect":
                log.info("Bridge sent disconnect frame.")
                break
            text = data.get("text")
            if text is None:
                log.warning("Bridge sent a non-text frame, ignoring.")
                continue
            try:
                msg = json.loads(text)
            except json.JSONDecodeError:
                log.exception("Bridge sent invalid JSON: %r", text[:200])
                continue
            if not isinstance(msg, dict):
                log.error("Bridge sent a non-object message: %r", text[:200])
                continue

            if "event" in msg:
                if msg.get("event") == "hello":
                    bridge_hello = msg.get("state") or {}
                    log.info("Bridge hello: %s", json.dumps(bridge_hello, ensure_ascii=False))
                else:
                    log.info("Bridge event: %s", json.dumps(msg, ensure_ascii=False)[:300])
                continue

            mid = msg.get("id")
            fut = pending.get(mid)
            if fut is not None and not fut.done():
                fut.set_result(msg)
            else:
                log.debug("Unmatched bridge reply id=%s", mid)
    except WebSocketDisconnect:
        log.info("Bridge WebSocket disconnected client=%s", ws.client)
    except Exception:
        log.exception("reader() error client=%s", ws.client)
    finally:
        await _cleanup_bridge(ws)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    global bridge_ws
    candidate = ws.headers.get("x-bridge-token", "")
    if not _token_ok(candidate, BRIDGE_TOKEN):
        log.warning(
            "Bridge refused client=%s token_configured=%s token_sent=%s",
            ws.client,
            bool(BRIDGE_TOKEN),
            bool(candidate),
        )
        await ws.close(code=1008)
        return

    await ws.accept()
    old = bridge_ws
    bridge_ws = ws
    if old is not None:
        log.warning("A new bridge replaced the previous one. old=%s new=%s", old.client, ws.client)
        _fail_pending("Bridge replaced by a new connection")
        try:
            await old.close(code=BRIDGE_REPLACED_CLOSE_CODE)
        except Exception:
            log.exception("Failed to close replaced bridge client=%s", old.client)
    log.info("Bridge connected: %s", ws.client)
    await _reader(ws)


sse_transport = SseServerTransport("/mcp/messages/")


def _query_key(scope: dict) -> str:
    query = parse_qs(scope.get("query_string", b"").decode("latin-1"))
    values = query.get("key", [])
    return values[0] if values else ""


async def _reject(send, status_code: int, text: str) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status_code,
            "headers": [(b"content-type", b"text/plain; charset=utf-8")],
        }
    )
    await send({"type": "http.response.body", "body": text.encode("utf-8")})


class MCPMiddleware:
    def __init__(self, asgi_app):
        self.asgi_app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http":
            path = scope.get("path", "")
            method = scope.get("method", "GET")

            if path == "/mcp/sse" and method == "GET":
                if MCP_KEY and not _token_ok(_query_key(scope), MCP_KEY):
                    log.warning("MCP SSE refused: bad or missing key client=%s", scope.get("client"))
                    await _reject(send, 401, "unauthorized")
                    return
                try:
                    async with sse_transport.connect_sse(scope, receive, send) as (r, w):
                        await mcp._mcp_server.run(
                            r,
                            w,
                            mcp._mcp_server.create_initialization_options(),
                        )
                except Exception:
                    log.exception("SSE handler error client=%s", scope.get("client"))
                return

            if path in ("/mcp/messages/", "/mcp/messages") and method == "POST":
                try:
                    await sse_transport.handle_post_message(scope, receive, send)
                except Exception:
                    log.exception("Messages handler error client=%s", scope.get("client"))
                return

        await self.asgi_app(scope, receive, send)


app = MCPMiddleware(app)

"""
Toy phone bridge (runs in Termux on the phone)

Local side : Intiface Central WebSocket server, Buttplug protocol v4 (JSON over WebSocket)
Cloud side : reverse WebSocket to the Zeabur relay (/ws), commands arrive from the MCP tools

Intiface Central accepts a single client, so this one bridge manages every configured toy
(TOY_DEVICE_HINTS, default "velvo,kisstoy"). Each toy has its own channels, levels, timer and pattern.

Channels:
- Every output feature of a device becomes one channel. When a device has a single feature of a type the
  channel is named after the type ("vibrate", "rotate"); with several it is "vibrate1", "vibrate2", ...
  numbered in FeatureIndex order (the same order Intiface Central lists the sliders).
- The bare type name ("vibrate") always addresses every feature of that type at once, so the original
  Velvo commands keep working unchanged.
- CHANNEL_ALIASES gives channels human names, e.g. KissToy "vibrate1=震动,vibrate2=吮吸".

Behaviour:
- Every command is bounded by MAX_COMMAND_SECONDS and executed locally (timers/patterns run here).
- Losing the cloud link does NOT stop a running command; it finishes on its own timer.
- Losing Intiface or a device resets that device's state; nothing is resumed automatically.
- SIGINT/SIGTERM stops every device before exiting.
"""

import asyncio
import json
import logging
import logging.handlers
import math
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("toy.bridge")

PROTOCOL_MAJOR = 4
PROTOCOL_MINOR = 0
CLOUD_REPLACED_CLOSE_CODE = 4001
DEFAULT_DEVICE = "velvo"
DEFAULT_ALIASES = "kisstoy:vibrate1=震动,vibrate2=吮吸"


class BridgeError(Exception):
    """Expected, reportable failure."""


class ConfigError(BridgeError):
    pass


class IntifaceError(BridgeError):
    pass


class ControllerError(BridgeError):
    pass


def setup_logging(log_file: Optional[str]) -> None:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s.%(funcName)s:%(lineno)d %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8"
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def _parse_hints(raw: str) -> tuple:
    hints = []
    for part in raw.split(","):
        hint = part.strip().lower()
        if not hint:
            continue
        if hint in hints:
            raise ConfigError("TOY_DEVICE_HINTS contains {!r} twice".format(hint))
        hints.append(hint)
    if not hints:
        raise ConfigError("TOY_DEVICE_HINTS is empty")
    for a in hints:
        for b in hints:
            if a != b and a in b:
                raise ConfigError(
                    "TOY_DEVICE_HINTS {!r} is a substring of {!r}; a device named like {!r} would match both".format(a, b, b)
                )
    return tuple(hints)


def _parse_aliases(raw: str, hints: tuple) -> dict:
    """'kisstoy:vibrate1=震动,vibrate2=吮吸;velvo:vibrate=兔耳朵' -> {hint: {alias: channel}}"""
    result: dict = {}
    for block in raw.split(";"):
        block = block.strip()
        if not block:
            continue
        if ":" not in block:
            raise ConfigError("CHANNEL_ALIASES block {!r} must look like hint:channel=name,...".format(block))
        hint, body = block.split(":", 1)
        hint = hint.strip().lower()
        if hint not in hints:
            raise ConfigError("CHANNEL_ALIASES names hint {!r} which is not in TOY_DEVICE_HINTS {}".format(hint, list(hints)))
        mapping = result.setdefault(hint, {})
        for pair in body.split(","):
            pair = pair.strip()
            if not pair:
                continue
            if "=" not in pair:
                raise ConfigError("CHANNEL_ALIASES entry {!r} must look like channel=name".format(pair))
            channel, alias = (x.strip() for x in pair.split("=", 1))
            if not channel or not alias:
                raise ConfigError("CHANNEL_ALIASES entry {!r} has an empty side".format(pair))
            if alias in mapping:
                raise ConfigError("CHANNEL_ALIASES alias {!r} used twice for {!r}".format(alias, hint))
            mapping[alias] = channel.lower()
    return result


@dataclass(frozen=True)
class Config:
    cloud_url: str
    token: str
    intiface_url: str
    device_hints: tuple
    aliases: dict
    max_seconds: float
    ramp_tick: float
    reassert_all: bool
    scan_retry: float
    log_file: Optional[str]
    max_steps: int = 200

    @staticmethod
    def from_env() -> "Config":
        def required(name: str) -> str:
            value = os.getenv(name, "").strip()
            if not value:
                raise ConfigError("missing required env {}".format(name))
            return value

        def number(name: str, default: float) -> float:
            raw = os.getenv(name, "").strip()
            if not raw:
                return default
            try:
                value = float(raw)
            except ValueError as exc:
                raise ConfigError("env {}={!r} is not a number".format(name, raw)) from exc
            if value <= 0:
                raise ConfigError("env {}={!r} must be > 0".format(name, raw))
            return value

        def boolean(name: str, default: bool) -> bool:
            raw = os.getenv(name, "").strip().lower()
            if not raw:
                return default
            if raw in ("1", "true", "yes", "on"):
                return True
            if raw in ("0", "false", "no", "off"):
                return False
            raise ConfigError("env {}={!r} is not a boolean".format(name, raw))

        cloud_url = required("BRIDGE_CLOUD_URL")
        if not cloud_url.startswith(("ws://", "wss://")):
            raise ConfigError("BRIDGE_CLOUD_URL must start with ws:// or wss://, got {!r}".format(cloud_url))
        intiface_url = os.getenv("INTIFACE_URL", "ws://127.0.0.1:12345").strip()
        if not intiface_url.startswith(("ws://", "wss://")):
            raise ConfigError("INTIFACE_URL must start with ws:// or wss://, got {!r}".format(intiface_url))

        # TOY_DEVICE_HINTS wins; otherwise keep honouring the old single-device VELVO_DEVICE_HINT and add KissToy.
        hints_raw = os.getenv("TOY_DEVICE_HINTS", "").strip()
        if not hints_raw:
            hints_raw = (os.getenv("VELVO_DEVICE_HINT", "").strip() or DEFAULT_DEVICE) + ",kisstoy"
        hints = _parse_hints(hints_raw)
        aliases_raw = os.getenv("CHANNEL_ALIASES")
        if aliases_raw is None:
            aliases_raw = DEFAULT_ALIASES if "kisstoy" in hints else ""

        return Config(
            cloud_url=cloud_url,
            token=required("BRIDGE_TOKEN"),
            intiface_url=intiface_url,
            device_hints=hints,
            aliases=_parse_aliases(aliases_raw, hints),
            max_seconds=number("MAX_COMMAND_SECONDS", 600.0),
            ramp_tick=number("RAMP_TICK_SECONDS", 0.5),
            reassert_all=boolean("REASSERT_ON_CHANGE", True),
            scan_retry=number("SCAN_RETRY_SECONDS", 10.0),
            log_file=os.getenv("BRIDGE_LOG_FILE", "").strip() or None,
        )


@dataclass(frozen=True)
class Channel:
    name: str     # "vibrate", "vibrate1", "rotate", ...
    otype: str    # Buttplug output type: "Vibrate", "Rotate", "Oscillate", ...
    feature: int  # FeatureIndex
    lo: int
    hi: int


@dataclass
class DeviceInfo:
    name: str
    index: int
    gap_ms: int
    channels: list            # list[Channel], ordered by (type, FeatureIndex)
    battery_feature: Optional[int]
    groups: dict = field(default_factory=dict)  # lowercase type name -> list[channel name]

    def channel(self, name: str) -> Optional[Channel]:
        for ch in self.channels:
            if ch.name == name:
                return ch
        return None

    def describe(self, aliases: dict) -> dict:
        back = {v: k for k, v in aliases.items()}
        return {
            "index": self.index,
            "channels": [
                {"name": ch.name, "alias": back.get(ch.name), "type": ch.otype, "range": [ch.lo, ch.hi]}
                for ch in self.channels
            ],
            "battery_readable": self.battery_feature is not None,
        }


def build_channels(features_by_type: dict) -> tuple:
    """features_by_type: {otype: [(fidx, lo, hi), ...]} -> (channels, groups)"""
    channels = []
    groups: dict = {}
    for otype in sorted(features_by_type):
        feats = sorted(features_by_type[otype], key=lambda f: f[0])
        base = otype.lower()
        names = [base] if len(feats) == 1 else ["{}{}".format(base, i + 1) for i in range(len(feats))]
        for name, (fidx, lo, hi) in zip(names, feats):
            channels.append(Channel(name=name, otype=otype, feature=fidx, lo=lo, hi=hi))
        groups[base] = names
    return channels, groups


class IntifaceLink:
    """Buttplug v4 client over the local Intiface Central WebSocket; tracks one device per configured hint."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.ws = None
        self.ready = asyncio.Event()
        self.devices: dict = {}  # hint -> DeviceInfo
        self.on_device_lost: Optional[Callable[[str], Awaitable[None]]] = None
        self._pending: dict = {}
        self._next_id = 1
        self._max_ping_ms = 0
        self._was_ready = False
        self._bg: set = set()
        self._unmatched_warned: set = set()

    # ---- lifecycle -------------------------------------------------------------------------

    async def run_forever(self) -> None:
        delay = 1.0
        while True:
            try:
                await self._session()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Intiface session failed url=%s was_ready=%s", self.cfg.intiface_url, self._was_ready)
            delay = 1.0 if self._was_ready else min(delay * 2, 30.0)
            log.info("Reconnecting to Intiface in %.1fs (start the server in Intiface Central if it is not running)", delay)
            await asyncio.sleep(delay)

    async def _session(self) -> None:
        self._was_ready = False
        log.info("Connecting to Intiface url=%s", self.cfg.intiface_url)
        async with connect(
            self.cfg.intiface_url, open_timeout=10, ping_interval=None, max_size=4 * 1024 * 1024
        ) as ws:
            self.ws = ws
            self._next_id = 1
            recv = asyncio.create_task(self._recv_loop(ws), name="intiface-recv")
            aux: list = []
            try:
                await self._handshake()
                await self.request("RequestDeviceList", {}, timeout=10)
                self.ready.set()
                self._was_ready = True
                log.info("Intiface ready max_ping_ms=%s", self._max_ping_ms)
                if self._max_ping_ms > 0:
                    aux.append(asyncio.create_task(self._ping_loop(), name="intiface-ping"))
                aux.append(asyncio.create_task(self._scan_loop(), name="intiface-scan"))
                done, _ = await asyncio.wait({recv, *aux}, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
                log.info("Intiface connection ended")
            finally:
                self.ready.clear()
                tasks = [recv, *aux]
                for task in tasks:
                    task.cancel()
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for task, result in zip(tasks, results):
                    if isinstance(result, Exception):
                        log.error(
                            "Intiface task %s ended with error",
                            task.get_name(),
                            exc_info=(type(result), result, result.__traceback__),
                        )
                self.ws = None
                self._fail_pending("Intiface connection closed")
                self._drop_all("Intiface session ended")

    async def _handshake(self) -> None:
        name, payload = await self.request(
            "RequestServerInfo",
            {
                "ClientName": "toy-bridge",
                "ProtocolVersionMajor": PROTOCOL_MAJOR,
                "ProtocolVersionMinor": PROTOCOL_MINOR,
            },
            timeout=10,
        )
        if name != "ServerInfo":
            raise IntifaceError("handshake expected ServerInfo, got {}: {!r}".format(name, payload))
        major = payload.get("ProtocolVersionMajor")
        if major != PROTOCOL_MAJOR:
            raise IntifaceError(
                "server selected protocol major {!r}, this bridge speaks v{} only".format(major, PROTOCOL_MAJOR)
            )
        self._max_ping_ms = int(payload.get("MaxPingTime", 0) or 0)
        log.info("Handshake ok server=%r payload=%r", payload.get("ServerName"), payload)

    async def _ping_loop(self) -> None:
        interval = max(0.2, self._max_ping_ms / 2000.0)
        while True:
            await asyncio.sleep(interval)
            await self.request("Ping", {}, timeout=max(1.0, self._max_ping_ms / 1000.0))

    async def _scan_loop(self) -> None:
        while True:
            missing = [h for h in self.cfg.device_hints if h not in self.devices]
            if missing:
                try:
                    await self.request("StartScanning", {}, timeout=10)
                    log.info("Scanning for missing devices hints=%s", missing)
                except IntifaceError:
                    log.warning("StartScanning was rejected missing=%s", missing, exc_info=True)
                await asyncio.sleep(self.cfg.scan_retry)
            else:
                await asyncio.sleep(1.0)

    # ---- wire ------------------------------------------------------------------------------

    async def request(self, name: str, body: dict, timeout: float = 10.0):
        ws = self.ws
        if ws is None:
            raise IntifaceError("not connected to Intiface")
        mid = self._next_id
        self._next_id = self._next_id % 4294967295 + 1
        fut = asyncio.get_running_loop().create_future()
        self._pending[mid] = fut
        frame = json.dumps([{name: {"Id": mid, **body}}])
        try:
            await ws.send(frame)
            resp_name, payload = await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError as exc:
            log.error("Intiface request timed out name=%s id=%s body=%r timeout=%ss", name, mid, body, timeout)
            raise IntifaceError("{} timed out after {}s".format(name, timeout)) from exc
        except ConnectionClosed as exc:
            log.error("Intiface connection closed while sending name=%s id=%s", name, mid, exc_info=True)
            raise IntifaceError("Intiface connection closed while sending {}".format(name)) from exc
        finally:
            self._pending.pop(mid, None)
        if resp_name == "Error":
            raise IntifaceError(
                "{} rejected: code={} message={}".format(name, payload.get("ErrorCode"), payload.get("ErrorMessage"))
            )
        return resp_name, payload

    def _fail_pending(self, reason: str) -> None:
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(IntifaceError(reason))
        self._pending.clear()

    async def _recv_loop(self, ws) -> None:
        try:
            async for raw in ws:
                try:
                    messages = json.loads(raw)
                except json.JSONDecodeError:
                    log.exception("Intiface sent invalid JSON raw=%r", str(raw)[:300])
                    continue
                if not isinstance(messages, list):
                    log.error("Intiface sent a non-array frame raw=%r", str(raw)[:300])
                    continue
                for item in messages:
                    if not isinstance(item, dict) or len(item) != 1:
                        log.error("Intiface sent a malformed message item=%r", item)
                        continue
                    ((name, payload),) = item.items()
                    try:
                        self._dispatch(name, payload)
                    except Exception:
                        log.exception("Failed to dispatch message name=%s payload=%r", name, payload)
        finally:
            self._fail_pending("Intiface receive loop ended")

    def _dispatch(self, name: str, payload: dict) -> None:
        if name == "DeviceList":
            self._update_devices(payload)
        mid = payload.get("Id", 0)
        fut = self._pending.get(mid) if mid else None
        if fut is not None and not fut.done():
            fut.set_result((name, payload))
        elif name == "ScanningFinished":
            log.info("Scanning finished")
        elif name not in ("DeviceList", "Ok"):
            log.debug("Unmatched Intiface message name=%s payload=%r", name, payload)

    # ---- devices ---------------------------------------------------------------------------

    @staticmethod
    def _parse_device(dev: dict) -> DeviceInfo:
        index = int(dev["DeviceIndex"])
        features = dev.get("DeviceFeatures") or {}
        items = features.items() if isinstance(features, dict) else enumerate(features)
        by_type: dict = {}
        battery: Optional[int] = None
        for key, feat in items:
            fidx = int(feat.get("FeatureIndex", key))
            for otype, info in (feat.get("Output") or {}).items():
                rng = (info or {}).get("Value")
                if isinstance(rng, list) and len(rng) == 2:
                    by_type.setdefault(otype, []).append((fidx, int(rng[0]), int(rng[1])))
            battery_info = (feat.get("Input") or {}).get("Battery")
            if battery_info is not None and "Read" in (battery_info.get("Command") or []):
                battery = fidx
        channels, groups = build_channels(by_type)
        return DeviceInfo(
            name=str(dev.get("DeviceName", "")),
            index=index,
            gap_ms=int(dev.get("DeviceMessageTimingGap", 0) or 0),
            channels=channels,
            battery_feature=battery,
            groups=groups,
        )

    def _update_devices(self, payload: dict) -> None:
        raw = payload.get("Devices") or {}
        entries = list(raw.values()) if isinstance(raw, dict) else list(raw)
        found: dict = {}
        unmatched = []
        for dev in entries:
            try:
                info = self._parse_device(dev)
            except (KeyError, TypeError, ValueError, AttributeError):
                log.exception("Cannot parse device entry=%r", dev)
                continue
            names = (info.name.lower(), str(dev.get("DeviceDisplayName") or "").lower())
            hint = next((h for h in self.cfg.device_hints if any(h in n for n in names)), None)
            if hint is None:
                unmatched.append(info.name)
            elif hint in found:
                log.warning("Second device matches hint=%r name=%r; keeping index=%s", hint, info.name, found[hint].index)
            else:
                found[hint] = info

        for hint in self.cfg.device_hints:
            old = self.devices.get(hint)
            new = found.get(hint)
            if new is not None:
                self.devices[hint] = new
                if old is None or old.index != new.index:
                    log.info(
                        "Device online hint=%s name=%r index=%s channels=%s battery_feature=%s",
                        hint, new.name, new.index,
                        [(c.name, c.otype, "feature{}".format(c.feature), [c.lo, c.hi]) for c in new.channels],
                        new.battery_feature,
                    )
            else:
                self.devices.pop(hint, None)
            if old is not None and (new is None or new.index != old.index):
                log.warning("Device gone hint=%s name=%r index=%s", hint, old.name, old.index)
                self._schedule_lost(hint)

        for name in unmatched:
            if name not in self._unmatched_warned:
                log.warning("Device %r matches none of hints=%s; ignored", name, list(self.cfg.device_hints))
                self._unmatched_warned.add(name)

    def _drop_all(self, reason: str) -> None:
        for hint in list(self.devices):
            dev = self.devices.pop(hint)
            log.warning("Dropping device hint=%s name=%r reason=%s", hint, dev.name, reason)
            self._schedule_lost(hint)

    def _schedule_lost(self, hint: str) -> None:
        if self.on_device_lost is None:
            return
        task = asyncio.create_task(self.on_device_lost(hint), name="device-lost-" + hint)
        self._bg.add(task)
        task.add_done_callback(self._bg_done)

    def _bg_done(self, task: asyncio.Task) -> None:
        self._bg.discard(task)
        if not task.cancelled() and task.exception() is not None:
            exc = task.exception()
            log.error("Background task %s failed", task.get_name(), exc_info=(type(exc), exc, exc.__traceback__))

    # ---- device commands -------------------------------------------------------------------

    async def send_output(self, dev: DeviceInfo, ch: Channel, value: int) -> None:
        await self.request(
            "OutputCmd",
            {
                "DeviceIndex": dev.index,
                "FeatureIndex": ch.feature,
                "Command": {ch.otype: {"Value": int(value)}},
            },
            timeout=5,
        )

    async def stop_device(self, dev: DeviceInfo) -> None:
        await self.request(
            "StopCmd",
            {"DeviceIndex": dev.index, "Inputs": False, "Outputs": True},
            timeout=5,
        )

    async def read_battery(self, dev: DeviceInfo) -> Optional[int]:
        if dev.battery_feature is None:
            return None
        name, payload = await self.request(
            "InputCmd",
            {"DeviceIndex": dev.index, "FeatureIndex": dev.battery_feature, "Type": "Battery", "Command": "Read"},
            timeout=5,
        )
        if name != "InputReading":
            raise IntifaceError("battery read expected InputReading, got {}: {!r}".format(name, payload))
        value = ((payload.get("Reading") or {}).get("Battery") or {}).get("Value")
        return int(value) if value is not None else None


class ToyController:
    """One per configured toy: channel levels, validation, timers and patterns for that toy only."""

    def __init__(self, cfg: Config, link: IntifaceLink, hint: str) -> None:
        self.cfg = cfg
        self.link = link
        self.hint = hint
        self.aliases: dict = cfg.aliases.get(hint, {})
        self.levels: dict = {}  # channel name -> level
        self.last_error: Optional[str] = None
        self._lock = asyncio.Lock()
        self._runner: Optional[asyncio.Task] = None
        self._runner_kind: Optional[str] = None
        self._runner_deadline = 0.0

    # ---- helpers ---------------------------------------------------------------------------

    def _dev(self) -> DeviceInfo:
        dev = self.link.devices.get(self.hint)
        if dev is None or not self.link.ready.is_set():
            raise ControllerError(
                "{} 未连接：请确认 Intiface Central 已启动服务、玩具已开机且没被其他 App 连着".format(self.hint)
            )
        return dev

    def _channel_names(self, dev: DeviceInfo, key: str) -> list:
        """Resolve a user key (alias, channel name or bare type name) to channel names."""
        name = self.aliases.get(key, key)
        if not isinstance(name, str):
            raise ControllerError("通道名必须是字符串，收到 {!r}".format(key))
        lowered = name.lower()
        if dev.channel(lowered) is not None:
            return [lowered]
        if lowered in dev.groups:
            return list(dev.groups[lowered])
        known = [c.name for c in dev.channels] + list(dev.groups) + list(self.aliases)
        raise ControllerError("{} 没有通道 {!r}，可用：{}".format(self.hint, key, sorted(set(known))))

    @staticmethod
    def _int(label: str, value: Any) -> int:
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ControllerError("{} 必须是整数，收到 {!r}".format(label, value))
        return value

    def _targets(self, label: str, levels: Any) -> dict:
        """{user key: level} -> {channel name: level}, validated against the device's real ranges."""
        if not isinstance(levels, dict) or not levels:
            raise ControllerError("{} 必须是非空对象 {{通道: 强度}}".format(label))
        dev = self._dev()
        targets: dict = {}
        for key, value in levels.items():
            level = self._int("{}.{}".format(label, key), value)
            for name in self._channel_names(dev, key):
                ch = dev.channel(name)
                if level < ch.lo or level > ch.hi:
                    raise ControllerError("{}.{}={} 超出设备实际范围 [{}, {}]".format(label, key, level, ch.lo, ch.hi))
                if name in targets and targets[name] != level:
                    raise ControllerError("{} 里通道 {} 被设了两个不同的值".format(label, name))
                targets[name] = level
        return targets

    def _seconds(self, seconds: Any) -> float:
        if seconds is None:
            return self.cfg.max_seconds
        try:
            value = float(seconds)
        except (TypeError, ValueError) as exc:
            raise ControllerError("seconds 必须是数字，收到 {!r}".format(seconds)) from exc
        if not math.isfinite(value) or value <= 0 or value > self.cfg.max_seconds:
            raise ControllerError("seconds={} 必须在 (0, {}] 之内".format(seconds, self.cfg.max_seconds))
        return value

    def _levels_view(self) -> dict:
        """Current level of every channel (0 when never set), keyed by alias when one exists."""
        back = {v: k for k, v in self.aliases.items()}
        dev = self.link.devices.get(self.hint)
        names = [c.name for c in dev.channels] if dev else list(self.levels)
        return {back.get(name, name): self.levels.get(name, 0) for name in names}

    async def _apply(self, targets: dict, force: bool) -> None:
        async with self._lock:
            dev = self._dev()
            changed = [n for n, v in targets.items() if force or self.levels.get(n, 0) != v]
            if not changed:
                return
            order = list(changed)
            if self.cfg.reassert_all:
                # Same as before the multi-toy rewrite: re-send every other channel at its current level
                # (0 if never set). Lovense devices with rotation can cancel a channel when another changes.
                order += [c.name for c in dev.channels if c.name not in changed]
            for name in order:
                ch = dev.channel(name)
                if ch is None:
                    continue
                value = targets.get(name, self.levels.get(name, 0))
                await self.link.send_output(dev, ch, max(ch.lo, min(ch.hi, value)))
                self.levels[name] = value
                if dev.gap_ms > 0:
                    await asyncio.sleep(dev.gap_ms / 1000.0)

    async def _stop_outputs(self, reason: str) -> None:
        async with self._lock:
            dev = self.link.devices.get(self.hint)
            if dev is not None and self.link.ready.is_set():
                try:
                    await self.link.stop_device(dev)
                except IntifaceError:
                    log.exception(
                        "StopCmd failed hint=%s reason=%s device=%r levels=%s; zeroing each channel instead",
                        self.hint, reason, dev.name, self.levels,
                    )
                    for ch in dev.channels:
                        await self.link.send_output(dev, ch, max(ch.lo, min(ch.hi, 0)))
            self.levels = {}
            log.info("Outputs stopped hint=%s reason=%s", self.hint, reason)

    async def _finish(self, reason: str) -> None:
        try:
            await self._stop_outputs(reason)
        except Exception as exc:
            self.last_error = "stop failed ({}): {!r}".format(reason, exc)
            log.exception("Stop failed hint=%s reason=%s levels=%s", self.hint, reason, self.levels)

    # ---- runner management -----------------------------------------------------------------

    async def cancel_runner(self) -> None:
        task = self._runner
        kind = self._runner_kind
        self._runner = None
        self._runner_kind = None
        self._runner_deadline = 0.0
        if task is not None and not task.done():
            task.cancel()
            await asyncio.wait({task})
            if not task.cancelled() and task.exception() is not None:
                exc = task.exception()
                log.error("Runner hint=%s kind=%s failed while cancelling", self.hint, kind, exc_info=(type(exc), exc, exc.__traceback__))

    def _start_runner(self, kind: str, work: Callable[[], Awaitable[None]], total_seconds: float) -> None:
        self._runner_kind = kind
        self._runner_deadline = asyncio.get_running_loop().time() + total_seconds
        self._runner = asyncio.create_task(self._runner_body(kind, work), name="{}-{}".format(self.hint, kind))

    async def _runner_body(self, kind: str, work: Callable[[], Awaitable[None]]) -> None:
        try:
            await work()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = "{} runner failed: {!r}".format(kind, exc)
            log.exception("Runner failed hint=%s kind=%s levels=%s", self.hint, kind, self.levels)
        await self._finish("{} ended".format(kind))
        if self._runner is asyncio.current_task():
            self._runner = None
            self._runner_kind = None
            self._runner_deadline = 0.0

    async def _timed_work(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def _pattern_work(self, steps: list, repeat: int) -> None:
        for _ in range(repeat):
            for step in steps:
                await self._play_step(step)

    async def _play_step(self, step: dict) -> None:
        loop = asyncio.get_running_loop()
        duration = step["duration"]
        names = set(self.levels) | set(step["targets"])
        start = {n: self.levels.get(n, 0) for n in names}
        target = {n: step["targets"].get(n, start[n]) for n in names}
        t0 = loop.time()
        if step["ramp"]:
            ticks = max(1, math.ceil(duration / self.cfg.ramp_tick))
            for i in range(1, ticks + 1):
                level = {n: int(round(start[n] + (target[n] - start[n]) * i / ticks)) for n in names}
                await self._apply(level, force=False)
                await asyncio.sleep(max(0.0, t0 + duration * i / ticks - loop.time()))
        else:
            await self._apply(target, force=False)
            await asyncio.sleep(max(0.0, t0 + duration - loop.time()))

    async def handle_device_lost(self) -> None:
        await self.cancel_runner()
        self.levels = {}
        log.warning("Device lost hint=%s: runner cancelled and levels reset", self.hint)

    # ---- commands --------------------------------------------------------------------------

    async def cmd_status(self, battery: bool) -> dict:
        dev = self.link.devices.get(self.hint)
        loop = asyncio.get_running_loop()
        remaining = max(0.0, self._runner_deadline - loop.time()) if self._runner_kind else 0.0
        result: dict = {
            "device": self.hint,
            "connected": dev is not None and self.link.ready.is_set(),
            "name": dev.name if dev else None,
            "info": dev.describe(self.aliases) if dev else None,
            "levels": self._levels_view(),
            "running": self._runner_kind,
            "remaining_seconds": round(remaining, 1),
            "last_error": self.last_error,
        }
        if battery and dev is not None and self.link.ready.is_set():
            try:
                result["battery"] = await self.link.read_battery(dev)
            except Exception as exc:
                log.exception("Battery read failed hint=%s device=%r", self.hint, dev.name)
                result["battery"] = None
                result["battery_error"] = repr(exc)
        return result

    async def cmd_set(self, levels: Any, seconds: Any) -> dict:
        targets = self._targets("levels", levels)
        duration = self._seconds(seconds)
        await self.cancel_runner()
        try:
            await self._apply(targets, force=True)
        except Exception:
            log.exception("Apply failed hint=%s targets=%s levels=%s; stopping for safety", self.hint, targets, self.levels)
            await self._finish("apply failed")
            raise
        self.last_error = None
        running = any(v != 0 for v in self.levels.values())
        if running:
            self._start_runner("timed", lambda: self._timed_work(duration), duration)
        return {"device": self.hint, "levels": self._levels_view(), "stops_in_seconds": duration if running else 0}

    def _parse_steps(self, steps: Any, repeat: Any) -> tuple:
        if not isinstance(steps, list) or not steps:
            raise ControllerError("steps 必须是非空列表")
        if len(steps) > self.cfg.max_steps:
            raise ControllerError("steps 最多 {} 步，收到 {} 步".format(self.cfg.max_steps, len(steps)))
        if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
            raise ControllerError("repeat 必须是 >= 1 的整数，收到 {!r}".format(repeat))
        parsed = []
        total = 0.0
        for i, step in enumerate(steps):
            if not isinstance(step, dict):
                raise ControllerError("steps[{}] 必须是对象".format(i))
            unknown = set(step) - {"levels", "duration", "ramp"}
            if unknown:
                raise ControllerError("steps[{}] 含未知字段 {}".format(i, sorted(unknown)))
            try:
                duration = float(step.get("duration"))
            except (TypeError, ValueError) as exc:
                raise ControllerError("steps[{}].duration 必须是数字，收到 {!r}".format(i, step.get("duration"))) from exc
            if not math.isfinite(duration) or duration <= 0:
                raise ControllerError("steps[{}].duration 必须大于 0，收到 {!r}".format(i, duration))
            ramp = step.get("ramp", False)
            if not isinstance(ramp, bool):
                raise ControllerError("steps[{}].ramp 必须是布尔值，收到 {!r}".format(i, ramp))
            levels = step.get("levels") or {}
            targets = self._targets("steps[{}].levels".format(i), levels) if levels else {}
            parsed.append({"duration": duration, "ramp": ramp, "targets": targets})
            total += duration
        total *= repeat
        if total > self.cfg.max_seconds:
            raise ControllerError("整段节奏总时长 {:.1f} 秒超过上限 {} 秒".format(total, self.cfg.max_seconds))
        return parsed, repeat, total

    async def cmd_pattern(self, steps: Any, repeat: Any) -> dict:
        self._dev()
        parsed, count, total = self._parse_steps(steps, 1 if repeat is None else repeat)
        await self.cancel_runner()
        self.last_error = None
        self._start_runner("pattern", lambda: self._pattern_work(parsed, count), total)
        return {"device": self.hint, "steps": len(parsed), "repeat": count, "total_seconds": round(total, 1)}

    async def cmd_stop(self) -> dict:
        await self.cancel_runner()
        await self._stop_outputs("stop command")
        return {"device": self.hint, "stopped": True, "connected": self.hint in self.link.devices}


def _legacy_levels(params: dict) -> Optional[dict]:
    """Old Velvo wire format {"vibrate": v, "rotate": r} -> {"vibrate": v, "rotate": r} without the Nones."""
    levels = {k: params[k] for k in ("vibrate", "rotate") if params.get(k) is not None}
    return levels or None


def _legacy_steps(steps: Any) -> Any:
    if not isinstance(steps, list):
        return steps
    out = []
    for step in steps:
        if isinstance(step, dict) and "levels" not in step and ("vibrate" in step or "rotate" in step):
            step = dict(step)
            levels = {k: step.pop(k) for k in ("vibrate", "rotate") if k in step}
            step["levels"] = {k: v for k, v in levels.items() if v is not None}
        out.append(step)
    return out


class Bridge:
    """Routes cloud commands to the right ToyController."""

    def __init__(self, cfg: Config, link: IntifaceLink) -> None:
        self.cfg = cfg
        self.link = link
        self.toys = {hint: ToyController(cfg, link, hint) for hint in cfg.device_hints}
        link.on_device_lost = self._on_lost

    async def _on_lost(self, hint: str) -> None:
        toy = self.toys.get(hint)
        if toy is not None:
            await toy.handle_device_lost()

    def _toy(self, params: dict) -> ToyController:
        device = params.get("device", DEFAULT_DEVICE)
        if not isinstance(device, str):
            raise ControllerError("device 必须是字符串，收到 {!r}".format(device))
        key = device.strip().lower()
        toy = self.toys.get(key)
        if toy is None:
            raise ControllerError("未知玩具 {!r}，已配置：{}".format(device, list(self.toys)))
        return toy

    async def status_all(self, battery: bool) -> dict:
        return {
            "bridge_connected": True,
            "intiface_connected": self.link.ready.is_set(),
            "max_command_seconds": self.cfg.max_seconds,
            "toys": [await toy.cmd_status(battery) for toy in self.toys.values()],
        }

    async def stop_all(self, reason: str) -> dict:
        results = {}
        for hint, toy in self.toys.items():
            try:
                results[hint] = await toy.cmd_stop()
            except Exception as exc:
                log.exception("Stop failed hint=%s reason=%s", hint, reason)
                results[hint] = {"stopped": False, "error": repr(exc)}
        return {"stopped_all": True, "results": results}

    async def dispatch(self, cmd: Any, params: dict) -> Any:
        if cmd == "devices":
            return await self.status_all(bool(params.get("battery", False)))
        if cmd == "status":
            if "device" not in params:
                # Old velvo_status: keep its flat shape for the Velvo.
                return await self._toy({}).cmd_status(bool(params.get("battery", True)))
            return await self._toy(params).cmd_status(bool(params.get("battery", True)))
        if cmd == "set":
            levels = params.get("levels")
            if levels is None:
                levels = _legacy_levels(params)
            if levels is None:
                raise ControllerError("至少要指定一个通道的强度")
            return await self._toy(params).cmd_set(levels, params.get("seconds"))
        if cmd == "pattern":
            return await self._toy(params).cmd_pattern(_legacy_steps(params.get("steps")), params.get("repeat", 1))
        if cmd == "stop":
            if params.get("all"):
                return await self.stop_all("stop all command")
            return await self._toy(params).cmd_stop()
        raise ControllerError("unknown command {!r}".format(cmd))


class CloudLink:
    """Reverse WebSocket to the Zeabur relay; executes commands received from the MCP tools."""

    def __init__(self, cfg: Config, bridge: Bridge) -> None:
        self.cfg = cfg
        self.bridge = bridge

    async def run_forever(self) -> None:
        while True:
            try:
                async for ws in connect(
                    self.cfg.cloud_url,
                    additional_headers={"X-Bridge-Token": self.cfg.token},
                    open_timeout=15,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1024 * 1024,
                ):
                    try:
                        await self._serve(ws)
                    except ConnectionClosed:
                        log.warning("Cloud link lost url=%s", self.cfg.cloud_url, exc_info=True)
                    if ws.close_code == CLOUD_REPLACED_CLOSE_CODE:
                        log.error("Another bridge replaced this one; waiting 30s before reconnecting")
                        await asyncio.sleep(30)
                    else:
                        await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Cloud link failed url=%s (bad token or wrong URL?); retrying in 30s", self.cfg.cloud_url)
                await asyncio.sleep(30)

    async def _serve(self, ws) -> None:
        log.info("Cloud link up url=%s", self.cfg.cloud_url)
        hello = await self.bridge.status_all(battery=False)
        await ws.send(json.dumps({"event": "hello", "state": hello}, ensure_ascii=False))
        inflight: set = set()
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.exception("Cloud sent invalid JSON raw=%r", str(raw)[:300])
                continue
            if not isinstance(msg, dict):
                log.error("Cloud sent a non-object message raw=%r", str(raw)[:300])
                continue
            task = asyncio.create_task(self._handle(ws, msg), name="cmd-" + str(msg.get("cmd")))
            inflight.add(task)
            task.add_done_callback(inflight.discard)
        log.info("Cloud link closed code=%s", ws.close_code)

    async def _handle(self, ws, msg: dict) -> None:
        mid = msg.get("id")
        cmd = msg.get("cmd")
        params = msg.get("params") or {}
        try:
            result = await self.bridge.dispatch(cmd, params)
            reply = {"id": mid, "ok": True, "result": result}
        except BridgeError as exc:
            log.warning("Command rejected cmd=%s params=%r reason=%s", cmd, params, exc, exc_info=True)
            reply = {"id": mid, "ok": False, "error": str(exc)}
        except Exception as exc:
            log.exception("Command crashed cmd=%s params=%r", cmd, params)
            reply = {"id": mid, "ok": False, "error": "bridge internal error: {!r}".format(exc)}
        try:
            await ws.send(json.dumps(reply, ensure_ascii=False))
        except ConnectionClosed:
            log.warning("Reply dropped, cloud link closed id=%s cmd=%s", mid, cmd, exc_info=True)


async def main() -> int:
    cfg = Config.from_env()
    setup_logging(cfg.log_file)
    log.info(
        "Toy bridge starting intiface=%s cloud=%s hints=%s aliases=%s max_seconds=%s reassert_all=%s",
        cfg.intiface_url,
        cfg.cloud_url,
        list(cfg.device_hints),
        cfg.aliases,
        cfg.max_seconds,
        cfg.reassert_all,
    )

    link = IntifaceLink(cfg)
    bridge = Bridge(cfg, link)
    cloud = CloudLink(cfg, bridge)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            log.exception("Signal handler unavailable sig=%s; use Ctrl-C via KeyboardInterrupt", sig)

    workers = [
        asyncio.create_task(link.run_forever(), name="intiface-link"),
        asyncio.create_task(cloud.run_forever(), name="cloud-link"),
    ]
    stopper = asyncio.create_task(stop_event.wait(), name="stop-wait")
    exit_code = 0
    try:
        done, _ = await asyncio.wait({stopper, *workers}, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is not stopper:
                exit_code = 1
                exc = task.exception() if not task.cancelled() else None
                log.error("Worker %s ended unexpectedly", task.get_name(), exc_info=(type(exc), exc, exc.__traceback__) if exc else None)
    finally:
        log.info("Shutting down: stopping every device")
        try:
            await asyncio.wait_for(bridge.stop_all("shutdown"), timeout=8)
        except Exception:
            log.exception(
                "Failed to stop devices during shutdown levels=%s",
                {h: t.levels for h, t in bridge.toys.items()},
            )
        stopper.cancel()
        for task in workers:
            task.cancel()
        await asyncio.gather(stopper, *workers, return_exceptions=True)
    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except ConfigError as exc:
        logging.basicConfig(level=logging.INFO)
        log.error("Configuration error: %s", exc, exc_info=True)
        sys.exit(2)
    except KeyboardInterrupt:
        log.info("Interrupted")
        sys.exit(0)
    except Exception:
        log.exception("Bridge crashed")
        sys.exit(1)

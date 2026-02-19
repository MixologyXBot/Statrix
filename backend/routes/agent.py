# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import base64
import gzip
import json
import logging
from time import monotonic

from fastapi import APIRouter, Form, Request

from ..background.monitor_loop import handle_checkin
from ..database import db
from ..utils.time import utcnow

router = APIRouter()
logger = logging.getLogger(__name__)

_MAX_PAYLOAD_BYTES = 65_536          # 64 KB raw payload
_MAX_DECOMPRESSED_BYTES = 524_288    # 512 KB after gzip decompression
_AGENT_MIN_INTERVAL_SECONDS = 30     # min seconds between requests per SID
_agent_last_seen: dict[str, float] = {}


def _mask_sid(sid: str) -> str:
    if not sid or len(sid) <= 10:
        return "***"
    return f"{sid[:6]}...{sid[-4:]}"


def _check_agent_rate_limit(sid: str) -> bool:
    now = monotonic()
    last = _agent_last_seen.get(sid, 0)
    if now - last < _AGENT_MIN_INTERVAL_SECONDS:
        return False
    _agent_last_seen[sid] = now
    return True


@router.post("/v2/")
async def receive_agent_data(request: Request, j: str = Form(...)):
    try:
        if len(j) > _MAX_PAYLOAD_BYTES:
            return {"status": "error", "message": "Payload too large"}

        decoded = base64.b64decode(j)
        decompressed = gzip.decompress(decoded)
        if len(decompressed) > _MAX_DECOMPRESSED_BYTES:
            return {"status": "error", "message": "Decompressed payload too large"}

        data = json.loads(decompressed.decode("utf-8"))

        return await _process_server_payload(data, source="v2")

    except base64.binascii.Error:
        logger.error("Invalid base64 data from agent")
        return {"status": "error", "message": "Invalid encoding"}
    except gzip.BadGzipFile:
        logger.error("Invalid gzip data from agent")
        return {"status": "error", "message": "Invalid compression"}
    except json.JSONDecodeError:
        logger.error("Invalid JSON data from agent")
        return {"status": "error", "message": "Invalid JSON"}
    except Exception:
        logger.exception("Error processing agent data")
        return {"status": "error", "message": "Internal error"}


@router.post("/win/")
async def receive_windows_agent_data(request: Request, payload: dict):
    try:
        version = str(payload.get("version") or "").strip().lower()
        if version in {"install", "uninstall"}:
            sid = payload.get("SID")
            logger.info("Received Windows agent %s notice for SID: %s", version, _mask_sid(sid or ""))
            return {"status": "success"}

        return await _process_server_payload(payload, source="win")
    except Exception:
        logger.exception("Error processing Windows agent data")
        return {"status": "error", "message": "Internal error"}


async def _process_server_payload(data: dict, source: str = "v2"):
    """Store a server payload coming from any supported agent transport."""
    sid = data.get("SID")
    if not sid:
        logger.warning("Agent payload missing SID (source=%s)", source)
        return {"status": "error", "message": "Missing SID"}

    if not _check_agent_rate_limit(sid):
        return {"status": "error", "message": "Rate limited"}

    server = await db.get_server_monitor_by_sid(sid)
    if not server:
        logger.warning("Agent data for unknown SID: %s (source=%s)", _mask_sid(sid), source)
        return {"status": "error", "message": "Unknown SID"}

    now = utcnow()

    cpu_percent = _safe_float(data.get("cpu"))
    ram_percent = _safe_float(data.get("ram"))

    cpu_io_wait = _safe_float(data.get("wa"))
    cpu_steal = _safe_float(data.get("st"))
    cpu_user = _safe_float(data.get("us"))
    cpu_system = _safe_float(data.get("sy"))

    ram_swap_percent = _safe_float(data.get("ramswap"))
    ram_buff_percent = _safe_float(data.get("rambuff"))
    ram_cache_percent = _safe_float(data.get("ramcache"))

    network_in, network_out = _parse_network_data(data.get("nics"))

    disk_percent = _parse_disk_data(data.get("disks"))

    # Record history FIRST so timestamp is in server_history before
    # last_report_at is updated.  If this fails, last_report_at stays
    # unchanged — keeping the CHECK column accurate to actual recorded data.
    await db.create_server_history(
        server_id=server["id"],
        cpu_percent=cpu_percent,
        ram_percent=ram_percent,
        load_1=_safe_float(data.get("load1")),
        load_5=_safe_float(data.get("load5")),
        load_15=_safe_float(data.get("load15")),
        disks=data.get("disks"),
        nics=data.get("nics"),
        temperature=data.get("temp"),
        cpu_io_wait=cpu_io_wait,
        cpu_steal=cpu_steal,
        cpu_user=cpu_user,
        cpu_system=cpu_system,
        ram_swap_percent=ram_swap_percent,
        ram_buff_percent=ram_buff_percent,
        ram_cache_percent=ram_cache_percent,
        network_in=network_in,
        network_out=network_out,
        disk_percent=disk_percent,
    )

    await db.update_server_monitor(
        server["id"],
        os=_safe_str(data.get("os")),
        kernel=_safe_str(data.get("kernel")),
        hostname=_safe_str(data.get("hostname")),
        cpu_model=_safe_str(data.get("cpumodel")),
        cpu_sockets=_safe_int(data.get("cpusockets")),
        cpu_cores=_safe_int(data.get("cpucores")),
        cpu_threads=_safe_int(data.get("cputhreads")),
        ram_size=_safe_int(data.get("ramsize")),
        ram_swap_size=_safe_int(data.get("ramswapsize")),
        last_report_at=now,
    )
    await handle_checkin(
        monitor_id=server["id"],
        cache_kind="server",
        db_type="server",
        display_type="heartbeat-server-agent",
        name=server.get("name") or str(server["id"]),
        target=server.get("hostname") or sid,
    )

    logger.info(
        "Received agent data for server: %s (SID: %s, source=%s)",
        server["name"], _mask_sid(sid), source,
    )
    return {"status": "success"}


def _safe_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            try:
                decoded = base64.b64decode(value).decode("utf-8")
                return int(decoded)
            except Exception:
                return int(value)
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            try:
                decoded = base64.b64decode(value).decode("utf-8")
                return float(decoded)
            except Exception:
                return float(value)
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_str(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            try:
                decoded = base64.b64decode(value).decode("utf-8")
                return decoded if decoded else value
            except Exception:
                return value
        return str(value)
    except Exception:
        return None


def _is_virtual_nic(interface_name: str) -> bool:
    name = str(interface_name or "").strip().lower()
    if not name:
        return True
    virtual_prefixes = (
        "lo", "veth", "br-", "docker", "virbr", "ifb", "cni", "flannel", "kube", "podman"
    )
    return any(name.startswith(prefix) for prefix in virtual_prefixes)


def _safe_nonnegative_int(raw: str) -> int | None:
    try:
        value = int(str(raw or "").strip())
        if value < 0:
            return None
        return value
    except Exception:
        return None


def _parse_network_data(nics_b64: str | None) -> tuple:
    if not nics_b64:
        return None, None

    try:
        try:
            decoded = base64.b64decode(nics_b64).decode("utf-8")
        except Exception:
            decoded = nics_b64

        rows = []

        for nic_data in decoded.split(";"):
            if not nic_data:
                continue
            parts = nic_data.split(",")
            if len(parts) >= 3:
                iface = str(parts[0] or "").strip()
                rx = _safe_nonnegative_int(parts[1])
                tx = _safe_nonnegative_int(parts[2])
                if rx is None or tx is None:
                    continue
                rows.append((iface, rx, tx))

        if not rows:
            return None, None

        physical_rows = [row for row in rows if not _is_virtual_nic(row[0])]
        source_rows = physical_rows if physical_rows else rows

        total_rx = sum(row[1] for row in source_rows)
        total_tx = sum(row[2] for row in source_rows)

        return total_rx, total_tx
    except Exception:
        return None, None


def _parse_disk_data(disks_b64: str | None) -> float | None:
    if not disks_b64:
        return None

    try:
        try:
            decoded = base64.b64decode(disks_b64).decode("utf-8")
        except Exception:
            decoded = disks_b64

        total_space = 0
        used_space = 0

        for disk_data in decoded.split(";"):
            if not disk_data:
                continue
            parts = disk_data.split(",")
            total = 0
            used = 0
            if len(parts) >= 5:
                total = int(parts[2]) if parts[2].isdigit() else 0
                used = int(parts[3]) if parts[3].isdigit() else 0
            elif len(parts) >= 4:
                total = int(parts[1]) if parts[1].isdigit() else 0
                used = int(parts[2]) if parts[2].isdigit() else 0
            total_space += total
            used_space += used

        if total_space == 0:
            return None

        return round((used_space / total_space) * 100, 2)
    except Exception:
        return None

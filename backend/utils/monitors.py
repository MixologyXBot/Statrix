# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import uuid
import logging
from typing import Any

from ..database import db

logger = logging.getLogger(__name__)

ADMIN_INCIDENT_MONITOR_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")
ADMIN_INCIDENT_MONITOR_TYPE = "uptime"


def is_placeholder_monitor_id(value: uuid.UUID | None) -> bool:
    if value is None:
        return True
    try:
        return uuid.UUID(str(value)) == ADMIN_INCIDENT_MONITOR_ID
    except Exception:
        return False


async def resolve_monitor_context(
    monitor_id: uuid.UUID | None,
    monitor_type: str,
) -> dict[str, Any]:
    if is_placeholder_monitor_id(monitor_id):
        return {
            "monitor_id": None,
            "monitor_source": None,
            "monitor_name": None,
        }

    if monitor_type == "uptime":
        monitor = await db.get_uptime_monitor_by_id(monitor_id)
        return {
            "monitor_id": str(monitor_id),
            "monitor_source": "website",
            "monitor_name": monitor.get("name") if monitor else None,
        }

    if monitor_type == "heartbeat":
        heartbeat_monitor = await db.get_heartbeat_monitor_by_id(monitor_id)
        if heartbeat_monitor:
            hb_type = str(heartbeat_monitor.get("heartbeat_type") or "cronjob").strip().lower()
            return {
                "monitor_id": str(monitor_id),
                "monitor_source": "heartbeat-server-agent" if hb_type == "server_agent" else "heartbeat-cronjob",
                "monitor_name": heartbeat_monitor.get("name"),
            }

        server_monitor = await db.get_server_monitor_by_id(monitor_id)
        if server_monitor:
            return {
                "monitor_id": str(monitor_id),
                "monitor_source": "heartbeat-server-agent",
                "monitor_name": server_monitor.get("name"),
            }

        return {
            "monitor_id": str(monitor_id),
            "monitor_source": "heartbeat",
            "monitor_name": None,
        }

    return {
        "monitor_id": str(monitor_id) if monitor_id else None,
        "monitor_source": None,
        "monitor_name": None,
    }

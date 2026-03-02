# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import logging

from fastapi import APIRouter, HTTPException, Query, status

from ..background.monitor_loop import handle_checkin
from ..database import db

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/hb/")
async def heartbeat_ping(s: str = Query(..., description="Heartbeat Monitor SID")):
    if not s:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Missing SID parameter"
        )

    monitor = await db.get_heartbeat_monitor_by_sid(s)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Unknown heartbeat SID"
        )

    if not monitor.get("enabled"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Heartbeat monitor is disabled",
        )

    if monitor.get("heartbeat_type", "cronjob") != "cronjob":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This SID is not a cronjob heartbeat monitor",
        )

    try:
        await db.record_heartbeat_ping(monitor["id"])

        await handle_checkin(
            monitor_id=monitor["id"],
            cache_kind="heartbeat",
            db_type="heartbeat",
            display_type="heartbeat-cronjob",
            name=monitor.get("name") or str(monitor["id"]),
            target=monitor.get("sid") or "",
        )

        logger.debug(
            "Heartbeat ping received for: %s (SID: %s)",
            monitor["name"],
            s,
        )

        return "OK"

    except Exception:
        logger.exception("Error recording heartbeat ping")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record ping",
        )

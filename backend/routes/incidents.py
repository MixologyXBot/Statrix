# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from fastapi import APIRouter, HTTPException, status, Query
from datetime import timedelta
import asyncio
import logging
import uuid

from ..database import db
from ..models import (
    IncidentCreateRequest,
    IncidentResponse,
    IncidentTemplateResponse,
)
from ..utils.monitors import (
    ADMIN_INCIDENT_MONITOR_ID,
    ADMIN_INCIDENT_MONITOR_TYPE,
    is_placeholder_monitor_id,
    resolve_monitor_context,
)

router = APIRouter()
logger = logging.getLogger(__name__)
RESOLVED_RETENTION_HOURS = 48

INCIDENT_TEMPLATES = [
    {
        "key": "major_outage",
        "name": "Major Outage",
        "incident_type": "down",
        "title": "Major service outage",
        "description": "We are currently investigating a major outage affecting multiple services. Our team is actively working on mitigation and recovery.",
    },
    {
        "key": "partial_outage",
        "name": "Partial Outage",
        "incident_type": "warning",
        "title": "Partial service disruption",
        "description": "Some users may experience intermittent errors or failed requests. We are investigating and will share additional updates shortly.",
    },
    {
        "key": "degraded_performance",
        "name": "Degraded Performance",
        "incident_type": "warning",
        "title": "Degraded performance",
        "description": "We are observing elevated latency and slower response times. We are actively investigating the cause.",
    },
    {
        "key": "scheduled_maintenance",
        "name": "Scheduled Maintenance",
        "incident_type": "info",
        "title": "Scheduled maintenance in progress",
        "description": "We are performing planned maintenance. Some services may be briefly unavailable during this window.",
    },
    {
        "key": "security_investigation",
        "name": "Security Investigation",
        "incident_type": "warning",
        "title": "Security event under investigation",
        "description": "We are investigating a potential security-related event. There is no confirmed customer impact at this time.",
    },
]

TEMPLATE_BY_KEY = {item["key"]: item for item in INCIDENT_TEMPLATES}


def _normalize_monitor_source(value: str | None) -> str:
    normalized = (value or "all").strip().lower()
    if normalized not in {"all", "website", "heartbeat-cronjob", "heartbeat-server-agent"}:
        return "all"
    return normalized


async def _resolve_monitor_selection(
    monitor_source: str | None,
    monitor_id: uuid.UUID | None
) -> dict:
    source_value = _normalize_monitor_source(monitor_source)

    if source_value == "all":
        if monitor_id is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="monitor_id requires monitor_source to be selected"
            )
        return {
            "monitor_type": ADMIN_INCIDENT_MONITOR_TYPE,
            "monitor_id": ADMIN_INCIDENT_MONITOR_ID,
            "monitor_source": None,
            "monitor_name": None
        }

    if monitor_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please select a monitor"
        )

    if source_value == "website":
        monitor = await db.get_uptime_monitor_by_id(monitor_id)
        if not monitor:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Selected website monitor not found"
            )
        return {
            "monitor_type": "uptime",
            "monitor_id": monitor["id"],
            "monitor_source": "website",
            "monitor_name": monitor.get("name")
        }

    if source_value == "heartbeat-cronjob":
        monitor = await db.get_heartbeat_monitor_by_id(monitor_id)
        if not monitor:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Selected heartbeat cronjob monitor not found"
            )
        heartbeat_type = str(monitor.get("heartbeat_type") or "cronjob").strip().lower()
        if heartbeat_type != "cronjob":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Selected monitor is not a cronjob heartbeat monitor"
            )
        return {
            "monitor_type": "heartbeat",
            "monitor_id": monitor["id"],
            "monitor_source": "heartbeat-cronjob",
            "monitor_name": monitor.get("name")
        }

    if source_value == "heartbeat-server-agent":
        monitor = await db.get_server_monitor_by_id(monitor_id)
        if not monitor:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Selected heartbeat server-agent monitor not found"
            )
        return {
            "monitor_type": "heartbeat",
            "monitor_id": monitor["id"],
            "monitor_source": "heartbeat-server-agent",
            "monitor_name": monitor.get("name")
        }

    return {
        "monitor_type": ADMIN_INCIDENT_MONITOR_TYPE,
        "monitor_id": ADMIN_INCIDENT_MONITOR_ID,
        "monitor_source": None,
        "monitor_name": None
    }


async def _resolve_incident_monitor_context(incident: dict) -> dict:
    payload = dict(incident)
    monitor_id = payload.get("monitor_id")
    monitor_type = str(payload.get("monitor_type") or "").strip().lower()
    context = await resolve_monitor_context(monitor_id, monitor_type)
    payload["monitor_id"] = context["monitor_id"]
    payload["monitor_source"] = context["monitor_source"]
    payload["monitor_name"] = context["monitor_name"]
    return payload


def _to_incident_response(incident: dict) -> IncidentResponse:
    payload = dict(incident)
    if not payload.get("source"):
        payload["source"] = "monitor"
    if payload.get("hidden_from_status_page") is None:
        payload["hidden_from_status_page"] = False
    if "hidden_from_status_page_at" not in payload:
        payload["hidden_from_status_page_at"] = None
    resolved_at = payload.get("resolved_at")
    if payload.get("status") == "resolved" and resolved_at:
        payload["resolved_expires_at"] = resolved_at + timedelta(hours=RESOLVED_RETENTION_HOURS)
    return IncidentResponse(**payload)


@router.get("/templates", response_model=list[IncidentTemplateResponse])
async def get_incident_templates():
    return [IncidentTemplateResponse(**template) for template in INCIDENT_TEMPLATES]


@router.get("", response_model=list[IncidentResponse])
async def get_incidents(
    status_filter: str | None = Query(None, description="Filter by status: open, resolved"),
    monitor_type: str | None = Query(None, description="Filter by monitor type"),
    source: str | None = Query(None, description="Filter by source: monitor, admin"),
    include_recent_resolved_hours: int | None = Query(
        None,
        ge=1,
        le=168,
        description="If set, return open incidents + resolved incidents newer than this many hours"
    ),
    limit: int = Query(200, ge=1, le=500, description="Maximum incidents to return")
):
    incidents = await db.get_incidents(
        status_filter=status_filter,
        monitor_type=monitor_type,
        source=source,
        include_recent_resolved_hours=include_recent_resolved_hours,
        limit=limit
    )
    enriched_results = await asyncio.gather(
        *[_resolve_incident_monitor_context(inc) for inc in incidents],
        return_exceptions=True
    )
    enriched = []
    for incident, result in zip(incidents, enriched_results):
        if isinstance(result, Exception):
            logger.warning("Failed to enrich incident monitor context for %s: %s", incident.get("id"), result)
            fallback = dict(incident)
            fallback["monitor_source"] = None
            fallback["monitor_name"] = None
            if is_placeholder_monitor_id(fallback.get("monitor_id")):
                fallback["monitor_id"] = None
            enriched.append(fallback)
        else:
            enriched.append(result)
    return [_to_incident_response(inc) for inc in enriched]


@router.post("/admin", response_model=IncidentResponse, status_code=status.HTTP_201_CREATED)
async def create_admin_incident(payload: IncidentCreateRequest):
    title = (payload.title or "").strip()
    if not title:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Title is required"
        )

    template = None
    if payload.template_key:
        template = TEMPLATE_BY_KEY.get(payload.template_key)
        if template is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unknown incident template"
            )

    description = (payload.description or "").strip() or None
    if template and not description:
        description = template["description"]

    monitor_selection = await _resolve_monitor_selection(payload.monitor_source, payload.monitor_id)

    incident_id = await db.create_incident(
        monitor_type=monitor_selection["monitor_type"],
        monitor_id=monitor_selection["monitor_id"],
        incident_type=payload.incident_type,
        title=title,
        description=description,
        source="admin",
        template_key=payload.template_key
    )
    created = await db.get_incident_by_id(incident_id)
    if not created:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Incident was created but could not be reloaded"
        )
    try:
        created = await _resolve_incident_monitor_context(created)
    except Exception as exc:
        logger.warning("Failed to enrich created incident %s: %s", created.get("id"), exc)
        if is_placeholder_monitor_id(created.get("monitor_id")):
            created["monitor_id"] = None
        created["monitor_source"] = None
        created["monitor_name"] = None
    return _to_incident_response(created)


@router.get("/{incident_id}", response_model=IncidentResponse)
async def get_incident(incident_id: uuid.UUID):
    incident = await db.get_incident_by_id(incident_id)
    if incident:
        try:
            incident = await _resolve_incident_monitor_context(incident)
        except Exception as exc:
            logger.warning("Failed to enrich incident %s: %s", incident_id, exc)
            if is_placeholder_monitor_id(incident.get("monitor_id")):
                incident["monitor_id"] = None
            incident["monitor_source"] = None
            incident["monitor_name"] = None
        return _to_incident_response(incident)

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Incident not found"
    )


@router.post("/{incident_id}/resolve")
async def resolve_incident(incident_id: uuid.UUID):
    success = await db.resolve_incident(incident_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident not found"
        )
    return {"message": "Incident resolved"}


@router.post("/{incident_id}/hide-from-status")
async def hide_incident_from_status(incident_id: uuid.UUID):
    incident = await db.get_incident_by_id(incident_id)
    if not incident:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incident not found"
        )

    if (incident.get("source") or "monitor") != "admin":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only admin incidents can be hidden from the public status page"
        )

    if incident.get("status") != "resolved":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incident must be resolved before it can be hidden from status page"
        )

    success = await db.hide_incident_from_status_page(incident_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unable to hide incident from status page"
        )
    return {"message": "Incident hidden from public status page"}


@router.get("/stats/summary")
async def get_incident_stats():
    incidents = await db.get_incidents(limit=500)

    open_count = len([i for i in incidents if i["status"] == "open"])

    by_type = {}
    by_source = {}
    for inc in incidents:
        mtype = inc["monitor_type"]
        by_type[mtype] = by_type.get(mtype, 0) + 1
        source = inc.get("source") or "monitor"
        by_source[source] = by_source.get(source, 0) + 1

    return {
        "open_incidents": open_count,
        "by_monitor_type": by_type,
        "by_source": by_source,
        "total_incidents": len(incidents)
    }

# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, validator


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: uuid.UUID
    email: str
    role: str
    name: str | None = None

    class Config:
        from_attributes = True


class UptimeMonitorCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    type: int = Field(default=1, ge=1, le=1)
    target: str = Field(..., max_length=500)
    port: int | None = None
    check_interval: int = Field(default=1, ge=1, le=1440)
    timeout: int = Field(default=5, ge=5, le=60)
    category: str | None = Field(None, max_length=100)
    private_notes: str | None = None

    @validator("type")
    def validate_type(cls, v):
        if v != 1:
            raise ValueError("Invalid monitor type. Website Monitor must use type=1")
        return v


class UptimeMonitorConfig(BaseModel):
    follow_redirects: bool = True
    verify_ssl: bool = True
    http_method: str = "GET"
    custom_headers: str | None = None
    post_data: str | None = None
    http_auth_username: str | None = None
    http_auth_password: str | None = None
    expected_status_codes: str | None = None
    keyword_to_search: str | None = None
    keyword_must_contain: bool = True


class UptimeMonitorUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=255)
    target: str | None = Field(None, max_length=500)
    port: int | None = None
    check_interval: int | None = Field(None, ge=1, le=1440)
    timeout: int | None = Field(None, ge=5, le=60)
    category: str | None = Field(None, max_length=100)
    private_notes: str | None = None
    enabled: bool | None = None
    notifications_enabled: bool | None = None
    config: UptimeMonitorConfig | None = None


class UptimeMonitorResponse(BaseModel):
    id: uuid.UUID
    name: str
    type: int
    target: str
    port: int | None
    check_interval: int
    timeout: int
    category: str | None
    enabled: bool
    notifications_enabled: bool
    is_public: bool
    maintenance_mode: bool = False
    status: str | None = None
    status_since: datetime | None = None
    created_at: datetime
    updated_at: datetime | None = None
    last_check_at: datetime | None = None
    last_up_at: datetime | None = None

    class Config:
        from_attributes = True


class ServerMonitorCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    category: str | None = Field(None, max_length=100)


class ServerMonitorUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=255)
    category: str | None = Field(None, max_length=100)
    enabled: bool | None = None
    notifications_enabled: bool | None = None
    is_public: bool | None = None


class ServerMonitorResponse(BaseModel):
    id: uuid.UUID
    sid: str
    name: str
    os: str | None
    kernel: str | None
    hostname: str | None
    cpu_model: str | None
    cpu_cores: int | None
    cpu_threads: int | None
    ram_size: int | None
    enabled: bool
    notifications_enabled: bool
    is_public: bool
    maintenance_mode: bool = False
    category: str | None
    heartbeat_type: str = "server_agent"
    status: str | None = None
    status_since: datetime | None = None
    created_at: datetime
    last_report_at: datetime | None

    class Config:
        from_attributes = True


class ServerHistoryData(BaseModel):
    timestamp: datetime
    cpu_percent: float | None
    cpu_io_wait: float | None = None
    cpu_steal: float | None = None
    cpu_user: float | None = None
    cpu_system: float | None = None
    ram_percent: float | None
    ram_swap_percent: float | None = None
    ram_buff_percent: float | None = None
    ram_cache_percent: float | None = None
    load_1: float | None
    load_5: float | None
    load_15: float | None
    network_in: int | None = None
    network_out: int | None = None
    disk_percent: float | None = None
    nics: str | None = None
    disks: str | None = None
    temperature: str | None = None

    class Config:
        from_attributes = True


class HeartbeatMonitorCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    heartbeat_type: str = Field(default="cronjob")
    timeout: int = Field(default=60, ge=1, le=1440)
    grace_period: int = Field(default=5, ge=0, le=60)
    category: str | None = Field(None, max_length=100)

    @validator("heartbeat_type")
    def validate_heartbeat_type(cls, v):
        value = (v or "").strip().lower()
        if value not in {"cronjob", "server_agent"}:
            raise ValueError("heartbeat_type must be 'cronjob' or 'server_agent'")
        return value


class HeartbeatMonitorUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=255)
    heartbeat_type: str | None = None
    timeout: int | None = Field(None, ge=1, le=1440)
    grace_period: int | None = Field(None, ge=0, le=60)
    category: str | None = Field(None, max_length=100)
    enabled: bool | None = None
    notifications_enabled: bool | None = None
    is_public: bool | None = None

    @validator("heartbeat_type")
    def validate_optional_heartbeat_type(cls, v):
        if v is None:
            return v
        value = v.strip().lower()
        if value not in {"cronjob", "server_agent"}:
            raise ValueError("heartbeat_type must be 'cronjob' or 'server_agent'")
        return value


class HeartbeatMonitorResponse(BaseModel):
    id: uuid.UUID
    sid: str
    name: str
    heartbeat_type: str
    timeout: int
    grace_period: int
    enabled: bool
    notifications_enabled: bool
    is_public: bool
    maintenance_mode: bool = False
    category: str | None
    status: str | None = None
    status_since: datetime | None = None
    created_at: datetime
    last_ping_at: datetime | None

    class Config:
        from_attributes = True


class IncidentCreateRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=500)
    description: str | None = Field(None, max_length=5000)
    incident_type: str = Field(default="warning")
    template_key: str | None = Field(None, max_length=100)
    monitor_source: str | None = Field(None, max_length=64)
    monitor_id: uuid.UUID | None = None

    @validator("incident_type")
    def validate_incident_type(cls, v):
        value = (v or "").strip().lower()
        if value not in {"down", "up", "warning", "info"}:
            raise ValueError("incident_type must be one of: down, up, warning, info")
        return value

    @validator("monitor_source")
    def validate_monitor_source(cls, v):
        if v is None:
            return v
        value = (v or "").strip().lower()
        allowed = {"all", "website", "heartbeat-cronjob", "heartbeat-server-agent"}
        if value not in allowed:
            raise ValueError(
                "monitor_source must be one of: all, website, heartbeat-cronjob, heartbeat-server-agent"
            )
        return value


class IncidentTemplateResponse(BaseModel):
    key: str
    name: str
    incident_type: str
    title: str
    description: str


class IncidentResponse(BaseModel):
    id: uuid.UUID
    monitor_type: str
    monitor_id: uuid.UUID | None
    monitor_source: str | None = None
    monitor_name: str | None = None
    incident_type: str
    source: str = "monitor"
    template_key: str | None = None
    status: str
    title: str
    description: str | None
    started_at: datetime
    resolved_at: datetime | None
    resolved_expires_at: datetime | None = None
    hidden_from_status_page: bool = False
    hidden_from_status_page_at: datetime | None = None
    notification_sent: bool

    class Config:
        from_attributes = True


class MaintenanceScheduleRequest(BaseModel):
    start_at: datetime
    end_at: datetime

    @validator("end_at")
    def validate_end_after_start(cls, v, values):
        start_at = values.get("start_at")
        if start_at and v <= start_at:
            raise ValueError("end_at must be after start_at")
        return v

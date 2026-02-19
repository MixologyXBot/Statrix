// This file is a part of Statrix
// Coding : Priyanshu Dey [@HellFireDevil18]

const API_BASE = window.location.origin;

const state = {
    user: null,
    monitors: {
        uptime: [],
        server: [],
        heartbeat: []
    },
    incidents: [],
    overallUptime: null,
    activeTab: 'overview',
    systemResources: null,
    lastCheckTimestamps: new Map(),
    pagination: {
        uptime: { page: 1, perPage: 20, total: 0 },
        server: { page: 1, perPage: 20, total: 0 },
        heartbeat: { page: 1, perPage: 20, total: 0 }
    },
    refreshInterval: null,
    checkAgeInterval: null,
    charts: {
        serverCpuRam: null,
        responseTime: null,
        uptime: null,
        response: null
    },
    serverDetailsView: null,
    serverHistoryCache: new Map(),
    sidebarCollapsed: localStorage.getItem('sidebarCollapsed') !== 'false'
};

const MONITOR_SOURCE = Object.freeze({
    WEBSITE: 'website',
    HEARTBEAT_CRONJOB: 'heartbeat-cronjob',
    HEARTBEAT_SERVER_AGENT: 'heartbeat-server-agent'
});
const DASHBOARD_POLL_MS = 60000;
const CHECK_AGE_REFRESH_MS = 1000;
const INCIDENT_RESOLVED_RETENTION_HOURS = 48;
let incidentTemplates = [];
let incidentTemplateMap = {};
let loadAllDataRequestSeq = 0;

function getUnifiedMonitorTypeKey(monitor) {
    const sourceType = monitor.monitorType || monitor._source;
    if (
        sourceType === MONITOR_SOURCE.WEBSITE
        || sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB
        || sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT
    ) {
        return sourceType;
    }
    if (monitor.heartbeat_type === 'server_agent') return MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT;
    if (monitor.heartbeat_type === 'cronjob') return MONITOR_SOURCE.HEARTBEAT_CRONJOB;
    return MONITOR_SOURCE.WEBSITE;
}

function getSourceTypeFromMonitor(monitor) {
    return getUnifiedMonitorTypeKey(monitor);
}

function getSourceTypeLabel(sourceType, monitor = null) {
    if (sourceType === MONITOR_SOURCE.WEBSITE) return 'Website';
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) return 'Cronjob';
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) return 'Server Agent';
    return getSourceTypeLabel(getUnifiedMonitorTypeKey(monitor || {}));
}

function getCompactSourceTypeLabel(sourceType) {
    if (sourceType === MONITOR_SOURCE.WEBSITE) return 'web';
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) return 'cron';
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) return 'agent';
    return 'web';
}

function getMonitorGroupKey(sourceType) {
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) return 'server';
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) return 'heartbeat';
    return 'uptime';
}

function getApiBaseBySourceType(sourceType) {
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) return '/api/heartbeat-monitors/server-agent';
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) return '/api/heartbeat-monitors';
    return '/api/uptime-monitors';
}

function normalizeMonitorName(value) {
    return String(value || '').trim().toLowerCase();
}

function isDuplicateMonitorName(name, excludeId = null) {
    const normalized = normalizeMonitorName(name);
    if (!normalized) return false;

    const allMonitors = [
        ...state.monitors.uptime,
        ...state.monitors.server,
        ...state.monitors.heartbeat
    ];

    return allMonitors.some((monitor) => {
        if (!monitor) return false;
        if (excludeId && String(monitor.id) === String(excludeId)) return false;
        return normalizeMonitorName(monitor.name) === normalized;
    });
}

function openDashboardMonitorDetails(event, sourceType, monitorId) {
    if (event) event.preventDefault();
    if (!monitorId) return false;

    if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
        showServerMetrics(monitorId).catch((error) => {
            console.error('Failed to open server details:', error);
            showToast('Failed to open server details', 'error');
        });
        return false;
    }

    showMonitorDetails(monitorId).catch((error) => {
        console.error('Failed to open monitor details:', error);
        showToast('Failed to open monitor details', 'error');
    });
    return false;
}

function toTimestampMs(value) {
    if (!value) return null;
    let normalized = value;
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(value) && !/[zZ]|[+-]\d{2}:?\d{2}$/.test(value)) {
        normalized = `${value}Z`;
    }
    const date = new Date(normalized);
    const ms = date.getTime();
    return Number.isNaN(ms) ? null : ms;
}

function asNumberOrNull(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
}

function getLastCheckTimestampMs(monitor, sourceType) {
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
        return toTimestampMs(monitor.last_report_at);
    }
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) {
        return toTimestampMs(monitor.last_ping_at);
    }
    return toTimestampMs(monitor.last_check_at);
}

function getStatusSinceTimestampMs(monitor, sourceType, status) {
    const normalizedStatus = String(status || '').toLowerCase();
    const explicitStatusSince = toTimestampMs(monitor.status_since);
    if (explicitStatusSince) {
        return explicitStatusSince;
    }

    if (normalizedStatus === 'down') {
        return toTimestampMs(monitor.down_since) ?? getLastCheckTimestampMs(monitor, sourceType) ?? toTimestampMs(monitor.created_at);
    }
    if (normalizedStatus === 'up') {
        if (sourceType === MONITOR_SOURCE.WEBSITE) {
            return (
                toTimestampMs(monitor.last_up_at)
                ?? toTimestampMs(monitor.last_checkin_at)
                ?? toTimestampMs(monitor.last_check_at)
                ?? toTimestampMs(monitor.first_data_at)
                ?? toTimestampMs(monitor.created_at)
            );
        }
        if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
            return (
                toTimestampMs(monitor.last_checkin_at)
                ?? toTimestampMs(monitor.last_report_at)
                ?? toTimestampMs(monitor.first_data_at)
                ?? toTimestampMs(monitor.created_at)
            );
        }
        if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) {
            return (
                toTimestampMs(monitor.last_checkin_at)
                ?? toTimestampMs(monitor.last_ping_at)
                ?? toTimestampMs(monitor.first_data_at)
                ?? toTimestampMs(monitor.created_at)
            );
        }
    }
    if (normalizedStatus === 'maintenance') {
        return toTimestampMs(monitor.maintenance_start_at) ?? toTimestampMs(monitor.created_at);
    }
    return toTimestampMs(monitor.created_at);
}

function formatDurationFromMs(diffMs) {
    if (!Number.isFinite(diffMs) || diffMs <= 0) return '--';
    const totalMinutes = Math.floor(diffMs / (1000 * 60));
    const days = Math.floor(totalMinutes / (60 * 24));
    const hours = Math.floor((totalMinutes % (60 * 24)) / 60);
    const minutes = totalMinutes % 60;
    if (days > 0) return `${days}d ${hours}hr`;
    if (hours > 0) return `${hours}hr ${minutes}min`;
    return `${minutes}min`;
}

function formatCheckAge(ms) {
    if (!ms) return '--';
    const diffMs = Math.max(0, Date.now() - ms);
    const totalSec = Math.floor(diffMs / 1000);
    if (totalSec < 60) return `${Math.max(1, totalSec)}s`;

    const totalMin = Math.floor(totalSec / 60);
    if (totalMin < 60) return `${totalMin}m`;

    const totalHour = Math.floor(totalMin / 60);
    if (totalHour < 24) return `${totalHour}h`;

    const totalDay = Math.floor(totalHour / 24);
    return `${totalDay}d`;
}

function formatUptimeSeconds(totalSeconds) {
    if (!Number.isFinite(totalSeconds) || totalSeconds < 0) return '--';
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = Math.floor(totalSeconds % 60);
    if (days > 0) return `${days}d ${hours}h ${minutes}m ${seconds}s`;
    if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    return `${seconds}s`;
}

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function simplifyOsName(value) {
    const raw = String(value || '').trim();
    if (!raw) return '';

    const lower = raw.toLowerCase();

    if (lower.includes('windows 11')) return 'Windows 11';
    if (lower.includes('windows 10')) return 'Windows 10';
    if (lower.includes('windows server 2025')) return 'Windows Server 2025';
    if (lower.includes('windows server 2022')) return 'Windows Server 2022';
    if (lower.includes('windows server 2019')) return 'Windows Server 2019';
    if (lower.includes('windows server 2016')) return 'Windows Server 2016';
    if (lower.includes('windows server')) return 'Windows Server';
    if (lower.includes('windows')) return 'Windows';
    if (lower.includes('ubuntu')) return 'Ubuntu';
    if (lower.includes('debian')) return 'Debian';
    if (lower.includes('rocky')) return 'Rocky Linux';
    if (lower.includes('almalinux')) return 'AlmaLinux';
    if (lower.includes('centos')) return 'CentOS';
    if (lower.includes('red hat') || lower.includes('rhel')) return 'RHEL';
    if (lower.includes('amazon linux') || lower.includes('amzn')) return 'Amazon Linux';
    if (lower.includes('fedora')) return 'Fedora';
    if (lower.includes('opensuse')) return 'openSUSE';
    if (lower.includes('suse')) return 'SUSE Linux';
    if (lower.includes('linux mint')) return 'Linux Mint';
    if (lower.includes('kali')) return 'Kali Linux';
    if (lower.includes('arch')) return 'Arch Linux';
    if (lower.includes('alpine')) return 'Alpine Linux';
    if (lower.includes('gentoo')) return 'Gentoo';
    if (lower.includes('darwin') || lower.includes('mac os') || lower.includes('macos')) return 'macOS';

    const cleaned = raw
        .replace(/\(.*?\)/g, ' ')
        .replace(/gnu\/linux/ig, '')
        .replace(/\s+/g, ' ')
        .trim();
    if (!cleaned) return raw;
    const primary = cleaned.split(/[,:;/]/)[0].trim();
    return primary || cleaned;
}

function updateCheckAgeCells() {
    document.querySelectorAll('.check-age[data-last-check-ts]').forEach((el) => {
        const raw = el.getAttribute('data-last-check-ts');
        const ms = raw ? Number(raw) : Number.NaN;
        if (!Number.isFinite(ms) || ms <= 0) {
            el.textContent = '--';
            return;
        }
        el.textContent = formatCheckAge(ms);
    });
}

function formatUpDownDuration(status, sinceMs) {
    if (!Number.isFinite(sinceMs) || sinceMs <= 0) {
        return '--';
    }
    return formatDurationFromMs(Math.max(0, Date.now() - sinceMs));
}

function updateUpDownCells() {
    document.querySelectorAll('.up-down-age[data-status][data-status-since-ts]').forEach((el) => {
        const status = String(el.getAttribute('data-status') || '').toLowerCase();
        const raw = el.getAttribute('data-status-since-ts');
        const sinceMs = raw ? Number(raw) : Number.NaN;
        el.textContent = formatUpDownDuration(status, sinceMs);
    });
}

function getStableLastCheckTimestamp(key, nextTimestampMs) {
    const prev = state.lastCheckTimestamps.get(key);
    if (Number.isFinite(nextTimestampMs) && nextTimestampMs > 0) {
        if (!Number.isFinite(prev) || prev == null || nextTimestampMs >= prev) {
            state.lastCheckTimestamps.set(key, nextTimestampMs);
            return nextTimestampMs;
        }
        // Prevent visible backwards jumps from stale refreshes
        return prev;
    }
    if (Number.isFinite(prev) && prev > 0) {
        return prev;
    }
    return null;
}

document.addEventListener('DOMContentLoaded', async () => {
    try {
        await checkAuth();
        setupNavigation();
        setupLogout();
        setupSidebar();
        setupUserDropdown();

        await loadUserData();
        await loadAllData();

        state.refreshInterval = setInterval(loadAllData, DASHBOARD_POLL_MS);
        state.checkAgeInterval = setInterval(() => {
            updateCheckAgeCells();
            updateUpDownCells();
        }, CHECK_AGE_REFRESH_MS);

        document.body.classList.add('loaded');

        setTimeout(() => {
            if (!window.location.hash) {
                switchTab('overview');
            }
        }, 50);
    } catch (error) {
        console.error('Initialization error:', error);
    }
});

function setupSidebar() {
    const sidebar = document.getElementById('sidebar');
    const toggleBtn = document.getElementById('sidebar-toggle');
    const mobileMenuBtn = document.getElementById('mobile-menu-btn');

    if (state.sidebarCollapsed && sidebar) {
        sidebar.classList.add('collapsed');
    }

    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
            sidebar.classList.toggle('collapsed');
            state.sidebarCollapsed = sidebar.classList.contains('collapsed');
            localStorage.setItem('sidebarCollapsed', state.sidebarCollapsed);
        });
    }

    if (mobileMenuBtn) {
        mobileMenuBtn.addEventListener('click', () => {
            sidebar.classList.toggle('mobile-open');
        });
    }

    document.addEventListener('click', (e) => {
        if (sidebar && sidebar.classList.contains('mobile-open')) {
            if (!sidebar.contains(e.target) && !mobileMenuBtn.contains(e.target)) {
                sidebar.classList.remove('mobile-open');
            }
        }
    });
}

function setupUserDropdown() {
    const dropdown = document.getElementById('user-dropdown');
    const dropdownBtn = document.getElementById('user-dropdown-btn');
    const logoutBtnDropdown = document.getElementById('logout-btn-dropdown');

    if (dropdownBtn) {
        dropdownBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            dropdown.classList.toggle('open');
        });
    }

    // Close dropdown when clicking outside
    document.addEventListener('click', (e) => {
        if (dropdown && !dropdown.contains(e.target)) {
            dropdown.classList.remove('open');
        }
    });

    const dropdownItems = document.querySelectorAll('.dropdown-item[data-tab]');
    dropdownItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const tab = item.dataset.tab;
            switchTab(tab);
            dropdown.classList.remove('open');
        });
    });

    if (logoutBtnDropdown) {
        logoutBtnDropdown.addEventListener('click', () => {
            localStorage.removeItem('statrix_token');
            localStorage.removeItem('statrix_user');
            window.location.href = '/edit';
        });
    }
}

async function checkAuth() {
    const token = localStorage.getItem('statrix_token');
    if (!token) {
        window.location.href = '/edit';
        throw new Error('No token found');
    }
}

async function apiRequest(endpoint, options = {}) {
    const token = localStorage.getItem('statrix_token');

    const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
        ...options.headers
    };

    try {
        const response = await fetch(`${API_BASE}${endpoint}`, {
            ...options,
            headers
        });

        if (response.status === 401) {
            localStorage.removeItem('statrix_token');
            localStorage.removeItem('statrix_user');
            window.location.href = '/edit';
            throw new Error('Unauthorized');
        }

        return response;
    } catch (error) {
        if (error.message === 'Unauthorized') throw error;
        showToast('Network error occurred', 'error');
        throw error;
    }
}

async function readApiError(response, fallback) {
    try {
        const text = await response.text();
        if (text) {
            try {
                const data = JSON.parse(text);
                if (data && data.detail) {
                    if (Array.isArray(data.detail)) {
                        return data.detail.map(d => d?.msg || d?.detail || String(d)).join(', ');
                    }
                    return String(data.detail);
                }
            } catch (_) {
                return text;
            }
        }
    } catch (_) {
        // ignore
    }
    return fallback;
}

async function loadUserData() {
    let cachedUser = null;
    try {
        cachedUser = JSON.parse(localStorage.getItem('statrix_user') || 'null');
    } catch (_) {
        cachedUser = null;
    }

    let user = cachedUser && typeof cachedUser === 'object' ? cachedUser : null;

    // Always prefer live identity from backend to avoid stale/local fallback identities.
    try {
        const response = await apiRequest('/api/auth/me');
        if (response.ok) {
            const freshUser = await response.json();
            if (freshUser && typeof freshUser === 'object') {
                user = freshUser;
                localStorage.setItem('statrix_user', JSON.stringify(freshUser));
            }
        }
    } catch (_) {
        // Network/auth errors are handled by apiRequest; keep cached user if available.
    }

    if (!user || !user.email) {
        user = { email: '', role: 'admin', name: 'Admin' };
    }

    state.user = user;
    updateUserUI();
}

function updateUserUI() {
    const emailDisplay = document.getElementById('user-email-display');
    const nameDisplay = document.getElementById('user-name-display');
    const initialDisplay = document.getElementById('user-initial');
    const profileDisplay = document.getElementById('user-profile');

    // Account overview panel elements
    const accountName = document.getElementById('account-name');
    const accountUsername = document.getElementById('account-username');
    const accountEmail = document.getElementById('account-email');
    const accountRole = document.getElementById('account-role');
    const accountLastActivity = document.getElementById('account-last-activity');

    const email = state.user.email || '';
    const initial = email ? email.charAt(0).toUpperCase() : 'A';
    const name = state.user.name || 'Admin';

    if (emailDisplay) emailDisplay.textContent = email || '--';
    if (nameDisplay) nameDisplay.textContent = name;
    if (initialDisplay) initialDisplay.textContent = initial;

    if (accountName) accountName.textContent = name;
    if (accountUsername) accountUsername.textContent = state.user.username || name;
    if (accountEmail) accountEmail.textContent = email || '--';
    if (accountRole) accountRole.textContent = state.user.role || 'Admin';
    if (accountLastActivity) accountLastActivity.textContent = 'Just now';

    if (profileDisplay) {
        profileDisplay.innerHTML = `
            <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1.5rem;">
                <div style="width: 64px; height: 64px; background: linear-gradient(135deg, var(--primary) 0%, #369a93 100%); border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 1.5rem; font-weight: bold; color: #fff;">
                    ${initial}
                </div>
                <div>
                    <h4 style="margin: 0; font-size: 1.1rem; color: #fff;">${name}</h4>
                    <p class="text-muted" style="margin: 0;">${email || '--'}</p>
                </div>
            </div>
            <div class="settings-item">
                <span class="settings-label">Role</span>
                <span class="settings-value">${state.user.role || 'Admin'}</span>
            </div>
            <div class="settings-item">
                <span class="settings-label">Email</span>
                <span class="settings-value">${email || '--'}</span>
            </div>
            <div class="settings-item">
                <span class="settings-label">Joined</span>
                <span class="settings-value">${new Date().toLocaleDateString()}</span>
            </div>
        `;
    }
}

/**
 * Load all data (resilient: one failing API does not break the whole dashboard)
 */
async function loadAllData() {
    const requestSeq = ++loadAllDataRequestSeq;
    try {
        const endpoints = [
            '/api/uptime-monitors',
            '/api/heartbeat-monitors/server-agent',
            '/api/heartbeat-monitors',
            '/api/incidents'
        ];
        const fetchList = async (url) => {
            const response = await apiRequest(url);
            if (!response.ok) {
                const message = await readApiError(response, `Failed to load ${url}`);
                throw new Error(message);
            }
            const payload = await response.json();
            if (!Array.isArray(payload)) {
                throw new Error(`Invalid payload for ${url}`);
            }
            return payload;
        };

        const results = await Promise.allSettled(endpoints.map((url) => fetchList(url)));
        if (requestSeq !== loadAllDataRequestSeq) return;

        if (results[0].status === 'fulfilled') {
            state.monitors.uptime = results[0].value.map(m => ({
                ...m,
                maintenance_mode: m.maintenance_mode === true,
                _source: MONITOR_SOURCE.WEBSITE,
                monitor_kind: 'website'
            }));
        } else {
            console.warn('Failed to refresh uptime monitors:', results[0].reason);
        }

        if (results[1].status === 'fulfilled') {
            state.monitors.server = results[1].value.map(m => ({
                ...m,
                maintenance_mode: m.maintenance_mode === true,
                _source: MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT,
                monitor_kind: 'heartbeat',
                heartbeat_type: 'server_agent'
            }));
        } else {
            console.warn('Failed to refresh server monitors:', results[1].reason);
        }

        if (results[2].status === 'fulfilled') {
            state.monitors.heartbeat = results[2].value.map(m => ({
                ...m,
                maintenance_mode: m.maintenance_mode === true,
                _source: MONITOR_SOURCE.HEARTBEAT_CRONJOB,
                monitor_kind: 'heartbeat',
                heartbeat_type: m.heartbeat_type || 'cronjob'
            }));
        } else {
            console.warn('Failed to refresh heartbeat monitors:', results[2].reason);
        }

        if (results[3].status === 'fulfilled') {
            state.incidents = results[3].value;
        } else {
            console.warn('Failed to refresh incidents; keeping previous data:', results[3].reason);
        }

        // Fetch from same status API as public status page (single source of truth)
        try {
            const tzOffsetMinutes = -new Date().getTimezoneOffset();
            const statusRes = await fetch(`/api/public/status?tz_offset_minutes=${encodeURIComponent(String(tzOffsetMinutes))}&_=${Date.now()}`, {
                credentials: 'include',
                cache: 'no-store'
            });
            if (requestSeq !== loadAllDataRequestSeq) return;
            if (statusRes.ok) {
                const statusData = await statusRes.json();
                if (requestSeq !== loadAllDataRequestSeq) return;

                if (statusData.monitors && Array.isArray(statusData.monitors)) {
                    statusData.monitors.forEach(statusMonitor => {
                        const monitorList = statusMonitor.type === 'uptime'
                            ? state.monitors.uptime
                            : (statusMonitor.type === 'heartbeat' && statusMonitor.heartbeat_type === 'server_agent')
                                ? state.monitors.server
                                : statusMonitor.type === 'heartbeat'
                                    ? state.monitors.heartbeat
                                    : [];
                        const monitor = monitorList.find(m => String(m.id) === String(statusMonitor.id));
                        if (monitor) {
                            monitor.uptime_percentage = statusMonitor.uptime_percentage;
                            monitor.status = statusMonitor.status;
                            monitor.status_since = statusMonitor.status_since || null;
                            monitor.first_data_at = statusMonitor.first_data_at || monitor.first_data_at || null;
                            if (typeof statusMonitor.response_time_avg === 'number') {
                                monitor.response_time_avg = statusMonitor.response_time_avg;
                            }
                            if (typeof statusMonitor.maintenance_mode === 'boolean') {
                                monitor.maintenance_mode = statusMonitor.maintenance_mode;
                            }
                            if (statusMonitor.heartbeat_type) {
                                monitor.heartbeat_type = statusMonitor.heartbeat_type;
                            }
                            if (statusMonitor.monitor_kind) {
                                monitor.monitor_kind = statusMonitor.monitor_kind;
                            }
                            if (statusMonitor.history && Array.isArray(statusMonitor.history)) {
                                monitor.history = statusMonitor.history;
                            }
                            const isServerMonitor = statusMonitor.type === 'heartbeat' && statusMonitor.heartbeat_type === 'server_agent';
                            if (isServerMonitor && statusMonitor.metrics && typeof statusMonitor.metrics === 'object') {
                                const metrics = statusMonitor.metrics;
                                monitor.metrics = { ...metrics };

                                const cpu = asNumberOrNull(metrics.cpu);
                                const ram = asNumberOrNull(metrics.ram);
                                const disk = asNumberOrNull(metrics.disk_percent);
                                const netIn = asNumberOrNull(metrics.network_in);
                                const netOut = asNumberOrNull(metrics.network_out);
                                const load1 = asNumberOrNull(metrics.load_1);
                                const load5 = asNumberOrNull(metrics.load_5);
                                const load15 = asNumberOrNull(metrics.load_15);

                                if (cpu !== null) monitor.cpu_percent = cpu;
                                if (ram !== null) monitor.ram_percent = ram;
                                if (disk !== null) monitor.disk_percent = disk;
                                if (netIn !== null) monitor.network_in = netIn;
                                if (netOut !== null) monitor.network_out = netOut;
                                if (load1 !== null) monitor.load_1 = load1;
                                if (load5 !== null) monitor.load_5 = load5;
                                if (load15 !== null) monitor.load_15 = load15;
                            }
                        }
                    });
                }
                if (statusData.overall_uptime !== null && statusData.overall_uptime !== undefined) {
                    state.overallUptime = statusData.overall_uptime;
                }
            }
        } catch (e) {
            console.warn('Failed to fetch status data for uptime:', e);
        }
        if (requestSeq !== loadAllDataRequestSeq) return;

        updateSidebarCounts();
        refreshCurrentView();
        updateStats();
        if (state.activeTab === 'system') {
            await loadSystemResources();
        }
    } catch (error) {
        console.error('Error loading data:', error);
        showToast('Failed to load dashboard data', 'error');
    }
}

async function loadSystemResources(showToastOnError = false) {
    try {
        const response = await apiRequest('/api/system/resources');
        if (!response.ok) {
            const message = await readApiError(response, 'Failed to load system resources');
            if (showToastOnError) showToast(message, 'error');
            return;
        }
        const data = await response.json();
        state.systemResources = data;
        renderSystemResources(data);
    } catch (error) {
        if (showToastOnError) showToast('Failed to load system resources', 'error');
    }
}

function renderSystemResources(data) {
    if (!data) return;
    const cache = data.cache || {};
    const counts = cache.counts || {};
    const redis = data.redis || {};
    const database = data.database || {};
    const dbMetrics = database.metrics || {};
    const yesNo = (value) => (value === true ? 'Yes' : value === false ? 'No' : '--');
    const display = (value) => (value === null || value === undefined || value === '' ? '--' : String(value));
    const formatInt = (value) => (Number.isFinite(value) ? Number(value).toLocaleString() : '--');
    const formatPercent = (value) => (Number.isFinite(value) ? `${Number(value).toFixed(2)}%` : '--');
    const formatCacheBackend = (value) => {
        const key = String(value || '').trim().toLowerCase();
        if (!key) return '--';
        if (key === 'inmemory') return 'In-Memory';
        if (key === 'redis') return 'Redis';
        return value;
    };

    const cacheBackend = document.getElementById('system-cache-backend');
    const cacheEnabled = document.getElementById('system-cache-enabled');
    const cacheLoaded = document.getElementById('system-cache-loaded');
    const cacheTotal = document.getElementById('system-cache-total');
    const cacheBreakdown = document.getElementById('system-cache-breakdown');
    if (cacheBackend) cacheBackend.textContent = formatCacheBackend(cache.backend);
    if (cacheEnabled) cacheEnabled.textContent = cache.enabled ? 'Yes' : 'No';
    if (cacheLoaded) cacheLoaded.textContent = cache.loaded_at ? new Date(cache.loaded_at).toISOString() : '--';
    if (cacheTotal) cacheTotal.textContent = formatInt(counts.total_items);

    if (cacheBreakdown) {
        const rows = Object.entries(counts)
            .filter(([key]) => key !== 'total_items')
            .map(([key, value]) => `
                <div class="settings-item" style="padding: 0.4rem 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                    <span class="settings-label">${escapeHtml(key.replace(/_/g, ' '))}</span>
                    <span class="settings-value">${formatInt(value)}</span>
                </div>
            `)
            .join('');
        cacheBreakdown.innerHTML = rows || '<span class="settings-value text-muted">No cache data</span>';
    }

    const serverTime = document.getElementById('system-server-time');
    const appStarted = document.getElementById('system-app-started');
    const uptimeEl = document.getElementById('system-uptime');
    const pidEl = document.getElementById('system-pid');
    const pyEl = document.getElementById('system-python');
    const platformEl = document.getElementById('system-platform');
    const memoryEl = document.getElementById('system-memory');

    if (serverTime) serverTime.textContent = data.server_time_utc ? new Date(data.server_time_utc).toISOString() : '--';
    if (appStarted) appStarted.textContent = data.app_start_utc ? new Date(data.app_start_utc).toISOString() : '--';
    if (uptimeEl) uptimeEl.textContent = formatUptimeSeconds(data.uptime_seconds);
    if (pidEl) pidEl.textContent = data.process?.pid ?? '--';
    if (pyEl) pyEl.textContent = data.process?.python_version ?? '--';
    if (platformEl) platformEl.textContent = data.process?.platform ?? '--';
    if (memoryEl) memoryEl.textContent = data.process?.memory_mb != null ? data.process.memory_mb : '--';

    const redisConfigured = document.getElementById('system-redis-configured');
    const redisProvider = document.getElementById('system-redis-provider');
    const redisEndpoint = document.getElementById('system-redis-endpoint');
    const redisScheme = document.getElementById('system-redis-scheme');
    const redisTls = document.getElementById('system-redis-tls');
    const redisConnected = document.getElementById('system-redis-connected');
    const redisHealthy = document.getElementById('system-redis-healthy');
    const redisPrefix = document.getElementById('system-redis-prefix');
    const redisLastError = document.getElementById('system-redis-last-error');

    if (redisConfigured) redisConfigured.textContent = yesNo(redis.configured);
    if (redisProvider) redisProvider.textContent = display(redis.provider);
    if (redisEndpoint) redisEndpoint.textContent = display(redis.endpoint);
    if (redisScheme) redisScheme.textContent = display(redis.scheme);
    if (redisTls) redisTls.textContent = yesNo(redis.tls_enabled);
    if (redisConnected) redisConnected.textContent = yesNo(redis.connected);
    if (redisHealthy) redisHealthy.textContent = yesNo(redis.healthy);
    if (redisPrefix) redisPrefix.textContent = display(redis.key_prefix);
    if (redisLastError) redisLastError.textContent = display(redis.last_error);

    const pgConfigured = document.getElementById('system-pg-configured');
    const pgProvider = document.getElementById('system-pg-provider');
    const pgEndpoint = document.getElementById('system-pg-endpoint');
    const pgDatabase = document.getElementById('system-pg-database');
    const pgSsl = document.getElementById('system-pg-ssl');
    const pgPoolMin = document.getElementById('system-pg-pool-min');
    const pgPoolMax = document.getElementById('system-pg-pool-max');
    const pgPoolSize = document.getElementById('system-pg-pool-size');
    const pgPoolIdle = document.getElementById('system-pg-pool-idle');
    const pgPoolInUse = document.getElementById('system-pg-pool-in-use');
    const pgPoolUtilization = document.getElementById('system-pg-pool-utilization');
    const pgStatementCache = document.getElementById('system-pg-statement-cache');
    const pgActiveConnections = document.getElementById('system-pg-active-connections');
    const pgIdleConnections = document.getElementById('system-pg-idle-connections');
    const pgTotalConnections = document.getElementById('system-pg-total-connections');
    const pgMaxConnections = document.getElementById('system-pg-max-connections');
    const pgDatabaseSize = document.getElementById('system-pg-database-size');
    const pgCacheHit = document.getElementById('system-pg-cache-hit');
    const pgVersion = document.getElementById('system-pg-version');
    const pgUptime = document.getElementById('system-pg-uptime');
    const pgLastError = document.getElementById('system-pg-last-error');

    if (pgConfigured) pgConfigured.textContent = yesNo(database.configured);
    if (pgProvider) pgProvider.textContent = display(database.provider);
    if (pgEndpoint) pgEndpoint.textContent = display(database.endpoint);
    if (pgDatabase) pgDatabase.textContent = display(dbMetrics.database_name ?? database.database_name);
    if (pgSsl) pgSsl.textContent = display(database.ssl_mode);
    if (pgPoolMin) pgPoolMin.textContent = formatInt(database.pool_min_size);
    if (pgPoolMax) pgPoolMax.textContent = formatInt(database.pool_max_size);
    if (pgPoolSize) pgPoolSize.textContent = formatInt(database.pool_size);
    if (pgPoolIdle) pgPoolIdle.textContent = formatInt(database.pool_idle_connections);
    if (pgPoolInUse) pgPoolInUse.textContent = formatInt(database.pool_in_use_connections);
    if (pgPoolUtilization) pgPoolUtilization.textContent = formatPercent(database.pool_utilization_percent);
    if (pgStatementCache) pgStatementCache.textContent = formatInt(database.statement_cache_size);
    if (pgActiveConnections) pgActiveConnections.textContent = formatInt(dbMetrics.active_connections);
    if (pgIdleConnections) pgIdleConnections.textContent = formatInt(dbMetrics.idle_connections);
    if (pgTotalConnections) pgTotalConnections.textContent = formatInt(dbMetrics.total_connections);
    if (pgMaxConnections) pgMaxConnections.textContent = formatInt(dbMetrics.max_connections);
    if (pgDatabaseSize) pgDatabaseSize.textContent = display(dbMetrics.database_size_pretty);
    if (pgCacheHit) pgCacheHit.textContent = formatPercent(dbMetrics.cache_hit_ratio_percent);
    if (pgVersion) pgVersion.textContent = display(dbMetrics.server_version);
    if (pgUptime) pgUptime.textContent = formatUptimeSeconds(dbMetrics.server_uptime_seconds);
    if (pgLastError) pgLastError.textContent = display(dbMetrics.last_error);
}

function updateSidebarCounts() {
    const uptimeCount = document.getElementById('uptime-count');
    const serverCount = document.getElementById('server-count');
    const heartbeatCount = document.getElementById('heartbeat-count');

    if (uptimeCount) uptimeCount.textContent = state.monitors.uptime.length;
    if (serverCount) serverCount.textContent = state.monitors.server.length;
    if (heartbeatCount) heartbeatCount.textContent = state.monitors.heartbeat.length;
}

function refreshCurrentView() {
    const tab = state.activeTab;
    switch (tab) {
        case 'overview': loadOverview(); break;
        case 'uptime': loadUptimeTab(); break;
        case 'incidents': loadIncidentsTab(); break;
        case 'reports': loadReportsTab(); break;
        case 'system': loadSystemResources(); break;
    }
}

function setupNavigation() {
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const tab = item.dataset.tab;
            if (tab) switchTab(tab);
        });
    });

    // Breadcrumb navigation
    document.querySelectorAll('.ht-breadcrumb a[data-tab]').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            switchTab(link.dataset.tab);
        });
    });

    // Top bar logo link
    const topBarLogo = document.querySelector('.top-bar-logo[data-tab]');
    if (topBarLogo) {
        topBarLogo.addEventListener('click', (e) => {
            e.preventDefault();
            switchTab(topBarLogo.dataset.tab);
        });
    }

    // Stat cards in overview - manual click handlers (not auto-triggered)
    document.querySelectorAll('.stat-card').forEach(card => {
        card.addEventListener('click', (e) => {
            e.preventDefault();
            switchTab('uptime');
        });
    });
}

function switchTab(tabName) {
    if (!document.getElementById(`tab-${tabName}`)) {
        tabName = 'overview';
    }
    state.activeTab = tabName;

    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.tab === tabName);
    });

    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.toggle('active', pane.id === `tab-${tabName}`);
    });

    const titles = {
        'overview': 'Overview',
        'uptime': 'Uptime Monitors',
        'incidents': 'Incidents',
        'reports': 'Reports & Analytics',
        'system': 'System Resources'
    };
    const pageTitleEl = document.getElementById('page-title');
    if (pageTitleEl) pageTitleEl.textContent = titles[tabName] || 'Dashboard';

    const sidebar = document.getElementById('sidebar');
    if (sidebar) sidebar.classList.remove('mobile-open');

    refreshCurrentView();
}

function filterMonitors(type) {
    const searchInput = document.getElementById(`${type}-search`);
    const query = searchInput ? searchInput.value.toLowerCase().trim() : '';

    let monitors = [];
    let containerId = '';

    switch (type) {
        case 'uptime':
            monitors = [
                ...state.monitors.uptime.map(m => ({ ...m, monitorType: 'website' })),
                ...state.monitors.heartbeat.map(m => ({ ...m, monitorType: 'heartbeat-cronjob' })),
                ...state.monitors.server.map(m => ({ ...m, monitorType: 'heartbeat-server-agent' }))
            ];
            containerId = 'uptime-monitors-list';
            break;
        default:
            monitors = [
                ...state.monitors.uptime.map(m => ({ ...m, monitorType: 'website' })),
                ...state.monitors.heartbeat.map(m => ({ ...m, monitorType: 'heartbeat-cronjob' })),
                ...state.monitors.server.map(m => ({ ...m, monitorType: 'heartbeat-server-agent' }))
            ];
            containerId = 'uptime-monitors-list';
            break;
    }

    let filtered = monitors;

    if (type === 'uptime') {
        const activeTypeTab = document.querySelector('.monitor-type-tab.active');
        const filterType = activeTypeTab ? activeTypeTab.dataset.type : 'all';
        if (filterType !== 'all') {
            filtered = filtered.filter(m => (m.monitorType || m._source) === filterType);
        }
    }

    if (query) {
        filtered = filtered.filter(m =>
            m.name.toLowerCase().includes(query) ||
            (m.target && m.target.toLowerCase().includes(query)) ||
            (m.sid && m.sid.toLowerCase().includes(query))
        );
    }

    if (state.filters && state.filters[type]) {
        const { status, type: typeFilters } = state.filters[type];

        if (status && status.length > 0) {
            filtered = filtered.filter(m => {
                if (type === 'uptime') {
                    const sourceType = getSourceTypeFromMonitor(m);
                    let monitorStatus = 'paused';
                    if (m.maintenance_mode === true) {
                        monitorStatus = 'paused';
                    } else if (m.status === 'down') {
                        monitorStatus = 'down';
                    } else if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
                        monitorStatus = m.last_report_at ? 'up' : 'paused';
                    } else if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) {
                        monitorStatus = m.last_ping_at ? 'up' : 'paused';
                    } else if (typeof m.enabled === 'boolean') {
                        monitorStatus = m.enabled ? 'up' : 'paused';
                    }
                    return status.includes(monitorStatus);
                }
                let monitorStatus = m.enabled ? 'up' : 'paused';
                if (m.status === 'down') monitorStatus = 'down';
                return status.includes(monitorStatus);
            });
        }

        if (typeFilters && typeFilters.length > 0) {
            filtered = filtered.filter(m => {
                if (type === 'uptime') {
                    const unifiedType = getUnifiedMonitorTypeKey(m);
                    return typeFilters.includes(unifiedType);
                }
                const monitorType = m.type || 'http';
                return typeFilters.includes(monitorType);
            });
        }
    }

    if (state.sort && state.sort[type]) {
        const { sortBy, desc } = state.sort[type];

        filtered.sort((a, b) => {
            let comparison = 0;

            switch (sortBy) {
                case 'name':
                    comparison = a.name.localeCompare(b.name);
                    break;
                case 'added':
                    const addedA = a.created_at ? new Date(a.created_at).getTime() : 0;
                    const addedB = b.created_at ? new Date(b.created_at).getTime() : 0;
                    comparison = addedA - addedB;
                    break;
                case 'type':
                    const typeA = getSourceTypeLabel(getSourceTypeFromMonitor(a), a);
                    const typeB = getSourceTypeLabel(getSourceTypeFromMonitor(b), b);
                    comparison = String(typeA).localeCompare(String(typeB));
                    break;
                case 'status':
                    const statusOrder = { 'down': 0, 'paused': 1, 'up': 2, 'unknown': 3 };
                    if (type === 'uptime') {
                        const sourceTypeA = getSourceTypeFromMonitor(a);
                        const sourceTypeB = getSourceTypeFromMonitor(b);
                        let statusA = 'paused';
                        let statusB = 'paused';
                        if (a.maintenance_mode === true) statusA = 'paused';
                        else if (a.status === 'down') statusA = 'down';
                        else if (sourceTypeA === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) statusA = a.last_report_at ? 'up' : 'paused';
                        else if (sourceTypeA === MONITOR_SOURCE.HEARTBEAT_CRONJOB) statusA = a.last_ping_at ? 'up' : 'paused';
                        else if (typeof a.enabled === 'boolean') statusA = a.enabled ? 'up' : 'paused';
                        if (b.maintenance_mode === true) statusB = 'paused';
                        else if (b.status === 'down') statusB = 'down';
                        else if (sourceTypeB === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) statusB = b.last_report_at ? 'up' : 'paused';
                        else if (sourceTypeB === MONITOR_SOURCE.HEARTBEAT_CRONJOB) statusB = b.last_ping_at ? 'up' : 'paused';
                        else if (typeof b.enabled === 'boolean') statusB = b.enabled ? 'up' : 'paused';
                        comparison = statusOrder[statusA] - statusOrder[statusB];
                    } else {
                        const statusA = a.status === 'down' ? 'down' : (a.enabled ? 'up' : 'paused');
                        const statusB = b.status === 'down' ? 'down' : (b.enabled ? 'up' : 'paused');
                        comparison = statusOrder[statusA] - statusOrder[statusB];
                    }
                    break;
                case 'uptime':
                    const uptimeA = a.uptime_percentage || 0;
                    const uptimeB = b.uptime_percentage || 0;
                    comparison = uptimeA - uptimeB;
                    break;
                case 'response':
                    const responseA = a.response_time_avg || 0;
                    const responseB = b.response_time_avg || 0;
                    comparison = responseA - responseB;
                    break;
            }

            return desc ? -comparison : comparison;
        });
    }

    renderMonitorTable(filtered, containerId, false, type);
}

function clearSearch(type) {
    const searchInput = document.getElementById(`${type}-search`);
    if (searchInput) {
        searchInput.value = '';
        filterMonitors(type);
    }
}

function positionDropdownMenu(type, menuKey) {
    const menu = document.getElementById(`${type}-${menuKey}-menu`);
    const button = document.getElementById(`${type}-${menuKey}-btn`);
    if (!menu || !button) return;

    const rect = button.getBoundingClientRect();
    const menuRect = menu.getBoundingClientRect();
    const top = rect.bottom + window.scrollY + 6;
    let left = rect.right + window.scrollX - menuRect.width;

    if (!menuRect.width) {
        left = rect.left + window.scrollX;
    }

    if (left < 8) left = 8;

    menu.style.top = `${top}px`;
    menu.style.left = `${left}px`;
    menu.style.right = 'auto';
}

function toggleFilterMenu(type) {
    const menu = document.getElementById(`${type}-filter-menu`);
    if (!menu) return;

    const isVisible = menu.classList.contains('show');

    // Close all OTHER menus (not the target menu)
    document.querySelectorAll('.dropdown-menu').forEach(m => {
        if (m !== menu) {
            m.classList.remove('show');
            m.style.display = 'none';
        }
    });

    if (isVisible) {
        menu.classList.remove('show');
        setTimeout(() => { menu.style.display = 'none'; }, 200);
    } else {
        menu.style.display = 'block';
        positionDropdownMenu(type, 'filter');
        setTimeout(() => { menu.classList.add('show'); }, 10);
    }

    // Prevent clicks inside menu from closing it
    menu.onclick = (e) => {
        e.stopPropagation();
    };
}

function toggleSortMenu(type) {
    const menu = document.getElementById(`${type}-sort-menu`);
    if (!menu) return;

    const isVisible = menu.classList.contains('show');

    // Close all OTHER menus (not the target menu)
    document.querySelectorAll('.dropdown-menu').forEach(m => {
        if (m !== menu) {
            m.classList.remove('show');
            m.style.display = 'none';
        }
    });

    if (isVisible) {
        menu.classList.remove('show');
        setTimeout(() => { menu.style.display = 'none'; }, 200);
    } else {
        menu.style.display = 'block';
        positionDropdownMenu(type, 'sort');
        setTimeout(() => { menu.classList.add('show'); }, 10);
    }

    // Prevent clicks inside menu from closing it
    menu.onclick = (e) => {
        e.stopPropagation();
    };
}

function applyFilters(type) {
    const statusFilters = Array.from(document.querySelectorAll(`input[name="filter-status"]:checked`))
        .map(cb => cb.value);

    const typeFilters = Array.from(document.querySelectorAll(`input[name="filter-type"]:checked`))
        .map(cb => cb.value);

    state.filters = state.filters || {};
    state.filters[type] = { status: statusFilters, type: typeFilters };

    filterMonitors(type);
}

function resetFilters(type) {
    document.querySelectorAll(`input[name="filter-status"]`).forEach(cb => cb.checked = true);
    document.querySelectorAll(`input[name="filter-type"]`).forEach(cb => cb.checked = true);

    applyFilters(type);
    toggleFilterMenu(type);
}

function applySort(type) {
    const sortBy = document.querySelector(`input[name="sort-by"]:checked`)?.value || 'name';
    const desc = document.getElementById('sort-desc')?.checked || false;

    state.sort = state.sort || {};
    state.sort[type] = { sortBy, desc };

    filterMonitors(type);
}

document.addEventListener('click', (e) => {
    if (!e.target.closest('.dropdown-toggle')) {
        document.querySelectorAll('.dropdown-menu').forEach(menu => {
            if (!menu.contains(e.target)) {
                menu.classList.remove('show');
                menu.style.display = 'none';
            }
        });
    }
});

function setupLogout() {
    document.getElementById('logout-button').addEventListener('click', () => {
        localStorage.removeItem('statrix_token');
        localStorage.removeItem('statrix_user');
        window.location.href = '/edit';
    });
}

function updateStats() {
    const total = state.monitors.uptime.length + state.monitors.server.length + state.monitors.heartbeat.length;

    // Calculate stats per type
    const websiteTotal = state.monitors.uptime.length;
    const websiteDown = state.monitors.uptime.filter(m => (m.status === 'down') || !m.enabled).length;

    const serverTotal = state.monitors.server.length;
    const serverDown = state.monitors.server.filter(m => !m.last_report_at).length;

    const heartbeatTotal = state.monitors.heartbeat.length;
    const heartbeatDown = state.monitors.heartbeat.filter(m => !m.last_ping_at).length;
    const uptimeTotal = total;
    const uptimeDown = websiteDown + serverDown + heartbeatDown;

    const incidentsOpen = state.incidents.filter(i => i.status === 'open').length;
    const incidentsTotal = state.incidents.length;

    // Use accurate overall uptime from status API if available, otherwise fallback to simple logic
    let uptimePercent;
    let upCount = 0;
    let downCount = 0;
    if (state.overallUptime !== null && state.overallUptime !== undefined) {
        uptimePercent = state.overallUptime.toFixed(4);
        upCount = Math.round((state.overallUptime / 100) * total);
        downCount = total - upCount;
    } else {
        // Fallback: Simple logic for demo: "Up" if enabled/reported recently
        upCount =
            state.monitors.uptime.filter(m => m.enabled).length +
            state.monitors.server.filter(m => m.last_report_at).length +
            state.monitors.heartbeat.filter(m => m.last_ping_at).length;

        downCount = total - upCount;
        uptimePercent = total > 0 ? ((upCount / total) * 100).toFixed(4) : '100.0000';
    }

    const statrixWebsiteTotal = document.getElementById('statrix-website-total');
    if (statrixWebsiteTotal) statrixWebsiteTotal.textContent = `${websiteTotal}`;

    const statrixHeartbeatTotal = document.getElementById('statrix-heartbeat-total');
    if (statrixHeartbeatTotal) statrixHeartbeatTotal.textContent = `${heartbeatTotal}`;

    const statrixServerTotal = document.getElementById('statrix-server-total');
    if (statrixServerTotal) statrixServerTotal.textContent = `${serverTotal}`;

    const overallUptimeEl = document.getElementById('overall-uptime-value');
    if (overallUptimeEl) overallUptimeEl.textContent = `${uptimePercent}%`;

    const statTotalEl = document.getElementById('stat-total-monitors');
    const statUpEl = document.getElementById('stat-up-monitors');
    const statDownEl = document.getElementById('stat-down-monitors');
    const statUptimeEl = document.getElementById('stat-overall-uptime');

    if (statTotalEl) statTotalEl.textContent = total;
    if (statUpEl) statUpEl.textContent = upCount;
    if (statDownEl) statDownEl.textContent = downCount;
    if (statUptimeEl) statUptimeEl.textContent = `${uptimePercent}%`;

    const uptimeTotalEl = document.getElementById('stat-uptime-total');
    const uptimeDownEl = document.getElementById('stat-uptime-down');
    const serverTotalEl = document.getElementById('stat-server-total');
    const serverDownEl = document.getElementById('stat-server-down');
    const heartbeatTotalEl = document.getElementById('stat-heartbeat-total');
    const heartbeatMissedEl = document.getElementById('stat-heartbeat-missed');
    const incidentsOpenEl = document.getElementById('stat-incidents-open');
    const incidentsTotalEl = document.getElementById('stat-incidents-total');

    if (uptimeTotalEl) uptimeTotalEl.textContent = uptimeTotal;
    if (uptimeDownEl) uptimeDownEl.textContent = uptimeDown;
    if (serverTotalEl) serverTotalEl.textContent = serverTotal;
    if (serverDownEl) serverDownEl.textContent = serverDown;
    if (heartbeatTotalEl) heartbeatTotalEl.textContent = heartbeatTotal;
    if (heartbeatMissedEl) heartbeatMissedEl.textContent = heartbeatDown;
    if (incidentsOpenEl) incidentsOpenEl.textContent = incidentsOpen;
    if (incidentsTotalEl) incidentsTotalEl.textContent = incidentsTotal;

    const totalChecksEl = document.getElementById('total-checks');
    const avgResponseTimeEl = document.getElementById('avg-response-time');
    const uptimePercentageEl = document.getElementById('uptime-percentage');

    if (totalChecksEl) {
        // Estimate checks: assuming 60 second interval
        const checksPerDay = total * (24 * 60);
        const totalChecks = checksPerDay * 60; // 60 days
        totalChecksEl.textContent = formatNumber(totalChecks);
    }

    if (avgResponseTimeEl) {
        const monitorsWithResponse = state.monitors.uptime.filter(m => m.response_time_avg != null);
        if (monitorsWithResponse.length > 0) {
            const avgResponse = monitorsWithResponse.reduce((sum, m) => sum + (m.response_time_avg || 0), 0) / monitorsWithResponse.length;
            avgResponseTimeEl.textContent = `${Math.round(avgResponse)} ms`;
        }
    }

    if (uptimePercentageEl) {
        uptimePercentageEl.textContent = `${uptimePercent}%`;
    }

    const accountUptimeEl = document.getElementById('account-uptime');
    if (accountUptimeEl) accountUptimeEl.textContent = `${uptimePercent}%`;

    const uptimeCountEl = document.getElementById('uptime-count');
    const serverCountEl = document.getElementById('server-count');
    const heartbeatCountEl = document.getElementById('heartbeat-count');

    if (uptimeCountEl) uptimeCountEl.textContent = websiteTotal;
    if (serverCountEl) serverCountEl.textContent = serverTotal;
    if (heartbeatCountEl) heartbeatCountEl.textContent = heartbeatTotal;
}

function formatNumber(num, decimals = 0) {
    if (typeof num !== 'number') return '--';
    return num.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function loadOverview() {
    updateStats();
}

function loadUptimeTab() {
    const allMonitors = [
        ...state.monitors.uptime.map(m => ({ ...m, monitorType: 'website' })),
        ...state.monitors.heartbeat.map(m => ({ ...m, monitorType: 'heartbeat-cronjob' })),
        ...state.monitors.server.map(m => ({ ...m, monitorType: 'heartbeat-server-agent' }))
    ];

    const activeTypeTab = document.querySelector('.monitor-type-tab.active');
    const filterType = activeTypeTab ? activeTypeTab.dataset.type : 'all';

    let filteredMonitors = allMonitors;
    if (filterType !== 'all') {
        filteredMonitors = allMonitors.filter(m => m.monitorType === filterType);
    }

    renderMonitorTable(filteredMonitors, 'uptime-monitors-list', false, 'uptime');
}

function filterByType(type) {
    document.querySelectorAll('.monitor-type-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.type === type);
    });

    loadUptimeTab();
}

function getIncidentMonitorTypeLabel(monitorSource) {
    const value = String(monitorSource || '').trim().toLowerCase();
    if (value === 'website') return 'Website';
    if (value === 'heartbeat-cronjob') return 'Cronjob';
    if (value === 'heartbeat-server-agent') return 'Server Agent';
    return '';
}

function getIncidentMonitorSelectOptions() {
    const monitors = [
        ...state.monitors.uptime.map((m) => ({
            id: m.id,
            name: m.name,
            monitorSource: 'website'
        })),
        ...state.monitors.heartbeat.map((m) => ({
            id: m.id,
            name: m.name,
            monitorSource: 'heartbeat-cronjob'
        })),
        ...state.monitors.server.map((m) => ({
            id: m.id,
            name: m.name,
            monitorSource: 'heartbeat-server-agent'
        }))
    ];

    monitors.sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), undefined, { sensitivity: 'base' }));

    return monitors.map((monitor) => {
        const typeLabel = getIncidentMonitorTypeLabel(monitor.monitorSource);
        const name = escapeHtml(monitor.name || 'Unnamed monitor');
        const value = `${monitor.monitorSource}|${monitor.id}`;
        return `<option value="${escapeHtml(value)}">${name}${typeLabel ? ` (${escapeHtml(typeLabel)})` : ''}</option>`;
    }).join('');
}

function loadIncidentsTab() {
    const container = document.getElementById('incidents-table');
    if (!container) return;

    if (state.incidents.length === 0) {
        container.innerHTML = `
            <div class="no-data-state">
                <i class="fas fa-check-circle"></i>
                <h3>No Incidents</h3>
                <p>Create an incident notice when you need to communicate downtime, maintenance, or degraded service.</p>
            </div>
        `;
        return;
    }

    const html = state.incidents.map((incident) => {
        const incidentType = (incident.incident_type || 'warning').toLowerCase();
        const incidentTypeClass = incidentType === 'down'
            ? 'critical'
            : incidentType === 'up'
                ? 'recovery'
            : incidentType === 'info'
                ? 'info'
                : 'warning';
        const incidentTypeLabel = incidentType === 'down'
            ? 'Critical'
            : incidentType === 'info'
                ? 'Info'
                : incidentType === 'up'
                    ? 'Recovery'
                    : 'Warning';
        const sourceLabel = (incident.source || 'monitor') === 'admin'
            ? 'Admin Notice'
            : `${String(incident.monitor_type || 'monitor').toUpperCase()} monitor`;
        const isAdminNotice = (incident.source || 'monitor') === 'admin';
        const monitorTypeLabel = getIncidentMonitorTypeLabel(incident.monitor_source);
        const affectedMonitorLabel = incident.monitor_name
            ? `${incident.monitor_name}${monitorTypeLabel ? ` (${monitorTypeLabel})` : ''}`
            : (isAdminNotice ? 'All Services' : '--');
        const startedLabel = incident.started_at
            ? new Date(incident.started_at).toLocaleString()
            : '--';
        const resolvedLabel = incident.resolved_at
            ? new Date(incident.resolved_at).toLocaleString()
            : null;
        const resolvedExpires = incident.resolved_expires_at
            ? new Date(incident.resolved_expires_at).toLocaleString()
            : (incident.resolved_at
                ? new Date(new Date(incident.resolved_at).getTime() + (INCIDENT_RESOLVED_RETENTION_HOURS * 60 * 60 * 1000)).toLocaleString()
                : null);
        const isHiddenFromStatusPage = incident.hidden_from_status_page === true;
        const hiddenFromStatusAt = incident.hidden_from_status_page_at
            ? new Date(incident.hidden_from_status_page_at).toLocaleString()
            : null;
        const statusClass = incident.status === 'open' ? 'open' : 'resolved';
        const statusText = incident.status === 'open' ? 'Open' : 'Resolved';
        const description = incident.description ? escapeHtml(incident.description) : '';

        return `
            <div class="incident-row">
                <div class="monitor-name-cell">
                    <div>
                        <div style="font-weight: 600; color: #fff; margin-bottom: 0.35rem;">${escapeHtml(incident.title || 'Untitled Incident')}</div>
                        <div style="display:flex; align-items:center; gap:0.4rem; flex-wrap:wrap; margin-bottom:0.35rem;">
                            <span class="incident-source-badge">${escapeHtml(sourceLabel)}</span>
                            <span class="incident-source-badge">Affected: ${escapeHtml(affectedMonitorLabel)}</span>
                            <span class="incident-type-badge incident-type-${incidentTypeClass}">${incidentTypeLabel}</span>
                        </div>
                        ${description ? `<div class="incident-description-preview">${description}</div>` : ''}
                        <div class="incident-meta-line">
                            <span><i class="fas fa-play-circle"></i> Started: ${escapeHtml(startedLabel)}</span>
                            ${resolvedLabel ? `<span><i class="fas fa-check-circle"></i> Resolved: ${escapeHtml(resolvedLabel)}</span>` : ''}
                            ${isAdminNotice && isHiddenFromStatusPage
                ? `<span><i class="fas fa-eye-slash"></i> Removed from public status page${hiddenFromStatusAt ? `: ${escapeHtml(hiddenFromStatusAt)}` : ''}</span>`
                : (isAdminNotice && resolvedExpires
                    ? `<span><i class="fas fa-hourglass-end"></i> Hidden from status page: ${escapeHtml(resolvedExpires)}</span>`
                    : '')}
                        </div>
                    </div>
                </div>
                <div class="monitor-status-cell">
                    <span class="status-badge ${statusClass}">${statusText}</span>
                </div>
                <div class="monitor-actions">
                    ${incident.status === 'open' ? `<button class="btn btn-small btn-primary" onclick="resolveIncident('${incident.id}')">Resolve</button>` : ''}
                    ${incident.status === 'resolved' && isAdminNotice && !isHiddenFromStatusPage ? `<button class="btn btn-small btn-danger" onclick="hideIncidentFromStatus('${incident.id}')">Delete</button>` : ''}
                </div>
            </div>
        `;
    }).join('');

    container.innerHTML = html;
}

async function fetchIncidentTemplates(force = false) {
    if (!force && incidentTemplates.length > 0) {
        return incidentTemplates;
    }

    const response = await apiRequest('/api/incidents/templates');
    if (!response.ok) {
        throw new Error('Failed to load incident templates');
    }

    const payload = await response.json();
    incidentTemplates = Array.isArray(payload) ? payload : [];
    incidentTemplateMap = {};
    incidentTemplates.forEach((template) => {
        if (template && template.key) {
            incidentTemplateMap[template.key] = template;
        }
    });
    return incidentTemplates;
}

async function openCreateIncidentModal() {
    try {
        await fetchIncidentTemplates();
    } catch (error) {
        console.warn('Failed to load incident templates:', error);
        incidentTemplates = [];
        incidentTemplateMap = {};
        showToast('Templates are unavailable right now. You can still create a custom incident.', 'warning');
    }

    const templateOptions = incidentTemplates.map((template) => `
        <option value="${escapeHtml(template.key)}">${escapeHtml(template.name)}</option>
    `).join('');
    const monitorOptions = getIncidentMonitorSelectOptions();

    showModal(`
        <div class="modal-header">
            <h2 class="modal-title">Create Incident Notice</h2>
        </div>
        <div class="modal-body">
            <div class="form-group">
                <label for="incident-template-select">Template</label>
                <select id="incident-template-select" onchange="applyIncidentTemplate()">
                    <option value="">Custom incident (no template)</option>
                    ${templateOptions}
                </select>
                <p class="text-muted" style="margin-top: 0.4rem; font-size: 0.75rem;">
                    Templates prefill the title, severity, and message for common outage scenarios.
                </p>
            </div>

            <div class="form-group">
                <label for="incident-severity">Severity</label>
                <select id="incident-severity">
                    <option value="down">Critical Outage</option>
                    <option value="warning" selected>Warning / Degraded</option>
                    <option value="info">Informational</option>
                    <option value="up">Recovery</option>
                </select>
            </div>

            <div class="form-group">
                <label for="incident-monitor-select">Affected Monitor</label>
                <select id="incident-monitor-select">
                    <option value="all">All Services</option>
                    ${monitorOptions}
                </select>
                <p class="text-muted" style="margin-top: 0.4rem; font-size: 0.75rem;">
                    Select a specific monitor so users can quickly identify what is impacted.
                </p>
            </div>

            <div class="form-group">
                <label for="incident-title">Title</label>
                <input id="incident-title" type="text" maxlength="500" placeholder="e.g. Partial API outage in progress">
            </div>

            <div class="form-group">
                <label for="incident-description">Description</label>
                <textarea id="incident-description" rows="5" maxlength="5000" placeholder="Share current impact, what is affected, and next update timing."></textarea>
                <p class="text-muted" style="margin-top: 0.4rem; font-size: 0.75rem;">
                    After an incident is resolved, the notice remains visible on the public status page for ${INCIDENT_RESOLVED_RETENTION_HOURS} hours.
                </p>
            </div>
        </div>
        <div class="modal-footer">
            <button class="btn btn-secondary" onclick="hideModal()">Cancel</button>
            <button class="btn btn-primary" onclick="createAdminIncident()">Publish Incident</button>
        </div>
    `);

    const titleInput = document.getElementById('incident-title');
    if (titleInput) titleInput.focus();
}

function applyIncidentTemplate() {
    const templateSelect = document.getElementById('incident-template-select');
    const severitySelect = document.getElementById('incident-severity');
    const titleInput = document.getElementById('incident-title');
    const descriptionInput = document.getElementById('incident-description');

    if (!templateSelect || !severitySelect || !titleInput || !descriptionInput) {
        return;
    }

    const templateKey = templateSelect.value;
    if (!templateKey) return;

    const template = incidentTemplateMap[templateKey];
    if (!template) return;

    severitySelect.value = template.incident_type || 'warning';
    titleInput.value = template.title || '';
    descriptionInput.value = template.description || '';
}

async function createAdminIncident() {
    const templateSelect = document.getElementById('incident-template-select');
    const severitySelect = document.getElementById('incident-severity');
    const monitorSelect = document.getElementById('incident-monitor-select');
    const titleInput = document.getElementById('incident-title');
    const descriptionInput = document.getElementById('incident-description');

    const title = (titleInput?.value || '').trim();
    const description = (descriptionInput?.value || '').trim();
    const incidentType = (severitySelect?.value || 'warning').trim().toLowerCase();
    const templateKey = (templateSelect?.value || '').trim();
    const monitorValue = (monitorSelect?.value || 'all').trim();

    let monitorSource = 'all';
    let monitorId = null;
    if (monitorValue && monitorValue !== 'all') {
        const separatorIndex = monitorValue.indexOf('|');
        if (separatorIndex > 0) {
            monitorSource = monitorValue.slice(0, separatorIndex);
            monitorId = monitorValue.slice(separatorIndex + 1) || null;
        }
    }

    if (!title) {
        showToast('Incident title is required', 'error');
        return;
    }

    try {
        const response = await apiRequest('/api/incidents/admin', {
            method: 'POST',
            body: JSON.stringify({
                title,
                description: description || null,
                incident_type: incidentType,
                template_key: templateKey || null,
                monitor_source: monitorSource,
                monitor_id: monitorId
            })
        });

        if (!response.ok) {
            const message = await readApiError(response, 'Failed to create incident');
            showToast(message, 'error');
            return;
        }

        showToast('Incident notice published');
        hideModal();
        await loadAllData();
        switchTab('incidents');
    } catch (error) {
        console.error(error);
        showToast('Failed to create incident', 'error');
    }
}

function getStatusDropdownOptions(sourceType, monitor) {
    const isMaintenanceMode = monitor.maintenance_mode === true;

    if (isMaintenanceMode) {
        return `
            <a href="#" class="dropdown-item" onclick="endMaintenanceMode('${sourceType}', '${monitor.id}'); closeAllDropdowns(); return false;">
                <i class="fas fa-check-circle"></i> Active Mode
            </a>
        `;
    }

    return `
        <a href="#" class="dropdown-item" onclick="setMaintenanceMode('${sourceType}', '${monitor.id}'); closeAllDropdowns(); return false;">
            <i class="fas fa-wrench"></i> Maintenance Mode
        </a>
    `;
}

function clampPercentage(value) {
    const numeric = asNumberOrNull(value);
    if (numeric === null) return null;
    return Math.max(0, Math.min(100, numeric));
}

function formatBytes(value) {
    const numeric = asNumberOrNull(value);
    if (numeric === null || numeric < 0) return '--';
    if (numeric < 1024) return `${numeric.toFixed(0)} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let scaled = numeric / 1024;
    let unitIndex = 0;
    while (scaled >= 1024 && unitIndex < units.length - 1) {
        scaled /= 1024;
        unitIndex += 1;
    }
    return `${scaled.toFixed(scaled >= 100 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatBytesPerSecond(value) {
    const formatted = formatBytes(value);
    return formatted === '--' ? '--' : `${formatted}/s`;
}

function getServerMetricValue(monitor, keys) {
    for (const key of keys) {
        const metricValue = asNumberOrNull(monitor?.metrics?.[key]);
        if (metricValue !== null) return metricValue;

        const directValue = asNumberOrNull(monitor?.[key]);
        if (directValue !== null) return directValue;
    }
    return null;
}

function normalizeBarPercent(percent, minVisiblePercent = 0) {
    if (!Number.isFinite(percent) || percent <= 0) return 0;
    return Math.min(100, Math.max(minVisiblePercent, percent));
}

function buildServerQuickBar(fillPercent, fillClass, titleText, fillStyle = '') {
    const width = normalizeBarPercent(fillPercent);
    const safeTitle = escapeHtml(titleText || '');
    const safeFillStyle = fillStyle ? ` ${fillStyle}` : '';
    return `
        <div class="server-quick-bar" title="${safeTitle}">
            <div class="server-quick-bar-fill ${fillClass}" style="width:${width.toFixed(1)}%;${safeFillStyle}"></div>
        </div>
    `;
}

function getServerUsageBarsHtml(monitor) {
    const status = String(monitor?.status || '').toLowerCase();
    if (status === 'down' || status === 'unknown' || status === 'no_data' || status === 'not_created') {
        return '';
    }

    const cpu = clampPercentage(getServerMetricValue(monitor, ['cpu', 'cpu_percent', 'cpu_usage']));
    const ram = clampPercentage(getServerMetricValue(monitor, ['ram', 'ram_percent', 'ram_usage']));
    const disk = clampPercentage(getServerMetricValue(monitor, ['disk_percent', 'disk_usage']));
    const netIn = getServerMetricValue(monitor, ['network_in']);
    const netOut = getServerMetricValue(monitor, ['network_out']);

    const hasAnyMetric = [cpu, ram, disk, netIn, netOut].some(v => v !== null);
    if (!hasAnyMetric) return '';

    const cpuWidth = normalizeBarPercent(cpu ?? 0, 2);
    const ramWidth = normalizeBarPercent(ram ?? 0, 2);
    const diskWidth = normalizeBarPercent(disk ?? 0, 2);

    const inBitsPerSec = Math.max(0, netIn || 0) * 8;
    const outBitsPerSec = Math.max(0, netOut || 0) * 8;
    const netTotalBitsPerSec = inBitsPerSec + outBitsPerSec;
    const netWidth = netTotalBitsPerSec > 0
        ? normalizeBarPercent((netTotalBitsPerSec / 100000000) * 100, 3)
        : 0;
    const netInRatio = netTotalBitsPerSec > 0 ? (inBitsPerSec / netTotalBitsPerSec) * 100 : 50;
    const netGradient = `background: linear-gradient(to right, #70c86d ${netInRatio.toFixed(1)}%, #56c1df ${netInRatio.toFixed(1)}%);`;

    return `
        <div class="server-quick-bars">
            ${buildServerQuickBar(cpuWidth, 'server-quick-bar-fill-cpu', `CPU: ${cpu === null ? '--' : `${cpu.toFixed(1)}%`}`)}
            ${buildServerQuickBar(ramWidth, 'server-quick-bar-fill-ram', `RAM: ${ram === null ? '--' : `${ram.toFixed(1)}%`}`)}
            ${buildServerQuickBar(diskWidth, 'server-quick-bar-fill-disk', `DISK: ${disk === null ? '--' : `${disk.toFixed(1)}%`}`)}
            ${buildServerQuickBar(netWidth, 'server-quick-bar-fill-net', `NET In ${formatBytesPerSecond(netIn || 0)} • Out ${formatBytesPerSecond(netOut || 0)}`, netGradient)}
        </div>
    `;
}

function renderMonitorTable(monitors, containerId, isOverview = false, type = null, showActions = null) {
    const container = document.getElementById(containerId);
    if (!container) return;

    if (showActions === null || showActions === undefined) {
        showActions = !isOverview;
    }

    if (monitors.length === 0) {
        container.innerHTML = `
            <div class="panel-placeholder">
                <i class="fas fa-inbox"></i>
                <h3>No Monitors Found</h3>
                <p>Add your first monitor to start tracking uptime.</p>
            </div>
        `;

        const paginationContainer = document.getElementById(`${containerId}-pagination`);
        if (paginationContainer) paginationContainer.innerHTML = '';
        return;
    }

    // Use pagination if not overview
    let displayData = monitors;
    if (!isOverview && type) {
        const paginated = getPaginatedData(monitors, type);
        displayData = paginated.data;

        renderPagination(type, `${containerId}-pagination`);
    }

    let html = `
        <table class="data-table">
            <thead>
                <tr>
                    <th class="col-name">Name</th>
                    ${!isOverview ? '<th class="col-uptime">Uptime</th>' : ''}
                    <th class="col-type">Type</th>
                    <th class="col-up-down">Up/Down <i class="fas fa-sort"></i></th>
                    <th class="col-added">Added <i class="fas fa-sort"></i></th>
                    <th class="col-check">Check</th>
                    <th class="col-status">Status</th>
                    ${showActions ? '<th class="col-actions">Action</th>' : ''}
                </tr>
            </thead>
            <tbody>
    `;
    displayData.forEach(m => {
        // Determine status, badge, and source type
        let statusClass = 'unknown';
        let subText = '';
        let badge = '';
        // Use actual uptime from status API (same as public status page) - never fake 100%
        const uptimeValue = m.uptime_percentage !== null && m.uptime_percentage !== undefined
            ? (typeof m.uptime_percentage === 'number' ? m.uptime_percentage.toFixed(4) + '%' : `${m.uptime_percentage}%`)
            : 'N/A';
        let uptimeHtml = uptimeValue;
        const sourceType = getSourceTypeFromMonitor(m);
        let metricsHtml = '';

        if (sourceType === MONITOR_SOURCE.WEBSITE) {
            statusClass = m.maintenance_mode === true ? 'maintenance' : (m.status || (m.enabled ? 'up' : 'unknown'));
            subText = m.target || '';
            badge = getCompactSourceTypeLabel(sourceType);
            // Keep uptimeHtml from status API; do not overwrite with 100%
        } else if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
            statusClass = m.maintenance_mode === true ? 'maintenance' : (m.status || (m.last_report_at ? 'up' : 'unknown'));
            subText = '';
            badge = getCompactSourceTypeLabel(sourceType);
            // Keep uptime from status API; do not overwrite with 100%
            metricsHtml = isOverview ? '' : getServerUsageBarsHtml(m);
        } else if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) {
            statusClass = m.maintenance_mode === true ? 'maintenance' : (m.status || (m.last_ping_at ? 'up' : 'unknown'));
            subText = 'Ping Policy: 1min';
            badge = getCompactSourceTypeLabel(sourceType);
        }

        let lastCheckTimestampMs = getLastCheckTimestampMs(m, sourceType);
        if (sourceType === MONITOR_SOURCE.WEBSITE) {
            if (!lastCheckTimestampMs) {
                lastCheckTimestampMs = toTimestampMs(m.last_up_at);
            }
        }
        const stableKey = `${sourceType}:${m.id}`;
        lastCheckTimestampMs = getStableLastCheckTimestamp(stableKey, lastCheckTimestampMs);

        const statusSinceMs = getStatusSinceTimestampMs(m, sourceType, statusClass);
        const upDownDuration = formatUpDownDuration(statusClass, statusSinceMs);
        const upDownColor = statusClass === 'up'
            ? '#44b6ae'
            : (statusClass === 'down' ? '#e74c3c' : '#6b7785');

        // 7-day bar: use real history from status API; only show for monitors with data; gray for unknown/down
        // 7-day bar: use real history from status API ('up'|'partial'|'down'|'unknown'|'not_created')
        const hasHistory = m.history && Array.isArray(m.history) && m.history.length > 0;
        const isActiveOrUp = statusClass === 'up';
        let uptimeBarHtml = '';
        if (hasHistory) {
            uptimeBarHtml = '<div class="mini-uptime-bar" style="display: flex; gap: 2px; margin-top: 4px;">';
            const len = Math.min(7, m.history.length);
            for (let i = 0; i < 7; i++) {
                const entry = i < len ? m.history[i] : 'unknown';
                const dayStatus = (entry && typeof entry === 'object') ? (entry.status || 'unknown') : entry;
                const color = dayStatus === 'up'
                    ? '#44b6ae'
                    : (dayStatus === 'down'
                        ? '#e74c3c'
                        : (dayStatus === 'partial'
                            ? '#f3c200'
                            : (dayStatus === 'maintenance' ? '#1f2937' : '#6b7785')));
                uptimeBarHtml += `<div class="mini-bar-segment" style="width: 8px; height: 16px; border-radius: 2px; background: ${color};"></div>`;
            }
            uptimeBarHtml += '</div>';
        } else if (isActiveOrUp) {
            uptimeBarHtml = '<div class="mini-uptime-bar" style="display: flex; gap: 2px; margin-top: 4px;">';
            for (let i = 0; i < 7; i++) {
                uptimeBarHtml += `<div class="mini-bar-segment" style="width: 8px; height: 16px; border-radius: 2px; background: #6b7785;"></div>`;
            }
            uptimeBarHtml += '</div>';
        }
        // Down/unknown with no history: no bar

        // Format added date
        const addedDate = m.created_at ? new Date(m.created_at).toLocaleDateString() : '--';

        const lastCheckText = formatCheckAge(lastCheckTimestampMs);

        const isPublic = m.is_public || false;
        const canOpenDetails = true;
        const isPaused = !m.enabled;

        // Actions - 3 Button Layout (Eye, Pencil, Gear)
        let actions = '';
        if (showActions) {
            actions = `
                <button class="btn-action-icon" onclick="setVisibility('${sourceType}', '${m.id}', ${!isPublic})" title="${isPublic ? 'Make Private' : 'Make Public'}">
                    <i class="fas fa-${isPublic ? 'eye' : 'eye-slash'}" style="color: ${isPublic ? '#44b6ae' : 'inherit'};"></i>
                </button>
                <button class="btn-action-icon" onclick="showAddMonitorModal('${sourceType}', '${m.id}')" title="Edit Monitor">
                    <i class="fas fa-pencil-alt"></i>
                </button>
                <button class="btn-action-icon" onclick="openMonitorTools('${sourceType}', '${m.id}')" title="Monitor Tools">
                    <i class="fas fa-cog"></i>
                </button>
            `;
        }

        // Status for dropdown button
        const statusBadgeClass = statusClass === 'up' ? 'status-up' : (statusClass === 'down' ? 'status-down' : 'status-unknown');
        let statusText = 'Unknown';
        if (statusClass === 'up') statusText = 'Active';
        else if (statusClass === 'down') statusText = 'Down';
        else if (statusClass === 'maintenance') statusText = 'Maintenance';
        else if (statusClass === 'paused') statusText = 'Paused';
        else statusText = 'Unknown';

        const monitorId = String(m.id);
        const monitorNameMarkup = sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT && !isOverview
            ? (
                canOpenDetails
                    ? `
                <a href="#" class="monitor-name-link server-name-link" onclick="return openDashboardMonitorDetails(event, '${sourceType}', '${monitorId}')">
                    <i class="fas fa-chevron-right server-name-chevron"></i>
                    <span class="monitor-name-text">${escapeHtml(m.name)}</span>
                </a>
            `
                    : `
                <span class="server-name-link">
                    <i class="fas fa-chevron-right server-name-chevron"></i>
                    <span class="monitor-name-text">${escapeHtml(m.name)}</span>
                </span>
            `
            )
            : (
                canOpenDetails
                    ? `
                <a href="#" class="monitor-name-link" onclick="return openDashboardMonitorDetails(event, '${sourceType}', '${monitorId}')">
                    <span class="monitor-name-text">${escapeHtml(m.name)}</span>
                </a>
            `
                    : `
                <span class="monitor-name-text">${escapeHtml(m.name)}</span>
            `
            );

        const uptimeMarkup = (canOpenDetails && uptimeValue !== 'N/A')
            ? `<a href="#" class="monitor-name-link uptime-percent" onclick="return openDashboardMonitorDetails(event, '${sourceType}', '${monitorId}')">${uptimeHtml}</a>`
            : uptimeHtml;


        html += `
            <tr class="data-row" data-id="${m.id}">
                <td class="col-name ${sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT && !isOverview ? 'server-name-cell' : ''}">
                    <div class="monitor-name-info">
                        ${monitorNameMarkup}
                        ${metricsHtml}
                        ${subText ? `<span class="monitor-sub-text">${subText}</span>` : ''}
                    </div>
                </td>
                ${!isOverview ? `
                <td class="col-uptime">
                    <div class="uptime-cell-content">
                        <span class="uptime-percent">${uptimeMarkup}</span>
                        ${uptimeBarHtml}
                </div>
                </td>
                ` : ''}
                <td class="col-type">
                    <span class="type-badge">${badge}</span>
                </td>
                <td class="col-up-down">
                    <span class="up-down-age" data-status="${statusClass}" data-status-since-ts="${statusSinceMs || ''}" style="color: ${upDownColor};">${upDownDuration}</span>
                </td>
                <td class="col-added">
                    ${addedDate}
                </td>
                <td class="col-check">
                    <span class="check-age" data-last-check-ts="${lastCheckTimestampMs || ''}">${lastCheckText}</span>
                </td>
                <td class="col-status">
                    <div class="status-dropdown" style="position: relative; display: inline-block;">
                        <button class="status-dropdown-btn dropdown-toggle ${statusBadgeClass}" onclick="toggleStatusDropdown(event, this)" style="border: none; background: transparent; cursor: pointer; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 600;">
                            ${statusText} <i class="fas fa-caret-down" style="margin-left: 4px; font-size: 10px;"></i>
                        </button>
                        <div class="dropdown-menu status-dropdown-menu" data-monitor-id="${m.id}" style="display: none; min-width: 280px; position: absolute; top: 100%; left: 0; margin-top: 4px; z-index: 1000;">
                            ${getStatusDropdownOptions(sourceType, m)}
                        </div>
                    </div>
                </td>
                ${showActions ? `
                <td class="col-actions">
                    <div class="action-buttons">${actions}</div>
                </td>
                ` : ''}
            </tr>
        `;
    });

    html += '</tbody></table>';
    container.innerHTML = html;
    updateCheckAgeCells();
    updateUpDownCells();
}

function getBgClass(val) {
    if (val < 60) return 'bg-success';
    if (val < 90) return 'bg-warning';
    return 'bg-danger';
}

function formatBytesFromKb(kb) {
    if (!kb || isNaN(kb)) return '--';
    const bytes = Number(kb) * 1024;
    const gb = bytes / (1024 ** 3);
    if (gb >= 1) return `${gb.toFixed(1)} GB`;
    const mb = bytes / (1024 ** 2);
    if (mb >= 1) return `${mb.toFixed(0)} MB`;
    const kbValue = bytes / 1024;
    return `${kbValue.toFixed(0)} KB`;
}

function formatPercent(value) {
    if (value === null || value === undefined || isNaN(value)) return '--';
    return `${Number(value).toFixed(1)}%`;
}

function formatDecimals(value, decimals = 2) {
    if (value === null || value === undefined || isNaN(value)) return '--';
    return Number(value).toFixed(decimals);
}

const SERVER_METRICS_RANGE_OPTIONS = [3, 6, 12, 24, 72, 168];
const SERVER_METRICS_CACHE_TTL_MS = 120000;

function getServerRangeLabel(hours) {
    if (hours % 24 === 0 && hours >= 24) return `${hours / 24}d`;
    return `${hours}h`;
}

function destroyServerChart() {
    ['serverCpuRam', 'serverCpuBreakdown', 'serverRamBreakdown', 'serverNetwork', 'serverDisk'].forEach((key) => {
        if (!state.charts[key]) return;
        try {
            state.charts[key].destroy();
        } catch (e) {
            // Ignore Chart.js destroy errors on detached canvases.
        }
        state.charts[key] = null;
    });
}

function formatBitsPerSecond(bitsPerSecond) {
    const value = asNumberOrNull(bitsPerSecond);
    if (value === null || value < 0) return '--';
    if (value < 1000) return `${value.toFixed(0)} bps`;
    if (value < 1000000) return `${(value / 1000).toFixed(2)} Kbps`;
    if (value < 1000000000) return `${(value / 1000000).toFixed(2)} Mbps`;
    return `${(value / 1000000000).toFixed(2)} Gbps`;
}

function toMbpsFromBytesPerSecond(bytesPerSecond) {
    const value = asNumberOrNull(bytesPerSecond);
    if (value === null || value < 0) return null;
    return (value * 8) / 1000000;
}

function getLastFiniteValue(values) {
    for (let i = values.length - 1; i >= 0; i -= 1) {
        const value = asNumberOrNull(values[i]);
        if (value !== null) return value;
    }
    return null;
}

function getSeriesStats(values) {
    const cleaned = values.map(asNumberOrNull).filter(v => v !== null);
    if (cleaned.length === 0) return null;
    const current = cleaned[cleaned.length - 1];
    const avg = cleaned.reduce((sum, v) => sum + v, 0) / cleaned.length;
    const max = Math.max(...cleaned);
    return { current, avg, max };
}

function setServerMetricText(elementId, value) {
    const el = document.getElementById(elementId);
    if (el) el.textContent = value;
}

function decodeMaybeBase64(value) {
    if (typeof value !== 'string') return '';
    const raw = value.trim();
    if (!raw) return '';

    if (raw.includes(',') || raw.includes(';')) {
        return raw;
    }

    try {
        const decoded = atob(raw);
        if (decoded && /[,;]/.test(decoded)) return decoded;
        if (decoded && /^[\x09\x0A\x0D\x20-\x7E]+$/.test(decoded)) return decoded;
    } catch (e) {
        // Not base64; fall back to raw payload.
    }
    return raw;
}

function parseAgentNicRows(rawValue) {
    const decoded = decodeMaybeBase64(rawValue);
    if (!decoded) return [];

    return decoded
        .split(';')
        .map(item => item.trim())
        .filter(Boolean)
        .map((item) => {
            const parts = item.split(',');
            const name = (parts[0] || '').trim();
            const inbound = Math.max(0, asNumberOrNull(parts[1]) || 0);
            const outbound = Math.max(0, asNumberOrNull(parts[2]) || 0);
            return {
                name,
                inbound,
                outbound,
                total: inbound + outbound
            };
        })
        .filter(row => row.name);
}

function parseAgentDiskRows(rawValue) {
    const decoded = decodeMaybeBase64(rawValue);
    if (!decoded) return [];

    return decoded
        .split(';')
        .map(item => item.trim())
        .filter(Boolean)
        .map((item) => {
            const parts = item.split(',');
            const mount = (parts[0] || '').trim();
            let fsType = '--';
            let total = null;
            let used = null;
            let available = null;

            if (parts.length >= 5) {
                fsType = (parts[1] || '--').trim() || '--';
                total = asNumberOrNull(parts[2]);
                used = asNumberOrNull(parts[3]);
                available = asNumberOrNull(parts[4]);
            } else if (parts.length >= 4) {
                total = asNumberOrNull(parts[1]);
                used = asNumberOrNull(parts[2]);
                available = asNumberOrNull(parts[3]);
            }

            let usagePercent = null;
            if (total !== null && total > 0 && used !== null) {
                usagePercent = Math.max(0, Math.min(100, (used / total) * 100));
            }

            return {
                mount,
                fsType,
                total,
                used,
                available,
                usagePercent
            };
        })
        .filter(row => row.mount);
}

function formatServerChartLabel(timestamp, hours) {
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) return '--';

    if (hours <= 24) {
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }
    return [
        date.toLocaleDateString([], { month: 'short', day: 'numeric' }),
        date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    ];
}

function downsampleServerHistory(history, maxPoints) {
    if (!Array.isArray(history) || history.length <= maxPoints || maxPoints < 3) {
        return history;
    }

    const sampled = [];
    let lastIndex = -1;
    const step = (history.length - 1) / (maxPoints - 1);

    for (let i = 0; i < maxPoints; i += 1) {
        const index = Math.round(i * step);
        if (index === lastIndex) continue;
        sampled.push(history[index]);
        lastIndex = index;
    }

    if (sampled[0] !== history[0]) sampled.unshift(history[0]);
    if (sampled[sampled.length - 1] !== history[history.length - 1]) sampled.push(history[history.length - 1]);
    return sampled;
}

function createServerDetailChart(chartKey, canvasId, labels, datasets, options = {}) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || typeof Chart === 'undefined') return;

    if (state.charts[chartKey]) {
        try { state.charts[chartKey].destroy(); } catch (e) { }
        state.charts[chartKey] = null;
    }

    const yScale = {
        beginAtZero: true,
        ticks: {
            color: '#96a8bd',
            maxTicksLimit: 6,
            callback: options.yTickFormatter || undefined
        },
        grid: { color: 'rgba(78, 93, 111, 0.35)' }
    };
    if (typeof options.maxY === 'number') {
        yScale.max = options.maxY;
    }

    state.charts[chartKey] = new Chart(canvas, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: {
                decimation: {
                    enabled: true,
                    algorithm: 'lttb',
                    samples: options.decimationSamples || 180
                },
                legend: {
                    position: 'top',
                    labels: {
                        color: '#d7e2f0',
                        boxWidth: 14,
                        usePointStyle: false
                    }
                },
                tooltip: {
                    intersect: false,
                    mode: 'index'
                }
            },
            scales: {
                x: {
                    offset: true,
                    ticks: {
                        color: '#8da0b6',
                        maxRotation: 0,
                        minRotation: 0,
                        autoSkip: true,
                        maxTicksLimit: options.xMaxTicksLimit || 8,
                        padding: options.xTickPadding || 8,
                        font: { size: options.xTickFontSize || 11 }
                    },
                    grid: { color: 'rgba(78, 93, 111, 0.25)' }
                },
                y: yScale
            }
        }
    });
}

function renderServerDetailTables(lastPoint) {
    const nicRows = parseAgentNicRows(lastPoint?.nics)
        .sort((a, b) => b.total - a.total)
        .slice(0, 10);
    const diskRows = parseAgentDiskRows(lastPoint?.disks)
        .sort((a, b) => (b.usagePercent || 0) - (a.usagePercent || 0))
        .slice(0, 10);

    const nicBody = document.getElementById('server-nic-table-body');
    if (nicBody) {
        nicBody.innerHTML = nicRows.length > 0
            ? nicRows.map((row) => {
                const total = Math.max(row.total, 1);
                const inboundWidth = (row.inbound / total) * 100;
                const outboundWidth = (row.outbound / total) * 100;
                return `
                    <tr>
                        <td class="server-detail-name-cell">${escapeHtml(row.name)}</td>
                        <td>
                            <div class="server-nic-usage-bar">
                                <span class="server-nic-usage-in" style="width:${inboundWidth.toFixed(1)}%"></span>
                                <span class="server-nic-usage-out" style="width:${outboundWidth.toFixed(1)}%"></span>
                            </div>
                        </td>
                        <td class="server-detail-values-cell">
                            In ${formatBytesPerSecond(row.inbound)} | Out ${formatBytesPerSecond(row.outbound)}
                        </td>
                    </tr>
                `;
            }).join('')
            : '<tr><td colspan="3" class="server-detail-empty-row">No interface usage data in latest report.</td></tr>';
    }

    const diskBody = document.getElementById('server-disk-table-body');
    if (diskBody) {
        diskBody.innerHTML = diskRows.length > 0
            ? diskRows.map((row) => {
                const usage = row.usagePercent === null ? 0 : row.usagePercent;
                const toneClass = usage >= 90
                    ? 'server-disk-usage-fill-danger'
                    : usage >= 75
                        ? 'server-disk-usage-fill-warn'
                        : 'server-disk-usage-fill-ok';
                return `
                    <tr>
                        <td class="server-detail-name-cell">${escapeHtml(row.mount)}</td>
                        <td>${escapeHtml(row.fsType)}</td>
                        <td>${row.total === null ? '--' : formatBytes(row.total)}</td>
                        <td>${row.used === null ? '--' : formatBytes(row.used)}</td>
                        <td>${row.available === null ? '--' : formatBytes(row.available)}</td>
                        <td>
                            <div class="server-disk-usage-cell">
                                <div class="server-disk-usage-track">
                                    <span class="server-disk-usage-fill ${toneClass}" style="width:${usage.toFixed(1)}%"></span>
                                </div>
                                <span>${row.usagePercent === null ? '--' : `${row.usagePercent.toFixed(2)}%`}</span>
                            </div>
                        </td>
                    </tr>
                `;
            }).join('')
            : '<tr><td colspan="6" class="server-detail-empty-row">No disk usage data in latest report.</td></tr>';
    }
}

function renderServerDetailStats(history) {
    const lastPoint = history[history.length - 1] || {};
    const cpuSeries = history.map(point => asNumberOrNull(point.cpu_percent));
    const ramSeries = history.map(point => asNumberOrNull(point.ram_percent));
    const diskSeries = history.map(point => asNumberOrNull(point.disk_percent));
    const networkInSeries = history.map(point => Math.max(0, asNumberOrNull(point.network_in) || 0));
    const networkOutSeries = history.map(point => Math.max(0, asNumberOrNull(point.network_out) || 0));

    setServerMetricText('server-metric-cpu', formatPercent(lastPoint.cpu_percent));
    setServerMetricText('server-metric-ram', formatPercent(lastPoint.ram_percent));
    setServerMetricText('server-metric-disk', formatPercent(lastPoint.disk_percent));
    setServerMetricText(
        'server-metric-network',
        `In ${formatBytesPerSecond(lastPoint.network_in || 0)} | Out ${formatBytesPerSecond(lastPoint.network_out || 0)}`
    );
    setServerMetricText(
        'server-metric-load',
        `${formatDecimals(lastPoint.load_1)} / ${formatDecimals(lastPoint.load_5)} / ${formatDecimals(lastPoint.load_15)}`
    );
    setServerMetricText(
        'server-metric-iowait-steal',
        `${formatPercent(lastPoint.cpu_io_wait)} / ${formatPercent(lastPoint.cpu_steal)}`
    );

    const cpuStats = getSeriesStats(cpuSeries);
    setServerMetricText('server-cpu-current', cpuStats ? `${cpuStats.current.toFixed(2)}%` : '--');
    setServerMetricText('server-cpu-avg', cpuStats ? `${cpuStats.avg.toFixed(2)}%` : '--');
    setServerMetricText('server-cpu-max', cpuStats ? `${cpuStats.max.toFixed(2)}%` : '--');

    const ramStats = getSeriesStats(ramSeries);
    setServerMetricText('server-ram-current', ramStats ? `${ramStats.current.toFixed(2)}%` : '--');
    setServerMetricText('server-ram-avg', ramStats ? `${ramStats.avg.toFixed(2)}%` : '--');
    setServerMetricText('server-ram-max', ramStats ? `${ramStats.max.toFixed(2)}%` : '--');

    const diskStats = getSeriesStats(diskSeries);
    setServerMetricText('server-disk-current', diskStats ? `${diskStats.current.toFixed(2)}%` : '--');
    setServerMetricText('server-disk-avg', diskStats ? `${diskStats.avg.toFixed(2)}%` : '--');
    setServerMetricText('server-disk-max', diskStats ? `${diskStats.max.toFixed(2)}%` : '--');

    const currentIn = getLastFiniteValue(networkInSeries) || 0;
    const currentOut = getLastFiniteValue(networkOutSeries) || 0;
    const avgIn = networkInSeries.length > 0
        ? networkInSeries.reduce((sum, value) => sum + value, 0) / networkInSeries.length
        : 0;
    const avgOut = networkOutSeries.length > 0
        ? networkOutSeries.reduce((sum, value) => sum + value, 0) / networkOutSeries.length
        : 0;
    const maxIn = networkInSeries.length > 0 ? Math.max(...networkInSeries) : 0;
    const maxOut = networkOutSeries.length > 0 ? Math.max(...networkOutSeries) : 0;

    const timestamps = history.map(point => toTimestampMs(point.timestamp));
    let totalInBytes = 0;
    let totalOutBytes = 0;
    for (let i = 1; i < history.length; i += 1) {
        const prevTs = timestamps[i - 1];
        const currTs = timestamps[i];
        if (!Number.isFinite(prevTs) || !Number.isFinite(currTs)) continue;

        const dtSec = (currTs - prevTs) / 1000;
        if (!Number.isFinite(dtSec) || dtSec <= 0 || dtSec > 21600) continue;

        totalInBytes += ((networkInSeries[i - 1] + networkInSeries[i]) / 2) * dtSec;
        totalOutBytes += ((networkOutSeries[i - 1] + networkOutSeries[i]) / 2) * dtSec;
    }

    setServerMetricText('server-net-current', formatBitsPerSecond((currentIn + currentOut) * 8));
    setServerMetricText('server-net-avg', formatBitsPerSecond((avgIn + avgOut) * 8));
    setServerMetricText('server-net-max', formatBitsPerSecond((maxIn + maxOut) * 8));
    setServerMetricText('server-net-total', formatBytes(totalInBytes + totalOutBytes));
}

function renderServerDetailCharts(history, hours) {
    const labels = history.map(point => formatServerChartLabel(point.timestamp, hours));
    const sharedChartOptions = {
        xMaxTicksLimit: hours <= 24 ? 10 : (hours <= 72 ? 6 : 5),
        xTickFontSize: hours <= 24 ? 11 : 10,
        xTickPadding: hours <= 24 ? 8 : 10,
        decimationSamples: hours <= 24 ? 220 : 150
    };

    createServerDetailChart('serverCpuRam', 'server-chart-cpu', labels, [
        {
            label: 'CPU %',
            data: history.map(point => asNumberOrNull(point.cpu_percent)),
            borderColor: '#47c3ba',
            backgroundColor: 'rgba(71, 195, 186, 0.12)',
            fill: true,
            pointRadius: 0,
            tension: 0.25
        },
        {
            label: 'IOWait %',
            data: history.map(point => asNumberOrNull(point.cpu_io_wait)),
            borderColor: '#f15b5b',
            backgroundColor: 'rgba(241, 91, 91, 0.05)',
            fill: false,
            pointRadius: 0,
            tension: 0.25
        },
        {
            label: 'Steal %',
            data: history.map(point => asNumberOrNull(point.cpu_steal)),
            borderColor: '#e3d450',
            backgroundColor: 'rgba(227, 212, 80, 0.05)',
            fill: false,
            pointRadius: 0,
            tension: 0.25
        }
    ], {
        ...sharedChartOptions,
        maxY: 100,
        yTickFormatter: (value) => `${value}%`
    });

    createServerDetailChart('serverRamBreakdown', 'server-chart-ram', labels, [
        {
            label: 'RAM %',
            data: history.map(point => asNumberOrNull(point.ram_percent)),
            borderColor: '#5cb8df',
            backgroundColor: 'rgba(92, 184, 223, 0.12)',
            fill: true,
            pointRadius: 0,
            tension: 0.25
        },
        {
            label: 'Swap %',
            data: history.map(point => asNumberOrNull(point.ram_swap_percent)),
            borderColor: '#c26cae',
            backgroundColor: 'rgba(194, 108, 174, 0.05)',
            fill: false,
            pointRadius: 0,
            tension: 0.25
        },
        {
            label: 'Cache %',
            data: history.map(point => asNumberOrNull(point.ram_cache_percent)),
            borderColor: '#8ccf52',
            backgroundColor: 'rgba(140, 207, 82, 0.05)',
            fill: false,
            pointRadius: 0,
            tension: 0.25
        }
    ], {
        ...sharedChartOptions,
        maxY: 100,
        yTickFormatter: (value) => `${value}%`
    });

    createServerDetailChart('serverNetwork', 'server-chart-network', labels, [
        {
            label: 'In Mbps',
            data: history.map(point => toMbpsFromBytesPerSecond(point.network_in)),
            borderColor: '#80c56b',
            backgroundColor: 'rgba(128, 197, 107, 0.14)',
            fill: true,
            pointRadius: 0,
            tension: 0.2
        },
        {
            label: 'Out Mbps',
            data: history.map(point => toMbpsFromBytesPerSecond(point.network_out)),
            borderColor: '#d2a365',
            backgroundColor: 'rgba(210, 163, 101, 0.12)',
            fill: true,
            pointRadius: 0,
            tension: 0.2
        }
    ], {
        ...sharedChartOptions,
        yTickFormatter: (value) => `${value} Mbps`
    });

    createServerDetailChart('serverDisk', 'server-chart-disk', labels, [
        {
            label: 'Disk %',
            data: history.map(point => asNumberOrNull(point.disk_percent)),
            borderColor: '#63c9ee',
            backgroundColor: 'rgba(99, 201, 238, 0.12)',
            fill: true,
            pointRadius: 0,
            tension: 0.2
        }
    ], {
        ...sharedChartOptions,
        maxY: 100,
        yTickFormatter: (value) => `${value}%`
    });
}

async function loadServerMetricsPanel(serverId, hours = 24) {
    const view = state.serverDetailsView;
    if (!view || String(view.serverId) !== String(serverId)) return;

    const normalizedHours = Math.max(1, Math.min(720, Number(hours) || 24));
    view.hours = normalizedHours;

    document.querySelectorAll('.server-range-btn').forEach((btn) => {
        const buttonHours = Number(btn.getAttribute('data-hours'));
        btn.classList.toggle('active', buttonHours === normalizedHours);
    });
    setServerMetricText('server-metric-range', `Range: ${getServerRangeLabel(normalizedHours)}`);
    setServerMetricText('server-metric-samples', 'Loading…');

    const emptyEl = document.getElementById('server-metric-empty');
    if (emptyEl) emptyEl.style.display = 'none';

    const cacheKey = `${serverId}:${normalizedHours}`;
    const cached = state.serverHistoryCache.get(cacheKey);
    const nowMs = Date.now();

    const applyHistory = (history, fromCache = false) => {
        if (!Array.isArray(history)) {
            throw new Error('Invalid server metrics payload');
        }

        if (!state.serverDetailsView || String(state.serverDetailsView.serverId) !== String(serverId)) {
            return;
        }

        const sortedHistory = [...history].sort((a, b) => {
            const aMs = toTimestampMs(a.timestamp) || 0;
            const bMs = toTimestampMs(b.timestamp) || 0;
            return aMs - bMs;
        });

        const maxRenderedPoints = normalizedHours <= 24
            ? 480
            : normalizedHours <= 72
                ? 320
                : normalizedHours <= 168
                    ? 260
                    : 180;
        const chartHistory = downsampleServerHistory(sortedHistory, maxRenderedPoints);
        const sampleText = sortedHistory.length === chartHistory.length
            ? `${sortedHistory.length} samples`
            : `${sortedHistory.length} samples (${chartHistory.length} rendered)`;
        setServerMetricText('server-metric-samples', fromCache ? `${sampleText} • cached` : sampleText);

        if (!sortedHistory.length) {
            destroyServerChart();
            renderServerDetailTables(view.latestDetailedPoint || null);
            ['server-metric-cpu', 'server-metric-ram', 'server-metric-disk', 'server-metric-network', 'server-metric-load', 'server-metric-iowait-steal'].forEach((id) => {
                setServerMetricText(id, '--');
            });
            ['server-cpu-current', 'server-cpu-avg', 'server-cpu-max', 'server-ram-current', 'server-ram-avg', 'server-ram-max', 'server-net-current', 'server-net-avg', 'server-net-max', 'server-net-total', 'server-disk-current', 'server-disk-avg', 'server-disk-max'].forEach((id) => {
                setServerMetricText(id, '--');
            });

            if (emptyEl) {
                emptyEl.textContent = 'No metrics received yet. Install the agent and wait for the first report.';
                emptyEl.style.display = 'block';
            }
            return;
        }

        const detailedPoint = [...sortedHistory].reverse().find(point => point && (point.nics || point.disks));
        if (detailedPoint) {
            view.latestDetailedPoint = detailedPoint;
        }

        renderServerDetailStats(sortedHistory);
        renderServerDetailCharts(chartHistory, normalizedHours);
        renderServerDetailTables(view.latestDetailedPoint || sortedHistory[sortedHistory.length - 1]);
    };

    if (cached && nowMs - cached.fetchedAt <= SERVER_METRICS_CACHE_TTL_MS) {
        try {
            applyHistory(cached.history, true);
            return;
        } catch (e) {
            // If cached payload is invalid, fall back to network.
            state.serverHistoryCache.delete(cacheKey);
        }
    }

    try {
        const response = await apiRequest(`/api/heartbeat-monitors/server-agent/${serverId}/history?hours=${normalizedHours}`);
        if (!response.ok) {
            const message = await readApiError(response, 'Failed to load server metrics');
            throw new Error(message);
        }

        const history = await response.json();
        state.serverHistoryCache.set(cacheKey, { fetchedAt: nowMs, history });
        applyHistory(history, false);
    } catch (error) {
        setServerMetricText('server-metric-samples', 'Failed to load');
        if (emptyEl) {
            emptyEl.textContent = 'Failed to load server metric history.';
            emptyEl.style.display = 'block';
        }
        showToast('Failed to load server metrics', 'error');
    }
}

async function showServerMetrics(serverId) {
    const server = state.monitors.server.find(s => String(s.id) === String(serverId));
    if (!server) {
        showToast('Server monitor not found', 'error');
        return;
    }

    const title = server.name || 'Server';
    const hostname = server.hostname || '--';
    const osRaw = server.os || '--';
    const os = server.os ? simplifyOsName(server.os) : '--';
    const kernel = server.kernel || '--';
    const cpuModel = server.cpu_model || '--';
    const ramSize = formatBytesFromKb(server.ram_size);
    const lastReport = server.last_report_at ? new Date(server.last_report_at).toLocaleString() : 'Never';
    const cpuTopology = (Number.isFinite(server.cpu_cores) || Number.isFinite(server.cpu_threads))
        ? `${Number.isFinite(server.cpu_cores) ? server.cpu_cores : '--'} / ${Number.isFinite(server.cpu_threads) ? server.cpu_threads : '--'}`
        : '--';

    const statusRaw = String(server.status || 'unknown').toLowerCase();
    const statusClass = statusRaw === 'up' ? 'status-up' : (statusRaw === 'down' ? 'status-down' : 'status-unknown');
    const statusText = statusRaw === 'up' ? 'Active' : (statusRaw === 'down' ? 'Down' : 'Unknown');

    destroyServerChart();

    showModal(`
        <div class="modal-header server-details-header">
            <div class="server-details-title-wrap">
                <div class="server-details-title-row">
                    <h2 class="modal-title">${escapeHtml(title)}</h2>
                    <span class="server-details-status ${statusClass}">${statusText}</span>
                </div>
                <div class="server-details-meta">Hostname: ${escapeHtml(hostname)} • Last report: ${escapeHtml(lastReport)}</div>
            </div>
            <div class="server-details-toolbar">
                <div class="server-details-toolbar-meta">
                    <span class="text-muted" id="server-metric-range">Range: 24h</span>
                    <span class="text-muted" id="server-metric-samples">Loading…</span>
                </div>
                <div class="server-range-group">
                    ${SERVER_METRICS_RANGE_OPTIONS.map(hours => `
                        <button class="server-range-btn ${hours === 24 ? 'active' : ''}" data-hours="${hours}" onclick="loadServerMetricsPanel('${String(serverId)}', ${hours})">
                            ${getServerRangeLabel(hours)}
                        </button>
                    `).join('')}
                </div>
            </div>
        </div>
        <div class="modal-body server-details-body">
            <div class="metrics-grid server-details-info-grid">
                <div class="metric-card">
                    <div class="metric-label">OS</div>
                    <div class="metric-value metric-value-truncate" title="${escapeHtml(osRaw)}">${escapeHtml(os)}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Kernel</div>
                    <div class="metric-value metric-value-truncate" title="${escapeHtml(kernel)}">${escapeHtml(kernel)}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">CPU</div>
                    <div class="metric-value metric-value-truncate" title="${escapeHtml(cpuModel)}">${escapeHtml(cpuModel)}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">RAM</div>
                    <div class="metric-value">${escapeHtml(ramSize)}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">SID</div>
                    <div class="metric-value metric-value-mono metric-value-truncate" title="${escapeHtml(server.sid || '--')}">${escapeHtml(server.sid || '--')}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Cores / Threads</div>
                    <div class="metric-value">${escapeHtml(cpuTopology)}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">CPU Usage</div>
                    <div class="metric-value" id="server-metric-cpu">--</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">RAM Usage</div>
                    <div class="metric-value" id="server-metric-ram">--</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Disk Usage</div>
                    <div class="metric-value" id="server-metric-disk">--</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Network</div>
                    <div class="metric-value metric-value-truncate" id="server-metric-network">--</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Load (1/5/15)</div>
                    <div class="metric-value" id="server-metric-load">--</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">IOWait / Steal</div>
                    <div class="metric-value" id="server-metric-iowait-steal">--</div>
                </div>
            </div>

            <div class="server-detail-chart-grid">
                <div class="server-detail-chart-card">
                    <div class="server-detail-chart-header">
                        <h3>CPU Usage</h3>
                    </div>
                    <div class="server-detail-chart-canvas-wrap">
                        <canvas id="server-chart-cpu"></canvas>
                    </div>
                    <div class="server-detail-chart-stats">
                        <div><span>Current</span><strong id="server-cpu-current">--</strong></div>
                        <div><span>Average</span><strong id="server-cpu-avg">--</strong></div>
                        <div><span>Max</span><strong id="server-cpu-max">--</strong></div>
                    </div>
                </div>

                <div class="server-detail-chart-card">
                    <div class="server-detail-chart-header">
                        <h3>RAM Usage</h3>
                    </div>
                    <div class="server-detail-chart-canvas-wrap">
                        <canvas id="server-chart-ram"></canvas>
                    </div>
                    <div class="server-detail-chart-stats">
                        <div><span>Current</span><strong id="server-ram-current">--</strong></div>
                        <div><span>Average</span><strong id="server-ram-avg">--</strong></div>
                        <div><span>Max</span><strong id="server-ram-max">--</strong></div>
                    </div>
                </div>

                <div class="server-detail-chart-card">
                    <div class="server-detail-chart-header">
                        <h3>Network Usage</h3>
                    </div>
                    <div class="server-detail-chart-canvas-wrap">
                        <canvas id="server-chart-network"></canvas>
                    </div>
                    <div class="server-detail-chart-stats server-detail-chart-stats-wide">
                        <div><span>Current</span><strong id="server-net-current">--</strong></div>
                        <div><span>Average</span><strong id="server-net-avg">--</strong></div>
                        <div><span>Peak</span><strong id="server-net-max">--</strong></div>
                        <div><span>Total</span><strong id="server-net-total">--</strong></div>
                    </div>
                </div>

                <div class="server-detail-chart-card">
                    <div class="server-detail-chart-header">
                        <h3>Disk Usage</h3>
                    </div>
                    <div class="server-detail-chart-canvas-wrap">
                        <canvas id="server-chart-disk"></canvas>
                    </div>
                    <div class="server-detail-chart-stats">
                        <div><span>Current</span><strong id="server-disk-current">--</strong></div>
                        <div><span>Average</span><strong id="server-disk-avg">--</strong></div>
                        <div><span>Max</span><strong id="server-disk-max">--</strong></div>
                    </div>
                </div>
            </div>

            <div class="server-detail-table-grid">
                <div class="server-detail-table-card">
                    <div class="server-detail-table-header">
                        <h3>Network Interfaces</h3>
                    </div>
                    <div class="server-detail-table-wrap">
                        <table class="server-detail-table">
                            <thead>
                                <tr>
                                    <th>NIC</th>
                                    <th>Usage (In/Out)</th>
                                    <th>Rates</th>
                                </tr>
                            </thead>
                            <tbody id="server-nic-table-body">
                                <tr><td colspan="3" class="server-detail-empty-row">Loading interface usage...</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>

                <div class="server-detail-table-card">
                    <div class="server-detail-table-header">
                        <h3>Disk Mounts</h3>
                    </div>
                    <div class="server-detail-table-wrap">
                        <table class="server-detail-table">
                            <thead>
                                <tr>
                                    <th>Mount</th>
                                    <th>FS</th>
                                    <th>Total</th>
                                    <th>Used</th>
                                    <th>Avail</th>
                                    <th>Usage</th>
                                </tr>
                            </thead>
                            <tbody id="server-disk-table-body">
                                <tr><td colspan="6" class="server-detail-empty-row">Loading disk usage...</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div class="server-details-empty" id="server-metric-empty" style="display:none;"></div>
        </div>
    `);

    const modalContainer = document.getElementById('modal-container');
    if (modalContainer) {
        modalContainer.classList.add('modal-server-details');
    }

    state.serverDetailsView = { serverId: String(serverId), hours: 24 };
    await loadServerMetricsPanel(serverId, 24);
}

function showModal(content) {
    const overlay = document.getElementById('modal-overlay');
    const contentDiv = document.getElementById('modal-content');
    const modalContainer = document.getElementById('modal-container');
    state.serverDetailsView = null;
    destroyServerChart();
    if (modalContainer) {
        modalContainer.classList.remove('modal-agent-command');
        modalContainer.classList.remove('modal-server-details');
    }
    contentDiv.innerHTML = content;
    overlay.classList.add('active');
    overlay.classList.add('show');
    overlay.style.opacity = '1';
    overlay.style.visibility = 'visible';
    overlay.style.pointerEvents = 'all';
    if (modalContainer) {
        modalContainer.style.display = 'block';
        modalContainer.style.transform = 'scale(1)';
    }
}

function hideModal() {
    const overlay = document.getElementById('modal-overlay');
    overlay.classList.remove('active');
    overlay.classList.remove('show');
    overlay.style.opacity = '';
    overlay.style.visibility = '';
    overlay.style.pointerEvents = '';
    const modalContainer = document.getElementById('modal-container');
    if (modalContainer) {
        modalContainer.style.transform = '';
        modalContainer.classList.remove('modal-agent-command');
        modalContainer.classList.remove('modal-server-details');
    }
    state.serverDetailsView = null;
    destroyServerChart();
}

function showToast(message, type = 'info', duration = 3000) {
    const existingToasts = document.querySelectorAll('.toast-notification');
    if (existingToasts.length >= 3) {
        existingToasts[0].remove();
    }

    const toast = document.createElement('div');
    toast.className = `toast-notification toast-${type}`;

    let icon = 'info-circle';
    if (type === 'success') icon = 'check-circle';
    if (type === 'error') icon = 'exclamation-circle';
    if (type === 'warning') icon = 'exclamation-triangle';

    toast.innerHTML = `
        <i class="fas fa-${icon} toast-icon"></i>
        <span class="toast-message">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">
            <i class="fas fa-times"></i>
        </button>
    `;

    document.body.appendChild(toast);

    // Animate in
    setTimeout(() => toast.classList.add('toast-show'), 10);

    // Auto remove after duration
    setTimeout(() => {
        toast.classList.remove('toast-show');
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

function confirmDialog(options) {
    return new Promise((resolve) => {
        const {
            title = 'Confirm Action',
            message = 'Are you sure you want to proceed?',
            confirmText = 'Confirm',
            cancelText = 'Cancel',
            type = 'warning'
        } = options;

        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay active';
        overlay.style.zIndex = '2000';

        const modal = document.createElement('div');
        modal.className = 'modal modal-sm';
        modal.style.maxWidth = '450px';
        modal.style.transform = 'scale(1)';

        let icon = 'exclamation-triangle';
        let iconColor = 'var(--status-warn)';

        if (type === 'danger') {
            icon = 'exclamation-circle';
            iconColor = 'var(--status-down)';
        } else if (type === 'info') {
            icon = 'info-circle';
            iconColor = 'var(--primary)';
        }

        modal.innerHTML = `
            <div class="modal-header">
                <h3 class="modal-title">${title}</h3>
            </div>
            <div class="modal-body" style="text-align: center; padding: 2rem 1.5rem;">
                <div class="confirm-dialog-icon" style="color: ${iconColor}; font-size: 3rem; margin-bottom: 1rem;">
                    <i class="fas fa-${icon}"></i>
                </div>
                <p class="confirm-dialog-message">${message}</p>
            </div>
            <div class="modal-footer" style="justify-content: center; gap: 1rem;">
                <button class="btn btn-secondary" id="confirm-cancel">${cancelText}</button>
                <button class="btn btn-${type === 'danger' ? 'danger' : 'primary'}" id="confirm-confirm">${confirmText}</button>
            </div>
        `;

        overlay.appendChild(modal);
        document.body.appendChild(overlay);

        const handleResult = (result) => {
            overlay.remove();
            resolve(result);
        };

        document.getElementById('confirm-cancel').addEventListener('click', () => handleResult(false));
        document.getElementById('confirm-confirm').addEventListener('click', () => handleResult(true));

        // Close on backdrop click
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) handleResult(false);
        });

        // Close on Escape key
        const handleEscape = (e) => {
            if (e.key === 'Escape') {
                document.removeEventListener('keydown', handleEscape);
                handleResult(false);
            }
        };
        document.addEventListener('keydown', handleEscape);
    });
}

async function showMonitorDetails(id) {
    let monitor = state.monitors.uptime.find(m => String(m.id) === String(id));
    if (!monitor) monitor = state.monitors.server.find(m => String(m.id) === String(id));
    if (!monitor) monitor = state.monitors.heartbeat.find(m => String(m.id) === String(id));

    if (!monitor) {
        showToast('Monitor not found', 'error');
        return;
    }

    const sourceType = monitor._source || MONITOR_SOURCE.WEBSITE;

    showModal(`
        <div class="modal-header">
            <h2 class="modal-title">${escapeHtml(monitor.name || 'Monitor')}</h2>
        </div>
        <div class="modal-body">
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                <div class="form-group">
                    <label>Type</label>
                    <div>${getSourceTypeLabel(sourceType, monitor)}</div>
                </div>
                <div class="form-group">
                    <label>Status</label>
                    <div>${monitor.enabled ? 'Enabled' : 'Disabled'}</div>
                </div>
                ${monitor.target ? `
                    <div class="form-group">
                        <label>Target</label>
                        <div>${monitor.target}</div>
                    </div>
                ` : ''}
                ${monitor.created_at ? `
                    <div class="form-group">
                        <label>Created</label>
                        <div>${new Date(monitor.created_at).toLocaleString()}</div>
                    </div>
                ` : ''}
            </div>
        </div>
        <div class="modal-footer">
            <button class="btn btn-secondary" onclick="hideModal()">Close</button>
            <button class="btn btn-primary" onclick="hideModal(); showAddMonitorModal('${sourceType}', '${monitor.id}')">Edit Monitor</button>
            <button class="btn btn-danger" onclick="showDeleteMonitorDialog('${sourceType}', '${monitor.id}')">
                <i class="fas fa-trash-alt"></i> Delete Monitor
            </button>
        </div>
    `);
}

function findMonitorBySourceAndId(sourceType, id) {
    const groupKey = getMonitorGroupKey(sourceType);
    const group = state.monitors[groupKey] || [];
    let monitor = group.find(m => String(m.id) === String(id));
    if (monitor) return monitor;

    monitor = state.monitors.uptime.find(m => String(m.id) === String(id));
    if (monitor) return monitor;
    monitor = state.monitors.server.find(m => String(m.id) === String(id));
    if (monitor) return monitor;
    return state.monitors.heartbeat.find(m => String(m.id) === String(id)) || null;
}

function formatMonitorDateTime(value) {
    if (!value) return '--';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
}

function getMonitorStatusText(monitor) {
    if (!monitor) return 'Unknown';
    if (monitor.maintenance_mode) return 'Maintenance';
    if (monitor.enabled === false) return 'Paused';
    const status = String(monitor.status || '').toLowerCase();
    if (status === 'up') return 'Up';
    if (status === 'down') return 'Down';
    return 'Unknown';
}

function getMonitorDeleteDetails(sourceType, monitor) {
    const details = [
        ['Name', monitor?.name || '--'],
        ['Type', getSourceTypeLabel(sourceType, monitor)],
        ['ID', monitor?.id || '--'],
        ['Status', getMonitorStatusText(monitor)],
        ['Category', monitor?.category || '--'],
        ['Created', formatMonitorDateTime(monitor?.created_at)]
    ];

    if (sourceType === MONITOR_SOURCE.WEBSITE) {
        details.push(['Target', monitor?.target || '--']);
        details.push(['Last Check', formatMonitorDateTime(monitor?.last_check_at || monitor?.last_up_at)]);
    } else if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
        details.push(['SID', monitor?.sid || '--']);
        details.push(['Hostname', monitor?.hostname || '--']);
        details.push(['Last Report', formatMonitorDateTime(monitor?.last_report_at)]);
    } else {
        details.push(['SID', monitor?.sid || '--']);
        details.push(['Ping Policy', '1 minute']);
        details.push(['Last Ping', formatMonitorDateTime(monitor?.last_ping_at)]);
    }

    return details;
}

function showDeleteMonitorDialog(sourceType, id, returnMode = '', returnPlatform = '') {
    const monitor = findMonitorBySourceAndId(sourceType, id);
    if (!monitor) {
        showToast('Monitor not found', 'error');
        return;
    }

    const details = getMonitorDeleteDetails(sourceType, monitor);
    const detailsHtml = details.map(([label, value]) => `
        <div class="form-group" style="margin:0;">
            <label style="font-size:0.78rem; color:var(--text-muted); margin-bottom:4px; display:block;">${escapeHtml(label)}</label>
            <div style="font-size:0.92rem; color:var(--text-primary); word-break:break-word;">${escapeHtml(String(value ?? '--'))}</div>
        </div>
    `).join('');

    const backAction = sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT
        ? `showServerAgentCommandCenter('${id}', '${returnMode || 'install'}', '${returnPlatform || 'linux'}')`
        : `openMonitorTools('${sourceType}', '${id}')`;

    showModal(`
        <div class="modal-header">
            <h2 class="modal-title"><i class="fas fa-trash-alt" style="color:#e74c3c;"></i> Delete Monitor</h2>
        </div>
        <div class="modal-body">
            <p style="margin:0 0 12px; color:var(--text-muted);">
                This action permanently deletes this monitor and associated records. Please review details before confirming.
            </p>
            <div style="padding:12px; border:1px solid var(--border-color); border-radius:8px; background:rgba(0,0,0,0.14);">
                <div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:12px;">
                    ${detailsHtml}
                </div>
            </div>
        </div>
        <div class="modal-footer">
            <button class="btn btn-secondary" onclick="${backAction}">Back</button>
            <button class="btn btn-danger" onclick="deleteMonitorBySourceType('${sourceType}', '${id}')">
                <i class="fas fa-trash-alt"></i> Delete Monitor
            </button>
        </div>
    `);
}

async function deleteMonitorBySourceType(sourceType, id) {
    const monitor = findMonitorBySourceAndId(sourceType, id);
    const endpointBase = getApiBaseBySourceType(sourceType);
    const endpoint = `${endpointBase}/${id}`;

    try {
        const response = await apiRequest(endpoint, { method: 'DELETE' });
        if (!response.ok) {
            const message = await readApiError(response, 'Failed to delete monitor');
            showToast(message, 'error');
            return;
        }

        hideModal();
        showToast(`Monitor "${monitor?.name || id}" deleted successfully`, 'success');
        await loadAllData();
    } catch (error) {
        console.error('Failed to delete monitor:', error);
        showToast('Failed to delete monitor', 'error');
    }
}

async function openMonitorTools(sourceType, id) {
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) {
        await showHeartbeatPingUrl(id, 'url');
        return;
    }
    if (sourceType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
        await showServerAgentCommandCenter(id, 'install', 'linux');
        return;
    }
    await showMonitorDetails(id);
}

/**
 * Show Add Monitor Modal. Pass editId to open in edit mode.
 */
function showAddMonitorModalFromCurrentFilter() {
    const activeType = document.querySelector('.monitor-type-tab.active')?.dataset.type || 'website';
    const allowedTypes = new Set(['website', 'heartbeat-cronjob', 'heartbeat-server-agent']);
    const selectedType = allowedTypes.has(activeType) ? activeType : 'website';
    showAddMonitorModal(selectedType);
}

function showAddMonitorModal(type = 'website', editId = null) {
    state.editMonitorId = editId || null;
    state.editMonitorType = type || null;
    const typeMap = {
        'website': 'website',
        'heartbeat-cronjob': 'heartbeat',
        'heartbeat-server-agent': 'heartbeat',
    };
    const heartbeatTypeMap = {
        'heartbeat': 'cronjob',
        'heartbeat-cronjob': 'cronjob',
        'heartbeat-server-agent': 'server_agent',
    };
    const initialType = typeMap[type] || 'website';

    const overlay = document.getElementById('modal-overlay');
    const modalContainer = document.getElementById('modal-container');
    const modalContent = document.getElementById('modal-content');

    modalContent.innerHTML = getAddMonitorModalHTML(initialType, !!editId);

    overlay.classList.add('active');
    overlay.classList.add('show');
    overlay.style.opacity = '1';
    overlay.style.visibility = 'visible';
    overlay.style.pointerEvents = 'all';
    modalContainer.style.display = 'block';
    modalContainer.style.transform = 'scale(1)';

    onMonitorTypeChange();

    if (!editId && initialType === 'heartbeat') {
        const heartbeatTypeEl = document.getElementById('mon-heartbeat-type');
        if (heartbeatTypeEl) {
            heartbeatTypeEl.value = heartbeatTypeMap[type] || 'cronjob';
        }
    }

    if (editId) {
        const monitor = state.monitors.uptime.find(m => String(m.id) === String(editId))
            || state.monitors.server.find(m => String(m.id) === String(editId))
            || state.monitors.heartbeat.find(m => String(m.id) === String(editId));
        if (monitor) prefillEditMonitorForm(monitor, type);
    }
}

function getAddMonitorModalHTML(initialType, isEdit = false) {
    const title = isEdit ? 'Edit Monitor' : 'Add Monitor';
    const titleIcon = isEdit ? 'fa-pencil-alt' : 'fa-plus-circle';
    const submitLabel = isEdit ? 'Update Monitor' : 'Add Monitor';
    const submitIcon = isEdit ? 'fa-save' : 'fa-plus';
    return `
        <div class="add-monitor-modal" style="background:#232930; color:#fff; padding:0; border-radius:8px; max-height:80vh; overflow-y:auto;">
            <div style="padding:20px 24px 12px; border-bottom:1px solid #2d343d; display:flex; justify-content:space-between; align-items:center;">
                <h2 style="margin:0; font-size:1.15rem; font-weight:600; color:#fff;">
                    <i class="fas ${titleIcon}" style="color:#44b6ae; margin-right:8px;"></i>${title}
                </h2>
            </div>
            <div style="padding:20px 24px;">
                <form id="add-monitor-form" onsubmit="return false;">
                    <!-- Monitor Type -->
                    <div class="form-group" style="margin-bottom:16px;">
                        <label style="display:block; margin-bottom:6px; font-size:0.85rem; color:#c0c6ce; font-weight:500;">Monitor Type</label>
                        <select id="monitor-type-select" onchange="onMonitorTypeChange()" style="width:100%; padding:9px 12px; background:#1a1f26; border:1px solid #3a424d; border-radius:4px; color:#fff; font-size:0.9rem;">
                            <option value="website" ${initialType === 'website' ? 'selected' : ''}>Website Monitor</option>
                            <option value="heartbeat" ${initialType === 'heartbeat' ? 'selected' : ''}>Heartbeat Monitor</option>
                        </select>
                    </div>

                    <!-- Dynamic fields container -->
                    <div id="monitor-dynamic-fields"></div>

                    <!-- Advanced Settings Toggle -->
                    <div class="form-group" id="advanced-settings-toggle" style="margin-bottom:12px; margin-top:8px;">
                        <a href="#" onclick="toggleAdvancedSettings(); return false;" style="color:#44b6ae; text-decoration:none; font-size:0.85rem;">
                            <i class="fas fa-cog"></i> show advanced settings <i class="fas fa-chevron-down" id="advanced-toggle-icon"></i>
                        </a>
                    </div>

                    <!-- Advanced Settings -->
                    <div id="advanced-settings" style="display:none;">
                        <div id="monitor-advanced-fields"></div>
                    </div>

                    <!-- Submit -->
                    <div style="padding-top:16px; border-top:1px solid #2d343d; margin-top:16px; display:flex; justify-content:flex-end; gap:10px;">
                        <button type="button" class="btn btn-secondary" onclick="hideModal()" style="padding:9px 18px;">Cancel</button>
                        <button type="button" class="btn btn-primary" id="add-monitor-submit-btn" onclick="submitAddMonitor()" style="padding:9px 24px; background:#44b6ae; border-color:#44b6ae;">
                            <i class="fas ${submitIcon}"></i> ${submitLabel}
                        </button>
                    </div>
                </form>
            </div>
        </div>
    `;
}

function prefillEditMonitorForm(monitor, type) {
    const monitorTypeSelect = document.getElementById('monitor-type-select');
    if (monitorTypeSelect) {
        if (monitor.heartbeat_type === 'server_agent' || monitor.heartbeat_type === 'cronjob') {
            monitorTypeSelect.value = 'heartbeat';
        } else {
            monitorTypeSelect.value = 'website';
        }
        onMonitorTypeChange();
    }

    const nameEl = document.getElementById('mon-name');
    const targetEl = document.getElementById('mon-target');
    const categoryEl = document.getElementById('mon-category');
    if (nameEl) nameEl.value = monitor.name || '';
    if (targetEl) targetEl.value = monitor.target || '';
    if (categoryEl) categoryEl.value = monitor.category || '';

    const heartbeatTypeEl = document.getElementById('mon-heartbeat-type');
    if (heartbeatTypeEl) {
        heartbeatTypeEl.value = monitor.heartbeat_type === 'server_agent'
            ? 'server_agent'
            : 'cronjob';
    }
}

/**
 * Called when Monitor Type dropdown changes. Re-renders dynamic fields.
 */
function onMonitorTypeChange() {
    const type = document.getElementById('monitor-type-select').value;
    const dynamicContainer = document.getElementById('monitor-dynamic-fields');
    const advancedContainer = document.getElementById('monitor-advanced-fields');
    const advancedToggle = document.getElementById('advanced-settings-toggle');

    dynamicContainer.innerHTML = getDynamicFieldsHTML(type);
    advancedContainer.innerHTML = getAdvancedFieldsHTML(type);
    const hasAdvancedFields = advancedContainer.innerHTML.trim().length > 0;

    const advSettings = document.getElementById('advanced-settings');
    if (advSettings) advSettings.style.display = 'none';
    if (advancedToggle) advancedToggle.style.display = hasAdvancedFields ? '' : 'none';
    const icon = document.getElementById('advanced-toggle-icon');
    if (icon) { icon.className = 'fas fa-chevron-down'; }
}

/**
 * Shared input styles for the dark modal
 */
const _inputStyle = 'width:100%; padding:9px 12px; background:#1a1f26; border:1px solid #3a424d; border-radius:4px; color:#fff; font-size:0.9rem;';
const _labelStyle = 'display:block; margin-bottom:6px; font-size:0.85rem; color:#c0c6ce; font-weight:500;';
const _hintStyle = 'font-size:0.78rem; color:#6b7785; margin-top:4px; margin-bottom:0;';
const _groupStyle = 'margin-bottom:16px;';

function getDynamicFieldsHTML(type) {
    let html = '';

    // Monitor Name (all types)
    html += `
        <div class="form-group" style="${_groupStyle}">
            <label style="${_labelStyle}">Monitor Name *</label>
            <input type="text" id="mon-name" placeholder="My Website" style="${_inputStyle}">
            <p style="${_hintStyle}">A user friendly name for your monitor to help you identify it. E.g: My 3rd Website</p>
        </div>
    `;

    // Website type fields
    if (type === 'website') {
        html += `
            <div class="form-group" style="${_groupStyle}">
                <label style="${_labelStyle}">Website Link *</label>
                <input type="text" id="mon-target" placeholder="https://example.com" style="${_inputStyle}">
                <p style="${_hintStyle}">E.g: http://google.com or google.com or https://google.com</p>
                <p style="${_hintStyle}">This will be the link our system will access to make sure your website is online.</p>
            </div>
        `;
    }

    // Heartbeat type fields
    if (type === 'heartbeat') {
        html += `
            <div class="form-group" style="${_groupStyle}">
                <label style="${_labelStyle}">Heartbeat Type</label>
                <select id="mon-heartbeat-type" style="${_inputStyle}">
                    <option value="cronjob" selected>Cronjob</option>
                    <option value="server_agent">Server Agent</option>
                </select>
                <p style="${_hintStyle}">Cronjob: we provide a URL for your app/cron to ping every minute. Server Agent: install the agent and collect server metrics.</p>
            </div>
        `;
    }

    // Category (all types)
    html += `
        <div class="form-group" style="${_groupStyle}">
            <label style="${_labelStyle}">(Optional) Category</label>
            <input type="text" id="mon-category" placeholder="Production" style="${_inputStyle}">
            <p style="${_hintStyle}">Assign a Category for this monitor so it will be automatically grouped in your Status Pages.</p>
        </div>
    `;


    return html;
}

function getAdvancedFieldsHTML(type) {
    if (type !== 'website') {
        return '';
    }

    let html = '<div style="border-top:1px solid #2d343d; padding-top:14px; margin-top:4px;">';
    html += '<h4 style="font-size:0.9rem; color:#44b6ae; margin:0 0 14px 0; font-weight:600;">Advanced Settings</h4>';

    html += `
        <div class="form-group" style="${_groupStyle}">
            <label style="${_labelStyle}">Timeout</label>
            <select id="mon-timeout" style="${_inputStyle}">
                <option value="1">1 second</option>
                <option value="2">2 seconds</option>
                <option value="3">3 seconds</option>
                <option value="5">5 seconds</option>
                <option value="10" selected>10 seconds (recommended)</option>
                <option value="15">15 seconds</option>
            </select>
            <p style="${_hintStyle}">Time after which the target is considered offline if it does not respond.</p>
        </div>
    `;

    html += '</div>';
    return html;
}

function toggleAdvancedSettings() {
    const settings = document.getElementById('advanced-settings');
    const icon = document.getElementById('advanced-toggle-icon');
    if (settings) {
        const isVisible = settings.style.display !== 'none';
        settings.style.display = isVisible ? 'none' : 'block';
        if (icon) {
            icon.className = isVisible ? 'fas fa-chevron-down' : 'fas fa-chevron-up';
        }
    }
}


/**
 * Submit Add Monitor. Uses PATCH when state.editMonitorId is set.
 */
async function submitAddMonitor() {
    const type = document.getElementById('monitor-type-select').value;
    const name = document.getElementById('mon-name')?.value?.trim();
    const editId = state.editMonitorId;
    const existingEditMonitor = editId
        ? state.monitors.uptime.find((m) => String(m.id) === String(editId))
            || state.monitors.server.find((m) => String(m.id) === String(editId))
            || state.monitors.heartbeat.find((m) => String(m.id) === String(editId))
        : null;

    if (!name) {
        showToast('Monitor name is required', 'error');
        return;
    }
    const requestedNameNormalized = normalizeMonitorName(name);
    const currentEditNameNormalized = normalizeMonitorName(existingEditMonitor?.name || '');
    const isUnchangedEditName = !!(editId && requestedNameNormalized === currentEditNameNormalized);
    if (!isUnchangedEditName && isDuplicateMonitorName(name, editId || null)) {
        showToast('A monitor with this name already exists', 'error');
        return;
    }

    const target = document.getElementById('mon-target')?.value?.trim() || '';
    if (type === 'website' && !target) {
        showToast('Website Link is required', 'error');
        return;
    }

    const editType = state.editMonitorType || 'website';
    if (editId) {
        showToast('Updating monitor...', 'info');
        try {
            let endpoint = '';
            const category = document.getElementById('mon-category')?.value?.trim() || '';
            if (editType === MONITOR_SOURCE.HEARTBEAT_CRONJOB) {
                endpoint = `/api/heartbeat-monitors/${editId}`;
                const body = { name, category, heartbeat_type: 'cronjob' };
                const res = await apiRequest(endpoint, { method: 'PATCH', body: JSON.stringify(body) });
                if (res.ok) { state.editMonitorId = null; state.editMonitorType = null; showToast('Monitor updated.', 'success'); hideModal(); loadAllData(); }
                else { const err = await res.json().catch(() => ({})); showToast(err.detail || 'Update failed', 'error'); }
            } else if (editType === MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT) {
                endpoint = `/api/heartbeat-monitors/server-agent/${editId}`;
                const body = { name, category };
                const res = await apiRequest(endpoint, { method: 'PATCH', body: JSON.stringify(body) });
                if (res.ok) { state.editMonitorId = null; state.editMonitorType = null; showToast('Monitor updated.', 'success'); hideModal(); loadAllData(); }
                else { const err = await res.json().catch(() => ({})); showToast(err.detail || 'Update failed', 'error'); }
            } else {
                endpoint = `/api/uptime-monitors/${editId}`;
                const body = { name, target: target || undefined, category };
                const res = await apiRequest(endpoint, { method: 'PATCH', body: JSON.stringify(body) });
                if (res.ok) { state.editMonitorId = null; state.editMonitorType = null; showToast('Monitor updated.', 'success'); hideModal(); loadAllData(); }
                else { const err = await res.json().catch(() => ({})); showToast(err.detail || 'Update failed', 'error'); }
            }
        } catch (e) {
            console.error(e);
            showToast('Update failed', 'error');
        }
        return;
    }

    showToast('Creating monitor...', 'info');

    try {
        let endpoint = '';
        let body = {};
        let createdMonitorType = '';
        let createdHeartbeatType = '';

        const category = document.getElementById('mon-category')?.value?.trim() || '';

        switch (type) {
            case 'website':
                endpoint = '/api/uptime-monitors';
                createdMonitorType = 'website';
                body = {
                    name: name,
                    type: 1,
                    target: target,
                    category: category,
                    timeout: parseInt(document.getElementById('mon-timeout')?.value) || 10
                };
                break;
            case 'heartbeat':
                {
                    const heartbeatType = document.getElementById('mon-heartbeat-type')?.value || 'cronjob';
                    createdMonitorType = 'heartbeat';
                    createdHeartbeatType = heartbeatType;
                    if (heartbeatType === 'server_agent') {
                        endpoint = '/api/heartbeat-monitors/server-agent';
                        body = {
                            name: name,
                            category: category
                        };
                    } else {
                        endpoint = '/api/heartbeat-monitors';
                        body = {
                            name: name,
                            heartbeat_type: 'cronjob',
                            category: category,
                            grace_period: 3
                        };
                    }
                }
                break;
            default:
                endpoint = '/api/uptime-monitors';
                createdMonitorType = 'website';
                body = {
                    name: name,
                    type: 1,
                    target: target,
                    category: category
                };
        }

        const res = await apiRequest(endpoint, {
            method: 'POST',
            body: JSON.stringify(body)
        });

        if (res.ok) {
            const createdMonitor = await res.json().catch(() => null);
            const responseHeartbeatType = String(createdMonitor?.heartbeat_type || '').trim().toLowerCase();
            const effectiveHeartbeatType =
                responseHeartbeatType === 'server_agent' || responseHeartbeatType === 'cronjob'
                    ? responseHeartbeatType
                    : createdHeartbeatType;
            showToast('Monitor created successfully!', 'success');
            hideModal();

            let setupShown = false;
            if (createdMonitorType === 'heartbeat') {
                const responseId = createdMonitor?.id ? String(createdMonitor.id) : null;
                if (responseId) {
                    if (effectiveHeartbeatType === 'server_agent') {
                        await showServerAgentCommandCenter(responseId, 'install', 'linux');
                    } else {
                        await showHeartbeatPingUrl(responseId, 'url');
                    }
                    setupShown = true;
                }
            }

            await loadAllData();

            if (createdMonitorType === 'heartbeat' && !setupShown) {
                const createdId = resolveCreatedHeartbeatMonitorId(createdMonitor, effectiveHeartbeatType, name);
                if (!createdId) {
                    showToast('Monitor created. Open Monitor Tools from the row actions to view setup commands.', 'warning');
                    return;
                }
                if (effectiveHeartbeatType === 'server_agent') {
                    await showServerAgentCommandCenter(createdId, 'install', 'linux');
                } else {
                    await showHeartbeatPingUrl(createdId, 'url');
                }
            }
        } else {
            const errData = await res.json().catch(() => null);
            showToast(errData?.detail || 'Failed to create monitor', 'error');
        }
    } catch (e) {
        console.error(e);
        showToast('An error occurred while creating the monitor', 'error');
    }
}

function resolveCreatedHeartbeatMonitorId(createdMonitor, heartbeatType, monitorName) {
    if (createdMonitor && createdMonitor.id) {
        return String(createdMonitor.id);
    }

    const targetType = heartbeatType === 'server_agent' ? 'server_agent' : 'cronjob';
    const sourceList = targetType === 'server_agent' ? state.monitors.server : state.monitors.heartbeat;
    if (!Array.isArray(sourceList) || sourceList.length === 0) {
        return null;
    }

    const normalizedName = String(monitorName || '').trim().toLowerCase();
    const exactNameMatches = sourceList.filter((m) => String(m?.name || '').trim().toLowerCase() === normalizedName);
    const candidates = (exactNameMatches.length > 0 ? exactNameMatches : sourceList).slice();

    candidates.sort((a, b) => {
        const aTs = Date.parse(a?.created_at || '') || 0;
        const bTs = Date.parse(b?.created_at || '') || 0;
        return bTs - aTs;
    });

    const chosen = candidates[0];
    return chosen && chosen.id ? String(chosen.id) : null;
}

function getHeartbeatCommandVariant(data, variant) {
    const selected = (variant || 'url').toLowerCase();
    if (selected === 'curl') return data.curl_command || data.ping_url || '';
    if (selected === 'wget') return data.wget_command || data.ping_url || '';
    return data.ping_url || '';
}

async function copyTextToClipboard(text, successMessage = 'Copied to clipboard') {
    const value = String(text || '');
    if (!value) {
        showToast('Nothing to copy', 'warning');
        return;
    }

    try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(value);
            showToast(successMessage, 'success');
            return;
        }
    } catch (_) {
        // Fall back to execCommand path below
    }

    const temp = document.createElement('textarea');
    temp.value = value;
    temp.setAttribute('readonly', 'readonly');
    temp.style.position = 'fixed';
    temp.style.top = '-9999px';
    temp.style.left = '-9999px';
    document.body.appendChild(temp);
    temp.select();
    temp.setSelectionRange(0, temp.value.length);

    try {
        const ok = document.execCommand('copy');
        if (ok) showToast(successMessage, 'success');
        else showToast('Copy failed', 'error');
    } catch (_) {
        showToast('Copy failed', 'error');
    } finally {
        document.body.removeChild(temp);
    }
}

async function copyTextFromElement(elementId, successMessage = 'Copied to clipboard') {
    const element = document.getElementById(elementId);
    if (!element) {
        showToast('Copy source not found', 'error');
        return;
    }
    const value = ('value' in element) ? element.value : element.textContent;
    await copyTextToClipboard(value || '', successMessage);
}

async function showHeartbeatPingUrl(id, variant = 'url') {
    try {
        const res = await apiRequest(`/api/heartbeat-monitors/${id}/ping-url`);
        if (!res.ok) {
            showToast('Failed to load heartbeat URL', 'error');
            return;
        }
        const data = await res.json();
        const selectedVariant = (variant || 'url').toLowerCase();
        const commandValue = getHeartbeatCommandVariant(data, selectedVariant);
        const variantLabel = selectedVariant === 'url' ? 'Direct URL' : selectedVariant.toUpperCase();
        showModal(`
            <div class="modal-header"><h2 class="modal-title">Heartbeat Configuration</h2></div>
            <div class="modal-body">
                <p class="mb-3 text-muted">Use one of these options in your cron/app task:</p>
                <div class="form-group mb-3">
                    <label class="form-label">Command Type</label>
                    <select class="form-control" onchange="showHeartbeatPingUrl('${id}', this.value)">
                        <option value="url" ${selectedVariant === 'url' ? 'selected' : ''}>Direct URL</option>
                        <option value="curl" ${selectedVariant === 'curl' ? 'selected' : ''}>cURL</option>
                        <option value="wget" ${selectedVariant === 'wget' ? 'selected' : ''}>wget</option>
                    </select>
                </div>
                <label class="form-label">${variantLabel}</label>
                <textarea id="heartbeat-command-output" readonly style="width:100%; min-height:100px; background: var(--bg-main); padding: 0.85rem; border-radius: var(--radius-md); font-family: monospace; border: 1px solid var(--border-color); color: var(--text-primary); resize: vertical;">${escapeHtml(commandValue)}</textarea>
                <div style="display:flex; justify-content:flex-end; margin-top:8px;">
                    <button class="btn btn-secondary btn-small" onclick="copyTextFromElement('heartbeat-command-output', 'Heartbeat command copied')">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                </div>
                <p class="text-muted mt-3" style="font-size: 0.75rem;">SID: ${escapeHtml(data.sid || '')}</p>
            </div>
            <div class="modal-footer">
                <button class="btn btn-danger" onclick="showDeleteMonitorDialog('${MONITOR_SOURCE.HEARTBEAT_CRONJOB}', '${id}')">
                    <i class="fas fa-trash-alt"></i> Delete Monitor
                </button>
                <button class="btn btn-primary" onclick="hideModal()">Done</button>
            </div>
        `);
    } catch (e) {
        console.error(e);
        showToast('Failed to load heartbeat URL', 'error');
    }
}

function readServerAgentCommandOptions() {
    return {
        run_as_root: !!document.getElementById('agent-opt-run-root')?.checked,
        monitor_services: !!document.getElementById('agent-opt-monitor-services')?.checked,
        services: (document.getElementById('agent-opt-services')?.value || '').trim(),
        monitor_raid: !!document.getElementById('agent-opt-monitor-raid')?.checked,
        monitor_drive: !!document.getElementById('agent-opt-monitor-drive')?.checked,
        view_processes: !!document.getElementById('agent-opt-view-processes')?.checked,
        overwrite_ports: !!document.getElementById('agent-opt-overwrite-ports')?.checked,
        ports: (document.getElementById('agent-opt-ports')?.value || '').trim()
    };
}

function getServerAgentLiveStatus(monitor) {
    if (!monitor) return 'Waiting for first agent report...';
    if (!monitor.last_report_at) return `Waiting for Agent (SID: ${monitor.sid}) to report in...`;
    const ts = new Date(monitor.last_report_at);
    return `Agent is reporting. Last update: ${ts.toLocaleString()}`;
}

async function showServerAgentCommandCenter(id, mode = 'install', platform = 'linux', explicitOptions = null) {
    const defaults = {
        run_as_root: false,
        monitor_services: false,
        services: '',
        monitor_raid: false,
        monitor_drive: false,
        view_processes: false,
        overwrite_ports: false,
        ports: ''
    };
    const modeValue = (mode || 'install').toLowerCase();
    const platformValue = (platform || 'linux').toLowerCase();
    const modalOptions = readServerAgentCommandOptions();
    const options = { ...defaults, ...(explicitOptions || {}), ...modalOptions };

    const params = new URLSearchParams({
        platform: platformValue,
        mode: modeValue,
        run_as_root: String(options.run_as_root),
        monitor_services: String(options.monitor_services),
        services: options.services || '',
        monitor_raid: String(options.monitor_raid),
        monitor_drive: String(options.monitor_drive),
        view_processes: String(options.view_processes),
        overwrite_ports: String(options.overwrite_ports),
        ports: options.ports || ''
    });

    try {
        const res = await apiRequest(`/api/heartbeat-monitors/server-agent/${id}/command?${params.toString()}`);
        if (!res.ok) {
            const message = await readApiError(res, 'Failed to load agent command');
            showToast(message, 'error');
            return;
        }

        const data = await res.json();
        const monitor = state.monitors.server.find(m => String(m.id) === String(id));
        const liveStatus = getServerAgentLiveStatus(monitor);
        const isUninstallMode = modeValue === 'uninstall';
        const commandLabel = isUninstallMode
            ? 'Uninstall Code'
            : (modeValue === 'update' ? 'Update Code' : 'Install Code');
        const roleLabel = platformValue === 'windows' ? 'Administrator' : 'root';
        const commandHint = isUninstallMode
            ? `Run the uninstall code as ${roleLabel} on your server.`
            : `Run the ${modeValue} code as ${roleLabel} on your server terminal.`;

        showModal(`
            <div class="modal-header">
                <h2 class="modal-title">Statrix Server Monitoring Agent</h2>
            </div>
            <div class="modal-body agent-command-modal-body">
                <div class="agent-command-platform-row">
                    <button class="btn btn-small ${platformValue === 'linux' ? 'btn-primary' : 'btn-secondary'}" onclick="showServerAgentCommandCenter('${id}', '${modeValue}', 'linux')">
                        <i class="fab fa-linux"></i> Linux
                    </button>
                    <button class="btn btn-small ${platformValue === 'windows' ? 'btn-primary' : 'btn-secondary'}" onclick="showServerAgentCommandCenter('${id}', '${modeValue}', 'windows')">
                        <i class="fab fa-windows"></i> Windows
                    </button>
                    <button class="btn btn-small ${platformValue === 'macos' ? 'btn-primary' : 'btn-secondary'}" onclick="showServerAgentCommandCenter('${id}', '${modeValue}', 'macos')">
                        <i class="fab fa-apple"></i> macOS
                    </button>
                </div>

                <p class="agent-command-hint">${commandHint}</p>

                ${!isUninstallMode ? `
                    <div class="agent-command-options">
                        <label class="agent-command-option">
                            <input type="checkbox" id="agent-opt-run-root" ${options.run_as_root ? 'checked' : ''} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                            Run agent as root
                        </label>
                        <label class="agent-command-option">
                            <input type="checkbox" id="agent-opt-monitor-services" ${options.monitor_services ? 'checked' : ''} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                            Monitor services
                        </label>
                        <input type="text" id="agent-opt-services" class="agent-command-inline-input" placeholder="ssh,nginx,mysql" value="${escapeHtml(options.services || '')}" ${options.monitor_services ? '' : 'disabled'} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                        <label class="agent-command-option">
                            <input type="checkbox" id="agent-opt-monitor-raid" ${options.monitor_raid ? 'checked' : ''} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                            Monitor software RAID / ZFS pools
                        </label>
                        <label class="agent-command-option">
                            <input type="checkbox" id="agent-opt-monitor-drive" ${options.monitor_drive ? 'checked' : ''} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                            Monitor drive health
                        </label>
                        <label class="agent-command-option">
                            <input type="checkbox" id="agent-opt-view-processes" ${options.view_processes ? 'checked' : ''} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                            View running processes
                        </label>
                        <label class="agent-command-option">
                            <input type="checkbox" id="agent-opt-overwrite-ports" ${options.overwrite_ports ? 'checked' : ''} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                            Overwrite monitored ports
                        </label>
                        <input type="text" id="agent-opt-ports" class="agent-command-inline-input" placeholder="80,443" value="${escapeHtml(options.ports || '')}" ${options.overwrite_ports ? '' : 'disabled'} onchange="showServerAgentCommandCenter('${id}', '${modeValue}', '${platformValue}')">
                    </div>
                ` : ''}

                <label class="agent-command-label">${commandLabel}</label>
                <textarea id="server-agent-command-output" readonly class="agent-command-output">${escapeHtml(data.command || '')}</textarea>
                <div class="agent-command-copy-row">
                    <button class="btn btn-secondary btn-small" onclick="copyTextFromElement('server-agent-command-output', 'Agent command copied')">
                        <i class="fas fa-copy"></i> Copy
                    </button>
                </div>

                <div class="agent-command-status">
                    <strong style="color:var(--text-primary);">Live Agent Status:</strong> ${escapeHtml(liveStatus)}
                </div>
            </div>
            <div class="modal-footer agent-command-footer">
                <div class="agent-command-mode-group">
                    <button class="btn btn-small ${modeValue === 'install' ? 'btn-primary' : 'btn-secondary'}" onclick="showServerAgentCommandCenter('${id}', 'install', '${platformValue}')">
                        <i class="fas fa-cloud-download-alt"></i> Install
                    </button>
                    <button class="btn btn-small ${modeValue === 'update' ? 'btn-primary' : 'btn-secondary'}" onclick="showServerAgentCommandCenter('${id}', 'update', '${platformValue}')">
                        <i class="fas fa-upload"></i> Update
                    </button>
                    <button class="btn btn-small ${modeValue === 'uninstall' ? 'btn-primary' : 'btn-secondary'}" onclick="showServerAgentCommandCenter('${id}', 'uninstall', '${platformValue}')">
                        <i class="fas fa-times-circle"></i> Uninstall
                    </button>
                </div>
                <div style="display:flex; gap:8px; flex-wrap:wrap;">
                    <button class="btn btn-danger" onclick="showDeleteMonitorDialog('${MONITOR_SOURCE.HEARTBEAT_SERVER_AGENT}', '${id}', '${modeValue}', '${platformValue}')">
                        <i class="fas fa-trash-alt"></i> Delete Monitor
                    </button>
                    <button class="btn btn-secondary" onclick="hideModal()">Close</button>
                </div>
            </div>
        `);
        const modalContainer = document.getElementById('modal-container');
        if (modalContainer) {
            modalContainer.classList.add('modal-agent-command');
        }
    } catch (e) {
        console.error(e);
        showToast('Failed to load agent command', 'error');
    }
}

/**
 * Action: Show Server Install Command (legacy alias)
 */
async function showServerInstallCommand(id, platform = 'linux') {
    await showServerAgentCommandCenter(id, 'install', platform);
}

async function resolveIncident(id) {
    try {
        const res = await apiRequest(`/api/incidents/${id}/resolve`, { method: 'POST' });
        if (res.ok) {
            showToast('Incident resolved');
            loadAllData();
        } else {
            showToast('Failed to resolve incident', 'error');
        }
    } catch (e) { console.error(e); }
}

async function hideIncidentFromStatus(id) {
    const confirmed = window.confirm('Delete this resolved incident from the public status page? It will remain available in dashboard history.');
    if (!confirmed) return;

    try {
        const res = await apiRequest(`/api/incidents/${id}/hide-from-status`, { method: 'POST' });
        if (res.ok) {
            showToast('Incident deleted from public status page');
            await loadAllData();
        } else {
            const message = await readApiError(res, 'Failed to delete incident from public status page');
            showToast(message, 'error');
        }
    } catch (e) {
        console.error(e);
        showToast('Failed to delete incident from public status page', 'error');
    }
}

function loadReportsTab() {
    loadReportData();
}

function loadReportData() {
    const period = getReportPeriodDays();

    // Calculate summary stats
    updateReportVisibility();
    updateReportSummary(period);

    updateUptimeChart(period);
    if (reportVisibility.showResponse) {
        updateResponseChart(period);
    } else {
        if (state.charts.response) {
            state.charts.response.destroy();
            state.charts.response = null;
        }
        setChartEmptyState('response-chart', null);
    }

    loadMonitorBreakdown();

    loadIncidentTimeline(period);
}

function updateReportSummary(period) {
    const reportUptimeEl = document.getElementById('report-uptime');
    const reportIncidentsEl = document.getElementById('report-incidents');
    const reportAvgResponseEl = document.getElementById('report-avg-response');
    const reportChecksEl = document.getElementById('report-checks');

    const uptimeValues = [
        ...state.monitors.uptime,
        ...state.monitors.server,
        ...state.monitors.heartbeat
    ]
        .map(m => (Number.isFinite(m.uptime_percentage) ? Number(m.uptime_percentage) : null))
        .filter(v => Number.isFinite(v));

    let uptimePercent = null;
    if (Number.isFinite(state.overallUptime)) {
        uptimePercent = state.overallUptime;
    } else if (uptimeValues.length > 0) {
        uptimePercent = uptimeValues.reduce((sum, v) => sum + v, 0) / uptimeValues.length;
    }

    if (reportUptimeEl) {
        reportUptimeEl.textContent = uptimePercent !== null ? `${uptimePercent.toFixed(4)}%` : '--';
    }

    // Incidents count
    if (reportIncidentsEl) reportIncidentsEl.textContent = state.incidents.length;

    // Average response time (from uptime monitors)
    const monitorsWithResponse = state.monitors.uptime.filter(m => m.response_time_avg != null);
    if (monitorsWithResponse.length > 0) {
        const avgResponse = monitorsWithResponse.reduce((sum, m) => sum + (m.response_time_avg || 0), 0) / monitorsWithResponse.length;
        if (reportAvgResponseEl) reportAvgResponseEl.textContent = `${Math.round(avgResponse)} ms`;
    } else if (reportAvgResponseEl) {
        reportAvgResponseEl.textContent = '-- ms';
    }

    if (reportChecksEl) {
        const periodDays = getReportPeriodDays();
        const checks = state.monitors.uptime
            .map(m => computeChecksForMonitor(m, periodDays))
            .filter(v => Number.isFinite(v));
        if (checks.length > 0) {
            const totalChecks = checks.reduce((sum, v) => sum + v, 0);
            reportChecksEl.textContent = formatNumber(totalChecks);
        } else {
            reportChecksEl.textContent = '--';
        }
    }
}

async function updateUptimeChart(period) {
    const ctx = document.getElementById('uptime-chart');
    if (!ctx || typeof Chart === 'undefined') return;

    const chartType = document.getElementById('uptime-chart-type')?.value || 'daily';
    const selectedPeriodDays = Number.isFinite(Number(period)) ? Number(period) : getReportPeriodDays();
    let chartSeries = null;

    if (chartType === 'daily') {
        // Daily view should stay a true short-range daily chart.
        const dailyLocal = buildDailyUptimeSeries();
        chartSeries = trimSeriesToPeriod(dailyLocal, Math.min(7, selectedPeriodDays));
    } else {
        let dailySeries = await buildPeriodDailyUptimeSeries(selectedPeriodDays);
        if (!dailySeries) {
            // Fallback to already-loaded monitor history if period data fetch fails.
            dailySeries = buildDailyUptimeSeries();
        }
        if (dailySeries) {
            chartSeries = trimSeriesToPeriod(dailySeries, selectedPeriodDays);
            if (chartType === 'weekly') {
                chartSeries = aggregateDailySeriesToWeekly(chartSeries);
            } else if (chartType === 'monthly') {
                chartSeries = aggregateDailySeriesToMonthly(chartSeries);
            }
        }
    }

    if (!chartSeries || !Array.isArray(chartSeries.data) || chartSeries.data.every((v) => !Number.isFinite(v))) {
        if (state.charts.uptime) {
            state.charts.uptime.destroy();
            state.charts.uptime = null;
        }
        setChartEmptyState('uptime-chart', 'No uptime history available for this view.');
        return;
    }

    const labels = chartSeries.labels;
    const data = chartSeries.data;
    setChartEmptyState('uptime-chart', null);

    // Destroy existing chart
    if (state.charts.uptime) {
        state.charts.uptime.destroy();
    }

    // Create chart
    state.charts.uptime = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: chartType === 'daily' ? 'Daily Uptime %' : chartType === 'weekly' ? 'Weekly Uptime %' : 'Monthly Uptime %',
                data,
                borderColor: '#44b6ae',
                backgroundColor: 'rgba(68, 182, 174, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 3,
                pointHoverRadius: 5,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1e2a38',
                    titleColor: '#fff',
                    bodyColor: '#c5d0de',
                    borderColor: '#2d3a4a',
                    borderWidth: 1,
                    padding: 10,
                    callbacks: {
                        label: (context) => `${context.parsed.y.toFixed(2)}% uptime`
                    }
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(45, 58, 74, 0.3)', drawBorder: false },
                    ticks: { color: '#889097', font: { size: 11 } }
                },
                y: {
                    min: 0,
                    max: 100,
                    grid: { color: 'rgba(45, 58, 74, 0.3)', drawBorder: false },
                    ticks: {
                        color: '#889097',
                        font: { size: 11 },
                        callback: (value) => `${value}%`
                    }
                }
            }
        }
    });
}

function updateResponseChart(period) {
    const ctx = document.getElementById('response-chart');
    if (!ctx || typeof Chart === 'undefined') return;

    const responseMonitors = state.monitors.uptime
        .filter(m => Number.isFinite(m.response_time_avg))
        .slice(0, 12);

    if (responseMonitors.length === 0) {
        if (state.charts.response) {
            state.charts.response.destroy();
            state.charts.response = null;
        }
        setChartEmptyState('response-chart', 'No response time data available yet.');
        return;
    }

    const labels = responseMonitors.map(m => m.name);
    const data = responseMonitors.map(m => Math.round(m.response_time_avg));
    setChartEmptyState('response-chart', null);

    // Destroy existing chart
    if (state.charts.response) {
        state.charts.response.destroy();
    }

    // Create chart
    state.charts.response = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Response Time (ms)',
                data,
                backgroundColor: 'rgba(68, 182, 174, 0.6)',
                borderColor: '#44b6ae',
                borderWidth: 1,
                borderRadius: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#1e2a38',
                    titleColor: '#fff',
                    bodyColor: '#c5d0de',
                    borderColor: '#2d3a4a',
                    borderWidth: 1,
                    padding: 10,
                    callbacks: {
                        label: (context) => `${Math.round(context.parsed.y)} ms`
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: '#889097', font: { size: 11 } }
                },
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(45, 58, 74, 0.3)', drawBorder: false },
                    ticks: {
                        color: '#889097',
                        font: { size: 11 },
                        callback: (value) => `${value}ms`
                    }
                }
            }
        }
    });
}

function loadMonitorBreakdown() {
    const container = document.getElementById('report-monitors-table');
    if (!container) return;

    const periodDays = getReportPeriodDays();
    const showResponse = reportVisibility.showResponse;
    const showChecks = reportVisibility.showChecks;
    const allMonitors = [
        ...state.monitors.uptime.map(m => ({ ...m, category: 'Website' })),
        ...state.monitors.server.map(m => ({ ...m, category: 'Server Agent' })),
        ...state.monitors.heartbeat.map(m => ({ ...m, category: 'Cronjob' }))
    ];

    if (allMonitors.length === 0) {
        container.innerHTML = `
            <div class="no-data-state">
                <i class="fas fa-chart-bar"></i>
                <h3>No Monitors</h3>
                <p>Add monitors to see breakdown reports.</p>
            </div>
        `;
        return;
    }

    const html = `
        <table class="report-table">
            <thead>
                <tr>
                    <th>Monitor</th>
                    <th>Type</th>
                    <th>Uptime</th>
                    ${showResponse ? '<th>Avg Response</th>' : ''}
                    ${showChecks ? '<th>Total Checks</th>' : ''}
                </tr>
            </thead>
            <tbody>
                ${allMonitors.map(m => {
        const uptime = Number.isFinite(m.uptime_percentage) ? Math.max(0, Math.min(100, Number(m.uptime_percentage))) : null;
        const response = Number.isFinite(m.response_time_avg) ? Math.round(m.response_time_avg) : null;
        const checks = computeChecksForMonitor(m, periodDays);

        return `
                        <tr>
                            <td>
                                <div style="font-weight: 600; color: #fff;">${m.name}</div>
                            </td>
                            <td>
                                <span class="status-badge status-paused">${m.category}</span>
                            </td>
                            <td>
                                <div style="display: flex; align-items: center; gap: 0.75rem;">
                                    <div class="uptime-bar-container">
                                        <div class="uptime-bar" style="width: ${uptime !== null ? uptime : 0}%"></div>
                                    </div>
                                    <span style="font-weight: 600;">${uptime !== null ? `${uptime.toFixed(2)}%` : '--'}</span>
                                </div>
                            </td>
                            ${showResponse ? `<td>${response !== null ? `${response} ms` : '--'}</td>` : ''}
                            ${showChecks ? `<td>${Number.isFinite(checks) ? formatNumber(checks) : '--'}</td>` : ''}
                        </tr>
                    `;
    }).join('')}
            </tbody>
        </table>
    `;

    container.innerHTML = html;
}

function loadIncidentTimeline(period) {
    const container = document.getElementById('incident-timeline');
    if (!container) return;

    if (state.incidents.length === 0) {
        container.innerHTML = `
            <div class="no-data-state">
                <i class="fas fa-check-circle"></i>
                <h3>No Incidents</h3>
                <p>No incidents were recorded during this period.</p>
            </div>
        `;
        return;
    }

    const html = state.incidents.map(incident => {
        const sourceLabel = (incident.source || 'monitor') === 'admin'
            ? 'Admin notice'
            : `${String(incident.monitor_type || 'monitor').toUpperCase()} monitor`;
        const monitorTypeLabel = getIncidentMonitorTypeLabel(incident.monitor_source);
        const affectedLabel = incident.monitor_name
            ? `${incident.monitor_name}${monitorTypeLabel ? ` (${monitorTypeLabel})` : ''}`
            : ((incident.source || 'monitor') === 'admin' ? 'All services' : '--');
        const stateLabel = incident.status === 'open'
            ? 'Active'
            : 'Resolved';
        const timeLabel = incident.status === 'resolved' && incident.resolved_at
            ? new Date(incident.resolved_at).toLocaleString()
            : new Date(incident.started_at).toLocaleString();
        return `
        <div class="timeline-item ${incident.status === 'open' ? 'timeline-incident' : 'timeline-resolved'}">
            <div class="timeline-icon">
                <i class="fas ${incident.status === 'open' ? 'fa-exclamation-triangle' : 'fa-check'}"></i>
            </div>
            <div class="timeline-content">
                <div class="timeline-title">${escapeHtml(incident.title || 'Untitled Incident')}</div>
                <div class="timeline-description">${escapeHtml(sourceLabel)} • Affected: ${escapeHtml(affectedLabel)} • ${stateLabel}</div>
                <div class="timeline-time">${escapeHtml(timeLabel)}</div>
            </div>
        </div>
    `;
    }).join('');

    container.innerHTML = html;
}

function getReportPeriodDays() {
    const periodValue = parseInt(document.getElementById('report-period')?.value || '30', 10);
    return Number.isFinite(periodValue) ? periodValue : 30;
}

const reportVisibility = {
    showResponse: true,
    showChecks: true
};
const REPORT_TREND_CACHE_TTL_MS = 120000;
const reportTrendCache = new Map();
const reportTrendInFlight = new Map();

function updateReportVisibility() {
    const hasWebsite = state.monitors.uptime.length > 0;
    reportVisibility.showResponse = hasWebsite;
    reportVisibility.showChecks = hasWebsite;

    const responseCard = document.getElementById('report-avg-response-card');
    if (responseCard) responseCard.style.display = reportVisibility.showResponse ? '' : 'none';
    const checksCard = document.getElementById('report-total-checks-card');
    if (checksCard) checksCard.style.display = reportVisibility.showChecks ? '' : 'none';
    const responseChartCard = document.getElementById('response-chart-card');
    if (responseChartCard) responseChartCard.style.display = reportVisibility.showResponse ? '' : 'none';
}

function computeChecksForMonitor(monitor, periodDays) {
    if (!Number.isFinite(periodDays) || periodDays <= 0) return null;
    const sourceType = getSourceTypeFromMonitor(monitor);
    if (sourceType !== MONITOR_SOURCE.WEBSITE) return null;

    // Uptime checks run on a fixed 1-minute cadence.
    const interval = 1;

    if (monitor.enabled === false) return 0;

    const checksPerDay = (24 * 60) / interval;
    return Math.round(checksPerDay * periodDays);
}

function setChartEmptyState(chartId, message) {
    const canvas = document.getElementById(chartId);
    if (!canvas) return;
    const body = canvas.parentElement;
    if (!body) return;

    let empty = body.querySelector('.chart-empty');
    if (!empty) {
        empty = document.createElement('div');
        empty.className = 'chart-empty';
        body.appendChild(empty);
    }

    if (message) {
        empty.textContent = message;
        empty.style.display = 'flex';
        canvas.style.display = 'none';
    } else {
        empty.style.display = 'none';
        canvas.style.display = '';
    }
}

function getHistoryStatusValue(entry) {
    const status = typeof entry === 'object' && entry !== null ? entry.status : entry;
    if (status === 'up') return 1;
    if (status === 'partial') return 0.5;
    if (status === 'no_data') return null;
    if (status === 'unknown') return null;
    if (status === 'down') return 0;
    if (status === 'maintenance') return 1;
    if (status === 'not_created') return null;
    return null;
}

function buildDailyUptimeSeries() {
    const monitors = [
        ...state.monitors.uptime,
        ...state.monitors.server,
        ...state.monitors.heartbeat
    ].filter((m) => Number.isFinite(m.uptime_percentage));
    const histories = monitors
        .map(m => (Array.isArray(m.history) ? m.history : null))
        .filter(Boolean);

    if (histories.length === 0) return null;

    const days = Math.min(7, Math.max(...histories.map(h => h.length || 0)));
    if (!Number.isFinite(days) || days <= 0) return null;

    const labels = [];
    const dates = [];
    const data = [];

    for (let i = days - 1; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        dates.push(date);
        labels.push(date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
    }

    for (let i = 0; i < days; i++) {
        let sum = 0;
        let count = 0;
        histories.forEach(history => {
            const index = history.length - days + i;
            const entry = history[index] ?? history[i];
            const value = getHistoryStatusValue(entry);
            if (value === null) return;
            sum += value;
            count += 1;
        });
        data.push(count > 0 ? (sum / count) * 100 : null);
    }

    return { labels, dates, data };
}

function buildDailySeriesFromStatusPayload(payload, offset = 0) {
    const monitors = Array.isArray(payload?.monitors)
        ? payload.monitors.filter((m) => Number.isFinite(m.uptime_percentage))
        : [];
    const histories = monitors
        .map((m) => (Array.isArray(m.history) ? m.history : null))
        .filter(Boolean);
    if (histories.length === 0) return null;

    const days = Math.min(7, Math.max(...histories.map((h) => h.length || 0)));
    if (!Number.isFinite(days) || days <= 0) return null;

    const labels = [];
    const dates = [];
    const data = [];
    const baseDate = new Date();
    baseDate.setDate(baseDate.getDate() + Number(offset || 0));

    for (let i = days - 1; i >= 0; i--) {
        const date = new Date(baseDate);
        date.setDate(baseDate.getDate() - i);
        dates.push(date);
        labels.push(date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
    }

    for (let i = 0; i < days; i++) {
        let sum = 0;
        let count = 0;
        histories.forEach((history) => {
            const index = history.length - days + i;
            const entry = history[index] ?? history[i];
            const value = getHistoryStatusValue(entry);
            if (value === null) return;
            sum += value;
            count += 1;
        });
        data.push(count > 0 ? (sum / count) * 100 : null);
    }

    return { labels, dates, data };
}

async function fetchPublicStatusByOffset(offset = 0) {
    const params = new URLSearchParams();
    if (offset !== 0) params.set('offset', String(offset));
    params.set('tz_offset_minutes', String(-new Date().getTimezoneOffset()));
    const endpoint = `/api/public/status${params.toString() ? `?${params.toString()}` : ''}`;
    const res = await fetch(endpoint, { credentials: 'include', cache: 'no-store' });
    if (!res.ok) {
        throw new Error(`Status request failed (${res.status})`);
    }
    return res.json();
}

async function buildPeriodDailyUptimeSeries(periodDays) {
    const safePeriod = Math.max(7, Math.min(365, Number.isFinite(periodDays) ? Number(periodDays) : 30));
    const cacheKey = `period-${safePeriod}`;
    const nowTs = Date.now();
    const cached = reportTrendCache.get(cacheKey);
    if (cached && (nowTs - cached.ts) < REPORT_TREND_CACHE_TTL_MS) {
        return cached.data;
    }

    if (reportTrendInFlight.has(cacheKey)) {
        return reportTrendInFlight.get(cacheKey);
    }

    const task = (async () => {
        const chunkCount = Math.ceil(safePeriod / 7);
        const rows = [];

        for (let chunk = chunkCount - 1; chunk >= 0; chunk--) {
            const offset = -(chunk * 7);
            try {
                const payload = await fetchPublicStatusByOffset(offset);
                const daily = buildDailySeriesFromStatusPayload(payload, offset);
                if (!daily) continue;
                daily.data.forEach((value, idx) => {
                    const date = daily.dates[idx];
                    if (!date) return;
                    rows.push({
                        key: date.toISOString().slice(0, 10),
                        date,
                        label: daily.labels[idx],
                        value
                    });
                });
            } catch (e) {
                console.warn(`Failed to load report trend chunk (offset=${offset})`, e);
            }
        }

        if (rows.length === 0) return null;

        const byDay = new Map();
        rows
            .sort((a, b) => a.date.getTime() - b.date.getTime())
            .forEach((row) => byDay.set(row.key, row));
        const merged = Array.from(byDay.values()).slice(-safePeriod);

        const result = {
            labels: merged.map((r) => r.label),
            dates: merged.map((r) => r.date),
            data: merged.map((r) => r.value)
        };
        reportTrendCache.set(cacheKey, { ts: Date.now(), data: result });
        return result;
    })();

    reportTrendInFlight.set(cacheKey, task);
    try {
        return await task;
    } finally {
        reportTrendInFlight.delete(cacheKey);
    }
}

function trimSeriesToPeriod(series, periodDays) {
    if (!series || !Array.isArray(series.data) || series.data.length === 0) return null;
    const cap = Math.max(1, Number.isFinite(periodDays) ? periodDays : 30);
    const startIndex = Math.max(0, series.data.length - cap);
    return {
        labels: series.labels.slice(startIndex),
        dates: (series.dates || []).slice(startIndex),
        data: series.data.slice(startIndex)
    };
}

function aggregateDailySeriesToWeekly(series) {
    if (!series || !Array.isArray(series.data) || series.data.length === 0) return null;
    const labels = [];
    const dates = [];
    const data = [];
    const windowSize = 7;

    for (let i = 0; i < series.data.length; i += windowSize) {
        const values = series.data.slice(i, i + windowSize).filter((v) => Number.isFinite(v));
        if (values.length === 0) continue;
        const startLabel = series.labels[i];
        const endLabel = series.labels[Math.min(i + windowSize - 1, series.labels.length - 1)];
        labels.push(startLabel === endLabel ? startLabel : `${startLabel} - ${endLabel}`);
        dates.push(series.dates && series.dates[i] ? series.dates[i] : new Date());
        data.push(values.reduce((sum, v) => sum + v, 0) / values.length);
    }

    return labels.length > 0 ? { labels, dates, data } : null;
}

function aggregateDailySeriesToMonthly(series) {
    if (!series || !Array.isArray(series.data) || series.data.length === 0) return null;
    const buckets = [];
    const bucketByKey = new Map();

    series.data.forEach((value, index) => {
        const date = series.dates && series.dates[index] ? series.dates[index] : null;
        if (!date || !Number.isFinite(value)) return;
        const key = `${date.getFullYear()}-${date.getMonth()}`;
        if (!bucketByKey.has(key)) {
            const item = {
                key,
                label: date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }),
                date,
                values: []
            };
            bucketByKey.set(key, item);
            buckets.push(item);
        }
        bucketByKey.get(key).values.push(value);
    });

    if (buckets.length === 0) return null;
    return {
        labels: buckets.map((b) => b.label),
        dates: buckets.map((b) => b.date),
        data: buckets.map((b) => b.values.reduce((sum, v) => sum + v, 0) / b.values.length)
    };
}

function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

// Bulk selection system removed

function changePage(type, page) {
    state.pagination[type].page = page;
    refreshCurrentView();
}

function changePerPage(type, perPage) {
    state.pagination[type].perPage = parseInt(perPage);
    state.pagination[type].page = 1; // Reset to first page
    refreshCurrentView();
}

function getPaginatedData(data, type) {
    const { page, perPage } = state.pagination[type];
    const start = (page - 1) * perPage;
    const end = start + perPage;

    state.pagination[type].total = data.length;

    return {
        data: data.slice(start, end),
        totalPages: Math.ceil(data.length / perPage),
        currentPage: page,
        total: data.length,
        start: start + 1,
        end: Math.min(end, data.length)
    };
}

function renderPagination(type, containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const { page, perPage, total } = state.pagination[type];
    const totalPages = Math.ceil(total / perPage);

    if (totalPages <= 1) {
        container.innerHTML = '';
        return;
    }

    let html = `
        <div class="pagination-container">
            <div class="pagination-info">
                Showing <strong>${(page - 1) * perPage + 1}</strong> to <strong>${Math.min(page * perPage, total)}</strong> of <strong>${total}</strong>
            </div>
            <div class="pagination">
                <button class="pagination-btn" onclick="changePage('${type}', ${page - 1})" ${page === 1 ? 'disabled' : ''}>
                    <i class="fas fa-chevron-left"></i>
                </button>
    `;

    // Page numbers
    const maxVisiblePages = 5;
    let startPage = Math.max(1, page - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);

    if (startPage > 1) {
        html += `<button class="pagination-btn" onclick="changePage('${type}', 1)">1</button>`;
        if (startPage > 2) {
            html += `<span class="pagination-ellipsis">...</span>`;
        }
    }

    for (let i = startPage; i <= endPage; i++) {
        html += `<button class="pagination-btn ${i === page ? 'active' : ''}" onclick="changePage('${type}', ${i})">${i}</button>`;
    }

    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            html += `<span class="pagination-ellipsis">...</span>`;
        }
        html += `<button class="pagination-btn" onclick="changePage('${type}', ${totalPages})">${totalPages}</button>`;
    }

    html += `
                <button class="pagination-btn" onclick="changePage('${type}', ${page + 1})" ${page === totalPages ? 'disabled' : ''}>
                    <i class="fas fa-chevron-right"></i>
                </button>
            </div>
        </div>
    `;

    container.innerHTML = html;
}

async function setVisibility(type, monitorId, isPublic) {
    if (isPublic) {
        await makePublic(type, monitorId);
    } else {
        await makePrivate(type, monitorId);
    }
}

async function makePublic(type, id) {
    try {
        const endpoint = `${getApiBaseBySourceType(type)}/${id}/make-public`;

        const res = await apiRequest(endpoint, {
            method: 'POST'
        });

        if (res.ok) {
            showToast('This report is now Public, and you can share its link.', 'success');
            loadAllData();
        } else {
            showToast('Failed to make monitor public', 'error');
        }
    } catch (error) {
        console.error('Error making monitor public:', error);
        showToast('Error making monitor public', 'error');
    }
}

async function makePrivate(type, id) {
    try {
        const endpoint = `${getApiBaseBySourceType(type)}/${id}/make-private`;

        const res = await apiRequest(endpoint, {
            method: 'POST'
        });

        if (res.ok) {
            showToast('This report is now Private.', 'success');
            loadAllData();
        } else {
            showToast('Failed to make monitor private', 'error');
        }
    } catch (error) {
        console.error('Error making monitor private:', error);
        showToast('Error making monitor private', 'error');
    }
}

async function setMaintenanceMode(type, id) {
    try {
        const endpoint = `/api/maintenance/${encodeURIComponent(type)}/${id}/start`;
        const res = await apiRequest(endpoint, {
            method: 'POST'
        });

        if (res.ok) {
            showToast('Maintenance mode enabled', 'success');
            await loadAllData();
        } else {
            const message = await readApiError(res, 'Failed to enable maintenance mode');
            showToast(message, 'error');
        }
    } catch (error) {
        console.error('Error enabling maintenance mode:', error);
        showToast('Error enabling maintenance mode', 'error');
    }
}

async function endMaintenanceMode(type, id) {
    try {
        const endpoint = `/api/maintenance/${encodeURIComponent(type)}/${id}/end`;
        const res = await apiRequest(endpoint, { method: 'POST' });

        if (res.ok) {
            showToast('Maintenance mode ended', 'success');
            await loadAllData();
        } else {
            const message = await readApiError(res, 'Failed to end maintenance mode');
            showToast(message, 'error');
        }
    } catch (error) {
        console.error('Error ending maintenance mode:', error);
        showToast('Error ending maintenance mode', 'error');
    }
}

function closeAllDropdowns() {
    document.querySelectorAll('.status-dropdown-menu').forEach(menu => {
        menu.classList.remove('show');
        menu.style.display = 'none';
    });
}

function toggleStatusDropdown(event, buttonEl) {
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }
    const container = buttonEl?.closest?.('.status-dropdown');
    if (!container) return;
    const dropdown = container.querySelector('.status-dropdown-menu');
    if (!dropdown) return;

    const isVisible = dropdown.classList.contains('show');
    closeAllDropdowns();

    if (!isVisible) {
        const buttonRect = buttonEl.getBoundingClientRect();
        const dropdownHeight = 220;
        const spaceBelow = window.innerHeight - buttonRect.bottom;
        if (spaceBelow < dropdownHeight) {
            dropdown.style.top = 'auto';
            dropdown.style.bottom = '100%';
            dropdown.style.marginBottom = '4px';
            dropdown.style.marginTop = '0';
        } else {
            dropdown.style.top = '100%';
            dropdown.style.bottom = 'auto';
            dropdown.style.marginTop = '4px';
            dropdown.style.marginBottom = '0';
        }
        dropdown.style.display = 'block';
        dropdown.classList.add('show');
        dropdown.onclick = (e) => e.stopPropagation();
    }
}

/**
 * Close dropdowns when clicking outside
 */
document.addEventListener('click', (e) => {
    // Don't close if click was inside a status dropdown, dropdown-toggle, or dropdown-menu
    if (e.target.closest('.status-dropdown') ||
        e.target.closest('.dropdown-toggle') || e.target.closest('.dropdown-menu')) return;
    closeAllDropdowns();
});

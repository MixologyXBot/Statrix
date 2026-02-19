// This file is a part of Statrix
// Coding : Priyanshu Dey [@HellFireDevil18]

const API_BASE = window.location.origin;

async function fetchStatus() {
    try {
        const tzOffsetMinutes = -new Date().getTimezoneOffset();
        const response = await fetch(`${API_BASE}/api/public/status?tz_offset_minutes=${encodeURIComponent(String(tzOffsetMinutes))}`);
        if (!response.ok) {
            throw new Error('Failed to fetch status');
        }
        return await response.json();
    } catch (error) {
        console.error('Error fetching status:', error);
        return null;
    }
}

function updateOverallStatus(data) {
    const statusBadge = document.getElementById('overall-status');
    const statusText = statusBadge.querySelector('.status-text');
    const uptimeValue = document.getElementById('overall-uptime');

    if (!data) {
        statusText.textContent = 'Error';
        statusBadge.className = 'status-badge status-down';
        return;
    }

    uptimeValue.textContent = (data.overall_uptime != null)
        ? `${data.overall_uptime.toFixed(4)}%`
        : 'N/A';

    if (data.status === 'operational') {
        statusBadge.className = 'status-badge status-up';
        statusText.textContent = 'All Systems Operational';
    } else if (data.status === 'degraded') {
        statusBadge.className = 'status-badge status-degraded';
        statusText.textContent = 'Some Issues';
    } else {
        statusBadge.className = 'status-badge status-down';
        statusText.textContent = 'System Outage';
    }
}

function updateIncidentsBanner(data) {
    const banner = document.getElementById('incidents-banner');
    const description = document.getElementById('incident-description');
    if (!data) return;

    if (data.incidents && data.incidents.length > 0) {
        banner.style.display = 'block';
        description.textContent = `${data.incidents.length} active incident${data.incidents.length > 1 ? 's' : ''}`;
    } else {
        banner.style.display = 'none';
    }
}

function generateUptimeBar(uptimePercentage) {
    // Generate 30 segments (each representing ~3 days)
    const segments = 30;
    const upSegments = Math.round((uptimePercentage / 100) * segments);

    let html = '<div class="uptime-bar">';
    for (let i = 0; i < segments; i++) {
        const status = i < upSegments ? 'up' : 'down';
        html += `<div class="uptime-bar-segment ${status}"></div>`;
    }
    html += '</div>';

    return html;
}

function updateMonitorsList(data) {
    const container = document.getElementById('monitors-container');

    if (!data || !data.monitors || data.monitors.length === 0) {
        container.innerHTML = '<p class="no-data" style="text-align: center; padding: 2rem; color: var(--text-muted);">No monitors configured</p>';
        return;
    }

    const groups = {};
    const uncategorized = [];

    for (const monitor of data.monitors) {
        if (monitor.category) {
            if (!groups[monitor.category]) {
                groups[monitor.category] = [];
            }
            groups[monitor.category].push(monitor);
        } else {
            uncategorized.push(monitor);
        }
    }

    let html = '';

    for (const [category, monitors] of Object.entries(groups)) {
        html += `
            <div class="monitor-group">
                <div class="monitor-group-header">${category}</div>
        `;

        for (const monitor of monitors) {
            html += renderMonitorItem(monitor);
        }

        html += '</div>';
    }

    if (uncategorized.length > 0) {
        html += '<div class="monitor-group">';
        for (const monitor of uncategorized) {
            html += renderMonitorItem(monitor);
        }
        html += '</div>';
    }

    container.innerHTML = html;
}

function renderMonitorItem(monitor) {
    const statusClass = monitor.status === 'up' ? 'up' : monitor.status === 'down' ? 'down' : 'unknown';
    const typeLabel = getTypeLabel(monitor);
    const uptimeDisplay = (monitor.uptime_percentage !== undefined && monitor.uptime_percentage !== null)
        ? `${monitor.uptime_percentage.toFixed(4)}%`
        : 'N/A';

    return `
        <div class="monitor-item">
            <div class="monitor-info">
                <span class="monitor-status ${statusClass}"></span>
                <span class="monitor-name">${monitor.name}</span>
                ${typeLabel ? `<span class="monitor-type">${typeLabel}</span>` : ''}
            </div>
            <div class="monitor-stats">
                ${(monitor.uptime_percentage !== undefined && monitor.uptime_percentage !== null) ? generateUptimeBar(monitor.uptime_percentage) : ''}
                <span class="uptime-percentage">${uptimeDisplay}</span>
            </div>
        </div>
    `;
}

function getTypeLabel(monitor) {
    if (!monitor) return '';
    if (monitor.type === 'uptime') return 'Website Monitor';
    if (monitor.type === 'heartbeat') {
        const hbType = monitor.heartbeat_type || 'cronjob';
        return hbType === 'server_agent' ? 'Heartbeat (Server Agent)' : 'Heartbeat (Cronjob)';
    }
    return '';
}

async function loadStatus() {
    const data = await fetchStatus();

    updateOverallStatus(data);
    updateIncidentsBanner(data);
    updateMonitorsList(data);
}

document.addEventListener('DOMContentLoaded', () => {
    loadStatus();

    setInterval(loadStatus, 60000);
});

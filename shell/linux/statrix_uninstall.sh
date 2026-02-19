#!/bin/bash
#
# Statrix Server Monitoring Agent - Linux Uninstall Script
# Copyright 2015 - 2026 @ HellFireDevil18
# Part of the Statrix monitoring platform by HellFireDevil18
# Original uninstall concept by HetrixTools - Modified for Statrix
#

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

SID="$1"
STATRIX_ENDPOINT=""

echo "Checking root privileges..."
if [ "$EUID" -ne 0 ]; then
    echo "ERROR: Please run the uninstall script as root."
    exit 1
fi
echo "... done."

if [ -f /etc/statrix/statrix.cfg ]; then
    if [ -z "$SID" ]; then
        SID=$(grep -E '^SID="' /etc/statrix/statrix.cfg | awk -F'"' '{print $2}')
    fi
    STATRIX_ENDPOINT=$(grep -E '^STATRIX_ENDPOINT="' /etc/statrix/statrix.cfg | awk -F'"' '{print $2}')
fi

echo "Stopping any running Statrix agents..."
pkill -9 -f statrix_agent.sh 2>/dev/null || true
echo "... done."

echo "Removing any old Statrix cron entries..."
if command -v crontab >/dev/null 2>&1; then
    crontab -u root -l 2>/dev/null | grep -v 'statrix_agent.sh' | crontab -u root - >/dev/null 2>&1 || true
    if id -u statrix >/dev/null 2>&1; then
        crontab -u statrix -l 2>/dev/null | grep -v 'statrix_agent.sh' | crontab -u statrix - >/dev/null 2>&1 || true
    fi
fi
echo "... done."

echo "Removing any old Statrix systemd units..."
if command -v systemctl >/dev/null 2>&1; then
    systemctl stop statrix_agent.timer >/dev/null 2>&1 || true
    systemctl disable statrix_agent.timer >/dev/null 2>&1 || true
    systemctl stop statrix_agent.service >/dev/null 2>&1 || true
    systemctl disable statrix_agent.service >/dev/null 2>&1 || true
    systemctl daemon-reload >/dev/null 2>&1 || true
fi
rm -f /etc/systemd/system/statrix_agent.timer >/dev/null 2>&1 || true
rm -f /etc/systemd/system/statrix_agent.service >/dev/null 2>&1 || true
echo "... done."

echo "Removing Statrix files..."
rm -rf /etc/statrix >/dev/null 2>&1 || true
echo "... done."

echo "Removing Statrix user..."
if id -u statrix >/dev/null 2>&1; then
    userdel statrix >/dev/null 2>&1 || true
fi
echo "... done."

if [ -n "$SID" ] && [ -n "$STATRIX_ENDPOINT" ]; then
    echo "Sending uninstall notice to Statrix..."
    POST_DATA="v=uninstall&s=$SID"
    if command -v wget >/dev/null 2>&1; then
        wget -t 1 -T 15 -qO- --post-data "$POST_DATA" "$STATRIX_ENDPOINT/" >/dev/null 2>&1 || true
    elif command -v curl >/dev/null 2>&1; then
        curl -s --max-time 15 --data "$POST_DATA" "$STATRIX_ENDPOINT/" >/dev/null 2>&1 || true
    fi
    echo "... done."
fi

echo "Statrix agent uninstallation completed."

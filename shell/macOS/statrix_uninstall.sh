#!/bin/bash
#
#
#	Statrix Server Monitoring Agent - macOS Uninstall Script
#	Copyright 2015 - 2026 @  HellFireDevil18
#	Part of the Statrix monitoring platform by HellFireDevil18
#	Original uninstall concept by HetrixTools - Modified for Statrix
#
#
#		DISCLAIMER OF WARRANTY
#
#	The Software is provided "AS IS" and "WITH ALL FAULTS," without warranty of any kind, 
#	including without limitation the warranties of merchantability, fitness for a particular purpose and non-infringement. 
#	HellFireDevil18 makes no warranty that the Software is free of defects or is suitable for any particular purpose. 
#	In no event shall HellFireDevil18 be responsible for loss or damages arising from the installation or use of the Software, 
#	including but not limited to any indirect, punitive, special, incidental or consequential damages of any character including, 
#	without limitation, damages for loss of goodwill, work stoppage, computer failure or malfunction, or any and all other commercial damages or losses. 
#	The entire risk as to the quality and performance of the Software is borne by you, the user.
#
#		END OF DISCLAIMER OF WARRANTY

# Set PATH
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/homebrew/bin:/opt/homebrew/sbin

# Check if uninstall script is run by root
echo "Checking root privileges..."
if [ "$EUID" -ne 0 ]
	then echo "ERROR: Please run the uninstall script as root."
	exit 1
fi
echo "... done."

# Check if this is macOS
echo "Checking operating system..."
if [ "$(uname)" != "Darwin" ]
	then echo "ERROR: This uninstall script is for macOS only."
	exit 1
fi
echo "... done."

# Fetch Server Unique ID (optional — used to notify the platform)
SID=$1
STATRIX_ENDPOINT=""
if [ -f /opt/statrix/statrix.cfg ]
then
	STATRIX_ENDPOINT=$(grep 'STATRIX_ENDPOINT="' /opt/statrix/statrix.cfg | awk -F'"' '{ print $2 }')
	if [ -z "$SID" ]
	then
		SID=$(grep 'SID="' /opt/statrix/statrix.cfg | awk -F'"' '{ print $2 }')
	fi
fi

# Removing launchd job (if exists)
echo "Removing Statrix launchd job, if exists..."
PLIST="/Library/LaunchDaemons/com.statrix.agent.plist"
if [ -f "$PLIST" ]
then
	launchctl unload "$PLIST" 2>/dev/null
	rm -f "$PLIST"
	echo "Launchd job removed."
else
	echo "No launchd job found."
fi
echo "... done."

# Killing any running Statrix agents
echo "Killing any Statrix agent scripts that may be currently running..."
pkill -9 -f statrix_agent.sh 2>/dev/null
pkill -9 -f run_agent.sh 2>/dev/null
echo "... done."

# Checking if _statrix user exists
echo "Checking if _statrix user exists..."
if id -u _statrix >/dev/null 2>&1
then
	echo "The _statrix user exists, killing its processes..."
	pkill -9 -u _statrix 2>/dev/null
	echo "Deleting _statrix user..."
	dscl . -delete /Users/_statrix 2>/dev/null
	echo "User deleted."
else
	echo "The _statrix user doesn't exist..."
fi
echo "... done."

# Remove agent folder
echo "Checking if Statrix agent folder exists..."
if [ -d /opt/statrix ]
then
	echo "Statrix agent folder found, deleting it..."
	rm -rf /opt/statrix
else
	echo "No Statrix agent folder found..."
fi
echo "... done."

# Removing old crontab entry (if any legacy entry exists)
echo "Removing any old Statrix crontab entry, if exists..."
crontab -l 2>/dev/null | grep -v 'statrix' | crontab - 2>/dev/null
echo "... done."

# Cleaning up uninstall file
echo "Cleaning up the uninstall file..."
if [ -f "$0" ]
then
	rm -f "$0"
fi
echo "... done."

# Let Statrix platform know uninstall has been completed
if [ -n "$SID" ]
then
	echo "Letting Statrix platform know the uninstallation has been completed..."
	POST="v=uninstall&s=$SID"
	if [ -n "$STATRIX_ENDPOINT" ]
	then
		curl -s --retry 3 --retry-delay 1 --max-time 15 --data "$POST" "$STATRIX_ENDPOINT/" > /dev/null 2>&1
	fi
	echo "... done."
fi

# All done
echo "Statrix agent uninstallation completed."

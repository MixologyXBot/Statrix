#!/bin/bash
#
#
#	Statrix Server Monitoring Agent - macOS Update Script
#	Copyright 2015 - 2026 @  HellFireDevil18
#	Part of the Statrix monitoring platform by HellFireDevil18
#	Original update concept by HetrixTools - Modified for Statrix
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

# Old Agent Path
AGENT="/opt/statrix/statrix_agent.sh"

# Old Config Path
CONFIG="/opt/statrix/statrix.cfg"

# Check if update script is run by root
echo "Checking root privileges..."
if [ "$EUID" -ne 0 ]
	then echo "ERROR: Please run the update script as root."
	exit 1
fi
echo "... done."

# Check if this is macOS
echo "Checking operating system..."
if [ "$(uname)" != "Darwin" ]
	then echo "ERROR: This update script is for macOS only."
	exit 1
fi
echo "... done."

# Look for the old agent
echo "Looking for the old agent..."
if [ -f "$AGENT" ]
then
	echo "... done."
else
	echo "ERROR: No old agent found. Nothing to update."
	exit 1
fi

# Look for the old config
echo "Looking for the old config file..."
if [ -f "$CONFIG" ]
then
	echo "... done."
	EXTRACT=$CONFIG
else
	echo "ERROR: No config file found."
	exit 1
fi

# Extract data from the old config
echo "Extracting configs from the old agent..."
# SID (Server ID)
SID=$(grep 'SID="' "$EXTRACT" | awk -F'"' '{ print $2 }')
# Statrix Endpoint
STATRIX_ENDPOINT=$(grep 'STATRIX_ENDPOINT="' "$EXTRACT" | awk -F'"' '{ print $2 }')
# Network Interfaces
NetworkInterfaces=$(grep 'NetworkInterfaces="' "$EXTRACT" | awk -F'"' '{ print $2 }')
# Check Services
CheckServices=$(grep 'CheckServices="' "$EXTRACT" | awk -F'"' '{ print $2 }')
# Check Software RAID Health
CheckSoftRAID=$(grep 'CheckSoftRAID=' "$EXTRACT" | awk -F'=' '{ print $2 }')
# Check Drive Health
CheckDriveHealth=$(grep 'CheckDriveHealth=' "$EXTRACT" | awk -F'=' '{ print $2 }')
# RunningProcesses
RunningProcesses=$(grep 'RunningProcesses=' "$EXTRACT" | awk -F'=' '{ print $2 }')
if [ -z "$RunningProcesses" ]; then RunningProcesses=0; fi
# Port Connections
ConnectionPorts=$(grep 'ConnectionPorts="' "$EXTRACT" | awk -F'"' '{ print $2 }')
# Secured Connection
SecuredConnection=$(grep 'SecuredConnection=' "$EXTRACT" | awk -F'=' '{ print $2 }')
# CollectEveryXSeconds
CollectEveryXSeconds=$(grep 'CollectEveryXSeconds=' "$EXTRACT" | awk -F'=' '{ print $2 }')
# OutgoingPings
OutgoingPings=$(grep 'OutgoingPings="' "$EXTRACT" | awk -F'"' '{ print $2 }')
# OutgoingPingsCount
OutgoingPingsCount=$(grep 'OutgoingPingsCount=' "$EXTRACT" | awk -F'=' '{ print $2 }')
# DEBUG
DEBUG=$(grep 'DEBUG=' "$EXTRACT" | awk -F'=' '{ print $2 }')
echo "... done."

if [ -z "$STATRIX_ENDPOINT" ]
then
	echo "ERROR: Missing STATRIX_ENDPOINT in existing config."
	exit 1
fi

# Fetching the new agent
echo "Fetching the new agent..."
if ! curl -sf -o "$AGENT" "$STATRIX_ENDPOINT/shell/macOS/statrix_agent.sh"
then
	echo "ERROR: Failed to download the agent script."
	exit 1
fi
echo "... done."

# Fetching the new config file
echo "Fetching the new config file..."
if ! curl -sf -o "$CONFIG" "$STATRIX_ENDPOINT/shell/macOS/statrix.cfg"
then
	echo "ERROR: Failed to download the agent configuration."
	exit 1
fi
echo "... done."

# Fetching the new wrapper script
echo "Fetching the wrapper script..."
if ! curl -sf -o /opt/statrix/run_agent.sh "$STATRIX_ENDPOINT/shell/macOS/run_agent.sh"
then
	echo "WARNING: Failed to download the wrapper script. Existing wrapper retained."
fi
echo "... done."

# Setting permissions
echo "Setting permissions..."
chmod +x /opt/statrix/statrix_agent.sh
chmod +x /opt/statrix/run_agent.sh
chmod 600 /opt/statrix/statrix.cfg
echo "... done."

# Restore endpoint
echo "Restoring Statrix endpoint..."
sed -i '' "s|STATRIX_ENDPOINT=\"\"|STATRIX_ENDPOINT=\"$STATRIX_ENDPOINT\"|" "$CONFIG"
echo "... done."

# Inserting Server ID (SID) into the agent config
echo "Inserting Server ID (SID) into agent config..."
sed -i '' "s/SID=\"\"/SID=\"$SID\"/" "$CONFIG"
echo "... done."

# Restore network interfaces
echo "Checking if any network interfaces are specified..."
if [ -n "$NetworkInterfaces" ]
then
	echo "Network interfaces found, inserting them into the agent config..."
	sed -i '' "s/NetworkInterfaces=\"\"/NetworkInterfaces=\"$NetworkInterfaces\"/" "$CONFIG"
fi
echo "... done."

# Restore services
echo "Checking if any services should be monitored..."
if [ -n "$CheckServices" ]
then
	echo "Services found, inserting them into the agent config..."
	sed -i '' "s/CheckServices=\"\"/CheckServices=\"$CheckServices\"/" "$CONFIG"
fi
echo "... done."

# Restore software RAID
echo "Checking if software RAID should be monitored..."
if [ "$CheckSoftRAID" = "1" ]
then
	echo "Enabling software RAID monitoring in the agent config..."
	sed -i '' "s/CheckSoftRAID=0/CheckSoftRAID=1/" "$CONFIG"
fi
echo "... done."

# Restore Drive Health
echo "Checking if Drive Health should be monitored..."
if [ "$CheckDriveHealth" = "1" ]
then
	echo "Enabling Drive Health monitoring in the agent config..."
	sed -i '' "s/CheckDriveHealth=0/CheckDriveHealth=1/" "$CONFIG"
fi
echo "... done."

# Restore running processes
echo "Checking if 'View running processes' should be enabled..."
if [ "$RunningProcesses" = "1" ]
then
	echo "Enabling 'View running processes' in the agent config..."
	sed -i '' "s/RunningProcesses=0/RunningProcesses=1/" "$CONFIG"
fi
echo "... done."

# Restore port connections
echo "Checking if any ports to monitor number of connections on..."
if [ -n "$ConnectionPorts" ]
then
	echo "Ports found, inserting them into the agent config..."
	sed -i '' "s/ConnectionPorts=\"\"/ConnectionPorts=\"$ConnectionPorts\"/" "$CONFIG"
fi
echo "... done."

# Restore secured connection
echo "Checking if secured connection is enabled..."
if [ -n "$SecuredConnection" ]
then
	echo "Inserting secured connection in the agent config..."
	sed -i '' "s/SecuredConnection=1/SecuredConnection=$SecuredConnection/" "$CONFIG"
fi
echo "... done."

# Restore CollectEveryXSeconds
echo "Checking CollectEveryXSeconds..."
if [ -n "$CollectEveryXSeconds" ]
then
	echo "Inserting CollectEveryXSeconds in the agent config..."
	sed -i '' "s/CollectEveryXSeconds=3/CollectEveryXSeconds=$CollectEveryXSeconds/" "$CONFIG"
fi
echo "... done."

# Restore OutgoingPings
echo "Checking OutgoingPings..."
if [ -n "$OutgoingPings" ]
then
	echo "Inserting OutgoingPings in the agent config..."
	sed -i '' "s/OutgoingPings=\"\"/OutgoingPings=\"$OutgoingPings\"/" "$CONFIG"
fi
echo "... done."

# Restore OutgoingPingsCount
echo "Checking OutgoingPingsCount..."
if [ -n "$OutgoingPingsCount" ]
then
	echo "Inserting OutgoingPingsCount in the agent config..."
	sed -i '' "s/OutgoingPingsCount=20/OutgoingPingsCount=$OutgoingPingsCount/" "$CONFIG"
fi
echo "... done."

# Restore DEBUG
echo "Checking DEBUG..."
if [ "$DEBUG" = "1" ]
then
	echo "Restoring DEBUG mode..."
	sed -i '' "s/DEBUG=0/DEBUG=1/" "$CONFIG"
fi
echo "... done."

# Refresh launchd job
echo "Refreshing launchd job..."
PLIST="/Library/LaunchDaemons/com.statrix.agent.plist"
if [ -f "$PLIST" ]
then
	# Determine current user from existing plist
	AGENT_USER=$(defaults read "$PLIST" UserName 2>/dev/null || echo "root")
	launchctl unload "$PLIST" 2>/dev/null
else
	AGENT_USER="root"
fi
cat > "$PLIST" << PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
	<plist version="1.0">
<dict>
	<key>Label</key>
	<string>com.statrix.agent</string>
	<key>ProgramArguments</key>
	<array>
		<string>/bin/bash</string>
		<string>/opt/statrix/run_agent.sh</string>
	</array>
	<key>WorkingDirectory</key>
	<string>/opt/statrix</string>
	<key>UserName</key>
	<string>${AGENT_USER}</string>
	<key>StartCalendarInterval</key>
	<dict>
		<key>Second</key>
		<integer>0</integer>
	</dict>
	<key>RunAtLoad</key>
	<true/>
	<key>AbandonProcessGroup</key>
	<true/>
</dict>
</plist>
PLIST_EOF
launchctl load "$PLIST" 2>/dev/null
echo "... done."

# Killing any running Statrix agents
echo "Making sure no Statrix agent scripts are currently running..."
pkill -f statrix_agent.sh 2>/dev/null
echo "... done."

# Assign permissions
echo "Assigning permissions..."
if id -u _statrix >/dev/null 2>&1 && [ "$AGENT_USER" = "_statrix" ]
then
	chown -R _statrix:staff /opt/statrix
	chmod -R 700 /opt/statrix
else
	chown -R root:wheel /opt/statrix
	chmod -R 700 /opt/statrix
fi
echo "... done."

# Cleaning up update file
echo "Cleaning up the update file..."
if [ -f "$0" ]
then
	rm -f "$0"
fi
echo "... done."

# All done
echo "Statrix agent update completed. It can take up to two (2) minutes for new data to be collected."

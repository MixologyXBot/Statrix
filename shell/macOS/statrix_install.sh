#!/bin/bash
#
#
#	Statrix Server Monitoring Agent - macOS Install Script
#	Copyright 2015 - 2026 @  HellFireDevil18
#	Part of the Statrix monitoring platform by HellFireDevil18
#	Original install concept by HetrixTools - Modified for Statrix
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

# Accept parameters: STATRIX_ENDPOINT, SID, USER, SERVICES, RAID, DRIVE, PROCESSES, PORTS
STATRIX_ENDPOINT="${1%/}"
SID=$2

# Check if install script is run by root
echo "Checking root privileges..."
if [ "$EUID" -ne 0 ]
	then echo "ERROR: Please run the install script as root."
	exit 1
fi
echo "... done."

# Check if this is macOS
echo "Checking operating system..."
if [ "$(uname)" != "Darwin" ]
	then echo "ERROR: This installer is for macOS only."
	exit 1
fi
echo "... done."

# Make sure STATRIX_ENDPOINT is not empty
echo "Checking Statrix Endpoint..."
if [ -z "$STATRIX_ENDPOINT" ]
	then echo "ERROR: First parameter (STATRIX_ENDPOINT) missing."
	exit 1
fi
echo "... done."

# Check if endpoint is reachable
echo "Checking endpoint availability..."
if ! curl -sf --head "$STATRIX_ENDPOINT/shell/macOS/statrix_agent.sh" > /dev/null 2>&1
then
	echo "ERROR: Could not reach $STATRIX_ENDPOINT/shell/macOS/statrix_agent.sh"
	exit 1
fi
echo "... done."

# Make sure SID is not empty
echo "Checking Server ID (SID)..."
if [ -z "$SID" ]
	then echo "ERROR: Second parameter (SID) missing."
	exit 1
fi
echo "... done."

# Check if user has selected to run agent as 'root' or not
if [ -z "$3" ]
	then echo "ERROR: Third parameter (user type) missing. Use '1' for root or '0' for statrix user."
	exit 1
fi

# Check for required system utilities
echo "Checking system utilities..."
for cmd in curl top vm_stat sysctl netstat df ifconfig; do
	command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: $cmd is required to run this agent." >&2; exit 1; }
done
echo "... done."

# Remove old agent (if exists)
echo "Checking if there's any old Statrix agent already installed..."
if [ -d /opt/statrix ]
then
	echo "Old Statrix agent found, deleting it..."
	rm -rf /opt/statrix
else
	echo "No old Statrix agent found..."
fi
echo "... done."

# Creating agent folder
echo "Creating the Statrix agent folder..."
mkdir -p /opt/statrix
echo "... done."

# Fetching the agent
echo "Fetching the agent..."
if ! curl -sf -o /opt/statrix/statrix_agent.sh "$STATRIX_ENDPOINT/shell/macOS/statrix_agent.sh"
then
	echo "ERROR: Failed to download the agent script."
	exit 1
fi
echo "... done."

# Fetching the config file
echo "Fetching the config file..."
if ! curl -sf -o /opt/statrix/statrix.cfg "$STATRIX_ENDPOINT/shell/macOS/statrix.cfg"
then
	echo "ERROR: Failed to download the agent configuration."
	exit 1
fi
echo "... done."

# Fetching the wrapper script
echo "Fetching the wrapper script..."
if ! curl -sf -o /opt/statrix/run_agent.sh "$STATRIX_ENDPOINT/shell/macOS/run_agent.sh"
then
	echo "ERROR: Failed to download the wrapper script."
	exit 1
fi
echo "... done."

# Setting permissions
echo "Setting permissions..."
chmod +x /opt/statrix/statrix_agent.sh
chmod +x /opt/statrix/run_agent.sh
chmod 600 /opt/statrix/statrix.cfg
echo "... done."

# Inserting Statrix Endpoint into the agent config
echo "Inserting Statrix Endpoint into agent config..."
sed -i '' "s|STATRIX_ENDPOINT=\"\"|STATRIX_ENDPOINT=\"$STATRIX_ENDPOINT\"|" /opt/statrix/statrix.cfg
echo "... done."

# Inserting Server ID (SID) into the agent config
echo "Inserting Server ID (SID) into agent config..."
sed -i '' "s/SID=\"\"/SID=\"$SID\"/" /opt/statrix/statrix.cfg
echo "... done."

# Check if any services are to be monitored
echo "Checking if any services should be monitored..."
if [ "$4" != "0" ]
then
	echo "Services found, inserting them into the agent config..."
	sed -i '' "s/CheckServices=\"\"/CheckServices=\"$4\"/" /opt/statrix/statrix.cfg
fi
echo "... done."

# Check if software RAID should be monitored
echo "Checking if software RAID should be monitored..."
if [ "$5" -eq "1" ] 2>/dev/null
then
	echo "Enabling software RAID monitoring in the agent config..."
	sed -i '' "s/CheckSoftRAID=0/CheckSoftRAID=1/" /opt/statrix/statrix.cfg
fi
echo "... done."

# Check if Drive Health should be monitored
echo "Checking if Drive Health should be monitored..."
if [ "$6" -eq "1" ] 2>/dev/null
then
	echo "Enabling Drive Health monitoring in the agent config..."
	sed -i '' "s/CheckDriveHealth=0/CheckDriveHealth=1/" /opt/statrix/statrix.cfg
fi
echo "... done."

# Check if 'View running processes' should be enabled
echo "Checking if 'View running processes' should be enabled..."
if [ "$7" -eq "1" ] 2>/dev/null
then
	echo "Enabling 'View running processes' in the agent config..."
	sed -i '' "s/RunningProcesses=0/RunningProcesses=1/" /opt/statrix/statrix.cfg
fi
echo "... done."

# Check if any ports to monitor number of connections on
echo "Checking if any ports to monitor number of connections on..."
if [ "$8" != "0" ]
then
	echo "Ports found, inserting them into the agent config..."
	sed -i '' "s/ConnectionPorts=\"\"/ConnectionPorts=\"$8\"/" /opt/statrix/statrix.cfg
fi
echo "... done."

# Killing any running Statrix agents
echo "Making sure no Statrix agent scripts are currently running..."
pkill -f statrix_agent.sh 2>/dev/null
echo "... done."

# Checking if _statrix user exists (macOS uses underscore prefix for service accounts)
echo "Checking if _statrix user already exists..."
if id -u _statrix >/dev/null 2>&1
then
	echo "The _statrix user already exists, killing its processes..."
	pkill -9 -u _statrix 2>/dev/null
	echo "Deleting _statrix user..."
	dscl . -delete /Users/_statrix 2>/dev/null
fi
if [ "$3" -ne "1" ] 2>/dev/null
then
	echo "Creating the _statrix user..."
	# Find an available UID in the service account range (400-499)
	HTUID=400
	while dscl . -list /Users UniqueID 2>/dev/null | awk '{print $2}' | grep -q "^${HTUID}$"; do
		HTUID=$((HTUID + 1))
	done
	dscl . -create /Users/_statrix
	dscl . -create /Users/_statrix UniqueID "$HTUID"
	dscl . -create /Users/_statrix PrimaryGroupID 20
	dscl . -create /Users/_statrix UserShell /usr/bin/false
	dscl . -create /Users/_statrix NFSHomeDirectory /opt/statrix
	dscl . -create /Users/_statrix RealName "Statrix Agent"
	# Hide the user from the login window
	dscl . -create /Users/_statrix IsHidden 1
	echo "Assigning permissions for the _statrix user..."
	chown -R _statrix:staff /opt/statrix
	chmod -R 700 /opt/statrix
else
	echo "Agent will run as 'root' user..."
	chown -R root:wheel /opt/statrix
	chmod -R 700 /opt/statrix
fi
echo "... done."

# Removing old launchd job (if exists)
echo "Removing any old Statrix launchd job, if exists..."
if launchctl list 2>/dev/null | grep -q "com.statrix.agent"
then
	launchctl unload /Library/LaunchDaemons/com.statrix.agent.plist 2>/dev/null
fi
rm -f /Library/LaunchDaemons/com.statrix.agent.plist 2>/dev/null
echo "... done."

# Removing old crontab entry (if exists)
echo "Removing any old Statrix crontab entry, if exists..."
crontab -l 2>/dev/null | grep -v 'statrix' | crontab - 2>/dev/null
echo "... done."

# Setting up the launchd job to run the agent every minute
echo "Setting up the launchd job..."
if [ "$3" -eq "1" ] 2>/dev/null
then
	AGENT_USER="root"
else
	AGENT_USER="_statrix"
fi
cat > /Library/LaunchDaemons/com.statrix.agent.plist << PLIST_EOF
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
launchctl load /Library/LaunchDaemons/com.statrix.agent.plist 2>/dev/null
echo "... done."

# Cleaning up install file
echo "Cleaning up the installation file..."
if [ -f "$0" ]
then
	rm -f "$0"
fi
echo "... done."

# Let Statrix platform know install has been completed
echo "Letting Statrix platform know the installation has been completed..."
POST="v=install&s=$SID"
curl -s --retry 3 --retry-delay 1 --max-time 15 --data "$POST" "$STATRIX_ENDPOINT/" > /dev/null 2>&1
echo "... done."

# Start the agent
if [ "$3" -eq "1" ] 2>/dev/null
then
	echo "Starting the agent under the 'root' user..."
	bash /opt/statrix/statrix_agent.sh > /dev/null 2>&1 &
else
	echo "Starting the agent under the '_statrix' user..."
	sudo -u _statrix bash /opt/statrix/statrix_agent.sh > /dev/null 2>&1 &
fi
echo "... done."

# All done
echo "Statrix agent installation completed."

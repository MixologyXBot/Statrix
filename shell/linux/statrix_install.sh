#!/bin/bash
#
#
#	Statrix Server Monitoring Agent - Install Script
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
#

# Set PATH
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Accept parameters: STATRIX_ENDPOINT, SID, USER, SERVICES, RAID, DRIVE, PROCESSES, PORTS
# Usage: bash <(curl -s https://your-status-domain.tld/shell/linux/statrix_install.sh) STATRIX_ENDPOINT SID [root|statrix] [services] [raid] [drive] [processes] [ports]

# Statrix Endpoint
STATRIX_ENDPOINT=$1

# Fetch Server Unique ID
SID=$2

# Check if install script is run by root
echo "Checking root privileges..."
if [ "$EUID" -ne 0 ]
	then echo "ERROR: Please run the install script as root."
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

# Make sure SID is not empty
echo "Checking Server ID (SID)..."
if [ -z "$SID" ]
	then echo "ERROR: Second parameter (SID) missing."
	exit 1
fi
echo "... done."

# Check if user has selected to run agent as 'root' or as 'statrix' user
if [ -z "$3" ]
	then echo "ERROR: Third parameter (user type) missing. Use '1' for root or '0' for statrix user."
	exit 1
fi

# Check for wget and cron/systemd availability
echo "Checking system utilities..."
command -v wget >/dev/null 2>&1 || { echo "ERROR: wget is required to run this agent." >&2; exit 1; }
USE_CRON=0
USE_SYSTEMD=0
SYSTEMCTL_AVAILABLE=0
EXISTING_CRON=0
CRON_ACTIVE=0
if command -v systemctl >/dev/null 2>&1; then
	if [ -d /run/systemd/system ] || systemctl list-units >/dev/null 2>&1; then
		SYSTEMCTL_AVAILABLE=1
	fi
fi
if command -v crontab >/dev/null 2>&1; then
	if crontab -u root -l 2>/dev/null | grep -q 'statrix_agent.sh'; then
		EXISTING_CRON=1
	elif id -u statrix >/dev/null 2>&1 && crontab -u statrix -l 2>/dev/null | grep -q 'statrix_agent.sh'; then
		EXISTING_CRON=1
	fi
	CRON_ACTIVE=$EXISTING_CRON
	if command -v pgrep >/dev/null 2>&1 && [ "$CRON_ACTIVE" -ne 1 ]; then
		for cron_process in cron crond cronie systemd-cron fcron busybox-cron busybox-crond; do
			if pgrep -x "$cron_process" >/dev/null 2>&1 || pgrep -f "$cron_process" >/dev/null 2>&1; then
				CRON_ACTIVE=1
				break
			fi
		done
	fi
	if [ "$CRON_ACTIVE" -ne 1 ] && [ "$SYSTEMCTL_AVAILABLE" -eq 1 ]; then
		for cron_service in cron crond cronie systemd-cron fcron busybox-cron busybox-crond; do
			if systemctl is-active --quiet "$cron_service"; then
				CRON_ACTIVE=1
				break
			fi
		done
		if [ "$CRON_ACTIVE" -ne 1 ]; then
			if systemctl list-units --type=service --state=active 2>/dev/null | grep -Ei '\bcron(ie)?\b' >/dev/null 2>&1; then
				CRON_ACTIVE=1
			fi
		fi
	fi
	if [ "$CRON_ACTIVE" -ne 1 ] && [ "$SYSTEMCTL_AVAILABLE" -ne 1 ]; then
		CRON_ACTIVE=1
	fi
	if [ "$CRON_ACTIVE" -eq 1 ]; then
		USE_CRON=1
	fi
fi
if [ "$USE_CRON" -ne 1 ] && [ "$SYSTEMCTL_AVAILABLE" -eq 1 ]; then
	USE_SYSTEMD=1
fi
if [ "$USE_CRON" -ne 1 ] && [ "$USE_SYSTEMD" -ne 1 ]; then
	echo "ERROR: Neither cron nor systemd is available to schedule the agent." >&2
	exit 1
fi
echo "... done."

# Remove old agent (if exists)
echo "Checking if there's any old statrix agent already installed..."
if [ -d /etc/statrix ]
then
	echo "Old statrix agent found, deleting it..."
	rm -rf /etc/statrix
else
	echo "No old statrix agent found..."
fi
echo "... done."

# Creating agent folder
echo "Creating the statrix agent folder..."
mkdir -p /etc/statrix
echo "... done."

# Fetching the agent
echo "Fetching the agent..."
if ! wget -t 1 -T 30 -qO /etc/statrix/statrix_agent.sh "$STATRIX_ENDPOINT/shell/linux/statrix_agent.sh"
then
	echo "ERROR: Failed to download the agent script from $STATRIX_ENDPOINT" >&2
	exit 1
fi
echo "... done."

# Fetching the config file
echo "Fetching the config file..."
if ! wget -t 1 -T 30 -qO /etc/statrix/statrix.cfg "$STATRIX_ENDPOINT/shell/linux/statrix.cfg"
then
	echo "ERROR: Failed to download the agent configuration from $STATRIX_ENDPOINT" >&2
	exit 1
fi
echo "... done."

# Inserting Statrix Endpoint into the agent config
echo "Inserting Statrix Endpoint into agent config..."
sed -i "s|STATRIX_ENDPOINT=\"\"|STATRIX_ENDPOINT=\"$STATRIX_ENDPOINT\"|" /etc/statrix/statrix.cfg
echo "... done."

# Inserting Server ID (SID) into the agent config
echo "Inserting Server ID (SID) into agent config..."
sed -i "s/SID=\"\"/SID=\"$SID\"/" /etc/statrix/statrix.cfg
echo "... done."

# Check if any services are to be monitored
echo "Checking if any services should be monitored..."
if [ "$4" != "0" ]
then
	echo "Services found, inserting them into the agent config..."
	sed -i "s/CheckServices=\"\"/CheckServices=\"$4\"/" /etc/statrix/statrix.cfg
fi
echo "... done."

# Check if software RAID should be monitored
echo "Checking if software RAID should be monitored..."
if [ "$5" -eq "1" ]
then
	echo "Enabling software RAID monitoring in the agent config..."
	sed -i "s/CheckSoftRAID=0/CheckSoftRAID=1/" /etc/statrix/statrix.cfg
fi
echo "... done."

# Check if Drive Health should be monitored
echo "Checking if Drive Health should be monitored..."
if [ "$6" -eq "1" ]
then
	echo "Enabling Drive Health monitoring in the agent config..."
	sed -i "s/CheckDriveHealth=0/CheckDriveHealth=1/" /etc/statrix/statrix.cfg
fi
echo "... done."

# Check if 'View running processes' should be enabled
echo "Checking if 'View running processes' should be enabled..."
if [ "$7" -eq "1" ]
then
	echo "Enabling 'View running processes' in the agent config..."
	sed -i "s/RunningProcesses=0/RunningProcesses=1/" /etc/statrix/statrix.cfg
fi
echo "... done."

# Check if any ports to monitor number of connections on
echo "Checking if any ports to monitor number of connections on..."
if [ "$8" != "0" ]
then
	echo "Ports found, inserting them into the agent config..."
	sed -i "s/ConnectionPorts=\"\"/ConnectionPorts=\"$8\"/" /etc/statrix/statrix.cfg
fi
echo "... done."

# Killing any running statrix agents
echo "Making sure no statrix agent scripts are currently running..."
ps aux | grep -ie statrix_agent.sh | awk '{print $2}' | xargs -r kill -9
echo "... done."

# Checking if statrix user exists
echo "Checking if statrix user already exists..."
if id -u statrix >/dev/null 2>&1
then
	echo "The statrix user already exists, killing its processes..."
	pkill -9 -u `id -u statrix`
	echo "Deleting statrix user..."
	userdel statrix
	echo "Creating the new statrix user..."
	useradd statrix -r -d /etc/statrix -s /bin/false
	echo "Assigning permissions for the statrix user..."
	chown -R statrix:statrix /etc/statrix
	chmod -R 700 /etc/statrix
else
	echo "The statrix user doesn't exist, creating it now..."
	useradd statrix -r -d /etc/statrix -s /bin/false
	echo "Assigning permissions for the statrix user..."
	chown -R statrix:statrix /etc/statrix
	chmod -R 700 /etc/statrix
fi
echo "... done."

# Removing old cronjob (if exists)
echo "Removing any old statrix cronjob, if exists..."
if command -v crontab >/dev/null 2>&1
then
	crontab -u root -l 2>/dev/null | grep -v 'statrix_agent.sh'  | crontab -u root - >/dev/null 2>&1
	crontab -u statrix -l 2>/dev/null | grep -v 'statrix_agent.sh'  | crontab -u statrix - >/dev/null 2>&1
fi
echo "... done."

# Removing old systemd service/timer (if exists)
if [ "$SYSTEMCTL_AVAILABLE" -eq 1 ]; then
	systemctl stop statrix_agent.timer >/dev/null 2>&1
	systemctl disable statrix_agent.timer >/dev/null 2>&1
	systemctl stop statrix_agent.service >/dev/null 2>&1
	systemctl disable statrix_agent.service >/dev/null 2>&1
	systemctl daemon-reload >/dev/null 2>&1
fi
rm -f /etc/systemd/system/statrix_agent.timer >/dev/null 2>&1
rm -f /etc/systemd/system/statrix_agent.service >/dev/null 2>&1

# Setup the new systemd or cronjob timer to run the agent every minute
if [ "$USE_CRON" -eq 1 ]
then
	# Default is running the agent as 'statrix' user, unless chosen otherwise (when $3 = 1, run as root)
	if [ "$3" -eq "1" ]
	then
		echo "Setting up the new cronjob as 'root' user..."
		crontab -u root -l 2>/dev/null | { cat; echo "* * * * * bash /etc/statrix/statrix_agent.sh >> /etc/statrix/statrix_cron.log 2>&1"; } | crontab -u root - >/dev/null 2>&1
	else
		echo "Setting up the new cronjob as 'statrix' user..."
		crontab -u statrix -l 2>/dev/null | { cat; echo "* * * * * bash /etc/statrix/statrix_agent.sh >> /etc/statrix/statrix_cron.log 2>&1"; } | crontab -u statrix - >/dev/null 2>&1
	fi
elif [ "$USE_SYSTEMD" -eq 1 ]
then
	echo "Setting up systemd timer..."
		if [ "$3" -eq "1" ]
		then
		SERVICE_USER=root
		else
		SERVICE_USER=statrix
		fi
	cat > /etc/systemd/system/statrix_agent.service <<EOF
[Unit]
Description=Statrix Agent

[Service]
Type=oneshot
User=$SERVICE_USER
ExecStart=/bin/bash /etc/statrix/statrix_agent.sh
EOF
	cat > /etc/systemd/system/statrix_agent.timer <<EOF
[Unit]
Description=Runs Statrix agent every minute

[Timer]
OnBootSec=1min
OnCalendar=*-*-* *:*:00 UTC
AccuracySec=1s
RandomizedDelaySec=0
Persistent=true
Unit=statrix_agent.service

[Install]
WantedBy=timers.target
EOF
	systemctl daemon-reload >/dev/null 2>&1
	systemctl enable --now statrix_agent.timer >/dev/null 2>&1
	systemctl restart statrix_agent.timer >/dev/null 2>&1
else
	echo "ERROR: Unable to configure scheduling for the agent." >&2
	exit 1
fi
echo "... done."

# Cleaning up install file
echo "Cleaning up the installation file..."
if [ -f $0 ]
then
	rm -f $0
fi
echo "... done."

# Let Statrix platform know install has been completed
echo "Letting Statrix platform know the installation has been completed..."
POST="v=install&s=$SID"
wget -t 1 -T 30 -qO- --post-data "$POST" "$STATRIX_ENDPOINT/" &> /dev/null
echo "... done."

# Start the agent
if [ "$3" -eq "1" ]
then
	echo "Starting the agent under the 'root' user..."
	bash /etc/statrix/statrix_agent.sh > /dev/null 2>&1 &
else
	echo "Starting the agent under the 'statrix' user..."
	sudo -u statrix bash /etc/statrix/statrix_agent.sh > /dev/null 2>&1 &
fi
echo "... done."

# All done
echo "Statrix agent installation completed."

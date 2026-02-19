#!/bin/bash
#
#
#	Statrix Server Monitoring Agent - macOS
#	Copyright 2015 - 2026 @  HellFireDevil18
#	Part of the Statrix monitoring platform by HellFireDevil18
#	Original agent concept by HetrixTools - Modified for Statrix
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

# Set PATH/Locale
export LC_NUMERIC="C"
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/homebrew/bin:/opt/homebrew/sbin
ScriptPath=$(dirname "${BASH_SOURCE[0]}")

# Agent Version (do not change)
Version="2.0.0"

# Load configuration file
if [ -f "$ScriptPath"/statrix.cfg ]
then
	. "$ScriptPath"/statrix.cfg
else
	echo "Error: Configuration file not found at $ScriptPath/statrix.cfg"
	exit 1
fi

if [ -z "$STATRIX_ENDPOINT" ]; then
	echo "Error: STATRIX_ENDPOINT is missing in $ScriptPath/statrix.cfg"
	exit 1
fi

# Script start time
ScriptStartTime=$(date +[%Y-%m-%d\ %T)

##############################################################################
# Helper: key-value store using temp files (bash 3.2 has no associative arrays)
##############################################################################
KV_DIR=$(mktemp -d /tmp/statrix_kv.XXXXXX)
trap "rm -rf '$KV_DIR'" EXIT

kv_set() { # usage: kv_set namespace key value
	local ns="$1" key="$2" val="$3"
	mkdir -p "$KV_DIR/$ns"
	printf '%s' "$val" > "$KV_DIR/$ns/$key"
}
kv_get() { # usage: kv_get namespace key [default]
	local ns="$1" key="$2" default="${3:-0}"
	if [ -f "$KV_DIR/$ns/$key" ]; then
		cat "$KV_DIR/$ns/$key"
	else
		echo "$default"
	fi
}

# Service status function
servicestatus() {
	if (( $(ps -ef | grep -E "[\/\ ]$1([^\/]|$)" | grep -v "grep" | wc -l) > 0 ))
	then
		echo "1"
	else
		if launchctl list 2>/dev/null | grep -qi "$1"
		then
			echo "1"
		else
			echo "0"
		fi
	fi
}

# Function used to prepare base64 str for url encoding
base64prep() {
	str=$1
	str="${str//+/%2B}"
	str="${str//\//%2F}"
	echo "$str"
}

# Function used to perform outgoing PING tests
pingstatus() {
	local TargetName=$1
	local PingTarget=$2
	if ! echo "$TargetName" | grep -qE '^[A-Za-z0-9._-]+$'; then
		if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Invalid PING target name value" >> "$ScriptPath"/debug.log; fi
		exit 1
	fi
	if ! echo "$PingTarget" | grep -qE '^[A-Za-z0-9.:_-]+$'; then
		if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Invalid PING target value" >> "$ScriptPath"/debug.log; fi
		exit 1
	fi
	PING_OUTPUT=$(ping "$PingTarget" -c "$OutgoingPingsCount" 2>/dev/null)
	if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T])PING_OUTPUT:\n$PING_OUTPUT" >> "$ScriptPath"/debug.log; fi
	PACKET_LOSS=$(echo "$PING_OUTPUT" | grep -o '[0-9.]*% packet loss' | cut -d'%' -f1)
	if [ -z "$PACKET_LOSS" ]; then
		if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Unable to extract packet loss" >> "$ScriptPath"/debug.log; fi
		exit 1
	fi
	RTT_LINE=$(echo "$PING_OUTPUT" | grep 'round-trip min/avg/max')
	if [ -n "$RTT_LINE" ]; then
		AVG_RTT=$(echo "$RTT_LINE" | awk -F'/' '{print $5}')
		AVG_RTT=$(echo | awk "{print $AVG_RTT * 1000}" | awk '{printf "%18.0f",$1}' | xargs)
	else
		AVG_RTT="0"
	fi
	echo "$TargetName,$PingTarget,$PACKET_LOSS,$AVG_RTT;" >> "$ScriptPath"/ping.txt
}

# Check if the agent needs to run Outgoing PING tests
if [ "$1" == "ping" ]
then
	if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Starting PING: $2 ($3) $OutgoingPingsCount times" >> "$ScriptPath"/debug.log; fi
	pingstatus "$2" "$3"
	exit 1
fi

# Clear debug.log every day at midnight
if [ -z "$(date +%H | sed 's/^0*//')" ] && [ -z "$(date +%M | sed 's/^0*//')" ] && [ -f "$ScriptPath"/debug.log ]
then
	rm -f "$ScriptPath"/debug.log
fi

# Start timers
START=$(date +%s)
tTIMEDIFF=0

# Get current minute
M=$(date +%M | sed 's/^0*//')
if [ -z "$M" ]; then
	M=0
	if [ -f "$ScriptPath"/statrix_cron.log ]; then
		rm -f "$ScriptPath"/statrix_cron.log
	fi
fi

if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Starting Statrix Agent v$Version (macOS)" >> "$ScriptPath"/debug.log; fi

# Kill any lingering agent processes
HTProcesses=$(pgrep -f statrix_agent.sh | wc -l | xargs)
if [ -z "$HTProcesses" ]; then HTProcesses=0; fi
if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Found $HTProcesses agent processes" >> "$ScriptPath"/debug.log; fi

if [ "$HTProcesses" -ge 50 ]; then
	pgrep -f statrix_agent.sh | xargs kill -9
fi
if [ "$HTProcesses" -ge 10 ]; then
	for PID in $(pgrep -f statrix_agent.sh); do
		PID_TIME=$(ps -p "$PID" -oetime= 2>/dev/null | tr '-' ':' | awk -F: '{total=0; m=1;} {for (i=0; i < NF; i++) {total += $(NF-i)*m; m *= i >= 2 ? 24 : 60 }} {print total}')
		if [ -n "$PID_TIME" ] && [ "$PID_TIME" -ge 90 ]; then
			kill -9 "$PID" 2>/dev/null
		fi
	done
fi

# Outgoing PING (background)
if [ -n "$OutgoingPings" ]; then
	OLD_IFS="$IFS"
	IFS='|'
	for i in $OutgoingPings; do
		IFS=',' read TargetName TargetIP <<< "$i"
		bash "$ScriptPath"/statrix_agent.sh ping "$TargetName" "$TargetIP" &
	done
	IFS="$OLD_IFS"
fi

# Network interfaces
if [ -n "$NetworkInterfaces" ]; then
	OLD_IFS="$IFS"; IFS=','; NetworkInterfacesArray=($NetworkInterfaces); IFS="$OLD_IFS"
else
	NetworkInterfacesArray=()
	for iface in $(networksetup -listallhardwareports 2>/dev/null | grep "^Device:" | awk '{print $2}'); do
		if ifconfig "$iface" 2>/dev/null | grep -q "status: active"; then
			NetworkInterfacesArray+=("$iface")
		fi
	done
	# Fallback
	if [ ${#NetworkInterfacesArray[@]} -eq 0 ]; then
		for iface in $(ifconfig -lu 2>/dev/null | tr ' ' '\n' | grep -E '^en[0-9]+$'); do
			if ifconfig "$iface" 2>/dev/null | grep -q "inet "; then
				NetworkInterfacesArray+=("$iface")
			fi
		done
	fi
fi
if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Network Interfaces: ${NetworkInterfacesArray[*]}" >> "$ScriptPath"/debug.log; fi

# Initial network usage
for NIC in "${NetworkInterfacesArray[@]}"; do
	NETSTAT_LINE=$(netstat -ibI "$NIC" 2>/dev/null | grep -w "$NIC" | grep -v "Link#" | head -1)
	if [ -z "$NETSTAT_LINE" ]; then
		NETSTAT_LINE=$(netstat -ibI "$NIC" 2>/dev/null | tail -1)
	fi
	init_rx=$(echo "$NETSTAT_LINE" | awk '{print $7}')
	init_tx=$(echo "$NETSTAT_LINE" | awk '{print $10}')
	kv_set "aRX" "$NIC" "${init_rx:-0}"
	kv_set "aTX" "$NIC" "${init_tx:-0}"
	kv_set "tRX" "$NIC" "0"
	kv_set "tTX" "$NIC" "0"
	if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Network Interface $NIC RX: ${init_rx:-0} TX: ${init_tx:-0}" >> "$ScriptPath"/debug.log; fi
done

# Auto-detect listening ports
if [ -z "${ConnectionPorts// }" ]; then
	if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Detecting external connection ports" >> "$ScriptPath"/debug.log; fi
	AutoDetectedPorts=$(lsof -iTCP -sTCP:LISTEN -nP 2>/dev/null | awk 'NR>1 {print $9}' | grep -oE '[0-9]+$' | sort -n | uniq | head -30 | tr '\n' ',' | sed 's/,$//')
	if [ -n "$AutoDetectedPorts" ]; then
		ConnectionPorts="$AutoDetectedPorts"
		if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Auto detected connection ports: $ConnectionPorts" >> "$ScriptPath"/debug.log; fi
	fi
fi

# Port connections init
ConnectionPortsArray=()
if [ -n "$ConnectionPorts" ]; then
	OLD_IFS="$IFS"; IFS=','; ConnectionPortsArray=($ConnectionPorts); IFS="$OLD_IFS"
	for cPort in "${ConnectionPortsArray[@]}"; do
		kv_set "conn" "$cPort" "0"
	done
fi

# Check Services (initial)
CheckServicesArray=()
if [ -n "$CheckServices" ]; then
	OLD_IFS="$IFS"; IFS=','; CheckServicesArray=($CheckServices); IFS="$OLD_IFS"
	for svc in "${CheckServicesArray[@]}"; do
		val=$(servicestatus "$svc")
		kv_set "srvcs" "$svc" "$val"
		if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Service $svc status: $val" >> "$ScriptPath"/debug.log; fi
	done
fi

# Calculate how many data sample loops
RunTimes=$(echo | awk "{print int(60 / $CollectEveryXSeconds)}")
if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Collecting data for $RunTimes loops" >> "$ScriptPath"/debug.log; fi

# Initialize totals
tCPU=0
tCPUus=0
tCPUsy=0
tRAM=0
tRAMSwap=0
tloadavg1=0
tloadavg5=0
tloadavg15=0

# Get total physical RAM in bytes
TOTAL_RAM_BYTES=$(sysctl -n hw.memsize 2>/dev/null)
PAGE_SIZE=$(sysctl -n hw.pagesize 2>/dev/null)
if [ -z "$PAGE_SIZE" ] || [ "$PAGE_SIZE" -eq 0 ] 2>/dev/null; then PAGE_SIZE=16384; fi

# Initial disk IOPS snapshot (per-disk via ioreg)
# Parse all IOBlockStorageDriver entries to get per-physical-disk read/write bytes
IOPS_DISK_LIST=""
IOPS_TIME_START=$(date +%s)
ioreg -c IOBlockStorageDriver -r -l -d 3 2>/dev/null | awk '
/IOBlockStorageDriver/{stats_r=""; stats_w=""; bsd=""}
/"Statistics"/{
    r=$0; sub(/.*"Bytes \(Read\)"=/, "", r); sub(/[,}].*/, "", r); stats_r=r
    w=$0; sub(/.*"Bytes \(Write\)"=/, "", w); sub(/[,}].*/, "", w); stats_w=w
}
/"BSD Name" = "disk[0-9]+"/{
    b=$0; sub(/.*"BSD Name" = "/, "", b); sub(/".*/, "", b); bsd=b
    if(bsd != "" && stats_r != "") print bsd ":" stats_r ":" stats_w
}
' | while IFS=: read disk rd wr; do
	kv_set "iops_r" "$disk" "${rd:-0}"
	kv_set "iops_w" "$disk" "${wr:-0}"
	echo "$disk"
done > "$KV_DIR/iops_disklist"
IOPS_DISK_LIST=$(cat "$KV_DIR/iops_disklist" 2>/dev/null | tr '\n' ' ')

# Build physical-disk-to-mount mapping via diskutil list
# Finds synthesized APFS containers and maps them back to physical disks
# Format stored: kv_set "iops_mnt" <physical_disk> <mount_point>
# Default mount for a physical disk is "/"
for disk in $IOPS_DISK_LIST; do
	kv_set "iops_mnt" "$disk" "/"
done
# Parse df mounts and trace each back to its physical disk
while read -r dev mnt; do
	# Extract base disk identifier (e.g., disk3 from /dev/disk3s1s1)
	base=$(echo "$dev" | sed 's|/dev/||; s/s[0-9].*//')
	# Check if it's a synthesized APFS container by looking for Physical Store
	phys=$(diskutil info "$base" 2>/dev/null | grep "Physical Store" | awk '{print $NF}' | sed 's/s[0-9].*//')
	if [ -z "$phys" ]; then
		phys="$base"
	fi
	# Only map the root mount "/" or /Volumes/* mounts (skip system volumes)
	case "$mnt" in
		/|/Volumes/*)
			kv_set "iops_mnt" "$phys" "$mnt"
			;;
	esac
done <<< "$(df -l 2>/dev/null | awk 'NR>1 && /\/dev\/disk/{print $1, $NF}')"

if [ "$DEBUG" -eq 1 ]; then
	for disk in $IOPS_DISK_LIST; do
		dr=$(kv_get "iops_r" "$disk" 0)
		dw=$(kv_get "iops_w" "$disk" 0)
		dm=$(kv_get "iops_mnt" "$disk" "/")
		echo -e "$ScriptStartTime-$(date +%T]) IOPS start: $disk ($dm) Read=$dr Write=$dw" >> "$ScriptPath"/debug.log
	done
fi

# Collect data loop
X=0
for i in $(seq "$RunTimes"); do
	X=$((X + 1))

	# CPU usage via top
	TOP_OUTPUT=$(top -l 2 -n 0 -s "$CollectEveryXSeconds" 2>/dev/null | grep "CPU usage" | tail -1)
	CPU_USER=$(echo "$TOP_OUTPUT" | awk -F'[:,]' '{print $2}' | grep -oE '[0-9]+\.[0-9]+')
	CPU_SYS=$(echo "$TOP_OUTPUT" | awk -F'[:,]' '{print $3}' | grep -oE '[0-9]+\.[0-9]+')
	CPU_IDLE=$(echo "$TOP_OUTPUT" | awk -F'[:,]' '{print $4}' | grep -oE '[0-9]+\.[0-9]+')

	if [ -n "$CPU_IDLE" ]; then
		CPU=$(echo | awk "{print 100 - $CPU_IDLE}")
	else
		CPU=0
	fi
	tCPU=$(echo | awk "{print $tCPU + $CPU}")
	tCPUus=$(echo | awk "{print $tCPUus + ${CPU_USER:-0}}")
	tCPUsy=$(echo | awk "{print $tCPUsy + ${CPU_SYS:-0}}")

	# CPU Load averages
	loadavg=$(sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' | xargs)
	la1=$(echo "$loadavg" | awk '{print $1}')
	la5=$(echo "$loadavg" | awk '{print $2}')
	la15=$(echo "$loadavg" | awk '{print $3}')
	tloadavg1=$(echo | awk "{print $tloadavg1 + ${la1:-0}}")
	tloadavg5=$(echo | awk "{print $tloadavg5 + ${la5:-0}}")
	tloadavg15=$(echo | awk "{print $tloadavg15 + ${la15:-0}}")

	if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) CPU: $CPU User: ${CPU_USER:-0} Sys: ${CPU_SYS:-0} Load: ${la1:-0} ${la5:-0} ${la15:-0}" >> "$ScriptPath"/debug.log; fi

	# RAM usage via vm_stat
	VMSTAT_OUTPUT=$(vm_stat 2>/dev/null)
	PAGES_ACTIVE=$(echo "$VMSTAT_OUTPUT" | grep "Pages active:" | awk '{print $3}' | tr -d '.')
	PAGES_WIRED=$(echo "$VMSTAT_OUTPUT" | grep "Pages wired down:" | awk '{print $4}' | tr -d '.')
	PAGES_COMPRESSED=$(echo "$VMSTAT_OUTPUT" | grep "Pages occupied by compressor:" | awk '{print $5}' | tr -d '.')
	PAGES_ACTIVE=${PAGES_ACTIVE:-0}
	PAGES_WIRED=${PAGES_WIRED:-0}
	PAGES_COMPRESSED=${PAGES_COMPRESSED:-0}

	USED_PAGES=$((PAGES_ACTIVE + PAGES_WIRED + PAGES_COMPRESSED))
	TOTAL_PAGES=$((TOTAL_RAM_BYTES / PAGE_SIZE))

	if [ "$TOTAL_PAGES" -gt 0 ]; then
		RAM=$(echo | awk "{print $USED_PAGES * 100 / $TOTAL_PAGES}")
	else
		RAM=0
	fi
	tRAM=$(echo | awk "{print $tRAM + $RAM}")

	# Swap usage
	SWAP_INFO=$(sysctl -n vm.swapusage 2>/dev/null)
	SWAP_TOTAL=$(echo "$SWAP_INFO" | grep -oE 'total = [0-9.]+[A-Z]' | grep -oE '[0-9.]+')
	SWAP_USED=$(echo "$SWAP_INFO" | grep -oE 'used = [0-9.]+[A-Z]' | grep -oE '[0-9.]+')
	if [ -n "$SWAP_TOTAL" ] && [ "$(echo "$SWAP_TOTAL" | awk '{print ($1 > 0)}')" = "1" ]; then
		RAMSwap=$(echo | awk "{print ${SWAP_USED:-0} * 100 / $SWAP_TOTAL}")
	else
		RAMSwap=0
	fi
	tRAMSwap=$(echo | awk "{print $tRAMSwap + $RAMSwap}")

	if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) RAM: $RAM Swap: $RAMSwap" >> "$ScriptPath"/debug.log; fi

	# Network usage
	END=$(date +%s)
	TIMEDIFF=$((END - START))
	if [ "$TIMEDIFF" -le 0 ]; then TIMEDIFF=1; fi
	tTIMEDIFF=$((tTIMEDIFF + TIMEDIFF))
	START=$END

	for NIC in "${NetworkInterfacesArray[@]}"; do
		NETSTAT_LINE=$(netstat -ibI "$NIC" 2>/dev/null | grep -w "$NIC" | grep -v "Link#" | head -1)
		if [ -z "$NETSTAT_LINE" ]; then
			NETSTAT_LINE=$(netstat -ibI "$NIC" 2>/dev/null | tail -1)
		fi
		CURR_RX=$(echo "$NETSTAT_LINE" | awk '{print $7}')
		CURR_TX=$(echo "$NETSTAT_LINE" | awk '{print $10}')
		CURR_RX=${CURR_RX:-0}
		CURR_TX=${CURR_TX:-0}

		PREV_RX=$(kv_get "aRX" "$NIC" 0)
		PREV_TX=$(kv_get "aTX" "$NIC" 0)
		PREV_TRX=$(kv_get "tRX" "$NIC" 0)
		PREV_TTX=$(kv_get "tTX" "$NIC" 0)

		RX=$(echo | awk "{print ($CURR_RX - $PREV_RX) / $TIMEDIFF}" | awk '{printf "%18.0f",$1}' | xargs)
		TX=$(echo | awk "{print ($CURR_TX - $PREV_TX) / $TIMEDIFF}" | awk '{printf "%18.0f",$1}' | xargs)

		kv_set "aRX" "$NIC" "$CURR_RX"
		kv_set "aTX" "$NIC" "$CURR_TX"
		kv_set "tRX" "$NIC" "$(echo | awk "{print $PREV_TRX + $RX}" | awk '{printf "%18.0f",$1}' | xargs)"
		kv_set "tTX" "$NIC" "$(echo | awk "{print $PREV_TTX + $TX}" | awk '{printf "%18.0f",$1}' | xargs)"
	done

	if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Network loop $X done" >> "$ScriptPath"/debug.log; fi

	# Port connections
	if [ ${#ConnectionPortsArray[@]} -gt 0 ]; then
		for cPort in "${ConnectionPortsArray[@]}"; do
			CONN_COUNT=$(lsof -iTCP:"$cPort" -sTCP:ESTABLISHED -nP 2>/dev/null | grep -v "^COMMAND" | wc -l | xargs)
			prev=$(kv_get "conn" "$cPort" 0)
			kv_set "conn" "$cPort" "$(echo | awk "{print $prev + $CONN_COUNT}")"
		done
	fi

	# Check if minute changed
	MM=$(date +%M | sed 's/^0*//')
	if [ -z "$MM" ]; then MM=0; fi
	if [ "$MM" -ne "$M" ]; then
		if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Minute changed, ending loop" >> "$ScriptPath"/debug.log; fi
		break
	fi
done

# Get user running the agent
User=$(whoami)

# Check if system requires reboot
RequiresReboot=0

# Operating System
OS_NAME=$(sw_vers -productName 2>/dev/null)
OS_VER=$(sw_vers -productVersion 2>/dev/null)
OS=$(echo -ne "$OS_NAME $OS_VER" | base64 | tr -d '\n\r\t ')

# Kernel
Kernel=$(uname -r | base64 | tr -d '\n\r\t ')

# Hostname
Hostname=$(uname -n | base64 | tr -d '\n\r\t ')

# Uptime
BOOT_TIME=$(sysctl -n kern.boottime 2>/dev/null | awk -F'sec = ' '{print $2}' | awk -F',' '{print $1}')
CURRENT_TIME=$(date +%s)
if [ -n "$BOOT_TIME" ]; then
	Uptime=$((CURRENT_TIME - BOOT_TIME))
else
	Uptime=0
fi

if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) User: $User Hostname: $(echo "$Hostname" | base64 -d 2>/dev/null) Uptime: $Uptime" >> "$ScriptPath"/debug.log; fi

# CPU model
CPUModel=$(sysctl -n machdep.cpu.brand_string 2>/dev/null)
if [ -z "$CPUModel" ]; then
	CPUModel=$(sysctl -n hw.model 2>/dev/null)
fi
CPUModel=$(echo -ne "$CPUModel" | base64 | tr -d '\n\r\t ')

# CPU info
CPUSockets=$(sysctl -n hw.packages 2>/dev/null || echo "1")
CPUCores=$(sysctl -n hw.physicalcpu 2>/dev/null || echo "1")
CPUThreads=$(sysctl -n hw.logicalcpu 2>/dev/null || echo "1")

# CPU clock speed (MHz)
CPUSpeed=$(sysctl -n hw.cpufrequency 2>/dev/null)
if [ -n "$CPUSpeed" ] && [ "$CPUSpeed" -gt 0 ] 2>/dev/null; then
	CPUSpeed=$((CPUSpeed / 1000000))
else
	# Try system_profiler for Intel Macs
	CPUSpeed=$(system_profiler SPHardwareDataType 2>/dev/null | grep -i "Processor Speed" | head -1 | grep -oE '[0-9.]+\s*GHz' | grep -oE '[0-9.]+' | awk '{printf "%d", $1 * 1000}')
	# Apple Silicon: get max P-cluster frequency from powermetrics
	if [ -z "$CPUSpeed" ] || [ "$CPUSpeed" -eq 0 ] 2>/dev/null; then
		if command -v powermetrics > /dev/null 2>&1; then
			CPUSpeed=$(powermetrics --samplers cpu_power -i 1 -n 1 2>/dev/null | grep "P-Cluster HW active residency" | grep -oE '[0-9]+ MHz' | tail -1 | grep -oE '[0-9]+')
		fi
	fi
	if [ -z "$CPUSpeed" ]; then CPUSpeed=0; fi
fi

# Averages
CPU=$(echo | awk "{print $tCPU / $X}")
CPUus=$(echo | awk "{print $tCPUus / $X}")
CPUsy=$(echo | awk "{print $tCPUsy / $X}")
CPUwa=0
CPUst=0
loadavg1=$(echo | awk "{print $tloadavg1 / $X}")
loadavg5=$(echo | awk "{print $tloadavg5 / $X}")
loadavg15=$(echo | awk "{print $tloadavg15 / $X}")

if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) CPU: $CPU Cores: $CPUCores Threads: $CPUThreads Speed: $CPUSpeed" >> "$ScriptPath"/debug.log; fi

# RAM size (in KB)
RAMSize=$((TOTAL_RAM_BYTES / 1024))
RAM=$(echo | awk "{print $tRAM / $X}")

# Swap
RAMSwapSize_raw=$(sysctl -n vm.swapusage 2>/dev/null | grep -oE 'total = [0-9.]+[A-Z]')
SWAP_NUM=$(echo "$RAMSwapSize_raw" | grep -oE '[0-9.]+')
SWAP_UNIT=$(echo "$RAMSwapSize_raw" | grep -oE '[A-Z]$')
case "$SWAP_UNIT" in
	G) RAMSwapSize=$(echo "$SWAP_NUM" | awk '{printf "%.0f", $1 * 1024 * 1024}') ;;
	M) RAMSwapSize=$(echo "$SWAP_NUM" | awk '{printf "%.0f", $1 * 1024}') ;;
	K) RAMSwapSize=$(echo "$SWAP_NUM" | awk '{printf "%.0f", $1}') ;;
	*) RAMSwapSize=0 ;;
esac
if [ "$RAMSwapSize" -gt 0 ] 2>/dev/null; then
	RAMSwap=$(echo | awk "{print $tRAMSwap / $X}")
else
	RAMSwap=0
fi
RAMBuff=0
RAMCache=0

if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) RAM Size: $RAMSize Usage: $RAM Swap Size: $RAMSwapSize Swap: $RAMSwap" >> "$ScriptPath"/debug.log; fi

# Disks usage
DISKs=""
if [ -n "$(df -T -b 2>/dev/null)" ]; then
	while IFS= read -r line; do
		mount_point=$(echo "$line" | awk '{for(i=9;i<=NF;i++) printf "%s"(i<NF?" ":""), $i; print ""}')
		fs_type=$(echo "$line" | awk '{print $2}')
		total_size=$(echo "$line" | awk '{print $3}')
		used_size=$(echo "$line" | awk '{print $4}')
		avail_size=$(echo "$line" | awk '{print $5}')
		if [ -n "$mount_point" ] && [ -n "$total_size" ]; then
			DISKs="$DISKs$mount_point,$fs_type,$total_size,$used_size,$avail_size;"
		fi
	done <<< "$(df -T -b 2>/dev/null | sed 1d | grep -v -E 'devfs|tmpfs|map |/System/Volumes/')"
else
	while IFS= read -r line; do
		mount_point=$(echo "$line" | awk '{for(i=6;i<=NF;i++) printf "%s"(i<NF?" ":""), $i; print ""}')
		total_size=$(echo "$line" | awk '{print $2 * 512}')
		used_size=$(echo "$line" | awk '{print $3 * 512}')
		avail_size=$(echo "$line" | awk '{print $4 * 512}')
		# Detect filesystem type from mount point
		fs_type=$(mount 2>/dev/null | grep " on $mount_point " | awk -F'[()]' '{print $2}' | awk -F',' '{print $1}')
		fs_type=${fs_type:-apfs}
		if [ -n "$mount_point" ] && [ -n "$total_size" ]; then
			DISKs="$DISKs$mount_point,$fs_type,$total_size,$used_size,$avail_size;"
		fi
	done <<< "$(df -b 2>/dev/null | sed 1d | grep -v -E 'devfs|tmpfs|map |/System/Volumes/')"
fi
DISKs=$(echo -ne "$DISKs" | base64 | tr -d '\n\r\t ')

if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) DISKs encoded" >> "$ScriptPath"/debug.log; fi

# Disk inodes
INODEs=""
while IFS= read -r line; do
	mount_point=$(echo "$line" | awk '{for(i=9;i<=NF;i++) printf "%s"(i<NF?" ":""), $i; print ""}')
	iused=$(echo "$line" | awk '{print $6}')
	ifree=$(echo "$line" | awk '{print $7}')
	itotal=$((iused + ifree))
	if [ -n "$mount_point" ]; then
		INODEs="$INODEs$mount_point,$itotal,$iused,$ifree;"
	fi
done <<< "$(df -i 2>/dev/null | sed 1d | grep -v -E 'devfs|tmpfs|map |/System/Volumes/')"
INODEs=$(echo -ne "$INODEs" | base64 | tr -d '\n\r\t ')

# Disk IOPS
IOPS=""
IOPS_TIME_END=$(date +%s)
IOPS_TIME_DIFF=$((IOPS_TIME_END - IOPS_TIME_START))
if [ "$IOPS_TIME_DIFF" -le 0 ]; then IOPS_TIME_DIFF=1; fi

ioreg -c IOBlockStorageDriver -r -l -d 3 2>/dev/null | awk '
/IOBlockStorageDriver/{stats_r=""; stats_w=""; bsd=""}
/"Statistics"/{
    r=$0; sub(/.*"Bytes \(Read\)"=/, "", r); sub(/[,}].*/, "", r); stats_r=r
    w=$0; sub(/.*"Bytes \(Write\)"=/, "", w); sub(/[,}].*/, "", w); stats_w=w
}
/"BSD Name" = "disk[0-9]+"/{
    b=$0; sub(/.*"BSD Name" = "/, "", b); sub(/".*/, "", b); bsd=b
    if(bsd != "" && stats_r != "") print bsd ":" stats_r ":" stats_w
}
' | while IFS=: read disk rd_end wr_end; do
	rd_start=$(kv_get "iops_r" "$disk" 0)
	wr_start=$(kv_get "iops_w" "$disk" 0)
	mnt=$(kv_get "iops_mnt" "$disk" "/")
	rd_bps=$(echo | awk "{print (${rd_end:-0} - ${rd_start:-0}) / $IOPS_TIME_DIFF}" | awk '{printf "%18.0f",$1}' | xargs)
	wr_bps=$(echo | awk "{print (${wr_end:-0} - ${wr_start:-0}) / $IOPS_TIME_DIFF}" | awk '{printf "%18.0f",$1}' | xargs)
	echo "$mnt,$rd_bps,$wr_bps;"
	if [ "$DEBUG" -eq 1 ]; then
		echo -e "$ScriptStartTime-$(date +%T]) IOPS end: $disk ($mnt) ReadEnd=${rd_end:-0} WriteEnd=${wr_end:-0} ReadBps=$rd_bps WriteBps=$wr_bps" >> "$ScriptPath"/debug.log
	fi
done > "$KV_DIR/iops_result"
IOPS=$(cat "$KV_DIR/iops_result" 2>/dev/null | tr -d '\n')
IOPS=$(echo -ne "$IOPS" | base64 | tr -d '\n\r\t ')

# Network final
NICS=""
IPv4=""
IPv6=""
for NIC in "${NetworkInterfacesArray[@]}"; do
	nic_rx=$(kv_get "tRX" "$NIC" 0)
	nic_tx=$(kv_get "tTX" "$NIC" 0)
	RX=$(echo | awk "{print $nic_rx / $X}" | awk '{printf "%18.0f",$1}' | xargs)
	TX=$(echo | awk "{print $nic_tx / $X}" | awk '{printf "%18.0f",$1}' | xargs)
	NICS="$NICS$NIC,$RX,$TX;"
	IPv4="$IPv4$NIC,$(ifconfig "$NIC" 2>/dev/null | grep "inet " | awk '{print $2}' | xargs | sed 's/ /,/g');"
	IPv6="$IPv6$NIC,$(ifconfig "$NIC" 2>/dev/null | grep "inet6 " | grep -v "fe80" | awk '{print $2}' | sed 's/%.*//g' | xargs | sed 's/ /,/g');"
done
NICS=$(echo -ne "$NICS" | base64 | tr -d '\n\r\t ')
IPv4=$(echo -ne "$IPv4" | base64 | tr -d '\n\r\t ')
IPv6=$(echo -ne "$IPv6" | base64 | tr -d '\n\r\t ')

if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) Network encoded" >> "$ScriptPath"/debug.log; fi

# Port connections
CONN=""
if [ ${#ConnectionPortsArray[@]} -gt 0 ]; then
	for cPort in "${ConnectionPortsArray[@]}"; do
		cval=$(kv_get "conn" "$cPort" 0)
		CON=$(echo | awk "{print $cval / $X}" | awk '{printf "%18.0f",$1}' | xargs)
		CONN="$CONN$cPort,$CON;"
	done
fi
CONN=$(echo -ne "$CONN" | base64 | tr -d '\n\r\t ')

# Temperature
TEMP=""
if [ "$(id -u)" -eq 0 ]; then
	# Intel Macs: try powermetrics smc sampler for CPU die temperature
	if command -v "powermetrics" > /dev/null 2>&1; then
		TEMP_RAW=$(powermetrics --samplers smc -i 1 -n 1 2>/dev/null | grep "CPU die temperature" | grep -oE '[0-9.]+')
		if [ -n "$TEMP_RAW" ]; then
			TEMP_VAL=$(echo "$TEMP_RAW" | awk '{printf "%18.0f", $1 * 1000}' | xargs)
			TEMP="CPU_die,$TEMP_VAL;"
		fi
	fi
	# Apple Silicon / fallback: get SSD temperature from smartctl (NVMe SMART)
	if [ -z "$TEMP" ] && command -v "smartctl" > /dev/null 2>&1; then
		for disk in $(diskutil list 2>/dev/null | grep "^/dev/disk[0-9]" | grep "physical" | awk '{print $1}' | sort -u); do
			SSD_TEMP=$(smartctl -A "$disk" 2>/dev/null | grep "^Temperature:" | grep -oE '[0-9]+')
			if [ -n "$SSD_TEMP" ]; then
				SSD_TEMP_MILLI=$((SSD_TEMP * 1000))
				TEMP="${TEMP}Core_Average,$SSD_TEMP_MILLI;"
			fi
		done
	fi
fi
TEMP=$(echo -ne "$TEMP" | base64 | tr -d '\n\r\t ')

# Services
SRVCS=""
if [ ${#CheckServicesArray[@]} -gt 0 ]; then
	for svc in "${CheckServicesArray[@]}"; do
		svc_status=$(kv_get "srvcs" "$svc" 0)
		# Re-check
		svc_status=$((svc_status + $(servicestatus "$svc")))
		if [ "$svc_status" -eq 0 ]; then
			SRVCS="$SRVCS$svc,0;"
		else
			SRVCS="$SRVCS$svc,1;"
		fi
	done
fi
SRVCS=$(echo -ne "$SRVCS" | base64 | tr -d '\n\r\t ')

# RAID
RAID=""
ZP=""
RAID=$(echo -ne "$RAID" | base64 | tr -d '\n\r\t ')
ZP=$(echo -ne "$ZP" | base64 | tr -d '\n\r\t ')

# Drive Health
DH=""
if [ "${CheckDriveHealth:-0}" -gt 0 ]; then
	if command -v "smartctl" > /dev/null 2>&1; then
		for disk in $(diskutil list 2>/dev/null | grep "^/dev/disk[0-9]" | grep "physical\|external" | awk '{print $1}' | sort -u); do
			DHealth=$(smartctl -A "$disk" 2>/dev/null)
			if [ -n "$DHealth" ] && echo "$DHealth" | grep -q -E 'Attribute|SMART'; then
				DHHealth=$(smartctl -H "$disk" 2>/dev/null)
				DHealth="$DHHealth\n$DHealth"
				DHealth=$(echo -ne "$DHealth" | base64 | tr -d '\n\r\t ')
				DInfo=$(smartctl -i "$disk" 2>/dev/null)
				DModel=$(echo "$DInfo" | grep -i -E "Device Model:|Model Number:|Product:" | head -1 | awk -F':' '{print $2}' | xargs)
				DSerial=$(echo "$DInfo" | grep -i "Serial Number:" | head -1 | awk -F':' '{print $2}' | xargs)
				diskname=${disk##*/}
				DH="${DH}1,${diskname},$DHealth,$DModel,$DSerial;"
			fi
		done
	fi
fi
DH=$(echo -ne "$DH" | base64 | tr -d '\n\r\t ')

# Running Processes
RPS1=""
RPS2=""
if [ "${RunningProcesses:-0}" -gt 0 ]; then
	if [ -f "$ScriptPath"/running_proc.txt ]; then
		RPS1=$(cat "$ScriptPath"/running_proc.txt)
	fi
	RPS2=$(ps -Ao pid,ppid,uid,user,pcpu,pmem,etime,comm 2>/dev/null | tail -n +2)
	RPS2=$(echo -ne "$RPS2" | base64 | tr -d '\n\r\t ')
	echo "$RPS2" > "$ScriptPath"/running_proc.txt
fi

# Custom Variables
CV=""
if [ -n "$CustomVars" ]; then
	if [ -s "$ScriptPath"/"$CustomVars" ]; then
		CV=$(cat "$ScriptPath"/"$CustomVars" | base64 | tr -d '\n\r\t ')
	fi
fi
if [ "$DEBUG" -eq 1 ]; then echo -e "$ScriptStartTime-$(date +%T]) CV: $CV" >> "$ScriptPath"/debug.log; fi

# Outgoing PING
OPING=""
if [ -n "$OutgoingPings" ]; then
	wait
	if [ -f "$ScriptPath"/ping.txt ]; then
		OPING=$(grep -v '^$' "$ScriptPath"/ping.txt | tr -d '\n' | base64 | tr -d '\n\r\t ')
		rm -f "$ScriptPath"/ping.txt
	fi
fi

# Secured Connection
if [ "${SecuredConnection:-1}" -gt 0 ]; then
	CurlSecure=""
else
	CurlSecure="--insecure"
fi

# Current time/date
Time=$(date "+%Y-%m-%d %T %Z" | base64 | tr -d '\n\r\t ')

# Prepare JSON
json='{"version":"'"$Version"'","SID":"'"$SID"'","agent":"3","user":"'"$User"'","os":"'"$OS"'","kernel":"'"$Kernel"'","hostname":"'"$Hostname"'","time":"'"$Time"'","reqreboot":"'"$RequiresReboot"'","uptime":"'"$Uptime"'","cpumodel":"'"$CPUModel"'","cpusockets":"'"$CPUSockets"'","cpucores":"'"$CPUCores"'","cputhreads":"'"$CPUThreads"'","cpuspeed":"'"$CPUSpeed"'","cpu":"'"$CPU"'","wa":"'"$CPUwa"'","st":"'"$CPUst"'","us":"'"$CPUus"'","sy":"'"$CPUsy"'","load1":"'"$loadavg1"'","load5":"'"$loadavg5"'","load15":"'"$loadavg15"'","ramsize":"'"$RAMSize"'","ram":"'"$RAM"'","ramswapsize":"'"$RAMSwapSize"'","ramswap":"'"$RAMSwap"'","rambuff":"'"$RAMBuff"'","ramcache":"'"$RAMCache"'","disks":"'"$DISKs"'","inodes":"'"$INODEs"'","iops":"'"$IOPS"'","raid":"'"$RAID"'","zp":"'"$ZP"'","dh":"'"$DH"'","nics":"'"$NICS"'","ipv4":"'"$IPv4"'","ipv6":"'"$IPv6"'","conn":"'"$CONN"'","temp":"'"$TEMP"'","serv":"'"$SRVCS"'","cust":"'"$CV"'","oping":"'"$OPING"'","rps1":"'"$RPS1"'","rps2":"'"$RPS2"'"}'

if [ "$DEBUG" -eq 1 ]; then
	echo -e "$ScriptStartTime-$(date +%T]) JSON:\n$json" >> "$ScriptPath"/debug.log
fi

# Post data using curl
jsoncomp=$(echo -ne "$json" | gzip -c | base64 | tr -d '\n\r\t ' | sed 's/\//%2F/g' | sed 's/+/%2B/g')

if [ "$DEBUG" -eq 1 ]; then
	echo -e "$ScriptStartTime-$(date +%T]) Posting data via curl" >> "$ScriptPath"/debug.log
	curl -v --retry 3 --retry-delay 1 --max-time 15 -X POST -d "j=$jsoncomp" $CurlSecure "$STATRIX_ENDPOINT/v2/" >> "$ScriptPath"/debug.log 2>&1
	echo -e "$ScriptStartTime-$(date +%T]) Data posted" >> "$ScriptPath"/debug.log
else
	curl -s --retry 3 --retry-delay 1 --max-time 15 -X POST -d "j=$jsoncomp" $CurlSecure "$STATRIX_ENDPOINT/v2/" &> /dev/null
fi

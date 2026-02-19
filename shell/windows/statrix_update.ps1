#
#	Statrix Server Monitoring Agent
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

# Check if the operating system is 64-bit
$is64BitOS = ([Environment]::Is64BitOperatingSystem)
# Check if the current PowerShell process is 32-bit
$is32BitProcess = -not ([Environment]::Is64BitProcess)
if ($is64BitOS -and $is32BitProcess) {
    Write-Host "Error: Please run this script in a 64-bit PowerShell session."
    exit 1
}

# Check if the script is running with elevated privileges
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if (-not $isAdmin) {
    Write-Host "Error: Please run this script as an Administrator."
    exit 1
}

# Make sure older versions of PowerShell are configured to allow TLS 1.2
# OSVersion needs to be considered to prevent downgrading stronger SystemDefault on newer versions of Windows Server
$commonSecurityProtocols = [Net.SecurityProtocolType]::Tls12
if ([System.Environment]::OSVersion.Version.Build -lt 17763 -and [Net.ServicePointManager]::SecurityProtocol -lt $commonSecurityProtocols) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor $commonSecurityProtocols
}

# Installation folder
$folderPath = "C:\Program Files\Statrix"
# Check if the folder exists
if (-not (Test-Path $folderPath)) {
    Write-Host "Error: Installation folder not found. Please run the installation script first."
    exit 1
}

# Load configuration file
$ConfigFile = "$folderPath\statrix.cfg"
if (-not (Test-Path $ConfigFile)) {
    Write-Host "Error: Configuration file not found. Please run the installation script first."
    exit 1
}

# Function to parse the configuration file
function Get-ConfigValue {
    param (
        [string]$Key
    )
    
    # Read the file and find the line containing the key
    $line = Get-Content $ConfigFile | Where-Object { $_ -match "^$Key=" }
    if ($line) {
        return $line.Split('=')[1].Trim().Trim('"', "'")
    } else {
        exit 1
    }
}

# Helper function to update a config line
function Update-ConfigLine {
    param (
        [string[]]$lines,
        [string]$key,
        [string]$value,
        [bool]$quoteValue = $false
    )
    $pattern = "^$key="
    $replacement = if ($quoteValue) { "$key=`"$value`"" } else { "$key=$value" }
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match $pattern) {
            $lines[$i] = $replacement
        }
    }
    return $lines
}

# Configs
$STATRIX_ENDPOINT = Get-ConfigValue -Key "STATRIX_ENDPOINT"
$SID = Get-ConfigValue -Key "SID"
$CollectEveryXSeconds = Get-ConfigValue -Key "CollectEveryXSeconds"
$NetworkInterfaces = Get-ConfigValue -Key "NetworkInterfaces"
$CheckServices = Get-ConfigValue -Key "CheckServices"
$CheckDriveHealth = Get-ConfigValue -Key "CheckDriveHealth"
$DEBUG = Get-ConfigValue -Key "DEBUG"

if ([string]::IsNullOrWhiteSpace($STATRIX_ENDPOINT)) {
    Write-Host "Error: Missing STATRIX_ENDPOINT in config."
    exit 1
}
$STATRIX_ENDPOINT = $STATRIX_ENDPOINT.TrimEnd('/')

Write-Host "Checking endpoint availability..."
$agentCheckUrl = "$STATRIX_ENDPOINT/shell/windows/statrix_agent.ps1"
try {
    Invoke-WebRequest -Uri $agentCheckUrl -Method Head -UseBasicParsing -ErrorAction Stop | Out-Null
    Write-Host "... done."
} catch {
    Write-Host "Error: Could not reach $agentCheckUrl"
    exit 1
}

# Download the agent
$wc = New-Object System.Net.WebClient
Write-Host "Downloading the agent..."
try {
    $wc.DownloadFile("$STATRIX_ENDPOINT/shell/windows/statrix_agent.ps1", "$folderPath\statrix_agent.ps1")
    Write-Host "... done."
    if ((Get-Item "$folderPath\statrix_agent.ps1").Length -eq 0) {
        Write-Host "Error: Downloaded agent script is empty. Please check your network connection and branch name."
        $wc.Dispose()
        exit 1
    }
} catch {
    Write-Host "Error: Failed to download the agent script. Please check your network connection and branch name."
    $wc.Dispose()
    exit 1
}
Write-Host "Downloading the config file..."
try {
    $wc.DownloadFile("$STATRIX_ENDPOINT/shell/windows/statrix.cfg", "$folderPath\statrix.cfg")
    Write-Host "... done."
    if ((Get-Item "$folderPath\statrix.cfg").Length -eq 0) {
        Write-Host "Error: Downloaded config file is empty. Please check your network connection and branch name."
        $wc.Dispose()
        exit 1
    }
} catch {
    Write-Host "Error: Failed to download the config file. Please check your network connection and branch name."
    $wc.Dispose()
    exit 1
}
$wc.Dispose()

# Read config file into memory
$configLines = Get-Content "$folderPath\statrix.cfg"

# Update all config values in memory
$configLines = Update-ConfigLine -lines $configLines -key "STATRIX_ENDPOINT" -value $STATRIX_ENDPOINT
$configLines = Update-ConfigLine -lines $configLines -key "SID" -value $SID
$configLines = Update-ConfigLine -lines $configLines -key "CollectEveryXSeconds" -value $CollectEveryXSeconds
$configLines = Update-ConfigLine -lines $configLines -key "NetworkInterfaces" -value $NetworkInterfaces -quoteValue $true
$configLines = Update-ConfigLine -lines $configLines -key "CheckServices" -value $CheckServices -quoteValue $true
$configLines = Update-ConfigLine -lines $configLines -key "CheckDriveHealth" -value $CheckDriveHealth
$configLines = Update-ConfigLine -lines $configLines -key "DEBUG" -value $DEBUG

# Write back to file once
Set-Content "$folderPath\statrix.cfg" $configLines

# Create the scheduled task
Write-Host "Checking the scheduled task..."
$taskName = "Statrix Server Monitoring Agent"
$processName = "powershell.exe"
$scriptName = "statrix_agent.ps1"
$existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "The scheduled task already exists..."
    # Find the processes matching the script being executed by the scheduled task
    Write-Host "Finding any running processes executed by the existing scheduled task..."
    $processes = Get-Process | Where-Object {
        $_.ProcessName -like "powershell*" -or $_.ProcessName -like "pwsh*"
    }
    foreach ($process in $processes) {
        try {
            $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($process.Id)").CommandLine
            if ($cmdLine -like "*$scriptName*") {
                Write-Host "Found process $($process.Id)"
                try {
                    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
                    Write-Host "Terminated process $($process.Id)"
                } catch {
                    Write-Host "Failed to terminate process $($process.Id)"
                }
            }
        } catch {
            Write-Host "Unable to access command line for process $($process.Id)."
        }
    }
    Write-Host "Deleting the existing scheduled task..."
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}
Write-Host "... done."
Write-Host "Creating the new scheduled task..."
# Calculate the next full minute
$currentTime = Get-Date
$nextFullMinute = $currentTime.AddMinutes(1).Date.AddHours($currentTime.Hour).AddMinutes($currentTime.Minute)
# Define task action
$taskAction = New-ScheduledTaskAction -Execute $processName -Argument "-ExecutionPolicy Bypass -File `"$folderPath\statrix_agent.ps1`""
# Define task trigger to start at the next full minute and repeat every minute
$taskTrigger = New-ScheduledTaskTrigger -Once -At $nextFullMinute -RepetitionInterval (New-TimeSpan -Minutes 1) -RepetitionDuration (New-TimeSpan -Days 9999)
# Define task principal
$taskPrincipal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
# Define task settings with parallel execution and execution time limit
$taskSettings = New-ScheduledTaskSettingsSet -DontStopIfGoingOnBatteries -StartWhenAvailable -MultipleInstances Parallel
# Register the scheduled task
Register-ScheduledTask -TaskName $taskName -Action $taskAction -Trigger $taskTrigger -Settings $taskSettings -Principal $taskPrincipal
# Set the execution time limit explicitly using Set-ScheduledTask
$task = Get-ScheduledTask -TaskName $taskName
$task.Settings.ExecutionTimeLimit = "PT2M"
Set-ScheduledTask -TaskName $taskName -TaskPath "\" -Settings $task.Settings
Write-Host "... done."

# Start the scheduled task
$currentSecond = (Get-Date).Second
if ($currentSecond -ge 2 -and $currentSecond -le 50) {
    Write-Host "Starting the scheduled task..."
    Start-ScheduledTask -TaskName $taskName
    Write-Host "... done."
}

Write-Host "Update completed successfully."

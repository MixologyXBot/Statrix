#
#	Statrix Server Monitoring Agent - Uninstall Script
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

# Script path
$ScriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path

# Get optional Server ID from args
$SID = if ($args.Count -ge 1 -and $args[0]) { $args[0] } else { "" }
$STATRIX_ENDPOINT = ""

# Installation folder
$folderPath = "C:\Program Files\Statrix"
$configPath = "$folderPath\statrix.cfg"

if (Test-Path $configPath) {
    $cfgLines = Get-Content $configPath
    $sidLine = $cfgLines | Where-Object { $_ -match "^\s*SID\s*=" } | Select-Object -First 1
    $endpointLine = $cfgLines | Where-Object { $_ -match "^\s*STATRIX_ENDPOINT\s*=" } | Select-Object -First 1
    if ($sidLine -and [string]::IsNullOrWhiteSpace($SID)) {
        $SID = ($sidLine.Split('=', 2)[1]).Trim().Trim('"', "'")
    }
    if ($endpointLine) {
        $STATRIX_ENDPOINT = ($endpointLine.Split('=', 2)[1]).Trim().Trim('"', "'").TrimEnd('/')
    }
}

# Make sure the SID is not empty
Write-Host "Checking Server ID (SID)..."
if ($SID -eq "") {
    Write-Host "No Server ID provided."
}
Write-Host "... done."

# Check if the folder exists
Write-Host "Checking installation folder..."
if (-Not (Test-Path -Path $folderPath)) {
    Write-Host "Folder does not exist: $folderPath"
} else {
    Write-Host "Folder already exists: $folderPath"
    # Delete the old agent
    Write-Host "Deleting the old agent..."
    Remove-Item -Path $folderPath -Recurse
}
Write-Host "... done."

# Delete the scheduled task
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
            Write-Host "Error accessing command line for process $($process.Id)."
        }
    }
    Write-Host "Deleting the existing scheduled task..."
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}
Write-Host "... done."

# If SID and endpoint are available, send the uninstallation notice to Statrix
if ($SID -ne "" -and $STATRIX_ENDPOINT -ne "") {
    # Confirm uninstallation
    Write-Host "Letting Statrix know the uninstallation has been completed..."
    # Create a custom object with all the data
    $Data = [PSCustomObject]@{
        version = 'uninstall'
        SID = $SID
    }
    # Convert the object to JSON
    $Data = $Data | ConvertTo-Json
    # Send the data
    $APIURL = "$STATRIX_ENDPOINT/win/"
    $Headers = @{
        'Content-Type' = 'application/json'
    }
    $MaxRetries = 3
    $Timeout = 15
    $RetryCount = 0
    $Success = $false
    while ($RetryCount -lt $MaxRetries -and -not $Success) {
        try {
            $Response = Invoke-RestMethod -Uri $APIURL -Method Post -Headers $Headers -Body $Data -TimeoutSec $Timeout
            $Success = $true
        } catch {
            $RetryCount++
            if ($RetryCount -ne $MaxRetries) {
                Start-Sleep -Seconds 1
            }
        }
    }
    Write-Host "... done."
}

Write-Host "Uninstallation completed successfully."

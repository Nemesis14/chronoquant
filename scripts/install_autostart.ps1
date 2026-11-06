# Task Scheduler feladat: ChronoQuant (API + worker) inditasa Windows-belepeskor (idempotens).
# A regi Streamlit-os 'ChronoQuantUiAutostart' feladatot is torli.
#   .\install_autostart.ps1              telepites / frissites
#   .\install_autostart.ps1 -Uninstall   eltavolitas
param([switch]$Uninstall)

$TaskName   = 'ChronoQuantAutostart'
$LegacyTask = 'ChronoQuantUiAutostart'

Unregister-ScheduledTask -TaskName $LegacyTask -Confirm:$false -ErrorAction SilentlyContinue

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Eltavolitva: $TaskName (es a regi $LegacyTask, ha letezett)"
    exit 0
}

$start = Join-Path $PSScriptRoot 'start.ps1'
$user  = "$env:USERDOMAIN\$env:USERNAME"

$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$start`" -NoBrowser"
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$trigger.Delay = 'PT30S'   # halozat/lemezek alljanak fel
$settings = New-ScheduledTaskSettingsSet `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal -Force | Out-Null
Write-Host "Telepitve: $TaskName (belepeskor, 30 mp kesleltetessel; regi $LegacyTask torolve). Teszt: Start-ScheduledTask -TaskName $TaskName"

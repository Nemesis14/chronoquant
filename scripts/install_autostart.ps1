# Task Scheduler feladat: ChronoQuant UI indítása Windows-belépéskor (idempotens).
#   .\install_autostart.ps1              telepítés / frissítés
#   .\install_autostart.ps1 -Uninstall   eltávolítás
param([switch]$Uninstall)

$TaskName = 'ChronoQuantUiAutostart'

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Eltávolítva: $TaskName"
    exit 0
}

$start = Join-Path $PSScriptRoot 'start_ui.ps1'
$user  = "$env:USERDOMAIN\$env:USERNAME"

$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$start`""
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$trigger.Delay = 'PT30S'   # hálózat/lemezek álljanak fel
$settings = New-ScheduledTaskSettingsSet `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal -Force | Out-Null
Write-Host "Telepítve: $TaskName (belépéskor, 30 mp késleltetéssel). Teszt: Start-ScheduledTask -TaskName $TaskName"

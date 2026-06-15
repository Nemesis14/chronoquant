$jiraPath = "d:\repos\chronoquant\_jira_"
$archivePath = "$jiraPath\archive"

if (-not (Test-Path $archivePath)) {
    New-Item -ItemType Directory -Path $archivePath | Out-Null
}

$epics = Get-ChildItem -Path $jiraPath -Directory | Where-Object { $_.Name -match '^epic_' }

foreach ($epic in $epics) {
    $files = Get-ChildItem -Path $epic.FullName -File
    if ($files.Count -eq 0) { continue }

    $nonDone = $files | Where-Object { $_.Name -notmatch '^done_' }
    if ($nonDone.Count -eq 0) {
        Move-Item -Path $epic.FullName -Destination "$archivePath\$($epic.Name)" -Force
        Write-Host "*** EPIC ARCHIVALVA: $($epic.Name) ***"
    }
}

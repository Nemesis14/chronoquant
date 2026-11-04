# Közös beállítások a ChronoQuant UI indító scriptekhez (dot-source-olva).
$script:Root     = Split-Path -Parent $PSScriptRoot
$script:Port     = 8501
$script:BindHost = '127.0.0.1'
# nem 'localhost': az IPv6 (::1) próba lassítja/megbuktatja a health-checket
$script:BaseUrl  = "http://127.0.0.1:$script:Port"
$script:LogDir   = Join-Path $script:Root 'logs'
$script:Python   = Join-Path $script:Root '.venv\Scripts\python.exe'

function Test-UiHealthy {
    try {
        $r = Invoke-WebRequest -Uri "$script:BaseUrl/_stcore/health" -UseBasicParsing -TimeoutSec 2
        return $r.StatusCode -eq 200
    } catch { return $false }
}

function Get-UiListenerPid {
    $c = Get-NetTCPConnection -LocalPort $script:Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($c) { return $c.OwningProcess } else { return $null }
}

function Stop-Ui {
    $listenerPid = Get-UiListenerPid
    if (-not $listenerPid) { Write-Host 'ChronoQuant UI nem fut.'; return }
    & taskkill /PID $listenerPid /T /F | Out-Null
    for ($i = 0; $i -lt 20 -and (Get-UiListenerPid); $i++) { Start-Sleep -Milliseconds 250 }
    Write-Host "ChronoQuant UI leállítva (pid=$listenerPid)."
}

# Chrome-ban nyitja meg az URL-t. Ha a Chrome már fut, a meglévő ablakban új fül nyílik
# (így a life-plan és a ChronoQuant egy ablakban, két fülön jelenik meg). Fallback: alapértelmezett böngésző.
function Open-InChrome([string]$Url) {
    $chrome = @(
        "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1
    if ($chrome) { Start-Process -FilePath $chrome -ArgumentList $Url } else { Start-Process $Url }
}

# Kozos beallitasok a ChronoQuant indito scriptekhez (dot-source-olva).
$script:Root     = Split-Path -Parent $PSScriptRoot
$script:Port     = if ($env:CQ_PORT) { [int]$env:CQ_PORT } else { 8600 }
$script:BindHost = '127.0.0.1'
# nem 'localhost': az IPv6 (::1) proba lassitja/megbuktatja a health-checkeket
$script:BaseUrl  = "http://127.0.0.1:$script:Port"
$script:LogDir   = Join-Path $script:Root 'logs'
$script:Python   = Join-Path $script:Root '.venv\Scripts\python.exe'
$script:WorkerPidFile = Join-Path $script:LogDir 'worker.pid'
# A src/ konyvtar kerul a Python path-ra (api.main, worker, utils ...).
$env:PYTHONPATH = Join-Path $script:Root 'src'

function Test-ApiHealthy {
    try {
        $r = Invoke-WebRequest -Uri "$script:BaseUrl/api/health" -UseBasicParsing -TimeoutSec 2
        return $r.StatusCode -eq 200
    } catch { return $false }
}

function Get-ApiListenerPid {
    $c = Get-NetTCPConnection -LocalPort $script:Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($c) { return $c.OwningProcess } else { return $null }
}

function Stop-Api {
    $listenerPid = Get-ApiListenerPid
    if (-not $listenerPid) { Write-Host 'ChronoQuant API nem fut.'; return }
    # taskkill /T: az uvicorn --reload gyerekfolyamatait is leallitja
    & taskkill /PID $listenerPid /T /F | Out-Null
    for ($i = 0; $i -lt 20 -and (Get-ApiListenerPid); $i++) { Start-Sleep -Milliseconds 250 }
    Write-Host "ChronoQuant API leallitva (pid=$listenerPid)."
}

function Get-WorkerPid {
    if (-not (Test-Path $script:WorkerPidFile)) { return $null }
    $id = [int](Get-Content $script:WorkerPidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($id -and (Get-Process -Id $id -ErrorAction SilentlyContinue)) { return $id }
    return $null
}

function Start-Worker {
    if (Get-WorkerPid) { Write-Host "Worker mar fut (pid=$(Get-WorkerPid))."; return }
    New-Item -ItemType Directory -Force $script:LogDir | Out-Null
    $p = Start-Process -FilePath $script:Python -ArgumentList '-m', 'worker' `
        -WorkingDirectory $script:Root -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $script:LogDir 'worker.out.log') `
        -RedirectStandardError  (Join-Path $script:LogDir 'worker.err.log')
    Set-Content -Path $script:WorkerPidFile -Value $p.Id
    Write-Host "Worker elindult (pid=$($p.Id))."
}

function Stop-Worker {
    $id = Get-WorkerPid
    if (-not $id) { Write-Host 'Worker nem fut.'; return }
    & taskkill /PID $id /T /F | Out-Null
    Remove-Item $script:WorkerPidFile -ErrorAction SilentlyContinue
    Write-Host "Worker leallitva (pid=$id)."
}

# Chrome-ban nyitja meg az URL-t (meglevo ablakban uj ful). Fallback: alapertelmezett bongeszo.
function Open-InChrome([string]$Url) {
    $chrome = @(
        "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1
    if ($chrome) { Start-Process -FilePath $chrome -ArgumentList $Url } else { Start-Process $Url }
}

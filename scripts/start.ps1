# ChronoQuant elesben: rejtett BFF (:8600, a frontend/dist-et is kiszolgalja) + worker; log: logs/.
# Idempotens: ami mar fut, nem indul masodszor.
#   -NoBrowser   nem nyitja meg a bongeszot (teszt / autostart)
#   -Build       indulas elott `npm ci && npm run build` a frontend/-ben
param([switch]$NoBrowser, [switch]$Build)

. "$PSScriptRoot\_common.ps1"

if ($Build -or -not (Test-Path (Join-Path $script:Root 'frontend\dist\index.html'))) {
    Write-Host 'frontend/dist hianyzik vagy -Build: frontend build...'
    Push-Location (Join-Path $script:Root 'frontend')
    try { & npm ci; & npm run build } finally { Pop-Location }
}

if (-not (Test-ApiHealthy)) {
    if (Get-ApiListenerPid) {
        Write-Error "A $script:Port-es porton mas folyamat fut, de nem ChronoQuant (/api/health nem valaszol)."
        exit 1
    }
    New-Item -ItemType Directory -Force $script:LogDir | Out-Null
    Start-Process -FilePath $script:Python `
        -ArgumentList '-m', 'uvicorn', 'api.main:app', '--app-dir', 'src', '--host', $script:BindHost, '--port', $script:Port `
        -WorkingDirectory $script:Root -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $script:LogDir 'api.out.log') `
        -RedirectStandardError  (Join-Path $script:LogDir 'api.err.log')

    $deadline = (Get-Date).AddSeconds(900)
    while (-not (Test-ApiHealthy) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 500 }
    if (-not (Test-ApiHealthy)) {
        Write-Error "Az API nem allt fel 900 mp alatt. Nezd meg: $script:LogDir\api.err.log"
        exit 1
    }
}

Start-Worker

if (-not $NoBrowser) { Open-InChrome $script:BaseUrl }
Write-Host "ChronoQuant fut: $script:BaseUrl"

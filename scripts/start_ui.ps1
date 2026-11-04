# ChronoQuant Streamlit UI indítása: rejtett szerver (:8501), vár a health-re, megnyitja Chrome-ban (új fül).
# Idempotens: ha már fut, nem indít másodikat, csak megnyitja a böngészőt.
#   -NoBrowser   nem nyitja meg a böngészőt (teszt)
param([switch]$NoBrowser)

. "$PSScriptRoot\_common.ps1"

if (-not (Test-UiHealthy)) {
    if (Get-UiListenerPid) {
        Write-Error "A $script:Port-es porton más folyamat fut, de nem a ChronoQuant UI."
        exit 1
    }
    New-Item -ItemType Directory -Force $script:LogDir | Out-Null
    $env:STREAMLIT_CONFIG_DIR = 'src/ui'
    # python.exe rejtett ablakkal (nem pythonw): a stdout/stderr így logfájlba irányítható.
    Start-Process -FilePath $script:Python `
        -ArgumentList '-m', 'streamlit', 'run', 'src/ui/main.py',
            '--server.headless', 'true', '--server.address', $script:BindHost, '--server.port', $script:Port `
        -WorkingDirectory $script:Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $script:LogDir 'ui.out.log') `
        -RedirectStandardError  (Join-Path $script:LogDir 'ui.err.log')

    $deadline = (Get-Date).AddSeconds(90)
    while (-not (Test-UiHealthy) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 500 }
    if (-not (Test-UiHealthy)) {
        Write-Error "Az UI nem állt fel 90 mp alatt. Nézd meg: $script:LogDir\ui.err.log"
        exit 1
    }
}

if (-not $NoBrowser) { Open-InChrome $script:BaseUrl }
Write-Host "ChronoQuant UI fut: $script:BaseUrl"

# Fejlesztoi mod: az eles peldany leall, helyette --reload BFF (:8600) + worker + Vite (:5173).
# Kilepes: zard be a ket megnyilt ablakot (BFF, Vite), `.\stop.ps1` a workerhez, majd `.\start.ps1` az eles modhoz.
. "$PSScriptRoot\_common.ps1"

Stop-Api
Stop-Worker

Start-Process -FilePath $script:Python `
    -ArgumentList '-m', 'uvicorn', 'api.main:app', '--app-dir', 'src', '--reload', '--reload-dir', 'src',
        '--host', $script:BindHost, '--port', $script:Port `
    -WorkingDirectory $script:Root
Start-Worker
Start-Process -FilePath 'cmd.exe' -ArgumentList '/k', 'npm run dev' -WorkingDirectory (Join-Path $script:Root 'frontend')

Start-Sleep -Seconds 4
Start-Process 'http://localhost:5173'

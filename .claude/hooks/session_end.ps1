$raw = [Console]::In.ReadToEnd()
try {
    $data = $raw | ConvertFrom-Json
    $message = $data.prompt
} catch {
    $message = $raw
}

if ($message -match '(?i)(done|k[eé]sz)') {
    Write-Output "[SESSION_END] Ellenorizd a _doc_/project_overview.md frissitesi igenyet a session tartalma alapjan. Frissiteni kell ha: uj DB tabla/oszlop/semavaltozas, uj/inaktiv ML modell, trading strategy valtozas, uj modul/agent/konvencio, asset config valtozas. Ha igen: frissitsd az erintett szekciokat es jelezd mit valtoztattals. Ha nem: ird hogy nincs valtozas."
}

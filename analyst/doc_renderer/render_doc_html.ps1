param(
    [string]$OutNotebook = "_chronoquant_docs.ipynb"
)

$ErrorActionPreference = "Stop"

$RootDir = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$NotebookPath = Join-Path $RootDir $OutNotebook
$OutputHtml = [IO.Path]::ChangeExtension($NotebookPath, ".html")

Push-Location $RootDir
try {
    uv run python analyst/doc_renderer/build_doc_notebook.py --out $OutNotebook | Out-Host
    quarto render $NotebookPath --to html
}
finally {
    Pop-Location
}

Write-Host "Rendered: $OutputHtml"

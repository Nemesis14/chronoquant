param(
    [string]$Endpoint = "https://mcp.atlassian.com/v1/mcp"
)

$ErrorActionPreference = "Stop"

$envPath = Join-Path $PSScriptRoot ".env"
if (-not (Test-Path -LiteralPath $envPath)) {
    throw "Missing .agent/.env for Atlassian MCP credentials."
}

$values = @{}
foreach ($line in Get-Content -LiteralPath $envPath) {
    $trimmed = $line.Trim()
    if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("#")) {
        continue
    }

    $parts = $line -split "=", 2
    if ($parts.Count -ne 2) {
        continue
    }

    $key = $parts[0].Trim()
    $val = $parts[1].Trim().Trim('"').Trim("'")
    $values[$key] = $val
}

if (-not $values.ContainsKey("email") -or -not $values.ContainsKey("api_token")) {
    throw "Missing email or api_token in .agent/.env."
}

$credential = "{0}:{1}" -f $values["email"], $values["api_token"]
$encoded    = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($credential))
$header     = "Authorization: Basic $encoded"

$env:PATH = "C:\Progra~1\nodejs;$env:PATH"
& "C:\Progra~1\nodejs\npx.cmd" -y mcp-remote@latest $Endpoint --header $header

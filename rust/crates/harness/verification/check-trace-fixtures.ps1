param(
    [Parameter(Mandatory = $true)][string]$EvidenceDirectory
)
$ErrorActionPreference = 'Stop'
$evidence = [System.IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$validLog = Join-Path $evidence 'valid.log'
$invalidLog = Join-Path $evidence 'invalid.log'
$checker = Join-Path $PSScriptRoot 'check-trace.ps1'

& powershell -NoProfile -NonInteractive -File $checker -TracePath (Join-Path $PSScriptRoot 'trace-valid.json') 2>&1 |
    Tee-Object -FilePath $validLog
if ($LASTEXITCODE -ne 0) { throw 'valid trace fixture was rejected.' }

& powershell -NoProfile -NonInteractive -File $checker -TracePath (Join-Path $PSScriptRoot 'trace-invalid-stale.json') 2>&1 |
    Tee-Object -FilePath $invalidLog
if ($LASTEXITCODE -eq 0) { throw 'invalid stale-publication fixture was accepted.' }

Write-Output 'trace fixtures: valid accepted; stale publication rejected.'

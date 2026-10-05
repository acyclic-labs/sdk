param(
    [Parameter(Mandatory = $true)][string]$EvidenceDirectory
)
$ErrorActionPreference = 'Stop'
$evidence = [System.IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$checker = Join-Path $PSScriptRoot 'check-trace.ps1'

$validLog = Join-Path $evidence 'valid.log'
& powershell -NoProfile -NonInteractive -File $checker -TracePath (Join-Path $PSScriptRoot 'trace-valid.json') 2>&1 |
    Tee-Object -FilePath $validLog
if ($LASTEXITCODE -ne 0) { throw 'valid trace fixture was rejected.' }

$invalidCases = @(
    @{ Name = 'swapped-parent'; File = 'trace-invalid-swapped-parent.json' },
    @{ Name = 'missing-id'; File = 'trace-invalid-missing-id.json' },
    @{ Name = 'wrong-agent'; File = 'trace-invalid-wrong-agent.json' },
    @{ Name = 'forged-capture'; File = 'trace-invalid-forged-capture.json' },
    @{ Name = 'orphan-delivery'; File = 'trace-invalid-orphan-delivery.json' },
    @{ Name = 'double-delivery'; File = 'trace-invalid-double-delivery.json' },
    @{ Name = 'invalid-bool'; File = 'trace-invalid-bool.json' },
    @{ Name = 'overspend'; File = 'trace-invalid-overspend.json' },
    @{ Name = 'stale-publication'; File = 'trace-invalid-stale.json' }
)

foreach ($case in $invalidCases) {
    $log = Join-Path $evidence "$($case.Name).log"
    & powershell -NoProfile -NonInteractive -File $checker -TracePath (Join-Path $PSScriptRoot $case.File) 2>&1 |
        Tee-Object -FilePath $log
    if ($LASTEXITCODE -eq 0) { throw "invalid trace fixture '$($case.Name)' was accepted." }
}

Write-Output "trace fixtures: valid accepted; $($invalidCases.Count) invalid traces rejected."

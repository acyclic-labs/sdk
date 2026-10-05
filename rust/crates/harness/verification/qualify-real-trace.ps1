param(
    [Parameter(Mandatory = $true)][string]$EvidenceDirectory
)
$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..\..')).Path
$evidence = [System.IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$tracePath = Join-Path $evidence 'real-harness-trace.json'
$manifestPath = Join-Path $evidence 'real-harness-trace.manifest.json'

# This is the named implementation-conformance gate. The exporter is an
# ignored test by design; invoking it through this script is what makes the
# real-trace qualification explicit and repeatable.
$env:GRAPHCODER_REAL_TRACE_PATH = $tracePath
$env:GRAPHCODER_REAL_TRACE_SOURCE_COMMIT = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($env:GRAPHCODER_REAL_TRACE_SOURCE_COMMIT)) {
    throw 'unable to bind the real trace to the source commit.'
}
Push-Location $repoRoot
try {
    & cargo test -p acyclic-harness --features test-support,native-process-tree `
        exports_real_harness_trace_for_canonical_checker -- --ignored
    if ($LASTEXITCODE -ne 0) {
        throw "real Harness trace exporter failed (exit $LASTEXITCODE)."
    }
} finally {
    Pop-Location
}

& (Join-Path $PSScriptRoot 'check-real-trace.ps1') `
    -TracePath $tracePath -ManifestPath $manifestPath
if ($LASTEXITCODE -ne 0) {
    throw "real Harness trace qualification failed (exit $LASTEXITCODE)."
}
Write-Output "real Harness trace qualification passed: $evidence"

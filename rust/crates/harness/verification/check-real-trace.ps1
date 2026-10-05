param(
    [Parameter(Mandatory = $true)][string]$TracePath,
    [Parameter(Mandatory = $true)][string]$ManifestPath
)
$ErrorActionPreference = 'Stop'

$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
if ($manifest.kind -ne 'real_harness_trace_manifest') {
    throw 'real trace manifest has an unsupported kind.'
}
foreach ($name in @(
    'fork_admitted_sequence',
    'fork_completed_sequence',
    'child_execution_model_started_sequence',
    'child_execution_operation',
    'fork_operation',
    'publication_operation',
    'completion_operation'
)) {
    if ($null -eq $manifest.source.$name) {
        throw "real trace manifest is missing source.$name."
    }
}
if ($manifest.normalization.generation.rule -ne 'first observed immutable project generation maps to ordinal zero') {
    throw 'real trace manifest uses an unknown generation normalization.'
}

$checker = Join-Path $PSScriptRoot 'check-trace.ps1'
& $checker -TracePath $TracePath
if ($LASTEXITCODE -ne 0) {
    throw "canonical trace checker rejected the real Harness trace (exit $LASTEXITCODE)."
}
Write-Output 'real Harness trace passed canonical finite adapter with provenance manifest.'

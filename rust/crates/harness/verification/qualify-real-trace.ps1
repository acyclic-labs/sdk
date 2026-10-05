param(
    [Parameter(Mandatory = $true)][string]$EvidenceDirectory
)
$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..\..')).Path
$evidence = [System.IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$tracePath = Join-Path $evidence 'real-harness-trace.json'
$manifestPath = Join-Path $evidence 'real-harness-trace.manifest.json'

$status = (& git -C $repoRoot status --porcelain --untracked-files=all)
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($status -join "`n"))) {
    throw 'real trace qualification requires a clean tracked and untracked source worktree.'
}
$sourceTree = (& git -C $repoRoot show -s --format=%T HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($sourceTree)) {
    throw 'unable to bind the real trace to the source tree.'
}

# This is the named implementation-conformance gate. The exporter is an
# ignored test by design; invoking it through this script is what makes the
# real-trace qualification explicit and repeatable.
$env:GRAPHCODER_REAL_TRACE_PATH = $tracePath
$env:GRAPHCODER_REAL_TRACE_SOURCE_COMMIT = (& git -C $repoRoot rev-parse HEAD).Trim()
$env:GRAPHCODER_REAL_TRACE_SOURCE_TREE = $sourceTree
$env:GRAPHCODER_REAL_TRACE_SOURCE_CLEAN = 'true'
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

# Mutation checks ensure the semantic source parser is doing the work claimed
# by the gate. Each copy keeps the original trace and provenance but changes one
# source field; every mutation must be rejected before the finite adapter runs.
$checker = Join-Path $PSScriptRoot 'check-real-trace.ps1'
$traceMutations = @(
    @{ Name = 'trace-missing-publication'; Apply = { param($events) @($events | Where-Object { $_.kind -ne 'workspace_published' }) } },
    @{ Name = 'trace-missing-model-start'; Apply = { param($events) @($events | Where-Object { $_.kind -ne 'model_started' }) } },
    @{ Name = 'trace-missing-completion'; Apply = { param($events) @($events | Where-Object { $_.kind -ne 'agent_completed' }) } },
    @{ Name = 'trace-corrupt-agent'; Apply = { param($events) foreach ($event in $events) { if ($event.kind -eq 'model_started') { $event.agent = 3 } }; @($events) } },
    @{ Name = 'trace-corrupt-capture'; Apply = { param($events) foreach ($event in $events) { if ($event.kind -eq 'fork_admitted') { $event.captured_generation = 1 } }; @($events) } }
)
function Sha256Hex([byte[]]$Bytes) {
    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try {
        return (-join ($algorithm.ComputeHash($Bytes) | ForEach-Object { $_.ToString('x2') }))
    } finally {
        $algorithm.Dispose()
    }
}

$traceMutationEvidence = Join-Path $evidence 'trace-mutations'
New-Item -ItemType Directory -Force -Path $traceMutationEvidence | Out-Null
$originalTrace = @(Get-Content -LiteralPath $tracePath -Raw | ConvertFrom-Json)
foreach ($mutation in $traceMutations) {
    $mutantTracePath = Join-Path $traceMutationEvidence "$($mutation.Name).json"
    $mutantManifestPath = Join-Path $traceMutationEvidence "$($mutation.Name).manifest.json"
    $eventsCopy = @($originalTrace | ConvertTo-Json -Depth 100 | ConvertFrom-Json)
    $mutantEvents = & $mutation.Apply $eventsCopy
    $mutantBytes = [System.Text.Encoding]::UTF8.GetBytes(($mutantEvents | ConvertTo-Json -Depth 100))
    [System.IO.File]::WriteAllBytes($mutantTracePath, $mutantBytes)
    $mutantManifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    $mutantManifest.trace_binding.trace_path = $mutantTracePath
    $mutantManifest.trace_binding.trace_sha256 = Sha256Hex $mutantBytes
    $mutantManifest | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $mutantManifestPath -Encoding utf8
    $mutationLog = Join-Path $traceMutationEvidence "$($mutation.Name).log"
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & powershell -NoProfile -NonInteractive -File $checker `
            -TracePath $mutantTracePath -ManifestPath $mutantManifestPath *> $mutationLog
        $mutationExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
    if ($mutationExit -eq 0) {
        throw "trace mutation '$($mutation.Name)' was accepted."
    }
}
$traceMutationCount = $traceMutations.Count

$mutations = @(
    @{ Name = 'task'; Apply = { param($m) $m.source.task = "$($m.source.task)-mutated" } },
    @{ Name = 'parent-step'; Apply = { param($m) $m.source.parent_step = [int64]$m.source.parent_step + 1 } },
    @{ Name = 'child-agent'; Apply = { param($m) $m.source.child_agent = [int64]$m.source.child_agent + 1 } },
    @{ Name = 'seed'; Apply = { param($m) $m.source.seed.operation_id = '00000000-0000-0000-0000-000000000000' } },
    @{ Name = 'seed-noncanonical'; Apply = { param($m) $m.source.seed_canonical_bytes_hex = "20$($m.source.seed_canonical_bytes_hex)" } },
    @{ Name = 'seed-digest'; Apply = { param($m) $m.source.seed_digest = @(0..31) } },
    @{ Name = 'report'; Apply = { param($m) $m.source.report_sha256 = ('0' * 64) } },
    @{ Name = 'publication'; Apply = { param($m) $m.source.publication.operation_id = '00000000-0000-0000-0000-000000000000' } },
    @{ Name = 'declaration'; Apply = { param($m) $m.source.declaration.suffix = @() } },
    @{ Name = 'generation'; Apply = { param($m) $m.normalization.generation.raw_captured_generation = $null } },
    @{ Name = 'generation-ordinal'; Apply = { param($m) $m.normalization.generation.finite_ordinal = 1 } },
    @{ Name = 'parent-revision'; Apply = { param($m) $m.source.parent_conversation_revision = [int64]$m.source.parent_conversation_revision + 1 } },
    @{ Name = 'model-start'; Apply = { param($m) $m.source.child_execution_model_started_step = [int64]$m.source.child_execution_model_started_step + 1 } },
    @{ Name = 'completion-digest'; Apply = { param($m) $m.source.completion_output_digest = @(0..31) } }
)
$mutationEvidence = Join-Path $evidence 'mutations'
New-Item -ItemType Directory -Force -Path $mutationEvidence | Out-Null
foreach ($mutation in $mutations) {
    $mutantManifestPath = Join-Path $mutationEvidence "$($mutation.Name).manifest.json"
    $mutant = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    & $mutation.Apply $mutant
    $mutant | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $mutantManifestPath -Encoding utf8
    $mutationLog = Join-Path $mutationEvidence "$($mutation.Name).log"
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & powershell -NoProfile -NonInteractive -File $checker `
            -TracePath $tracePath -ManifestPath $mutantManifestPath *> $mutationLog
        $mutationExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
    if ($mutationExit -eq 0) {
        throw "source mutation '$($mutation.Name)' was accepted."
    }
}
Write-Output "real source semantic mutations: $($mutations.Count) rejected."
Write-Output "real trace corruption and missing-observation mutations: $traceMutationCount rejected."
Write-Output 'causal publication negative gate: not run; no independent cross-stream causal witness is available.'
Write-Output "real Harness trace qualification passed: $evidence"

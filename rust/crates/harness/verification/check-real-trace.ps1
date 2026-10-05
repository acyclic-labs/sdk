param(
    [Parameter(Mandatory = $true)][string]$TracePath,
    [Parameter(Mandatory = $true)][string]$ManifestPath
)
$ErrorActionPreference = 'Stop'

$manifestPathResolved = [System.IO.Path]::GetFullPath($ManifestPath)
$tracePathResolved = [System.IO.Path]::GetFullPath($TracePath)
$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
if ($manifest.kind -ne 'real_harness_trace_manifest') {
    throw 'real trace manifest has an unsupported kind.'
}
foreach ($name in @(
    'fork_admitted_sequence',
    'fork_completed_sequence',
    'parent_conversation_revision',
    'parent_conversation_operation',
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
foreach ($name in @(
    'fork_operation_id',
    'publication_operation_id',
    'child_operation_id',
    'parent_event_operation_id',
    'completion_operation_id'
)) {
    if ($null -eq $manifest.identity_binding.$name) {
        throw "real trace manifest is missing identity_binding.$name."
    }
}
foreach ($name in @('trace_path', 'trace_sha256')) {
    if ($null -eq $manifest.trace_binding.$name) {
        throw "real trace manifest is missing trace_binding.$name."
    }
}

function Hex([byte[]]$Bytes) {
    return (-join ($Bytes | ForEach-Object { $_.ToString('x2') }))
}

function Sha256Hex([byte[]]$Bytes) {
    $algorithm = [System.Security.Cryptography.SHA256]::Create()
    try { return Hex ($algorithm.ComputeHash($Bytes)) } finally { $algorithm.Dispose() }
}

function DecodeHex([string]$Value, [string]$Name) {
    if (($Value.Length % 2) -ne 0) { throw "$Name has odd hex length." }
    $bytes = [byte[]]::new($Value.Length / 2)
    for ($index = 0; $index -lt $bytes.Length; $index++) {
        try { $bytes[$index] = [Convert]::ToByte($Value.Substring($index * 2, 2), 16) }
        catch { throw "$Name is not valid hex." }
    }
    return $bytes
}

$traceBytes = [System.IO.File]::ReadAllBytes($tracePathResolved)
if ([System.IO.Path]::GetFullPath([string]$manifest.trace_binding.trace_path) -ne $tracePathResolved) {
    throw 'real trace manifest is not bound to the requested trace path.'
}
if ($manifest.trace_binding.trace_sha256 -ne (Sha256Hex $traceBytes)) {
    throw 'real trace digest does not match the requested trace bytes.'
}
foreach ($record in @(
    @{ Bytes = $manifest.source.admission_record_bytes_hex; Digest = $manifest.source.admission_record_sha256; Name = 'admission record' },
    @{ Bytes = $manifest.source.completion_record_bytes_hex; Digest = $manifest.source.completion_record_sha256; Name = 'completion record' },
    @{ Bytes = $manifest.source.parent_event_canonical_bytes_hex; Digest = $manifest.source.parent_event_sha256; Name = 'parent event' },
    @{ Bytes = $manifest.source.child_model_event_canonical_bytes_hex; Digest = $manifest.source.child_model_event_sha256; Name = 'child model event' }
)) {
    $recordBytes = DecodeHex ([string]$record.Bytes) $record.Name
    if ($record.Digest -ne (Sha256Hex $recordBytes)) {
        throw "$($record.Name) digest does not match its canonical bytes."
    }
}

$trace = @(Get-Content -LiteralPath $TracePath -Raw | ConvertFrom-Json)
$admission = @($trace | Where-Object kind -eq 'fork_admitted')
$publication = @($trace | Where-Object kind -eq 'workspace_published')
$started = @($trace | Where-Object kind -eq 'model_started')
$completed = @($trace | Where-Object kind -eq 'agent_completed')
if ($admission.Count -ne 1 -or $publication.Count -ne 1 -or $started.Count -ne 1 -or $completed.Count -ne 1) {
    throw 'real trace must contain exactly one fork, publication, model start, and completion witness.'
}
if ($admission[0].fork_operation_id -ne $manifest.identity_binding.fork_operation_id -or
    $admission[0].child_operation_id -ne $manifest.identity_binding.child_operation_id -or
    $publication[0].fork_operation_id -ne $manifest.identity_binding.fork_operation_id -or
    $publication[0].publication_operation_id -ne $manifest.identity_binding.publication_operation_id -or
    $started[0].child_operation_id -ne $manifest.identity_binding.child_operation_id -or
    $completed[0].child_operation_id -ne $manifest.identity_binding.child_operation_id) {
    throw 'real trace event identities do not match the authenticated source binding.'
}
if ($manifest.identity_binding.parent_event_operation_id -ne $manifest.identity_binding.fork_operation_id -or
    $manifest.identity_binding.completion_operation_id -ne $manifest.identity_binding.child_operation_id) {
    throw 'real source operation identities are not bound across publication and completion.'
}
if ($null -ne $publication[0].PSObject.Properties['current_generation']) {
    throw 'real trace claims an unproven current workspace generation.'
}
if ($manifest.normalization.generation.rule -ne 'first observed immutable project generation maps to ordinal zero') {
    throw 'real trace manifest uses an unknown generation normalization.'
}
if ($manifest.source.parent_conversation_event -ne 'ForkPublished') {
    throw 'real trace manifest does not identify the parent ForkPublished witness.'
}
if ($manifest.source.admission_record -ne 'fork_prepared' -and
    $manifest.source.admission_record -ne 'fork_admitted_legacy') {
    throw 'real trace manifest uses an unsupported admission record kind.'
}
if ([int64]$manifest.source.fork_admitted_sequence -ge [int64]$manifest.source.fork_completed_sequence) {
    throw 'registry completion does not follow its prepared admission record.'
}
if ($manifest.ordering.basis -ne 'causal projection across independently ordered durable streams' -or
    $manifest.ordering.cross_stream_sequences_compared -ne $false -or
    $manifest.ordering.source_chronology_is_not_projected -ne $true) {
    throw 'real trace manifest does not declare independent-stream causal ordering.'
}

$checker = Join-Path $PSScriptRoot 'check-trace.ps1'
& $checker -TracePath $TracePath
if ($LASTEXITCODE -ne 0) {
    throw "canonical trace checker rejected the real Harness trace (exit $LASTEXITCODE)."
}
Write-Output 'real Harness trace passed canonical finite adapter with provenance manifest.'

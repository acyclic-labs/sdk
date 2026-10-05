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
    'root_task',
    'child_task',
    'child_authority',
    'admission_record_bytes_hex',
    'completion_record_bytes_hex',
    'parent_event_canonical_bytes_hex',
    'child_model_event_canonical_bytes_hex',
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
foreach ($name in @(
    'source_commit',
    'exporter_source_path',
    'exporter_source_sha256',
    'binary_path',
    'binary_sha256',
    'real_checker_path',
    'real_checker_sha256',
    'trace_checker_path',
    'trace_checker_sha256',
    'qualification_path',
    'qualification_sha256'
)) {
    if ($null -eq $manifest.provenance.$name -or [string]::IsNullOrWhiteSpace([string]$manifest.provenance.$name)) {
        throw "real trace manifest is missing provenance.$name."
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

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..\..')).Path
$observedCommit = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $manifest.provenance.source_commit -ne $observedCommit) {
    throw 'real trace provenance is not bound to the current source commit.'
}
foreach ($entry in @(
    @{ Path = $manifest.provenance.exporter_source_path; Digest = $manifest.provenance.exporter_source_sha256; Name = 'exporter source' },
    @{ Path = $manifest.provenance.binary_path; Digest = $manifest.provenance.binary_sha256; Name = 'test binary' },
    @{ Path = $manifest.provenance.real_checker_path; Digest = $manifest.provenance.real_checker_sha256; Name = 'real checker' },
    @{ Path = $manifest.provenance.trace_checker_path; Digest = $manifest.provenance.trace_checker_sha256; Name = 'trace checker' },
    @{ Path = $manifest.provenance.qualification_path; Digest = $manifest.provenance.qualification_sha256; Name = 'qualification script' }
)) {
    $path = [System.IO.Path]::GetFullPath([string]$entry.Path)
    if (-not [System.IO.File]::Exists($path)) { throw "$($entry.Name) provenance path is missing." }
    $observed = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($observed -ne ([string]$entry.Digest).ToLowerInvariant()) {
        throw "$($entry.Name) provenance digest changed."
    }
}

function ParseCanonicalJson([string]$Hex, [string]$Name) {
    $bytes = DecodeHex $Hex $Name
    try { return ([System.Text.Encoding]::UTF8.GetString($bytes) | ConvertFrom-Json) }
    catch { throw "$Name canonical bytes are not valid JSON." }
}

$admissionRecord = ParseCanonicalJson ([string]$manifest.source.admission_record_bytes_hex) 'admission record'
# StoredRecord is an envelope: { version, event: { kind, message } }.
$admissionEvent = $admissionRecord.event
if ($null -eq $admissionEvent -or
    ([string]$admissionEvent.kind -ne 'fork_prepared' -and
     [string]$admissionEvent.kind -ne 'fork_admitted')) {
    throw 'admission record is not a supported durable fork envelope.'
}
if (($manifest.source.admission_record -eq 'fork_prepared' -and [string]$admissionEvent.kind -ne 'fork_prepared') -or
    ($manifest.source.admission_record -eq 'fork_admitted_legacy' -and [string]$admissionEvent.kind -ne 'fork_admitted')) {
    throw 'admission record kind does not match the recorded source kind.'
}
$admissionMessage = $admissionEvent.message
if ($null -eq $admissionMessage) { throw 'admission record has no typed message body.' }
if ([string]$admissionMessage.parent_operation -ne [string]$manifest.source.root_operation -or
    [string]$admissionMessage.child_operation -ne [string]$manifest.identity_binding.child_operation_id -or
    [string]$admissionMessage.fork_operation -ne [string]$manifest.identity_binding.fork_operation_id) {
    throw 'admission record fields do not match the identity binding.'
}
if ([string]$admissionMessage.parent -ne [string]$manifest.source.root_task -or
    [string]$admissionMessage.child -ne [string]$manifest.source.child_task) {
    throw 'admission record task authorities do not match the source binding.'
}

$completionRecord = ParseCanonicalJson ([string]$manifest.source.completion_record_bytes_hex) 'completion record'
if ($null -eq $completionRecord.event -or [string]$completionRecord.event.kind -ne 'fork_completed') {
    throw 'completion record is not a ForkCompleted envelope.'
}
$completionMessage = $completionRecord.event.message
if ($null -eq $completionMessage -or
    [string]$completionMessage.child -ne [string]$manifest.source.child_task -or
    [string]$completionMessage.operation -ne [string]$manifest.identity_binding.child_operation_id) {
    throw 'completion record operation does not match the child operation binding.'
}

$parentEvent = ParseCanonicalJson ([string]$manifest.source.parent_event_canonical_bytes_hex) 'parent event'
if ($null -eq $parentEvent.payload -or
    [string]$parentEvent.payload.kind -ne 'fork_published' -or
    [string]$parentEvent.operation_id -ne [string]$manifest.identity_binding.parent_event_operation_id -or
    [string]$parentEvent.payload.seed.operation_id -ne [string]$manifest.identity_binding.fork_operation_id) {
    throw 'parent ForkPublished event fields do not match the identity binding.'
}
$expectedChildAuthority = $manifest.source.child_authority | ConvertTo-Json -Compress -Depth 20
$observedChildAuthority = $parentEvent.payload.seed.child | ConvertTo-Json -Compress -Depth 20
if ($observedChildAuthority -ne $expectedChildAuthority) {
    throw 'parent ForkPublished child authority does not match the source binding.'
}
if ([string]$parentEvent.payload.seed.child.kind -ne [string]$manifest.source.child_authority.kind -or
    [string]$parentEvent.payload.seed.child.id -ne [string]$manifest.source.child_authority.id) {
    throw 'parent ForkPublished child authority fields do not match the source binding.'
}

$childModelEvent = ParseCanonicalJson ([string]$manifest.source.child_model_event_canonical_bytes_hex) 'child model event'
if ([string]$childModelEvent.kind -ne 'model_started') {
    throw 'child source event is not ModelStarted.'
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
if ($manifest.identity_binding.fork_operation_id -eq $manifest.identity_binding.publication_operation_id -or
    $manifest.identity_binding.fork_operation_id -eq $manifest.identity_binding.child_operation_id -or
    $manifest.identity_binding.publication_operation_id -eq $manifest.identity_binding.child_operation_id) {
    throw 'real source operation identities are not pairwise distinct.'
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

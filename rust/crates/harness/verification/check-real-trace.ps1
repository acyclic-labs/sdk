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
    'child_execution_model_started_step',
    'child_execution_model_started_request_digest_hex',
    'root_task',
    'child_task',
    'child_depth',
    'child_authority',
    'parent_step',
    'task',
    'prompt',
    'child_agent',
    'seed',
    'seed_canonical_bytes_hex',
    'seed_sha256',
    'seed_digest',
    'report',
    'report_canonical_bytes_hex',
    'report_sha256',
    'publication',
    'publication_canonical_bytes_hex',
    'publication_sha256',
    'declaration',
    'declaration_canonical_bytes_hex',
    'declaration_sha256',
    'parent_seed_authority',
    'parent_seed_revision',
    'completion_output',
    'completion_output_ref',
    'completion_output_digest',
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
    'source_tree',
    'source_clean',
    'workspace_manifest_path',
    'workspace_manifest_sha256',
    'lockfile_path',
    'lockfile_sha256',
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

$python = Get-Command python -ErrorAction SilentlyContinue
if ($null -eq $python) {
    throw 'real trace qualification requires Python with the blake3 package for an independent SDK digest check.'
}

function InvokePythonHex([byte[]]$Bytes, [string]$Code, [string]$Name) {
    $temporary = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllBytes($temporary, $Bytes)
        $output = @(& $python.Source -c $Code $temporary 2>&1)
        if ($LASTEXITCODE -ne 0) {
            throw "$Name helper failed: $($output -join ' ')"
        }
        $hex = ($output -join "`n").Trim()
        if ($hex -notmatch '^[0-9a-fA-F]+$') {
            throw "$Name helper returned invalid hexadecimal output."
        }
        return $hex.ToLowerInvariant()
    } finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

$canonicalJsonCode = 'import json,sys; raw=open(sys.argv[1],"rb").read(); value=json.loads(raw.decode("utf-8")); sys.stdout.write(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode("utf-8").hex())'
$blake3Code = 'import blake3,sys; sys.stdout.write(blake3.blake3(open(sys.argv[1],"rb").read()).hexdigest())'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..\..')).Path
$observedCommit = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $manifest.provenance.source_commit -ne $observedCommit) {
    throw 'real trace provenance is not bound to the current source commit.'
}
$observedTree = (& git -C $repoRoot show -s --format=%T HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $manifest.provenance.source_tree -ne $observedTree) {
    throw 'real trace provenance is not bound to the current source tree.'
}
if ($manifest.provenance.source_clean -ne $true) {
    throw 'real trace provenance was not produced from a clean source worktree.'
}
$status = (& git -C $repoRoot status --porcelain --untracked-files=all)
if ($LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace(($status -join "`n"))) {
    throw 'real trace qualification requires a clean tracked and untracked source worktree.'
}
foreach ($entry in @(
    @{ Path = $manifest.provenance.workspace_manifest_path; Digest = $manifest.provenance.workspace_manifest_sha256; Name = 'workspace manifest' },
    @{ Path = $manifest.provenance.lockfile_path; Digest = $manifest.provenance.lockfile_sha256; Name = 'lockfile' },
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
    $canonicalHex = InvokePythonHex $bytes $canonicalJsonCode $Name
    if ($canonicalHex -ne (Hex $bytes)) {
        throw "$Name bytes are not canonical JSON."
    }
    try { return ([System.Text.Encoding]::UTF8.GetString($bytes) | ConvertFrom-Json) }
    catch { throw "$Name canonical bytes are not valid JSON." }
}

function CompactJson($Value) {
    if ($null -eq $Value) { return 'null' }
    return ($Value | ConvertTo-Json -Compress -Depth 100)
}

function RequireJsonEqual($Observed, $Expected, [string]$Name) {
    if ((CompactJson $Observed) -ne (CompactJson $Expected)) {
        throw "$Name does not match its authoritative source value."
    }
}

function JsonByteArrayHex($Value, [string]$Name) {
    if ($null -eq $Value) { throw "$Name is missing." }
    try {
        $bytes = [byte[]]@($Value | ForEach-Object { [byte]$_ })
        return (Hex $bytes)
    } catch {
        throw "$Name is not a byte array."
    }
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
if ([int64]$admissionMessage.parent_step -ne [int64]$manifest.source.parent_step -or
    [string]$admissionMessage.task -ne [string]$manifest.source.task -or
    [string]$admissionMessage.prompt -ne [string]$manifest.source.prompt -or
    [string]$admissionMessage.child_agent -ne [string]$manifest.source.child_agent) {
    throw 'admission record task, step, prompt, or child agent fields do not match the source binding.'
}

$admissionSeed = $admissionMessage.seed
$admissionReport = $admissionMessage.report
$admissionPublication = $admissionMessage.publication
$admissionDeclaration = $admissionMessage.declaration
if ($null -eq $admissionSeed -or $null -eq $admissionReport -or
    $null -eq $admissionPublication -or $null -eq $admissionDeclaration) {
    throw 'admission record omitted a required typed seed, report, publication, or declaration.'
}
$seedCanonical = ParseCanonicalJson ([string]$manifest.source.seed_canonical_bytes_hex) 'seed'
$reportCanonical = ParseCanonicalJson ([string]$manifest.source.report_canonical_bytes_hex) 'report'
$publicationCanonical = ParseCanonicalJson ([string]$manifest.source.publication_canonical_bytes_hex) 'publication'
$declarationCanonical = ParseCanonicalJson ([string]$manifest.source.declaration_canonical_bytes_hex) 'declaration'
RequireJsonEqual $admissionSeed $seedCanonical 'admission seed'
RequireJsonEqual $admissionReport $reportCanonical 'admission report'
RequireJsonEqual $admissionPublication $publicationCanonical 'admission publication'
RequireJsonEqual $admissionDeclaration $declarationCanonical 'admission declaration'
RequireJsonEqual $manifest.source.seed $seedCanonical 'manifest seed'
RequireJsonEqual $manifest.source.report $reportCanonical 'manifest report'
RequireJsonEqual $manifest.source.publication $publicationCanonical 'manifest publication'
RequireJsonEqual $manifest.source.declaration $declarationCanonical 'manifest declaration'
if ((JsonByteArrayHex $admissionMessage.seed_digest 'admission seed digest') -ne
    (JsonByteArrayHex $manifest.source.seed_digest 'manifest seed digest')) {
    throw 'admission seed digest does not match the source binding.'
}
$recomputedSeedDigest = InvokePythonHex (DecodeHex ([string]$manifest.source.seed_canonical_bytes_hex) 'seed') $blake3Code 'seed digest'
if ($recomputedSeedDigest -ne (JsonByteArrayHex $admissionMessage.seed_digest 'admission seed digest')) {
    throw 'admission seed digest does not match the SDK blake3 digest of canonical seed bytes.'
}
if ([string]$admissionPublication.operation_id -ne [string]$manifest.identity_binding.publication_operation_id -or
    [string]$admissionPublication.parent_operation -ne [string]$manifest.source.root_operation -or
    [int64]$admissionPublication.step -ne [int64]$manifest.source.parent_step) {
    throw 'admission publication operation, parent, or step does not match the source binding.'
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
RequireJsonEqual $completionMessage.output $manifest.source.completion_output 'completion output'
RequireJsonEqual $completionMessage.output_ref $manifest.source.completion_output_ref 'completion output reference'
RequireJsonEqual $completionMessage.output_digest $manifest.source.completion_output_digest 'completion output digest'
if ($null -eq $completionMessage.output_digest) {
    throw 'completion record has no durable output digest.'
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
RequireJsonEqual $parentEvent.payload.seed $seedCanonical 'parent ForkPublished seed'
RequireJsonEqual $parentEvent.payload.seed.parent $manifest.source.parent_seed_authority 'parent ForkPublished parent authority'
if ([int64]$parentEvent.payload.seed.parent_revision -ne [int64]$manifest.source.parent_seed_revision) {
    throw 'parent ForkPublished parent revision does not match the source binding.'
}

$childModelEvent = ParseCanonicalJson ([string]$manifest.source.child_model_event_canonical_bytes_hex) 'child model event'
if ([string]$childModelEvent.kind -ne 'model_started') {
    throw 'child source event is not ModelStarted.'
}
if ([int64]$childModelEvent.step -ne [int64]$manifest.source.child_execution_model_started_step) {
    throw 'child ModelStarted step does not match the source binding.'
}
if ((JsonByteArrayHex $childModelEvent.request_digest 'child ModelStarted request digest') -ne
    ([string]$manifest.source.child_execution_model_started_request_digest_hex).ToLowerInvariant()) {
    throw 'child ModelStarted request digest does not match the source binding.'
}
$projectCaptures = @($reportCanonical.captures | Where-Object {
    $_.kind -eq 'captured' -and $null -ne $_.value -and
    $null -ne $_.value.revision -and $_.value.revision.kind -eq 'project'
})
if ($projectCaptures.Count -ne 1) {
    throw 'fork report does not contain exactly one captured project generation.'
}
RequireJsonEqual $projectCaptures[0].value.revision.reference.generation `
    $manifest.normalization.generation.raw_captured_generation `
    'captured project generation'

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
foreach ($record in @(
    @{ Bytes = $manifest.source.seed_canonical_bytes_hex; Digest = $manifest.source.seed_sha256; Name = 'seed' },
    @{ Bytes = $manifest.source.report_canonical_bytes_hex; Digest = $manifest.source.report_sha256; Name = 'report' },
    @{ Bytes = $manifest.source.publication_canonical_bytes_hex; Digest = $manifest.source.publication_sha256; Name = 'publication' },
    @{ Bytes = $manifest.source.declaration_canonical_bytes_hex; Digest = $manifest.source.declaration_sha256; Name = 'declaration' }
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
if ($completed[0].outcome_durable -ne $true) {
    throw 'real completion trace does not assert a durable outcome.'
}
if ($admission[0].parent -ne 1 -or $admission[0].child -ne 2 -or
    $publication[0].parent -ne 1 -or $publication[0].child -ne 2 -or
    $started[0].agent -ne 2 -or $completed[0].agent -ne 2) {
    throw 'real trace normalization labels do not match the declared task/agent witnesses.'
}
if ([int64]$admission[0].depth -ne [int64]$manifest.source.child_depth) {
    throw 'real trace child depth does not match the durable session witness.'
}
if ([string]$manifest.normalization.task_ids.'1' -ne [string]$manifest.source.root_task -or
    [string]$manifest.normalization.task_ids.'2' -ne [string]$manifest.source.child_task -or
    [string]$manifest.normalization.agent_ids.'2' -ne [string]$manifest.source.child_agent) {
    throw 'real trace normalization maps do not match the durable identity witnesses.'
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

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string]$PackageRoot,
    [Parameter(Mandatory = $true)] [string]$ReceiptPath,
    [string]$SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..\..')).Path,
    [string]$OutputRoot = '',
    [switch]$SkipTamperCheck
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-FullPath([string]$Path) {
    return [IO.Path]::GetFullPath((Resolve-Path -LiteralPath $Path).Path)
}

function Is-PortablePath([string]$Path) {
    return -not [string]::IsNullOrWhiteSpace($Path) -and
        -not [IO.Path]::IsPathRooted($Path) -and
        $Path -notmatch '\\' -and $Path -notmatch '(^|/)\.\.?(/|$)' -and
        $Path -notmatch '//'
}

function Get-ArtifactPath([string]$Root, [string]$RelativePath) {
    if (-not (Is-PortablePath $RelativePath)) { throw "Artifact path is not portable: $RelativePath" }
    $rootFull = (Resolve-FullPath $Root).TrimEnd('\') + '\'
    $full = [IO.Path]::GetFullPath((Join-Path $Root ($RelativePath -replace '/', [IO.Path]::DirectorySeparatorChar)))
    if (-not $full.StartsWith($rootFull, [StringComparison]::OrdinalIgnoreCase)) { throw "Artifact path escapes package root: $RelativePath" }
    return $full
}

function Read-U16([byte[]]$Bytes, [int]$Offset) {
    if ($Offset -lt 0 -or $Offset + 2 -gt $Bytes.Length) { throw 'PE field is outside the image' }
    return [BitConverter]::ToUInt16($Bytes, $Offset)
}

function Read-U32([byte[]]$Bytes, [int]$Offset) {
    if ($Offset -lt 0 -or $Offset + 4 -gt $Bytes.Length) { throw 'PE field is outside the image' }
    return [BitConverter]::ToUInt32($Bytes, $Offset)
}

function Convert-RvaToFileOffset([byte[]]$Bytes, [int]$SectionOffset, [int]$SectionCount, [uint32]$Rva) {
    for ($index = 0; $index -lt $SectionCount; $index++) {
        $header = $SectionOffset + (40 * $index)
        $virtualSize = Read-U32 $Bytes ($header + 8)
        $virtualAddress = Read-U32 $Bytes ($header + 12)
        $rawSize = Read-U32 $Bytes ($header + 16)
        $rawPointer = Read-U32 $Bytes ($header + 20)
        $span = [Math]::Max($virtualSize, $rawSize)
        if ($Rva -ge $virtualAddress -and $Rva -lt ($virtualAddress + $span)) {
            $offset = [int]($rawPointer + ($Rva - $virtualAddress))
            if ($offset -lt 0 -or $offset -ge $Bytes.Length) { throw 'PE RVA points outside the image' }
            return $offset
        }
    }
    throw "PE RVA has no section mapping: $Rva"
}

function Read-PeCString([byte[]]$Bytes, [int]$Offset) {
    $end = $Offset
    while ($end -lt $Bytes.Length -and $Bytes[$end] -ne 0) { $end++ }
    if ($end -ge $Bytes.Length) { throw 'PE string is unterminated' }
    return [Text.Encoding]::ASCII.GetString($Bytes, $Offset, $end - $Offset)
}

function Test-PeImage([string]$Path) {
    $bytes = [IO.File]::ReadAllBytes($Path)
    if ($bytes.Length -lt 0x40 -or $bytes[0] -ne 0x4d -or $bytes[1] -ne 0x5a) { throw "PE image has no MZ header: $Path" }
    $peOffset = [int](Read-U32 $bytes 0x3c)
    if ($peOffset -lt 0 -or $peOffset + 24 -gt $bytes.Length -or $bytes[$peOffset] -ne 0x50 -or $bytes[$peOffset + 1] -ne 0x45 -or $bytes[$peOffset + 2] -ne 0 -or $bytes[$peOffset + 3] -ne 0) { throw "PE image has no valid PE signature: $Path" }
    $sectionCount = [int](Read-U16 $bytes ($peOffset + 6))
    $optionalSize = [int](Read-U16 $bytes ($peOffset + 20))
    $optionalOffset = $peOffset + 24
    $magic = Read-U16 $bytes $optionalOffset
    if ($magic -ne 0x10b -and $magic -ne 0x20b) { throw "PE image has an unsupported optional header: $Path" }
    $sectionOffset = $optionalOffset + $optionalSize
    $directoryOffset = $optionalOffset + $(if ($magic -eq 0x20b) { 112 } else { 96 })
    $exportRva = Read-U32 $bytes $directoryOffset
    $exportSize = Read-U32 $bytes ($directoryOffset + 4)
    if ($exportRva -eq 0 -or $exportSize -eq 0) { throw "PE image has no export directory: $Path" }
    $exportOffset = Convert-RvaToFileOffset $bytes $sectionOffset $sectionCount $exportRva
    $nameCount = [int](Read-U32 $bytes ($exportOffset + 24))
    $namesRva = Read-U32 $bytes ($exportOffset + 32)
    $namesOffset = Convert-RvaToFileOffset $bytes $sectionOffset $sectionCount $namesRva
    $exportNames = @()
    for ($index = 0; $index -lt $nameCount; $index++) {
        $nameRva = Read-U32 $bytes ($namesOffset + (4 * $index))
        $nameOffset = Convert-RvaToFileOffset $bytes $sectionOffset $sectionCount $nameRva
        $exportNames += Read-PeCString $bytes $nameOffset
    }
    if ('acyclic_embedded_abi_version' -notin $exportNames) { throw "PE image does not export acyclic_embedded_abi_version: $Path" }
    return [ordered]@{ mz = $true; pe = $true; export = 'acyclic_embedded_abi_version' }
}

function Assert-ReceiptPackage([object]$Receipt, [string]$Root) {
    if ($Receipt.schema -ne 'acyclic.sdk.embedded.release-abi.v1') { throw 'Unsupported embedded release ABI receipt schema' }
    if ($Receipt.status -ne 'qualified-local-release-package') { throw 'Receipt is not qualified' }
    if ($Receipt.source.revision -notmatch '^[0-9a-fA-F]{40}$' -or $Receipt.source.source_digest -notmatch '^[0-9a-fA-F]{64}$') { throw 'Rust source binding is not immutable' }
    if ($Receipt.source.crate -ne 'rust/crates/sdk-embedded-prototype') { throw 'Receipt is not bound to the Rust embedded crate' }
    if ([int]$Receipt.source.abi_version -lt 1) { throw 'ABI version is invalid' }
    if ($Receipt.toolchain.offline_locked -ne $true) { throw 'Release recipe is not locked and offline' }
    foreach ($name in @('rust_unit_tests','c_consumer','python_ctypes_consumer','cpp_cross_thread_consumer','cmake_ctest','clean_prefix_cpp_consumer')) {
        if ($Receipt.foreign_consumers.$name.status -ne 'passed') { throw "Foreign consumer did not pass: $name" }
    }
    foreach ($name in @('c','python','cpp')) {
        $consumer = $Receipt.consumer_receipts.$name
        if ($consumer.status -ne 'passed' -or [string]::IsNullOrWhiteSpace($consumer.language) -or [string]::IsNullOrWhiteSpace($consumer.source) -or [string]::IsNullOrWhiteSpace($consumer.command) -or $consumer.package_artifact -ne 'bin/acyclic_sdk_embedded_prototype.dll' -or @($consumer.checks).Count -eq 0) {
            throw "Actual foreign consumer receipt is incomplete: $name"
        }
    }
    if ([int]$Receipt.foreign_consumers.cmake_ctest.passed -ne [int]$Receipt.foreign_consumers.cmake_ctest.total) { throw 'CMake test receipt is incomplete' }
    if ([int]$Receipt.reproducibility.independent_clean_builds -ne 2 -or $Receipt.reproducibility.source_digests_equal -ne $true -or $Receipt.reproducibility.artifact_hashes_equal -ne $true) { throw 'Clean build reproducibility receipt is incomplete' }
    if ([int]$Receipt.reproducibility.pe_coff_timestamp_normalized -ne 0 -or [int]$Receipt.reproducibility.pe_debug_timestamps_normalized -ne 0 -or $Receipt.reproducibility.codeview_pdb_guid_normalized -ne $true) { throw 'PE normalization receipt is incomplete' }
    if ($Receipt.validation.normalized_pe.mz -ne $true -or $Receipt.validation.normalized_pe.pe -ne $true -or $Receipt.validation.normalized_pe.export -ne 'acyclic_embedded_abi_version' -or $Receipt.validation.normalized_consumers.c -ne 'passed' -or $Receipt.validation.normalized_consumers.python -ne 'passed' -or $Receipt.validation.normalized_consumers.cpp -ne 'passed' -or $Receipt.validation.tampered_artifact_rejected -ne $true) { throw 'Runtime validation receipt is incomplete' }
    if ($Receipt.publication.registry_published -ne $false -or $Receipt.publication.production_deployed -ne $false) { throw 'Release ABI receipt claims publication or deployment' }

    $dllRelative = 'bin/acyclic_sdk_embedded_prototype.dll'
    $dllPath = Get-ArtifactPath $Root $dllRelative
    if (-not (Test-Path -LiteralPath $dllPath -PathType Leaf)) { throw "Runtime artifact is missing: $dllRelative" }
    $pe = Test-PeImage $dllPath
    $hashes = @{}
    foreach ($property in $Receipt.artifacts.psobject.Properties) {
        $relative = [string]$property.Name
        $path = Get-ArtifactPath $Root $relative
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Artifact is missing: $relative" }
        $actual = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actual -ne ([string]$property.Value).ToLowerInvariant()) { throw "Artifact hash mismatch: $relative" }
        $hashes[$relative] = $actual
    }
    return [ordered]@{ package_root = (Resolve-FullPath $Root); pe = $pe; artifact_count = $hashes.Count; artifact_hashes = $hashes }
}

function Get-UnifiedArtifactDigest([object]$Artifacts) {
    $canonical = New-Object Text.StringBuilder
    $names = @($Artifacts.psobject.Properties.Name)
    [Array]::Sort($names, [StringComparer]::Ordinal)
    foreach ($name in $names) {
        [void]$canonical.Append([string]$name)
        [void]$canonical.Append([char]0)
        [void]$canonical.Append('sha256:')
        [void]$canonical.Append(([string]$Artifacts.psobject.Properties[$name].Value).ToLowerInvariant())
        [void]$canonical.Append([char]0)
    }
    $digest = [Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($canonical.ToString()))
    return 'sha256:' + [Convert]::ToHexString($digest).ToLowerInvariant()
}

$sourceFull = Resolve-FullPath $SourceRoot
$packageFull = Resolve-FullPath $PackageRoot
if ([string]::IsNullOrWhiteSpace($OutputRoot)) { $OutputRoot = $PackageRoot }
$outputFull = Resolve-FullPath $OutputRoot
$sourcePrefix = $sourceFull.TrimEnd('\') + '\'
$outputPrefix = $outputFull.TrimEnd('\') + '\'
if ($outputFull.Equals($sourceFull, [StringComparison]::OrdinalIgnoreCase) -or $outputFull.StartsWith($sourcePrefix, [StringComparison]::OrdinalIgnoreCase) -or $packageFull.StartsWith($sourcePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Generation output must be isolated from the Rust source root'
}
$receipt = Get-Content -Raw -LiteralPath $ReceiptPath | ConvertFrom-Json
$schemaPath = Join-Path $PSScriptRoot 'release-abi-receipt.schema.json'
$schema = Get-Content -Raw -LiteralPath $schemaPath | ConvertFrom-Json
if ($schema.'$id' -ne 'https://sdk.acyclic.dev/schemas/embedded-release-abi-receipt.v1.json') { throw 'Receipt schema identity changed unexpectedly' }
$validated = Assert-ReceiptPackage $receipt $packageFull
$unifiedEvidencePath = Join-Path $PSScriptRoot 'release-abi-generation-evidence.json'
$unifiedEvidence = Get-Content -Raw -LiteralPath $unifiedEvidencePath | ConvertFrom-Json
if ($unifiedEvidence.schema -ne 'acyclic.sdk.qualification.evidence.v1' -or $unifiedEvidence.language -ne 'cpp' -or $unifiedEvidence.source_revision -ne $receipt.source.revision -or $unifiedEvidence.contract_digest -ne ('sha256:' + $receipt.source.source_digest) -or $unifiedEvidence.artifact_digest -ne (Get-UnifiedArtifactDigest $receipt.artifacts) -or $unifiedEvidence.embedded.status -ne 'qualified' -or $unifiedEvidence.install.status -ne 'qualified' -or @($unifiedEvidence.embedded.tests).Count -lt 5) {
    throw 'Unified Rust qualification evidence is not bound to the embedded ABI receipt'
}
$tamperRejected = $null
if (-not $SkipTamperCheck) {
    $tempRoot = Join-Path ([IO.Path]::GetTempPath()) "acyclic-release-abi-tamper-$PID"
    New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null
    try {
        $tampered = Join-Path $tempRoot 'package'
        Copy-Item -LiteralPath $packageFull -Destination $tampered -Recurse
        $tamperedDll = Join-Path $tampered 'bin\acyclic_sdk_embedded_prototype.dll'
        $tamperedBytes = [IO.File]::ReadAllBytes($tamperedDll)
        if ($tamperedBytes.Length -lt 0x200) { throw 'Runtime artifact is unexpectedly small for tamper test' }
        $tamperedBytes[0x1ff] = $tamperedBytes[0x1ff] -bxor 0x01
        [IO.File]::WriteAllBytes($tamperedDll, $tamperedBytes)
        try { Assert-ReceiptPackage $receipt $tampered | Out-Null; throw 'Tampered artifact was accepted' } catch { $tamperRejected = $_.Exception.Message -like '*Artifact hash mismatch*' }
        if (-not $tamperRejected) { throw 'Tamper rejection did not identify the modified artifact' }
    } finally {
        if (Test-Path -LiteralPath $tempRoot) { Remove-Item -LiteralPath $tempRoot -Recurse -Force }
    }
}
[ordered]@{
    schema = 'acyclic.sdk.embedded.release-abi-validation.v1'
    status = 'passed'
    source_output_isolated = $true
    rust_validator_schema = (Resolve-FullPath $schemaPath)
    unified_rust_evidence = (Resolve-FullPath $unifiedEvidencePath)
    unified_evidence_bound = $true
    package = $validated
    tamper_rejected = if ($SkipTamperCheck) { $null } else { $tamperRejected }
} | ConvertTo-Json -Depth 10

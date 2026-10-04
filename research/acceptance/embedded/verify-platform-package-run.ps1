[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ReceiptRoot,
    [string]$SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")),
    [string]$OutputPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Read-Json([string]$Path) {
    try {
        return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    } catch {
        throw "Cannot read JSON receipt '$Path': $($_.Exception.Message)"
    }
}

function Get-Sha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-CanonicalArtifactDigest($Artifacts) {
    $lines = foreach ($property in ($Artifacts.psobject.Properties | Sort-Object Name)) {
        "$($property.Name) $($property.Value.ToLowerInvariant())"
    }
    $bytes = [Text.Encoding]::UTF8.GetBytes(($lines -join "`n"))
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Assert-Hex([string]$Value, [int]$Length, [string]$Label) {
    if ([string]::IsNullOrWhiteSpace($Value) -or $Value -notmatch "^[0-9a-fA-F]{$Length}$") {
        throw "$Label must be a $Length-character hexadecimal digest"
    }
}

function Assert-Equal([string]$Actual, [string]$Expected, [string]$Label) {
    if ($Actual -cne $Expected) { throw "$Label mismatch: expected '$Expected', got '$Actual'" }
}

function Assert-PortableRelative([string]$Value, [string]$Label) {
    if ([string]::IsNullOrWhiteSpace($Value) -or
        $Value -match '^(?:[A-Za-z]:[\\/]|[\\/])' -or
        $Value -match '(^|[\\/])\.\.([\\/]|$)') {
        throw "$Label must be a portable relative path: '$Value'"
    }
}

$rootPath = (Resolve-Path -LiteralPath $ReceiptRoot).Path
$sourcePath = (Resolve-Path -LiteralPath $SourceRoot).Path
$receiptFiles = @(Get-ChildItem -LiteralPath $rootPath -Recurse -File -Filter "platform-package.json" | Sort-Object FullName)
if ($receiptFiles.Count -ne 6) {
    throw "Expected exactly six platform-package.json receipts under $rootPath, found $($receiptFiles.Count)"
}

$expectedTargets = [ordered]@{
    "windows-x64-msvc" = "x86_64-pc-windows-msvc"
    "windows-arm64-msvc" = "aarch64-pc-windows-msvc"
    "linux-x64-musl" = "x86_64-unknown-linux-musl"
    "linux-arm64-musl" = "aarch64-unknown-linux-musl"
    "macos-x64" = "x86_64-apple-darwin"
    "macos-arm64" = "aarch64-apple-darwin"
}
$targetToPlatform = @{}
foreach ($platform in $expectedTargets.Keys) { $targetToPlatform[$expectedTargets[$platform]] = $platform }

$actualRevision = (& git -C $sourcePath rev-parse HEAD).Trim()
Assert-Hex $actualRevision 40 "source checkout revision"
$dirtyEntries = @(& git -C $sourcePath status --porcelain --untracked-files=all)
if ($dirtyEntries.Count -ne 0) {
    throw "Source checkout is dirty; embedded ABI run provenance requires a clean tree (first entry: $($dirtyEntries[0]))"
}
$records = @()
$seenTargets = @{}
$runSourceDigest = $null

foreach ($receiptFile in $receiptFiles) {
    $receipt = Read-Json $receiptFile.FullName
    if ($receipt.schema -ne "acyclic.sdk.embedded.platform-package.v1") { throw "Unsupported receipt schema in $($receiptFile.FullName)" }
    $target = [string]$receipt.target
    if (-not $targetToPlatform.ContainsKey($target)) { throw "Unexpected embedded target '$target' in $($receiptFile.FullName)" }
    if ($seenTargets.ContainsKey($target)) { throw "Duplicate embedded target '$target'" }
    $seenTargets[$target] = $true
    if ($receipt.status -ne "passed") { throw "Embedded target '$target' did not execute successfully: $($receipt.status)" }
    Assert-Equal ([string]$receipt.source_revision) $actualRevision "source revision for $target"

    $sourceDigestLines = foreach ($input in @($receipt.source_inputs)) {
        Assert-PortableRelative ([string]$input) "source input for $target"
        $inputPath = Join-Path $sourcePath ($input.Replace('/', [IO.Path]::DirectorySeparatorChar))
        if (-not (Test-Path -LiteralPath $inputPath -PathType Leaf)) { throw "Missing source input '$input' for $target" }
        "$input $(Get-Sha256 $inputPath)"
    }
    $sourceDigestBytes = [Text.Encoding]::UTF8.GetBytes(($sourceDigestLines -join "`n"))
    $sourceDigest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($sourceDigestBytes)).ToLowerInvariant()
    Assert-Equal ([string]$receipt.source_digest) "sha256:$sourceDigest" "source digest for $target"
    if ($null -eq $runSourceDigest) { $runSourceDigest = "sha256:$sourceDigest" }
    Assert-Equal ([string]$receipt.source_digest) $runSourceDigest "run source digest for $target"

    $receiptDirectory = Split-Path -Parent $receiptFile.FullName
    $packageRoot = Join-Path $receiptDirectory "prefix"
    if (-not (Test-Path -LiteralPath $packageRoot -PathType Container)) { throw "Installed package prefix is missing for $target" }
    foreach ($artifact in $receipt.artifacts.psobject.Properties) {
        Assert-PortableRelative ([string]$artifact.Name) "artifact path for $target"
        Assert-Hex ([string]$artifact.Value) 64 "artifact hash $target/$($artifact.Name)"
        $artifactPath = Join-Path $packageRoot ($artifact.Name.Replace('/', [IO.Path]::DirectorySeparatorChar))
        if (-not (Test-Path -LiteralPath $artifactPath -PathType Leaf)) { throw "Receipt names missing installed artifact '$($artifact.Name)' for $target" }
        Assert-Equal (Get-Sha256 $artifactPath) ([string]$artifact.Value).ToLowerInvariant() "installed artifact hash $target/$($artifact.Name)"
    }
    $runtimeProperty = $receipt.artifacts.psobject.Properties[[string]$receipt.runtime_artifact]
    if ($null -eq $runtimeProperty) { throw "Runtime artifact '$($receipt.runtime_artifact)' is not included in the package for $target" }
    foreach ($name in @("c", "python", "cpp")) {
        $consumer = $receipt.consumers.psobject.Properties[$name].Value
        if ($consumer.status -ne "passed" -or -not [bool]$consumer.invoked -or [int]$consumer.exit_code -ne 0) {
            throw "Consumer '$name' for $target was not invoked successfully"
        }
        Assert-Equal ([string]$consumer.source_revision) $actualRevision "$name source revision for $target"
        Assert-Hex ([string]$consumer.source_sha256) 64 "$name source hash for $target"
        Assert-PortableRelative ([string]$consumer.source) "$name source path for $target"
        $consumerSource = Join-Path $sourcePath ($consumer.source.Replace('/', [IO.Path]::DirectorySeparatorChar))
        if (-not (Test-Path -LiteralPath $consumerSource -PathType Leaf)) { throw "Missing consumer source '$($consumer.source)' for $target" }
        Assert-Equal (Get-Sha256 $consumerSource) ([string]$consumer.source_sha256).ToLowerInvariant() "$name source hash for $target"
        $artifactProperty = $receipt.artifacts.psobject.Properties[[string]$consumer.package_artifact]
        Assert-PortableRelative ([string]$consumer.package_artifact) "$name package artifact path for $target"
        if ($null -eq $artifactProperty) { throw "Consumer '$name' references an artifact outside the package for $target" }
        Assert-Equal ([string]$consumer.package_artifact_sha256) ([string]$artifactProperty.Value).ToLowerInvariant() "$name package hash for $target"
        $requiredChecks = switch ($name) {
            "c" { @("layout", "append", "read", "release", "stale_handles") }
            "python" { @("append", "follow", "owned_buffers", "cancel", "stale_handles") }
            "cpp" { @("blocked_pull_wakeup", "cross_thread_cancel", "clean_prefix_install") }
        }
        foreach ($check in $requiredChecks) {
            if (@($consumer.checks) -notcontains $check) { throw "Consumer '$name' is missing behavior check '$check' for $target" }
        }
    }
    if ($receipt.ctest -ne "passed" -or $receipt.clean_prefix -ne "passed") { throw "CTest or clean-prefix verification did not pass for $target" }

    $platform = $targetToPlatform[$target]
    $records += [ordered]@{
        platform = $platform
        target = $target
        receipt = [IO.Path]::GetRelativePath($rootPath, $receiptFile.FullName).Replace('\', '/')
        receipt_sha256 = Get-Sha256 $receiptFile.FullName
        artifact_digest = "sha256:$(Get-CanonicalArtifactDigest $receipt.artifacts)"
        source_revision = [string]$receipt.source_revision
        source_digest = [string]$receipt.source_digest
        consumers = [ordered]@{ c = [string]$receipt.consumers.c.status; python = [string]$receipt.consumers.python.status; cpp = [string]$receipt.consumers.cpp.status }
    }
}

foreach ($platform in $expectedTargets.Keys) {
    if (-not ($records | Where-Object platform -eq $platform)) { throw "Missing required platform receipt '$platform'" }
}
$records = @($records | Sort-Object platform)
$recordLines = foreach ($record in $records) { "$($record.platform) $($record.target) $($record.receipt_sha256) $($record.artifact_digest)" }
$recordBytes = [Text.Encoding]::UTF8.GetBytes(($recordLines -join "`n"))
$artifactDigest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($recordBytes)).ToLowerInvariant()

if ([string]::IsNullOrWhiteSpace($OutputPath)) { $OutputPath = Join-Path $rootPath "embedded-abi-run-manifest.json" }
$manifest = [ordered]@{
    schema = "acyclic.sdk.embedded.run-manifest.v1"
    status = "qualified"
    source_revision = $actualRevision
    source_digest = $runSourceDigest
    platform_count = $records.Count
    platforms = $records
    artifact_digest = "sha256:$artifactDigest"
}
$outputDirectory = Split-Path -Parent ([IO.Path]::GetFullPath($OutputPath))
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
$manifest | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $OutputPath -Encoding utf8NoBOM
Write-Host "Embedded ABI run qualified: $OutputPath"

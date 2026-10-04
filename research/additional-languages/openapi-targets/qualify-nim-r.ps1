[CmdletBinding()]
param(
    [string] $Root,
    [Parameter(Mandatory = $true)] [ValidateSet('nim', 'r')] [string] $TargetId,
    [switch] $SkipGeneration
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') '..')).Path
}
$jarSha256 = '41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE'
$families = @('actors', 'workers', 'stream', 'objects', 'inference')
$specRoot = Join-Path $Root 'research/additional-languages/target/bash'
$target = Join-Path $Root ("research/additional-languages/target/" + $TargetId)
$packages = Join-Path $target 'packages'
$jar = Join-Path $specRoot 'openapi-generator-cli-7.25.0.jar'
$zipWriter = Join-Path $scriptDir 'write-deterministic-zip.ps1'

New-Item -ItemType Directory -Force -Path $target, $packages | Out-Null
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw "Missing pinned OpenAPI Generator jar: $jar" }
if ((Get-FileHash $jar -Algorithm SHA256).Hash -ne $jarSha256) { throw 'OpenAPI Generator checksum mismatch' }

if (-not $SkipGeneration) {
    foreach ($family in $families) {
        $spec = Join-Path $specRoot "$family.json"
        $package = Join-Path $packages $family
        if (-not (Test-Path -LiteralPath $spec -PathType Leaf)) { throw "Missing Rust OpenAPI projection: $spec" }
        if (Test-Path -LiteralPath $package) { Remove-Item -LiteralPath $package -Recurse -Force }
        New-Item -ItemType Directory -Force -Path $package | Out-Null
        $name = "acyclic_" + $family + "_" + $TargetId
        if ($TargetId -eq 'nim') {
            New-Item -ItemType Directory -Force -Path (Join-Path $package 'openapiclient/models') | Out-Null
        }
        $props = if ($TargetId -eq 'nim') { "packageVersion=0.1.0,packageName=$name" } else { "packageVersion=0.1.0,packageName=acyclic.$family.r" }
        $packageName = if ($TargetId -eq 'nim') { $name } else { "acyclic.$family.r" }
        & java '-Xmx768m' '-jar' $jar generate -i $spec -g $TargetId -o $package --package-name $packageName "--additional-properties=$props" | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "$TargetId package generation failed: $family" }
        if ($TargetId -eq 'nim') {
            $nimble = @(
                "# Generated from Rust-owned OpenAPI projection: $family",
                'version = "0.1.0"',
                'author = "Acyclic contributors"',
                'description = "Rust-derived Acyclic HTTP client"',
                'license = "Apache-2.0"',
                'srcDir = "."',
                'requires "nim >= 2.0.0"'
            ) -join [Environment]::NewLine
            Set-Content -LiteralPath (Join-Path $package "$name.nimble") -Value ($nimble + [Environment]::NewLine) -Encoding utf8NoBOM
        } else {
            # OpenAPI Generator's R template emits a Travis-only cache path
            # (`/home/travis/R/Library`). It is CI metadata rather than package
            # content and fails the portable-artifact gate, so the Rust-owned
            # adapter removes it before hashing/installing the package.
            $travis = Join-Path $package '.travis.yml'
            if (Test-Path -LiteralPath $travis -PathType Leaf) { Remove-Item -LiteralPath $travis -Force }
        }
    }
}

if ($TargetId -eq 'nim') {
    $adapter = Join-Path $scriptDir 'apply-nim-compatibility.ps1'
    if (-not (Test-Path -LiteralPath $adapter -PathType Leaf)) { throw "Missing Rust-owned Nim compatibility adapter: $adapter" }
    & pwsh -NoProfile -File $adapter -PackagesRoot $packages
    if ($LASTEXITCODE -ne 0) { throw 'Nim compatibility adaptation failed' }
}

$operationChecks = @{}
$runtimeFiles = @{}
foreach ($family in $families) {
    $specPath = Join-Path $specRoot "$family.json"
    $package = Join-Path $packages $family
    if (-not (Test-Path -LiteralPath $package -PathType Container)) { throw "Generated package is missing: $family" }
    $spec = Get-Content -LiteralPath $specPath -Raw | ConvertFrom-Json
    $operations = @(
        foreach ($path in $spec.paths.PSObject.Properties) {
            foreach ($method in $path.Value.PSObject.Properties) {
                if ($method.Name -in @('parameters', 'summary', 'description')) { continue }
                if ($null -ne $method.Value.operationId) { [string]$method.Value.operationId }
            }
        }
    )
    if ($operations.Count -eq 0) { throw "Rust OpenAPI projection has no operation IDs: $family" }
    $sourceFiles = @(Get-ChildItem -LiteralPath $package -Recurse -File | Where-Object { $_.Extension -in @('.nim', '.R', '.r') })
    if ($sourceFiles.Count -eq 0) { throw "Generated source is missing: $family" }
    $source = (($sourceFiles | ForEach-Object { Get-Content -LiteralPath $_.FullName -Raw }) -join [Environment]::NewLine)
    foreach ($operation in $operations) {
        if ($source.IndexOf($operation, [StringComparison]::OrdinalIgnoreCase) -lt 0) {
            throw "Generated $TargetId source does not contain Rust operationId '$operation': $family"
        }
    }
    $operationChecks[$family] = $operations.Count
    if ($family -eq 'stream') {
        if ($source -notmatch '(?i)(stream|tail|follow|read|children)') { throw "Stream package has no streaming operation evidence" }
        $streamMethods = @($spec.paths.PSObject.Properties | ForEach-Object { $_.Value.PSObject.Properties } | Where-Object { $_.Name -notin @('parameters', 'summary', 'description') } | Where-Object { $_.Value.operationId } | ForEach-Object { $_.Value.operationId })
        if ($streamMethods.Count -lt 1) { throw 'Rust stream projection has no operation evidence' }
    }
    if ($source -notmatch '(?i)(json|toJson|parseJson|application/json)') { throw "JSON serialization evidence is missing: $family" }
    if ($TargetId -eq 'nim' -and -not (Get-ChildItem -LiteralPath $package -Filter '*.nimble' -File)) { throw "Nimble metadata is missing: $family" }
    if ($TargetId -eq 'r' -and ((-not (Test-Path -LiteralPath (Join-Path $package 'DESCRIPTION'))) -or (-not (Test-Path -LiteralPath (Join-Path $package 'NAMESPACE'))))) { throw "CRAN metadata is missing: $family" }
    $runtimeFiles[$family] = $sourceFiles.Count
}

$archive = Join-Path $target ("acyclic-http-" + $TargetId + "-0.1.0.zip")
& pwsh -NoProfile -File $zipWriter -Root $packages -Archive $archive
if ($LASTEXITCODE -ne 0) { throw "Deterministic $TargetId archive validation failed" }
$copy = Join-Path $target 'repro-copy'
if (Test-Path -LiteralPath $copy) { Remove-Item -LiteralPath $copy -Recurse -Force }
Copy-Item -LiteralPath $packages -Destination $copy -Recurse
$archive2 = Join-Path $target ("acyclic-http-" + $TargetId + "-repro.zip")
& pwsh -NoProfile -File $zipWriter -Root $copy -Archive $archive2
if ($LASTEXITCODE -ne 0) { throw "Reproduction archive validation failed: $TargetId" }
$hash1 = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash
$hash2 = (Get-FileHash -LiteralPath $archive2 -Algorithm SHA256).Hash
if ($hash1 -ne $hash2) { throw "Independent $TargetId archives differ: $hash1 vs $hash2" }

$runtimeCommand = if ($TargetId -eq 'nim') { 'nim' } else { 'Rscript' }
$runtime = Get-Command $runtimeCommand -ErrorAction SilentlyContinue
$runtimeStatus = if ($null -ne $runtime) { 'available-unverified' } else { 'pending-runtime' }
$runtimeReason = if ($null -ne $runtime) { 'Runtime is present; compile/install smoke is the next bounded check.' } else { "$runtimeCommand is not installed on this worker; package execution remains queued." }
$receipt = [ordered]@{
    schema = 'acyclic.sdk.openapi.target-qualification.v1'
    target = $TargetId
    source = [ordered]@{ authority = 'Rust OpenAPI projections'; projections = $families }
    generator = [ordered]@{ name = "OpenAPI Generator $TargetId"; version = '7.25.0'; jar_sha256 = $jarSha256 }
    static = [ordered]@{ package_layout = 'pass'; json_serialization = 'pass'; stream_operation_inventory = 'pass'; operation_ids = $operationChecks; source_files = $runtimeFiles }
    reproducibility = [ordered]@{ archive_sha256 = $hash1; reproduction_sha256 = $hash2; byte_identical = ($hash1 -eq $hash2) }
    runtime = [ordered]@{ command = $runtimeCommand; status = $runtimeStatus; reason = $runtimeReason }
    remote = [ordered]@{ transport = 'json-http'; native_grpc = 'unqualified'; embedded = 'none' }
}
$receipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $target 'qualification-receipt.json') -Encoding utf8NoBOM
Write-Output "$TargetId static qualification passed; archive=$hash1; runtime=$runtimeStatus"

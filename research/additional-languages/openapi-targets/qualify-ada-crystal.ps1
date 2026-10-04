[CmdletBinding()]
param(
    [string] $Root,
    [Parameter(Mandatory = $true)] [ValidateSet('ada', 'crystal')] [string] $TargetId
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') '..')).Path }
$jarSha256 = '41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE'
$families = @('actors', 'workers', 'stream', 'objects', 'inference')
$specRoot = Join-Path $Root 'research/additional-languages/target/bash'
$target = Join-Path $Root ("research/additional-languages/target/$TargetId")
$packages = Join-Path $target 'packages'
$jar = Join-Path $specRoot 'openapi-generator-cli-7.25.0.jar'
$zipWriter = Join-Path $scriptDir 'write-deterministic-zip.ps1'
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw "Missing pinned OpenAPI Generator jar: $jar" }
if ((Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash -ne $jarSha256) { throw 'OpenAPI Generator checksum mismatch' }
if ($TargetId -eq 'ada') {
    $provisioner = Join-Path $scriptDir 'provision-ada-deps.ps1'
    if (-not (Test-Path -LiteralPath $provisioner -PathType Leaf)) { throw "Missing pinned Ada dependency provisioner: $provisioner" }
    & pwsh -NoProfile -File $provisioner -Root $Root
    if ($LASTEXITCODE -ne 0) { throw 'Pinned Ada dependency provisioning failed' }
}

function ConvertTo-Pascal([string] $Value) { return $Value.Substring(0, 1).ToUpperInvariant() + $Value.Substring(1) }
function Normalize-Identifier([string] $Value) { return (($Value.ToLowerInvariant() -replace '[^a-z0-9]', '')) }
function Test-OperationPresence([string] $Operation, [string] $NormalizedSource) {
    $normalized = Normalize-Identifier $Operation
    if ($NormalizedSource.IndexOf($normalized, [StringComparison]::Ordinal) -ge 0) { return $true }
    $tokens = [regex]::Matches($Operation, '[A-Z]?[a-z]+|[0-9]+') | ForEach-Object { $_.Value.ToLowerInvariant() }
    return (@($tokens | Where-Object { $NormalizedSource.IndexOf($_, [StringComparison]::Ordinal) -lt 0 }).Count -eq 0)
}
New-Item -ItemType Directory -Force -Path $target, $packages | Out-Null
foreach ($family in $families) {
    $spec = Join-Path $specRoot "$family.json"
    $package = Join-Path $packages $family
    if (-not (Test-Path -LiteralPath $spec -PathType Leaf)) { throw "Missing Rust OpenAPI projection: $spec" }
    if (Test-Path -LiteralPath $package) { Remove-Item -LiteralPath $package -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $package | Out-Null
    $name = "acyclic_${family}_${TargetId}"
    $namespace = 'Acyclic' + (ConvertTo-Pascal $family)
    $properties = if ($TargetId -eq 'ada') {
        "projectName=$name,openApiName=$namespace,modelPackage=$namespace,apiPackage=$namespace"
    } else {
        "moduleName=${namespace}Http,shardName=$name,shardVersion=0.1.0,shardLicense=Apache-2.0,shardDescription=Rust-derived Acyclic HTTP client"
    }
    & java '-Xmx768m' '-jar' $jar generate -i $spec -g $TargetId -o $package "--additional-properties=$properties" | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "$TargetId package generation failed: $family" }
    $specDocument = Get-Content -LiteralPath $spec -Raw | ConvertFrom-Json
    $operations = @(
        foreach ($path in $specDocument.paths.PSObject.Properties) {
            foreach ($method in $path.Value.PSObject.Properties) {
                if ($method.Name -in @('parameters', 'summary', 'description')) { continue }
                if ($null -ne $method.Value.operationId) { [string]$method.Value.operationId }
            }
        }
    )
    $sourceFiles = @(Get-ChildItem -LiteralPath $package -Recurse -File | Where-Object { $_.Extension -in @('.adb', '.ads', '.cr') })
    if ($sourceFiles.Count -eq 0) { throw "Generated source is missing: $family" }
    $source = (($sourceFiles | ForEach-Object { Get-Content -LiteralPath $_.FullName -Raw }) -join [Environment]::NewLine)
    $normalizedSource = Normalize-Identifier $source
    foreach ($operation in $operations) {
        if (-not (Test-OperationPresence $operation $normalizedSource)) { throw "Generated $TargetId source does not contain Rust operationId '$operation': $family" }
    }
    if ($source -notmatch '(?i)(json|to_json|from_json|application/json)') { throw "JSON serialization evidence is missing: $family" }
    if ($family -eq 'stream' -and $source -notmatch '(?i)(stream|tail|follow|read|children)') { throw "Stream operation evidence is missing: $family" }
}
if ($TargetId -eq 'ada') {
    $adapter = Join-Path $scriptDir 'apply-ada-compatibility.ps1'
    if (-not (Test-Path -LiteralPath $adapter -PathType Leaf)) { throw "Missing Rust-owned Ada compatibility adapter: $adapter" }
    & pwsh -NoProfile -File $adapter -PackagesRoot $packages
    if ($LASTEXITCODE -ne 0) { throw 'Ada compatibility adaptation failed' }
}

$archive = Join-Path $target ("acyclic-http-$TargetId-0.1.0.zip")
& pwsh -NoProfile -File $zipWriter -Root $packages -Archive $archive
if ($LASTEXITCODE -ne 0) { throw "Deterministic $TargetId archive validation failed" }
$copy = Join-Path $target 'repro-copy'
if (Test-Path -LiteralPath $copy) { Remove-Item -LiteralPath $copy -Recurse -Force }
Copy-Item -LiteralPath $packages -Destination $copy -Recurse
$archive2 = Join-Path $target ("acyclic-http-$TargetId-repro.zip")
& pwsh -NoProfile -File $zipWriter -Root $copy -Archive $archive2
if ($LASTEXITCODE -ne 0) { throw "Reproduction archive validation failed: $TargetId" }
$hash1 = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash
$hash2 = (Get-FileHash -LiteralPath $archive2 -Algorithm SHA256).Hash
if ($hash1 -ne $hash2) { throw "Independent $TargetId archives differ: $hash1 vs $hash2" }
$runtimeCommand = if ($TargetId -eq 'ada') { 'gnat' } else { 'crystal' }
$runtime = Get-Command $runtimeCommand -ErrorAction SilentlyContinue
$runtimeStatus = if ($null -ne $runtime) { 'available-unverified' } else { 'pending-runtime' }
$projectionHashes = [ordered]@{}
foreach ($family in $families) { $projectionHashes[$family] = (Get-FileHash -LiteralPath (Join-Path $specRoot "$family.json") -Algorithm SHA256).Hash }
$receipt = [ordered]@{
    schema = 'acyclic.sdk.openapi.target-qualification.v1'
    target = $TargetId
    source = [ordered]@{ authority = 'Rust OpenAPI projections'; projections = $families; projection_sha256 = $projectionHashes }
    generator = [ordered]@{ name = "OpenAPI Generator $TargetId"; version = '7.25.0'; jar_sha256 = $jarSha256 }
    static = [ordered]@{ package_layout = 'pass'; json_serialization = 'pass'; operation_ids = 'pass'; stream_operation_inventory = 'pass' }
    reproducibility = [ordered]@{ archive_sha256 = $hash1; reproduction_sha256 = $hash2; byte_identical = ($hash1 -eq $hash2) }
    runtime = [ordered]@{ command = $runtimeCommand; status = $runtimeStatus; reason = if ($null -ne $runtime) { 'Runtime is present; compile/install smoke remains to be run.' } else { "$runtimeCommand is not installed on this worker; package execution remains queued." } }
    remote = [ordered]@{ transport = 'json-http'; native_grpc = 'unqualified'; embedded = 'none' }
}
$receipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $target 'qualification-receipt.json') -Encoding utf8NoBOM
Write-Output "$TargetId static qualification passed; archive=$hash1; runtime=$runtimeStatus"

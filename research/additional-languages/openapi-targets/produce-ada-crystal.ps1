[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [ValidateSet('ada', 'crystal')] [string] $TargetId,
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [Parameter(Mandatory = $true)] [string] $AuthorityManifest,
    [Parameter(Mandatory = $true)] [string] $OutputRoot,
    [Parameter(Mandatory = $true)] [string] $TargetOutput,
    [Parameter(Mandatory = $true)] [string] $Request
)

$ErrorActionPreference = 'Stop'
$families = @('actors', 'workers', 'stream', 'objects', 'inference')

foreach ($required in @($AuthorityManifest, $Request)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Required source-bound input is missing: $required" }
}
$authority = Get-Content -LiteralPath $AuthorityManifest -Raw | ConvertFrom-Json
if ($authority.schema -ne 'acyclic.sdk.examples.source-authority.v1' -or
    [string]::IsNullOrWhiteSpace([string]$authority.source_revision) -or
    [string]::IsNullOrWhiteSpace([string]$authority.source_sha256) -or
    $authority.source_files.Count -eq 0) {
    throw 'Rust source authority manifest is incomplete or has an unexpected schema'
}
$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
$requestedRevision = [string]$requestDocument.source.revision
if (-not [string]::IsNullOrWhiteSpace($requestedRevision) -and $requestedRevision -ne [string]$authority.source_revision) {
    throw "Rust source authority revision does not match producer request: expected $requestedRevision, got $($authority.source_revision)"
}

$stage = Join-Path $OutputRoot 'openapi'
$stageReceipt = Join-Path $stage 'stage-receipt.json'
if (-not (Test-Path -LiteralPath $stageReceipt -PathType Leaf)) { throw "Rust OpenAPI stage receipt is missing: $stageReceipt" }
$receipt = Get-Content -LiteralPath $stageReceipt -Raw | ConvertFrom-Json
if ($receipt.schema -ne 'acyclic.sdk.openapi.stage-receipt.v1' -or $receipt.projections.Count -ne 5) {
    throw 'Rust OpenAPI stage receipt is not the expected five-family projection'
}
$jar = [string](& pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/ensure-openapi-generator.ps1') '-SourceRoot' $SourceRoot)
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw 'Pinned OpenAPI Generator bootstrap failed' }
$jarSha256 = (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant()

function ConvertTo-Pascal([string] $Value) {
    return $Value.Substring(0, 1).ToUpperInvariant() + $Value.Substring(1)
}
$packageRoot = Join-Path $TargetOutput 'generated'
New-Item -ItemType Directory -Force -Path $packageRoot | Out-Null
foreach ($family in $families) {
    $spec = Join-Path $stage "$family.json"
    if (-not (Test-Path -LiteralPath $spec -PathType Leaf)) { throw "Rust OpenAPI projection is missing: $family" }
    $destination = Join-Path $packageRoot $family
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    $name = "acyclic_${family}_${TargetId}"
    $namespace = 'Acyclic' + (ConvertTo-Pascal $family)
    $properties = if ($TargetId -eq 'ada') {
        "projectName=$name,openApiName=$namespace,modelPackage=$namespace,apiPackage=$namespace"
    } else {
        "moduleName=${namespace}Http,shardName=$name,shardVersion=0.1.0,shardLicense=Apache-2.0,shardDescription=Rust-derived Acyclic HTTP client"
    }
    & java '-Xmx768m' '-jar' $jar 'generate' '-i' $spec '-g' $TargetId '-o' $destination "--additional-properties=$properties"
    if ($LASTEXITCODE -ne 0) { throw "OpenAPI Generator failed for $TargetId/$family" }
}

if ($TargetId -eq 'ada') {
    $adapter = Join-Path $SourceRoot 'research/additional-languages/openapi-targets/apply-ada-compatibility.ps1'
    & pwsh '-NoProfile' '-File' $adapter '-PackagesRoot' $packageRoot
    if ($LASTEXITCODE -ne 0) { throw 'Rust-owned Ada AWS compatibility adapter failed' }
}

$zipWriter = Join-Path $SourceRoot 'research/additional-languages/openapi-targets/write-deterministic-zip.ps1'
$archive = Join-Path $TargetOutput ("acyclic-http-$TargetId-0.1.0.zip")
& pwsh '-NoProfile' '-File' $zipWriter '-Root' $packageRoot '-Archive' $archive
if ($LASTEXITCODE -ne 0) { throw "Deterministic archive validation failed for $TargetId" }
$hash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash
$report = [ordered]@{
    schema = 'acyclic.sdk.openapi.http-producer-receipt.v1'
    target = $TargetId
    source_revision = [string]$authority.source_revision
    source_sha256 = [string]$authority.source_sha256
    generator = [ordered]@{ name = "OpenAPI Generator $TargetId"; version = '7.25.0'; jar_sha256 = $jarSha256 }
    families = $families
    archive = [ordered]@{ path = [IO.Path]::GetFileName($archive); sha256 = $hash; bytes = (Get-Item -LiteralPath $archive).Length }
    runtime = [ordered]@{ status = 'pending'; reason = if ($TargetId -eq 'ada') { 'GNAT/Alire is required for compile/install smoke' } else { 'Crystal and shards are required for compile/install smoke' } }
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $TargetOutput 'producer-receipt.json') -Encoding utf8NoBOM
Write-Output "$TargetId Rust-derived five-family HTTP package staged at $TargetOutput ($hash)"

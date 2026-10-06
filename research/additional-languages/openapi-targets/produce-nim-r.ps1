[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [ValidateSet('nim', 'r')] [string] $TargetId,
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [Parameter(Mandatory = $true)] [string] $AuthorityManifest,
    [Parameter(Mandatory = $true)] [string] $OutputRoot,
    [Parameter(Mandatory = $true)] [string] $TargetOutput,
    [Parameter(Mandatory = $true)] [string] $Request
)

$ErrorActionPreference = 'Stop'
$families = @('actors', 'workers', 'stream', 'objects', 'inference')

function Resolve-RepoPath([string] $Path) {
    if ([IO.Path]::IsPathRooted($Path)) { return [IO.Path]::GetFullPath($Path) }
    return [IO.Path]::GetFullPath((Join-Path $SourceRoot $Path))
}

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
foreach ($family in $families) {
    if (-not (Test-Path -LiteralPath (Join-Path $stage "$family.json") -PathType Leaf)) {
        throw "Rust OpenAPI projection is missing: $family"
    }
}

$jar = [string](& pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/ensure-openapi-generator.ps1') '-SourceRoot' $SourceRoot)
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw 'Pinned OpenAPI Generator bootstrap failed' }
$jarSha256 = (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant()
if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw 'java is required for the pinned OpenAPI Generator jar' }

$packageRoot = Join-Path $TargetOutput 'generated'
New-Item -ItemType Directory -Force -Path $packageRoot | Out-Null
foreach ($family in $families) {
    $spec = Join-Path $stage "$family.json"
    $destination = Join-Path $packageRoot $family
    $name = "acyclic_" + $family + "_" + $TargetId
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    # OpenAPI Generator's Nim template writes model files without creating the
    # model directory. Pre-create it so a clean checkout is reproducible.
    if ($TargetId -eq 'nim') {
        New-Item -ItemType Directory -Force -Path (Join-Path $destination 'openapiclient/models') | Out-Null
    }
    $properties = if ($TargetId -eq 'nim') {
        "packageVersion=0.1.0,packageName=$name"
    } else {
        # R package names cannot contain underscores; use an installable dotted name.
        "packageVersion=0.1.0,packageName=acyclic.$family.r"
    }
    $packageName = if ($TargetId -eq 'nim') { $name } else { "acyclic.$family.r" }
    & java '-Xmx768m' '-jar' $jar 'generate' '-i' $spec '-g' $TargetId '-o' $destination '--package-name' $packageName "--additional-properties=$properties"
    if ($LASTEXITCODE -ne 0) { throw "OpenAPI Generator failed for $TargetId/$family" }
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
        Set-Content -LiteralPath (Join-Path $destination "$name.nimble") -Value ($nimble + [Environment]::NewLine) -Encoding utf8NoBOM
    }
}

$zipWriter = Join-Path $SourceRoot 'research/additional-languages/openapi-targets/write-deterministic-zip.ps1'
$archive = Join-Path $TargetOutput ("acyclic-http-" + $TargetId + "-0.1.0.zip")
$rustGenerationEntrypoint = $env:ACYCLIC_RUST_GENERATION_ENTRYPOINT -eq '1'
$hash = $null
$bytes = $null
if (-not $rustGenerationEntrypoint) {
    & pwsh '-NoProfile' '-File' $zipWriter '-Root' $packageRoot '-Archive' $archive
    if ($LASTEXITCODE -ne 0) { throw "Deterministic archive validation failed for $TargetId" }
    $hash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash
    $bytes = (Get-Item -LiteralPath $archive).Length
}
$report = [ordered]@{
    schema = 'acyclic.sdk.openapi.http-producer-receipt.v1'
    target = $TargetId
    source_revision = [string]$authority.source_revision
    source_sha256 = [string]$authority.source_sha256
    generator = [ordered]@{ name = "OpenAPI Generator $TargetId"; version = '7.25.0'; jar_sha256 = $jarSha256 }
    families = $families
    archive = if ($rustGenerationEntrypoint) { $null } else { [ordered]@{ path = [IO.Path]::GetFileName($archive); sha256 = $hash; bytes = $bytes } }
    package_layout = if ($TargetId -eq 'nim') { 'Nimble metadata plus generated OpenAPI client tree' } else { 'CRAN DESCRIPTION/NAMESPACE plus generated OpenAPI client tree' }
    runtime = [ordered]@{ status = 'pending'; reason = if ($TargetId -eq 'nim') { 'nim and nimble are required for compile/install smoke' } else { 'Rscript and R CMD are required for package/check smoke' } }
}
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $TargetOutput 'producer-receipt.json') -Encoding utf8NoBOM
Write-Output "$TargetId Rust-derived five-family HTTP package staged at $TargetOutput ($hash)"

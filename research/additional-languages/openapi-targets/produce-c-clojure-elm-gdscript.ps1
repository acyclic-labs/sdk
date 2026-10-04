[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [ValidateSet('c', 'clojure', 'elm', 'gdscript')] [string] $TargetId,
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
if ($authority.schema -ne 'acyclic.sdk.examples.source-authority.v1' -or [string]::IsNullOrWhiteSpace([string]$authority.source_revision) -or [string]::IsNullOrWhiteSpace([string]$authority.source_sha256)) { throw 'Rust source authority manifest is incomplete' }
$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
if (-not [string]::IsNullOrWhiteSpace([string]$requestDocument.source.revision) -and [string]$requestDocument.source.revision -ne [string]$authority.source_revision) { throw 'Rust source authority revision does not match producer request' }
$stage = Join-Path $OutputRoot 'openapi'
foreach ($family in $families) { if (-not (Test-Path -LiteralPath (Join-Path $stage "$family.json") -PathType Leaf)) { throw "Rust OpenAPI projection is missing: $family" } }
$jar = [string](& pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/ensure-openapi-generator.ps1') '-SourceRoot' $SourceRoot)
if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw 'Pinned OpenAPI Generator bootstrap failed' }
$jarSha256 = (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant()
$packageRoot = Join-Path $TargetOutput 'generated'
New-Item -ItemType Directory -Force -Path $packageRoot | Out-Null
foreach ($family in $families) {
    $destination = Join-Path $packageRoot $family
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    $name = "acyclic_${family}_${TargetId}"
    & java '-Xmx768m' '-jar' $jar generate '-i' (Join-Path $stage "$family.json") '-g' $TargetId '-o' $destination '--package-name' $name '--additional-properties=packageVersion=0.1.0'
    if ($LASTEXITCODE -ne 0) { throw "OpenAPI Generator failed for $TargetId/$family" }
}
$zip = Join-Path $TargetOutput "acyclic-http-$TargetId-0.1.0.zip"
& pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/write-deterministic-zip.ps1') '-Root' $packageRoot '-Archive' $zip
if ($LASTEXITCODE -ne 0) { throw "Deterministic archive validation failed for $TargetId" }
$hash = (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash.ToLowerInvariant()
[ordered]@{
    schema = 'acyclic.sdk.openapi.http-producer-receipt.v1'
    target = $TargetId
    source_revision = [string]$authority.source_revision
    source_sha256 = [string]$authority.source_sha256
    generator = [ordered]@{ name = "OpenAPI Generator $TargetId"; version = '7.25.0'; jar_sha256 = $jarSha256 }
    families = $families
    archive = [ordered]@{ path = [IO.Path]::GetFileName($zip); sha256 = $hash; bytes = (Get-Item -LiteralPath $zip).Length }
    runtime = [ordered]@{ status = 'pending'; reason = 'Target runtime installation and HTTP/stream/cancellation recovery consumer remain to be qualified.' }
} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $TargetOutput 'producer-receipt.json') -Encoding utf8NoBOM
Write-Output "$TargetId Rust-derived five-family HTTP package staged at $TargetOutput ($hash)"

param(
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [Parameter(Mandatory = $true)] [string] $AuthorityManifest,
    [Parameter(Mandatory = $true)] [string] $OutputRoot,
    [Parameter(Mandatory = $true)] [string] $TargetOutput,
    [Parameter(Mandatory = $true)] [string] $Request
)

$ErrorActionPreference = "Stop"

function Resolve-RepoPath([string] $Path) {
    if ([IO.Path]::IsPathRooted($Path)) { return [IO.Path]::GetFullPath($Path) }
    return [IO.Path]::GetFullPath((Join-Path $SourceRoot $Path))
}

if (-not (Test-Path -LiteralPath $AuthorityManifest -PathType Leaf)) {
    throw "Rust source-authority manifest is missing: $AuthorityManifest"
}
if (-not (Test-Path -LiteralPath $Request -PathType Leaf)) {
    throw "generation request is missing: $Request"
}
$authority = Get-Content -LiteralPath $AuthorityManifest -Raw | ConvertFrom-Json
if ($authority.schema -ne "acyclic.sdk.examples.source-authority.v1" -or
    [string]::IsNullOrWhiteSpace([string]$authority.source_revision) -or
    [string]::IsNullOrWhiteSpace([string]$authority.source_sha256) -or
    $authority.source_files.Count -eq 0) {
    throw "Rust source authority manifest is incomplete or has an unexpected schema"
}
$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
$requestedRevision = [string]$requestDocument.source.revision
if (-not [string]::IsNullOrWhiteSpace($requestedRevision) -and $requestedRevision -ne [string]$authority.source_revision) {
    throw "Rust source authority revision does not match producer request: expected $requestedRevision, got $($authority.source_revision)"
}

$openapiStage = Join-Path $OutputRoot "openapi"
$stageReceipt = Join-Path $openapiStage "stage-receipt.json"
if (-not (Test-Path -LiteralPath $stageReceipt -PathType Leaf)) {
    throw "Rust OpenAPI stage receipt is missing: $stageReceipt"
}
$receipt = Get-Content -LiteralPath $stageReceipt -Raw | ConvertFrom-Json
if ($receipt.schema -ne "acyclic.sdk.openapi.stage-receipt.v1" -or $receipt.projections.Count -ne 5) {
    throw "Rust OpenAPI stage receipt is not the expected five-family projection"
}
foreach ($family in @("actors", "workers", "stream", "objects", "inference")) {
    $projection = Join-Path $openapiStage "$family.json"
    if (-not (Test-Path -LiteralPath $projection -PathType Leaf)) {
        throw "Rust OpenAPI projection is missing: $projection"
    }
}

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if ($null -eq $cargo) { throw "cargo is required for the Rust-owned Julia producer" }
$prototype = Resolve-RepoPath "rust/crates/sdk-openapi-prototype/Cargo.toml"
New-Item -ItemType Directory -Force -Path (Join-Path $TargetOutput "src") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $TargetOutput "family-projections") | Out-Null

function Invoke-RustJulia([string[]] $Arguments) {
    & $cargo.Source run --quiet --locked --manifest-path $prototype -- @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Rust Julia producer failed with exit code $LASTEXITCODE" }
}

Invoke-RustJulia @("--julia-adaptation", (Join-Path $TargetOutput "src/AcyclicWorkers.jl"))
foreach ($family in @("actors", "workers", "stream", "objects", "inference")) {
    Invoke-RustJulia @(
        "--julia-family-adaptation", $family,
        (Join-Path $TargetOutput "family-projections/$family.jl")
    )
}

$portableCheck = Join-Path $SourceRoot "research/additional-languages/openapi-targets/write-deterministic-zip.ps1"
& pwsh '-NoProfile' '-File' $portableCheck '-Root' $TargetOutput '-ValidateOnly'
if ($LASTEXITCODE -ne 0) { throw "Julia generated output failed portable artifact validation" }
$packageArchive = "$TargetOutput-deterministic.zip"
& pwsh '-NoProfile' '-File' $portableCheck '-Root' $TargetOutput '-Archive' $packageArchive
if ($LASTEXITCODE -ne 0) { throw "Julia generated package archive failed deterministic validation" }

@"
name = "AcyclicWorkers"
uuid = "a2e0de7a-8af3-4b28-90b7-c4b3be8a2b42"
version = "1.0.0"
license = "Apache-2.0"

[deps]
Base64 = "2a0f44e3-6c83-55bd-87e4-b1978d98bd5f"
Downloads = "f43a241f-c20a-4ad4-852c-f6b1247861c6"

[compat]
julia = "1.10"
"@ | Set-Content -LiteralPath (Join-Path $TargetOutput "Project.toml") -NoNewline

@"
Copyright (c) Acyclic SDK contributors

Licensed under the Apache License, Version 2.0 (the ""License"");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an ""AS IS"" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"@ | Set-Content -LiteralPath (Join-Path $TargetOutput "LICENSE") -NoNewline

Write-Output "Rust-owned Julia package staged at $TargetOutput; deterministic archive at $packageArchive"

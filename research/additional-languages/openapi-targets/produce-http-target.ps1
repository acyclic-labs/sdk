[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $TargetId,
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [Parameter(Mandatory = $true)] [string] $AuthorityManifest,
    [Parameter(Mandatory = $true)] [string] $OutputRoot,
    [Parameter(Mandatory = $true)] [string] $TargetOutput,
    [Parameter(Mandatory = $true)] [string] $Request
)

$ErrorActionPreference = 'Stop'
$jarSha256 = '41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE'
$families = @('actors', 'workers', 'stream', 'objects', 'inference')

function Resolve-RepoPath([string] $Path) {
    if ([IO.Path]::IsPathRooted($Path)) { return [IO.Path]::GetFullPath($Path) }
    return [IO.Path]::GetFullPath((Join-Path $SourceRoot $Path))
}

if ($TargetId -notin @('bash', 'perl', 'powershell')) { throw "Unsupported HTTP target: $TargetId" }
foreach ($required in @($AuthorityManifest, $Request)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Required source-bound input is missing: $required" }
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

$jar = Resolve-RepoPath 'research/additional-languages/target/bash/openapi-generator-cli-7.25.0.jar'
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw "Pinned OpenAPI Generator jar is missing: $jar" }
if ((Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash -ne $jarSha256) { throw 'Pinned OpenAPI Generator jar checksum mismatch' }
if (-not (Get-Command java -ErrorAction SilentlyContinue)) { throw 'java is required for the pinned OpenAPI Generator jar' }

$packageRoot = Join-Path $TargetOutput 'generated'
New-Item -ItemType Directory -Force -Path $packageRoot | Out-Null
foreach ($family in $families) {
    $spec = Join-Path $stage "$family.json"
    $destination = Join-Path $packageRoot $family
    $name = switch ($TargetId) {
        'bash' { "acyclic-$family-bash" }
        'perl' { "Acyclic-$family" }
        'powershell' { "Acyclic$((Get-Culture).TextInfo.ToTitleCase($family))Http" }
    }
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    $properties = if ($TargetId -eq 'powershell') {
        "packageName=$name,packageVersion=1.0.0,packageGuid=00000000-0000-0000-0000-000000000001,licenseUri=https://www.apache.org/licenses/LICENSE-2.0"
    } elseif ($TargetId -eq 'perl') {
        'artifactVersion=0.1.0'
    } else {
        'artifactVersion=0.1.0'
    }
    & java '-Xmx768m' '-jar' $jar 'generate' '-i' $spec '-g' $TargetId '-o' $destination '--package-name' $name "--additional-properties=$properties"
    if ($LASTEXITCODE -ne 0) { throw "OpenAPI Generator failed for $TargetId/$family" }
}

if ($TargetId -eq 'perl') {
    & pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/apply-perl-runtime-adaptation.ps1') '-GeneratedRoot' (Join-Path $packageRoot 'workers')
    if ($LASTEXITCODE -ne 0) { throw 'Perl Rust-owned runtime adaptation failed' }
}
if ($TargetId -eq 'powershell') {
    & pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/apply-powershell-byte-adaptation.ps1') '-GeneratedRoot' (Join-Path $packageRoot 'workers')
    if ($LASTEXITCODE -ne 0) { throw 'PowerShell Rust-owned byte adaptation failed' }
}

$archive = switch ($TargetId) {
    'bash' { Join-Path $TargetOutput 'acyclic-http-bash-0.1.0.zip' }
    'perl' { Join-Path $TargetOutput 'acyclic-http-perl-0.1.0.zip' }
    'powershell' { Join-Path $TargetOutput 'acyclic-http-powershell-1.0.0.zip' }
}
$zipWriter = Join-Path $SourceRoot 'research/additional-languages/openapi-targets/write-deterministic-zip.ps1'
& pwsh '-NoProfile' '-File' $zipWriter '-Root' $packageRoot '-Archive' $archive
if ($LASTEXITCODE -ne 0) { throw "Deterministic archive validation failed for $TargetId" }
Write-Output "$TargetId Rust-derived five-family HTTP package staged at $TargetOutput"

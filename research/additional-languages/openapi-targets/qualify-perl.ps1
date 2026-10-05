[CmdletBinding()]
param(
    [string]$Root,
    [switch]$SkipRustGeneration,
    [switch]$SkipDependencyInstall
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') '..')).Path
}
$version = '7.25.0'
$jarSha256 = '41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE'
$work = Join-Path $Root 'research/additional-languages/target/perl'
$jar = Join-Path $Root "research/additional-languages/target/bash/openapi-generator-cli-$version.jar"
$families = @('actors', 'workers', 'stream', 'objects', 'inference')
$specRoot = Join-Path $Root 'research/additional-languages/target/bash'
$packages = Join-Path $work 'packages'
$localLib = Join-Path $work 'local-lib'
$zip = Join-Path $work 'acyclic-http-perl-0.1.0.zip'
$fixture = Join-Path $scriptDir 'perl-fixture.py'
$consumer = Join-Path $scriptDir 'perl-consumer.pl'
$adapt = Join-Path $scriptDir 'apply-perl-runtime-adaptation.ps1'
$port = 18766

New-Item -ItemType Directory -Force -Path $work | Out-Null
if (-not (Test-Path -LiteralPath $jar)) { throw "Missing pinned OpenAPI Generator jar: $jar" }
if ((Get-FileHash $jar -Algorithm SHA256).Hash -ne $jarSha256) { throw 'OpenAPI Generator checksum mismatch' }
if (-not $SkipRustGeneration) {
    Push-Location $Root
    try {
        foreach ($family in $families) {
            $spec = Join-Path $specRoot "$family.json"
            if ($family -eq 'actors') { cargo run --quiet --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- $spec | Out-Host }
            else { cargo run --quiet --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- --contract $family $spec | Out-Host }
            if ($LASTEXITCODE -ne 0) { throw "Rust OpenAPI projection failed: $family" }
        }
    } finally { Pop-Location }
}
New-Item -ItemType Directory -Force -Path $packages | Out-Null
foreach ($family in $families) {
    $spec = Join-Path $specRoot "$family.json"
    if (-not (Test-Path -LiteralPath $spec)) { throw "Missing Rust OpenAPI projection: $spec" }
    $package = Join-Path $packages $family
    if (Test-Path -LiteralPath $package) { Remove-Item -LiteralPath $package -Recurse -Force }
    $heap = if ($family -in @('workers', 'inference')) { '-Xmx768m' } else { '-Xmx512m' }
    java $heap -jar $jar generate -i $spec -g perl -o $package --package-name "Acyclic-$family" --additional-properties=artifactVersion=0.1.0 | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Perl package generation failed: $family" }
    & pwsh -NoProfile -File $adapt -GeneratedRoot $package
}

if (-not $SkipDependencyInstall) {
    New-Item -ItemType Directory -Force -Path $localLib | Out-Null
    $dependencyPackage = Join-Path $packages 'actors'
    Push-Location $dependencyPackage
    try {
        & cpanm '--local-lib-contained' $localLib '--notest' '--skip-satisfied' '--installdeps' '.'
        if ($LASTEXITCODE -ne 0) { throw 'Perl dependency installation failed' }
    } finally { Pop-Location }
}

$server = Start-Process -FilePath 'python' -ArgumentList @('-u', $fixture, $port) -PassThru -RedirectStandardOutput (Join-Path $work 'fixture.out') -RedirectStandardError (Join-Path $work 'fixture.err')
try {
    Start-Sleep -Milliseconds 800
    $oldPerl5Lib = $env:PERL5LIB
    try {
        foreach ($family in $families) {
            $env:PERL5LIB = "$(Join-Path $localLib 'lib/perl5');$(Join-Path $packages $family)\lib"
            & perl $consumer $family
            if ($LASTEXITCODE -ne 0) { throw "Perl generated consumer failed: $family" }
        }
    } finally { $env:PERL5LIB = $oldPerl5Lib }
} finally {
    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
}

$zipWriter = Join-Path $scriptDir 'write-deterministic-zip.ps1'
& pwsh -NoProfile -File $zipWriter -Root $packages -Archive $zip
if ($LASTEXITCODE -ne 0) { throw 'Deterministic Perl archive validation failed' }
Write-Output "Perl five-family HTTP qualification passed; artifact SHA256 $((Get-FileHash $zip -Algorithm SHA256).Hash)"

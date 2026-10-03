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
$spec = Join-Path $Root 'research/additional-languages/target/bash/actors.json'
$package = Join-Path $work 'package'
$localLib = Join-Path $work 'local-lib'
$zip = Join-Path $work 'acyclic-actors-perl-0.1.0.zip'
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
        cargo run --quiet --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- $spec | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Rust Actors OpenAPI projection failed' }
    } finally { Pop-Location }
}
if (-not (Test-Path -LiteralPath $spec)) { throw "Missing Rust OpenAPI projection: $spec" }
if (Test-Path -LiteralPath $package) { Remove-Item -LiteralPath $package -Recurse -Force }
java '-Xmx768m' -jar $jar generate -i $spec -g perl -o $package --package-name AcyclicSDK --additional-properties=artifactVersion=0.1.0 | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Perl package generation failed' }
& pwsh -NoProfile -File $adapt -GeneratedRoot $package

if (-not $SkipDependencyInstall) {
    New-Item -ItemType Directory -Force -Path $localLib | Out-Null
    Push-Location $package
    try {
        & cpanm '--local-lib-contained' $localLib '--notest' '--skip-satisfied' '--installdeps' '.'
        if ($LASTEXITCODE -ne 0) { throw 'Perl dependency installation failed' }
    } finally { Pop-Location }
}

$server = Start-Process -FilePath 'python' -ArgumentList @('-u', $fixture, $port) -PassThru -RedirectStandardOutput (Join-Path $work 'fixture.out') -RedirectStandardError (Join-Path $work 'fixture.err')
try {
    Start-Sleep -Milliseconds 800
    $oldPerl5Lib = $env:PERL5LIB
    $env:PERL5LIB = "$(Join-Path $localLib 'lib/perl5');$(Join-Path $package 'lib')"
    try {
        & perl $consumer
        if ($LASTEXITCODE -ne 0) { throw 'Perl generated consumer failed' }
    } finally { $env:PERL5LIB = $oldPerl5Lib }
} finally {
    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
}

Remove-Item -LiteralPath $zip -Force -ErrorAction SilentlyContinue
Compress-Archive -Path (Join-Path $package '*') -DestinationPath $zip -CompressionLevel Optimal
Write-Output "Perl Actors HTTP qualification passed; artifact SHA256 $((Get-FileHash $zip -Algorithm SHA256).Hash)"

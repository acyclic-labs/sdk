[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Manifest,
    [Parameter(Mandatory)] [string] $OutputRoot,
    [string] $Endpoint = '127.0.0.1:50051',
    [string] $RubyPackageRoot,
    [string] $PhpPackageRoot,
    [string] $DartPackageRoot,
    [int] $TimeoutMs = 5000,
    [switch] $GeneratePackages,
    [string] $SourceRoot,
    [string] $Authority,
    [string] $Request
)

$ErrorActionPreference = 'Stop'
$manifestPath = (Resolve-Path -LiteralPath $Manifest).Path
$manifestDocument = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
$started = [ordered]@{
    schema = 'acyclic.sdk.rpd.live-run.v1'
    authority = $manifestDocument.authority
    endpoint = $Endpoint
    outputs = [ordered]@{}
    compiler_checks = [ordered]@{}
    transport_surface = [ordered]@{}
}

if ($GeneratePackages) {
    if (-not $SourceRoot -or -not $Authority -or -not $Request) {
        throw '-GeneratePackages requires -SourceRoot, -Authority, and -Request'
    }
    $authorityDocument = Get-Content -LiteralPath $Authority -Raw | ConvertFrom-Json
    if ([string]$authorityDocument.source_git_sha -ne [string]$manifestDocument.authority.source_git_sha -or
        [string]$authorityDocument.source_revision -ne [string]$manifestDocument.authority.model_digest) {
        throw 'Generation authority does not match the typed request manifest'
    }
    $adapter = Join-Path $PSScriptRoot '..\..\acceptance\ruby-php-dart\package-adapter.ps1'
    $packageOutput = Join-Path $OutputRoot 'packages'
    New-Item -ItemType Directory -Force -Path $packageOutput | Out-Null
    & $adapter -SourceRoot $SourceRoot -Authority $Authority -Request $Request -Output $packageOutput
    if ($LASTEXITCODE -ne 0) { throw "package adapter exited with $LASTEXITCODE" }
    $RubyPackageRoot = Join-Path $packageOutput 'ruby'
    $PhpPackageRoot = Join-Path $packageOutput 'php'
    $DartPackageRoot = Join-Path $packageOutput 'dart'
}

function Test-GeneratedPackage([string] $Language, [string] $PackageRoot) {
    $receipt = switch ($Language) {
        'ruby' { Join-Path $PackageRoot 'generated/provenance.json' }
        'php' { Join-Path $PackageRoot 'src/provenance.json' }
        'dart' { Join-Path $PackageRoot 'lib/src/generated/provenance.json' }
    }
    if (-not (Test-Path -LiteralPath $receipt)) { throw "$Language package is missing generation provenance: $receipt" }
    $provenance = Get-Content -LiteralPath $receipt -Raw | ConvertFrom-Json
    if ([string]$provenance.source_git_sha -ne [string]$manifestDocument.authority.source_git_sha -or
        [string]$provenance.rust_model_digest -ne [string]$manifestDocument.authority.model_digest) {
        throw "$Language package provenance does not match the Rust typed request manifest"
    }
    $files = switch ($Language) {
        'ruby' { @(Get-ChildItem (Join-Path $PackageRoot 'generated') -Recurse -Filter '*_pb.rb' -File) }
        'php' { @(Get-ChildItem (Join-Path $PackageRoot 'src') -Recurse -Filter '*.php' -File) }
        'dart' { @(Get-ChildItem (Join-Path $PackageRoot 'lib/src/generated') -Recurse -Filter '*.pb.dart' -File) }
    }
    if ($files.Count -eq 0) { throw "$Language package contains no generated sources" }
    $surfaceFiles = @($files | Where-Object { $_.Extension -in @('.rb', '.php', '.dart') })
    $surfaceText = (($surfaceFiles | ForEach-Object { Get-Content -LiteralPath $_.FullName -Raw }) -join "`n")
    $surface = [ordered]@{
        server_streaming = $false
        cancellation = $false
        transport = 'grpc'
    }
    if ($Language -eq 'ruby') {
        # The Ruby generator spells RPC methods in snake_case. Keep this
        # declaration tied to generated service sources, rather than a
        # handwritten capability list.
        $surface.server_streaming = $surfaceText -match '(?im)\bdef\s+get_object\b'
        $surface.cancellation = $surfaceText -match '(?im)\bdef\s+cancel\b'
    } elseif ($Language -eq 'php') {
        $surface.server_streaming = $surfaceText -match '(?im)\bfunction\s+GetObject\s*\('
        $surface.cancellation = $surfaceText -match '(?im)\bfunction\s+Cancel\s*\('
    } else {
        $surface.server_streaming = $surfaceText -match '(?im)\b(getObject|GetObject)\s*\('
        $surface.cancellation = $surfaceText -match '(?im)\b(cancel|Cancel)\s*\('
        $pubspec = Join-Path $PackageRoot 'pubspec.yaml'
        if (-not (Test-Path -LiteralPath $pubspec) -or
            -not ((Get-Content -LiteralPath $pubspec -Raw) -match '(?im)^\s*grpc:\s*')) {
            throw 'Dart generated package does not declare its grpc transport dependency'
        }
    }
    if (-not $surface.server_streaming -or -not $surface.cancellation) {
        throw "$Language generated transport surface is missing streaming or cancellation support"
    }
    $started.transport_surface[$Language] = $surface
    $compiler = switch ($Language) { 'ruby' { 'ruby' } 'php' { 'php' } 'dart' { 'dart' } }
    if (-not (Get-Command $compiler -ErrorAction SilentlyContinue)) { throw "$Language compiler/runtime is unavailable: $compiler" }
    if ($Language -eq 'ruby') {
        foreach ($file in $files) { & ruby -c $file.FullName *> $null; if ($LASTEXITCODE -ne 0) { throw "Ruby syntax check failed: $($file.Name)" } }
    } elseif ($Language -eq 'php') {
        foreach ($file in $files) { & php -l $file.FullName *> $null; if ($LASTEXITCODE -ne 0) { throw "PHP syntax check failed: $($file.Name)" } }
    } else {
        Push-Location $PackageRoot
        try { & dart analyze --no-fatal-infos *> $null; if ($LASTEXITCODE -ne 0) { throw 'Dart analyzer failed' } } finally { Pop-Location }
    }
    $started.compiler_checks[$Language] = [ordered]@{ files = $files.Count; compiler = $compiler; status = 'passed' }
}

function Invoke-Required([string] $Command, [string[]] $Arguments, [string] $Language, [string] $PackageRoot, [string] $Script) {
    if (-not $PackageRoot) { throw "-$Language`PackageRoot is required for an installed consumer run" }
    if (-not (Test-Path -LiteralPath $PackageRoot)) { throw "$Language package root does not exist: $PackageRoot" }
    if (-not (Get-Command $Command -ErrorAction SilentlyContinue)) { throw "$Language runtime is unavailable on this host: $Command" }
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Language live consumer exited with $LASTEXITCODE" }
    $started.outputs[$Language] = Join-Path $OutputRoot "$Language-live-receipt.json"
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
if ($RubyPackageRoot) {
    Test-GeneratedPackage 'ruby' $RubyPackageRoot
    Invoke-Required 'ruby' @($root + '\ruby_live.rb', '--package-root', $RubyPackageRoot, '--manifest', $manifestPath, '--endpoint', $Endpoint, '--timeout-ms', $TimeoutMs, '--output', (Join-Path $OutputRoot 'ruby-live-receipt.json')) 'ruby' $RubyPackageRoot $root
}
if ($PhpPackageRoot) {
    Test-GeneratedPackage 'php' $PhpPackageRoot
    Invoke-Required 'php' @($root + '\php_live.php', '--package-root', $PhpPackageRoot, '--manifest', $manifestPath, '--endpoint', $Endpoint, '--timeout-ms', $TimeoutMs, '--output', (Join-Path $OutputRoot 'php-live-receipt.json')) 'php' $PhpPackageRoot $root
}
if ($DartPackageRoot) {
    Test-GeneratedPackage 'dart' $DartPackageRoot
    Invoke-Required 'dart' @('run', (Join-Path $root 'dart_live.dart'), '--manifest', $manifestPath, '--endpoint', $Endpoint, '--timeout-ms', $TimeoutMs, '--output', (Join-Path $OutputRoot 'dart-live-receipt.json')) 'dart' $DartPackageRoot $root
}

$started | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $OutputRoot 'live-run.json')

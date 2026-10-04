[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $SourceRoot,
    [Parameter(Mandatory = $true)]
    [string] $Authority,
    [Parameter(Mandatory = $true)]
    [string] $Request,
    [Parameter(Mandatory = $true)]
    [string] $Output
)

$ErrorActionPreference = 'Stop'

function Resolve-File([string] $Path, [string] $Name) {
    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction Stop
    if (-not (Test-Path -LiteralPath $resolved.Path -PathType Leaf)) {
        throw "$Name must be a file: $Path"
    }
    return $resolved.Path
}

function Resolve-Directory([string] $Path, [string] $Name) {
    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction Stop
    if (-not (Test-Path -LiteralPath $resolved.Path -PathType Container)) {
        throw "$Name must be a directory: $Path"
    }
    return $resolved.Path
}

function Sha256([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Run-Checked([string] $Program, [string[]] $Arguments, [string] $WorkingDirectory) {
    Push-Location -LiteralPath $WorkingDirectory
    try {
        & $Program @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "$Program exited with code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
}

function Copy-Package([string] $Source, [string] $Destination, [bool] $StripTooling = $true) {
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
    if (-not $StripTooling) {
        Copy-Item -Path (Join-Path $Source '*') -Destination $Destination -Recurse -Force
        return
    }

    # Exclude toolchains before copying. Dart's pinned SDK and pub cache are
    # hundreds of megabytes, so copying then deleting them makes a producer
    # appear hung and can leave an oversized package artifact behind.
    $ignored = @('.dart_tool', '.pub-cache', '.toolchain', '.bundle', 'vendor/bundle', 'tmp', 'log')
    $sourceRoot = (Resolve-Path -LiteralPath $Source).Path.TrimEnd('\', '/')
    foreach ($file in Get-ChildItem -LiteralPath $sourceRoot -Recurse -File -Force) {
        $relative = $file.FullName.Substring($sourceRoot.Length + 1).Replace('\', '/')
        $excluded = $ignored | Where-Object {
            $relative -eq $_ -or $relative.StartsWith("$($_)/", [StringComparison]::OrdinalIgnoreCase)
        }
        if ($excluded) {
            continue
        }
        $target = Join-Path $Destination ($relative.Replace('/', [IO.Path]::DirectorySeparatorChar))
        New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null
        Copy-Item -LiteralPath $file.FullName -Destination $target -Force
    }
}

$sourceRoot = Resolve-Directory $SourceRoot 'SourceRoot'
$authority = Resolve-File $Authority 'Authority'
$request = Resolve-File $Request 'Request'
$outputParent = [IO.Path]::GetFullPath($Output)
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..\..')).Path

$authorityDocument = Get-Content -LiteralPath $authority -Raw | ConvertFrom-Json
if ($null -eq $authorityDocument.families -or @($authorityDocument.families).Count -eq 0) {
    throw 'Authority must contain at least one Rust-owned family'
}
foreach ($family in @($authorityDocument.families)) {
    if ([string]::IsNullOrWhiteSpace([string] $family.source) -or
        [string]::IsNullOrWhiteSpace([string] $family.source_sha256)) {
        throw 'Every authority family must bind a source and source_sha256'
    }
    $schema = Join-Path $sourceRoot ([string] $family.source)
    if (-not (Test-Path -LiteralPath $schema -PathType Leaf)) {
        throw "Authority source is missing: $($family.source)"
    }
    if ((Sha256 $schema) -ne ([string] $family.source_sha256).ToLowerInvariant()) {
        throw "Authority source hash mismatch: $($family.source)"
    }
    if (-not [string]::IsNullOrWhiteSpace([string] $family.descriptor)) {
        $descriptor = Join-Path $sourceRoot ([string] $family.descriptor)
        if (-not (Test-Path -LiteralPath $descriptor -PathType Leaf)) {
            throw "Authority descriptor is missing: $($family.descriptor)"
        }
        if (-not [string]::IsNullOrWhiteSpace([string] $family.descriptor_sha256) -and
            (Sha256 $descriptor) -ne ([string] $family.descriptor_sha256).ToLowerInvariant()) {
            throw "Authority descriptor hash mismatch: $($family.descriptor)"
        }
    }
}

$requestDocument = Get-Content -LiteralPath $request -Raw | ConvertFrom-Json
$languages = @($requestDocument.languages | ForEach-Object { ([string] $_).ToLowerInvariant() })
if ($languages.Count -eq 0) {
    $languages = @('ruby', 'php', 'dart')
}
$unsupported = @($languages | Where-Object { $_ -notin @('ruby', 'php', 'dart') })
if ($unsupported.Count -ne 0) {
    throw "Unsupported language(s): $($unsupported -join ', ')"
}
$languages = @($languages | Select-Object -Unique)

if (Test-Path -LiteralPath $outputParent) {
    if ((Get-ChildItem -LiteralPath $outputParent -Force | Measure-Object).Count -ne 0) {
        throw "Output must be empty: $outputParent"
    }
} else {
    New-Item -ItemType Directory -Path $outputParent -Force | Out-Null
}

$stage = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-ruby-php-dart-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $stage -Force | Out-Null
try {
    $commands = @{}
    if ($languages -contains 'ruby') {
        $package = Join-Path $stage 'ruby'
        Copy-Package (Join-Path $repoRoot 'ruby') $package
        $ruby = if ($env:RUBY) { $env:RUBY } else { 'ruby' }
        Run-Checked $ruby @('generate.rb', '--schema-root', $sourceRoot, '--manifest', $authority) $package
        Copy-Package $package (Join-Path $outputParent 'ruby')
        $commands['ruby'] = @{ runtime = [IO.Path]::GetFileName($ruby); generator = 'generate.rb'; lock = (Get-Content (Join-Path $package 'generator.lock.json') -Raw | ConvertFrom-Json).generator_version }
    }
    if ($languages -contains 'php') {
        $package = Join-Path $stage 'php'
        Copy-Package (Join-Path $repoRoot 'php') $package
        $php = if ($env:PHP) { $env:PHP } else { 'php' }
        Run-Checked $php @('tools/generate.php', '--schema-root', $sourceRoot, '--manifest', $authority) $package
        Copy-Package $package (Join-Path $outputParent 'php')
        $commands['php'] = @{ runtime = [IO.Path]::GetFileName($php); generator = 'tools/generate.php'; lock = (Get-Content (Join-Path $package 'generator.lock.json') -Raw | ConvertFrom-Json).generator_version }
    }
    if ($languages -contains 'dart') {
        $package = Join-Path $stage 'dart'
        # Keep SDK and pub caches out of the staged source package. The lock
        # file and portable receipt carry their pinned versions, so consumers
        # receive a normal installable Dart package rather than a toolchain
        # archive.
        Copy-Package (Join-Path $repoRoot 'dart') $package
        $dart = if ($env:DART) { $env:DART } else { 'dart' }
        Run-Checked $dart @('run', 'tool/generate.dart', '--schema-root', $sourceRoot, '--manifest', $authority) $package
        Copy-Package $package (Join-Path $outputParent 'dart')
        $dartLockPath = Join-Path $package 'generator.lock.yaml'
        # Windows PowerShell can preserve adapted file metadata when a raw
        # Get-Content result is assigned directly into a hashtable. Force a
        # scalar string so the portable receipt stays small and deterministic.
        $dartLock = [string](Get-Content -LiteralPath $dartLockPath -Raw)
        $commands['dart'] = @{ runtime = [IO.Path]::GetFileName($dart); generator = 'tool/generate.dart'; lock = $dartLock }
    }
    [ordered]@{
        schema = 'acyclic.ruby-php-dart.package-adapter.v1'
        authority_sha256 = Sha256 $authority
        request_sha256 = Sha256 $request
        authority_source_revision = $authorityDocument.source_revision
        source_root = 'caller-supplied authority input (path omitted for portability)'
        languages = $languages
        commands = $commands
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $outputParent 'generation-receipt.json') -Encoding UTF8
} finally {
    if (Test-Path -LiteralPath $stage) {
        Remove-Item -LiteralPath $stage -Recurse -Force
    }
}

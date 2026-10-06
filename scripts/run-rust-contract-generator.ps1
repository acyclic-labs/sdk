[CmdletBinding()]
param(
    [ValidateSet('family-write', 'family-check', 'all-write', 'all-check')]
    [string]$Mode = 'all-write',
    [string]$SourceRoot = (Join-Path $PSScriptRoot '..'),
    [string]$OutputRoot,
    [string]$Node = 'node'
)

$ErrorActionPreference = 'Stop'

function Resolve-FullPath([string]$Path) {
    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }
    return [IO.Path]::GetFullPath((Join-Path (Get-Location) $Path))
}

$source = Resolve-FullPath $SourceRoot
if (-not (Test-Path -LiteralPath $source -PathType Container)) {
    throw "Rust SDK source root does not exist: $source"
}
$entrypoint = Join-Path $source 'scripts/run-rust-contract-generator.mjs'
if (-not (Test-Path -LiteralPath $entrypoint -PathType Leaf)) {
    throw "Rust contract generator entrypoint is missing: $entrypoint"
}

$ownsOutput = [string]::IsNullOrWhiteSpace($OutputRoot)
if ($ownsOutput) {
    $workspaceRoot = if ($env:ACYCLIC_SDK_WORK_ROOT) {
        Resolve-FullPath $env:ACYCLIC_SDK_WORK_ROOT
    } else {
        [IO.Path]::GetTempPath().TrimEnd('\', '/')
    }
    $OutputRoot = Join-Path $workspaceRoot ("acyclic-sdk-contracts-" + [Guid]::NewGuid().ToString('N'))
} else {
    $OutputRoot = Resolve-FullPath $OutputRoot
}

$sourcePrefix = $source.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
$outputFull = [IO.Path]::GetFullPath($OutputRoot)
if ($outputFull.Equals($source, [StringComparison]::OrdinalIgnoreCase) -or
    $outputFull.StartsWith($sourcePrefix, [StringComparison]::OrdinalIgnoreCase) -or
    $source.StartsWith($outputFull.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Contract output must be outside the Rust source root: $outputFull"
}

New-Item -ItemType Directory -Force -Path $outputFull | Out-Null
try {
    & $Node $entrypoint $Mode $source $outputFull
    if ($LASTEXITCODE -ne 0) {
        throw "Rust contract generation failed with exit code $LASTEXITCODE"
    }
    Write-Output $outputFull
} finally {
    if ($ownsOutput -and (Test-Path -LiteralPath $outputFull)) {
        Remove-Item -LiteralPath $outputFull -Recurse -Force
    }
}

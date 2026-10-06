[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $Archive,
    [Parameter(Mandatory = $true)] [string] $WorkRoot,
    [string] $Package = 'acyclic-actors'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Full-Path([string] $Path) {
    return [System.IO.Path]::GetFullPath($Path)
}

$Archive = Full-Path $Archive
$WorkRoot = Full-Path $WorkRoot
if (-not (Test-Path -LiteralPath $Archive -PathType Leaf)) {
    throw "Rust package archive was not found: $Archive"
}
$tar = Get-Command tar -ErrorAction Stop
$cargo = Get-Command cargo -ErrorAction Stop
if (Test-Path -LiteralPath $WorkRoot) {
    Remove-Item -LiteralPath $WorkRoot -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $WorkRoot | Out-Null
& $tar.Source -xzf $Archive -C $WorkRoot
if ($LASTEXITCODE -ne 0) {
    throw "Rust package extraction failed with exit code $LASTEXITCODE"
}
$bundle = Get-ChildItem -LiteralPath $WorkRoot -Directory | Select-Object -First 1
if ($null -eq $bundle) {
    throw 'Rust package archive did not contain a workspace directory'
}
$consumer = Join-Path $WorkRoot 'consumer'
New-Item -ItemType Directory -Force -Path (Join-Path $consumer 'src') | Out-Null
$relativePackage = "../$($bundle.Name)/rust/crates/$($Package -replace '^acyclic-', '')"
@"
[package]
name = "acyclic-rust-consumer-probe"
version = "0.1.0"
edition = "2024"

[dependencies]
$Package = { path = "$relativePackage" }
"@ | Set-Content -LiteralPath (Join-Path $consumer 'Cargo.toml') -Encoding utf8NoBOM
'fn main() {}' | Set-Content -LiteralPath (Join-Path $consumer 'src/main.rs') -Encoding utf8NoBOM
& $cargo.Source generate-lockfile --manifest-path (Join-Path $consumer 'Cargo.toml') --offline
if ($LASTEXITCODE -ne 0) {
    throw "Rust consumer lockfile generation failed with exit code $LASTEXITCODE"
}
$target = Join-Path $WorkRoot 'consumer-target'
& $cargo.Source check --manifest-path (Join-Path $consumer 'Cargo.toml') --locked --offline --target-dir $target
if ($LASTEXITCODE -ne 0) {
    throw "Rust consumer check failed with exit code $LASTEXITCODE"
}
Write-Host "Rust consumer package check passed for $Package"

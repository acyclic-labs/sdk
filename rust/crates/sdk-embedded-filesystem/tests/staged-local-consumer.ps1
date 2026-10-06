<#
.SYNOPSIS
    Stages and runs an installed-style Filesystem/Harness Rust consumer.

The consumer is created below the caller-selected staging root, while the
product crates are consumed through explicit local paths. This keeps the
proof independent from registry publication and places Cargo's lockfile and
build output on the reusable Q: target volume.
#>

[CmdletBinding()]
param(
    [string]$SourceRoot = "",
    [string]$StageRoot = "Q:\sdk\.tmp-fs-harness-local-consumer",
    [string]$TargetDir = "Q:\sdk\work\rust-source-root-wire-target",
    [string]$EmbeddedBundle = "Q:\sdk\embedded-package-bundle"
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($SourceRoot)) {
    $SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../../..")).Path
}
$sourceRoot = (Resolve-Path $SourceRoot).Path
$stageRoot = [System.IO.Path]::GetFullPath($StageRoot)
$targetDir = [System.IO.Path]::GetFullPath($TargetDir)
$consumerSource = Join-Path $stageRoot "src\lib.rs"
$consumerManifest = Join-Path $stageRoot "Cargo.toml"

if (-not $stageRoot.StartsWith("Q:\sdk\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "StageRoot must remain under Q:\sdk\ to preserve the bounded disk policy"
}
if (-not $targetDir.StartsWith("Q:\sdk\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "TargetDir must remain under Q:\sdk\ to preserve the bounded disk policy"
}

if (Test-Path $stageRoot) {
    Remove-Item -LiteralPath $stageRoot -Recurse -Force
}
New-Item -ItemType Directory -Force -Path (Join-Path $stageRoot "src") | Out-Null
Copy-Item (Join-Path $sourceRoot "rust\crates\sdk-embedded-filesystem\src\lib.rs") $consumerSource

$filesystem = (Resolve-Path (Join-Path $sourceRoot "rust\crates\filesystem")).Path.Replace("\", "/")
$harness = (Resolve-Path (Join-Path $sourceRoot "rust\crates\harness")).Path.Replace("\", "/")
@"
[package]
name = "acyclic-fs-harness-local-consumer"
version = "0.1.0"
edition = "2024"

[workspace]

[features]
default = ["filesystem", "harness"]
filesystem = ["dep:acyclic-fs"]
harness = ["dep:acyclic-harness"]

[dependencies]
acyclic-fs = { version = "=0.2.0", path = "$filesystem", default-features = false, features = ["memory"], optional = true }
acyclic-harness = { version = "=0.2.0", path = "$harness", default-features = false, features = ["filesystem"], optional = true }
bytes = "1.10.1"
tokio = { version = "1.48.0", default-features = false, features = ["macros", "rt", "sync"] }
"@ | Set-Content -LiteralPath $consumerManifest

$env:CARGO_TARGET_DIR = $targetDir
cargo test --manifest-path $consumerManifest --offline
if ($LASTEXITCODE -ne 0) {
    throw "staged local Filesystem/Harness consumer failed with exit code $LASTEXITCODE"
}

$archive = Join-Path $sourceRoot "rust\crates\harness\src\generated\harness-archived-v2.bin"
$archiveHash = (Get-FileHash $archive -Algorithm SHA256).Hash
$harnessOut = Get-ChildItem (Join-Path $targetDir "debug\build") -Directory -ErrorAction SilentlyContinue |
    Where-Object Name -Like "acyclic-harness-*" |
    Where-Object { Test-Path (Join-Path $_.FullName "out\harness_descriptor.bin") } |
    Sort-Object LastWriteTime |
    Select-Object -Last 1
if ($null -eq $harnessOut) {
    throw "staged consumer did not produce the Harness handshake descriptor"
}
$builtHash = (Get-FileHash (Join-Path $harnessOut.FullName "out\harness_descriptor.bin") -Algorithm SHA256).Hash
if ($archiveHash -ne $builtHash) {
    throw "staged consumer handshake changed: expected $archiveHash, got $builtHash"
}

$vendorStatus = "not-requested"
if (Test-Path $EmbeddedBundle) {
    $vendor = Join-Path $EmbeddedBundle "vendor"
    $blake3 = Get-ChildItem $vendor -Directory -Filter "blake3-*" -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $blake3) {
        $vendorStatus = "blocked: embedded bundle is missing vendor/blake3"
    } else {
        $bundleManifest = Join-Path $EmbeddedBundle "source\rust\crates\sdk-embedded-filesystem\Cargo.toml"
        $bundleConfig = Join-Path $EmbeddedBundle ".cargo\config.toml"
        if (-not (Test-Path $bundleManifest) -or -not (Test-Path $bundleConfig)) {
            throw "embedded bundle is missing its staged consumer manifest or Cargo source replacement"
        }
        $previousCargoHome = $env:CARGO_HOME
        $previousCargoTargetDir = $env:CARGO_TARGET_DIR
        try {
            $env:CARGO_HOME = Join-Path $EmbeddedBundle "cargo-home"
            $env:CARGO_TARGET_DIR = $targetDir
            Push-Location $EmbeddedBundle
            cargo test --manifest-path $bundleManifest --offline --locked
            if ($LASTEXITCODE -ne 0) {
                throw "embedded bundle consumer failed with exit code $LASTEXITCODE"
            }
            $vendorStatus = "available-and-passed:$($blake3.Name)"
        }
        finally {
            Pop-Location
            if ($null -eq $previousCargoHome) {
                Remove-Item Env:CARGO_HOME -ErrorAction SilentlyContinue
            } else {
                $env:CARGO_HOME = $previousCargoHome
            }
            if ($null -eq $previousCargoTargetDir) {
                Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
            } else {
                $env:CARGO_TARGET_DIR = $previousCargoTargetDir
            }
        }
    }
}

Write-Output "staged local consumer passed; handshake=$archiveHash; embedded-bundle=$vendorStatus"

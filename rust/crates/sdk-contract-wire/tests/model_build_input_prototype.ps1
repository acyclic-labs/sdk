<#!
.SYNOPSIS
    Runs the Rust descriptor authority proof for Filesystem/Harness.

The normal package builds remain the default. This script generates model-owned
descriptor inputs, checks semantic parity against immutable fixtures, then
compiles both product crates with those inputs. Harness keeps its archived
descriptor bytes for the handshake digest and verifies the copied output.
#>

[CmdletBinding()]
param(
    [string]$TargetDir = "Q:\sdk\work\rust-source-root-wire-target"
)

$ErrorActionPreference = "Stop"
$previousCargoTargetDir = $env:CARGO_TARGET_DIR
$env:CARGO_TARGET_DIR = $TargetDir
$repo = (Resolve-Path (Join-Path $PSScriptRoot "../../../..")).Path
$manifest = Join-Path $repo "rust\crates\sdk-contract-wire\Cargo.toml"
$modelRoot = Join-Path $env:TEMP "acyclic-sdk-contract-build-input-$PID"
$filesystemArchive = Join-Path $repo "rust\crates\filesystem\src\generated\acyclic-filesystem-v2.bin"
$filesystemFixture = Join-Path $repo "rust\crates\sdk-contract-wire\tests\fixtures\filesystem-v2.descriptor.bin"
$harnessArchive = Join-Path $repo "rust\crates\sdk-contract-wire\tests\fixtures\harness-v2.descriptor.bin"

function Invoke-Checked([string]$File, [string[]]$Arguments) {
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$File failed with exit code $LASTEXITCODE"
    }
}

function Get-LatestGeneratedOutput([string]$Prefix) {
    $buildRoot = Join-Path $TargetDir "debug\build"
    $package = Get-ChildItem $buildRoot -Directory |
        Where-Object Name -Like "$Prefix-*" |
        Where-Object { Test-Path (Join-Path $_.FullName "out") } |
        Sort-Object LastWriteTime | Select-Object -Last 1
    if ($null -eq $package) {
        throw "No generated output directory found for $Prefix"
    }
    return Join-Path $package.FullName "out"
}

function Assert-SameFile([string]$Expected, [string]$Actual, [string]$Label) {
    $expectedHash = (Get-FileHash $Expected -Algorithm SHA256).Hash
    $actualHash = (Get-FileHash $Actual -Algorithm SHA256).Hash
    if ($expectedHash -ne $actualHash) {
        throw "Generated $Label differs: expected $expectedHash, got $actualHash"
    }
}

try {
    New-Item -ItemType Directory -Force -Path $modelRoot | Out-Null
    Invoke-Checked cargo @(
        "run", "--manifest-path", $manifest, "--offline",
        "--bin", "sdk-contract-wire", "--", "generate", "--out", $modelRoot
    )
    Invoke-Checked cargo @(
        "run", "--manifest-path", $manifest, "--offline",
        "--bin", "sdk-contract-wire", "--", "check", "--out", $modelRoot
    )
    # Product artifacts are emitted and checked by the same Rust exporter that
    # owns the family descriptors; committed copies are only its checked output.
    Invoke-Checked cargo @(
        "run", "--manifest-path", $manifest, "--offline",
        "--bin", "sdk-contract-wire", "--", "check-products", "--root", $repo
    )

    # Capture the default generated APIs before exercising the descriptor override.
    Remove-Item Env:ACYCLIC_FS_MODEL_DESCRIPTOR -ErrorAction SilentlyContinue
    Remove-Item Env:ACYCLIC_HARNESS_MODEL_DESCRIPTOR -ErrorAction SilentlyContinue
    Remove-Item Env:ACYCLIC_HARNESS_ARCHIVED_DESCRIPTOR -ErrorAction SilentlyContinue
    Invoke-Checked cargo @(
        "check", "--manifest-path", (Join-Path $repo "rust\crates\filesystem\Cargo.toml"),
        "--offline", "--locked"
    )
    Invoke-Checked cargo @(
        "check", "--manifest-path", (Join-Path $repo "rust\crates\harness\Cargo.toml"),
        "--offline", "--locked", "--features", "grpc"
    )
    $baseline = Join-Path $modelRoot "baseline"
    New-Item -ItemType Directory -Force -Path $baseline | Out-Null
    $fsBaseline = Join-Path $baseline "filesystem"
    $harnessBaseline = Join-Path $baseline "harness"
    New-Item -ItemType Directory -Force -Path $fsBaseline,$harnessBaseline | Out-Null
    $fsOut = Get-LatestGeneratedOutput "acyclic-fs"
    $harnessOut = Get-LatestGeneratedOutput "acyclic-harness"
    Copy-Item (Join-Path $fsOut "*.rs") $fsBaseline
    Copy-Item (Join-Path $harnessOut "*.rs") $harnessBaseline

    $fsDescriptor = Join-Path $modelRoot "filesystem\v2\filesystem.fds.bin"
    $harnessDescriptor = Join-Path $modelRoot "harness\v2\harness.fds.bin"
    Assert-SameFile $fsDescriptor (Join-Path $repo "rust\crates\filesystem\src\generated\rust-model-filesystem-v2.bin") "Filesystem model artifact"
    Assert-SameFile $harnessDescriptor (Join-Path $repo "rust\crates\harness\src\generated\rust-model-harness-v2.bin") "Harness model artifact"
    $env:ACYCLIC_FS_MODEL_DESCRIPTOR = $fsDescriptor
    $env:ACYCLIC_HARNESS_MODEL_DESCRIPTOR = $harnessDescriptor
    $env:ACYCLIC_HARNESS_ARCHIVED_DESCRIPTOR = $harnessArchive

    Invoke-Checked cargo @(
        "check", "--manifest-path", (Join-Path $repo "rust\crates\filesystem\Cargo.toml"),
        "--offline", "--locked"
    )
    Invoke-Checked cargo @(
        "check", "--manifest-path", (Join-Path $repo "rust\crates\harness\Cargo.toml"),
        "--offline", "--locked", "--features", "grpc"
    )

    $fsModelOut = Get-LatestGeneratedOutput "acyclic-fs"
    $harnessModelOut = Get-LatestGeneratedOutput "acyclic-harness"
    foreach ($file in Get-ChildItem $fsBaseline -Filter "*.rs") {
        Assert-SameFile $file.FullName (Join-Path $fsModelOut $file.Name) "Filesystem $($file.Name)"
    }
    foreach ($file in Get-ChildItem $harnessBaseline -Filter "*.rs") {
        Assert-SameFile $file.FullName (Join-Path $harnessModelOut $file.Name) "Harness $($file.Name)"
    }

    Assert-SameFile $filesystemFixture $filesystemArchive "Filesystem handshake archive"
    $builtArchive = Get-ChildItem $harnessModelOut -Filter "harness_descriptor.bin"
    if ($null -eq $builtArchive) {
        throw "The Harness build did not produce harness_descriptor.bin"
    }
    $expectedHash = (Get-FileHash $harnessArchive -Algorithm SHA256).Hash
    $actualHash = (Get-FileHash $builtArchive.FullName -Algorithm SHA256).Hash
    if ($expectedHash -ne $actualHash) {
        throw "Harness handshake archive changed: expected $expectedHash, got $actualHash"
    }
    Write-Output "model descriptor parity, generated bindings, and archived handshake identity passed"
}
finally {
    Remove-Item -LiteralPath $modelRoot -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item Env:ACYCLIC_FS_MODEL_DESCRIPTOR -ErrorAction SilentlyContinue
    Remove-Item Env:ACYCLIC_HARNESS_MODEL_DESCRIPTOR -ErrorAction SilentlyContinue
    Remove-Item Env:ACYCLIC_HARNESS_ARCHIVED_DESCRIPTOR -ErrorAction SilentlyContinue
    if ($null -eq $previousCargoTargetDir) {
        Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
    } else {
        $env:CARGO_TARGET_DIR = $previousCargoTargetDir
    }
}

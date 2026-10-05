[CmdletBinding()]
param(
    [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")),
    [string]$BuildDirectory = (Join-Path $PSScriptRoot ".build")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Checked([string]$Command, [string[]]$Arguments) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed with exit code $LASTEXITCODE"
    }
}

$rootPath = (Resolve-Path -LiteralPath $Root).Path
$buildPath = [System.IO.Path]::GetFullPath($BuildDirectory)
$rustTarget = Join-Path $buildPath "rust-target"
$cmakeBuild = Join-Path $buildPath "consumer"
$prefix = Join-Path $buildPath "prefix"
$installedConsumer = Join-Path $buildPath "installed-consumer"
$receiptPath = Join-Path $buildPath "portable-package-receipt.json"
New-Item -ItemType Directory -Force -Path $buildPath | Out-Null

$manifest = Join-Path $rootPath "rust\crates\sdk-embedded-prototype\Cargo.toml"
$consumerSource = Join-Path $rootPath "cpp\embedded-consumer"
$sourceFiles = @(
    "rust/crates/sdk-embedded-prototype/Cargo.toml",
    "rust/crates/sdk-embedded-prototype/Cargo.lock",
    "rust/crates/sdk-embedded-prototype/build.rs",
    "rust/crates/sdk-embedded-prototype/src/lib.rs",
    "cpp/embedded-consumer/CMakeLists.txt",
    "cpp/embedded-consumer/AcyclicEmbeddedConfig.cmake.in",
    "cpp/embedded-consumer/include/acyclic/embedded.hpp",
    "cpp/embedded-consumer/main.cpp",
    "cpp/embedded-consumer/negative_lifetime_smoke.cpp",
    "cpp/embedded-consumer/cross_thread_cancel_smoke.cpp"
)

Invoke-Checked "cargo" @("build", "--locked", "--release", "--manifest-path", $manifest, "--target-dir", $rustTarget)
$release = Join-Path $rustTarget "release"
$runtime = Join-Path $release "acyclic_sdk_embedded_prototype.dll"
$importLibrary = Join-Path $release "acyclic_sdk_embedded_prototype.dll.lib"
$header = Get-ChildItem -LiteralPath (Join-Path $release "build") -Recurse -File -Filter "acyclic_embedded_prototype.h" | Select-Object -First 1
if (-not (Test-Path -LiteralPath $runtime -PathType Leaf)) { throw "Rust release DLL was not produced: $runtime" }
if (-not (Test-Path -LiteralPath $importLibrary -PathType Leaf)) { throw "Rust import library was not produced: $importLibrary" }
if (-not $header) { throw "cbindgen header was not produced under $release\build" }

$clang = (Get-Command clang++.exe -ErrorAction Stop).Source
$msvcLink = Get-ChildItem -LiteralPath "${env:ProgramFiles(x86)}\Microsoft Visual Studio" -Recurse -File -Filter link.exe -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match '\\Hostx64\\x64\\link\.exe$' } | Select-Object -First 1
if ($msvcLink) {
    # Git for Windows also ships a link.exe (the symlink utility). Put the
    # MSVC linker first so clang's Windows ABI probe cannot invoke the wrong
    # executable and stall during CMake's try_compile.
    $env:PATH = "$($msvcLink.Directory.FullName);$env:PATH"
}
$clangForCMake = $clang.Replace('\', '/')
Invoke-Checked "cmake" @("-S", $consumerSource, "-B", $cmakeBuild, "-G", "Ninja", "-DCMAKE_CXX_COMPILER=$clangForCMake", "-DACYCLIC_EMBEDDED_ROOT=$release", "-DACYCLIC_EMBEDDED_HEADER=$($header.FullName)")
Invoke-Checked "cmake" @("--build", $cmakeBuild)
Invoke-Checked "ctest" @("--test-dir", $cmakeBuild, "--output-on-failure")
Invoke-Checked "cmake" @("--install", $cmakeBuild, "--prefix", $prefix)

$expected = @(
    "include/acyclic/embedded.hpp",
    "include/acyclic_embedded_prototype.h",
    "bin/acyclic_sdk_embedded_prototype.dll",
    "lib/acyclic_sdk_embedded_prototype.dll.lib",
    "lib/cmake/AcyclicEmbedded/AcyclicEmbeddedConfig.cmake",
    "lib/cmake/AcyclicEmbedded/AcyclicEmbeddedConfigVersion.cmake"
)
$actual = @(Get-ChildItem -LiteralPath $prefix -Recurse -File | ForEach-Object {
    $_.FullName.Substring($prefix.Length + 1).Replace("\", "/")
} | Sort-Object)
if ((Compare-Object $expected $actual)) { throw "Installed package file set differs from the expected portable package" }

Invoke-Checked "cmake" @("-S", (Join-Path $consumerSource "install-consumer"), "-B", $installedConsumer, "-G", "Ninja", "-DCMAKE_CXX_COMPILER=$clangForCMake", "-DCMAKE_PREFIX_PATH=$prefix")
Invoke-Checked "cmake" @("--build", $installedConsumer)
$oldPath = $env:PATH
try {
    $env:PATH = "$(Join-Path $prefix 'bin');$oldPath"
    Invoke-Checked (Join-Path $installedConsumer "acyclic_cpp_installed_consumer.exe") @()
} finally {
    $env:PATH = $oldPath
}

$sourceDigestLines = foreach ($relative in $sourceFiles) {
    $path = Join-Path $rootPath ($relative.Replace("/", "\"))
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Source-bound input is missing: $relative" }
    "$relative $( (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant() )"
}
$sourceDigest = [Convert]::ToHexString(([System.Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes(($sourceDigestLines -join "`n"))))).ToLowerInvariant()
$artifacts = @{}
foreach ($relative in $expected) {
    $path = Join-Path $prefix ($relative.Replace("/", "\"))
    $artifacts[$relative] = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
}
$receipt = [ordered]@{
    schema_version = 1
    status = "portable-local-install-passed"
    source_digest = $sourceDigest
    generator = "cargo + cbindgen 0.29.4 + CMake/Ninja"
    platform = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
    tests = [ordered]@{ ctest = "passed"; clean_prefix_consumer = "passed"; registry_published = $false }
    artifacts = $artifacts
}
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
Write-Host "Portable C++ embedded package test passed; source-bound receipt: $receiptPath"

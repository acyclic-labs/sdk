[CmdletBinding()]
param(
    [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")),
    [string]$BuildDirectory = (Join-Path $PSScriptRoot ".build"),
    [string]$RustTarget = "",
    [string]$PlatformReceiptPath = "",
    [switch]$SkipExecution
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Checked([string]$Command, [string[]]$Arguments) {
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed with exit code $LASTEXITCODE"
    }
}

function Assert-PeMachine([string]$Path, [string]$Target) {
    $bytes = [System.IO.File]::ReadAllBytes($Path)
    $peOffset = [BitConverter]::ToInt32($bytes, 0x3C)
    $machine = [BitConverter]::ToUInt16($bytes, $peOffset + 4)
    $expected = switch -Regex ($Target) {
        '^x86_64-' { 0x8664; break }
        '^aarch64-' { 0xAA64; break }
        default { throw "Unsupported Windows Rust target for PE validation: $Target" }
    }
    if ($machine -ne $expected) {
        throw "PE machine mismatch for ${Target}: expected 0x$('{0:X4}' -f $expected), got 0x$('{0:X4}' -f $machine)"
    }
}

function Assert-PythonArm64 {
    $machine = (& python -c "import platform; print(platform.machine())").Trim().ToUpperInvariant()
    if ($machine -notin @("ARM64", "AARCH64")) {
        throw "Native ARM64 execution requires an ARM64 Python process; observed $machine"
    }
}

function Normalize-PeTimestamp([string]$Path) {
    # rustc/LLVM writes build-specific time and PDB identity data into the PE
    # image.  Normalize those non-runtime fields before hashing or installing
    # the release DLL so two clean builds from identical Rust inputs produce
    # the same package bytes.
    $bytes = [System.IO.File]::ReadAllBytes($Path)
    if ($bytes.Length -lt 0x40 -or $bytes[0] -ne 0x4D -or $bytes[1] -ne 0x5A) {
        throw "The release artifact is not a PE image: $Path"
    }
    $peOffset = [BitConverter]::ToInt32($bytes, 0x3C)
    if ($peOffset -lt 0 -or $peOffset + 12 -gt $bytes.Length -or
        $bytes[$peOffset] -ne 0x50 -or $bytes[$peOffset + 1] -ne 0x45 -or
        $bytes[$peOffset + 2] -ne 0 -or $bytes[$peOffset + 3] -ne 0) {
        throw "The release artifact has no valid PE signature: $Path"
    }
    $timestampOffset = $peOffset + 8
    [Array]::Clear($bytes, $timestampOffset, 4)

    $optionalOffset = $peOffset + 24
    $optionalSize = [BitConverter]::ToUInt16($bytes, $peOffset + 20)
    $sectionOffset = $optionalOffset + $optionalSize
    $sectionCount = [BitConverter]::ToUInt16($bytes, $peOffset + 6)
    $magic = [BitConverter]::ToUInt16($bytes, $optionalOffset)
    $dataDirectoryOffset = if ($magic -eq 0x20B) { $optionalOffset + 112 } else { $optionalOffset + 96 }
    $debugDirectoryOffset = $dataDirectoryOffset + (6 * 8)
    $debugRva = [BitConverter]::ToUInt32($bytes, $debugDirectoryOffset)
    $debugSize = [BitConverter]::ToUInt32($bytes, $debugDirectoryOffset + 4)
    if ($debugRva -ne 0 -and $debugSize -ne 0) {
        $debugFileOffset = -1
        for ($index = 0; $index -lt $sectionCount; $index++) {
            $header = $sectionOffset + (40 * $index)
            $virtualSize = [BitConverter]::ToUInt32($bytes, $header + 8)
            $virtualAddress = [BitConverter]::ToUInt32($bytes, $header + 12)
            $rawSize = [BitConverter]::ToUInt32($bytes, $header + 16)
            $rawPointer = [BitConverter]::ToUInt32($bytes, $header + 20)
            $span = [Math]::Max($virtualSize, $rawSize)
            if ($debugRva -ge $virtualAddress -and $debugRva -lt ($virtualAddress + $span)) {
                $debugFileOffset = $rawPointer + ($debugRva - $virtualAddress)
                break
            }
        }
        if ($debugFileOffset -lt 0 -or $debugFileOffset + $debugSize -gt $bytes.Length -or ($debugSize % 28) -ne 0) {
            throw "The release artifact has an invalid PE debug directory: $Path"
        }
        for ($entry = 0; $entry -lt $debugSize; $entry += 28) {
            $entryOffset = $debugFileOffset + $entry
            [Array]::Clear($bytes, $entryOffset + 4, 4)
            $type = [BitConverter]::ToUInt32($bytes, $entryOffset + 12)
            $dataSize = [BitConverter]::ToUInt32($bytes, $entryOffset + 16)
            $dataPointer = [BitConverter]::ToUInt32($bytes, $entryOffset + 24)
            # CodeView RSDS records contain a random PDB GUID. The package
            # intentionally excludes the PDB, so the GUID has no runtime use.
            if ($type -eq 2 -and $dataSize -ge 20 -and $dataPointer + $dataSize -le $bytes.Length -and
                [BitConverter]::ToUInt32($bytes, $dataPointer) -eq 0x53445352) {
                [Array]::Clear($bytes, $dataPointer + 4, 16)
            }
        }
    }
    [System.IO.File]::WriteAllBytes($Path, $bytes)
}

$rootPath = (Resolve-Path -LiteralPath $Root).Path
$buildPath = [System.IO.Path]::GetFullPath($BuildDirectory)
$rustTargetDirectory = Join-Path $buildPath "rust-target"
$cmakeBuild = Join-Path $buildPath "consumer"
$prefix = Join-Path $buildPath "prefix"
$installedConsumer = Join-Path $buildPath "installed-consumer"
$receiptPath = Join-Path $buildPath "portable-package-receipt.json"
if ($buildPath.StartsWith($rootPath + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or
    $buildPath.Equals($rootPath, [StringComparison]::OrdinalIgnoreCase)) {
    throw "BuildDirectory must be outside the source checkout: $buildPath"
}
New-Item -ItemType Directory -Force -Path $buildPath | Out-Null
if ([string]::IsNullOrWhiteSpace($PlatformReceiptPath)) {
    $PlatformReceiptPath = Join-Path $buildPath "platform-package.json"
}

if ([string]::IsNullOrWhiteSpace($RustTarget)) {
    $RustTarget = (& rustc -vV | Select-String '^host: ' | ForEach-Object { $_.Line.Substring(6).Trim() })
}
if ([string]::IsNullOrWhiteSpace($RustTarget) -or $RustTarget -notmatch '^[^-]+-pc-windows-msvc$') {
    throw "RustTarget must be a Windows MSVC target triple: $RustTarget"
}
if ($RustTarget -eq "aarch64-pc-windows-msvc" -and -not $SkipExecution) {
    $nativeArchitectures = @($env:PROCESSOR_ARCHITEW6432, $env:PROCESSOR_ARCHITECTURE) |
        Where-Object { $_ } |
        ForEach-Object { $_.ToUpperInvariant() }
    if ($nativeArchitectures -notcontains "ARM64") {
        throw "Native ARM64 execution requires an ARM64 Windows host; observed $($nativeArchitectures -join ', ')"
    }
}

$manifest = Join-Path $rootPath "rust\crates\sdk-embedded-prototype\Cargo.toml"
$consumerSource = Join-Path $rootPath "cpp\embedded-consumer"
$sourceFiles = @(
    "rust/crates/sdk-embedded-prototype/Cargo.toml",
    "rust/crates/sdk-embedded-prototype/Cargo.lock",
    "rust/crates/sdk-embedded-prototype/build.rs",
    "rust/crates/sdk-embedded-prototype/src/lib.rs",
    "rust/crates/sdk-embedded-prototype/tests/c_consumer.c",
    "rust/crates/sdk-embedded-prototype/tests/python_consumer.py",
    "cpp/embedded-consumer/CMakeLists.txt",
    "cpp/embedded-consumer/AcyclicEmbeddedConfig.cmake.in",
    "cpp/embedded-consumer/include/acyclic/embedded.hpp",
    "cpp/embedded-consumer/main.cpp",
    "cpp/embedded-consumer/negative_lifetime_smoke.cpp",
    "cpp/embedded-consumer/cross_thread_cancel_smoke.cpp"
)

Invoke-Checked "cargo" @("build", "--locked", "--offline", "--release", "--manifest-path", $manifest, "--target", $RustTarget, "--target-dir", $rustTargetDirectory)
$release = Join-Path (Join-Path $rustTargetDirectory $RustTarget) "release"
$runtime = Join-Path $release "acyclic_sdk_embedded_prototype.dll"
$importLibrary = Join-Path $release "acyclic_sdk_embedded_prototype.dll.lib"
$header = Get-ChildItem -LiteralPath (Join-Path $release "build") -Recurse -File -Filter "acyclic_embedded_prototype.h" | Select-Object -First 1
if (-not (Test-Path -LiteralPath $runtime -PathType Leaf)) { throw "Rust release DLL was not produced: $runtime" }
if (-not (Test-Path -LiteralPath $importLibrary -PathType Leaf)) { throw "Rust import library was not produced: $importLibrary" }
if (-not $header) { throw "cbindgen header was not produced under $release\build" }
Assert-PeMachine $runtime $RustTarget
Normalize-PeTimestamp $runtime

$clang = $null
$clangC = $null
if ($RustTarget -eq "aarch64-pc-windows-msvc" -and
    $env:CC_aarch64_pc_windows_msvc -and
    (Test-Path -LiteralPath $env:CC_aarch64_pc_windows_msvc)) {
    $clangC = (Resolve-Path -LiteralPath $env:CC_aarch64_pc_windows_msvc).Path
    $armClangPlusPlus = Join-Path (Split-Path -Parent $clangC) "clang++.exe"
    if (Test-Path -LiteralPath $armClangPlusPlus) { $clang = $armClangPlusPlus }
}
if (-not $clangC) { $clangC = (Get-Command clang.exe -ErrorAction Stop).Source }
if (-not $clang) { $clang = (Get-Command clang++.exe -ErrorAction Stop).Source }
$cmakeTargetArgs = @()
if ($RustTarget -eq "aarch64-pc-windows-msvc") {
    $env:CC_aarch64_pc_windows_msvc = $clangC
    # Cargo invokes the linker without a target flag.  Wrap clang so an ARM64
    # build cannot silently link the host x64 architecture.
    $linkerWrapper = Join-Path $buildPath "aarch64-clang-linker.cmd"
    @("@echo off", "`"$clangC`" --target=$RustTarget %*") | Set-Content -LiteralPath $linkerWrapper -Encoding ascii
    $env:CARGO_TARGET_AARCH64_PC_WINDOWS_MSVC_LINKER = $linkerWrapper
    $cmakeTargetArgs = @(
        "-DCMAKE_CXX_COMPILER_TARGET=$RustTarget",
        "-DCMAKE_C_COMPILER_TARGET=$RustTarget",
        "-DCMAKE_SYSTEM_NAME=Windows",
        "-DCMAKE_SYSTEM_PROCESSOR=ARM64",
        "-DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY"
    )
    Assert-PeMachine $clangC $RustTarget
}
$linkArch = if ($RustTarget -eq "aarch64-pc-windows-msvc") { "arm64" } else { "x64" }
$msvcLink = Get-ChildItem -LiteralPath "${env:ProgramFiles(x86)}\Microsoft Visual Studio" -Recurse -File -Filter link.exe -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match "\\Host(?:x64|arm64)\\$linkArch\\link\.exe$" } | Select-Object -First 1
if ($msvcLink) {
    # Git for Windows also ships a link.exe (the symlink utility). Put the
    # MSVC linker first so clang's Windows ABI probe cannot invoke the wrong
    # executable and stall during CMake's try_compile.
    $env:PATH = "$($msvcLink.Directory.FullName);$env:PATH"
}
$clangForCMake = $clang.Replace('\', '/')
Invoke-Checked "cmake" (@("-S", $consumerSource, "-B", $cmakeBuild, "-G", "Ninja", "-DCMAKE_CXX_COMPILER=$clangForCMake", "-DACYCLIC_EMBEDDED_ROOT=$release", "-DACYCLIC_EMBEDDED_HEADER=$($header.FullName)") + $cmakeTargetArgs)
Invoke-Checked "cmake" @("--build", $cmakeBuild)
foreach ($executable in @(
    "acyclic_cpp_embedded_consumer.exe",
    "acyclic_cpp_embedded_negative.exe",
    "acyclic_cpp_embedded_cross_thread.exe"
)) {
    Assert-PeMachine (Join-Path $cmakeBuild $executable) $RustTarget
}
if (-not $SkipExecution) {
    Invoke-Checked "ctest" @("--test-dir", $cmakeBuild, "--output-on-failure")
}
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

$cConsumer = Join-Path $buildPath "c-consumer.exe"
$cSource = Join-Path $rootPath "rust\crates\sdk-embedded-prototype\tests\c_consumer.c"
$installedRuntime = Join-Path $prefix "bin\acyclic_sdk_embedded_prototype.dll"
$importLibraryInstalled = Join-Path $prefix "lib\acyclic_sdk_embedded_prototype.dll.lib"
Invoke-Checked $clangC (@("--target=$RustTarget", "-std=c11", "-I$(Join-Path $prefix 'include')", $cSource, $importLibraryInstalled, "-o", $cConsumer))
Assert-PeMachine $cConsumer $RustTarget
if (-not $SkipExecution) {
    if ($RustTarget -eq "aarch64-pc-windows-msvc") { Assert-PythonArm64 }
    $oldPath = $env:PATH
    try {
        $env:PATH = "$(Join-Path $prefix 'bin');$oldPath"
        Invoke-Checked $cConsumer @()
        Invoke-Checked "python" @((Join-Path $rootPath "rust\crates\sdk-embedded-prototype\tests\python_consumer.py"), $installedRuntime)
    } finally {
        $env:PATH = $oldPath
    }
}

Invoke-Checked "cmake" (@("-S", (Join-Path $consumerSource "install-consumer"), "-B", $installedConsumer, "-G", "Ninja", "-DCMAKE_CXX_COMPILER=$clangForCMake", "-DCMAKE_PREFIX_PATH=$prefix") + $cmakeTargetArgs)
Invoke-Checked "cmake" @("--build", $installedConsumer)
$installedExecutable = Join-Path $installedConsumer "acyclic_cpp_installed_consumer.exe"
Assert-PeMachine $installedExecutable $RustTarget
if (-not $SkipExecution) {
    $oldPath = $env:PATH
    try {
        $env:PATH = "$(Join-Path $prefix 'bin');$oldPath"
        Invoke-Checked (Join-Path $installedConsumer "acyclic_cpp_installed_consumer.exe") @()
    } finally {
        $env:PATH = $oldPath
    }
}

$sourceDigestLines = foreach ($relative in $sourceFiles) {
    $path = Join-Path $rootPath ($relative.Replace("/", "\"))
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Source-bound input is missing: $relative" }
    "$relative $( (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant() )"
}
$sourceDigest = [Convert]::ToHexString(([System.Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes(($sourceDigestLines -join "`n"))))).ToLowerInvariant()
$sourceRevision = (& git -C $rootPath rev-parse HEAD).Trim()
$artifacts = @{}
foreach ($relative in $expected) {
    $path = Join-Path $prefix ($relative.Replace("/", "\"))
    $artifacts[$relative] = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
}
$receipt = [ordered]@{
    schema_version = 1
    status = if ($SkipExecution) { "portable-cross-compile-install-passed" } else { "portable-local-install-passed" }
    target = $RustTarget
    execution = if ($SkipExecution) { "not-run-cross-target" } else { "native-consumers-passed" }
    source_digest = $sourceDigest
    generator = "cargo + cbindgen 0.29.4 + CMake/Ninja"
    platform = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
    reproducibility = [ordered]@{ pe_timestamp_normalized = $true; normalized_timestamp = 0 }
    tests = [ordered]@{
        c_consumer = if ($SkipExecution) { "compiled-only-cross-target" } else { "passed" }
        python_consumer = if ($SkipExecution) { "not-run-cross-target" } else { "passed" }
        ctest = if ($SkipExecution) { "compiled-only-cross-target" } else { "passed" }
        clean_prefix_consumer = if ($SkipExecution) { "compiled-only-cross-target" } else { "passed" }
        registry_published = $false
    }
    artifacts = $artifacts
}
$runtimeArtifact = "bin/acyclic_sdk_embedded_prototype.dll"
$runtimeArtifactHash = $artifacts[$runtimeArtifact]
$consumerStatus = if ($SkipExecution) { "compiled-only-cross-target" } else { "passed" }
$consumerInvoked = -not $SkipExecution
$consumerExitCode = if ($SkipExecution) { $null } else { 0 }
$platformReceipt = [ordered]@{
    schema = "acyclic.sdk.embedded.platform-package.v1"
    status = if ($SkipExecution) { "pending-execution" } else { "passed" }
    target = $RustTarget
    source_revision = $sourceRevision
    source_digest = "sha256:$sourceDigest"
    source_inputs = @($sourceFiles)
    runtime = "acyclic_sdk_embedded_prototype.dll"
    runtime_artifact = $runtimeArtifact
    package_root = "prefix"
    artifacts = $artifacts
    consumers = [ordered]@{
        c = [ordered]@{
            status = $consumerStatus; scope = "embedded-native-abi"; invoked = $consumerInvoked; exit_code = $consumerExitCode
            source_revision = $sourceRevision; source = "rust/crates/sdk-embedded-prototype/tests/c_consumer.c"
            source_sha256 = (Get-FileHash -LiteralPath $cSource -Algorithm SHA256).Hash.ToLowerInvariant()
            package_artifact = $runtimeArtifact; package_artifact_sha256 = $runtimeArtifactHash
            checks = @("layout", "append", "read", "release", "stale_handles")
        }
        python = [ordered]@{
            status = $consumerStatus; scope = "embedded-native-abi"; invoked = $consumerInvoked; exit_code = $consumerExitCode
            source_revision = $sourceRevision; source = "rust/crates/sdk-embedded-prototype/tests/python_consumer.py"
            source_sha256 = (Get-FileHash -LiteralPath (Join-Path $rootPath "rust\crates\sdk-embedded-prototype\tests\python_consumer.py") -Algorithm SHA256).Hash.ToLowerInvariant()
            package_artifact = $runtimeArtifact; package_artifact_sha256 = $runtimeArtifactHash
            checks = @("append", "follow", "owned_buffers", "cancel", "stale_handles")
        }
        cpp = [ordered]@{
            status = $consumerStatus; scope = "embedded-native-abi"; invoked = $consumerInvoked; exit_code = $consumerExitCode
            source_revision = $sourceRevision; source = "cpp/embedded-consumer/cross_thread_cancel_smoke.cpp"
            source_sha256 = (Get-FileHash -LiteralPath (Join-Path $rootPath "cpp\embedded-consumer\cross_thread_cancel_smoke.cpp") -Algorithm SHA256).Hash.ToLowerInvariant()
            package_artifact = $runtimeArtifact; package_artifact_sha256 = $runtimeArtifactHash
            checks = @("blocked_pull_wakeup", "cross_thread_cancel", "clean_prefix_install")
        }
    }
    ctest = $consumerStatus
    clean_prefix = $consumerStatus
}
$platformReceipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $PlatformReceiptPath -Encoding utf8NoBOM
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
Write-Host "Portable C++ embedded package test passed; source-bound receipt: $receiptPath"

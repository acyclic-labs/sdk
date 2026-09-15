param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('windows')]
    [string] $Lane
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

New-Item -ItemType Directory -Force -Path `
    $env:SDK_ARTIFACT_DIR, $env:TOOLS_DIR | Out-Null
. .\scripts\ensure-bun.ps1
bun install --frozen-lockfile

# Blacksmith's Windows image does not enable the Client-ProjFS optional component.
# Exercise the portable workspace here and compile every ProjFS path; Linux and
# macOS execute the native-mount behavior. The Server image cannot load
# ProjectedFSLib.dll, so execute the portable feature set and still compile and
# link every all-feature test binary.
cargo test --workspace --exclude acyclic-fs-napi `
    --exclude acyclic-fs-daemon --locked
cargo test --workspace --all-features --no-run --locked
cargo build -p acyclic-fs-napi --locked
cargo run --locked -p acyclic-cli
bun run check
bun test typescript/packages
bun run --filter '@acyclic-labs/fs' test:composition

$clangDirectories = @()
if ($env:LLVM_PATH) {
    $clangDirectories += Join-Path $env:LLVM_PATH 'bin'
}
$clangDirectories += Join-Path $env:ProgramFiles 'LLVM\bin'
$vswhere = Join-Path ${env:ProgramFiles(x86)} `
    'Microsoft Visual Studio\Installer\vswhere.exe'
if (Test-Path -LiteralPath $vswhere) {
    $visualStudio = & $vswhere -latest -products * `
        -requires Microsoft.VisualStudio.Component.VC.Llvm.Clang `
        -property installationPath
    if ($visualStudio) {
        $clangDirectories += Join-Path $visualStudio 'VC\Tools\Llvm\x64\bin'
    }
}
$clangDirectory = $clangDirectories | Where-Object {
    Test-Path -LiteralPath (Join-Path $_ 'clang.exe')
} | Select-Object -First 1
if (-not $clangDirectory) {
    throw 'The Windows ARM64 cross-check requires the pinned clang toolchain.'
}
$env:PATH = "$clangDirectory;$env:PATH"
$env:CC_aarch64_pc_windows_msvc = Join-Path $clangDirectory 'clang.exe'
rustup target add aarch64-pc-windows-msvc
cargo check -p acyclic-fs -p acyclic-fs-napi --all-features `
    --target aarch64-pc-windows-msvc --locked

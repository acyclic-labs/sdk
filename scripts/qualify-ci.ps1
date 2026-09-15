param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('windows')]
    [string] $Lane
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

$projfs = Get-WindowsOptionalFeature -Online -FeatureName Client-ProjFS
if ($projfs.State -ne 'Enabled') {
    $enabled = Enable-WindowsOptionalFeature -Online `
        -FeatureName Client-ProjFS -NoRestart
    if ($enabled.RestartNeeded) {
        throw 'Client-ProjFS requires a restart on this runner image.'
    }
}

New-Item -ItemType Directory -Force -Path `
    $env:BUILD_ARTIFACTSTAGINGDIRECTORY, $env:TOOLS_DIR | Out-Null
. .\scripts\ensure-bun.ps1
bun install --frozen-lockfile

cargo test --workspace --all-features --locked
cargo test -p acyclic-fs --features native-mount --locked --lib `
    native_mount::adapter::tests::writable_projfs_captures_closes_renames_links_and_deletes `
    -- --ignored --test-threads=1
cargo test -p acyclic-fs --features native-mount --locked --lib `
    native_mount::usn::tests::live_journal_proves_unchanged_restart_and_fences_a_write `
    -- --ignored --test-threads=1
cargo build -p acyclic-fs-napi --locked
bun scripts/check-filesystem-napi.mjs `
    "$env:BUILD_ARTIFACTSTAGINGDIRECTORY/packages/native"
cargo run --locked -p acyclic-cli
bun run test

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
    throw 'The Windows ARM64 cross-check requires the image-provided clang.'
}
$env:PATH = "$clangDirectory;$env:PATH"
$env:CC_aarch64_pc_windows_msvc = Join-Path $clangDirectory 'clang.exe'
rustup target add aarch64-pc-windows-msvc
cargo check -p acyclic-fs -p acyclic-fs-napi --all-features `
    --target aarch64-pc-windows-msvc --locked

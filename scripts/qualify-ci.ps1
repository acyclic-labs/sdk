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

# Blacksmith's Windows Server image cannot load ProjectedFSLib.dll. Execute the
# largest workspace set whose dependency graph is genuinely portable, then test
# acyclic-fs without native mounting. The all-feature no-run build below still
# compiles and links every ProjFS path; Linux and macOS execute native mounts.
cargo test --workspace `
    --exclude acyclic-fs `
    --exclude acyclic-memory `
    --exclude acyclic-conformance `
    --exclude acyclic-sdk `
    --exclude acyclic-fs-daemon `
    --exclude acyclic-fs-napi `
    --locked
cargo test -p acyclic-fs --no-default-features `
    --features local,memory,native-watch --locked
cargo test --workspace --all-features --no-run --locked
cargo test --manifest-path `
    plugins/acyclic-agent-workspaces/control/Cargo.toml --locked
cargo clippy --manifest-path `
    plugins/acyclic-agent-workspaces/control/Cargo.toml `
    --all-targets --all-features --locked -- -D warnings
cargo build -p acyclic-fs-napi --locked
cargo run --locked -p acyclic-cli -- harness-demo
python plugins/acyclic-agent-workspaces/scripts/package.py `
    --output (Join-Path $env:SDK_ARTIFACT_DIR 'agent-workspaces-plugin')
python plugins/acyclic-agent-workspaces/scripts/validate-package.py `
    (Join-Path $env:SDK_ARTIFACT_DIR 'agent-workspaces-plugin\marketplace')
bun run check
bun test --parallel=4 typescript/packages
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

# acyclic CLI plugin (plugin/): the Windows smoke drives the named-pipe
# transport, UTF-16LE names, rewind and copy-mode forks through the real
# release binary, which is then retained as this lane's qualified artifact.
$env:CARGO_PROFILE_RELEASE_STRIP = 'symbols'
cargo build --release --locked -p acyclic
Remove-Item Env:CARGO_PROFILE_RELEASE_STRIP
$pluginTarget = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { 'target' }
$env:ACYCLIC_BIN = (Join-Path $pluginTarget 'release\acyclic.exe') -replace '\\', '/'
bash plugin/tests/acceptance/windows-smoke.sh
bash plugin/scripts/retain-binary.sh $env:ACYCLIC_BIN `
    ((Join-Path $env:SDK_ARTIFACT_DIR 'plugin\win32-x64') -replace '\\', '/')

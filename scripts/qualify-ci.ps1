param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('windows')]
    [string] $Lane
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

New-Item -ItemType Directory -Force -Path `
    $env:BUILD_ARTIFACTSTAGINGDIRECTORY, $env:TOOLS_DIR | Out-Null
. .\scripts\ensure-bun.ps1
bun install --frozen-lockfile

# Blacksmith's Windows Server 2025 image intentionally omits the Client-ProjFS
# optional component. Exercise the portable workspace here and compile every
# ProjFS path; Linux and macOS execute the native-mount behavior.
# The Server image cannot load ProjectedFSLib.dll, so execute the portable
# feature set and still compile and link every all-feature test binary.
cargo test --workspace --locked
cargo test --workspace --all-features --no-run --locked
cargo build -p acyclic-fs-napi --locked
bun scripts/check-filesystem-napi.mjs `
    "$env:BUILD_ARTIFACTSTAGINGDIRECTORY/packages/native"
cargo run --locked -p acyclic-cli
bun run test
rustup target add aarch64-pc-windows-msvc
cargo check -p acyclic-fs -p acyclic-fs-napi --all-features `
    --target aarch64-pc-windows-msvc --locked

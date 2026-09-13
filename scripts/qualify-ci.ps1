param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('windows')]
    [string] $Lane
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

if ($Lane -ne 'windows') {
    throw "unknown qualification lane: $Lane"
}

New-Item -ItemType Directory -Force -Path $env:BUILD_ARTIFACTSTAGINGDIRECTORY | Out-Null
. .\scripts\ensure-bun.ps1
cargo test --workspace --all-features --locked
cargo test -p acyclic-fs --features native-mount --locked --lib native_mount::adapter::tests::writable_projfs_captures_closes_renames_links_and_deletes -- --ignored --test-threads=1
cargo build -p acyclic-fs-napi --locked
bun scripts/check-filesystem-napi.mjs "$env:BUILD_ARTIFACTSTAGINGDIRECTORY/packages/native"
rustup target add aarch64-pc-windows-msvc
cargo check -p acyclic-fs -p acyclic-fs-napi --all-features --target aarch64-pc-windows-msvc --locked

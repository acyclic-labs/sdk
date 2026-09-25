param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('windows')]
    [string] $Lane
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

New-Item -ItemType Directory -Force -Path `
    $env:SDK_TEMP_DIR, $env:SDK_ARTIFACT_DIR, $env:TOOLS_DIR | Out-Null
. .\scripts\ensure-bun.ps1
bun install --frozen-lockfile

# Independent builds run beside the main test build in their own target
# directories so Cargo's build lock never serializes them; the shared compiler
# cache still deduplicates identical crates across them.
function Start-Background([string] $Name, [string] $Command) {
    $log = Join-Path $env:SDK_TEMP_DIR "background-$Name.log"
    $script = "`$ErrorActionPreference = 'Stop'; `$PSNativeCommandUseErrorActionPreference = `$true; $Command"
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($script))
    $process = Start-Process -FilePath (Get-Process -Id $PID).Path `
        -ArgumentList '-NoProfile', '-NonInteractive', '-EncodedCommand', $encoded `
        -NoNewWindow -PassThru -RedirectStandardOutput $log -RedirectStandardError "$log.err"
    # Cache the handle now so ExitCode stays readable after the process exits.
    $null = $process.Handle
    [pscustomobject]@{ Name = $Name; Process = $process; Log = $log }
}
function Complete-Background($Task) {
    $Task.Process.WaitForExit()
    Write-Host "::group::$($Task.Name)"
    foreach ($path in $Task.Log, "$($Task.Log).err") {
        if (Test-Path -LiteralPath $path) { Get-Content -LiteralPath $path | Write-Host }
    }
    Write-Host '::endgroup::'
    if ($Task.Process.ExitCode -ne 0) {
        throw "$($Task.Name) failed with exit code $($Task.Process.ExitCode)"
    }
}

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
cargo fetch --locked

$CargoTargetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $PWD 'target' }
$ReleaseTargetDir = "$CargoTargetDir-release"
$PluginOutput = Join-Path $env:SDK_ARTIFACT_DIR 'acyclic-plugin'
$release = Start-Background release @"
`$env:CARGO_TARGET_DIR = '$ReleaseTargetDir'
node scripts/build-product.mjs
node plugin/scripts/package.mjs --binary '$(Join-Path $ReleaseTargetDir 'release\acyclic.exe')' --out '$PluginOutput'
node plugin/scripts/validate-package.mjs '$PluginOutput'
"@
$napi = Start-Background napi `
    "cargo build -p acyclic-fs-napi --locked --target-dir '$CargoTargetDir-napi'"
$arm64 = Start-Background aarch64 @"
cargo check -p acyclic-fs -p acyclic-fs-napi --all-features --target aarch64-pc-windows-msvc --locked --target-dir '$CargoTargetDir-aarch64'
"@

# The workflow enables the Client-ProjFS optional feature before this lane, so
# the complete all-feature workspace, including ProjFS-backed acyclic-fs, runs
# from one build instead of separate portable, no-default, and link-only builds.
cargo test --workspace --all-features --locked
cargo clippy -p acyclic-plugin --all-targets --all-features --locked -- -D warnings
bun run check
bun test --parallel=4 typescript/packages
bun run --filter '@acyclic-labs/fs' test:composition

Complete-Background $napi
Complete-Background $arm64
Complete-Background $release

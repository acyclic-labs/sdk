param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('windows', 'windows-napi', 'windows-arm64', 'windows-clippy', 'windows-rust', 'windows-rust-scenarios', 'windows-fork-join')]
    [string] $Lane
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

New-Item -ItemType Directory -Force -Path `
    $env:SDK_TEMP_DIR, $env:SDK_ARTIFACT_DIR, $env:TOOLS_DIR, `
    "$env:SDK_TEMP_DIR/observability" | Out-Null

$clangDirectory = $null
if ($Lane -in @('windows', 'windows-arm64', 'windows-rust', 'windows-rust-scenarios', 'windows-fork-join')) {
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
    if ($Lane -eq 'windows-arm64' -and -not $clangDirectory) {
        throw 'The Windows ARM64 cross-check requires the pinned clang toolchain.'
    }
    $env:PATH = "$clangDirectory;$env:PATH"
    if ($Lane -eq 'windows-arm64') {
        $env:CC_aarch64_pc_windows_msvc = Join-Path $clangDirectory 'clang.exe'
    }
}
cargo fetch --locked

$CargoTargetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $PWD 'target' }
function Invoke-WindowsRustTests {
    param([ValidateSet("units", "scenarios")][string] $Shard)
    $metadata = (cargo metadata --locked --no-deps --format-version 1) -join "`n"
    $selection = $metadata | node scripts/windows-rust-targets.mjs $Shard | ConvertFrom-Json
    $targetArguments = @($selection.cargoArgs)
    $exampleBuildArguments = @($selection.exampleBuildArgs)
    Write-Host ("Windows Rust {0}: {1} targets" -f $Shard, @($selection.targets).Count)
    if ($clangDirectory) {
        $lldLink = Join-Path $clangDirectory 'lld-link.exe'
        if (Test-Path -LiteralPath $lldLink) {
            $env:CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER = $lldLink
        }
    }

    # Keep the whole workspace selected: package subsets change feature
    # unification. Both target shards retain ProjFS and all workspace features.
    # This test also resolves the WASM crates and their web-sys dependency. Its
    # generated feature cfg list is too large for sccache's Windows rustc spawn
    # path (ERROR_FILENAME_EXCED_RANGE / OS error 206), so run this invocation
    # directly through rustc while leaving the independent native builds cached.
    $rustcWrapper = $env:RUSTC_WRAPPER
    Remove-Item Env:RUSTC_WRAPPER -ErrorAction SilentlyContinue
    try {
        if ($exampleBuildArguments.Count -gt 0) {
            cargo build @exampleBuildArguments --locked --profile test --target-dir "$CargoTargetDir"
        }
        try {
            cargo nextest run @targetArguments --locked --profile ci --test-threads "$env:RUST_TEST_THREADS" --target-dir "$CargoTargetDir"
        } finally {
            $junit = Join-Path $CargoTargetDir 'nextest/ci/junit.xml'
            if (Test-Path -LiteralPath $junit) {
                Copy-Item -LiteralPath $junit -Destination "$env:SDK_TEMP_DIR/observability/rust.xml"
            }
        }
        if ($Shard -eq 'units') {
            cargo test --workspace --all-features --locked --doc --target-dir "$CargoTargetDir"
        }
    } finally {
        if ($null -eq $rustcWrapper) {
            Remove-Item Env:RUSTC_WRAPPER -ErrorAction SilentlyContinue
        } else {
            $env:RUSTC_WRAPPER = $rustcWrapper
        }
    }
}

switch ($Lane) {
    'windows-rust' {
        Invoke-WindowsRustTests units
        return
    }
    'windows-rust-scenarios' {
        Invoke-WindowsRustTests scenarios
        return
    }
    'windows-fork-join' {
        if ($clangDirectory) {
            $lldLink = Join-Path $clangDirectory 'lld-link.exe'
            if (Test-Path -LiteralPath $lldLink) {
                $env:CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER = $lldLink
            }
        }

        # Black-box fork/join conformance through hooks, the CLI and live ProjFS
        # mounts. The shared test support module carries ignored tests of its own.
        cargo test -p acyclic-plugin --all-features --locked --test fork_join -- `
            --ignored --test-threads=1 --skip support::

        return
    }
    'windows-napi' {
        cargo build -p acyclic-fs-napi --locked --target-dir "$CargoTargetDir-napi"
        return
    }
    'windows-arm64' {
        rustup target add aarch64-pc-windows-msvc
        cargo check -p acyclic-fs -p acyclic-fs-napi --all-features `
            --target aarch64-pc-windows-msvc --locked --target-dir "$CargoTargetDir-aarch64"
        return
    }
    'windows-clippy' {
        $env:CARGO_TARGET_DIR = "$CargoTargetDir-clippy"
        cargo clippy -p acyclic-plugin --all-targets --all-features --locked -- -D warnings
        $env:CARGO_BUILD_JOBS = '4'
        node scripts/clippy-feature-sets.mjs 4
        return
    }
}

& .\scripts\test-qualify-ci-windows.ps1
. .\scripts\ensure-bun.ps1
bun install --frozen-lockfile

$wasmBindgenRoot = Join-Path $env:TOOLS_DIR 'cargo'
$wasmBindgenBin = Join-Path $wasmBindgenRoot 'bin\wasm-bindgen.exe'
# The CLI must match the wasm-bindgen crate pinned in the workspace manifest.
$wasmBindgenVersion = (Select-String -LiteralPath Cargo.toml -Pattern '^wasm-bindgen = "=([^"]+)"').Matches[0].Groups[1].Value
if (-not (Test-Path -LiteralPath $wasmBindgenBin) -or
    (& $wasmBindgenBin --version) -ne "wasm-bindgen $wasmBindgenVersion") {
    cargo install --locked wasm-bindgen-cli --version $wasmBindgenVersion --root $wasmBindgenRoot
}
if ((& $wasmBindgenBin --version) -ne "wasm-bindgen $wasmBindgenVersion") {
    throw "The WASM build requires wasm-bindgen $wasmBindgenVersion."
}
$env:PATH = "$(Split-Path -Parent $wasmBindgenBin);$env:PATH"
# web-sys's large feature set exceeds sccache's Windows rustc spawn path.
# Keep native build caching, but invoke the WASM check directly through rustc.
$rustcWrapper = $env:RUSTC_WRAPPER
Remove-Item Env:RUSTC_WRAPPER -ErrorAction SilentlyContinue
try {
    node scripts/build-wasm-groups.mjs
    bun x tsc -b --force --pretty false
    bun x tsc -p scripts --pretty false
    bun run check:docs
} finally {
    if ($null -eq $rustcWrapper) {
        Remove-Item Env:RUSTC_WRAPPER -ErrorAction SilentlyContinue
    } else {
        $env:RUSTC_WRAPPER = $rustcWrapper
    }
}
$StreamNativeRoot = Join-Path $env:SDK_ARTIFACT_DIR 'stream-native-bundle'
$StreamNativeBundle = Join-Path $StreamNativeRoot 'bundle'
$StreamNativeTargetDir = "$CargoTargetDir-stream-native"
node scripts/build-stream-native.mjs build `
    --target x86_64-pc-windows-msvc `
    --output $StreamNativeBundle `
    --target-dir $StreamNativeTargetDir
Copy-Item -LiteralPath (Join-Path $StreamNativeTargetDir 'stream-native-build-inputs.receipt.json') -Destination (Join-Path $StreamNativeRoot 'producer-receipt.json')
node scripts/build-stream-native.mjs stage --bundle $StreamNativeBundle
node scripts/build-stream-native.mjs check
# The test build links dozens of test executables; LLVM's linker links them far
# faster than link.exe. Release and binding builds above keep the default
# linker so their outputs match publication builds.
if ($clangDirectory) {
    $lldLink = Join-Path $clangDirectory 'lld-link.exe'
    if (Test-Path -LiteralPath $lldLink) {
        $env:CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER = $lldLink
    }
}

foreach ($family in @('actors')) {
  $clientBundle = Join-Path $StreamNativeRoot "$family-native/bundle"
  node "scripts/build-$family-native.mjs" build --target x86_64-pc-windows-msvc `
      --output $clientBundle --target-dir "$CargoTargetDir-$family-native"
  Copy-Item -LiteralPath "$CargoTargetDir-$family-native/$family-native-build-inputs.receipt.json" -Destination (Join-Path (Split-Path -Parent $clientBundle) 'producer-receipt.json')
  node "scripts/build-$family-native.mjs" stage --bundle $clientBundle
}
cargo build --offline --locked -p acyclic-actors --example conformance-certificate

# Bun's Windows isolated test worker crashed while loading the Pi native bridge.
bun test --isolate --reporter=junit `
    --reporter-outfile="$env:SDK_TEMP_DIR/observability/bun-test.xml" typescript/packages
bun run --filter '@acyclic-labs/fs' test:composition

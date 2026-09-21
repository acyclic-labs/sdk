param(
    [string] $Backend = 'windows-projfs'
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

if ($Backend -ne 'windows-projfs') {
    throw "unknown native mount backend: $Backend"
}
try {
    $projectedFs = Get-WindowsOptionalFeature -Online -FeatureName Client-ProjFS
    if ($projectedFs.State -ne 'Enabled') {
        [Console]::Error.WriteLine('{"code":"native_mount_prerequisite_missing","backend":"windows-projfs","reason":"Windows optional feature Client-ProjFS is not enabled"}')
        exit 2
    }
} catch {
    # DISM feature inspection requires elevation on some runners. The native
    # capability probe below remains authoritative and emits a JSON failure.
}
$repository = (& git rev-parse --show-toplevel).Trim()
if ($LASTEXITCODE -ne 0) {
    throw 'cannot resolve repository root'
}
$artifactDir = if ($env:SDK_ARTIFACT_DIR) {
    $env:SDK_ARTIFACT_DIR
} elseif ($env:RUNNER_TEMP) {
    Join-Path $env:RUNNER_TEMP 'acyclic-native-mount'
} else {
    Join-Path $env:TEMP 'acyclic-native-mount'
}
New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null
$releaseExecutable = if ($env:ACYCLIC_RELEASE_EXECUTABLE) {
    $env:ACYCLIC_RELEASE_EXECUTABLE
} else {
    $targetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $repository 'target' }
    cargo build --locked -p acyclic --release
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
    Join-Path $targetDir 'release\acyclic.exe'
}
if (-not (Test-Path -LiteralPath $releaseExecutable -PathType Leaf)) {
    throw "release executable is unavailable: $releaseExecutable"
}
$arguments = @(
    '--require-kind', $Backend,
    '--release-executable', $releaseExecutable,
    '--checkout-root', $repository,
    '--output', (Join-Path $artifactDir "$Backend.json")
)
if ($env:ACYCLIC_QUALIFIER_EXECUTABLE) {
    if (-not (Test-Path -LiteralPath $env:ACYCLIC_QUALIFIER_EXECUTABLE -PathType Leaf)) {
        throw "qualification executable is unavailable: $env:ACYCLIC_QUALIFIER_EXECUTABLE"
    }
    & $env:ACYCLIC_QUALIFIER_EXECUTABLE @arguments
} else {
    cargo run --locked -p acyclic-conformance --features local-runner `
        --bin native-mount-qualify -- @arguments
}
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

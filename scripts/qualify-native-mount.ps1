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
cargo run --locked -p acyclic-conformance --features local-runner `
    --bin native-mount-qualify -- `
    --require-kind $Backend `
    --checkout-root $repository `
    --output (Join-Path $artifactDir "$Backend.json")
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

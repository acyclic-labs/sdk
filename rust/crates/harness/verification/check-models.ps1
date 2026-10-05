param(
    [ValidateSet('ActivationRecovery', 'ForkBoundary')][string]$Model = 'ActivationRecovery',
    [Parameter(Mandatory = $true)][string]$ToolsJar,
    [Parameter(Mandatory = $true)][string]$EvidenceDirectory,
    [string]$Java = 'java'
)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$expectedJarHash = '936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88'
$jar = (Resolve-Path -LiteralPath $ToolsJar).Path
if ((Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expectedJarHash) {
    throw 'Expected the pinned TLA+ v1.7.4 tla2tools.jar artifact.'
}
$evidence = [System.IO.Path]::GetFullPath($EvidenceDirectory)
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$runId = [guid]::NewGuid().ToString('N')
$cases = if ($Model -eq 'ActivationRecovery') { @(
    @{ Name = 'safe'; Config = 'ActivationRecovery.cfg'; Exit = 0; Expected = 'Model checking completed. No error has been found.' },
    @{ Name = 'unsafe'; Config = 'ActivationRecoveryUnsafe.cfg'; Exit = 12; Expected = 'Invariant AdmittedClaimRetained is violated.' }
) } else { @(
    @{ Name = 'safe'; Config = 'ForkBoundary.cfg'; Exit = 0; Expected = 'Model checking completed. No error has been found.' },
    @{ Name = 'early-dispatch'; Config = 'ForkBoundaryEarlyDispatch.cfg'; Exit = 12; Expected = 'Invariant DispatchRequiresCompleteBatch is violated.' },
    @{ Name = 'mutable-capture'; Config = 'ForkBoundaryMutableCapture.cfg'; Exit = 12; Expected = 'Invariant InheritedCaptureRemainsPinned is violated.' }
) }
foreach ($case in $cases) {
    $log = Join-Path $evidence "$runId-$($case.Name).log"
    $states = Join-Path $evidence "$runId-$($case.Name)-states"
    & $Java -Xmx512m -XX:+UseParallelGC -cp $jar tlc2.TLC -workers 1 -fp 0 -seed 1 `
        -metadir $states -config (Join-Path $PSScriptRoot $case.Config) `
        (Join-Path $PSScriptRoot "$Model.tla") 2>&1 | Tee-Object -FilePath $log
    $observedExit = $LASTEXITCODE
    if ($observedExit -ne $case.Exit) {
        throw "Unexpected TLC exit for $($case.Name): $observedExit; expected $($case.Exit). Log: $log"
    }
    if (-not (Select-String -LiteralPath $log -SimpleMatch $case.Expected)) {
        throw "Missing expected TLC result for $($case.Name). Log: $log"
    }
}
exit 0

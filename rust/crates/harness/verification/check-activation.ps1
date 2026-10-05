param(
    [Parameter(Mandatory = $true)][string]$ToolsJar,
    [Parameter(Mandatory = $true)][string]$EvidenceDirectory,
    [string]$Java = 'java'
)
& (Join-Path $PSScriptRoot 'check-models.ps1') -Model ActivationRecovery @PSBoundParameters
exit $LASTEXITCODE

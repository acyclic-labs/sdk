param(
    [Parameter(Mandatory = $true)][string]$PackageRoot
)

$ErrorActionPreference = 'Stop'
$managed = Join-Path $PackageRoot 'lib/net8.0/Acyclic.Actors.dll'
$native = Join-Path $PackageRoot 'runtimes/win-x64/native/acyclic_actors_uniffi.dll'
$assembly = [Reflection.Assembly]::LoadFrom($managed)
$identity = $assembly.GetName()
$informational = $assembly.GetCustomAttributesData() |
    Where-Object AttributeType -eq ([Reflection.AssemblyInformationalVersionAttribute]) |
    Select-Object -First 1

Write-Output ("NUPKG_CORRECTED_PRODUCER=assembly={0}; version={1}; mvid={2}; managed={3}" -f
    $identity.Name, $identity.Version, $assembly.ManifestModule.ModuleVersionId, $managed)
if ($null -ne $informational) {
    Write-Output ("NUPKG_CORRECTED_PRODUCER_INFORMATIONAL={0}" -f $informational.ConstructorArguments[0].Value)
}

$libraries = [Collections.Generic.HashSet[string]]::new()
foreach ($type in $assembly.GetTypes()) {
    foreach ($method in $type.GetMethods([Reflection.BindingFlags]'Static,NonPublic,Public')) {
        foreach ($attribute in $method.GetCustomAttributesData()) {
            if ($attribute.AttributeType -eq [Runtime.InteropServices.DllImportAttribute]) {
                [void]$libraries.Add([string]$attribute.ConstructorArguments[0].Value)
            }
        }
    }
}
Write-Output ("NUPKG_CORRECTED_NATIVE_IMPORTS={0}" -f (($libraries | Sort-Object) -join ','))
Write-Output ("NUPKG_CORRECTED_NATIVE_CLOSURE={0}" -f ((Get-FileHash $native -Algorithm SHA256).Hash))

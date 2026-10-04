[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $PackagesRoot
)

$ErrorActionPreference = 'Stop'
foreach ($family in @('actors', 'workers', 'stream', 'objects', 'inference')) {
    $package = Join-Path $PackagesRoot $family
    if (-not (Test-Path -LiteralPath $package -PathType Container)) { throw "Missing Ada generated package: $package" }
    $projectName = 'acyclic' + $family
    $project = Join-Path $package "$projectName.gpr"
    $content = @"
-- Rust-owned compatibility project for OpenAPI Generator Ada output.
project $projectName is
   for Source_Dirs use ();
end $projectName;
"@
    Set-Content -LiteralPath $project -Value $content -Encoding utf8NoBOM
}

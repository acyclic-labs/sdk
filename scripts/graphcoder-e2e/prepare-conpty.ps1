param([Parameter(Mandatory = $true)][string]$OutputAssembly)
$ErrorActionPreference = 'Stop'
$source = Join-Path $PSScriptRoot '../graphcoder-conpty.cs'
# Compile ahead of suite timing. Use Windows PowerShell, matching the host.
Add-Type -TypeDefinition ([IO.File]::ReadAllText($source)) -OutputAssembly $OutputAssembly
[Console]::Out.WriteLine([IO.Path]::GetFullPath($OutputAssembly))

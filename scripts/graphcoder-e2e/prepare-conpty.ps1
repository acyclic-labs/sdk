param([Parameter(Mandatory = $true)][string]$OutputAssembly, [switch]$Executable)
$ErrorActionPreference = 'Stop'
$source = Join-Path $PSScriptRoot '../graphcoder-conpty.cs'
# Compile once ahead of suite timing, using the same real ConPTY transport.
$kind = if ($Executable) { 'ConsoleApplication' } else { 'Library' }
Add-Type -TypeDefinition ([IO.File]::ReadAllText($source)) -OutputAssembly $OutputAssembly -OutputType $kind
[Console]::Out.WriteLine([IO.Path]::GetFullPath($OutputAssembly))

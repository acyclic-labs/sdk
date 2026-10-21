# Actual Win32 ConPTY transport for test-graphcoder-pty.mjs; never a redirected-stdin substitute.
param(
    [Parameter(Mandatory = $true)][string]$Binary,
    [Parameter(Mandatory = $true)][string]$Workspace,
    [Parameter(Mandatory = $true)][string]$Catalog,
    [Parameter(Mandatory = $true)][string]$SettingsRoot,
    [string]$Model = "scripted/test",
    [int]$DeadlineMilliseconds = 90000,
    [string]$HostAssembly,
    [switch]$Resume
)
$ErrorActionPreference = 'Stop'
if (-not [IO.Path]::IsPathRooted($Binary) -or -not [IO.Path]::IsPathRooted($Workspace) -or -not [IO.Path]::IsPathRooted($Catalog)) {
    throw 'ConPTY requires absolute binary, workspace and catalog paths'
}
if ($DeadlineMilliseconds -lt 1000 -or $DeadlineMilliseconds -gt 120000) {
    throw 'ConPTY deadline must be finite (1000..120000 ms)'
}
if ($HostAssembly) { Add-Type -Path $HostAssembly }
else { Add-Type -Path (Join-Path $PSScriptRoot 'graphcoder-conpty.cs') }
try { exit [GraphcoderConPty]::Run($Binary, $Workspace, $Catalog, $SettingsRoot, $Model, $DeadlineMilliseconds, $Resume.IsPresent) }
catch { [Console]::Error.WriteLine($_.Exception.ToString()); exit 125 }

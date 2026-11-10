#Requires -Version 7.0
# The release assembler pins this installer to the coordinated release version.
$ErrorActionPreference = 'Stop'
$version = '@VERSION@'
if (-not $IsWindows) { throw 'Use install-graphcoder.sh on Linux or macOS.' }
$arch = switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
    X64 { 'x64' }
    Arm64 { 'arm64' }
    default { throw 'GraphCoder requires x86_64 or arm64 Windows.' }
}
$asset = "graphcoder-win32-$arch.exe"
$directory = if ($env:GRAPHCODER_INSTALL_DIR) { $env:GRAPHCODER_INSTALL_DIR } else {
    Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'GraphCoder/bin'
}
$directory = [System.IO.Path]::GetFullPath($directory)
[System.IO.Directory]::CreateDirectory($directory) | Out-Null
$staging = Join-Path $directory ('.graphcoder-install.' + [guid]::NewGuid())
[System.IO.Directory]::CreateDirectory($staging) | Out-Null
function Get-Asset([string] $Name, [string] $Destination) {
    if ($env:GRAPHCODER_ASSET_DIR) {
        Copy-Item -LiteralPath (Join-Path $env:GRAPHCODER_ASSET_DIR $Name) -Destination $Destination
    } else {
        Invoke-WebRequest -Uri "https://github.com/acyclic-labs/sdk/releases/download/acyclic-v$version/$Name" -OutFile $Destination
    }
}
try {
    $sums = Join-Path $staging 'SHA256SUMS'
    Get-Asset 'SHA256SUMS' $sums
    $entries = @(Get-Content -LiteralPath $sums | Where-Object {
        $_ -cmatch ('^[0-9a-f]{64}  ' + [regex]::Escape($asset) + '$')
    })
    if ($entries.Count -ne 1) { throw 'GraphCoder requires one exact SHA256SUMS entry.' }
    $expected = $entries[0].Substring(0, 64)
    $candidate = Join-Path $staging 'graphcoder.exe'
    Get-Asset $asset $candidate
    if ((Get-FileHash -LiteralPath $candidate -Algorithm SHA256).Hash.ToLowerInvariant() -cne $expected) {
        throw 'GraphCoder SHA-256 mismatch.'
    }
    $actual = & $candidate --version
    if ($LASTEXITCODE -ne 0 -or $actual -cne "graphcoder $version") { throw 'GraphCoder version mismatch.' }
    # Same-volume replacement; a running/locked executable fails without removing it.
    [System.IO.File]::Move($candidate, (Join-Path $directory 'graphcoder.exe'), $true)
    Write-Output "Installed $actual to $directory/graphcoder.exe"
    Write-Output "Add $directory to PATH if needed."
} finally {
    # Staging is a freshly created child of the resolved installation directory.
    Remove-Item -LiteralPath $staging -Recurse -Force
}

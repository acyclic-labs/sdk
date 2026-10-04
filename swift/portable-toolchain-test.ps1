[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ToolchainRoot,
    [string]$OutputDirectory = (Join-Path (Get-Location) 'build/swift-windows-portable')
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path $ToolchainRoot).Path
$swift = Get-ChildItem (Join-Path $root 'Toolchains') -Filter swift.exe -Recurse -File |
    Where-Object { $_.FullName -match '\\usr\\bin\\swift\.exe$' } |
    Select-Object -First 1
if (-not $swift) { throw "swift.exe was not found below $root\Toolchains" }
$sdk = Get-ChildItem (Join-Path $root 'Platforms') -Directory -Recurse |
    Where-Object { $_.Name -eq 'Windows.sdk' } | Select-Object -First 1
if (-not $sdk) { throw "Windows.sdk was not found below $root\Platforms" }
$runtime = Get-ChildItem (Join-Path $root 'Runtimes') -Directory -Recurse |
    Where-Object { $_.FullName -match '\\usr$' } | Select-Object -First 1
if (-not $runtime) { throw "Swift runtime usr directory was not found below $root\Runtimes" }
$runtimeBin = Join-Path $runtime.FullName 'bin'

$out = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $out | Out-Null
$source = Join-Path $out 'hello.swift'
$binary = Join-Path $out 'hello.exe'
Set-Content -LiteralPath $source -Value 'print("swift-portable-hello")' -Encoding utf8

# Swift 6.4 rejects the host's case-variant PATH entries. Build a clean child
# environment so this test is reproducible and does not alter the host PATH.
$psi = [Diagnostics.ProcessStartInfo]::new()
$psi.FileName = (Join-Path $swift.Directory.FullName 'swiftc.exe')
$psi.Arguments = '"' + $source + '" -sdk "' + $sdk.FullName + '" -o "' + $binary + '"'
$psi.WorkingDirectory = $out
$psi.UseShellExecute = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
$psi.Environment.Clear()
$psi.Environment['PATH'] = "C:\Windows\System32;C:\Windows;$($swift.Directory.FullName);$runtimeBin"
$psi.Environment['SystemRoot'] = 'C:\Windows'
$psi.Environment['TEMP'] = $out
$psi.Environment['TMP'] = $out
$psi.Environment['USERPROFILE'] = $out
$psi.Environment['HOMEDRIVE'] = ([System.IO.Path]::GetPathRoot($out)).TrimEnd('\')
$psi.Environment['HOMEPATH'] = $out.Substring(([System.IO.Path]::GetPathRoot($out)).Length - 1)
$psi.Environment['LOCALAPPDATA'] = $out
$psi.Environment['APPDATA'] = $out
$psi.Environment['PROGRAMDATA'] = 'C:\ProgramData'
$process = [Diagnostics.Process]::new()
$process.StartInfo = $psi
[void]$process.Start()
$stdout = $process.StandardOutput.ReadToEnd()
$stderr = $process.StandardError.ReadToEnd()
$process.WaitForExit()
if ($process.ExitCode -ne 0) { throw "swiftc failed ($($process.ExitCode)): $stderr$stdout" }

$env:Path = "C:\Windows\System32;C:\Windows;$runtimeBin;$($swift.Directory.FullName)"
$run = & $binary 2>&1
if ($LASTEXITCODE -ne 0 -or ($run -join "`n").Trim() -ne 'swift-portable-hello') {
    throw "compiled executable failed: exit $LASTEXITCODE output '$($run -join "`n")'"
}
$receipt = [ordered]@{
    status = 'passed'
    swiftc = $psi.FileName
    sdk = $sdk.FullName
    runtime = $runtimeBin
    source = $source
    binary = $binary
    output = ($run -join "`n").Trim()
    target = 'x86_64-unknown-windows-msvc'
}
$receipt | ConvertTo-Json -Depth 4 | Set-Content (Join-Path $out 'portable-toolchain-receipt.json') -Encoding utf8
Write-Output (Join-Path $out 'portable-toolchain-receipt.json')

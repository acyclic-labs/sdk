param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('start', 'finish')]
    [string] $Phase
)

# Installs the pinned Rust toolchain and unpacks locked crates on Windows.
# `start` launches the work through WMI so it outlives the workflow step (the
# runner ends every process in a step's job object) and overlaps the slow cache
# restores that follow; `finish` joins it, and repeats any part that did not
# complete inline so the outcome never depends on the overlap.

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

$toolchain = '1.94.0'
$log = Join-Path $env:RUNNER_TEMP 'rust-setup.log'
$status = Join-Path $env:RUNNER_TEMP 'rust-setup.status'
$work = @"
rustup toolchain install $toolchain --profile minimal --no-self-update --component clippy --component rustfmt
rustup default $toolchain
rustup target add aarch64-pc-windows-msvc --toolchain $toolchain
cargo fetch --locked
"@

if ($Phase -eq 'start') {
    $script = Join-Path $env:RUNNER_TEMP 'rust-setup.ps1'
    $inherited = foreach ($name in 'PATH', 'CARGO_HOME', 'RUSTUP_HOME', 'CARGO_INCREMENTAL') {
        $value = [Environment]::GetEnvironmentVariable($name)
        if ($value) { "`$env:$name = '$($value.Replace("'", "''"))'" }
    }
    @"
`$ErrorActionPreference = 'Stop'
`$PSNativeCommandUseErrorActionPreference = `$true
$($inherited -join "`n")
Set-Location -LiteralPath '$($env:GITHUB_WORKSPACE.Replace("'", "''"))'
try {
    & {
$work
    } *>> '$log'
    Set-Content -LiteralPath '$status' -Value 0
} catch {
    `$_ | Out-String | Add-Content -LiteralPath '$log'
    Set-Content -LiteralPath '$status' -Value 1
}
"@ | Set-Content -LiteralPath $script
    $shell = (Get-Process -Id $PID).Path
    try {
        $created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
            CommandLine = "`"$shell`" -NoProfile -NonInteractive -File `"$script`""
            CurrentDirectory = $env:GITHUB_WORKSPACE
        }
        if ($created.ReturnValue -ne 0) { throw "Win32_Process.Create returned $($created.ReturnValue)" }
        Write-Host "Started detached Rust setup as process $($created.ProcessId)"
    } catch {
        Write-Host "Detached Rust setup is unavailable; finish will run it inline: $_"
        Set-Content -LiteralPath $status -Value 1
    }
    exit 0
}

$deadline = (Get-Date).AddMinutes(10)
while (-not (Test-Path -LiteralPath $status) -and (Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 500
}
if (Test-Path -LiteralPath $log) {
    Write-Host '::group::Detached Rust setup'
    Get-Content -LiteralPath $log | Write-Host
    Write-Host '::endgroup::'
}
$succeeded = (Test-Path -LiteralPath $status) -and ((Get-Content -LiteralPath $status -Raw).Trim() -eq '0')
if (-not $succeeded) {
    Write-Host 'Completing Rust setup inline'
    Invoke-Expression $work
}
rustc --version
cargo fetch --locked --offline

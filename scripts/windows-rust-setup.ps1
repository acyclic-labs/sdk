param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('start', 'dependencies', 'finish')]
    [string] $Phase
)

# Installs the pinned Rust toolchain and unpacks locked crates on Windows.
# `start` launches the work through WMI so it outlives the workflow step (the
# runner ends every process in a step's job object) and overlaps the slow cache
# restores that follow. The detached toolchain install begins at once; crate
# unpacking waits for `dependencies`, which marks the crate archive cache as
# restored. `finish` joins the work and repeats any part that did not complete
# inline, so the outcome never depends on the overlap.

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Set-StrictMode -Version Latest

$toolchain = '1.94.0'
$log = Join-Path $env:RUNNER_TEMP 'rust-setup.log'
$status = Join-Path $env:RUNNER_TEMP 'rust-setup.status'
$restored = Join-Path $env:RUNNER_TEMP 'rust-setup.dependencies'
$install = @"
rustup toolchain install $toolchain --profile minimal --no-self-update --component clippy --component rustfmt
rustup default $toolchain
rustup target add aarch64-pc-windows-msvc --toolchain $toolchain
"@
$unpack = 'cargo fetch --locked'

switch ($Phase) {
    'start' {
        $script = Join-Path $env:RUNNER_TEMP 'rust-setup.ps1'
        $inherited = foreach ($name in 'PATH', 'CARGO_HOME', 'RUSTUP_HOME') {
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
$install
        while (-not (Test-Path -LiteralPath '$restored')) { Start-Sleep -Milliseconds 250 }
$unpack
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
    }
    'dependencies' {
        New-Item -ItemType File -Force -Path $restored | Out-Null
    }
    'finish' {
        New-Item -ItemType File -Force -Path $restored | Out-Null
        $deadline = (Get-Date).AddMinutes(10)
        while (-not (Test-Path -LiteralPath $status) -and (Get-Date) -lt $deadline) {
            Start-Sleep -Milliseconds 250
        }
        if (Test-Path -LiteralPath $log) {
            Write-Host '::group::Detached Rust setup'
            Get-Content -LiteralPath $log | Write-Host
            Write-Host '::endgroup::'
        }
        $succeeded = (Test-Path -LiteralPath $status) -and
            ((Get-Content -LiteralPath $status -Raw).Trim() -eq '0')
        if (-not $succeeded) {
            Write-Host 'Completing Rust setup inline'
            Invoke-Expression $install
            Invoke-Expression $unpack
        }
        rustc --version
        cargo fetch --locked --offline
    }
}

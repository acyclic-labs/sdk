$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$source = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'qualify-ci.ps1')
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseInput($source, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw ($errors.Message -join "`n") }
$dispatch = $ast.Find({ param($node) $node -is [System.Management.Automation.Language.SwitchStatementAst] }, $true)
$clangSetup = $ast.Find({ param($node)
    $node -is [System.Management.Automation.Language.IfStatementAst] -and
    $node.Clauses[0].Item1.Extent.Text -match '-in.*windows-arm64'
}, $true)
if ($null -eq $clangSetup) { throw 'missing lane-specific compiler setup' }
$script:Probes = 0
function Test-Path { $script:Probes++; return $false }
foreach ($Lane in 'windows-napi', 'windows-clippy') {
    & ([scriptblock]::Create($clangSetup.Extent.Text))
}
if ($script:Probes) { throw 'native-only audit probed the ARM64 compiler' }
foreach ($Lane in 'windows', 'windows-rust', 'windows-fork-join') {
    & ([scriptblock]::Create($clangSetup.Extent.Text))
}
$Lane = 'windows-arm64'
$missingCompiler = $false
try { & ([scriptblock]::Create($clangSetup.Extent.Text)) } catch {
    if ($_.Exception.Message -ne 'The Windows ARM64 cross-check requires the pinned clang toolchain.') { throw }
    $missingCompiler = $true
}
if (-not $missingCompiler) { throw 'ARM64 audit accepted a missing compiler' }

$settings = $ast.FindAll({ param($node)
    $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and
    $node.Left -is [System.Management.Automation.Language.VariableExpressionAst] -and
    $node.Left.VariablePath.UserPath -in 'ErrorActionPreference', 'PSNativeCommandUseErrorActionPreference'
}, $true) | ForEach-Object { $_.Extent.Text }
$powershell = (Get-Process -Id $PID).Path
# Execute the real dispatch under its production error settings in an isolated
# process. Only external operations are replaced, each by a real native process.
# Discover fault sites from execution instead of mirroring command text/order.
$probe = ($settings -join "`n") + "`n" + '$dispatchBody = {' + $dispatch.Extent.Text + "`n}`n" + @'
Set-StrictMode -Version Latest
$native = Join-Path $env:SystemRoot 'System32/cmd.exe'
$script:Calls = [System.Collections.Generic.List[string]]::new()
$script:FailAt = 0
function Invoke-ControlledNative([string] $Role) {
    $script:Calls.Add($Role)
    $exitCode = if ($script:Calls.Count -eq $script:FailAt) { 7 } else { 0 }
    & $native /d /c "exit $exitCode"
}
function cargo {
    $role = if ($args[0] -eq 'test' -and $args -contains '--doc') { 'doc' } else { [string]$args[0] }
    Invoke-ControlledNative $role
}
function node { Invoke-ControlledNative 'node' }
function rustup { Invoke-ControlledNative 'rustup' }
function Test-Path { return $false }
$CargoTargetDir = 'fixture-target'
$clangDirectory = $null
$env:RUST_TEST_THREADS = '32'
foreach ($Lane in 'windows-rust', 'windows-fork-join', 'windows-napi', 'windows-arm64', 'windows-clippy') {
    foreach ($wrapper in @($null, 'fixture-original-wrapper')) {
        $script:FailAt = 0
        $siteCount = $null
        do {
            $script:Calls.Clear()
            if ($null -eq $wrapper) { Remove-Item Env:RUSTC_WRAPPER -ErrorAction SilentlyContinue }
            else { $env:RUSTC_WRAPPER = $wrapper }
            $originalEnvironment = @{}
            foreach ($name in 'RUSTC_WRAPPER', 'CARGO_TARGET_DIR', 'CARGO_BUILD_JOBS', 'RUST_TEST_THREADS') {
                $originalEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
            }
            $rejected = $false
            try { & $dispatchBody } catch { $rejected = $true }
            if ([Environment]::GetEnvironmentVariable('RUSTC_WRAPPER') -cne $wrapper) {
                throw "$Lane failed to restore original compiler wrapper at fault site $script:FailAt"
            }
            if ($Lane -eq 'windows-rust') {
                foreach ($name in $originalEnvironment.Keys) {
                    if ([Environment]::GetEnvironmentVariable($name) -cne $originalEnvironment[$name]) {
                        throw "Rust qualification changed $name at fault site $script:FailAt"
                    }
                }
            }
            if ($script:FailAt -eq 0) {
                if ($rejected -or $script:Calls.Count -eq 0) { throw "$Lane did not complete controlled native execution" }
                if ($Lane -eq 'windows-rust') {
                    foreach ($role in 'run', 'nextest', 'doc') {
                        if (-not $script:Calls.Contains($role)) { throw "Rust qualification did not exercise $role" }
                    }
                }
                $siteCount = $script:Calls.Count
            } elseif (-not $rejected -or $script:Calls.Count -ne $script:FailAt) {
                throw "$Lane continued downstream of native failure at site $script:FailAt"
            }
            # The test process restores its own fixture environment between faults;
            # production owns restoration of RUSTC_WRAPPER, asserted above.
            foreach ($name in $originalEnvironment.Keys) {
                [Environment]::SetEnvironmentVariable($name, $originalEnvironment[$name])
            }
            $script:FailAt++
        } while ($script:FailAt -le $siteCount)
    }
}
Write-Output WINDOWS_NATIVE_FAILURE_CONTROLS_PASSED
'@
$start = [System.Diagnostics.ProcessStartInfo]::new($powershell)
$start.UseShellExecute = $false
$start.CreateNoWindow = $true
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
foreach ($argument in '-NoProfile', '-NonInteractive', '-EncodedCommand',
    [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($probe))) { $start.ArgumentList.Add($argument) }
$process = [System.Diagnostics.Process]::Start($start)
try {
    $outputTask = $process.StandardOutput.ReadToEndAsync()
    $errorTask = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit(10000)) { $process.Kill(); throw 'native failure probe timed out' }
    $output = $outputTask.GetAwaiter().GetResult() + $errorTask.GetAwaiter().GetResult()
    if ($process.ExitCode -ne 0 -or -not $output.Contains('WINDOWS_NATIVE_FAILURE_CONTROLS_PASSED')) {
        throw "Native dispatch failure controls failed: $output"
    }
} finally { $process.Dispose() }
$action = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot '../.github/actions/qualification-lane/action.yml')
$installation = ($action -split '    - name: Install Windows nextest', 2)[1] -split '    - name:', 2
$body = ($installation[0] -split '      run: \|\r?\n', 2)[1] -replace '(?m)^        ', ''
$install = [scriptblock]::Create($body)
$priorTemp = $env:RUNNER_TEMP
$priorGithubPath = $env:GITHUB_PATH
try {
    $env:RUNNER_TEMP = 'fixture-tools'
    $env:GITHUB_PATH = 'fixture-path'
    $script:Downloads = 0
    $script:Version = @('cargo-nextest 0.9.146 (8af696ddc 2026-09-21)', 'release: 0.9.146', 'commit-hash: 8af696ddcce8fff2962d6a5168b6d138b8616a35', 'host: x86_64-pc-windows-msvc')
    $script:InstalledVersion = $script:Version
    function Join-Path($Path, $ChildPath) { if ($ChildPath -eq 'cargo-nextest.exe') { return 'Fake-Nextest' }; return "$Path/$ChildPath" }
    function New-Item { }
    function Test-Path { return $true }
    function Fake-Nextest { return $script:Version }
    function Invoke-WebRequest { $script:Downloads++ }
    function Get-FileHash { return @{Hash='809b01f2c94d031ab6cf5f4d4a4f477c8f51b69c96ab3cb2fe4db7ecb222d0d3'} }
    function tar { $script:Version = $script:InstalledVersion }
    function Add-Content { }
    & $install
    if ($script:Downloads -ne 0) { throw 'valid multiline cached version was downloaded again' }
    $script:Version = @('cargo-nextest 0.9.146')
    & $install
    if ($script:Downloads -ne 0) { throw 'valid brief cached version was downloaded again' }
    foreach ($wrong in '0.9.145', '0.9.1461', '0.9.146-dev') {
        $script:Downloads = 0
        $script:Version = @("cargo-nextest $wrong (fixture)", "release: $wrong")
        & $install
        if ($script:Downloads -ne 1) { throw "unexpected release accepted: $wrong" }
    }
    $script:InstalledVersion = @('cargo-nextest 0.9.145 (fixture)', 'release: 0.9.145')
    $script:Version = $script:InstalledVersion
    $rejected = $false
    try { & $install } catch {
        if ($_.Exception.Message -ne 'Windows qualification requires cargo-nextest 0.9.146') { throw }
        $rejected = $true
    }
    if (-not $rejected) { throw 'wrong freshly installed release accepted' }
} finally {
    if ($null -eq $priorTemp) { Remove-Item Env:RUNNER_TEMP -ErrorAction SilentlyContinue }
    else { $env:RUNNER_TEMP = $priorTemp }
    if ($null -eq $priorGithubPath) { Remove-Item Env:GITHUB_PATH -ErrorAction SilentlyContinue }
    else { $env:GITHUB_PATH = $priorGithubPath }
}
$setupSource = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'windows-rust-setup.ps1')
$setupAst = [System.Management.Automation.Language.Parser]::ParseInput($setupSource, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw ($errors.Message -join "`n") }
$setupInstall = $setupAst.Find({ param($node)
    $node -is [System.Management.Automation.Language.AssignmentStatementAst] -and $node.Left.Extent.Text -eq '$install'
}, $true)
$setupTargets = $setupAst.Find({ param($node)
    $node -is [System.Management.Automation.Language.SwitchStatementAst] -and $node.Condition.Extent.Text -eq '$Lane'
}, $true)
if ($null -eq $setupInstall -or $null -eq $setupTargets) { throw 'missing lane-specific Rust target selection' }
$toolchain = 'fixture-toolchain'
$script:Targets = [System.Collections.Generic.List[string]]::new()
function rustup {
    if ($args[0] -eq 'target' -and $args[1] -eq 'add') { $script:Targets.Add([string]$args[2]) }
}
foreach ($Lane in 'windows', 'windows-arm64', 'windows-rust', 'windows-fork-join', 'windows-napi', 'windows-clippy') {
    $script:Targets.Clear()
    Invoke-Expression $setupInstall.Extent.Text
    Invoke-Expression $setupTargets.Extent.Text
    & ([scriptblock]::Create($install))
    $requiredTargets = switch ($Lane) {
        'windows' { 'wasm32-unknown-unknown' }
        'windows-arm64' { 'aarch64-pc-windows-msvc' }
    }
    if (($requiredTargets -join "`n") -cne (($script:Targets | Sort-Object -Unique) -join "`n")) { throw "incorrect Rust target coverage for $Lane" }
}
Write-Host 'Windows qualification audit dispatch, failure propagation, nextest version and required targets passed.'

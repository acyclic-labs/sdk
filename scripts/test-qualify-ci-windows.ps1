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
$probe = ($settings -join "`n") + "`n& '" + $powershell.Replace("'", "''") + "' -NoProfile -NonInteractive -Command 'exit 7'`nWrite-Output AUDIT_CONTINUED"
$start = [System.Diagnostics.ProcessStartInfo]::new($powershell)
$start.UseShellExecute = $false
$start.CreateNoWindow = $true
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
foreach ($argument in '-NoProfile', '-NonInteractive', '-EncodedCommand',
    [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($probe))) { $start.ArgumentList.Add($argument) }
$process = [System.Diagnostics.Process]::Start($start)
try {
    if (-not $process.WaitForExit(10000)) { $process.Kill(); throw 'native failure probe timed out' }
    $output = $process.StandardOutput.ReadToEnd() + $process.StandardError.ReadToEnd()
    if ($process.ExitCode -eq 0 -or $output.Contains('AUDIT_CONTINUED')) { throw 'production settings ignored native failure' }
} finally { $process.Dispose() }
$CargoTargetDir = 'fixture-target'
$clangDirectory = $null
$script:Calls = [System.Collections.Generic.List[string]]::new()
$script:FailCall = 0
function Record-Command([string] $Name, [object[]] $Arguments) {
    $script:Calls.Add("$Name $($Arguments -join ' ')")
    if ($script:Calls.Count -eq $script:FailCall) { throw 'simulated command failure' }
}
function cargo { Record-Command cargo $args }
function node {
    if ($args[0] -eq 'scripts/clippy-feature-sets.mjs' -and $env:CARGO_BUILD_JOBS -ne '4') {
        throw 'clippy worker allocation changed'
    }
    Record-Command node $args
}
function rustup { Record-Command rustup $args }

$expected = @{
    'windows-rust' = @(
        'cargo nextest run --workspace --all-features --locked --profile ci --test-threads 32 --target-dir fixture-target --config profile.test.opt-level=0 --config profile.test.package."*".opt-level=1 --config profile.test.package.acyclic-fs.opt-level=1 --config profile.test.package.sha2.opt-level=3 --config profile.test.package.blake3.opt-level=3',
        'cargo test --workspace --all-features --locked --doc --target-dir fixture-target --config profile.test.opt-level=0 --config profile.test.package."*".opt-level=1 --config profile.test.package.acyclic-fs.opt-level=1 --config profile.test.package.sha2.opt-level=3 --config profile.test.package.blake3.opt-level=3'
    )
    'windows-fork-join' = @(
        'cargo test -p acyclic-plugin --all-features --locked --test fork_join --ignored --test-threads=1 --skip support::'
    )
    'windows-napi' = @('cargo build -p acyclic-fs-napi --locked --target-dir fixture-target-napi')
    'windows-arm64' = @(
        'rustup target add aarch64-pc-windows-msvc',
        'cargo check -p acyclic-fs -p acyclic-fs-napi --all-features --target aarch64-pc-windows-msvc --locked --target-dir fixture-target-aarch64'
    )
    'windows-clippy' = @(
        'cargo clippy -p acyclic-plugin --all-targets --all-features --locked -D warnings',
        'node scripts/clippy-feature-sets.mjs 4'
    )
}
$priorTarget = $env:CARGO_TARGET_DIR
$priorTestThreads = $env:RUST_TEST_THREADS
$priorBuildJobs = $env:CARGO_BUILD_JOBS
$env:RUST_TEST_THREADS = '32'
try {
    foreach ($clause in $dispatch.Clauses) {
        $name = $clause.Item1.Value
        if (-not $expected.ContainsKey($name)) { throw "unexpected audit lane: $name" }
        if ($clause.Item2.Statements[-1] -isnot [System.Management.Automation.Language.ReturnStatementAst]) { throw "audit falls through: $name" }
        $text = $clause.Item2.Extent.Text
        # Mock PowerShell functions consume the separator native Cargo receives.
        if ($name -eq 'windows-clippy' -and $text -notmatch '--locked -- -D warnings') { throw 'missing clippy warning gate' }
        $body = [scriptblock]::Create($text.Substring(1, $text.Length - 2))
        $script:Calls.Clear()
        $script:FailCall = 0
        & $body
        if (($script:Calls -join "`n") -ne ($expected[$name] -join "`n")) { throw "audit commands changed: $name observed: $($script:Calls -join ';')" }
        for ($index = 1; $index -le $expected[$name].Count; $index++) {
            $script:Calls.Clear()
            $script:FailCall = $index
            $rejected = $false
            try { & $body } catch {
                if ($_.Exception.Message -ne 'simulated command failure') { throw }
                $rejected = $true
            }
            if (-not $rejected) { throw "audit failure ignored: $name command $index" }
        }
    }
    if ($dispatch.Clauses.Count -ne $expected.Count) { throw 'missing audit lane' }
} finally {
    if ($null -eq $priorTarget) { Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue }
    else { $env:CARGO_TARGET_DIR = $priorTarget }
    if ($null -eq $priorTestThreads) { Remove-Item Env:RUST_TEST_THREADS -ErrorAction SilentlyContinue }
    else { $env:RUST_TEST_THREADS = $priorTestThreads }
    if ($null -eq $priorBuildJobs) { Remove-Item Env:CARGO_BUILD_JOBS -ErrorAction SilentlyContinue }
    else { $env:CARGO_BUILD_JOBS = $priorBuildJobs }
}
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
foreach ($Lane in 'windows', 'windows-arm64', 'windows-rust', 'windows-fork-join', 'windows-napi', 'windows-clippy') {
    $script:Calls.Clear()
    $script:FailCall = 0
    Invoke-Expression $setupInstall.Extent.Text
    Invoke-Expression $setupTargets.Extent.Text
    & ([scriptblock]::Create($install))
    $expectedSetup = @(
        'rustup toolchain install fixture-toolchain --profile minimal --no-self-update --component clippy --component rustfmt',
        'rustup default fixture-toolchain'
    )
    if ($Lane -eq 'windows') { $expectedSetup += 'rustup target add wasm32-unknown-unknown --toolchain fixture-toolchain' }
    if ($Lane -eq 'windows-arm64') { $expectedSetup += 'rustup target add aarch64-pc-windows-msvc --toolchain fixture-toolchain' }
    if (($script:Calls -join "`n") -ne ($expectedSetup -join "`n")) { throw "incorrect Rust targets for $Lane" }
}
Write-Host 'Windows qualification audit dispatch, failure propagation, nextest version and required targets passed.'

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
function node { Record-Command node $args }
function rustup { Record-Command rustup $args }

$expected = @{
    'windows-rust' = @(
        'cargo test --workspace --all-features --locked'
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
        'node scripts/clippy-feature-sets.mjs'
    )
}
$priorTarget = $env:CARGO_TARGET_DIR
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
}
Write-Host 'Windows qualification audit dispatch and failure propagation passed.'

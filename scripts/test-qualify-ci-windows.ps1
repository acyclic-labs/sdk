$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$source = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'qualify-ci.ps1')
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseInput($source, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw ($errors.Message -join "`n") }
$dispatch = $ast.Find({ param($node) $node -is [System.Management.Automation.Language.SwitchStatementAst] }, $true)
$CargoTargetDir = 'fixture-target'
$script:Calls = [System.Collections.Generic.List[string]]::new()
$script:FailCommand = $null
function Record-Command([string] $Name, [object[]] $Arguments) {
    $script:Calls.Add("$Name $($Arguments -join ' ')")
    if ($Name -eq $script:FailCommand) { throw 'simulated command failure' }
}
function cargo { Record-Command cargo $args }
function node { Record-Command node $args }
function rustup { Record-Command rustup $args }

$expected = @{
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
        $text = $clause.Item2.Extent.Text
        # Mock PowerShell functions consume the separator native Cargo receives.
        if ($name -eq 'windows-clippy' -and $text -notmatch '--locked -- -D warnings') { throw 'missing clippy warning gate' }
        $body = [scriptblock]::Create($text.Substring(1, $text.Length - 2))
        $script:Calls.Clear()
        $script:FailCommand = $null
        & $body
        if (($script:Calls -join "`n") -ne ($expected[$name] -join "`n")) { throw "audit commands changed: $name observed: $($script:Calls -join ';')" }
        foreach ($command in @($expected[$name] | ForEach-Object { ($_ -split ' ')[0] } | Select-Object -Unique)) {
            $script:FailCommand = $command
            $rejected = $false
            try { & $body } catch {
                if ($_.Exception.Message -ne 'simulated command failure') { throw }
                $rejected = $true
            }
            if (-not $rejected) { throw "audit failure ignored: $name $command" }
        }
    }
    if ($dispatch.Clauses.Count -ne $expected.Count) { throw 'missing audit lane' }
} finally {
    if ($null -eq $priorTarget) { Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue }
    else { $env:CARGO_TARGET_DIR = $priorTarget }
}
Write-Host 'Windows qualification audit dispatch and failure propagation passed.'

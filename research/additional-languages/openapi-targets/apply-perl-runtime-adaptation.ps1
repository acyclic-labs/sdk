[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$GeneratedRoot
)

$ErrorActionPreference = 'Stop'
$cpanfile = Join-Path $GeneratedRoot 'cpanfile'
if (-not (Test-Path -LiteralPath $cpanfile)) {
    throw "Generated Perl package is missing cpanfile: $cpanfile"
}
$text = Get-Content -LiteralPath $cpanfile -Raw
$anchor = "requires 'JSON', '>= 2.00, < 2.80';"
$replacement = "requires 'JSON', '>= 2.00';"
if (-not $text.Contains($anchor)) {
    throw 'Perl JSON dependency anchor changed; refusing an unreviewed adaptation.'
}
Set-Content -LiteralPath $cpanfile -Value $text.Replace($anchor, $replacement) -NoNewline
Write-Output "Relaxed generated Perl JSON upper bound for current compatible JSON runtimes: $GeneratedRoot"

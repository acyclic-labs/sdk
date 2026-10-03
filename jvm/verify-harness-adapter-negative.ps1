param(
  [Parameter(Mandatory = $true)][string]$GeneratedRoot,
  [Parameter(Mandatory = $true)][string]$AdapterScript
)

$ErrorActionPreference = 'Stop'
$source = Join-Path $GeneratedRoot 'acyclic\harness\v2\Harness.java'
if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
  throw "Generated harness Java source is missing: $source"
}

# This is an independent fail-closed review: damage the accessor table marker
# and remove the adapted accessor, then require the maintained adapter to reject
# the source rather than silently emitting a possibly incompatible class.
$mutantRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('acyclic-harness-mutant-' + [guid]::NewGuid().ToString('N'))
$mutant = Join-Path $mutantRoot 'acyclic\harness\v2'
New-Item -ItemType Directory -Force $mutant | Out-Null
$text = [System.IO.File]::ReadAllText($source)
$text = $text.Replace('getFileDescriptor()', 'getDescriptor()')
$text = $text.Replace('FileDescriptor', 'MutatedDescriptor')
$text = $text.Replace('new java.lang.String[] { "Volume", "NormalizedPath", "ImmutableVersion", "Descriptor", "DisplayName", }', 'new java.lang.String[] { "Volume", "NormalizedPath", "ImmutableVersion", "MutatedDescriptor", "DisplayName", }')
$mutantSource = Join-Path $mutant 'Harness.java'
[System.IO.File]::WriteAllText($mutantSource, $text, [System.Text.UTF8Encoding]::new($false))
try {
  $savedErrorAction = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $AdapterScript -GeneratedRoot $mutantRoot 2>$null
  $adapterExitCode = $LASTEXITCODE
  $ErrorActionPreference = $savedErrorAction
  if ($adapterExitCode -eq 0) {
    throw 'Harness adapter accepted a mutated FieldAccessorTable marker.'
  }
  Write-Host 'Harness adapter rejected mutated accessor metadata as expected.'
}
finally {
  Remove-Item -LiteralPath $mutantRoot -Recurse -Force -ErrorAction SilentlyContinue
}

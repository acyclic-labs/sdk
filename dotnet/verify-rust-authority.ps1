param(
  [Parameter(Mandatory = $true)][string]$SchemaRoot
)

$ErrorActionPreference = 'Stop'
$manifestPath = Join-Path $SchemaRoot 'rust-authority.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
  throw "Rust authority manifest is missing: $manifestPath"
}
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ($manifest.schema -ne 'acyclic.sdk.rust-authority.v1' -or $manifest.authority -ne 'rust') {
  throw 'SchemaRoot manifest is not bound to the Rust authority.'
}
if (-not $manifest.families -or $manifest.families.Count -eq 0) {
  throw 'Rust authority manifest contains no emitted families.'
}

foreach ($family in $manifest.families) {
  foreach ($pair in @(@($family.source, $family.source_sha256), @($family.descriptor, $family.descriptor_sha256))) {
    $path = Join-Path $SchemaRoot ([string]$pair[0])
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
      throw "Rust authority family artifact is missing: $path"
    }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { $actual = [System.BitConverter]::ToString($sha.ComputeHash([System.IO.File]::ReadAllBytes($path))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
    $expected = ([string]$pair[1]).ToLowerInvariant()
    if ($actual -ne $expected) {
      throw "Rust authority hash mismatch for $path (expected $expected, got $actual)."
    }
  }
}
Write-Host ("Verified Rust authority manifest and {0} source/descriptor pairs." -f $manifest.families.Count)

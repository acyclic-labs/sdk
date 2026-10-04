[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)][string]$InputRoot,
  [Parameter(Mandatory = $true)][string]$OutputRoot
)

$ErrorActionPreference = 'Stop'
$inputRoot = [IO.Path]::GetFullPath($InputRoot)
$outputRoot = [IO.Path]::GetFullPath($OutputRoot)
$producerPath = Join-Path $inputRoot 'producer-output.json'
if (-not (Test-Path -LiteralPath $producerPath -PathType Leaf)) {
  throw "Rust producer receipt is missing: $producerPath"
}
$producer = Get-Content -LiteralPath $producerPath -Raw | ConvertFrom-Json
if ($producer.schema -ne 'acyclic.sdk.dotnet.embedded.producer-output.v2') {
  throw 'JVM staging requires the Rust-owned embedded producer receipt.'
}
if ([string]::IsNullOrWhiteSpace([string]$producer.source_revision) -or
    [string]$producer.source_revision -notmatch '^[0-9a-fA-F]{40}$') {
  throw 'Embedded producer receipt does not contain an exact source revision.'
}
$required = [ordered]@{
  'win-x64' = @{ directory = 'win-x86_64'; file = 'acyclic_sdk_embedded_prototype.dll' }
  'win-arm64' = @{ directory = 'win-aarch64'; file = 'acyclic_sdk_embedded_prototype.dll' }
  'linux-x64' = @{ directory = 'linux-x86_64-gnu'; file = 'libacyclic_sdk_embedded_prototype.so' }
  'linux-arm64' = @{ directory = 'linux-aarch64-gnu'; file = 'libacyclic_sdk_embedded_prototype.so' }
  'osx-x64' = @{ directory = 'osx-x86_64'; file = 'libacyclic_sdk_embedded_prototype.dylib' }
  'osx-arm64' = @{ directory = 'osx-aarch64'; file = 'libacyclic_sdk_embedded_prototype.dylib' }
}
$assets = @($producer.native_assets)
if ($assets.Count -lt $required.Count) {
  throw "Rust producer receipt has $($assets.Count) native assets; six JVM resources are required."
}
New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
$records = @()
foreach ($rid in $required.Keys) {
  $record = @($assets | Where-Object { $_.rid -eq $rid -and $_.file -eq $required[$rid].file })
  if ($record.Count -ne 1) { throw "Rust producer receipt has no unique $rid asset." }
  $source = Join-Path (Join-Path (Join-Path $inputRoot 'native') $rid) $required[$rid].file
  if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { throw "Native asset is missing: $source" }
  $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant()
  if ($actualHash -ne ([string]$record[0].sha256).ToLowerInvariant()) { throw "Native asset hash differs from Rust producer receipt: $rid" }
  if ((Get-Item -LiteralPath $source).Length -ne [int64]$record[0].bytes) { throw "Native asset size differs from Rust producer receipt: $rid" }
  $destinationDirectory = Join-Path $outputRoot $required[$rid].directory
  New-Item -ItemType Directory -Force -Path $destinationDirectory | Out-Null
  $destination = Join-Path $destinationDirectory $required[$rid].file
  Copy-Item -LiteralPath $source -Destination $destination -Force
  $records += [ordered]@{
    source_rid = $rid
    resource = "native/$($required[$rid].directory)/$($required[$rid].file)"
    file = $required[$rid].file
    sha256 = $actualHash
    bytes = (Get-Item -LiteralPath $destination).Length
  }
}
[ordered]@{
  schema = 'acyclic.sdk.jvm.embedded.native-manifest.v1'
  source_revision = [string]$producer.source_revision
  source_revision_kind = 'git-oid'
  source_inputs = @($producer.source_inputs)
  cargo_manifest = [string]$producer.cargo_manifest
  cargo_lock = [string]$producer.cargo_lock
  cargo_lock_sha256 = [string]$producer.cargo_lock_sha256
  resources = @($records)
} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $outputRoot 'native-manifest.json') -Encoding utf8NoBOM
Write-Output "staged Rust JVM embedded resources under $outputRoot"

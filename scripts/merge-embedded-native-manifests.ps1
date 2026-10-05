param(
  [Parameter(Mandatory = $true)][string[]]$InputRoot,
  [Parameter(Mandatory = $true)][string]$OutputRoot
)

$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
. (Join-Path $PSScriptRoot 'verify-dotnet-embedded-native-manifest.ps1')

$sourceSnapshot = Get-EmbeddedRustSourceClosure -Repository $root
$lockPath = Join-Path $root 'rust/crates/sdk-embedded-prototype/Cargo.lock'
$lockHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $lockPath).Hash.ToLowerInvariant()
$cargoCommand = 'cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --target <rust_target> --target-dir <target_dir>'
$expectedRids = @('win-x64', 'win-arm64', 'linux-x64', 'linux-arm64', 'linux-musl-x64', 'linux-musl-arm64', 'osx-x64', 'osx-arm64')
$recordsByRid = @{}
$manifests = @()

function Assert-EmbeddedMergeTreeSafe {
  param([Parameter(Mandatory = $true)][string]$Path)
  $resolved = [IO.Path]::GetFullPath($Path)
  Assert-EmbeddedNoReparsePath -Path $resolved
  if (Test-Path -LiteralPath $resolved -PathType Container) {
    foreach ($item in @(Get-ChildItem -LiteralPath $resolved -Recurse -Force -ErrorAction Stop)) {
      if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Native artifact tree contains a reparse point: $($item.FullName)"
      }
    }
  }
}

function Assert-EmbeddedMergeLocation {
  param([Parameter(Mandatory = $true)][string]$Path)
  $resolved = [IO.Path]::GetFullPath($Path)
  function Get-EmbeddedPathPrefix {
    param([Parameter(Mandatory = $true)][string]$Value)
    $full = [IO.Path]::GetFullPath($Value)
    $trimmed = $full.TrimEnd([char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar))
    if ([string]::IsNullOrEmpty($trimmed)) { return [string][IO.Path]::DirectorySeparatorChar }
    return $trimmed + [IO.Path]::DirectorySeparatorChar
  }
  $repoPrefix = Get-EmbeddedPathPrefix -Value $root
  $workspacePrefixes = @(
    ([IO.Path]::GetFullPath([IO.Path]::GetTempPath()))
  )
  if (-not [String]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) {
    $workspacePrefixes += [IO.Path]::GetFullPath($env:RUNNER_TEMP)
  }
  $workspacePrefixes = @($workspacePrefixes | ForEach-Object {
    $workspace = [IO.Path]::GetFullPath($_)
    if (-not (Test-Path -LiteralPath $workspace -PathType Container)) {
      throw "Native merge workspace does not exist: $workspace"
    }
    Assert-EmbeddedNoReparsePath -Path $workspace
    Get-EmbeddedPathPrefix -Value $workspace
  })
  if (-not ($resolved.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase) -or
      @($workspacePrefixes | Where-Object { $resolved.StartsWith($_, [StringComparison]::OrdinalIgnoreCase) }).Count -gt 0)) {
    throw "Native merge path is outside the repository or temporary workspace: $resolved"
  }
  Assert-EmbeddedMergeTreeSafe -Path $resolved
  return $resolved
}

foreach ($input in $InputRoot) {
  $inputPath = Assert-EmbeddedMergeLocation -Path $input
  if (-not (Test-Path -LiteralPath $inputPath -PathType Container)) { throw "Native artifact root is missing: $inputPath" }
  $candidates = @()
  $direct = Join-Path $inputPath 'native-manifest.json'
  if (Test-Path -LiteralPath $direct -PathType Leaf) { $candidates += $direct }
  $nested = Join-Path $inputPath 'native/native-manifest.json'
  if (Test-Path -LiteralPath $nested -PathType Leaf) { $candidates += $nested }
  if ($candidates.Count -eq 0) {
    $candidates = @(Get-ChildItem -LiteralPath $inputPath -Recurse -File -Filter 'native-manifest.json' | Select-Object -ExpandProperty FullName)
  }
  if ($candidates.Count -ne 1) { throw "Expected exactly one native-manifest.json under $inputPath, found $($candidates.Count)" }
  $manifestPath = [IO.Path]::GetFullPath($candidates[0])
  $nativeRoot = Split-Path -Parent $manifestPath
  $verified = Get-VerifiedEmbeddedNativeManifest -Repository $root -NativeRoot $nativeRoot -ManifestPath $manifestPath -AllowPartial
  if ($verified.SourceRevision -ne $sourceSnapshot.Revision -or $verified.SourceInputsSha256 -ne $sourceSnapshot.Digest) {
    throw "Native artifact source closure does not match the checked-out Rust source: $inputPath"
  }
  $manifests += $verified.Manifest
  foreach ($record in @($verified.Manifest.assets)) {
    $rid = [string]$record.rid
    if ($recordsByRid.ContainsKey($rid)) { throw "Duplicate native RID across downloaded artifacts: $rid" }
    $recordsByRid[$rid] = [pscustomobject]@{ Manifest = $verified.Manifest; Record = $record; Root = $nativeRoot }
  }
}

if ($recordsByRid.Count -ne $expectedRids.Count) { throw "Native artifact merge requires exactly eight RIDs, found $($recordsByRid.Count)" }
foreach ($rid in $expectedRids) {
  if (-not $recordsByRid.ContainsKey($rid)) { throw "Native artifact merge is missing RID: $rid" }
}

$out = Assert-EmbeddedMergeLocation -Path $OutputRoot
New-Item -ItemType Directory -Force -Path $out | Out-Null
Assert-EmbeddedMergeTreeSafe -Path $out
$records = @()
foreach ($rid in $expectedRids) {
  $entry = $recordsByRid[$rid]
  $record = $entry.Record
  $source = Join-Path (Join-Path $entry.Root $rid) ([string]$record.file)
  $source = [IO.Path]::GetFullPath($source)
  $sourceRoot = [IO.Path]::GetFullPath($entry.Root)
  $sourceTrimmed = $sourceRoot.TrimEnd([char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar))
  $sourcePrefix = if ([string]::IsNullOrEmpty($sourceTrimmed)) {
    [string][IO.Path]::DirectorySeparatorChar
  } else {
    $sourceTrimmed + [IO.Path]::DirectorySeparatorChar
  }
  if (-not $source.StartsWith($sourcePrefix, [StringComparison]::OrdinalIgnoreCase) -or
      -not (Test-Path -LiteralPath $source -PathType Leaf)) {
    throw "Native artifact source is outside its verified root or missing: $source"
  }
  Assert-EmbeddedNoReparsePath -Path $source
  $sourceInfo = Get-Item -LiteralPath $source -Force
  $sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant()
  if ($sourceHash -ne ([string]$record.sha256).ToLowerInvariant() -or
      [int64]$sourceInfo.Length -ne [int64]$record.bytes) {
    throw "Native artifact changed after manifest verification: $source"
  }
  $destinationDir = Join-Path $out $rid
  New-Item -ItemType Directory -Force -Path $destinationDir | Out-Null
  Assert-EmbeddedMergeTreeSafe -Path $destinationDir
  $destination = Join-Path $destinationDir ([string]$record.file)
  if (Test-Path -LiteralPath $destination) { Assert-EmbeddedNoReparsePath -Path $destination }
  Copy-Item -LiteralPath $source -Destination $destination -Force
  Assert-EmbeddedNoReparsePath -Path $destination
  $copiedInfo = Get-Item -LiteralPath $destination -Force
  $copiedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
  if ($copiedHash -ne ([string]$record.sha256).ToLowerInvariant() -or
      [int64]$copiedInfo.Length -ne [int64]$record.bytes) {
    throw "Copied native artifact does not match verified provenance: $destination"
  }
  $sourceHashAfterCopy = (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant()
  if ($sourceHashAfterCopy -ne $sourceHash) { throw "Native artifact changed during copy: $source" }
  $records += [pscustomobject]@{
    rust_target = [string]$record.rust_target
    rid = $rid
    file = [string]$record.file
    sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
    bytes = (Get-Item -LiteralPath $destination).Length
  }
}

Assert-EmbeddedRustSourceSnapshot -Repository $root -Snapshot $sourceSnapshot
$manifest = [ordered]@{
  schema = 'acyclic.sdk.dotnet.embedded.native-manifest.v1'
  source_revision = $sourceSnapshot.Revision
  source_revision_kind = 'git-oid'
  source_inputs = @($sourceSnapshot.Paths)
  cargo_manifest = 'rust/crates/sdk-embedded-prototype/Cargo.toml'
  cargo_lock = 'rust/crates/sdk-embedded-prototype/Cargo.lock'
  cargo_lock_sha256 = $lockHash
  source_inputs_sha256 = $sourceSnapshot.Digest
  cargo_command = $cargoCommand
  assets = @($records)
}
$manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $out 'native-manifest.json') -Encoding utf8NoBOM
Get-VerifiedEmbeddedNativeManifest -Repository $root -NativeRoot $out -ManifestPath (Join-Path $out 'native-manifest.json') | Out-Null
Write-Output "merged native artifacts under $out"

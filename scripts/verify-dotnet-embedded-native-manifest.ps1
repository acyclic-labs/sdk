function Get-EmbeddedRustSourceClosure {
  param([Parameter(Mandatory = $true)][string]$Repository)

  $repo = [IO.Path]::GetFullPath($Repository)
  $embeddedManifest = Join-Path $repo 'rust/crates/sdk-embedded-prototype/Cargo.toml'
  $metadataErrorPath = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-cargo-metadata-' + [Guid]::NewGuid().ToString('N') + '.err')
  $metadataError = ''
  Push-Location -LiteralPath $repo
  try {
    $metadataLines = @(& cargo metadata --format-version 1 --locked --manifest-path $embeddedManifest 2> $metadataErrorPath)
    $cargoExitCode = $LASTEXITCODE
    if (Test-Path -LiteralPath $metadataErrorPath) {
      $metadataErrorContent = Get-Content -LiteralPath $metadataErrorPath -Raw -ErrorAction SilentlyContinue
      if ($null -ne $metadataErrorContent) { $metadataError = $metadataErrorContent.Trim() }
    }
  } finally {
    Pop-Location
    Remove-Item -LiteralPath $metadataErrorPath -Force -ErrorAction SilentlyContinue
  }
  if ($cargoExitCode -ne 0 -or $metadataLines.Count -eq 0) {
    $detail = if ([string]::IsNullOrWhiteSpace($metadataError)) { 'no cargo diagnostic' } else { $metadataError }
    throw "Unable to resolve the Rust embedded Cargo source closure: $detail"
  }
  $metadata = ($metadataLines -join "`n") | ConvertFrom-Json
  $packageById = @{}
  foreach ($package in @($metadata.packages)) { $packageById[[string]$package.id] = $package }
  $rootId = [string]$metadata.resolve.root
  if ([string]::IsNullOrWhiteSpace($rootId) -or -not $packageById.ContainsKey($rootId)) {
    throw 'Rust embedded Cargo dependency graph has no reachable root package'
  }
  $reachable = New-Object System.Collections.Generic.HashSet[string]
  $queue = New-Object System.Collections.Generic.Queue[string]
  [void]$queue.Enqueue($rootId)
  while ($queue.Count -gt 0) {
    $packageId = $queue.Dequeue()
    if (-not $reachable.Add($packageId)) { continue }
    $node = @($metadata.resolve.nodes | Where-Object { [string]$_.id -eq $packageId }) | Select-Object -First 1
    foreach ($dependency in @($node.dependencies)) {
      $dependencyId = if ($dependency -is [string]) { [string]$dependency } else { [string]$dependency.pkg }
      if ($packageById.ContainsKey($dependencyId)) { [void]$queue.Enqueue($dependencyId) }
    }
  }
  $packages = @($reachable | ForEach-Object { $packageById[$_] } | Where-Object { [string]::IsNullOrWhiteSpace([string]$_.source) })
  if ($packages.Count -eq 0) { throw 'Rust embedded Cargo source closure is empty' }

  $paths = New-Object System.Collections.Generic.List[string]
  foreach ($package in $packages) {
    $manifestPath = [IO.Path]::GetFullPath([string]$package.manifest_path)
    $packageRoot = Split-Path -Parent $manifestPath
    if (-not (Test-EmbeddedPathWithin -Base $repo -Child $manifestPath)) {
      throw "Rust embedded dependency escapes the repository: $manifestPath"
    }
    foreach ($file in Get-ChildItem -LiteralPath $packageRoot -Recurse -Force -File -ErrorAction Stop) {
      $relative = Get-EmbeddedRepoRelativePath -Repository $repo -Path $file.FullName
      $parts = $relative -split '/'
      if (($parts | Where-Object { $_ -in @('.git', 'target', 'node_modules') }).Count -gt 0) { continue }
      [void]$paths.Add($relative)
    }
  }

  foreach ($rootInput in @('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'rust-toolchain', '.cargo/config.toml', '.cargo/config')) {
    $rootPath = Join-Path $repo $rootInput
    if (Test-Path -LiteralPath $rootPath -PathType Leaf) { [void]$paths.Add($rootInput.Replace('\', '/')) }
  }

  $ordered = [string[]]$paths
  [Array]::Sort($ordered, [StringComparer]::Ordinal)
  $ordered = @($ordered | Select-Object -Unique)
  $lines = foreach ($relative in $ordered) {
    $path = Join-Path $repo $relative
    "$relative`t$((Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant())"
  }
  $bytes = [Text.Encoding]::UTF8.GetBytes((($lines -join "`n") + "`n"))
  $digest = [Convert]::ToHexString(([Security.Cryptography.SHA256]::Create().ComputeHash($bytes))).ToLowerInvariant()
  [pscustomobject]@{ Paths = $ordered; Digest = $digest; Revision = (& git -C $repo rev-parse HEAD).Trim() }
}

function Test-EmbeddedPathWithin {
  param(
    [Parameter(Mandatory = $true)][string]$Base,
    [Parameter(Mandatory = $true)][string]$Child
  )
  $basePath = [IO.Path]::GetFullPath($Base)
  $childPath = [IO.Path]::GetFullPath($Child)
  $relative = [IO.Path]::GetRelativePath($basePath, $childPath)
  return $relative -eq '.' -or ($relative -ne '..' -and -not $relative.StartsWith('..' + [IO.Path]::DirectorySeparatorChar) -and -not $relative.StartsWith('..' + [IO.Path]::AltDirectorySeparatorChar) -and -not [IO.Path]::IsPathRooted($relative))
}

function Get-EmbeddedRepoRelativePath {
  param(
    [Parameter(Mandatory = $true)][string]$Repository,
    [Parameter(Mandatory = $true)][string]$Path
  )
  if (-not (Test-EmbeddedPathWithin -Base $Repository -Child $Path)) {
    throw "Path escapes the Rust repository: $Path"
  }
  $relative = [IO.Path]::GetRelativePath([IO.Path]::GetFullPath($Repository), [IO.Path]::GetFullPath($Path))
  return $relative.Replace('\', '/')
}

function Assert-EmbeddedNoReparsePath {
  param([Parameter(Mandatory = $true)][string]$Path)
  $current = [IO.Path]::GetFullPath($Path)
  while (-not [string]::IsNullOrWhiteSpace($current)) {
    if (Test-Path -LiteralPath $current) {
      $item = Get-Item -LiteralPath $current -Force
      if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Embedded native path contains a reparse point: $current"
      }
    }
    $parent = Split-Path -Parent $current
    if ($parent -eq $current) { break }
    $current = $parent
  }
}

function Assert-EmbeddedRustSourceSnapshot {
  param(
    [Parameter(Mandatory = $true)][string]$Repository,
    [Parameter(Mandatory = $true)]$Snapshot
  )
  $current = Get-EmbeddedRustSourceClosure -Repository $Repository
  if ($current.Revision -ne $Snapshot.Revision -or
      $current.Digest -ne $Snapshot.Digest -or
      (@($current.Paths) -join "`n") -ne (@($Snapshot.Paths) -join "`n")) {
    throw 'Rust embedded source closure changed during native packaging'
  }
}

function Get-VerifiedEmbeddedNativeManifest {
  param(
    [Parameter(Mandatory = $true)][string]$Repository,
    [Parameter(Mandatory = $true)][string]$NativeRoot,
    [Parameter(Mandatory = $true)][string]$ManifestPath,
    [switch]$AllowPartial
  )

  $repo = [IO.Path]::GetFullPath($Repository)
  $native = [IO.Path]::GetFullPath($NativeRoot)
  $manifestFile = [IO.Path]::GetFullPath($ManifestPath)
  Assert-EmbeddedNoReparsePath -Path $native
  if (-not (Test-Path -LiteralPath $manifestFile -PathType Leaf)) {
    throw "Embedded native provenance manifest is required: $manifestFile"
  }

  $manifest = Get-Content -LiteralPath $manifestFile -Raw | ConvertFrom-Json
  $sourceRevision = (& git -C $repo rev-parse HEAD).Trim()
  $lockPath = Join-Path $repo 'rust/crates/sdk-embedded-prototype/Cargo.lock'
  $lockHash = (Get-FileHash -LiteralPath $lockPath -Algorithm SHA256).Hash.ToLowerInvariant()
  $closure = Get-EmbeddedRustSourceClosure -Repository $repo

  if ($manifest.schema -ne 'acyclic.sdk.dotnet.embedded.native-manifest.v1') { throw 'Unsupported embedded native manifest schema' }
  if ($manifest.source_revision -ne $sourceRevision) { throw 'Embedded native source revision does not match the checked-out Rust source' }
  if ($manifest.source_revision_kind -ne 'git-oid') { throw 'Embedded native source revision kind is invalid' }
  if ($manifest.cargo_manifest -ne 'rust/crates/sdk-embedded-prototype/Cargo.toml' -or $manifest.cargo_lock -ne 'rust/crates/sdk-embedded-prototype/Cargo.lock') { throw 'Embedded native Cargo identity is invalid' }
  if ($manifest.cargo_lock_sha256.ToLowerInvariant() -ne $lockHash) { throw 'Embedded native Cargo.lock hash does not match the checked-out Rust source' }
  if ($manifest.cargo_command -ne 'cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --target <rust_target> --target-dir <target_dir>') { throw 'Embedded native Cargo command identity is invalid' }
  if ((@($manifest.source_inputs) -join "`n") -ne (@($closure.Paths) -join "`n")) { throw 'Embedded native source input closure does not match the checked-out Rust source' }
  if ($manifest.source_inputs_sha256.ToLowerInvariant() -ne $closure.Digest) { throw 'Embedded native source input closure digest does not match the checked-out Rust source' }

  $expected = @(
    @{ Rid='win-x64'; File='acyclic_sdk_embedded_prototype.dll'; Target='x86_64-pc-windows-msvc' },
    @{ Rid='win-arm64'; File='acyclic_sdk_embedded_prototype.dll'; Target='aarch64-pc-windows-msvc' },
    @{ Rid='linux-x64'; File='libacyclic_sdk_embedded_prototype.so'; Target='x86_64-unknown-linux-gnu' },
    @{ Rid='linux-arm64'; File='libacyclic_sdk_embedded_prototype.so'; Target='aarch64-unknown-linux-gnu' },
    @{ Rid='linux-musl-x64'; File='libacyclic_sdk_embedded_prototype.so'; Target='x86_64-unknown-linux-musl' },
    @{ Rid='linux-musl-arm64'; File='libacyclic_sdk_embedded_prototype.so'; Target='aarch64-unknown-linux-musl' },
    @{ Rid='osx-x64'; File='libacyclic_sdk_embedded_prototype.dylib'; Target='x86_64-apple-darwin' },
    @{ Rid='osx-arm64'; File='libacyclic_sdk_embedded_prototype.dylib'; Target='aarch64-apple-darwin' }
  )
  $records = @($manifest.assets)
  if ($records.Count -lt 1 -or $records.Count -gt 8) { throw "Embedded native provenance requires between 1 and 8 records, found $($records.Count)" }
  if (-not $AllowPartial -and $records.Count -ne 8) { throw "Embedded native provenance requires exactly 8 records, found $($records.Count)" }
  $verified = @()
  foreach ($spec in $expected) {
    $matches = @($records | Where-Object { $_.rid -eq $spec.Rid })
    if ($matches.Count -eq 0 -and $AllowPartial) { continue }
    if ($matches.Count -ne 1) { throw "Embedded native provenance requires exactly one record for $($spec.Rid)" }
    $record = $matches[0]
    if ($record.rust_target -ne $spec.Target -or $record.file -ne $spec.File) { throw "Embedded native identity does not match $($spec.Rid)" }
    $relative = [string]$record.file
    if ([IO.Path]::IsPathRooted($relative) -or $relative.Contains('..')) { throw "Embedded native asset path is unsafe for $($spec.Rid)" }
    $asset = [IO.Path]::GetFullPath((Join-Path (Join-Path $native $spec.Rid) $relative))
    Assert-EmbeddedNoReparsePath -Path $asset
    if (-not (Test-EmbeddedPathWithin -Base $native -Child $asset) -or -not (Test-Path -LiteralPath $asset -PathType Leaf)) { throw "Embedded native asset is missing for $($spec.Rid)" }
    $info = Get-Item -LiteralPath $asset
    $hash = (Get-FileHash -LiteralPath $asset -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($hash -ne ([string]$record.sha256).ToLowerInvariant()) { throw "Embedded native asset hash differs from provenance for $($spec.Rid)" }
    if ([long]$info.Length -ne [long]$record.bytes) { throw "Embedded native asset size differs from provenance for $($spec.Rid)" }
    $verified += [pscustomobject]@{ Rid=$spec.Rid; File=$relative; Path=$asset; Sha256=$hash; Bytes=[long]$info.Length }
  }
  [pscustomobject]@{ Manifest=$manifest; Assets=$verified; SourceRevision=$sourceRevision; CargoLockSha256=$lockHash; SourceInputsSha256=$closure.Digest; SourceSnapshot=$closure }
}

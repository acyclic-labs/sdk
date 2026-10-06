param(
  [string]$Output = "target/dotnet-embedded",
  [string]$Target = "",
  [switch]$All,
  [string]$NativeRoot = "",
  [switch]$NativeOnly,
  [switch]$PackageOnly,
  # Stage the Rust-owned Swift and C++ embedded source package beside the
  # verified ABI header and native library. This mode consumes an existing
  # native producer root; it never rebuilds or accepts the historical staged
  # JVM/.NET evidence tree.
  [switch]$SourceOnly,
  [string]$AbiHeader = ""
)

$ErrorActionPreference = "Stop"
if ($SourceOnly -and $NativeOnly) { throw '-SourceOnly and -NativeOnly cannot be combined' }
if ($SourceOnly) { $PackageOnly = $true }
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
. (Join-Path $PSScriptRoot "verify-dotnet-embedded-native-manifest.ps1")
$sourceClosure = Get-EmbeddedRustSourceClosure -Repository $root
$sourceRevision = $sourceClosure.Revision
if ($sourceRevision -notmatch '^[0-9a-fA-F]{40}$') { throw "The embedded package requires an exact Git source revision" }
$sourceInputs = @($sourceClosure.Paths)
$sourceInputsSha256 = $sourceClosure.Digest
$lockfilePath = Join-Path $root 'rust/crates/sdk-embedded-prototype/Cargo.lock'
$lockfileSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $lockfilePath).Hash.ToLowerInvariant()
$cargoCommand = 'cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --target <rust_target> --target-dir <target_dir>'
$out = if ([System.IO.Path]::IsPathRooted($Output)) { [System.IO.Path]::GetFullPath($Output) } else { [System.IO.Path]::GetFullPath((Join-Path $root $Output)) }
$authorityRoot = Join-Path $out "sdk-contract"
$authorityExporter = Join-Path $PSScriptRoot "export-rust-contract-authority.ps1"
$authorityManifestPath = Join-Path $authorityRoot "rust-authority.json"
$authorityManifestSha256 = $null
$targetMap = [ordered]@{
  "x86_64-pc-windows-msvc" = @{ Rid = "win-x64"; File = "acyclic_sdk_embedded_prototype.dll"; ImportFile = "acyclic_sdk_embedded_prototype.dll.lib" }
  "aarch64-pc-windows-msvc" = @{ Rid = "win-arm64"; File = "acyclic_sdk_embedded_prototype.dll"; ImportFile = "acyclic_sdk_embedded_prototype.dll.lib" }
  "x86_64-unknown-linux-gnu" = @{ Rid = "linux-x64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "aarch64-unknown-linux-gnu" = @{ Rid = "linux-arm64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "x86_64-unknown-linux-musl" = @{ Rid = "linux-musl-x64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "aarch64-unknown-linux-musl" = @{ Rid = "linux-musl-arm64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "x86_64-apple-darwin" = @{ Rid = "osx-x64"; File = "libacyclic_sdk_embedded_prototype.dylib" }
  "aarch64-apple-darwin" = @{ Rid = "osx-arm64"; File = "libacyclic_sdk_embedded_prototype.dylib" }
}

$nativeRoot = if ([string]::IsNullOrWhiteSpace($NativeRoot)) {
  Join-Path $out "native"
} elseif ([System.IO.Path]::IsPathRooted($NativeRoot)) {
  [System.IO.Path]::GetFullPath($NativeRoot)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $root $NativeRoot))
}
New-Item -ItemType Directory -Force -Path $nativeRoot | Out-Null

if ($PackageOnly -and $SourceOnly -and -not [string]::IsNullOrWhiteSpace($Target)) {
  $targets = @($Target)
} elseif ($PackageOnly) {
  $targets = @($targetMap.Keys)
} elseif ([string]::IsNullOrWhiteSpace($Target) -and -not $All) {
  $Target = (& rustc -vV | Select-String '^host: ' | ForEach-Object { $_.Line.Substring(6).Trim() })
  $targets = @($Target)
} elseif ($All) {
  $targets = @($targetMap.Keys)
} else {
  $targets = @($Target)
}
if ($targets.Count -eq 0) { throw "No Rust target was selected" }

# Matrix jobs may stage one RID at a time. Reuse only a partial manifest tied
# to this exact Rust closure; final package validation still requires all RIDs.
$existingManifest = $null
if (-not $PackageOnly -and -not $All) {
  $existingManifestPath = Join-Path $nativeRoot 'native-manifest.json'
  if (Test-Path -LiteralPath $existingManifestPath -PathType Leaf) {
    $existingManifest = Get-VerifiedEmbeddedNativeManifest -Repository $root -NativeRoot $nativeRoot -ManifestPath $existingManifestPath -AllowPartial
  }
}

# A release workflow may stage one RID per matrix job into a shared native root.
# Reuse only a provenance manifest that is already tied to this exact Rust
# closure; the final package path still requires all eight records.
$existingManifest = $null
if (-not $PackageOnly -and -not $All) {
  $existingManifestPath = Join-Path $nativeRoot 'native-manifest.json'
  if (Test-Path -LiteralPath $existingManifestPath -PathType Leaf) {
    $existingManifest = Get-VerifiedEmbeddedNativeManifest `
      -Repository $root `
      -NativeRoot $nativeRoot `
      -ManifestPath $existingManifestPath `
      -AllowPartial
  }
}

if ($PackageOnly) {
  $providedManifestPath = Join-Path $nativeRoot 'native-manifest.json'
  if (-not (Test-Path -LiteralPath $providedManifestPath -PathType Leaf)) {
    throw "PackageOnly requires the producer native manifest at $providedManifestPath"
  }
  $providedManifest = Get-Content -LiteralPath $providedManifestPath -Raw | ConvertFrom-Json
  if ($providedManifest.schema -ne 'acyclic.sdk.dotnet.embedded.native-manifest.v1' -or
      $providedManifest.source_revision -ne $sourceRevision -or
      $providedManifest.cargo_lock_sha256 -ne $lockfileSha256 -or
      $providedManifest.cargo_command -ne $cargoCommand -or
      $providedManifest.source_inputs_sha256 -ne $sourceInputsSha256 -or
      (@($providedManifest.source_inputs) -join '|') -ne (@($sourceInputs) -join '|')) {
    throw 'PackageOnly native provenance does not match the checked-out Rust source closure.'
  }
  $providedRecords = @($providedManifest.assets)
  if ($SourceOnly) {
    if ($providedRecords.Count -lt $targets.Count) {
      throw "SourceOnly expected at least $($targets.Count) native provenance records, found $($providedRecords.Count)"
    }
  } elseif ($providedRecords.Count -ne $targets.Count) {
    throw "PackageOnly expected $($targets.Count) native provenance records, found $($providedRecords.Count)"
  }
  foreach ($targetName in $targets) {
    $spec = $targetMap[$targetName]
    $installed = Join-Path (Join-Path $nativeRoot $spec.Rid) $spec.File
    $record = @($providedRecords | Where-Object {
      $_.rust_target -eq $targetName -and $_.rid -eq $spec.Rid -and $_.file -eq $spec.File
    })
    if ($record.Count -ne 1) { throw "PackageOnly has no unique provenance record for $targetName" }
    if (-not (Test-Path -LiteralPath $installed -PathType Leaf)) {
      throw "PackageOnly requires the native asset for $targetName at $installed"
    }
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $installed).Hash.ToLowerInvariant()
    if ($actualHash -ne ([string]$record[0].sha256).ToLowerInvariant()) {
      throw "PackageOnly native asset hash differs from producer provenance for $targetName"
    }
    if ((Get-Item -LiteralPath $installed).Length -ne [int64]$record[0].bytes) {
      throw "PackageOnly native asset size differs from producer provenance for $targetName"
    }
  }
}

function Invoke-EmbeddedRustBuild {
  param(
    [Parameter(Mandatory = $true)][string]$TargetName,
    [Parameter(Mandatory = $true)][string]$Manifest,
    [Parameter(Mandatory = $true)][string]$TargetDirectory
  )

  $linkerVariable = "CARGO_TARGET_$($TargetName.ToUpperInvariant().Replace('-', '_'))_LINKER"
  $savedRustFlags = $env:RUSTFLAGS
  $savedLinker = [Environment]::GetEnvironmentVariable($linkerVariable, "Process")
  $changedMuslEnvironment = $false
  $savedLibraryPath = $env:LIBRARY_PATH
  $changedLibraryEnvironment = $false
  try {
    if ($TargetName -like "*-unknown-linux-musl") {
      $compilerNames = if ($TargetName.StartsWith("aarch64-")) {
        @("aarch64-linux-musl-gcc", "musl-gcc")
      } else {
        @("x86_64-linux-musl-gcc", "musl-gcc")
      }
      $compiler = $null
      foreach ($name in $compilerNames) {
        $candidate = Get-Command $name -ErrorAction SilentlyContinue
        if ($candidate) { $compiler = $candidate.Source; break }
      }
      if (-not $compiler) {
        throw "No pinned/native musl linker is available for $TargetName. Use the pinned Alpine producer lane."
      }
      # The Debian musl wrappers do not always propagate the GCC support
      # directory to the final Rust link. Resolve it from the selected
      # compiler and pass it explicitly; otherwise ld reports that it cannot
      # find libgcc_s.so.1 even though the compiler package is installed.
      $gccSupport = (& $compiler -print-file-name=libgcc_s.so.1 2>$null | Select-Object -First 1).Trim()
      if (-not [string]::IsNullOrWhiteSpace($gccSupport) -and (Test-Path -LiteralPath $gccSupport -PathType Leaf)) {
        $gccDirectory = Split-Path -Parent $gccSupport
      } else {
        $searchDirectories = (& $compiler -print-search-dirs 2>$null |
          Select-String '^libraries: =' | ForEach-Object { $_.Line.Substring(11).Split([IO.Path]::PathSeparator) })
        $gccDirectory = $searchDirectories |
          Where-Object { Test-Path -LiteralPath (Join-Path $_ 'libgcc_s.so.1') -PathType Leaf } |
          Select-Object -First 1
      }
      if ([string]::IsNullOrWhiteSpace($gccDirectory)) {
        throw "The musl linker '$compiler' has no discoverable libgcc_s.so.1 directory for $TargetName."
      }
      # Keep musl libc dynamic while making compiler support resolution
      # deterministic on both x64 and arm64 runners.
      $env:RUSTFLAGS = (($savedRustFlags + " -C target-feature=-crt-static -C link-arg=-L$gccDirectory -C link-arg=-static-libgcc").Trim())
      [Environment]::SetEnvironmentVariable($linkerVariable, $compiler, "Process")
      $changedMuslEnvironment = $true
    }
    & cargo build --locked --release --manifest-path $Manifest --target $TargetName --target-dir $TargetDirectory
    if ($LASTEXITCODE -ne 0) { throw "Rust embedded ABI build failed for $TargetName : $LASTEXITCODE" }
  } finally {
    if ($changedMuslEnvironment) {
      $env:RUSTFLAGS = $savedRustFlags
      [Environment]::SetEnvironmentVariable($linkerVariable, $savedLinker, "Process")
      if ($changedLibraryEnvironment) { $env:LIBRARY_PATH = $savedLibraryPath }
    }
  }
}

$manifest = Join-Path $root "rust/crates/sdk-embedded-prototype/Cargo.toml"
$targetDir = Join-Path $out "rust-target"
$records = @()

foreach ($targetName in $targets) {
  if (-not $targetMap.Contains($targetName)) {
    throw "Unsupported embedded .NET target '$targetName'. Supported targets: $($targetMap.Keys -join ', ')"
  }
  $spec = $targetMap[$targetName]
  $installed = Join-Path (Join-Path $nativeRoot $spec.Rid) $spec.File
  if ($PackageOnly) {
    if (-not (Test-Path -LiteralPath $installed -PathType Leaf)) {
      throw "PackageOnly requires the native asset for $targetName at $installed"
    }
  } else {
    Invoke-EmbeddedRustBuild -TargetName $targetName -Manifest $manifest -TargetDirectory $targetDir
    $binary = Join-Path $targetDir "$targetName/release/$($spec.File)"
    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
      throw "Rust embedded ABI did not emit $binary"
    }
    $destination = Join-Path $nativeRoot $spec.Rid
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    $installed = Join-Path $destination $spec.File
    Copy-Item -LiteralPath $binary -Destination $installed -Force
    # Keep the ABI header in the same producer root as the native library so
    # downstream source-only staging cannot accidentally consume the older
    # staged-jvm-dotnet-evidence header. Every target build must emit the same
    # cbindgen bytes; differing copies are a hard provenance failure.
    $headerCandidates = @(Get-ChildItem -LiteralPath (Join-Path $targetDir "$targetName/release/build") -Recurse -File -Filter 'acyclic_embedded_prototype.h' -ErrorAction SilentlyContinue)
    if ($headerCandidates.Count -ne 1) {
      throw "Rust embedded ABI did not emit exactly one cbindgen header for $targetName"
    }
    $headerDestination = Join-Path $nativeRoot 'abi/acyclic_embedded_prototype.h'
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $headerDestination) | Out-Null
    $headerHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $headerCandidates[0].FullName).Hash.ToLowerInvariant()
    if (Test-Path -LiteralPath $headerDestination -PathType Leaf) {
      $existingHeaderHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $headerDestination).Hash.ToLowerInvariant()
      if ($existingHeaderHash -ne $headerHash) {
        throw "Rust embedded cbindgen headers differ across producer targets"
      }
    } else {
      Copy-Item -LiteralPath $headerCandidates[0].FullName -Destination $headerDestination -Force
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$spec.ImportFile)) {
      $importSource = Join-Path $targetDir "$targetName/release/$($spec.ImportFile)"
      if (-not (Test-Path -LiteralPath $importSource -PathType Leaf)) {
        throw "Rust embedded ABI did not emit $importSource"
      }
      Copy-Item -LiteralPath $importSource -Destination (Join-Path $destination $spec.ImportFile) -Force
    }
  }
  $records += [PSCustomObject]@{
    rust_target = $targetName
    rid = $spec.Rid
    file = $spec.File
    sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $installed).Hash.ToLowerInvariant()
    bytes = (Get-Item -LiteralPath $installed).Length
  }
}

if ($existingManifest) {
  $merged = @($existingManifest.Manifest.assets) + @($records)
  $records = @(
    foreach ($targetName in $targetMap.Keys) {
      $matches = @($merged | Where-Object { $_.rust_target -eq $targetName })
      if ($matches.Count -gt 1) { $matches[-1] }
      elseif ($matches.Count -eq 1) { $matches[0] }
    }
  )
}

if ($existingManifest) {
  $merged = @($existingManifest.Manifest.assets) + @($records)
  $records = @(
    foreach ($targetName in $targetMap.Keys) {
      $matches = @($merged | Where-Object { $_.rust_target -eq $targetName })
      if ($matches.Count -gt 1) { $matches[-1] }
      elseif ($matches.Count -eq 1) { $matches[0] }
    }
  )
}

Assert-EmbeddedRustSourceSnapshot -Repository $root -Snapshot $sourceClosure

$nativeManifest = [ordered]@{
  schema = "acyclic.sdk.dotnet.embedded.native-manifest.v1"
  source_revision = $sourceRevision
  source_revision_kind = "git-oid"
  source_inputs = @($sourceInputs)
  cargo_manifest = "rust/crates/sdk-embedded-prototype/Cargo.toml"
  cargo_lock = "rust/crates/sdk-embedded-prototype/Cargo.lock"
  cargo_lock_sha256 = $lockfileSha256
  source_inputs_sha256 = $sourceInputsSha256
  cargo_command = $cargoCommand
  assets = @($records)
}
$producerHeaderPath = Join-Path $nativeRoot 'abi/acyclic_embedded_prototype.h'
if (Test-Path -LiteralPath $producerHeaderPath -PathType Leaf) {
  $nativeManifest.abi_header = [ordered]@{
    path = 'abi/acyclic_embedded_prototype.h'
    sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $producerHeaderPath).Hash.ToLowerInvariant()
    bytes = [int64](Get-Item -LiteralPath $producerHeaderPath).Length
  }
}
$nativeManifestPath = Join-Path $nativeRoot "native-manifest.json"
$nativeManifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $nativeManifestPath -Encoding utf8NoBOM
$nativeManifestSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $nativeManifestPath).Hash.ToLowerInvariant()
if ($SourceOnly) {
  # Source-only packages are assembled from the same producer output as the
  # native package. Require the new wireCall ABI marker so a stale header from
  # staged-jvm-dotnet-evidence cannot be silently paired with this library.
  $headerPath = if ([string]::IsNullOrWhiteSpace($AbiHeader)) {
    Join-Path $nativeRoot 'abi/acyclic_embedded_prototype.h'
  } else {
    [IO.Path]::GetFullPath($AbiHeader)
  }
  if (-not (Test-Path -LiteralPath $headerPath -PathType Leaf)) {
    throw "Source-only embedded package requires the matching cbindgen header: $headerPath"
  }
  Assert-EmbeddedNoReparsePath -Path $headerPath
  $headerText = Get-Content -LiteralPath $headerPath -Raw
  foreach ($symbol in @('acyclic_embedded_abi_version', 'acyclic_embedded_engine_wire_call', 'acyclic_wire_result_release')) {
    if ($headerText -notmatch [regex]::Escape($symbol)) {
      throw "Source-only cbindgen header is stale or incomplete; missing ABI symbol $symbol"
    }
  }
  $headerHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $headerPath).Hash.ToLowerInvariant()
  $headerBytes = (Get-Item -LiteralPath $headerPath).Length
  if (Test-Path -LiteralPath $producerHeaderPath -PathType Leaf) {
    $producerHeaderHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $producerHeaderPath).Hash.ToLowerInvariant()
    if ($producerHeaderHash -ne $headerHash) {
      throw 'Source-only cbindgen header differs from the native producer header'
    }
  }

  if (-not (Test-Path -LiteralPath $authorityExporter -PathType Leaf)) {
    throw "Rust authority exporter is missing: $authorityExporter"
  }
  # The authority exporter intentionally accepts output only below this
  # checkout's target directory. Keep its temporary producer output there,
  # then copy the exact manifest into the source-only package tree.
  $sourceAuthorityRoot = Join-Path $root 'target/embedded-source-only-authority'
  $sourceAuthorityManifestPath = Join-Path $sourceAuthorityRoot 'rust-authority.json'
  & $authorityExporter -Root $root -Output $sourceAuthorityRoot | Out-Host
  if ($LASTEXITCODE -ne 0) { throw "Rust contract authority export failed with exit code $LASTEXITCODE" }
  if (-not (Test-Path -LiteralPath $sourceAuthorityManifestPath -PathType Leaf)) {
    throw "Rust contract authority export did not produce $sourceAuthorityManifestPath"
  }
  $authority = Get-Content -LiteralPath $sourceAuthorityManifestPath -Raw | ConvertFrom-Json
  $authorityGitSha = [string]$authority.source_git_sha
  $modelDigest = [string]$authority.source_revision
  if ($authorityGitSha -ne $sourceRevision) {
    throw "Rust authority source Git SHA differs from native producer: $authorityGitSha"
  }
  if ($modelDigest -notmatch '^[0-9a-fA-F]{64}$') {
    throw 'Rust authority must carry a 64-character model digest for source-only staging'
  }
  $authorityManifestSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $sourceAuthorityManifestPath).Hash.ToLowerInvariant()

  $productRoot = Join-Path $out 'generated-products'
  New-Item -ItemType Directory -Force -Path $productRoot | Out-Null
  $contractManifest = Join-Path $root 'rust/crates/sdk-contract-wire/Cargo.toml'
  & cargo run --locked --manifest-path $contractManifest --bin sdk-contract-wire -- `
    generate-products --root $root --out $productRoot --profile all-languages
  if ($LASTEXITCODE -ne 0) { throw 'Rust embedded source product generation failed' }
  Assert-EmbeddedRustSourceSnapshot -Repository $root -Snapshot $sourceClosure
  $sourcePackageRoot = Join-Path $out 'source-only'
  New-Item -ItemType Directory -Force -Path $sourcePackageRoot | Out-Null
  $authorityDestination = Join-Path $sourcePackageRoot 'sdk-contract/rust-authority.json'
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $authorityDestination) | Out-Null
  Copy-Item -LiteralPath $sourceAuthorityManifestPath -Destination $authorityDestination -Force
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $authorityDestination).Hash.ToLowerInvariant() -ne $authorityManifestSha256) {
    throw 'Copied Rust authority manifest differs from the generated authority manifest'
  }
  foreach ($relative in @(
      'swift/embedded/Package.swift',
      'swift/embedded/Sources/AcyclicEmbedded/AcyclicEmbedded.swift',
      'swift/embedded/Sources/AcyclicEmbeddedABI/module.modulemap',
      'cpp/embedded-consumer/include/acyclic/embedded.hpp')) {
    $source = Join-Path $productRoot $relative
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
      throw "Rust embedded source product is missing: $relative"
    }
    $destination = Join-Path $sourcePackageRoot $relative
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destination) | Out-Null
    Copy-Item -LiteralPath $source -Destination $destination -Force
  }
  $abiDestination = Join-Path $sourcePackageRoot 'abi/acyclic_embedded_prototype.h'
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $abiDestination) | Out-Null
  Copy-Item -LiteralPath $headerPath -Destination $abiDestination -Force
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $abiDestination).Hash.ToLowerInvariant() -ne $headerHash) {
    throw 'Copied cbindgen header differs from the producer header'
  }
  $headerCopies = @(
    $abiDestination,
    (Join-Path $sourcePackageRoot 'swift/embedded/Sources/AcyclicEmbeddedABI/acyclic_embedded_prototype.h'),
    (Join-Path $sourcePackageRoot 'cpp/embedded-consumer/include/acyclic_embedded_prototype.h')
  )
  foreach ($headerCopy in $headerCopies | Select-Object -Unique) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $headerCopy) | Out-Null
    if ($headerCopy -ne $abiDestination) {
      Copy-Item -LiteralPath $headerPath -Destination $headerCopy -Force
    }
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $headerCopy).Hash.ToLowerInvariant() -ne $headerHash) {
      throw "Copied cbindgen header differs at $headerCopy"
    }
  }
  $nativeManifestDestination = Join-Path $sourcePackageRoot 'native/native-manifest.json'
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $nativeManifestDestination) | Out-Null
  Copy-Item -LiteralPath $nativeManifestPath -Destination $nativeManifestDestination -Force
  if ((Get-FileHash -Algorithm SHA256 -LiteralPath $nativeManifestDestination).Hash.ToLowerInvariant() -ne $nativeManifestSha256) {
    throw 'Copied native manifest differs from the producer manifest'
  }

  $stagedAssets = @()
  $stagedImports = @()
  foreach ($targetName in $targets) {
    $spec = $targetMap[$targetName]
    $source = Join-Path (Join-Path $nativeRoot $spec.Rid) $spec.File
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
      throw "Source-only embedded package requires native asset $source"
    }
    Assert-EmbeddedNoReparsePath -Path $source
    $destination = Join-Path (Join-Path $sourcePackageRoot 'native') (Join-Path $spec.Rid $spec.File)
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destination) | Out-Null
    Copy-Item -LiteralPath $source -Destination $destination -Force
    $record = @($records | Where-Object { $_.rust_target -eq $targetName -and $_.rid -eq $spec.Rid })
    if ($record.Count -ne 1) { throw "Source-only native provenance is missing $targetName" }
    $stagedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
    if ($stagedHash -ne ([string]$record[0].sha256).ToLowerInvariant() -or
        (Get-Item -LiteralPath $destination).Length -ne [int64]$record[0].bytes) {
      throw "Copied source-only native asset differs from producer provenance for $targetName"
    }
    $stagedAssets += [ordered]@{
      rust_target = $targetName
      rid = $spec.Rid
      path = "native/$($spec.Rid)/$($spec.File)"
      sha256 = ([string]$record[0].sha256).ToLowerInvariant()
      bytes = [int64]$record[0].bytes
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$spec.ImportFile)) {
      $importSource = Join-Path (Join-Path $nativeRoot $spec.Rid) $spec.ImportFile
      if (-not (Test-Path -LiteralPath $importSource -PathType Leaf)) {
        throw "Source-only embedded package requires Windows import library $importSource"
      }
      Assert-EmbeddedNoReparsePath -Path $importSource
      $importDestination = Join-Path (Join-Path $sourcePackageRoot 'native') (Join-Path $spec.Rid $spec.ImportFile)
      Copy-Item -LiteralPath $importSource -Destination $importDestination -Force
      $importHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $importDestination).Hash.ToLowerInvariant()
      $importBytes = [int64](Get-Item -LiteralPath $importDestination).Length
      $stagedImports += [ordered]@{
        rust_target = $targetName
        rid = $spec.Rid
        path = "native/$($spec.Rid)/$($spec.ImportFile)"
        sha256 = $importHash
        bytes = $importBytes
      }
    }
  }
  $generatedRows = foreach ($relative in @(
      'swift/embedded/Package.swift',
      'swift/embedded/Sources/AcyclicEmbedded/AcyclicEmbedded.swift',
      'swift/embedded/Sources/AcyclicEmbeddedABI/module.modulemap',
      'cpp/embedded-consumer/include/acyclic/embedded.hpp')) {
    $path = Join-Path $sourcePackageRoot $relative
    [ordered]@{ path = $relative; sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant(); bytes = [int64](Get-Item -LiteralPath $path).Length }
  }
  $receipt = [ordered]@{
    schema = 'acyclic.sdk.embedded.source-only-package-receipt.v1'
    package_kind = 'source-only'
    source_git_sha = $sourceRevision
    source_revision = $sourceRevision
    source_revision_kind = 'git-oid'
    rust_model_digest = $modelDigest.ToLowerInvariant()
    source_model_revision = $modelDigest.ToLowerInvariant()
    source_digest = $modelDigest.ToLowerInvariant()
    model_hash = $modelDigest.ToLowerInvariant()
    model_hash_kind = 'rust-model-sha256'
    source_inputs = @($sourceInputs)
    source_inputs_sha256 = $sourceInputsSha256
    cargo_manifest = 'rust/crates/sdk-embedded-prototype/Cargo.toml'
    cargo_lock = 'rust/crates/sdk-embedded-prototype/Cargo.lock'
    cargo_lock_sha256 = $lockfileSha256
    rust_authority = 'sdk-contract/rust-authority.json'
    rust_authority_sha256 = $authorityManifestSha256
    native_manifest = 'native/native-manifest.json'
    native_manifest_sha256 = $nativeManifestSha256
    cbindgen = '0.29.4'
    abi_header = [ordered]@{ path = 'abi/acyclic_embedded_prototype.h'; sha256 = $headerHash; bytes = [int64]$headerBytes; staged_paths = @('abi/acyclic_embedded_prototype.h', 'swift/embedded/Sources/AcyclicEmbeddedABI/acyclic_embedded_prototype.h', 'cpp/embedded-consumer/include/acyclic_embedded_prototype.h'); required_symbols = @('acyclic_embedded_abi_version', 'acyclic_embedded_engine_wire_call', 'acyclic_wire_result_release') }
    generated_sources = @($generatedRows)
    native_assets = @($stagedAssets)
    native_import_libraries = @($stagedImports)
    registry_published = $false
  }
  $receiptPath = Join-Path $sourcePackageRoot 'source-only-receipt.json'
  $receipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
  Write-Output "staged source-only Swift/C++ embedded package under $sourcePackageRoot"
  exit 0
}
if ($NativeOnly) {
  Assert-EmbeddedRustSourceSnapshot -Repository $root -Snapshot $sourceClosure
  Write-Output "staged native embedded assets under $nativeRoot"
  exit 0
}

if (-not (Test-Path -LiteralPath $authorityExporter -PathType Leaf)) {
  throw "Rust authority exporter is missing: $authorityExporter"
}
& $authorityExporter -Root $root -Output $authorityRoot | Out-Host
if ($LASTEXITCODE -ne 0) { throw "Rust contract authority export failed with exit code $LASTEXITCODE" }
if (-not (Test-Path -LiteralPath $authorityManifestPath -PathType Leaf)) {
  throw "Rust contract authority export did not produce $authorityManifestPath"
}
$authorityManifestSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $authorityManifestPath).Hash.ToLowerInvariant()
Assert-EmbeddedRustSourceSnapshot -Repository $root -Snapshot $sourceClosure

$dotnetVersion = '8.0.425'
$dotnetCandidates = @()
$dotnetCommand = Get-Command dotnet -ErrorAction SilentlyContinue
if ($dotnetCommand) { $dotnetCandidates += $dotnetCommand.Source }
$dotnetCandidates += @(
  (Join-Path $root ".toolchains/dotnet/$dotnetVersion/dotnet.exe"),
  (Join-Path $root ".tmp-dotnet-sdk-official-$dotnetVersion/dotnet.exe"),
  "Q:\sdk\.tmp-dotnet-sdk-official-$dotnetVersion\dotnet.exe",
  (Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "acyclic/tool-cache/dotnet-sdk-$dotnetVersion-win-x64/dotnet.exe")
)
$dotnetPath = $null
foreach ($candidate in ($dotnetCandidates | Select-Object -Unique)) {
  if (-not $candidate -or -not (Test-Path -LiteralPath $candidate -PathType Leaf)) { continue }
  $actualVersion = (& $candidate --version 2>$null | Select-Object -First 1).Trim()
  if ($actualVersion -eq $dotnetVersion) {
    $dotnetPath = $candidate
    break
  }
}
if (-not $dotnetPath) { throw "Pinned .NET SDK $dotnetVersion is unavailable" }

$packageOutput = Join-Path $out "packages"
$obj = Join-Path $out "obj"
$build = Join-Path $out "build"
New-Item -ItemType Directory -Force -Path $packageOutput, $obj, $build | Out-Null
$project = Join-Path $root "dotnet/Acyclic.Sdk.Embedded.csproj"
& $dotnetPath pack $project --configuration Release --nologo `
  "-p:EmbeddedNativeRoot=$nativeRoot" `
  "-p:SchemaRoot=$authorityRoot" `
  "-p:BaseOutputPath=$build\" `
  "-p:BaseIntermediateOutputPath=$obj\" `
  "-p:PackageOutputPath=$packageOutput\" `
  '-p:ContinuousIntegrationBuild=true' `
  '-p:Deterministic=true' `
  '-p:DeterministicSourcePaths=true'
if ($LASTEXITCODE -ne 0) { throw "Embedded .NET package failed with exit code $LASTEXITCODE" }
Assert-EmbeddedRustSourceSnapshot -Repository $root -Snapshot $sourceClosure

$package = Join-Path $packageOutput "Acyclic.Sdk.Embedded.0.2.0-alpha.1.nupkg"
if (-not (Test-Path -LiteralPath $package -PathType Leaf)) { throw "Embedded package was not produced: $package" }
$manifestOutput = [ordered]@{
  schema = "acyclic.sdk.dotnet.embedded.producer-output.v2"
  source = "rust/crates/sdk-embedded-prototype"
  source_revision = $sourceRevision
  source_revision_kind = "git-oid"
  source_inputs = @($sourceInputs)
  cargo_manifest = "rust/crates/sdk-embedded-prototype/Cargo.toml"
  cargo_lock = "rust/crates/sdk-embedded-prototype/Cargo.lock"
  cargo_lock_sha256 = $lockfileSha256
  source_inputs_sha256 = $sourceInputsSha256
  cargo_command = $cargoCommand
  package = "Acyclic.Sdk.Embedded.0.2.0-alpha.1.nupkg"
  package_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $package).Hash.ToLowerInvariant()
  native_manifest = "native/native-manifest.json"
  native_manifest_sha256 = $nativeManifestSha256
  contract_authority = "sdk-contract"
  contract_authority_manifest_sha256 = $authorityManifestSha256
  native_assets = @($records)
  dotnet_sdk = $dotnetVersion
  targets = @($targets)
  native_root = "native"
  loader = "P/Invoke acyclic_sdk_embedded_prototype; NuGet RID asset selection"
  operations = @("append", "read", "follow", "cancel", "owned-buffers", "provider-recovery")
  registry_published = $false
}
$manifestOutput | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $out "producer-output.json") -Encoding utf8NoBOM
Write-Output "staged embedded .NET package: $package"

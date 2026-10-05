param(
  [string]$Output = "target/dotnet-embedded",
  [string]$Target = "",
  [switch]$All,
  [string]$NativeRoot = "",
  [switch]$NativeOnly,
  [switch]$PackageOnly
)

$ErrorActionPreference = "Stop"
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
  "x86_64-pc-windows-msvc" = @{ Rid = "win-x64"; File = "acyclic_sdk_embedded_prototype.dll" }
  "aarch64-pc-windows-msvc" = @{ Rid = "win-arm64"; File = "acyclic_sdk_embedded_prototype.dll" }
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

if ($PackageOnly) {
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
  if ($providedRecords.Count -ne $targets.Count) {
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
      # Keep musl libc dynamic while linking compiler support statically. The
      # Ubuntu musl wrapper can expose libgcc_s only through its private specs,
      # while -static-libgcc is stable across x64 and arm64 runners.
      $env:RUSTFLAGS = (($savedRustFlags + " -C target-feature=-crt-static -C link-arg=-static-libgcc").Trim())
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
$nativeManifestPath = Join-Path $nativeRoot "native-manifest.json"
$nativeManifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $nativeManifestPath -Encoding utf8NoBOM
$nativeManifestSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $nativeManifestPath).Hash.ToLowerInvariant()
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

param(
  [string]$Root = "",
  [string]$AuthorityOutput = "target/jvm-embedded/sdk-contract",
  [string]$NativeRoot = "target/dotnet-embedded/native",
  [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$repo = if ([string]::IsNullOrWhiteSpace($Root)) {
  [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
} elseif ([System.IO.Path]::IsPathRooted($Root)) {
  [System.IO.Path]::GetFullPath($Root)
} else {
  [System.IO.Path]::GetFullPath((Join-Path (Join-Path $PSScriptRoot "..") $Root))
}
. (Join-Path $PSScriptRoot "verify-dotnet-embedded-native-manifest.ps1")
$nativeRoot = if ([System.IO.Path]::IsPathRooted($NativeRoot)) {
  [System.IO.Path]::GetFullPath($NativeRoot)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo $NativeRoot))
}
if (-not (Test-Path -LiteralPath $nativeRoot -PathType Container)) {
  throw "Rust native asset root is missing: $nativeRoot"
}
$verifiedNative = Get-VerifiedEmbeddedNativeManifest `
  -Repository $repo `
  -NativeRoot $nativeRoot `
  -ManifestPath (Join-Path $nativeRoot "native-manifest.json")
$sourceSnapshot = $verifiedNative.SourceSnapshot
$authorityExporter = Join-Path $PSScriptRoot "export-rust-contract-authority.ps1"
if (-not (Test-Path -LiteralPath $authorityExporter -PathType Leaf)) {
  throw "Rust authority exporter is missing: $authorityExporter"
}
$authority = & $authorityExporter -Root $repo -Output $AuthorityOutput | Select-Object -Last 1
if ($LASTEXITCODE -ne 0) { throw "Rust contract authority export failed with exit code $LASTEXITCODE" }
$authority = [System.IO.Path]::GetFullPath($authority)
$authorityManifest = Join-Path $authority "rust-authority.json"
if (-not (Test-Path -LiteralPath $authorityManifest -PathType Leaf)) {
  throw "Rust contract authority export did not produce $authorityManifest"
}
Assert-EmbeddedRustSourceSnapshot -Repository $repo -Snapshot $sourceSnapshot

$nativeResources = Join-Path (Split-Path $authority -Parent) "jvm-native-resources-$PID"
New-Item -ItemType Directory -Force -Path $nativeResources | Out-Null
$runRoot = Join-Path (Split-Path $authority -Parent) ("jvm-maven-run-" + [guid]::NewGuid().ToString('N'))
$mavenRepository = Join-Path $runRoot "repository"
$mavenTarget = Join-Path $runRoot "target"
$mavenProtoc = Join-Path $runRoot "protoc-dependencies"
New-Item -ItemType Directory -Force -Path $runRoot,$mavenRepository,$mavenTarget,$mavenProtoc | Out-Null
$settings = Join-Path $runRoot "settings.xml"
$escapedRepository = [System.Security.SecurityElement]::Escape($mavenRepository)
$settingsXml = '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"><localRepository>' + $escapedRepository + '</localRepository></settings>'
[IO.File]::WriteAllText($settings, $settingsXml, [Text.UTF8Encoding]::new($false))
$nativeMap = [ordered]@{
  "win-x64" = "win-x86_64"
  "win-arm64" = "win-aarch64"
  "linux-x64" = "linux-x86_64-gnu"
  "linux-arm64" = "linux-aarch64-gnu"
  "linux-musl-x64" = "linux-x86_64-musl"
  "linux-musl-arm64" = "linux-aarch64-musl"
  "osx-x64" = "osx-x86_64"
  "osx-arm64" = "osx-aarch64"
}
$nativeManifestSource = Join-Path $nativeRoot "native-manifest.json"
Copy-Item -LiteralPath $nativeManifestSource -Destination (Join-Path $nativeResources "native-manifest.json") -Force
$nativeRecords = @()
foreach ($entry in $nativeMap.GetEnumerator()) {
  $rid = $entry.Key
  $javaRid = $entry.Value
  $record = @($verifiedNative.Assets | Where-Object { $_.Rid -eq $rid })
  if ($record.Count -ne 1) { throw "Verified native provenance is missing $rid" }
  $file = $record[0].File
  $source = $record[0].Path
  Assert-EmbeddedNoReparsePath -Path $source
  $sourceInfo = Get-Item -LiteralPath $source -Force
  $sourceHashBefore = (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant()
  if ($sourceHashBefore -ne $record[0].Sha256 -or [int64]$sourceInfo.Length -ne [int64]$record[0].Bytes) {
    throw "Verified native asset changed before JVM staging: $source"
  }
  $destination = Join-Path (Join-Path $nativeResources $javaRid) $file
  New-Item -ItemType Directory -Force -Path (Split-Path $destination -Parent) | Out-Null
  Copy-Item -LiteralPath $source -Destination $destination -Force
  Assert-EmbeddedNoReparsePath -Path $destination
  $destinationInfo = Get-Item -LiteralPath $destination -Force
  $destinationHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
  if ($destinationHash -ne $record[0].Sha256 -or [int64]$destinationInfo.Length -ne [int64]$record[0].Bytes) {
    throw "Staged JVM native asset does not match verified provenance: $destination"
  }
  $sourceHashAfter = (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash.ToLowerInvariant()
  if ($sourceHashAfter -ne $sourceHashBefore) { throw "Verified native asset changed during JVM staging: $source" }
  $nativeRecords += [ordered]@{ rid = $javaRid; source_rid = $rid; file = $file; sha256 = $record[0].Sha256; bytes = $record[0].Bytes }
}
Assert-EmbeddedRustSourceSnapshot -Repository $repo -Snapshot $sourceSnapshot
$maven = Get-Command mvn -ErrorAction SilentlyContinue
if (-not $maven) { throw "Maven is required to package the embedded JVM SDK" }
$pom = Join-Path $repo "jvm/embedded/pom.xml"
if (-not (Test-Path -LiteralPath $pom -PathType Leaf)) { throw "Embedded JVM POM is missing: $pom" }
$args = @(
  "--batch-mode", "--no-transfer-progress", "-f", $pom,
  "-Dsdk.contract.root=$authority",
  "-Dembedded.native.root=$nativeResources",
  "-Dsdk.package.build.directory=$mavenTarget",
  "-Dprotobuf.temporaryProtoFileDirectory=$mavenProtoc",
  "-Dmaven.repo.local=$mavenRepository",
  "-s", $settings
)
if ($SkipTests) { $args += "-DskipTests" }
$args += "package"
Assert-EmbeddedRustSourceSnapshot -Repository $repo -Snapshot $sourceSnapshot
& $maven.Source @args
if ($LASTEXITCODE -ne 0) { throw "Embedded JVM package failed with exit code $LASTEXITCODE" }
Assert-EmbeddedRustSourceSnapshot -Repository $repo -Snapshot $sourceSnapshot

$sourceRevision = (& git -C $repo rev-parse HEAD).Trim()
$package = Join-Path $mavenTarget "acyclic-embedded-jna-0.1.0.jar"
if (-not (Test-Path -LiteralPath $package -PathType Leaf)) { throw "Maven did not produce the embedded JVM package: $package" }
$record = [ordered]@{
  schema = "acyclic.sdk.jvm.embedded.producer-output.v1"
  source_revision = $sourceRevision
  source_inputs_sha256 = $sourceSnapshot.Digest
  authority = "rust"
  authority_root = $authority
  authority_manifest_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $authorityManifest).Hash.ToLowerInvariant()
  native_assets = $nativeRecords
  package = if (Test-Path -LiteralPath $package -PathType Leaf) { $package } else { $null }
  package_sha256 = if (Test-Path -LiteralPath $package -PathType Leaf) { (Get-FileHash -Algorithm SHA256 -LiteralPath $package).Hash.ToLowerInvariant() } else { $null }
  registry_published = $false
}
$record | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path (Split-Path $authority -Parent) "jvm-producer-output.json") -Encoding utf8NoBOM
Write-Output "staged embedded JVM package from Rust authority: $package"

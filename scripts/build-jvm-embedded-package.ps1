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

$nativeRoot = if ([System.IO.Path]::IsPathRooted($NativeRoot)) {
  [System.IO.Path]::GetFullPath($NativeRoot)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo $NativeRoot))
}
if (-not (Test-Path -LiteralPath $nativeRoot -PathType Container)) {
  throw "Rust native asset root is missing: $nativeRoot"
}
$nativeResources = Join-Path (Split-Path $authority -Parent) "jvm-native-resources-$PID"
New-Item -ItemType Directory -Force -Path $nativeResources | Out-Null
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
if (Test-Path -LiteralPath $nativeManifestSource -PathType Leaf) {
  Copy-Item -LiteralPath $nativeManifestSource -Destination (Join-Path $nativeResources "native-manifest.json") -Force
}
$nativeRecords = @()
foreach ($entry in $nativeMap.GetEnumerator()) {
  $rid = $entry.Key
  $javaRid = $entry.Value
  $file = if ($rid.StartsWith("win-")) { "acyclic_sdk_embedded_prototype.dll" } elseif ($rid.StartsWith("osx-")) { "libacyclic_sdk_embedded_prototype.dylib" } else { "libacyclic_sdk_embedded_prototype.so" }
  $source = Join-Path (Join-Path $nativeRoot $rid) $file
  if (-not (Test-Path -LiteralPath $source -PathType Leaf)) { throw "Rust native asset is missing: $source" }
  $destination = Join-Path (Join-Path $nativeResources $javaRid) $file
  New-Item -ItemType Directory -Force -Path (Split-Path $destination -Parent) | Out-Null
  Copy-Item -LiteralPath $source -Destination $destination -Force
  $nativeRecords += [ordered]@{ rid = $javaRid; source_rid = $rid; file = $file; sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant() }
}
$maven = Get-Command mvn -ErrorAction SilentlyContinue
if (-not $maven) { throw "Maven is required to package the embedded JVM SDK" }
$pom = Join-Path $repo "jvm/embedded/pom.xml"
if (-not (Test-Path -LiteralPath $pom -PathType Leaf)) { throw "Embedded JVM POM is missing: $pom" }
$args = @(
  "--batch-mode", "--no-transfer-progress", "-f", $pom,
  "-Dsdk.contract.root=$authority",
  "-Dembedded.native.root=$nativeResources"
)
if ($SkipTests) { $args += "-DskipTests" }
$args += "package"
& $maven.Source @args
if ($LASTEXITCODE -ne 0) { throw "Embedded JVM package failed with exit code $LASTEXITCODE" }

$sourceRevision = (& git -C $repo rev-parse HEAD).Trim()
$package = Join-Path $repo "jvm/embedded/target/acyclic-embedded-jna-0.1.0.jar"
$record = [ordered]@{
  schema = "acyclic.sdk.jvm.embedded.producer-output.v1"
  source_revision = $sourceRevision
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

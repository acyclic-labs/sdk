param(
  [Parameter(Mandatory = $false)][string]$SourceRoot,
  [Parameter(Mandatory = $true)][string]$Authority,
  [Parameter(Mandatory = $true)][string]$Request,
  [Parameter(Mandatory = $true)][string]$Output
)

$ErrorActionPreference = 'Stop'
$authority = [System.IO.Path]::GetFullPath($Authority)
$request = [System.IO.Path]::GetFullPath($Request)
$output = [System.IO.Path]::GetFullPath($Output)
$root = if ([string]::IsNullOrWhiteSpace($SourceRoot)) {
  # Keep direct invocations convenient while allowing the coordinator to bind
  # the package to an explicit checkout when the script is staged elsewhere.
  [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
} else {
  [System.IO.Path]::GetFullPath($SourceRoot)
}
$sourceJvm = Join-Path $root 'jvm'
$workspace = Join-Path $output 'workspace'
$workspaceJvm = Join-Path $workspace 'jvm'
$mavenLocal = Join-Path $output '.m2'
$pom = Join-Path $workspaceJvm 'pom.xml'
if (-not (Test-Path -LiteralPath $root -PathType Container)) { throw "Rust source root is missing: $root" }
if (-not (Test-Path -LiteralPath $sourceJvm -PathType Container)) { throw "JVM package root is missing: $sourceJvm" }
foreach ($path in @($authority, $request, (Join-Path $sourceJvm 'pom.xml'))) {
  $kind = if ($path -eq $authority) { 'Container' } else { 'Leaf' }
  if (-not (Test-Path -LiteralPath $path -PathType $kind)) { throw "Required Kotlin producer input is missing: $path" }
}

$sourcePrefix = $root.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
$outputPrefix = $output.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
if ($output.Equals($root, [System.StringComparison]::OrdinalIgnoreCase) -or
    $output.StartsWith($sourcePrefix, [System.StringComparison]::OrdinalIgnoreCase) -or
    $root.StartsWith($outputPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "Kotlin producer output must be disjoint from the Rust source root: $output"
}

$manifest = Join-Path $authority 'rust-authority.json'
if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) { throw "Rust authority manifest is missing: $manifest" }
$authorityDocument = Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json
if ($authorityDocument.schema -ne 'acyclic.sdk.rust-authority.v1' -or $authorityDocument.authority -ne 'rust') {
  throw 'Rust authority manifest has an unexpected schema or authority.'
}
$modelRevision = [string]$authorityDocument.source_revision
if ($modelRevision -notmatch '^[0-9a-fA-F]{64}$') {
  throw 'Rust authority manifest source_revision must be a 64-character model digest.'
}
$sourceRevision = (& git -C $root rev-parse HEAD 2>$null).Trim()
if ($sourceRevision -notmatch '^[0-9a-fA-F]{40}$') {
  throw "Could not resolve a Git source revision for $root"
}
$requestDocument = Get-Content -LiteralPath $request -Raw | ConvertFrom-Json
$requestSource = $requestDocument.PSObject.Properties['source']
$requestRevisionProperty = if ($null -ne $requestSource) { $requestSource.Value.PSObject.Properties['revision'] } else { $null }
$requestRevision = if ($null -ne $requestRevisionProperty) { [string]$requestRevisionProperty.Value } else { '' }
if ($requestRevision -and $requestRevision -ne $sourceRevision) {
  throw "Generation request source revision $requestRevision does not match checkout $sourceRevision"
}

# Keep Maven's generated target and antrun files outside the Rust checkout.
# The adapter is deliberately a staging boundary: producer execution must not
# make the source tree dirty or turn compiler output into source authority.
$null = New-Item -ItemType Directory -Force -Path $output, $workspace, $workspaceJvm, $mavenLocal
# Use PowerShell's portable file primitives so the same recipe works under
# Windows PowerShell and PowerShell Core on Unix.  A Windows-only copy utility
# and previously made the Rust-owned recipe depend on a disposable shim.
$excludedDirectories = @('target', 'obj', 'bin')
Get-ChildItem -LiteralPath $sourceJvm -Force | ForEach-Object {
  if ($_.PSIsContainer -and $excludedDirectories -contains $_.Name) { return }
  Copy-Item -LiteralPath $_.FullName -Destination (Join-Path $workspaceJvm $_.Name) -Recurse -Force
}

$maven = Get-Command mvn -ErrorAction SilentlyContinue
if ($null -eq $maven) { throw 'Maven is required for the pinned Kotlin producer.' }

$mavenArgs = @(
  '-B', '-ntp', '-f', $pom, '-DgenerateKotlin=true', '-DskipTests', '-Dmaven.test.skip=true',
  "-Dacyclic.schema.root=$authority", "-Dmaven.repo.local=$mavenLocal"
)
$isWindowsPlatform = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
  [System.Runtime.InteropServices.OSPlatform]::Windows
)
$pluginJar = Join-Path (
  Join-Path (
    Join-Path (
      Join-Path (Join-Path $mavenLocal 'io') 'grpc'
    ) 'protoc-gen-grpc-kotlin'
  ) '1.4.3'
) 'protoc-gen-grpc-kotlin-1.4.3-jdk8.jar'
$toolDirectory = Join-Path $output '.tools'
$null = New-Item -ItemType Directory -Force -Path $toolDirectory
if ($isWindowsPlatform) {
  $pluginExecutable = Join-Path $toolDirectory 'grpc-kotlin-plugin.cmd'
  $pluginText = "@echo off`r`nif not exist `"$pluginJar`" (`r`n  echo grpc-kotlin compiler $pluginJar is missing 1>&2`r`n  exit /b 2`r`n)`r`njava -jar `"$pluginJar`" %*`r`n"
  Set-Content -LiteralPath $pluginExecutable -Value $pluginText -Encoding utf8
} else {
  $pluginExecutable = Join-Path $toolDirectory 'grpc-kotlin-plugin'
  $pluginText = "#!/usr/bin/env sh`nset -eu`nif [ ! -f '$pluginJar' ]; then`n  echo 'grpc-kotlin compiler $pluginJar is missing' >&2`n  exit 2`nfi`nexec java -jar '$pluginJar' `"`$@`"`n"
  Set-Content -LiteralPath $pluginExecutable -Value $pluginText -Encoding utf8NoBOM
  & chmod +x $pluginExecutable
  if ($LASTEXITCODE -ne 0) { throw "Could not make the grpc-kotlin wrapper executable: $pluginExecutable" }
}
$mavenArgs += "-Dgrpc.kotlin.plugin.executable=$pluginExecutable"
& $maven.Source @mavenArgs 'install'
if ($LASTEXITCODE -ne 0) { throw "Kotlin producer failed with exit code $LASTEXITCODE." }

$jar = Join-Path (Join-Path $workspaceJvm 'target') 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw "Kotlin producer did not emit $jar" }
$artifact = Join-Path $output 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
Copy-Item -LiteralPath $jar -Destination $artifact -Force
if (-not (Test-Path -LiteralPath $pluginJar -PathType Leaf)) {
  throw "Kotlin producer completed without the pinned grpc-kotlin plugin: $pluginJar"
}
@{
  schema = 'acyclic.sdk.kotlin.producer-output.v1'
  source_revision = $sourceRevision
  source_revision_kind = 'git-oid'
  rust_model_revision = $modelRevision
  rust_model_revision_kind = 'rust-model-sha256'
  authority_manifest = [System.IO.Path]::GetFileName($manifest)
  authority_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $manifest).Hash
  request = [System.IO.Path]::GetFileName($request)
  request_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $request).Hash
  package_root = 'jvm'
  maven_repository = '.m2'
  pom_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $pom).Hash
  grpc_kotlin_plugin = 'io/grpc/protoc-gen-grpc-kotlin/1.4.3/protoc-gen-grpc-kotlin-1.4.3-jdk8.jar'
  grpc_kotlin_plugin_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $pluginJar).Hash
  artifact = [System.IO.Path]::GetFileName($artifact)
  artifact_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $artifact).Hash
  generator = 'protoc + grpc-java + grpc-kotlin'
  generator_versions = @{ protobuf = '4.31.1'; grpc = '1.75.0'; grpc_kotlin = '1.4.3' }
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $output 'producer-output.json') -Encoding utf8
Write-Output "staged Kotlin package: $artifact"

param(
  [Parameter(Mandatory = $true)][string]$SourceRoot,
  [Parameter(Mandatory = $true)][string]$Authority,
  [Parameter(Mandatory = $true)][string]$Request,
  [Parameter(Mandatory = $true)][string]$Output
)

$ErrorActionPreference = 'Stop'

$SourceRoot = [System.IO.Path]::GetFullPath($SourceRoot)
$Authority = [System.IO.Path]::GetFullPath($Authority)
$Request = [System.IO.Path]::GetFullPath($Request)

if (-not (Test-Path -LiteralPath $SourceRoot -PathType Container)) {
  throw "Rust source root is missing: $SourceRoot"
}
if (-not (Test-Path -LiteralPath $Authority -PathType Container)) {
  throw "Rust authority directory is missing: $Authority"
}
if (-not (Test-Path -LiteralPath $Request -PathType Leaf)) {
  throw "Generation request is missing: $Request"
}
$manifest = Join-Path $Authority 'rust-authority.json'
if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) {
  throw "Rust authority manifest is missing: $manifest"
}

$output = [System.IO.Path]::GetFullPath($Output)
$sourcePrefix = $SourceRoot.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
$outputPrefix = $output.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
if ($output.Equals($SourceRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
    $output.StartsWith($sourcePrefix, [System.StringComparison]::OrdinalIgnoreCase) -or
    $SourceRoot.StartsWith($outputPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "JVM producer output must be disjoint from the Rust source root: $output"
}
$workspace = Join-Path $output 'workspace'
$workspaceJvm = Join-Path $workspace 'jvm'
$mavenLocal = Join-Path $output '.m2'
$null = New-Item -ItemType Directory -Force -Path $output, $workspace, $workspaceJvm, $mavenLocal

$excludedDirectories = @('target', 'obj', 'bin')
Get-ChildItem -LiteralPath (Join-Path $SourceRoot 'jvm') -Force | ForEach-Object {
  if ($_.PSIsContainer -and $excludedDirectories -contains $_.Name) { return }
  Copy-Item -LiteralPath $_.FullName -Destination (Join-Path $workspaceJvm $_.Name) -Recurse -Force
}
foreach ($name in @('LICENSE', 'NOTICE')) {
  $license = Join-Path $SourceRoot $name
  if (Test-Path -LiteralPath $license -PathType Leaf) {
    Copy-Item -LiteralPath $license -Destination (Join-Path $workspace $name) -Force
  }
}

$authorityDocument = Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json
if ($authorityDocument.schema -ne 'acyclic.sdk.rust-authority.v1' -or $authorityDocument.authority -ne 'rust') {
  throw 'Rust authority manifest has an unexpected schema or authority.'
}
$modelRevision = [string]$authorityDocument.source_revision
if ($modelRevision -notmatch '^[0-9a-fA-F]{64}$') {
  throw 'Rust authority manifest source_revision must be a 64-character model digest.'
}
$sourceRevision = (& git -C $SourceRoot rev-parse HEAD 2>$null).Trim()
if ($sourceRevision -notmatch '^[0-9a-fA-F]{40}$') {
  throw "Could not resolve a Git source revision for $SourceRoot"
}
$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
$requestSource = $requestDocument.PSObject.Properties['source']
$requestRevisionProperty = if ($null -ne $requestSource) { $requestSource.Value.PSObject.Properties['revision'] } else { $null }
$requestRevision = if ($null -ne $requestRevisionProperty) { [string]$requestRevisionProperty.Value } else { '' }
if ($requestRevision -and $requestRevision -ne $sourceRevision) {
  throw "Generation request source revision $requestRevision does not match checkout $sourceRevision"
}

$runningOnWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
  [System.Runtime.InteropServices.OSPlatform]::Windows
)
if (-not $runningOnWindows) {
  $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
  if ($null -eq $pwsh) { throw 'PowerShell Core (pwsh) is required on Unix for the JVM producer adapter.' }
  $shimDirectory = Join-Path $output '.tools'
  $shim = Join-Path $shimDirectory 'powershell.exe'
  $null = New-Item -ItemType Directory -Force -Path $shimDirectory
  $shimText = @'
#!/usr/bin/env sh
exec pwsh "$@"
'@
  Set-Content -LiteralPath $shim -Value $shimText -Encoding utf8NoBOM
  & chmod +x $shim
  if ($LASTEXITCODE -ne 0) { throw "Could not make the PowerShell Core shim executable: $shim" }
  $env:PATH = "$shimDirectory$([IO.Path]::PathSeparator)$env:PATH"
}

$maven = Get-Command mvn -ErrorAction SilentlyContinue
if ($null -eq $maven) {
  throw 'Maven is required for the pinned JVM producer adapter.'
}

$pom = Join-Path $workspaceJvm 'pom.xml'
if (-not (Test-Path -LiteralPath $pom -PathType Leaf)) {
  throw "JVM producer POM is missing: $pom"
}

  & $maven.Source '-B' '-ntp' '-f' $pom `
  "-Dacyclic.schema.root=$Authority" `
  "-Dmaven.repo.local=$mavenLocal" `
  '-Dmaven.test.skip=true' 'package'
if ($LASTEXITCODE -ne 0) {
  throw "Pinned JVM producer failed with exit code $LASTEXITCODE."
}

$jar = Join-Path (Join-Path $workspaceJvm 'target') 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) {
  throw "JVM producer completed without its installable JAR: $jar"
}
Copy-Item -LiteralPath $jar -Destination (Join-Path $output 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar') -Force
$requestHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $Request).Hash
$authorityHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $manifest).Hash
@{
  schema = 'acyclic.sdk.jvm.producer-output.v1'
  source_revision = $sourceRevision
  source_revision_kind = 'git-oid'
  rust_model_revision = $modelRevision
  rust_model_revision_kind = 'rust-model-sha256'
  authority_manifest = 'rust-authority.json'
  authority_sha256 = $authorityHash
  request = [System.IO.Path]::GetFileName($Request)
  request_sha256 = $requestHash
  generator = 'protoc + grpc-java'
  generator_versions = @{
    protobuf = '4.31.1'
    grpc = '1.75.0'
    protobuf_maven_plugin = '0.6.1'
  }
  artifact = 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
  artifact_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $output 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar')).Hash
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $output 'producer-output.json') -Encoding utf8
Write-Output "staged JVM package: acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar"

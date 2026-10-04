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

foreach ($path in @($SourceRoot, $Authority, $Request)) {
  if (-not (Test-Path -LiteralPath $path)) {
    throw "Required producer input is missing: $path"
  }
}
$manifest = Join-Path $Authority 'rust-authority.json'
if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) {
  throw "Rust authority manifest is missing: $manifest"
}

$output = [System.IO.Path]::GetFullPath($Output)
$workspace = Join-Path $output 'workspace'
$workspaceJvm = Join-Path $workspace 'jvm'
$mavenLocal = Join-Path $output '.m2'
$null = New-Item -ItemType Directory -Force -Path $output, $workspace, $mavenLocal

& robocopy (Join-Path $SourceRoot 'jvm') $workspaceJvm /E /XD target obj bin /NFL /NDL /NJH /NJS /NC /NS | Out-Null
if ($LASTEXITCODE -gt 7) {
  throw "Could not stage the JVM producer source tree (robocopy exit code $LASTEXITCODE)."
}
foreach ($name in @('LICENSE', 'NOTICE')) {
  Copy-Item -LiteralPath (Join-Path $SourceRoot $name) -Destination (Join-Path $workspace $name) -Force
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

$jar = Join-Path $workspaceJvm 'target\acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) {
  throw "JVM producer completed without its installable JAR: $jar"
}
Copy-Item -LiteralPath $jar -Destination (Join-Path $output 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar') -Force
$requestHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $Request).Hash
$authorityHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $manifest).Hash
@{
  schema = 'acyclic.sdk.jvm.producer-output.v1'
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

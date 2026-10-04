[CmdletBinding()]
param(
  [Parameter(Mandatory)][string]$SdkRoot,
  [Parameter(Mandatory)][string]$PackageRoot,
  [Parameter(Mandatory)][string]$WireRoot,
  [Parameter(Mandatory)][string]$Endpoint,
  [Parameter(Mandatory)][string]$Manifest,
  [Parameter(Mandatory)][string]$OutputRoot
)
$ErrorActionPreference = 'Stop'
$SdkRoot = (Resolve-Path -LiteralPath $SdkRoot).Path
$PackageRoot = (Resolve-Path -LiteralPath $PackageRoot).Path
$WireRoot = (Resolve-Path -LiteralPath $WireRoot).Path
$Manifest = (Resolve-Path -LiteralPath $Manifest).Path
$OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
$manifestDocument = Get-Content -Raw -LiteralPath $Manifest | ConvertFrom-Json
$sourceSha = [string]$manifestDocument.authority.source_git_sha
if ($sourceSha -notmatch '^[0-9a-f]{40}$') { throw "typed request manifest has no exact source revision: $sourceSha" }
$checkoutSha = (& git -C $SdkRoot rev-parse HEAD).Trim()
if ($checkoutSha -ne $sourceSha) { throw "typed request manifest revision $sourceSha does not match checkout $checkoutSha" }
if (-not (Test-Path -LiteralPath (Join-Path $PackageRoot 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'))) { throw 'generated JVM package is missing' }
foreach ($path in @((Join-Path $WireRoot 'rust-authority.json'), (Join-Path $SdkRoot 'research/acceptance/scala-full-typed-build.sbt'), (Join-Path $SdkRoot 'research/acceptance/scala-full-typed-consumer.scala'))) {
  if (-not (Test-Path -LiteralPath $path)) { throw "Required ScalaPB input is missing: $path" }
}
New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$env:ACYCLIC_FIXTURE_ENDPOINT = $Endpoint
$env:ACYCLIC_TYPED_REQUEST_MANIFEST = $Manifest
$env:ACYCLIC_SOURCE_GIT_REVISION = $sourceSha
$env:ACYCLIC_SCALA_OBSERVATION_FILE = Join-Path $OutputRoot 'scala-typed-observations.json'
$run = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-scala-typed-' + [Guid]::NewGuid().ToString('N'))
$scala = Join-Path $run 'scala'
New-Item -ItemType Directory -Path (Join-Path $scala 'src/main/scala/acyclic/installed') -Force | Out-Null
$build = Get-Content -Raw -LiteralPath (Join-Path $SdkRoot 'research/acceptance/scala-full-typed-build.sbt')
$wireUri = $WireRoot.Replace('\','/')
$build = $build.Replace('file("proto/', 'file("' + $wireUri + '/')
$build = $build.Replace('file("rust/', 'file("' + $SdkRoot.Replace('\','/') + '/rust/')
Set-Content -LiteralPath (Join-Path $scala 'build.sbt') -Value $build -Encoding utf8
Copy-Item -LiteralPath (Join-Path $SdkRoot 'research/acceptance/scala-full-typed-consumer.scala') -Destination (Join-Path $scala 'src/main/scala/acyclic/installed/ScalaRustTypedManifestConsumerV2.scala')
Push-Location $scala
try {
  & sbt -batch -no-colors -Dsbt.supershell=false runMain acyclic.installed.ScalaRustTypedManifestConsumerV2
  if ($LASTEXITCODE -ne 0) { throw "ScalaPB typed consumer failed with exit code $LASTEXITCODE" }
} finally { Pop-Location }

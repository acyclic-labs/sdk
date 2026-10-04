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
$packageJar = Join-Path $PackageRoot 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
$mavenRepo = Join-Path $PackageRoot '.m2'
foreach ($path in @($packageJar, $mavenRepo, (Join-Path $SdkRoot 'jvm/kotlin-consumer/pom.xml'))) {
  if (-not (Test-Path -LiteralPath $path)) { throw "Required generated JVM input is missing: $path" }
}
New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$env:ACYCLIC_FIXTURE_ENDPOINT = $Endpoint
$env:ACYCLIC_TYPED_REQUEST_MANIFEST = $Manifest
$env:ACYCLIC_SOURCE_GIT_REVISION = $sourceSha
$env:ACYCLIC_JVM_TYPED_EVIDENCE = Join-Path $OutputRoot 'kotlin-typed-live.json'
$env:ACYCLIC_JVM_TYPED_OBSERVATION_FILE = Join-Path $OutputRoot 'kotlin-typed-observations.json'
$run = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-jvm-typed-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path (Join-Path $run 'src/test/kotlin/dev/acyclic/consumer') -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $SdkRoot 'jvm/kotlin-consumer/pom.xml') -Destination (Join-Path $run 'pom.xml')
Copy-Item -LiteralPath (Join-Path $SdkRoot 'research/acceptance/kotlin-full-typed-consumer.kt') -Destination (Join-Path $run 'src/test/kotlin/dev/acyclic/consumer/KotlinRustTypedManifestConsumerV2Test.kt')
Push-Location $run
try {
  & mvn -o -B -ntp -Dmaven.repo.local=$mavenRepo -Dtest=KotlinRustTypedManifestConsumerV2Test#generatedStubsExerciseRustSchemaV2All106Methods test
  if ($LASTEXITCODE -ne 0) { throw "Kotlin typed consumer failed with exit code $LASTEXITCODE" }
} finally { Pop-Location }
if (-not (Test-Path -LiteralPath $env:ACYCLIC_JVM_TYPED_EVIDENCE)) { throw 'Kotlin consumer did not emit live evidence' }
Copy-Item -LiteralPath $env:ACYCLIC_JVM_TYPED_EVIDENCE -Destination (Join-Path $OutputRoot 'kotlin-typed-live.json') -Force

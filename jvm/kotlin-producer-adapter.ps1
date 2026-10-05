param(
  [Parameter(Mandatory = $true)][string]$Authority,
  [Parameter(Mandatory = $true)][string]$Request,
  [Parameter(Mandatory = $true)][string]$Output
)

$ErrorActionPreference = 'Stop'
$authority = [System.IO.Path]::GetFullPath($Authority)
$request = [System.IO.Path]::GetFullPath($Request)
$output = [System.IO.Path]::GetFullPath($Output)
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$pom = Join-Path $root 'jvm\pom.xml'
$plugin = Join-Path $root 'jvm\grpc-kotlin-plugin.cmd'
foreach ($path in @($authority, $request, $pom, $plugin)) {
  if (-not (Test-Path -LiteralPath $path)) { throw "Required Kotlin producer input is missing: $path" }
}
$null = New-Item -ItemType Directory -Force -Path $output
$maven = Get-Command mvn -ErrorAction SilentlyContinue
if ($null -eq $maven) { throw 'Maven is required for the pinned Kotlin producer.' }

& $maven.Source '-B' '-ntp' '-f' $pom '-DgenerateKotlin=true' '-DskipTests' `
  "-Dacyclic.schema.root=$authority" "-Dgrpc.kotlin.plugin.executable=$plugin" 'install'
if ($LASTEXITCODE -ne 0) { throw "Kotlin producer failed with exit code $LASTEXITCODE." }

$jar = Join-Path $root 'jvm\target\acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
if (-not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw "Kotlin producer did not emit $jar" }
$artifact = Join-Path $output 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
Copy-Item -LiteralPath $jar -Destination $artifact -Force
$manifest = Join-Path $authority 'rust-authority.json'
@{
  schema = 'acyclic.sdk.kotlin.producer-output.v1'
  authority_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $manifest).Hash
  artifact = [System.IO.Path]::GetFileName($artifact)
  artifact_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $artifact).Hash
  generator = 'protoc + grpc-java + grpc-kotlin'
  generator_versions = @{ protobuf = '4.31.1'; grpc = '1.75.0'; grpc_kotlin = '1.4.3' }
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $output 'producer-output.json') -Encoding utf8
Write-Output "staged Kotlin package: $artifact"

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
$sourceJvm = Join-Path $root 'jvm'
$workspace = Join-Path $output 'workspace'
$workspaceJvm = Join-Path $workspace 'jvm'
$mavenLocal = Join-Path $output '.m2'
$pom = Join-Path $workspaceJvm 'pom.xml'
$plugin = Join-Path $sourceJvm 'grpc-kotlin-plugin.cmd'
foreach ($path in @($authority, $request, (Join-Path $sourceJvm 'pom.xml'))) {
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Required Kotlin producer input is missing: $path" }
}

# Keep Maven's generated target and antrun files outside the Rust checkout.
# The adapter is deliberately a staging boundary: producer execution must not
# make the source tree dirty or turn compiler output into source authority.
$null = New-Item -ItemType Directory -Force -Path $output, $workspace, $workspaceJvm, $mavenLocal
Get-ChildItem -LiteralPath $sourceJvm -Force | Copy-Item -Destination $workspaceJvm -Recurse -Force
foreach ($directory in Get-ChildItem -LiteralPath $workspaceJvm -Directory -Recurse -Force |
    Where-Object { $_.Name -in @('target', 'obj', 'bin') } |
    Sort-Object FullName -Descending) {
  Remove-Item -LiteralPath $directory.FullName -Recurse -Force
}

$maven = Get-Command mvn -ErrorAction SilentlyContinue
if ($null -eq $maven) { throw 'Maven is required for the pinned Kotlin producer.' }

$mavenArgs = @(
  '-B', '-ntp', '-f', $pom, '-DgenerateKotlin=true', '-DskipTests',
  "-Dacyclic.schema.root=$authority", "-Dmaven.repo.local=$mavenLocal"
)
$isWindowsPlatform = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
  [System.Runtime.InteropServices.OSPlatform]::Windows
)
if ($isWindowsPlatform -and (Test-Path -LiteralPath $plugin -PathType Leaf)) {
  $mavenArgs += "-Dgrpc.kotlin.plugin.executable=$plugin"
}
& $maven.Source @mavenArgs 'install'
if ($LASTEXITCODE -ne 0) { throw "Kotlin producer failed with exit code $LASTEXITCODE." }

$jar = Join-Path (Join-Path $workspaceJvm 'target') 'acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar'
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

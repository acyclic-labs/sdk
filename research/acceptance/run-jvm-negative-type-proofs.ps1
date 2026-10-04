[CmdletBinding()]
param(
  [Parameter(Mandatory)][string]$ScratchRoot,
  [Parameter(Mandatory)][string]$JavaPackageJar,
  [Parameter(Mandatory)][string]$ProtobufJar,
  [Parameter(Mandatory)][string]$MavenRepository,
  [Parameter(Mandatory)][string]$ScalaClasses,
  [Parameter(Mandatory)][string]$ScalaCompiler,
  [Parameter(Mandatory)][string]$ScalaLibrary,
  [Parameter(Mandatory)][string]$ScalaReflect,
  [Parameter(Mandatory)][string]$ScalaLenses,
  [Parameter(Mandatory)][string]$ScalaRuntimeClasspath,
  [Parameter(Mandatory)][string]$OutputRoot
)
$ErrorActionPreference = 'Stop'
$SdkRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$ScratchRoot = [IO.Path]::GetFullPath($ScratchRoot)
$OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
New-Item -ItemType Directory -Force -Path $ScratchRoot,$OutputRoot | Out-Null

function Require-File([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
    throw "Required generated input is missing: $Path"
  }
}
function Require-Path([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Required generated input is missing: $Path"
  }
}
foreach ($path in @($JavaPackageJar,$ProtobufJar,$ScalaCompiler,$ScalaLibrary,$ScalaReflect,$ScalaLenses)) {
  Require-File $path
}
Require-Path $ScalaClasses
Require-File (Join-Path $MavenRepository 'dev/acyclic/acyclic-sdk-jvm-transport/0.2.0-SNAPSHOT/acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar')

function Run-ExpectedCompileFailure([string]$Name, [string]$WorkingDirectory, [string[]]$Arguments, [string]$LogPath, [string[]]$RequiredDiagnostics) {
  & $Name @Arguments 2> $LogPath
  $exitCode = $LASTEXITCODE
  if ($exitCode -eq 0) { throw "$Name unexpectedly accepted an invalid generated type probe" }
  $diagnostics = Get-Content -Raw -LiteralPath $LogPath
  foreach ($needle in $RequiredDiagnostics) {
    if ($diagnostics -notmatch [regex]::Escape($needle)) {
      throw "$Name rejected the probe without the expected diagnostic '$needle'"
    }
  }
  return $exitCode
}

$javaRoot = Join-Path $ScratchRoot 'java'
$javaClasses = Join-Path $javaRoot 'classes'
$javaLog = Join-Path $OutputRoot 'java.stderr'
New-Item -ItemType Directory -Force -Path $javaClasses | Out-Null
$javaSource = Join-Path $SdkRoot 'research/acceptance/jvm-strong-typing/negative-java/InvalidJavaRustTypes.java'
$javaClasspath = "$JavaPackageJar;$ProtobufJar"
$javaExit = Run-ExpectedCompileFailure 'javac' $javaRoot @('-cp',$javaClasspath,'-d',$javaClasses,$javaSource) $javaLog @('ByteString','setOrigin')

$kotlinRoot = Join-Path $ScratchRoot 'kotlin'
$kotlinSourceRoot = Join-Path $kotlinRoot 'src/test/kotlin/dev/acyclic/negative'
New-Item -ItemType Directory -Force -Path $kotlinSourceRoot | Out-Null
Copy-Item (Join-Path $SdkRoot 'jvm/kotlin-consumer/pom.xml') (Join-Path $kotlinRoot 'pom.xml')
Copy-Item (Join-Path $SdkRoot 'research/acceptance/jvm-strong-typing/negative-kotlin/InvalidKotlinRustTypes.kt') (Join-Path $kotlinSourceRoot 'InvalidKotlinRustTypes.kt')
$kotlinTarget = Join-Path $kotlinRoot 'target'
$kotlinLog = Join-Path $OutputRoot 'kotlin.log'
Push-Location $kotlinRoot
try {
  & mvn -B -ntp "-Dmaven.repo.local=$MavenRepository" "-Dproject.build.directory=$kotlinTarget" -DskipTests test *> $kotlinLog
  $kotlinExit = $LASTEXITCODE
  if ($kotlinExit -eq 0) { throw 'mvn unexpectedly accepted an invalid Kotlin generated type probe' }
} finally { Pop-Location }
$kotlinDiagnostics = Get-Content -Raw -LiteralPath $kotlinLog
foreach ($needle in @('ByteString','OriginCase')) {
  if ($kotlinDiagnostics -notmatch [regex]::Escape($needle)) { throw "Kotlin probe lacked diagnostic '$needle'" }
}

$scalaRoot = Join-Path $ScratchRoot 'scala'
$scalaClassesOut = Join-Path $scalaRoot 'classes'
$scalaLog = Join-Path $OutputRoot 'scala.stderr'
New-Item -ItemType Directory -Force -Path $scalaClassesOut | Out-Null
$scalaSource = Join-Path $SdkRoot 'research/acceptance/jvm-strong-typing/negative-scala/InvalidScalaRustTypes.scala'
$scalaClasspath = "$ScalaClasses;$ScalaRuntimeClasspath;$ScalaLenses;$ScalaLibrary"
$scalaExit = Run-ExpectedCompileFailure 'java' $scalaRoot @('-cp',"$ScalaCompiler;$ScalaLibrary;$ScalaReflect",'scala.tools.nsc.Main','-d',$scalaClassesOut,'-classpath',$scalaClasspath,$scalaSource) $scalaLog @('Option[com.google.protobuf.ByteString]','required: String')

$receipt = [ordered]@{
  schema = 'acyclic.jvm.strong-typing-negative.v1'
  generated_package = (Get-FileHash -Algorithm SHA256 $JavaPackageJar).Hash
  java = [ordered]@{ status = 'rejected'; exit_code = $javaExit }
  kotlin = [ordered]@{ status = 'rejected'; exit_code = $kotlinExit }
  scala = [ordered]@{ status = 'rejected'; exit_code = $scalaExit }
}
$receipt | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $OutputRoot 'receipt.json') -Encoding utf8
Write-Output (Join-Path $OutputRoot 'receipt.json')

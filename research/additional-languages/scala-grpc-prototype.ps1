[CmdletBinding()]
param(
  [string]$Root,
  [switch]$RustGrpcFixture,
  [string]$WorkDirectory
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') '..')).Path }
$work = if ([string]::IsNullOrWhiteSpace($WorkDirectory)) {
  Join-Path $Root 'research/additional-languages/target/scala-grpc'
} else {
  [System.IO.Path]::GetFullPath($WorkDirectory)
}
$sbtVersion = '1.10.11'
$sbtSha256 = 'E988D533A020E5B60EC22C3B5DF4CD3E3DF465F4FDC3951C63036E21483978E4'
$sbtJar = Join-Path $work "sbt-launch-$sbtVersion.jar"
New-Item -ItemType Directory -Force -Path $work | Out-Null
if (-not (Test-Path $sbtJar)) {
  Invoke-WebRequest "https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/$sbtVersion/sbt-launch-$sbtVersion.jar" -OutFile $sbtJar
}
if ((Get-FileHash $sbtJar -Algorithm SHA256).Hash -ne $sbtSha256) { throw 'sbt launcher checksum mismatch' }

$project = Join-Path $work 'project'
New-Item -ItemType Directory -Force -Path $project | Out-Null
@'
sbt.version=1.10.11
'@ | Set-Content -LiteralPath (Join-Path $project 'build.properties') -Encoding ascii
@'
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"
'@ | Set-Content -LiteralPath (Join-Path $project 'plugins.sbt') -Encoding ascii
@'
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / organization := "dev.acyclic"
ThisBuild / version := "0.1.0"
name := "acyclic-sdk-scala-grpc-prototype"

Compile / PB.protoSources := Seq(
  file("../../../../proto/actors/v1"),
  file("../../../../rust/crates/stream/proto")
)
Compile / PB.targets := Seq(scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb")
Compile / PB.protocVersion := "3.25.5"

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17",
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.17",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.11.17",
  "io.grpc" % "grpc-inprocess" % "1.66.0",
  "io.grpc" % "grpc-netty-shaded" % "1.66.0",
  "com.google.protobuf" % "protobuf-java" % "3.25.1"
)
'@ | Set-Content -LiteralPath (Join-Path $work 'build.sbt') -Encoding ascii
$buildSbtPath = Join-Path $work 'build.sbt'
$protoActorsPath = (Join-Path $Root 'proto/actors/v1').Replace('\', '/')
$protoStreamPath = (Join-Path $Root 'rust/crates/stream/proto').Replace('\', '/')
$buildSbt = Get-Content -LiteralPath $buildSbtPath -Raw
$buildSbt = $buildSbt.Replace('file("../../../../proto/actors/v1")', ('file("' + $protoActorsPath + '")'))
$buildSbt = $buildSbt.Replace('file("../../../../rust/crates/stream/proto")', ('file("' + $protoStreamPath + '")'))
Set-Content -LiteralPath $buildSbtPath -Value $buildSbt -Encoding ascii

Copy-Item -LiteralPath (Join-Path $scriptDir 'scala-grpc-loopback.scala') -Destination (Join-Path $work 'ScalaGrpcLoopback.scala') -Force
$installedConsumerSource = Join-Path $scriptDir 'scala-installed-consumer.scala'

$boot = Join-Path $work '.sbt-boot'
$ivy = Join-Path $work '.ivy2'
$global = Join-Path $work '.sbt-global'
$coursier = Join-Path $work '.coursier'
Push-Location $work
$fixtureProcess = $null
try {
  if (-not $RustGrpcFixture) { throw 'RustGrpcFixture is required for the installed package qualification; the in-process bootstrap is diagnostic only.' }
  if ($RustGrpcFixture) {
    $fixtureTarget = Join-Path $work 'rust-target'
    & cargo build --manifest-path (Join-Path $Root 'rust/crates/sdk-examples/Cargo.toml') --locked --release --bin fixture-server --target-dir $fixtureTarget
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $fixtureBinary = Join-Path $fixtureTarget 'release/fixture-server.exe'
    if (-not (Test-Path -LiteralPath $fixtureBinary -PathType Leaf)) { throw "Rust fixture binary was not produced: $fixtureBinary" }
    $fixtureStdout = Join-Path $work 'rust-fixture.stdout.json'
    $fixtureStderr = Join-Path $work 'rust-fixture.stderr.log'
    $fixtureProcess = Start-Process -FilePath $fixtureBinary -ArgumentList @('--port', '0', '--grpc-port', '0', '--max-requests', '8') -RedirectStandardOutput $fixtureStdout -RedirectStandardError $fixtureStderr -PassThru -WindowStyle Hidden
    $fixtureMetadata = $null
    for ($attempt = 0; $attempt -lt 100 -and -not $fixtureMetadata; $attempt++) {
      Start-Sleep -Milliseconds 100
      if (Test-Path -LiteralPath $fixtureStdout) {
        $fixtureMetadata = Get-Content -LiteralPath $fixtureStdout -Raw | ConvertFrom-Json -ErrorAction SilentlyContinue
      }
      if ($fixtureProcess.HasExited -and -not $fixtureMetadata) { throw "Rust fixture exited before emitting metadata: $(Get-Content -LiteralPath $fixtureStderr -Raw)" }
    }
    if (-not $fixtureMetadata.grpc_address) { throw "Rust fixture did not emit grpc_address within 10 seconds" }
    $env:FIXTURE_GRPC_ADDRESS = $fixtureMetadata.grpc_address
  }
  & java "-Dsbt.boot.directory=$boot" "-Dsbt.ivy.home=$ivy" "-Dsbt.global.base=$global" "-Dsbt.coursier.home=$coursier" '-Dsbt.log.noformat=true' '-Dsbt.supershell=false' '-jar' $sbtJar 'compile' 'package' 'publishLocal'
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  $consumerWork = Join-Path $work 'installed-consumer'
  $consumerProject = Join-Path $consumerWork 'project'
  $consumerSource = Join-Path $consumerWork 'src/main/scala/acyclic/installed'
  New-Item -ItemType Directory -Force -Path $consumerProject, $consumerSource | Out-Null
  @'
sbt.version=1.10.11
'@ | Set-Content -LiteralPath (Join-Path $consumerProject 'build.properties') -Encoding ascii
  $localIvy = ($ivy.Replace('\', '/'))
  @"
ThisBuild / scalaVersion := "2.13.16"
name := "acyclic-installed-consumer"
resolvers += Resolver.file("qualified-local", file("$localIvy/local"))(Resolver.ivyStylePatterns)
libraryDependencies += "dev.acyclic" %% "acyclic-sdk-scala-grpc-prototype" % "0.1.0"
"@ | Set-Content -LiteralPath (Join-Path $consumerWork 'build.sbt') -Encoding ascii
  Copy-Item -LiteralPath $installedConsumerSource -Destination (Join-Path $consumerSource 'InstalledScalaGrpcConsumer.scala') -Force
  $consumerBoot = Join-Path $consumerWork '.sbt-boot'
  $consumerIvy = Join-Path $consumerWork '.ivy2'
  $consumerGlobal = Join-Path $consumerWork '.sbt-global'
  $consumerCoursier = Join-Path $consumerWork '.coursier'
  Push-Location $consumerWork
  try {
    & java "-Dsbt.boot.directory=$consumerBoot" "-Dsbt.ivy.home=$consumerIvy" "-Dsbt.global.base=$consumerGlobal" "-Dsbt.coursier.home=$consumerCoursier" '-Dsbt.log.noformat=true' '-Dsbt.supershell=false' '-jar' $sbtJar 'compile' 'runMain acyclic.installed.InstalledScalaGrpcConsumer'
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  } finally { Pop-Location }
  & java "-Dsbt.boot.directory=$boot" "-Dsbt.ivy.home=$ivy" "-Dsbt.global.base=$global" "-Dsbt.coursier.home=$coursier" '-Dsbt.log.noformat=true' '-Dsbt.supershell=false' '-jar' $sbtJar 'runMain acyclic.prototype.ScalaGrpcLoopback'
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
  if ($fixtureProcess -and -not $fixtureProcess.HasExited) { Stop-Process -Id $fixtureProcess.Id -Force }
  Remove-Item Env:FIXTURE_GRPC_ADDRESS -ErrorAction SilentlyContinue
  Pop-Location
}

$artifact = Join-Path $work 'target/scala-2.13/acyclic-sdk-scala-grpc-prototype_2.13-0.1.0.jar'
Write-Output "ScalaPB gRPC prototype passed compile/package/publishLocal: $artifact"
$protoRoot = Join-Path $Root 'proto'
$protoRows = Get-ChildItem -LiteralPath $protoRoot -Recurse -File -Filter '*.proto' | Sort-Object FullName | ForEach-Object {
  $relative = $_.FullName.Substring($Root.Length).TrimStart('\', '/') -replace '\\', '/'
  [ordered]@{ path = $relative; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
}
$sourceText = ($protoRows | ForEach-Object { "$($_.path) $($_.sha256)" }) -join "`n"
$sourceDigest = [Convert]::ToHexString(([System.Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($sourceText)))).ToLowerInvariant()
$generatedRows = Get-ChildItem -LiteralPath (Join-Path $work 'target/scala-2.13/src_managed') -Recurse -File -Filter '*.scala' -ErrorAction SilentlyContinue | Sort-Object FullName | ForEach-Object {
  $relative = $_.FullName.Substring($work.Length).TrimStart('\', '/') -replace '\\', '/'
  [ordered]@{ path = $relative; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
}
$receipt = [ordered]@{
  schema_version = 1
  status = 'local-prototype-passed'
  generator = [ordered]@{ scalapb = '0.11.17'; sbt_protoc = '1.0.7'; sbt = $sbtVersion; protoc = '3.25.5' }
  source_digest = $sourceDigest
  proto_files = @($protoRows)
  generated_files = @($generatedRows)
  artifact = [ordered]@{ coordinate = 'dev.acyclic:acyclic-sdk-scala-grpc-prototype_2.13:0.1.0'; sha256 = (Get-FileHash -LiteralPath $artifact -Algorithm SHA256).Hash.ToLowerInvariant() }
  installed_consumer = [ordered]@{
    source = 'research/additional-languages/scala-installed-consumer.scala'
    source_sha256 = (Get-FileHash -LiteralPath $installedConsumerSource -Algorithm SHA256).Hash.ToLowerInvariant()
    resolver = 'isolated Ivy resolver from the local publishLocal repository'
    status = 'passed against the Rust fixture over network gRPC'
  }
  loopback = 'Rust fixture auth, bytes, uint64, optional presence and server stream passed; cancellation remains a separate gate'
  rust_fixture = [bool]$RustGrpcFixture
  publication = 'isolated publishLocal only'
}
$receiptPath = Join-Path $work 'scala-grpc-receipt.json'
$receiptJson = $receipt | ConvertTo-Json -Depth 10
$receiptJson | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
$receiptJson | Set-Content -LiteralPath (Join-Path $Root 'research/additional-languages/scala-receipt.json') -Encoding utf8NoBOM
Get-FileHash $artifact -Algorithm SHA256
Write-Output "Source-bound ScalaPB receipt: $receiptPath"

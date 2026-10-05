[CmdletBinding()]
param([string]$Root, [switch]$SkipRustGeneration)
$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') '..')).Path }
$version = '7.25.0'
$jarSha256 = '41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE'
$work = Join-Path $Root 'research/additional-languages/target/bash'
$jar = Join-Path $work "openapi-generator-cli-$version.jar"
$families = @('actors','workers','stream','objects','inference')
$packages = Join-Path $work 'packages'
$zip = Join-Path $work 'acyclic-http-bash-0.1.0.zip'
New-Item -ItemType Directory -Force $work | Out-Null
if (!(Test-Path $jar)) { throw "Missing pinned OpenAPI Generator jar: $jar" }
if ((Get-FileHash $jar -Algorithm SHA256).Hash -ne $jarSha256) { throw 'OpenAPI Generator checksum mismatch' }
Push-Location $Root
try {
  foreach ($family in $families) {
    $spec = Join-Path $work "$family.json"
    if (-not $SkipRustGeneration) {
      if ($family -eq 'actors') { cargo run --quiet --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- $spec | Out-Host }
      else { cargo run --quiet --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- --contract $family $spec | Out-Host }
      if ($LASTEXITCODE -ne 0) { throw "Rust OpenAPI projection failed for $family" }
    }
    if (!(Test-Path $spec)) { throw "Missing Rust OpenAPI projection: $spec" }
    $out = Join-Path $packages $family
    if (Test-Path $out) { Remove-Item -LiteralPath $out -Recurse -Force }
    $heap = if ($family -eq 'workers' -or $family -eq 'inference') { '-Xmx768m' } else { '-Xmx512m' }
    java $heap -jar $jar generate -i $spec -g bash -o $out --package-name "acyclic-$family-bash" --additional-properties=artifactVersion=0.1.0 | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Bash package generation failed for $family" }
  }
} finally { Pop-Location }
$fixture = Join-Path $scriptDir 'bash-fixture.py'
$port = 18765
$out = Join-Path $work 'fixture.out'; $err = Join-Path $work 'fixture.err'
Remove-Item $out,$err -Force -ErrorAction SilentlyContinue
$server = Start-Process -FilePath 'python' -ArgumentList @('-u',$fixture,$port) -RedirectStandardOutput $out -RedirectStandardError $err -PassThru -WindowStyle Hidden
try {
  Start-Sleep -Milliseconds 800
  $bash = 'C:\Program Files\Git\usr\bin\bash.exe'
  $base = "http://127.0.0.1:$port"
  $auth = 'Authorization:Bearer bash-fixture-token'
  $common = @('--silent','--show-error','--host',$base,'--accept','application/json','--content-type','application/json')
  $invokeBody = '{"actorId":"actor-1","body":"AQID","method":"POST","url":"https://example.test"}'
  $actors = $invokeBody | & $bash (Join-Path $packages 'actors/client.sh') @common invokeActor $auth - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $actors -notmatch '"family":"actors"') { throw "Actors fixture failed: $actors" }
  $workersBody = '{"body":"AQID","method":"POST","url":"https://example.test"}'
  $workers = $workersBody | & $bash (Join-Path $packages 'workers/client.sh') @common invokeDeployment alias=prod $auth - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $workers -notmatch '"family":"workers"') { throw "Workers fixture failed: $workers" }
  $streamBody = '{"path":"root","limit":1}'
  $stream503Body = '{"path":"root","limit":0}'
  $stream503 = $stream503Body | & $bash (Join-Path $packages 'stream/client.sh') @common read $auth - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $stream503 -notmatch 'unavailable') { throw "Stream recovery error fixture failed: $stream503" }
  $stream200 = $streamBody | & $bash (Join-Path $packages 'stream/client.sh') @common read $auth - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $stream200 -notmatch '"family":"stream"') { throw "Stream polling fixture failed: $stream200" }
  $objectsBody = '{"body":"AQID","complete":true}'
  $objects = $objectsBody | & $bash (Join-Path $packages 'objects/client.sh') @common putObject $auth - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $objects -notmatch '"family":"objects"') { throw "Objects fixture failed: $objects" }
  $inferenceBody = '{"context":"AQID","maximumOutput":"18446744073709551615"}'
  $inference = $inferenceBody | & $bash (Join-Path $packages 'inference/client.sh') @common runsGenerate $auth - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $inference -notmatch '18446744073709551615') { throw "Inference fixture failed: $inference" }
  $unauth = $invokeBody | & $bash (Join-Path $packages 'actors/client.sh') @common invokeActor 'Authorization:Bearer wrong' - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $unauth -notmatch 'unauthenticated') { throw "Unauthenticated fixture failed: $unauth" }
} finally { Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue }
$zipWriter = Join-Path $scriptDir 'write-deterministic-zip.ps1'
& pwsh -NoProfile -File $zipWriter -Root $packages -Archive $zip
if ($LASTEXITCODE -ne 0) { throw 'Deterministic Bash archive validation failed' }
Write-Output "Bash five-family qualification passed; artifact SHA256 $((Get-FileHash $zip -Algorithm SHA256).Hash)"

[CmdletBinding()]
param([string]$Root)
$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') '..')).Path }
$version = '7.25.0'
$jarSha256 = '41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE'
$work = Join-Path $Root 'research/additional-languages/target/bash'
$jar = Join-Path $work "openapi-generator-cli-$version.jar"
$spec = Join-Path $work 'actors.json'
$package = Join-Path $work 'package'
$zip = Join-Path $work 'acyclic-actors-bash-0.1.0.zip'
New-Item -ItemType Directory -Force $work | Out-Null
if (!(Test-Path $jar)) { throw "Missing pinned OpenAPI Generator jar: $jar" }
if ((Get-FileHash $jar -Algorithm SHA256).Hash -ne $jarSha256) { throw 'OpenAPI Generator checksum mismatch' }
Push-Location $Root
try { cargo run --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- $spec | Out-Host } finally { Pop-Location }
if (Test-Path $package) { Remove-Item -LiteralPath $package -Recurse -Force }
java -jar $jar generate -i $spec -g bash -o $package --package-name acyclic-actors-bash --additional-properties=artifactVersion=0.1.0 | Out-Host
$fixture = Join-Path $scriptDir 'bash-fixture.py'
$port = 18765
$out = Join-Path $work 'fixture.out'; $err = Join-Path $work 'fixture.err'
Remove-Item $out,$err -Force -ErrorAction SilentlyContinue
$server = Start-Process -FilePath 'python' -ArgumentList @('-u',$fixture,$port) -RedirectStandardOutput $out -RedirectStandardError $err -PassThru -WindowStyle Hidden
try {
  Start-Sleep -Milliseconds 800
  $bash = 'C:\Program Files\Git\usr\bin\bash.exe'
  $client = Join-Path $package 'client.sh'
  $invokeBody = '{"actorId":"actor-1","body":"AQID","method":"POST","url":"https://example.test"}'
  $invoke = $invokeBody | & $bash $client --silent --show-error --host "http://127.0.0.1:$port" --accept application/json --content-type application/json invokeActor 'Authorization:Bearer bash-fixture-token' - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $invoke -notmatch '"body":"AQID"') { throw "Bash invoke fixture failed: $invoke" }
  $checkpoint = '{"actorId":"actor-1"}' | & $bash $client --silent --show-error --host "http://127.0.0.1:$port" --accept application/json --content-type application/json checkpointActor 'Authorization:Bearer bash-fixture-token' - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $checkpoint -notmatch '18446744073709551615') { throw "Bash uint64 fixture failed: $checkpoint" }
  $unauth = $invokeBody | & $bash $client --silent --show-error --host "http://127.0.0.1:$port" --accept application/json --content-type application/json invokeActor 'Authorization:Bearer wrong' - 2>&1 | Out-String
  if ($LASTEXITCODE -ne 0 -or $unauth -notmatch 'unauthenticated') { throw "Bash error fixture failed: $unauth" }
} finally { Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue }
Remove-Item $zip -Force -ErrorAction SilentlyContinue
Compress-Archive -Path (Join-Path $package '*') -DestinationPath $zip -CompressionLevel Optimal
Write-Output "Bash qualification passed; artifact SHA256 $((Get-FileHash $zip -Algorithm SHA256).Hash)"

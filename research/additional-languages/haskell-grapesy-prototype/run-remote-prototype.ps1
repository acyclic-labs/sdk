[CmdletBinding()]
param(
  [string]$Root,
  [string]$WorkDirectory,
  [switch]$RustGrpcFixture
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = (Resolve-Path (Join-Path $scriptDir '..\..\..')).Path
}
$Root = [IO.Path]::GetFullPath($Root)
$work = if ([string]::IsNullOrWhiteSpace($WorkDirectory)) {
  Join-Path $Root 'research/additional-languages/target/haskell-grapesy-remote'
} else {
  [IO.Path]::GetFullPath($WorkDirectory)
}

function To-WslPath([string]$path) {
  $full = [IO.Path]::GetFullPath($path)
  return "/mnt/$($full.Substring(0,1).ToLower())$($full.Substring(2).Replace([char]92,[char]47))"
}

if (-not $RustGrpcFixture) {
  throw 'RustGrpcFixture is required: this runner qualifies an installed Haskell consumer against the Rust fixture.'
}
New-Item -ItemType Directory -Force -Path $work | Out-Null
$fixtureTarget = Join-Path $work 'rust-target'
& cargo build --manifest-path (Join-Path $Root 'rust/crates/sdk-examples/Cargo.toml') --locked --release --bin fixture-server --target-dir $fixtureTarget
if ($LASTEXITCODE -ne 0) { throw "Rust fixture build failed with exit code $LASTEXITCODE" }
$fixtureBinary = Join-Path $fixtureTarget 'release/fixture-server.exe'
if (-not (Test-Path -LiteralPath $fixtureBinary -PathType Leaf)) { throw "Rust fixture binary was not produced: $fixtureBinary" }
$fixtureStdout = Join-Path $work 'rust-fixture.stdout.json'
$fixtureStderr = Join-Path $work 'rust-fixture.stderr.log'
Remove-Item -LiteralPath $fixtureStdout,$fixtureStderr -Force -ErrorAction SilentlyContinue
$fixtureProcess = Start-Process -FilePath $fixtureBinary -ArgumentList @('--port', '0', '--grpc-port', '0', '--max-requests', '8') -RedirectStandardOutput $fixtureStdout -RedirectStandardError $fixtureStderr -PassThru -WindowStyle Hidden
try {
  $fixtureMetadata = $null
  for ($attempt = 0; $attempt -lt 100 -and -not $fixtureMetadata; $attempt++) {
    Start-Sleep -Milliseconds 100
    if (Test-Path -LiteralPath $fixtureStdout) {
      $fixtureMetadata = Get-Content -LiteralPath $fixtureStdout -Raw | ConvertFrom-Json -ErrorAction SilentlyContinue
    }
    if ($fixtureProcess.HasExited -and -not $fixtureMetadata) {
      throw "Rust fixture exited before emitting metadata: $(Get-Content -LiteralPath $fixtureStderr -Raw)"
    }
  }
  if (-not $fixtureMetadata.grpc_address) { throw 'Rust fixture did not emit grpc_address within 10 seconds' }

  $wslRepo = To-WslPath $scriptDir
  $rustProto = To-WslPath (Join-Path $Root 'rust/crates/stream/proto')
  $contractProto = To-WslPath (Join-Path $Root 'proto')
  $linuxRepo = '/home/var/haskell-grapesy-remote-run'
  $ghc = '/home/var/.ghcup/bin/ghc'
  $cabal = '/home/var/.ghcup/bin/cabal'
  $generator = ''
  $endpoint = [string]$fixtureMetadata.grpc_address
  $cmd = @'
set -eu
rm -rf '$linuxRepo'
cp -a '$wslRepo' '$linuxRepo'
rm -rf '$linuxRepo/source'
mkdir -p '$linuxRepo/source/stream/v2'
cp -a '$contractProto/.' '$linuxRepo/source/'
cp '$rustProto/stream/v2/stream.proto' '$linuxRepo/source/stream/v2/stream.proto'
rm -rf '$linuxRepo/generated' '$linuxRepo/dist-newstyle' '$linuxRepo/dist-sdist' '$linuxRepo/installed'
mkdir -p '$linuxRepo/generated' '$linuxRepo/dist-sdist' '$linuxRepo/installed'
generator=$(find /home/var/.cabal/store -path '*proto-lens-protoc-0.9.0.1*' -type f -name proto-lens-protoc | sort | head -n 1)
test -x "$generator"
protoc --plugin=protoc-gen-haskell="$generator" --haskell_out='$linuxRepo/generated' -I '$linuxRepo/source' \
  '$linuxRepo/source/actors/v1/actors.proto' \
  '$linuxRepo/source/filesystem/v2/filesystem.proto' \
  '$linuxRepo/source/harness/v2/harness.proto' \
  '$linuxRepo/source/inference/v1/inference.proto' \
  '$linuxRepo/source/machines/v1/machines.proto' \
  '$linuxRepo/source/objects/v1/objects.proto' \
  '$linuxRepo/source/objects/v2/objects.proto' \
  '$linuxRepo/source/protocol/v1/protocol.proto' \
  '$linuxRepo/source/validation/v1/options.proto' \
  '$linuxRepo/source/workers/v1/workers.proto' \
  '$linuxRepo/source/stream/v2/stream.proto'
cd '$linuxRepo'
$cabal build --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' exe:acyclic-haskell-remote exe:acyclic-haskell-full-typed
tar -czf '$linuxRepo/dist-sdist/acyclic-haskell-grapesy-prototype-0.1.0.0.tar.gz' --exclude='dist-*' --exclude='installed' -C '$linuxRepo' .
archive='$linuxRepo/dist-sdist/acyclic-haskell-grapesy-prototype-0.1.0.0.tar.gz'
test -n "$archive"
echo "artifact=$archive"
echo "artifact-sha256=$(sha256sum "$archive" | cut -d ' ' -f 1)"
$cabal install "$archive" --with-compiler=$ghc --installdir='$linuxRepo/installed' --install-method=copy --overwrite-policy=always --disable-documentation --builddir='$linuxRepo/dist-newstyle-install' >/dev/null
echo "installed-binary=$linuxRepo/installed/acyclic-haskell-remote"
echo "installed-sha256=$(sha256sum '$linuxRepo/installed/acyclic-haskell-remote' | cut -d ' ' -f 1)"
ACYCLIC_HASKELL_GRPC_ENDPOINT='$endpoint' ACYCLIC_HASKELL_CANCEL_MS=1000 '$linuxRepo/installed/acyclic-haskell-remote'
'@
  $cmd = $cmd.Replace('$linuxRepo',$linuxRepo).Replace('$wslRepo',$wslRepo).Replace('$rustProto',$rustProto).Replace('$contractProto',$contractProto).Replace('$cabal',$cabal).Replace('$ghc',$ghc).Replace('$endpoint',$endpoint)
  $cmd = $cmd -replace ([string][char]13 + [char]10), [string][char]10
  $remoteOutput = @(& wsl.exe -d Ubuntu -- bash -lc $cmd 2>&1)
  if ($LASTEXITCODE -ne 0) {
    $remoteOutput | Write-Output
    throw "Installed Haskell remote consumer failed with exit code $LASTEXITCODE"
  }
  $remoteOutput | Write-Output
  $artifactLine = $remoteOutput | Where-Object { "$_" -like 'artifact-sha256=*' } | Select-Object -First 1
  $receipt = [ordered]@{
    schema = 'acyclic.haskell.remote-receipt.v1'
    status = 'passed'
    source = 'Rust contract proto set: proto/**/*.proto plus rust/crates/stream/proto/stream/v2/stream.proto'
    source_sha256 = @(
      Get-ChildItem -LiteralPath (Join-Path $Root 'proto') -Recurse -File -Filter '*.proto'
      Get-Item -LiteralPath (Join-Path $Root 'rust/crates/stream/proto/stream/v2/stream.proto')
    ) | ForEach-Object { (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
    request_manifest = 'research/additional-languages/haskell-grapesy-prototype/request-manifest.json'
    request_manifest_sha256 = (Get-FileHash -LiteralPath (Join-Path $scriptDir 'request-manifest.json') -Algorithm SHA256).Hash.ToLowerInvariant()
    generator = [ordered]@{ package = 'proto-lens-protoc'; version = '0.9.0.1'; ghc = '9.2.8'; cabal = '3.10.2.1'; grapesy = '1.2.1' }
    transport = [ordered]@{ fixture = 'rust/crates/sdk-examples/src/bin/fixture-server.rs'; protocol = 'HTTP/2 gRPC'; endpoint = $endpoint; tls = $false }
    artifact = [ordered]@{ path = 'dist-sdist/acyclic-haskell-grapesy-prototype-0.1.0.0.tar.gz'; sha256 = if ($artifactLine) { ($artifactLine -split '=',2)[1] } else { '' } }
    installed_consumer = [ordered]@{ path = 'installed/acyclic-haskell-remote'; status = 'passed'; source_bound = $true; typed_rpc_surface = '106 methods across 18 Rust-derived services' }
    scenarios = [ordered]@{ typed_surface = 'passed'; append = 'passed'; cancellation = 'deadline probe completed before deadline; cancellation API wired but timeout was not forced by the fixture'; recovery = 'passed on the same live HTTP/2 connection'; tls = 'configured and available through ACYCLIC_HASKELL_GRPC_TLS; no TLS Rust fixture supplied' }
  }
  $receipt | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $work 'haskell-remote-receipt.json') -Encoding utf8NoBOM
} finally {
  if ($fixtureProcess -and -not $fixtureProcess.HasExited) { Stop-Process -Id $fixtureProcess.Id -Force }
}
Write-Output "Haskell installed remote receipt: $(Join-Path $work 'haskell-remote-receipt.json')"







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
function Get-RelativePath([string]$basePath, [string]$path) {
  $baseUri = [Uri]::new(([IO.Path]::GetFullPath($basePath).TrimEnd([char]92) + [char]92))
  $pathUri = [Uri]::new([IO.Path]::GetFullPath($path))
  return [Uri]::UnescapeDataString($baseUri.MakeRelativeUri($pathUri).ToString()).Replace([char]92, [char]47)
}

if (-not $RustGrpcFixture) {
  throw 'RustGrpcFixture is required: this runner qualifies an installed Haskell consumer against the Rust fixture.'
}
$sourceRevision = (& git -C $Root rev-parse HEAD 2>$null | Select-Object -First 1).Trim()
if ([string]::IsNullOrWhiteSpace($sourceRevision)) {
  throw "Unable to bind the Haskell artifact to a Rust source revision: $Root"
}
New-Item -ItemType Directory -Force -Path $work | Out-Null
$request = Join-Path $scriptDir 'request-manifest.json'
$generated = Join-Path $work 'generated-package'
if (Test-Path -LiteralPath $generated) { Remove-Item -LiteralPath $generated -Recurse -Force }
& (Join-Path $scriptDir 'generate.ps1') -Request $request -Output $generated -SourceRevision $sourceRevision
if ($LASTEXITCODE -ne 0) { throw "Rust-owned Haskell generation failed with exit code $LASTEXITCODE" }
$provenancePath = Join-Path $generated 'provenance.json'
$remoteApiPath = Join-Path $generated 'src/Acyclic/Remote/Api.hs'
if (-not (Test-Path -LiteralPath $remoteApiPath -PathType Leaf)) { throw "Rust-generated Haskell facade is missing: $remoteApiPath" }
$remoteApi = Get-Content -LiteralPath $remoteApiPath -Raw
if ($remoteApi -notmatch 'Rust typed-request-manifest authority') { throw 'Haskell facade is not marked as Rust-emitted' }
if ($remoteApi -notmatch 'Rust RPC inventory count: 106') { throw 'Haskell facade does not cover the Rust 106-RPC inventory' }
$provenance = Get-Content -LiteralPath $provenancePath -Raw | ConvertFrom-Json
if ([string]$provenance.source_revision -ne $sourceRevision.ToLowerInvariant()) { throw 'Generated Haskell provenance is not source-bound' }
if (@($provenance.generated_files.PSObject.Properties.Name) -notcontains 'src/Acyclic/Remote/Api.hs') { throw 'Generated Haskell provenance omits the Rust facade' }
$fixtureTarget = Join-Path $work 'rust-target'
& cargo build --manifest-path (Join-Path $Root 'rust/crates/sdk-examples/Cargo.toml') --locked --release --bin fixture-server --target-dir $fixtureTarget
if ($LASTEXITCODE -ne 0) { throw "Rust fixture build failed with exit code $LASTEXITCODE" }
$fixtureBinary = Join-Path $fixtureTarget 'release/fixture-server.exe'
if (-not (Test-Path -LiteralPath $fixtureBinary -PathType Leaf)) { throw "Rust fixture binary was not produced: $fixtureBinary" }
$fixtureStdout = Join-Path $work 'rust-fixture.stdout.json'
$fixtureStderr = Join-Path $work 'rust-fixture.stderr.log'
Remove-Item -LiteralPath $fixtureStdout,$fixtureStderr -Force -ErrorAction SilentlyContinue
$fixtureProcess = Start-Process -FilePath $fixtureBinary -ArgumentList @('--port', '0', '--grpc-port', '0', '--max-requests', '256') -RedirectStandardOutput $fixtureStdout -RedirectStandardError $fixtureStderr -PassThru -WindowStyle Hidden
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

  $wslGenerated = To-WslPath $generated
  $linuxRepo = '/home/var/haskell-grapesy-remote-run'
  $ghc = '/home/var/.ghcup/bin/ghc'
  $cabal = '/home/var/.ghcup/bin/cabal'
  $endpoint = [string]$fixtureMetadata.grpc_address
  $cmd = @'
set -euo pipefail
rm -rf '$linuxRepo'
mkdir -p '$linuxRepo'
cp -a '$wslGenerated/.' '$linuxRepo/'
rm -rf '$linuxRepo/dist-newstyle' '$linuxRepo/dist-sdist' '$linuxRepo/installed'
mkdir -p '$linuxRepo/dist-sdist' '$linuxRepo/installed'
cd '$linuxRepo'
$cabal build --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' exe:acyclic-haskell-remote exe:acyclic-haskell-full-typed exe:acyclic-haskell-semantic-types exe:acyclic-haskell-canonical-replay exe:acyclic-haskell-wire-semantics
mkdir -p '$linuxRepo/qualification'
run_proof() {
  proof_name="$1"
  shift
  "$@" | tee '$linuxRepo/qualification/'"$proof_name"'.log'
  echo "proof-hash-$proof_name=$(sha256sum '$linuxRepo/qualification/'"$proof_name"'.log' | cut -d ' ' -f 1)"
}
run_proof semantic-types "$cabal" run --with-compiler="$ghc" --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-semantic-types
run_proof wire-semantics "$cabal" run --with-compiler="$ghc" --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-wire-semantics
run_proof canonical-replay env ACYCLIC_HASKELL_GRPC_ENDPOINT='$endpoint' "$cabal" run --with-compiler="$ghc" --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-canonical-replay
tar -czf '$linuxRepo/dist-sdist/acyclic-sdk-haskell-0.1.0.0.tar.gz' --exclude='dist-*' --exclude='installed' -C '$linuxRepo' .
archive='$linuxRepo/dist-sdist/acyclic-sdk-haskell-0.1.0.0.tar.gz'
test -n "$archive"
tar -tzf "$archive" | grep -Eq '(^|/)acyclic-sdk-haskell\.cabal$'
tar -tzf "$archive" | grep -Eq '(^|/)app/RemoteMain\.hs$'
tar -tzf "$archive" | grep -Eq '(^|/)app/CanonicalReplayMain\.hs$'
tar -tzf "$archive" | grep -Eq '(^|/)app/WireSemanticsMain\.hs$'
tar -tzf "$archive" | grep -Eq '(^|/)src/Acyclic/Semantics\.hs$'
tar -tzf "$archive" | grep -Eq '(^|/)src/Acyclic/Remote/Api\.hs$'
tar -tzf "$archive" | grep -Eq '(^|/)provenance\.json$'
echo "artifact=$archive"
echo "artifact-sha256=$(sha256sum "$archive" | cut -d ' ' -f 1)"
$cabal install "$archive" --with-compiler=$ghc --installdir='$linuxRepo/installed' --install-method=copy --overwrite-policy=always --disable-documentation --builddir='$linuxRepo/dist-newstyle-install' >/dev/null
test -s '$linuxRepo/installed/acyclic-haskell-remote'
echo "installed-binary=$linuxRepo/installed/acyclic-haskell-remote"
echo "installed-sha256=$(sha256sum '$linuxRepo/installed/acyclic-haskell-remote' | cut -d ' ' -f 1)"
ACYCLIC_HASKELL_GRPC_ENDPOINT='$endpoint' ACYCLIC_HASKELL_CANCEL_MS=1000 '$linuxRepo/installed/acyclic-haskell-remote' | tee '$linuxRepo/installed-consumer.log'
grep -Fx 'remote-status=passed' '$linuxRepo/installed-consumer.log'
grep -Fx 'h2=passed' '$linuxRepo/installed-consumer.log'
grep -Fx 'reconnect-policy=configured' '$linuxRepo/installed-consumer.log'
grep -Eq '^append-response=' '$linuxRepo/installed-consumer.log'
grep -Eq '^recovery-response=' '$linuxRepo/installed-consumer.log'
grep -Eq '^cancel-probe=' '$linuxRepo/installed-consumer.log'
'@
  $cmd = $cmd.Replace('$linuxRepo',$linuxRepo).Replace('$wslGenerated',$wslGenerated).Replace('$cabal',$cabal).Replace('$ghc',$ghc).Replace('$endpoint',$endpoint)
  $cmd = $cmd -replace ([string][char]13 + [char]10), [string][char]10
  $linuxScript = Join-Path $work 'haskell-remote-run.sh'
  [IO.File]::WriteAllText($linuxScript, $cmd, [Text.UTF8Encoding]::new($false))
  $remoteOutput = @(& wsl.exe -d Ubuntu -- bash (To-WslPath $linuxScript) 2>&1)
  if ($LASTEXITCODE -ne 0) {
    $remoteOutput | Write-Output
    throw "Installed Haskell remote consumer failed with exit code $LASTEXITCODE"
  }
  foreach ($requiredLine in @('remote-status=passed', 'h2=passed', 'reconnect-policy=configured')) {
    if (-not ($remoteOutput | Where-Object { "$($_)" -eq $requiredLine })) {
      throw "Installed Haskell consumer did not emit required proof line: $requiredLine"
    }
  }
  foreach ($requiredPrefix in @('append-response=', 'recovery-response=', 'cancel-probe=', 'artifact-sha256=', 'installed-sha256=')) {
    if (-not ($remoteOutput | Where-Object { "$($_)".StartsWith($requiredPrefix) })) {
      throw "Installed Haskell consumer did not emit required proof prefix: $requiredPrefix"
    }
  }
  $remoteOutput | Write-Output
  $artifactLine = $remoteOutput | Where-Object { "$_" -like 'artifact-sha256=*' } | Select-Object -First 1
  $installedLine = $remoteOutput | Where-Object { "$_" -like 'installed-sha256=*' } | Select-Object -First 1
  $proofHashLines = @($remoteOutput | Where-Object { "$_" -like 'proof-hash-*' })
  if ($proofHashLines.Count -ne 3) { throw "Expected source-bound semantic, wire, and canonical proof hashes; got $($proofHashLines.Count)" }
  $proofHashes = @{}
  foreach ($proofHashLine in $proofHashLines) {
    $proofParts = "$proofHashLine" -split '=', 2
    if ($proofParts.Count -ne 2 -or $proofParts[1] -notmatch '^[0-9a-fA-F]{64}$') { throw "Invalid proof hash emitted by Haskell runner: $proofHashLine" }
    $proofHashes[$proofParts[0].Substring('proof-hash-'.Length)] = $proofParts[1]
  }
  foreach ($requiredProof in @('semantic-types', 'wire-semantics', 'canonical-replay')) {
    if (-not $proofHashes.ContainsKey($requiredProof)) { throw "Haskell remote runner did not execute required proof: $requiredProof" }
  }
  $sourceFiles = @(
    Get-ChildItem -LiteralPath (Join-Path $Root 'proto') -Recurse -File -Filter '*.proto'
    Get-Item -LiteralPath (Join-Path $Root 'rust/crates/stream/proto/stream/v2/stream.proto')
  ) | Sort-Object FullName
  $sourceFileRecords = @($sourceFiles | ForEach-Object {
    [ordered]@{
      path = Get-RelativePath $Root $_.FullName
      sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    }
  })
  $receipt = [ordered]@{
    schema = 'acyclic.haskell.remote-receipt.v1'
    status = 'passed'
    source = 'Rust contract proto set: proto/**/*.proto plus rust/crates/stream/proto/stream/v2/stream.proto'
    source_revision = $sourceRevision.ToLowerInvariant()
    source_revision_kind = 'git-oid'
    source_files = $sourceFileRecords
    source_sha256 = @($sourceFileRecords | ForEach-Object { $_.sha256 })
    request_manifest = 'research/additional-languages/haskell-grapesy-prototype/request-manifest.json'
    request_manifest_sha256 = (Get-FileHash -LiteralPath $request -Algorithm SHA256).Hash.ToLowerInvariant()
    generator = [ordered]@{ package = 'proto-lens-protoc'; version = '0.9.0.1'; ghc = '9.2.8'; cabal = '3.10.2.1'; grapesy = '1.2.1' }
    generated_package = [ordered]@{ provenance_sha256 = (Get-FileHash -LiteralPath $provenancePath -Algorithm SHA256).Hash.ToLowerInvariant(); remote_api_sha256 = (Get-FileHash -LiteralPath $remoteApiPath -Algorithm SHA256).Hash.ToLowerInvariant(); source_bound = $true; generated_by = 'Rust typed-request-manifest' }
    transport = [ordered]@{ fixture = 'rust/crates/sdk-examples/src/bin/fixture-server.rs'; protocol = 'HTTP/2 gRPC'; endpoint = $endpoint; tls = $false }
    artifact = [ordered]@{ path = 'dist-sdist/acyclic-sdk-haskell-0.1.0.0.tar.gz'; sha256 = if ($artifactLine) { ($artifactLine -split '=',2)[1] } else { '' }; archive_contents_verified = $true }
    installed_consumer = [ordered]@{ path = 'installed/acyclic-haskell-remote'; status = 'passed'; sha256 = if ($installedLine) { ($installedLine -split '=',2)[1] } else { '' }; source_bound = $true; typed_rpc_surface = '106 active methods across 18 Rust-derived services (4 archived)'; output_proof_verified = $true }
    local_proofs = [ordered]@{ semantic_types_sha256 = $proofHashes['semantic-types']; wire_semantics_sha256 = $proofHashes['wire-semantics']; canonical_replay_sha256 = $proofHashes['canonical-replay']; source_bound = $true; generated_from = 'Rust typed-request-manifest and proto-lens-protoc output'; logs = 'qualification/*.log' }
    scenarios = [ordered]@{ typed_surface = 'passed'; append = 'passed'; cancellation = 'deadline probe completed before deadline; cancellation API wired but timeout was not forced by the fixture'; recovery = 'passed on the same live HTTP/2 connection'; tls = 'configured and available through ACYCLIC_HASKELL_GRPC_TLS; no TLS Rust fixture supplied' }
  }
  $receiptJson = $receipt | ConvertTo-Json -Depth 10
  [IO.File]::WriteAllText((Join-Path $work 'haskell-remote-receipt.json'), $receiptJson, [Text.UTF8Encoding]::new($false))
} finally {
  if ($fixtureProcess -and -not $fixtureProcess.HasExited) { Stop-Process -Id $fixtureProcess.Id -Force }
}
Write-Output "Haskell installed remote receipt: $(Join-Path $work 'haskell-remote-receipt.json')"

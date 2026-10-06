# Generate a fresh Rust-owned Haskell package and run the local generated-wire proof.
[CmdletBinding()]
param(
  [string]$OutputDirectory,
  [string]$WorkDirectory
)

$ErrorActionPreference = 'Stop'
$repo = $PSScriptRoot
$root = (Resolve-Path (Join-Path $repo '..\..\..')).Path
$request = Join-Path $repo 'request-manifest.json'
$sourceRevision = (& git -C $root rev-parse HEAD 2>$null | Select-Object -First 1).Trim()
if ($sourceRevision -notmatch '^[0-9a-fA-F]{40}$') { throw "Unable to bind Haskell package to a Rust source revision: $root" }
$generated = if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
  Join-Path $root "research/additional-languages/target/haskell-generated-$sourceRevision"
} else { [IO.Path]::GetFullPath($OutputDirectory) }
$work = if ([string]::IsNullOrWhiteSpace($WorkDirectory)) {
  Join-Path $root 'research/additional-languages/target/haskell-prototype-run'
} else { [IO.Path]::GetFullPath($WorkDirectory) }
if (Test-Path -LiteralPath $generated) { Remove-Item -LiteralPath $generated -Recurse -Force }
if (Test-Path -LiteralPath $work) { Remove-Item -LiteralPath $work -Recurse -Force }
New-Item -ItemType Directory -Force -Path $work | Out-Null
& (Join-Path $repo 'generate.ps1') -Request $request -Output $generated -SourceRevision $sourceRevision
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
function To-WslPath([string]$path) {
  $full = [IO.Path]::GetFullPath($path)
  return "/mnt/$($full.Substring(0,1).ToLower())$($full.Substring(2).Replace([char]92,[char]47))"
}
function Get-RelativePath([string]$basePath, [string]$path) {
  $baseUri = [Uri]::new(([IO.Path]::GetFullPath($basePath).TrimEnd([char]92) + [char]92))
  $pathUri = [Uri]::new([IO.Path]::GetFullPath($path))
  return [Uri]::UnescapeDataString($baseUri.MakeRelativeUri($pathUri).ToString()).Replace([char]92, [char]47)
}
$wslGenerated = To-WslPath $generated
$ghc = '/home/var/.ghcup/bin/ghc'
$cabal = '/home/var/.ghcup/bin/cabal'
$linuxRepo = '/home/var/haskell-grapesy-prototype-run'
$cmd = @'
set -euo pipefail
rm -rf '$linuxRepo'
mkdir -p '$linuxRepo'
cp -a '$wslGenerated/.' '$linuxRepo/'
rm -rf '$linuxRepo/dist-newstyle'
cd '$linuxRepo'
$cabal build --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' all
$cabal run --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-prototype
$cabal run --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-full-typed
mkdir -p '$linuxRepo/qualification'
run_proof() {
  proof_name="$1"
  shift
  "$cabal" run --with-compiler="$ghc" --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' "$proof_name" "$@" | tee '$linuxRepo/qualification/'"$proof_name"'.log'
  echo "proof-hash-$proof_name=$(sha256sum '$linuxRepo/qualification/'"$proof_name"'.log' | cut -d ' ' -f 1)"
}
run_proof acyclic-haskell-semantic-types
run_proof acyclic-haskell-wire-semantics
if [ -n "${ACYCLIC_HASKELL_GRPC_ENDPOINT:-}" ]; then
  run_proof acyclic-haskell-canonical-replay
fi
'@
$cmd = $cmd.Replace('$linuxRepo',$linuxRepo).Replace('$wslGenerated',$wslGenerated).Replace('$cabal',$cabal).Replace('$ghc',$ghc)
$cmd = $cmd -replace ([string][char]13 + [char]10), [string][char]10
$prototypeOutput = @(& wsl.exe -d Ubuntu -- bash -lc $cmd 2>&1)
$prototypeExitCode = $LASTEXITCODE
$prototypeOutput | Write-Output
if ($prototypeExitCode -ne 0) { throw "Haskell prototype failed with exit code $prototypeExitCode" }

$proofLines = @($prototypeOutput | Where-Object { "$($_)" -like 'proof-hash-*' })
$proofHashes = @{}
foreach ($proofLine in $proofLines) {
  $parts = "$proofLine" -split '=', 2
  if ($parts.Count -ne 2 -or $parts[1] -notmatch '^[0-9a-fA-F]{64}$') { throw "Invalid proof hash emitted by Haskell runner: $proofLine" }
  $proofHashes[$parts[0].Substring('proof-hash-'.Length)] = $parts[1].ToLowerInvariant()
}
foreach ($requiredProof in @('acyclic-haskell-semantic-types', 'acyclic-haskell-wire-semantics')) {
  if (-not $proofHashes.ContainsKey($requiredProof)) { throw "Haskell runner did not execute required proof: $requiredProof" }
}
$canonicalExecuted = $proofHashes.ContainsKey('acyclic-haskell-canonical-replay')
$sourceFiles = @(
  Get-ChildItem -LiteralPath (Join-Path $root 'proto') -Recurse -File -Filter '*.proto'
  Get-Item -LiteralPath (Join-Path $root 'rust/crates/stream/proto/stream/v2/stream.proto')
) | Sort-Object FullName
$sourceFileRecords = @($sourceFiles | ForEach-Object {
  $relative = Get-RelativePath $root $_.FullName
  [ordered]@{ path = $relative; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
})
$receipt = [ordered]@{
  schema = 'acyclic.haskell.local-receipt.v1'
  status = 'passed'
  source_revision = $sourceRevision.ToLowerInvariant()
  source_revision_kind = 'git-oid'
  source_files = $sourceFileRecords
  request_manifest = 'research/additional-languages/haskell-grapesy-prototype/request-manifest.json'
  request_manifest_sha256 = (Get-FileHash -LiteralPath $request -Algorithm SHA256).Hash.ToLowerInvariant()
  generated_package = [ordered]@{
    provenance_sha256 = (Get-FileHash -LiteralPath (Join-Path $generated 'provenance.json') -Algorithm SHA256).Hash.ToLowerInvariant()
    source_bound = $true
    generated_by = 'Rust typed-request-manifest and proto-lens-protoc 0.9.0.1'
  }
  typed_surface = [ordered]@{ active_methods = 106; services = 18; archived_services = 4; proof = 'acyclic-haskell-full-typed' }
  proofs = [ordered]@{
    semantic_types_sha256 = $proofHashes['acyclic-haskell-semantic-types']
    wire_semantics_sha256 = $proofHashes['acyclic-haskell-wire-semantics']
    canonical_replay_sha256 = if ($canonicalExecuted) { $proofHashes['acyclic-haskell-canonical-replay'] } else { $null }
    canonical_replay_executed = $canonicalExecuted
    logs = 'qualification/*.log'
  }
}
$receiptPath = Join-Path $work 'haskell-local-receipt.json'
$receipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
Write-Output "Haskell local receipt: $receiptPath"

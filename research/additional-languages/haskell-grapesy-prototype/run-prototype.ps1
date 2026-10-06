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
$wslGenerated = To-WslPath $generated
$ghc = '/home/var/.ghcup/bin/ghc'
$cabal = '/home/var/.ghcup/bin/cabal'
$linuxRepo = '/home/var/haskell-grapesy-prototype-run'
$cmd = @"
set -eu
rm -rf '$linuxRepo'
mkdir -p '$linuxRepo'
cp -a '$wslGenerated/.' '$linuxRepo/'
rm -rf '$linuxRepo/dist-newstyle'
cd '$linuxRepo'
$cabal build --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' all
$cabal run --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-prototype
$cabal run --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-full-typed
"@
$cmd = $cmd -replace ([string][char]13 + [char]10), [string][char]10
wsl.exe -d Ubuntu -- bash -lc $cmd
if ($LASTEXITCODE -ne 0) { throw "Haskell prototype failed with exit code $LASTEXITCODE" }

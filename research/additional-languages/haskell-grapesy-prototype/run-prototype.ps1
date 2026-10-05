# Regenerate Haskell bindings from the Rust proto and run the local generated-wire proof.
$ErrorActionPreference = 'Stop'
$repo = $PSScriptRoot
function To-WslPath([string]$path) {
  $full = [IO.Path]::GetFullPath($path)
  return "/mnt/$($full.Substring(0,1).ToLower())$($full.Substring(2).Replace([char]92,[char]47))"
}
$wslRepo = To-WslPath $repo
$ghc = '/home/var/.ghcup/bin/ghc'
$cabal = '/home/var/.ghcup/bin/cabal'
$generator = '/home/var/.cabal/store/ghc-9.2.8/proto-lens-protoc-0.9.0.1-e-proto-lens-protoc-f0605199134fd86e314544dedd7e3f4e568e90a6f5b04ece5303e70fe3b06bd3/bin/proto-lens-protoc'
$rustProto = To-WslPath (Join-Path $repo '../../../rust/crates/stream/proto')
$contractProto = To-WslPath (Join-Path $repo '../../../proto')
$linuxRepo = '/home/var/haskell-grapesy-prototype-run'
$cmd = @"
set -eu
rm -rf '$linuxRepo'
cp -a '$wslRepo' '$linuxRepo'
rm -rf '$linuxRepo/source'
mkdir -p '$linuxRepo/source/stream/v2'
cp -a '$contractProto/.' '$linuxRepo/source/'
cp '$rustProto/stream/v2/stream.proto' '$linuxRepo/source/stream/v2/stream.proto'
rm -rf '$linuxRepo/generated' '$linuxRepo/dist-newstyle'
mkdir -p '$linuxRepo/generated'
protoc --plugin=protoc-gen-haskell='$generator' --haskell_out='$linuxRepo/generated' -I '$linuxRepo/source' \
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
$cabal build --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' all
$cabal run --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-prototype
$cabal run --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-full-typed
"@
$cmd = $cmd -replace ([string][char]13 + [char]10), [string][char]10
wsl.exe -d Ubuntu -- bash -lc $cmd
if ($LASTEXITCODE -ne 0) { throw "Haskell prototype failed with exit code $LASTEXITCODE" }

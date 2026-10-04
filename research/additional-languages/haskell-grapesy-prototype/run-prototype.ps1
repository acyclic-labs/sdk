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
$linuxRepo = '/home/var/haskell-grapesy-prototype-run'
$cmd = @"
set -eu
rm -rf '$linuxRepo'
cp -a '$wslRepo' '$linuxRepo'
cp '$rustProto/stream/v2/stream.proto' '$linuxRepo/source/stream.proto'
rm -rf '$linuxRepo/generated' '$linuxRepo/dist-newstyle'
mkdir -p '$linuxRepo/generated'
protoc --plugin=protoc-gen-haskell='$generator' --haskell_out='$linuxRepo/generated' -I '$rustProto' '$rustProto/stream/v2/stream.proto'
cd '$linuxRepo'
$cabal build --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' all
$cabal run --with-compiler=$ghc --project-file='$linuxRepo/cabal.project' --builddir='$linuxRepo/dist-newstyle' acyclic-haskell-prototype
"@
$cmd = $cmd -replace ([string][char]13 + [char]10), [string][char]10
wsl.exe -d Ubuntu -- bash -lc $cmd
if ($LASTEXITCODE -ne 0) { throw "Haskell prototype failed with exit code $LASTEXITCODE" }

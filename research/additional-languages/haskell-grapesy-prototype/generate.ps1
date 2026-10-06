[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string] $Request,
  [Parameter(Mandatory)] [string] $Output,
  [string] $SourceRevision
)
$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = (Resolve-Path (Join-Path $scriptDir '..\..\..')).Path
$requestPath = (Resolve-Path -LiteralPath $Request).Path
$outputPath = [IO.Path]::GetFullPath($Output)
$requestDocument = Get-Content -LiteralPath $requestPath -Raw | ConvertFrom-Json
$requestSchema = [string]$requestDocument.schema
$legacyManifest = $requestSchema -eq 'acyclic.haskell.remote-request-manifest.v1' -and $requestDocument.rust_owned -eq $true
$languageProducerRequest = $requestSchema -eq 'acyclic.sdk.generation.request.v1' -and [string]$requestDocument.target -eq 'haskell' -and [string]$requestDocument.tool -eq 'sdk-language-producer'
if (-not ($legacyManifest -or $languageProducerRequest)) { throw 'Request must be a Rust-owned Haskell generation request' }
if ([string]::IsNullOrWhiteSpace($SourceRevision)) {
  $SourceRevision = [string]$requestDocument.source.revision
}
if ($SourceRevision -notmatch '^[0-9a-fA-F]{40}$') { throw '-SourceRevision or request source.revision must be a 40-character Rust git revision' }
$actualRevision = (& git -C $root rev-parse HEAD 2>$null | Select-Object -First 1).Trim()
if ($actualRevision -ne $SourceRevision.ToLowerInvariant()) { throw "Source revision does not match checkout: $actualRevision" }
if (Test-Path -LiteralPath $outputPath) {
  if (@(Get-ChildItem -LiteralPath $outputPath -Force).Count -gt 0) { throw "Output must be a new empty directory: $outputPath" }
} else { New-Item -ItemType Directory -Force -Path $outputPath | Out-Null }
function To-WslPath([string]$path) {
  $full = [IO.Path]::GetFullPath($path)
  return "/mnt/$($full.Substring(0,1).ToLower())$($full.Substring(2).Replace([char]92,[char]47))"
}
$wslRoot = To-WslPath $root
$wslOutput = To-WslPath $outputPath
$contractProto = Join-Path $root 'proto'
$target = "/tmp/acyclic-haskell-generator-$SourceRevision"
$semantics = To-WslPath (Join-Path $outputPath 'src/Acyclic/Semantics.hs')
$semanticTest = To-WslPath (Join-Path $outputPath 'app/SemanticTypesMain.hs')
New-Item -ItemType Directory -Force -Path (Join-Path $outputPath 'source/stream/v2'), (Join-Path $outputPath 'generated'), (Join-Path $outputPath 'app'), (Join-Path $outputPath 'src') | Out-Null
Copy-Item -LiteralPath (Join-Path $scriptDir 'acyclic-haskell-grapesy-prototype.cabal') -Destination (Join-Path $outputPath 'acyclic-sdk-haskell.cabal')
Copy-Item -LiteralPath (Join-Path $scriptDir 'cabal.project') -Destination $outputPath
Copy-Item -LiteralPath (Join-Path $scriptDir 'cabal.project.freeze') -Destination $outputPath
Copy-Item -LiteralPath (Join-Path $scriptDir 'hackage-root.json') -Destination $outputPath
Copy-Item -Path (Join-Path $scriptDir 'app\*.hs') -Destination (Join-Path $outputPath 'app')
Copy-Item -Path (Join-Path $scriptDir 'src\*') -Destination (Join-Path $outputPath 'src') -Recurse
New-Item -ItemType Directory -Force -Path (Join-Path $outputPath 'src/Acyclic') | Out-Null
Copy-Item -Path (Join-Path $contractProto '*') -Destination (Join-Path $outputPath 'source') -Recurse
Copy-Item -LiteralPath (Join-Path $root 'rust/crates/stream/proto/stream/v2/stream.proto') -Destination (Join-Path $outputPath 'source/stream/v2/stream.proto')
$cmd = @(
  'set -euo pipefail',
  'export PATH=/home/var/.cargo/bin:/home/var/.ghcup/bin:/usr/bin:/bin',
  'generator=$(/usr/bin/find /home/var/.cabal/store -path ''*proto-lens-protoc-0.9.0.1*'' -type f -name proto-lens-protoc | sort | head -n 1)',
  'test -x "$generator"',
  'protoc --experimental_allow_proto3_optional --plugin=protoc-gen-haskell="$generator" --haskell_out=''$wslOutput/generated'' -I ''$wslOutput/source'' \',
  '  ''$wslOutput/source/actors/v1/actors.proto'' ''$wslOutput/source/filesystem/v2/filesystem.proto'' \',
  '  ''$wslOutput/source/harness/v2/harness.proto'' ''$wslOutput/source/inference/v1/inference.proto'' \',
  '  ''$wslOutput/source/machines/v1/machines.proto'' ''$wslOutput/source/objects/v1/objects.proto'' \',
  '  ''$wslOutput/source/objects/v2/objects.proto'' ''$wslOutput/source/protocol/v1/protocol.proto'' \',
  '  ''$wslOutput/source/validation/v1/options.proto'' ''$wslOutput/source/workers/v1/workers.proto'' \',
  '  ''$wslOutput/source/stream/v2/stream.proto''',
  'cargo run --manifest-path ''$wslRoot/rust/crates/sdk-examples/Cargo.toml'' --locked --target-dir ''$target'' --bin typed-request-manifest -- --haskell-semantics-output ''$semantics''',
  'cargo run --manifest-path ''$wslRoot/rust/crates/sdk-examples/Cargo.toml'' --locked --target-dir ''$target'' --bin typed-request-manifest -- --haskell-semantics-test-output ''$semanticTest''',
  'rm -rf ''$target'''
) -join "`n"
$cmd = $cmd.Replace('$wslRoot', $wslRoot).Replace('$wslOutput', $wslOutput).Replace('$target', $target).Replace('$semantics', $semantics).Replace('$semanticTest', $semanticTest)
$linuxScript = Join-Path $outputPath 'generate-haskell.sh'
[IO.File]::WriteAllText($linuxScript, $cmd, [Text.UTF8Encoding]::new($false))
wsl.exe -d Ubuntu -- bash (To-WslPath $linuxScript)
if ($LASTEXITCODE -ne 0) { throw "Rust-owned Haskell generation failed with exit code $LASTEXITCODE" }
Remove-Item -LiteralPath $linuxScript -Force
$files = @(Get-ChildItem -LiteralPath $outputPath -Recurse -File | Where-Object { $_.Name -ne 'provenance.json' })
$fileHashes = [ordered]@{}
foreach ($file in $files | Sort-Object FullName) {
  $relative = [IO.Path]::GetRelativePath($outputPath, $file.FullName).Replace([char]92, [char]47)
  $fileHashes[$relative] = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
}
$requestHash = (Get-FileHash -LiteralPath $requestPath -Algorithm SHA256).Hash.ToLowerInvariant()
$provenance = [ordered]@{
  schema = 'acyclic.haskell.generated-package.v1'
  language = 'haskell'
  source_revision = $SourceRevision.ToLowerInvariant()
  source_revision_kind = 'git-oid'
  source_of_truth = 'Rust proto descriptors and Rust typed-request-manifest emitter'
  request = [IO.Path]::GetRelativePath($root, $requestPath).Replace([char]92, [char]47)
  request_sha256 = $requestHash
  generator = [ordered]@{
    protoc_plugin = 'proto-lens-protoc 0.9.0.1'
    proto_lens = '0.7.1.7'
    proto_lens_runtime = '0.7.0.8'
    proto_lens_protobuf_types = '0.7.2.3'
    grapesy = '1.2.1'
    ghc = '9.2.8'
    cabal = '3.10.2.1'
  }
  generated_files = $fileHashes
}
$provenance | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $outputPath 'provenance.json') -Encoding utf8NoBOM
Write-Output "Generated Rust-owned Haskell package: $outputPath"

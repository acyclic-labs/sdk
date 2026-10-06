$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
. (Join-Path $PSScriptRoot 'verify-dotnet-embedded-native-manifest.ps1')
$scratch = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-embedded-merge-' + [guid]::NewGuid().ToString('N'))
$inputs = @()
$targetSpecs = @(
  @{ Rid='win-x64'; Target='x86_64-pc-windows-msvc'; File='acyclic_sdk_embedded_prototype.dll' },
  @{ Rid='win-arm64'; Target='aarch64-pc-windows-msvc'; File='acyclic_sdk_embedded_prototype.dll' },
  @{ Rid='linux-x64'; Target='x86_64-unknown-linux-gnu'; File='libacyclic_sdk_embedded_prototype.so' },
  @{ Rid='linux-arm64'; Target='aarch64-unknown-linux-gnu'; File='libacyclic_sdk_embedded_prototype.so' },
  @{ Rid='linux-musl-x64'; Target='x86_64-unknown-linux-musl'; File='libacyclic_sdk_embedded_prototype.so' },
  @{ Rid='linux-musl-arm64'; Target='aarch64-unknown-linux-musl'; File='libacyclic_sdk_embedded_prototype.so' },
  @{ Rid='osx-x64'; Target='x86_64-apple-darwin'; File='libacyclic_sdk_embedded_prototype.dylib' },
  @{ Rid='osx-arm64'; Target='aarch64-apple-darwin'; File='libacyclic_sdk_embedded_prototype.dylib' }
)
try {
  New-Item -ItemType Directory -Force -Path $scratch | Out-Null
  $closure = Get-EmbeddedRustSourceClosure -Repository $root
  $lockPath = Join-Path $root 'rust/crates/sdk-embedded-prototype/Cargo.lock'
  $lockHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $lockPath).Hash.ToLowerInvariant()
  $cargoCommand = 'cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --target <rust_target> --target-dir <target_dir>'
  foreach ($spec in $targetSpecs) {
    $input = Join-Path $scratch $spec.Rid
    $native = Join-Path $input 'native'
    $assetDir = Join-Path $native $spec.Rid
    New-Item -ItemType Directory -Force -Path $assetDir | Out-Null
    $asset = Join-Path $assetDir $spec.File
    [IO.File]::WriteAllBytes($asset, [Text.Encoding]::UTF8.GetBytes("merge-$($spec.Rid)"))
    $record = [pscustomobject]@{ rust_target=$spec.Target; rid=$spec.Rid; file=$spec.File; sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath $asset).Hash.ToLowerInvariant(); bytes=(Get-Item $asset).Length }
    $manifest = [ordered]@{ schema='acyclic.sdk.dotnet.embedded.native-manifest.v1'; source_revision=$closure.Revision; source_revision_kind='git-oid'; source_inputs=@($closure.Paths); cargo_manifest='rust/crates/sdk-embedded-prototype/Cargo.toml'; cargo_lock='rust/crates/sdk-embedded-prototype/Cargo.lock'; cargo_lock_sha256=$lockHash; source_inputs_sha256=$closure.Digest; cargo_command=$cargoCommand; assets=@($record) }
    $manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $native 'native-manifest.json') -Encoding utf8NoBOM
    $inputs += $input
  }
  $output = Join-Path $scratch 'merged'
  & (Join-Path $PSScriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot $output | Out-Host
  $result = Get-Content -LiteralPath (Join-Path $output 'native-manifest.json') -Raw | ConvertFrom-Json
  if (@($result.assets).Count -ne 8) { throw 'cross-host merge did not produce eight assets' }
  Write-Output 'CROSS_HOST_NATIVE_MERGE=PASS'
}
finally {
  if (Test-Path -LiteralPath $scratch) { Remove-Item -LiteralPath $scratch -Recurse -Force }
}

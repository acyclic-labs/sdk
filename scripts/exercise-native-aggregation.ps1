$ErrorActionPreference = 'Stop'
$candidate = $PSScriptRoot
$scratch = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-native-exercise-' + [guid]::NewGuid().ToString('N'))
$fakeRepo = Join-Path $scratch 'repo'
$scriptRoot = Join-Path $fakeRepo 'scripts'
$fixedRevision = '0123456789abcdef0123456789abcdef01234567'
$global:fixedRevision = $fixedRevision
$global:realCargo = (Get-Command cargo.exe -ErrorAction Stop).Source
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
function Remove-ExerciseScratch {
  if (-not (Test-Path -LiteralPath $scratch)) { return }
  $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\') + '\'
  $resolved = [IO.Path]::GetFullPath($scratch)
  if ($resolved -eq $tempRoot.TrimEnd('\') -or -not $resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to remove exercise path outside the temporary root: $resolved"
  }
  $item = Get-Item -LiteralPath $resolved -Force
  if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw "Refusing to remove a reparse-point exercise path: $resolved"
  }
  Remove-Item -LiteralPath $resolved -Recurse -Force
}
function global:git {
  param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
  if ($Arguments -contains 'rev-parse') { Write-Output $global:fixedRevision; return }
  throw "unexpected fake git invocation: $($Arguments -join ' ')"
}
function global:cargo {
  param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
  if ($Arguments -contains 'metadata') {
    & $global:realCargo @Arguments
    return
  }
  throw "unexpected fake cargo invocation: $($Arguments -join ' ')"
}
try {
  $global:fakeRepo = $fakeRepo
  New-Item -ItemType Directory -Force -Path (Join-Path $fakeRepo 'rust/crates/sdk-embedded-prototype/src'), $scriptRoot | Out-Null
  Set-Content -LiteralPath (Join-Path $fakeRepo 'Cargo.lock') -Value "version = 3`n" -Encoding utf8NoBOM
  Set-Content -LiteralPath (Join-Path $fakeRepo 'rust/crates/sdk-embedded-prototype/Cargo.lock') -Value "version = 3`n" -Encoding utf8NoBOM
  Set-Content -LiteralPath (Join-Path $fakeRepo 'rust/crates/sdk-embedded-prototype/Cargo.toml') -Value "[package]`nname = 'sdk-embedded-prototype'`nversion = '0.1.0'`n`n[workspace]`n" -Encoding utf8NoBOM
  Set-Content -LiteralPath (Join-Path $fakeRepo 'rust/crates/sdk-embedded-prototype/src/lib.rs') -Value 'pub fn fixture() {}' -Encoding utf8NoBOM
  Copy-Item (Join-Path $candidate 'verify-dotnet-embedded-native-manifest.ps1') (Join-Path $scriptRoot 'verify-dotnet-embedded-native-manifest.ps1')
  Copy-Item (Join-Path $candidate 'merge-embedded-native-manifests.ps1') (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1')
  & $global:realCargo generate-lockfile --offline --manifest-path (Join-Path $fakeRepo 'rust/crates/sdk-embedded-prototype/Cargo.toml') | Out-Host
  & $global:realCargo metadata --format-version 1 --locked --manifest-path (Join-Path $fakeRepo 'rust/crates/sdk-embedded-prototype/Cargo.toml') | Out-Host
  if ($LASTEXITCODE -ne 0) { throw 'fixture cargo metadata failed before strict helper' }
  . (Join-Path $scriptRoot 'verify-dotnet-embedded-native-manifest.ps1')
  $closure = Get-EmbeddedRustSourceClosure -Repository $fakeRepo
  $lockPath = Join-Path $fakeRepo 'rust/crates/sdk-embedded-prototype/Cargo.lock'
  $lockHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $lockPath).Hash.ToLowerInvariant()
  $cargoCommand = 'cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --target <rust_target> --target-dir <target_dir>'
  $inputs = @()
  foreach ($spec in $targetSpecs) {
    $input = Join-Path $scratch $spec.Rid
    $native = Join-Path $input 'native'
    $assetDir = Join-Path $native $spec.Rid
    New-Item -ItemType Directory -Force -Path $assetDir | Out-Null
    $asset = Join-Path $assetDir $spec.File
    [IO.File]::WriteAllBytes($asset, [Text.Encoding]::UTF8.GetBytes("exercise-$($spec.Rid)"))
    $record = [pscustomobject]@{ rust_target=$spec.Target; rid=$spec.Rid; file=$spec.File; sha256=(Get-FileHash -Algorithm SHA256 -LiteralPath $asset).Hash.ToLowerInvariant(); bytes=(Get-Item $asset).Length }
    $manifest = [ordered]@{ schema='acyclic.sdk.dotnet.embedded.native-manifest.v1'; source_revision=$fixedRevision; source_revision_kind='git-oid'; source_inputs=@($closure.Paths); cargo_manifest='rust/crates/sdk-embedded-prototype/Cargo.toml'; cargo_lock='rust/crates/sdk-embedded-prototype/Cargo.lock'; cargo_lock_sha256=$lockHash; source_inputs_sha256=$closure.Digest; cargo_command=$cargoCommand; assets=@($record) }
    $manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $native 'native-manifest.json') -Encoding utf8NoBOM
    $inputs += $input
  }
  $merged = Join-Path $scratch 'merged'
  & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot $merged | Out-Host
  $result = Get-Content -LiteralPath (Join-Path $merged 'native-manifest.json') -Raw | ConvertFrom-Json
  if (@($result.assets).Count -ne 8) { throw 'aggregate output did not contain eight assets' }
  Write-Output 'AGGREGATE_EIGHT=PASS'

  $partial = Get-VerifiedEmbeddedNativeManifest -Repository $fakeRepo -NativeRoot (Join-Path $inputs[0] 'native') -ManifestPath (Join-Path $inputs[0] 'native/native-manifest.json') -AllowPartial
  if (@($partial.Manifest.assets).Count -ne 1) { throw 'partial manifest did not validate' }
  Write-Output 'PARTIAL_SINGLE_RID=PASS'

  $duplicateFailed = $false
  try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot ($inputs + $inputs[0]) -OutputRoot (Join-Path $scratch 'duplicate') | Out-Host } catch { $duplicateFailed = $true }
  if (-not $duplicateFailed) { throw 'duplicate RID was accepted' }
  Write-Output 'DUPLICATE_RID=PASS'

  $missingFailed = $false
  try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs[0..6] -OutputRoot (Join-Path $scratch 'missing') | Out-Host } catch { $missingFailed = $true }
  if (-not $missingFailed) { throw 'missing RID was accepted' }
  Write-Output 'MISSING_RID=PASS'

  $tamperAsset = Join-Path (Join-Path (Join-Path $inputs[2] 'native') $targetSpecs[2].Rid) $targetSpecs[2].File
  $originalBytes = [IO.File]::ReadAllBytes($tamperAsset)
  try {
    [IO.File]::WriteAllBytes($tamperAsset, $originalBytes + [byte]255)
    $tamperFailed = $false
    try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot (Join-Path $scratch 'tampered') | Out-Host } catch { $tamperFailed = $true }
    if (-not $tamperFailed) { throw 'native asset tamper was accepted' }
    Write-Output 'NATIVE_HASH_TAMPER=PASS'
  }
  finally {
    [IO.File]::WriteAllBytes($tamperAsset, $originalBytes)
  }

  $raceSource = Join-Path (Join-Path (Join-Path $inputs[4] 'native') $targetSpecs[4].Rid) $targetSpecs[4].File
  $global:lateMutationSource = $raceSource
  $global:lateMutationDone = $false
  function global:Copy-Item {
    param([string]$LiteralPath, [string]$Destination, [switch]$Force)
    if (-not $global:lateMutationDone -and $LiteralPath -eq $global:lateMutationSource) {
      [IO.File]::AppendAllText($LiteralPath, 'mutation-after-source-hash')
      $global:lateMutationDone = $true
    }
    Microsoft.PowerShell.Management\Copy-Item -LiteralPath $LiteralPath -Destination $Destination -Force:$Force
  }
  try {
    $raceFailed = $false
    try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot (Join-Path $scratch 'copy-race') | Out-Host } catch { $raceFailed = $true }
    if (-not $raceFailed -or -not $global:lateMutationDone) { throw 'source mutation during copy was accepted' }
    Write-Output 'SOURCE_MUTATION_DURING_COPY=PASS'
  }
  finally {
    Remove-Item Function:\Copy-Item -ErrorAction SilentlyContinue
    Remove-Variable lateMutationSource,lateMutationDone -Scope Global -ErrorAction SilentlyContinue
  }

  $reparsePath = Join-Path (Join-Path $inputs[5] 'native') 'reparse-fixture'
  try {
    New-Item -ItemType Junction -Path $reparsePath -Target (Join-Path (Join-Path $inputs[5] 'native') $targetSpecs[5].Rid) | Out-Null
    $reparseFailed = $false
    try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot (Join-Path $scratch 'reparse') | Out-Host } catch { $reparseFailed = $true }
    if (-not $reparseFailed) { throw 'reparse-point artifact tree was accepted' }
    Write-Output 'REPARSE_TREE=PASS'
  }
  finally {
    if (Test-Path -LiteralPath $reparsePath) { Remove-Item -LiteralPath $reparsePath -Force }
  }

  $identityManifestPath = Join-Path $inputs[3] 'native/native-manifest.json'
  $identityOriginal = Get-Content -LiteralPath $identityManifestPath -Raw
  try {
    $unknown = $identityOriginal | ConvertFrom-Json
    $unknown.assets[0].rid = 'unknown-rid'
    $unknown | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $identityManifestPath -Encoding utf8NoBOM
    $unknownFailed = $false
    try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot (Join-Path $scratch 'unknown') | Out-Host } catch { $unknownFailed = $true }
    if (-not $unknownFailed) { throw 'unknown RID was accepted' }
    Write-Output 'UNKNOWN_RID=PASS'
  }
  finally {
    Set-Content -LiteralPath $identityManifestPath -Value $identityOriginal -Encoding utf8NoBOM
  }

  try {
    $wrongTarget = $identityOriginal | ConvertFrom-Json
    $wrongTarget.assets[0].rust_target = 'x86_64-pc-windows-msvc'
    $wrongTarget | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $identityManifestPath -Encoding utf8NoBOM
    $wrongTargetFailed = $false
    try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot (Join-Path $scratch 'wrong-target') | Out-Host } catch { $wrongTargetFailed = $true }
    if (-not $wrongTargetFailed) { throw 'wrong target identity was accepted' }
    Write-Output 'WRONG_TARGET=PASS'
  }
  finally {
    Set-Content -LiteralPath $identityManifestPath -Value $identityOriginal -Encoding utf8NoBOM
  }

  $staleManifestPath = Join-Path $inputs[1] 'native/native-manifest.json'
  $stale = Get-Content $staleManifestPath -Raw | ConvertFrom-Json
  $stale.source_revision = 'fedcba9876543210fedcba9876543210fedcba98'
  $stale | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $staleManifestPath -Encoding utf8NoBOM
  $staleFailed = $false
  try { & (Join-Path $scriptRoot 'merge-embedded-native-manifests.ps1') -InputRoot $inputs -OutputRoot (Join-Path $scratch 'stale') | Out-Host } catch { $staleFailed = $true }
  if (-not $staleFailed) { throw 'stale source revision was accepted' }
  Write-Output 'STALE_SOURCE=PASS'
}
finally {
  Remove-Item Function:\git -ErrorAction SilentlyContinue
  Remove-Item Function:\cargo -ErrorAction SilentlyContinue
  Remove-ExerciseScratch
}

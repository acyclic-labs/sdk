param([string]$Repository = 'C:\Users\varun\.codex\worktrees\rust-sdk-docs-source\sdk')
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'verify-dotnet-embedded-native-manifest.ps1')
$root = [IO.Path]::GetFullPath($Repository)
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-strict-validation-' + [guid]::NewGuid().ToString('N'))
function Assert-EmbeddedValidationScratchSafe {
  param([Parameter(Mandatory = $true)][string]$Path)
  $resolved = [IO.Path]::GetFullPath($Path)
  $tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
  $prefix = $tempRoot.TrimEnd([char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)) + [IO.Path]::DirectorySeparatorChar
  if ($resolved -eq $tempRoot -or -not $resolved.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to remove validation fixture outside the temporary root: $resolved"
  }
  if (Test-Path -LiteralPath $resolved) {
    $item = Get-Item -LiteralPath $resolved -Force
    if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
      throw "Refusing to remove a reparse-point validation fixture: $resolved"
    }
  }
  return $resolved
}
New-Item -ItemType Directory -Path $fixture | Out-Null
try {
  $closure = Get-EmbeddedRustSourceClosure -Repository $root
  $revision = (& git -C $root rev-parse HEAD).Trim()
  $lockHash = (Get-FileHash -LiteralPath (Join-Path $root 'rust/crates/sdk-embedded-prototype/Cargo.lock') -Algorithm SHA256).Hash.ToLowerInvariant()
  $specs = @(
    @{ Rid='win-x64'; File='acyclic_sdk_embedded_prototype.dll'; Target='x86_64-pc-windows-msvc' },
    @{ Rid='win-arm64'; File='acyclic_sdk_embedded_prototype.dll'; Target='aarch64-pc-windows-msvc' },
    @{ Rid='linux-x64'; File='libacyclic_sdk_embedded_prototype.so'; Target='x86_64-unknown-linux-gnu' },
    @{ Rid='linux-arm64'; File='libacyclic_sdk_embedded_prototype.so'; Target='aarch64-unknown-linux-gnu' },
    @{ Rid='linux-musl-x64'; File='libacyclic_sdk_embedded_prototype.so'; Target='x86_64-unknown-linux-musl' },
    @{ Rid='linux-musl-arm64'; File='libacyclic_sdk_embedded_prototype.so'; Target='aarch64-unknown-linux-musl' },
    @{ Rid='osx-x64'; File='libacyclic_sdk_embedded_prototype.dylib'; Target='x86_64-apple-darwin' },
    @{ Rid='osx-arm64'; File='libacyclic_sdk_embedded_prototype.dylib'; Target='aarch64-apple-darwin' }
  )
  $assets = foreach ($spec in $specs) {
    $directory = Join-Path $fixture $spec.Rid
    New-Item -ItemType Directory -Path $directory | Out-Null
    $asset = Join-Path $directory $spec.File
    [IO.File]::WriteAllBytes($asset, [byte[]](1, 2, 3, 4))
    $item = Get-Item -LiteralPath $asset
    [ordered]@{
      rust_target=$spec.Target; rid=$spec.Rid; file=$spec.File
      sha256=(Get-FileHash -LiteralPath $asset -Algorithm SHA256).Hash.ToLowerInvariant()
      bytes=$item.Length
    }
  }
  $manifest = [ordered]@{
    schema='acyclic.sdk.dotnet.embedded.native-manifest.v1'
    source_revision=$revision; source_revision_kind='git-oid'
    source_inputs=@($closure.Paths); source_inputs_sha256=$closure.Digest
    cargo_manifest='rust/crates/sdk-embedded-prototype/Cargo.toml'
    cargo_lock='rust/crates/sdk-embedded-prototype/Cargo.lock'
    cargo_lock_sha256=$lockHash
    cargo_command='cargo build --locked --release --manifest-path rust/crates/sdk-embedded-prototype/Cargo.toml --target <rust_target> --target-dir <target_dir>'
    assets=@($assets)
  }
  $manifestPath = Join-Path $fixture 'native-manifest.json'
  $manifest | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $manifestPath -Encoding utf8NoBOM
  $verified = Get-VerifiedEmbeddedNativeManifest -Repository $root -NativeRoot $fixture -ManifestPath $manifestPath
  Write-Output "VALID_EIGHT_RID=$($verified.Assets.Count)"
  $original = Get-Content -LiteralPath $manifestPath -Raw
  $mutations = @(
    @{ Name='MISSING_RID'; Pattern='exactly 8 records'; Mutate={ $d=$original|ConvertFrom-Json; $d.assets=@($d.assets|Where-Object rid -ne 'linux-musl-arm64'); $d|ConvertTo-Json -Depth 12|Set-Content -LiteralPath $manifestPath -Encoding utf8NoBOM } },
    @{ Name='SOURCE_DIGEST_TAMPER'; Pattern='closure digest'; Mutate={ $d=$original|ConvertFrom-Json; $d.source_inputs_sha256='0'*64; $d|ConvertTo-Json -Depth 12|Set-Content -LiteralPath $manifestPath -Encoding utf8NoBOM } },
    @{ Name='SOURCE_INPUT_SET_TAMPER'; Pattern='source input closure'; Mutate={ $d=$original|ConvertFrom-Json; $d.source_inputs=@($d.source_inputs | Select-Object -Skip 1); $d|ConvertTo-Json -Depth 12|Set-Content -LiteralPath $manifestPath -Encoding utf8NoBOM } },
    @{ Name='CARGO_COMMAND_TAMPER'; Pattern='Cargo command identity'; Mutate={ $d=$original|ConvertFrom-Json; $d.cargo_command='cargo build'; $d|ConvertTo-Json -Depth 12|Set-Content -LiteralPath $manifestPath -Encoding utf8NoBOM } },
    @{ Name='NATIVE_HASH_TAMPER'; Pattern='hash differs'; Mutate={ [IO.File]::WriteAllBytes((Join-Path (Join-Path $fixture 'linux-x64') 'libacyclic_sdk_embedded_prototype.so'), [byte[]](9,9,9,9)) } }
  )
  foreach ($case in $mutations) {
    try { & $case.Mutate; try { Get-VerifiedEmbeddedNativeManifest -Repository $root -NativeRoot $fixture -ManifestPath $manifestPath | Out-Null; throw "$($case.Name) accepted" } catch { if ($_.Exception.Message -notmatch $case.Pattern) { throw }; Write-Output "$($case.Name)=PASS" } }
    finally { Set-Content -LiteralPath $manifestPath -Value $original -Encoding utf8NoBOM; if ($case.Name -eq 'NATIVE_HASH_TAMPER') { [IO.File]::WriteAllBytes((Join-Path (Join-Path $fixture 'linux-x64') 'libacyclic_sdk_embedded_prototype.so'), [byte[]](1,2,3,4)) } }
  }
} finally { if (Test-Path -LiteralPath $fixture) { Remove-Item -LiteralPath (Assert-EmbeddedValidationScratchSafe -Path $fixture) -Recurse -Force } }

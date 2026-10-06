param(
  [string]$Root = "",
  [Parameter(Mandatory = $true)][string]$Output
)

$ErrorActionPreference = "Stop"
$repo = if ([string]::IsNullOrWhiteSpace($Root)) {
  [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
} elseif ([System.IO.Path]::IsPathRooted($Root)) {
  [System.IO.Path]::GetFullPath($Root)
} else {
  [System.IO.Path]::GetFullPath((Join-Path (Join-Path $PSScriptRoot "..") $Root))
}
$authority = if ([System.IO.Path]::IsPathRooted($Output)) {
  [System.IO.Path]::GetFullPath($Output)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repo $Output))
}

if (-not (Test-Path -LiteralPath $repo -PathType Container)) {
  throw "Rust source root does not exist: $repo"
}
$manifest = Join-Path $repo "rust/crates/sdk-contract-wire/Cargo.toml"
if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) {
  throw "Rust authority exporter is missing: $manifest"
}

# The authority output is producer-owned, but recursive deletion is allowed only
# inside this checkout. Reject reparse points so a stale output cannot redirect
# deletion outside the intended repository tree.
$repoFull = [System.IO.Path]::GetFullPath($repo).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
$targetRoot = [System.IO.Path]::GetFullPath((Join-Path $repoFull "target")).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
$authorityFull = [System.IO.Path]::GetFullPath($authority).TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
$targetPrefix = $targetRoot + [System.IO.Path]::DirectorySeparatorChar
if (-not $authorityFull.StartsWith($targetPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "Authority output must be a strict descendant of the repository target directory: $authorityFull"
}
$cursor = $authorityFull
while ($true) {
  if (Test-Path -LiteralPath $cursor) {
    $item = Get-Item -LiteralPath $cursor -Force
    if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
      throw "Authority output path contains a reparse point: $cursor"
    }
  }
  if ($cursor -ieq $targetRoot) { break }
  $parent = [System.IO.Path]::GetDirectoryName($cursor)
  if ([string]::IsNullOrWhiteSpace($parent) -or $parent -ieq $cursor) {
    throw "Authority output path escaped the source root: $authorityFull"
  }
  $cursor = $parent.TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
}
if (Test-Path -LiteralPath $authorityFull) {
  $authorityItem = Get-Item -LiteralPath $authorityFull -Force
  if (-not $authorityItem.PSIsContainer) { throw "Authority output is not a directory: $authorityFull" }
  Remove-Item -LiteralPath $authorityFull -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $authorityFull | Out-Null
$authority = $authorityFull

$cargo = Get-Command cargo -ErrorAction SilentlyContinue
if (-not $cargo) { throw "cargo is required to export the Rust contract authority" }
$generateArgs = @(
  "run", "--locked", "--manifest-path", $manifest,
  "--bin", "sdk-contract-wire", "--",
  "generate", "--root", $repo, "--out", $authority
)
& $cargo.Source @generateArgs
if ($LASTEXITCODE -ne 0) { throw "Rust contract authority generation failed with exit code $LASTEXITCODE" }

$checkArgs = @(
  "run", "--locked", "--manifest-path", $manifest,
  "--bin", "sdk-contract-wire", "--",
  "check", "--root", $repo, "--out", $authority
)
& $cargo.Source @checkArgs
if ($LASTEXITCODE -ne 0) { throw "Rust contract authority drift check failed with exit code $LASTEXITCODE" }

$manifestPath = Join-Path $authority "rust-authority.json"
$goldensPath = Join-Path $authority "rust-family-goldens.json"
foreach ($path in @($manifestPath, $goldensPath, (Join-Path $authority "stream/v2/stream.proto"))) {
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Rust authority export is incomplete: $path" }
}
Write-Output $authority

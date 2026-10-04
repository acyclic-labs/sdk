[CmdletBinding()]
param(
    [string] $Root,
    [string] $AuthorityRevision,
    [string] $WslUser = 'var',
    [string] $CacheRoot
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Resolve-Path (Join-Path (Join-Path (Join-Path $scriptDir '..') '..') '..')).Path
}
if ([string]::IsNullOrWhiteSpace($AuthorityRevision)) {
    $AuthorityRevision = (& git -C $Root rev-parse HEAD).Trim()
}
if ($AuthorityRevision -notmatch '^[0-9a-fA-F]{12,64}$') { throw "AuthorityRevision must be a git revision: $AuthorityRevision" }

$deps = Join-Path $scriptDir 'ada-deps'
$manifest = Join-Path $deps 'alire.toml'
$lock = Join-Path $deps 'alire.lock'
$expectedManifest = 'C4F9E4800E9C0E17647464E1FFEAAD92BBC3D66F4E1C273A777F1C5CD80E1EBF'
$expectedLock = '47F54234BB091627D2259135A4D0F3828B66A73DAF5B6FD4FEF894F35CB8A41B'
if ((Get-FileHash -LiteralPath $manifest -Algorithm SHA256).Hash -ne $expectedManifest) { throw 'Ada manifest checksum mismatch' }
if ((Get-FileHash -LiteralPath $lock -Algorithm SHA256).Hash -ne $expectedLock) { throw 'Ada lock checksum mismatch' }

function Convert-ToWslPath([string] $WindowsPath) {
    $resolved = (Resolve-Path -LiteralPath $WindowsPath).Path
    if ($resolved -notmatch '^(?<drive>[A-Za-z]):\\(?<rest>.*)$') { throw "Expected an absolute Windows path: $resolved" }
    return (('/mnt/' + $Matches.drive.ToLowerInvariant() + '/' + $Matches.rest.Replace('\', '/')))
}

$rootWsl = Convert-ToWslPath $Root
$manifestWsl = Convert-ToWslPath $manifest
$lockWsl = Convert-ToWslPath $lock
$revision = $AuthorityRevision.ToLowerInvariant()
if ([string]::IsNullOrWhiteSpace($CacheRoot)) {
    $CacheRoot = "/home/$WslUser/.cache/acyclic/sdk-authority-$revision/ada"
}

# Root is used only to read the protected, already-pinned Alire cache. The copy
# is owned by the active WSL user, so downstream builds never need root access.
$bash = @'
set -eu
cache="$1"
user="$2"
manifest="$3"
lock="$4"
authority="$5"
source_project="/opt/acyclic-runtimes/ada-deps-pinned/acyclic_ada_deps"
source_alire="/root/.local/share/alire"
mkdir -p "$cache/alire" "$cache/project"
cp -a "$source_alire/builds" "$cache/alire/"
cp -a "$source_alire/toolchains" "$cache/alire/"
cp -a "$source_alire/releases" "$cache/alire/"
cp -a "$source_project/." "$cache/project/"
cp "$manifest" "$cache/project/alire.toml"
cp "$lock" "$cache/project/alire/alire.lock"

# Rewrite only the copied environment. Protected source paths remain untouched.
sed -e "s#/root/.local/share/alire#$cache/alire#g" \
    -e "s#/opt/acyclic-runtimes/ada-deps-pinned/acyclic_ada_deps#$cache/project#g" \
    "$source_project/alire/ada_deps_env.txt" > "$cache/project/alire/ada_deps_env.txt"
chown -R "$user:$user" "$cache"
find "$cache" -type d -exec chmod u+rwx,go+rx {} +
find "$cache" -type f -exec chmod u+rw,go+r {} +

manifest_hash=$(sha256sum "$manifest" | awk '{print toupper($1)}')
lock_hash=$(sha256sum "$lock" | awk '{print toupper($1)}')
cache_hash=$(find "$cache" -type f -printf '%P\n' | sort | while read -r p; do sha256sum "$cache/$p"; done | sha256sum | awk '{print toupper($1)}')
cat > "$cache/ada-user-cache-receipt.json" <<JSON
{
  "schema": "acyclic.sdk.openapi.ada-user-cache.v1",
  "status": "pass",
  "authority_revision": "$authority",
  "manifest_sha256": "$manifest_hash",
  "lock_sha256": "$lock_hash",
  "cache_root": "$cache",
  "environment": "$cache/project/alire/ada_deps_env.txt",
  "cache_content_sha256": "$cache_hash",
  "source": {
    "dependency_project": "$source_project",
    "alire_cache": "$source_alire",
    "copy_mode": "root-readable-pinned-cache-to-user-owned-copy"
  }
}
JSON
printf '%s\n' "$cache" "$cache/project/alire/ada_deps_env.txt" "$cache/ada-user-cache-receipt.json"
'@
$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($bash))
$result = & wsl.exe -u root -e bash -lc "echo $encoded | base64 -d | bash -s -- '$CacheRoot' '$WslUser' '$manifestWsl' '$lockWsl' '$revision'"
if ($LASTEXITCODE -ne 0) { throw 'Pinned Ada user cache provisioning failed' }
$result | ForEach-Object { Write-Output $_ }

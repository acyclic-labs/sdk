[CmdletBinding()]
param(
    [string] $Root,
    [string] $RuntimeRoot = '/opt/acyclic-runtimes/ada-deps-pinned'
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Resolve-Path (Join-Path (Join-Path (Join-Path $scriptDir '..') '..') '..')).Path
}
$deps = Join-Path $scriptDir 'ada-deps'
$manifest = Join-Path $deps 'alire.toml'
$lock = Join-Path $deps 'alire.lock'
$expectedManifest = 'C4F9E4800E9C0E17647464E1FFEAAD92BBC3D66F4E1C273A777F1C5CD80E1EBF'
$expectedLock = '47F54234BB091627D2259135A4D0F3828B66A73DAF5B6FD4FEF894F35CB8A41B'
foreach ($file in @($manifest, $lock)) {
    if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { throw "Missing Ada dependency evidence: $file" }
}
if ((Get-FileHash -LiteralPath $manifest -Algorithm SHA256).Hash -ne $expectedManifest) { throw 'Ada manifest checksum mismatch' }
if ((Get-FileHash -LiteralPath $lock -Algorithm SHA256).Hash -ne $expectedLock) { throw 'Ada lock checksum mismatch' }

function Convert-ToWslPath([string] $WindowsPath) {
    $resolved = (Resolve-Path -LiteralPath $WindowsPath).Path
    if ($resolved -notmatch '^(?<drive>[A-Za-z]):\\(?<rest>.*)$') { throw "Expected an absolute Windows path: $resolved" }
    return (('/mnt/' + $Matches.drive.ToLowerInvariant() + '/' + $Matches.rest.Replace('\', '/')))
}

$manifestWsl = Convert-ToWslPath $manifest
$lockWsl = Convert-ToWslPath $lock
$receiptWsl = "$RuntimeRoot/ada-deps-receipt.json"
$bash = @'
set -eu
runtime_root="$1"
manifest="$2"
lock="$3"
alr="/opt/acyclic-runtimes/alire-2.1.1/bin/alr"
project="$runtime_root/acyclic_ada_deps"
mkdir -p "$project/alire"
cp "$manifest" "$project/alire.toml"
cp "$lock" "$project/alire/alire.lock"
cat > "$project/acyclic_ada_deps.gpr" <<'GPR'
with "utilada_sys";
with "utilada_xml";
with "utilada_curl";
with "security";
project Acyclic_Ada_Deps is
   for Source_Dirs use ();
end Acyclic_Ada_Deps;
GPR
cd "$project"
"$alr" -n build
env_file="$project/alire/ada_deps_env.txt"
"$alr" -n printenv > "$env_file"
cat > "$runtime_root/ada-deps-receipt.json" <<JSON
{
  "schema": "acyclic.sdk.openapi.ada-dependencies.v1",
  "alire": "2.1.1",
  "manifest": "ada-deps/alire.toml",
  "lock": "ada-deps/alire.lock",
  "runtime_root": "$runtime_root",
  "project": "$project",
  "env_file": "$env_file",
  "status": "pass"
}
JSON
printf '%s\n' "$project" "$env_file" "$runtime_root/ada-deps-receipt.json"
'@
$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($bash))
$result = & wsl.exe -u root -e bash -lc "echo $encoded | base64 -d | bash -s -- '$RuntimeRoot' '$manifestWsl' '$lockWsl'"
if ($LASTEXITCODE -ne 0) { throw 'Pinned Ada dependency bootstrap failed' }
$result | ForEach-Object { Write-Output $_ }

[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)][string]$PackageRoot,
  [Parameter(Mandatory = $true)][ValidateSet('win-x64', 'win-arm64')][string]$Rid,
  [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')),
  [string]$BuildDirectory = (Join-Path ([IO.Path]::GetTempPath()) "acyclic-embedded-abi-$Rid")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Invoke-Checked([string]$Command, [string[]]$Arguments) {
  & $Command @Arguments
  if ($LASTEXITCODE -ne 0) { throw "$Command failed with exit code $LASTEXITCODE" }
}

$repo = [IO.Path]::GetFullPath($Root)
$package = [IO.Path]::GetFullPath($PackageRoot)
$build = [IO.Path]::GetFullPath($BuildDirectory)
$ridRoot = Join-Path $package $Rid
$runtimeName = 'acyclic_sdk_embedded_prototype.dll'
$runtime = Join-Path $ridRoot $runtimeName
$header = Join-Path $package 'abi/acyclic_embedded_prototype.h'
$import = Join-Path $ridRoot 'acyclic_sdk_embedded_prototype.dll.lib'
foreach ($required in @($runtime, $header, $import)) {
  if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Installed ABI input is missing: $required" }
}
if ($build.StartsWith($repo + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or $build.Equals($repo, [StringComparison]::OrdinalIgnoreCase)) {
  throw "BuildDirectory must be outside the source checkout: $build"
}
New-Item -ItemType Directory -Force -Path $build | Out-Null
$consumerSource = Join-Path $repo 'cpp/embedded-consumer'
$cmakeBuild = Join-Path $build 'consumer'
$prefix = Join-Path $build 'prefix'
$installedBuild = Join-Path $build 'installed-consumer'
$clang = (Get-Command clang++.exe -ErrorAction Stop).Source
$cc = (Get-Command clang.exe -ErrorAction Stop).Source

Invoke-Checked 'cmake' @('-S', $consumerSource, '-B', $cmakeBuild, '-G', 'Ninja', "-DCMAKE_CXX_COMPILER=$($clang.Replace('\', '/'))", "-DACYCLIC_EMBEDDED_ROOT=$($ridRoot.Replace('\', '/'))", "-DACYCLIC_EMBEDDED_HEADER=$($header.Replace('\', '/'))")
Invoke-Checked 'cmake' @('--build', $cmakeBuild)
Invoke-Checked 'ctest' @('--test-dir', $cmakeBuild, '--output-on-failure')
Invoke-Checked 'cmake' @('--install', $cmakeBuild, '--prefix', $prefix)

Invoke-Checked 'cmake' @('-S', (Join-Path $consumerSource 'install-consumer'), '-B', $installedBuild, '-G', 'Ninja', "-DCMAKE_CXX_COMPILER=$($clang.Replace('\', '/'))", "-DCMAKE_PREFIX_PATH=$($prefix.Replace('\', '/'))")
Invoke-Checked 'cmake' @('--build', $installedBuild)
$env:PATH = "$(Join-Path $prefix 'bin');$env:PATH"
Invoke-Checked (Join-Path $installedBuild 'acyclic_cpp_installed_consumer.exe') @()

$cConsumer = Join-Path $build 'c-consumer.exe'
Invoke-Checked $cc @('-std=c11', "-I$(Join-Path $prefix 'include')", (Join-Path $repo 'rust/crates/sdk-embedded-prototype/tests/c_consumer.c'), (Join-Path $prefix 'lib/acyclic_sdk_embedded_prototype.dll.lib'), '-o', $cConsumer)
$env:PATH = "$(Join-Path $prefix 'bin');$env:PATH"
Invoke-Checked $cConsumer @()
Invoke-Checked 'python' @((Join-Path $repo 'rust/crates/sdk-embedded-prototype/tests/python_consumer.py'), (Join-Path $prefix 'bin/acyclic_sdk_embedded_prototype.dll'))

$receipt = [ordered]@{
  schema = 'acyclic.sdk.embedded.abi-installed.v1'
  status = 'passed'
  rid = $Rid
  source_revision = (& git -C $repo rev-parse HEAD).Trim()
  package_root = 'embedded-native-merged'
  runtime = "$Rid/$runtimeName"
  runtime_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $runtime).Hash.ToLowerInvariant()
  header = 'abi/acyclic_embedded_prototype.h'
  header_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $header).Hash.ToLowerInvariant()
  consumers = @('installed C++ CMake consumer', 'installed C consumer', 'installed Python consumer')
}
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $build 'abi-installed-receipt.json') -Encoding utf8NoBOM
Write-Output "Installed embedded ABI consumers passed for $Rid"

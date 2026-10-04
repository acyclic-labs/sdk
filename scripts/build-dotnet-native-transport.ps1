param(
  [string]$Output = "target/native/sdk-dotnet-transport",
  [string]$Target = "x86_64-pc-windows-msvc",
  [switch]$All
)
$ErrorActionPreference = "Stop"
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$out = if ([System.IO.Path]::IsPathRooted($Output)) { [System.IO.Path]::GetFullPath($Output) } else { [System.IO.Path]::GetFullPath((Join-Path $root $Output)) }
$targetMap = @{
  "x86_64-pc-windows-msvc" = @{ Rid = "win-x64"; File = "sdk_dotnet_transport.dll" }
  "aarch64-pc-windows-msvc" = @{ Rid = "win-arm64"; File = "sdk_dotnet_transport.dll" }
  "x86_64-unknown-linux-gnu" = @{ Rid = "linux-x64"; File = "libsdk_dotnet_transport.so" }
  "x86_64-unknown-linux-musl" = @{ Rid = "linux-musl-x64"; File = "libsdk_dotnet_transport.so" }
  "aarch64-unknown-linux-gnu" = @{ Rid = "linux-arm64"; File = "libsdk_dotnet_transport.so" }
  "aarch64-unknown-linux-musl" = @{ Rid = "linux-musl-arm64"; File = "libsdk_dotnet_transport.so" }
  "x86_64-apple-darwin" = @{ Rid = "osx-x64"; File = "libsdk_dotnet_transport.dylib" }
  "aarch64-apple-darwin" = @{ Rid = "osx-arm64"; File = "libsdk_dotnet_transport.dylib" }
}

$targets = if ($All) { @($targetMap.Keys | Sort-Object) } else { @($Target) }
$targetDir = Join-Path $root "target/dotnet-transport"
foreach ($targetName in $targets) {
  if (-not $targetMap.ContainsKey($targetName)) {
    throw "Unsupported .NET native target '$targetName'. Supported targets: $($targetMap.Keys -join ', ')"
  }
  & cargo build --manifest-path (Join-Path $root "Cargo.toml") -p sdk-dotnet-transport --release --target $targetName --target-dir $targetDir
  if ($LASTEXITCODE -ne 0) { throw "Rust native .NET transport build failed for $targetName : $LASTEXITCODE" }
  $spec = $targetMap[$targetName]
  $binary = Join-Path $targetDir "$targetName/release/$($spec.File)"
  if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) { throw "Rust native transport did not emit $binary" }
  $destination = Join-Path $out $spec.Rid
  New-Item -ItemType Directory -Force -Path $destination | Out-Null
  $installed = Join-Path $destination $spec.File
  Copy-Item -LiteralPath $binary -Destination $installed -Force
  $hash = Get-FileHash -Algorithm SHA256 -LiteralPath $installed
  [PSCustomObject]@{ Target = $targetName; Rid = $spec.Rid; Path = $installed; SHA256 = $hash.Hash; Bytes = (Get-Item $installed).Length }
}


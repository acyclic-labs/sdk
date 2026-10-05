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
  $savedRustFlags = $env:RUSTFLAGS
  $savedLinkerVariable = $null
  $savedLinkerValue = $null
  try {
    if ($targetName -like "*-unknown-linux-musl") {
      # musl's default Rust target enables a static CRT, which suppresses
      # cdylib output. .NET RID assets must be dynamically loadable, so use
      # the pinned musl linker with a dynamic musl CRT exactly as the embedded
      # producer does.
      # Ubuntu's musl wrapper exposes libgcc_s only through its private specs;
      # link compiler support statically while keeping musl itself dynamic.
      $searchLine = (& musl-gcc -print-search-dirs 2>$null | Where-Object { $_ -like 'libraries: *' } | Select-Object -First 1)
      $searchRoots = @()
      if ($searchLine) {
        $searchRoots = @(($searchLine -replace '^libraries:\s*=', '').Split([IO.Path]::PathSeparator) | Where-Object { $_ })
      }
      $gccLibraryDirectory = $searchRoots |
        Where-Object { Test-Path -LiteralPath (Join-Path $_ 'libgcc.a') -PathType Leaf } |
        Select-Object -First 1
      if (-not $gccLibraryDirectory) {
        throw "The musl GCC installation did not expose libgcc.a for $targetName"
      }
      $env:RUSTFLAGS = (($savedRustFlags + " -C target-feature=-crt-static -C link-arg=-static-libgcc -C link-arg=-L$gccLibraryDirectory").Trim())
      $targetEnv = $targetName.ToUpperInvariant().Replace('-', '_')
      $savedLinkerVariable = "CARGO_TARGET_${targetEnv}_LINKER"
      $savedLinkerValue = [Environment]::GetEnvironmentVariable($savedLinkerVariable, "Process")
      [Environment]::SetEnvironmentVariable($savedLinkerVariable, "musl-gcc", "Process")
    }
    & cargo build --manifest-path (Join-Path $root "Cargo.toml") -p sdk-dotnet-transport --release --target $targetName --target-dir $targetDir
  } finally {
    $env:RUSTFLAGS = $savedRustFlags
    if ($savedLinkerVariable) {
      [Environment]::SetEnvironmentVariable($savedLinkerVariable, $savedLinkerValue, "Process")
    }
  }
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


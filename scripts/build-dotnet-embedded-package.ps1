param(
  [string]$Output = "target/dotnet-embedded",
  [string]$Target = "",
  [switch]$All,
  [string]$NativeRoot = "",
  [switch]$NativeOnly,
  [switch]$PackageOnly
)

$ErrorActionPreference = "Stop"
$root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$out = if ([System.IO.Path]::IsPathRooted($Output)) { [System.IO.Path]::GetFullPath($Output) } else { [System.IO.Path]::GetFullPath((Join-Path $root $Output)) }
$targetMap = [ordered]@{
  "x86_64-pc-windows-msvc" = @{ Rid = "win-x64"; File = "acyclic_sdk_embedded_prototype.dll" }
  "aarch64-pc-windows-msvc" = @{ Rid = "win-arm64"; File = "acyclic_sdk_embedded_prototype.dll" }
  "x86_64-unknown-linux-gnu" = @{ Rid = "linux-x64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "aarch64-unknown-linux-gnu" = @{ Rid = "linux-arm64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "x86_64-unknown-linux-musl" = @{ Rid = "linux-musl-x64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "aarch64-unknown-linux-musl" = @{ Rid = "linux-musl-arm64"; File = "libacyclic_sdk_embedded_prototype.so" }
  "x86_64-apple-darwin" = @{ Rid = "osx-x64"; File = "libacyclic_sdk_embedded_prototype.dylib" }
  "aarch64-apple-darwin" = @{ Rid = "osx-arm64"; File = "libacyclic_sdk_embedded_prototype.dylib" }
}

$nativeRoot = if ([string]::IsNullOrWhiteSpace($NativeRoot)) {
  Join-Path $out "native"
} elseif ([System.IO.Path]::IsPathRooted($NativeRoot)) {
  [System.IO.Path]::GetFullPath($NativeRoot)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $root $NativeRoot))
}
New-Item -ItemType Directory -Force -Path $nativeRoot | Out-Null

if ($PackageOnly) {
  $targets = @($targetMap.Keys)
} elseif ([string]::IsNullOrWhiteSpace($Target) -and -not $All) {
  $Target = (& rustc -vV | Select-String '^host: ' | ForEach-Object { $_.Line.Substring(6).Trim() })
  $targets = @($Target)
} elseif ($All) {
  $targets = @($targetMap.Keys)
} else {
  $targets = @($Target)
}
if ($targets.Count -eq 0) { throw "No Rust target was selected" }

function Invoke-EmbeddedRustBuild {
  param(
    [Parameter(Mandatory = $true)][string]$TargetName,
    [Parameter(Mandatory = $true)][string]$Manifest,
    [Parameter(Mandatory = $true)][string]$TargetDirectory
  )

  $linkerVariable = "CARGO_TARGET_$($TargetName.ToUpperInvariant().Replace('-', '_'))_LINKER"
  $savedRustFlags = $env:RUSTFLAGS
  $savedLinker = [Environment]::GetEnvironmentVariable($linkerVariable, "Process")
  $changedMuslEnvironment = $false
  try {
    if ($TargetName -like "*-unknown-linux-musl") {
      $compilerNames = if ($TargetName.StartsWith("aarch64-")) {
        @("aarch64-linux-musl-gcc", "musl-gcc")
      } else {
        @("x86_64-linux-musl-gcc", "musl-gcc")
      }
      $compiler = $null
      foreach ($name in $compilerNames) {
        $candidate = Get-Command $name -ErrorAction SilentlyContinue
        if ($candidate) { $compiler = $candidate.Source; break }
      }
      if (-not $compiler) {
        throw "No pinned/native musl linker is available for $TargetName. Use the pinned Alpine producer lane."
      }
      $env:RUSTFLAGS = (($savedRustFlags + " -C target-feature=-crt-static").Trim())
      [Environment]::SetEnvironmentVariable($linkerVariable, $compiler, "Process")
      $changedMuslEnvironment = $true
    }
    & cargo build --locked --release --manifest-path $Manifest --target $TargetName --target-dir $TargetDirectory
    if ($LASTEXITCODE -ne 0) { throw "Rust embedded ABI build failed for $TargetName : $LASTEXITCODE" }
  } finally {
    if ($changedMuslEnvironment) {
      $env:RUSTFLAGS = $savedRustFlags
      [Environment]::SetEnvironmentVariable($linkerVariable, $savedLinker, "Process")
    }
  }
}

$manifest = Join-Path $root "rust/crates/sdk-embedded-prototype/Cargo.toml"
$targetDir = Join-Path $out "rust-target"
$records = @()

foreach ($targetName in $targets) {
  if (-not $targetMap.Contains($targetName)) {
    throw "Unsupported embedded .NET target '$targetName'. Supported targets: $($targetMap.Keys -join ', ')"
  }
  $spec = $targetMap[$targetName]
  $installed = Join-Path (Join-Path $nativeRoot $spec.Rid) $spec.File
  if ($PackageOnly) {
    if (-not (Test-Path -LiteralPath $installed -PathType Leaf)) {
      throw "PackageOnly requires the native asset for $targetName at $installed"
    }
  } else {
    Invoke-EmbeddedRustBuild -TargetName $targetName -Manifest $manifest -TargetDirectory $targetDir
    $binary = Join-Path $targetDir "$targetName/release/$($spec.File)"
    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
      throw "Rust embedded ABI did not emit $binary"
    }
    $destination = Join-Path $nativeRoot $spec.Rid
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    $installed = Join-Path $destination $spec.File
    Copy-Item -LiteralPath $binary -Destination $installed -Force
  }
  $records += [PSCustomObject]@{
    rust_target = $targetName
    rid = $spec.Rid
    file = $spec.File
    sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $installed).Hash.ToLowerInvariant()
    bytes = (Get-Item -LiteralPath $installed).Length
  }
}

$records | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $nativeRoot "native-manifest.json") -Encoding utf8NoBOM
if ($NativeOnly) {
  Write-Output "staged native embedded assets under $nativeRoot"
  exit 0
}

$dotnetVersion = '8.0.425'
$dotnetCandidates = @()
$dotnetCommand = Get-Command dotnet -ErrorAction SilentlyContinue
if ($dotnetCommand) { $dotnetCandidates += $dotnetCommand.Source }
$dotnetCandidates += @(
  (Join-Path $root ".toolchains/dotnet/$dotnetVersion/dotnet.exe"),
  (Join-Path $root ".tmp-dotnet-sdk-official-$dotnetVersion/dotnet.exe"),
  "Q:\sdk\.tmp-dotnet-sdk-official-$dotnetVersion\dotnet.exe",
  (Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "acyclic/tool-cache/dotnet-sdk-$dotnetVersion-win-x64/dotnet.exe")
)
$dotnetPath = $null
foreach ($candidate in ($dotnetCandidates | Select-Object -Unique)) {
  if (-not $candidate -or -not (Test-Path -LiteralPath $candidate -PathType Leaf)) { continue }
  $actualVersion = (& $candidate --version 2>$null | Select-Object -First 1).Trim()
  if ($actualVersion -eq $dotnetVersion) {
    $dotnetPath = $candidate
    break
  }
}
if (-not $dotnetPath) { throw "Pinned .NET SDK $dotnetVersion is unavailable" }

$packageOutput = Join-Path $out "packages"
$obj = Join-Path $out "obj"
$build = Join-Path $out "build"
New-Item -ItemType Directory -Force -Path $packageOutput, $obj, $build | Out-Null
$project = Join-Path $root "dotnet/Acyclic.Sdk.Embedded.csproj"
& $dotnetPath pack $project --configuration Release --nologo \`
  "-p:EmbeddedNativeRoot=$nativeRoot" \`
  "-p:BaseOutputPath=$build\" \`
  "-p:BaseIntermediateOutputPath=$obj\" \`
  "-p:PackageOutputPath=$packageOutput\" \`
  '-p:ContinuousIntegrationBuild=true' \`
  '-p:Deterministic=true' \`
  '-p:DeterministicSourcePaths=true'
if ($LASTEXITCODE -ne 0) { throw "Embedded .NET package failed with exit code $LASTEXITCODE" }

$package = Join-Path $packageOutput "Acyclic.Sdk.Embedded.0.2.0-alpha.1.nupkg"
if (-not (Test-Path -LiteralPath $package -PathType Leaf)) { throw "Embedded package was not produced: $package" }
$manifestOutput = [ordered]@{
  schema = "acyclic.sdk.dotnet.embedded.producer-output.v2"
  source = "rust/crates/sdk-embedded-prototype"
  package = "Acyclic.Sdk.Embedded.0.2.0-alpha.1.nupkg"
  package_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $package).Hash.ToLowerInvariant()
  dotnet_sdk = $dotnetVersion
  targets = @($targets)
  native_root = "native"
  loader = "P/Invoke acyclic_sdk_embedded_prototype; NuGet RID asset selection"
  operations = @("append", "read", "follow", "cancel", "owned-buffers", "provider-recovery")
  registry_published = $false
}
$manifestOutput | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $out "producer-output.json") -Encoding utf8NoBOM
Write-Output "staged embedded .NET package: $package"

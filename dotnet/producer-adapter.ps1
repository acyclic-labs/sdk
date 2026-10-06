param(
  [Parameter(Mandatory = $true)][string]$SourceRoot,
  [Parameter(Mandatory = $true)][string]$Authority,
  [Parameter(Mandatory = $true)][string]$Request,
  [Parameter(Mandatory = $true)][string]$Output
)

$ErrorActionPreference = 'Stop'

$DotnetVersion = '8.0.425'
$DotnetSha256 = '111da7b604cd196b49167cfeecdaef4034b58cc7de71a91cbf9e162c9e8b0931'

function Resolve-PinnedDotnet([string] $SourceRoot) {
  $candidates = @(
    (Join-Path $SourceRoot ".toolchains/dotnet/$DotnetVersion/dotnet.exe"),
    (Join-Path $SourceRoot ".tmp-dotnet-sdk-official-$DotnetVersion/dotnet.exe"),
    "Q:\sdk\.tmp-dotnet-sdk-official-$DotnetVersion\dotnet.exe",
    (Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "acyclic/tool-cache/dotnet-sdk-$DotnetVersion-win-x64/dotnet.exe")
  )
  $command = Get-Command dotnet -ErrorAction SilentlyContinue
  if ($command) { $candidates += $command.Source }
  foreach ($candidate in ($candidates | Select-Object -Unique)) {
    if (-not $candidate -or -not (Test-Path -LiteralPath $candidate -PathType Leaf)) { continue }
    $resolved = (Resolve-Path -LiteralPath $candidate).Path
    $actual = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -ne $DotnetSha256) { continue }
    $version = (& $resolved --version 2>$null | Select-Object -First 1).Trim()
    if ($version -ne $DotnetVersion) {
      throw "Pinned .NET SDK checksum matched but version was '$version' (expected $DotnetVersion): $resolved"
    }
    return [pscustomobject]@{ Path = $resolved; Sha256 = $actual; Version = $version }
  }
  throw "Pinned .NET SDK $DotnetVersion is unavailable; provision the official SDK in the repository or acyclic tool cache"
}

$SourceRoot = [System.IO.Path]::GetFullPath($SourceRoot)
$Authority = [System.IO.Path]::GetFullPath($Authority)
$Request = [System.IO.Path]::GetFullPath($Request)

foreach ($path in @($SourceRoot, $Authority, $Request)) {
  if (-not (Test-Path -LiteralPath $path)) {
    throw "Required producer input is missing: $path"
  }
}
$manifest = Join-Path $Authority 'rust-authority.json'
if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) {
  throw "Rust authority manifest is missing: $manifest"
}
$authorityDocument = Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json
$authoritySourceRevision = [string]$authorityDocument.source_revision
$authoritySourceRevisionKind = [string]$authorityDocument.source_revision_kind
if ([string]::IsNullOrWhiteSpace($authoritySourceRevision) -or $authoritySourceRevision -eq 'unknown') {
  throw 'Rust authority manifest must carry a non-empty source model revision'
}
if ($authoritySourceRevisionKind -ne 'rust-model-sha256') {
  throw "Rust authority manifest has unsupported source revision kind: $authoritySourceRevisionKind"
}
$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
$sourceRevision = [string]$requestDocument.source.revision
$sourceDigest = [string]$requestDocument.source.digest
if ([string]::IsNullOrWhiteSpace($sourceRevision) -or $sourceRevision -eq 'unknown') {
  throw 'Generation request must carry the source Git revision'
}
if ([string]::IsNullOrWhiteSpace($sourceDigest) -or $sourceDigest -eq 'unknown') {
  throw 'Generation request must carry the source digest'
}

$output = [System.IO.Path]::GetFullPath($Output)
$workspace = Join-Path $output 'workspace'
$workspaceDotnet = Join-Path $workspace 'dotnet'
$build = Join-Path $output 'build'
$obj = Join-Path $output 'obj'
$packages = Join-Path $output 'packages'
$nuget = Join-Path $output '.nuget'
$null = New-Item -ItemType Directory -Force -Path $output, $workspace, $build, $obj, $packages, $nuget
# PathMap is owned by the project so its semicolon/comma escaping is handled by
# MSBuild.  Passing a multi-root value as a `dotnet pack` command-line property
# is parsed as a second property on Windows and makes the compiler reject the
# invocation before it can emit diagnostics.

& robocopy (Join-Path $SourceRoot 'dotnet') $workspaceDotnet /E /XD target obj bin consumer /NFL /NDL /NJH /NJS /NC /NS | Out-Null
if ($LASTEXITCODE -gt 7) {
  throw "Could not stage the .NET producer source tree (robocopy exit code $LASTEXITCODE)."
}
foreach ($name in @('LICENSE', 'NOTICE')) {
  Copy-Item -LiteralPath (Join-Path $SourceRoot $name) -Destination (Join-Path $workspace $name) -Force
}

$dotnetTool = Resolve-PinnedDotnet $SourceRoot
$dotnet = $dotnetTool.Path
$project = Join-Path $workspaceDotnet 'Acyclic.Sdk.Transport.csproj'
if (-not (Test-Path -LiteralPath $project -PathType Leaf)) {
  throw " .NET producer project is missing: $project"
}

& $dotnet pack $project '--configuration' 'Release' '--nologo' `
  "-p:SchemaRoot=$Authority" `
  "-p:BaseOutputPath=$build\" `
  "-p:BaseIntermediateOutputPath=$obj\" `
  "-p:PackageOutputPath=$packages\" `
  "-p:RestorePackagesPath=$nuget\" `
  '-p:ContinuousIntegrationBuild=true' `
  '-p:Deterministic=true' `
  '-p:DeterministicSourcePaths=true'
if ($LASTEXITCODE -ne 0) {
  throw "Pinned .NET producer failed with exit code $LASTEXITCODE."
}

$nupkg = Join-Path $packages 'Acyclic.Sdk.Transport.0.2.0-alpha.1.nupkg'
if (-not (Test-Path -LiteralPath $nupkg -PathType Leaf)) {
  throw ".NET producer completed without its installable package: $nupkg"
}
$staged = Join-Path $output 'Acyclic.Sdk.Transport.0.2.0-alpha.1.nupkg'
Copy-Item -LiteralPath $nupkg -Destination $staged -Force
$requestHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $Request).Hash
$authorityHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $manifest).Hash
@{
  schema = 'acyclic.sdk.dotnet.producer-output.v1'
  source_revision = $sourceRevision
  source_digest = $sourceDigest
  authority_source_revision = $authoritySourceRevision
  authority_source_revision_kind = $authoritySourceRevisionKind
  authority_manifest = 'rust-authority.json'
  authority_sha256 = $authorityHash
  request = [System.IO.Path]::GetFileName($Request)
  request_sha256 = $requestHash
  generator = 'Grpc.Tools + Grpc.Net.Client'
  generator_versions = @{
    google_protobuf = '3.31.1'
    grpc_tools = '2.71.0'
    grpc_net_client = '2.71.0'
    dotnet_sdk = $dotnetTool.Version
  }
  dotnet_path = $dotnetTool.Path
  dotnet_sha256 = $dotnetTool.Sha256
  artifact = 'Acyclic.Sdk.Transport.0.2.0-alpha.1.nupkg'
  artifact_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $staged).Hash
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $output 'producer-output.json') -Encoding utf8
Write-Output "staged .NET package: Acyclic.Sdk.Transport.0.2.0-alpha.1.nupkg"

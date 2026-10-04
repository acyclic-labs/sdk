param(
  [Parameter(Mandatory = $true)][string]$SourceRoot,
  [Parameter(Mandatory = $true)][string]$Authority,
  [Parameter(Mandatory = $true)][string]$Request,
  [Parameter(Mandatory = $true)][string]$Output
)

$ErrorActionPreference = 'Stop'

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

$dotnet = if ($env:SDK_DOTNET_BIN) { $env:SDK_DOTNET_BIN } else { 'dotnet' }
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
  authority_manifest = 'rust-authority.json'
  authority_sha256 = $authorityHash
  request = [System.IO.Path]::GetFileName($Request)
  request_sha256 = $requestHash
  generator = 'Grpc.Tools + Grpc.Net.Client'
  generator_versions = @{
    google_protobuf = '3.31.1'
    grpc_tools = '2.71.0'
    grpc_net_client = '2.71.0'
  }
  artifact = 'Acyclic.Sdk.Transport.0.2.0-alpha.1.nupkg'
  artifact_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $staged).Hash
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $output 'producer-output.json') -Encoding utf8
Write-Output "staged .NET package: Acyclic.Sdk.Transport.0.2.0-alpha.1.nupkg"

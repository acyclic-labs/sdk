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

if (-not (Test-Path -LiteralPath $SourceRoot -PathType Container)) {
  throw "Rust source root is missing: $SourceRoot"
}
if (-not (Test-Path -LiteralPath $Authority -PathType Container)) {
  throw "Rust authority directory is missing: $Authority"
}
if (-not (Test-Path -LiteralPath $Request -PathType Leaf)) {
  throw "Generation request is missing: $Request"
}
$manifest = Join-Path $Authority 'rust-authority.json'
if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) {
  throw "Rust authority manifest is missing: $manifest"
}

$output = [System.IO.Path]::GetFullPath($Output)
$sourcePrefix = $SourceRoot.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
$outputPrefix = $output.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
if ($output.Equals($SourceRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
    $output.StartsWith($sourcePrefix, [System.StringComparison]::OrdinalIgnoreCase) -or
    $SourceRoot.StartsWith($outputPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "The .NET producer output must be disjoint from the Rust source root: $output"
}
$workspace = Join-Path $output 'workspace'
$workspaceDotnet = Join-Path $workspace 'dotnet'
$build = Join-Path $output 'build'
$obj = Join-Path $output 'obj'
$packages = Join-Path $output 'packages'
$nuget = Join-Path $output '.nuget'
$null = New-Item -ItemType Directory -Force -Path $output, $workspace, $workspaceDotnet, $build, $obj, $packages, $nuget

if (Test-Path -LiteralPath $workspaceDotnet) {
  Get-ChildItem -LiteralPath $workspaceDotnet -Force | Remove-Item -Recurse -Force
}
$excludedDirectories = @('target', 'obj', 'bin', 'consumer', 'embedded-consumer')
Get-ChildItem -LiteralPath (Join-Path $SourceRoot 'dotnet') -Force | ForEach-Object {
  if ($_.PSIsContainer -and $excludedDirectories -contains $_.Name) { return }
  Copy-Item -LiteralPath $_.FullName -Destination (Join-Path $workspaceDotnet $_.Name) -Recurse -Force
}
foreach ($name in @('LICENSE', 'NOTICE')) {
  $license = Join-Path $SourceRoot $name
  if (Test-Path -LiteralPath $license -PathType Leaf) {
    Copy-Item -LiteralPath $license -Destination (Join-Path $workspace $name) -Force
  }
}

$authorityDocument = Get-Content -LiteralPath $manifest -Raw | ConvertFrom-Json
if ($authorityDocument.schema -ne 'acyclic.sdk.rust-authority.v1' -or
    $authorityDocument.authority -ne 'rust') {
  throw 'Rust authority manifest has an unexpected schema or authority.'
}
$sourceGitSha = [string]$authorityDocument.source_git_sha
if ($sourceGitSha -notmatch '^[0-9a-fA-F]{40}$') {
  throw 'Rust authority manifest source_git_sha must be a 40-character Git revision.'
}
$sourceRevision = (& git -C $SourceRoot rev-parse HEAD 2>$null).Trim()
if ($sourceRevision -notmatch '^[0-9a-fA-F]{40}$' -or
    $sourceRevision.ToLowerInvariant() -ne $sourceGitSha.ToLowerInvariant()) {
  throw "Rust authority source revision $sourceGitSha does not match checkout $sourceRevision"
}
$modelRevision = [string]$authorityDocument.source_revision
if ($modelRevision -notmatch '^[0-9a-fA-F]{64}$') {
  throw 'Rust authority manifest source_revision must be a 64-character model digest.'
}
$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
$requestSource = $requestDocument.PSObject.Properties['source']
$requestRevisionProperty = if ($null -ne $requestSource) { $requestSource.Value.PSObject.Properties['revision'] } else { $null }
$requestRevision = if ($null -ne $requestRevisionProperty) { [string]$requestRevisionProperty.Value } else { '' }
if ($requestRevision -and $requestRevision.ToLowerInvariant() -ne $sourceRevision.ToLowerInvariant()) {
  throw "Generation request source revision $requestRevision does not match checkout $sourceRevision"
}

# MSBuild's validation target is intentionally written for the Windows SDK
# host name.  Supply a tiny executable-compatible alias under PowerShell Core
# so the same Rust-owned producer recipe runs on Linux and macOS.
$runningOnWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
  [System.Runtime.InteropServices.OSPlatform]::Windows
)
if (-not $runningOnWindows) {
  $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
  if ($null -eq $pwsh) { throw 'PowerShell Core (pwsh) is required on Unix for the .NET producer adapter.' }
  $shimDirectory = Join-Path $output '.tools'
  $shim = Join-Path $shimDirectory 'powershell.exe'
  $null = New-Item -ItemType Directory -Force -Path $shimDirectory
  Set-Content -LiteralPath $shim -Value "#!/usr/bin/env sh`nexec pwsh \"`$@\"`n" -Encoding utf8NoBOM
  & chmod +x $shim
  if ($LASTEXITCODE -ne 0) { throw "Could not make the PowerShell Core shim executable: $shim" }
  $env:PATH = "$shimDirectory$([IO.Path]::PathSeparator)$env:PATH"
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
  '-p:ContinuousIntegrationBuild=true'
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
  source_revision = $sourceRevision.ToLowerInvariant()
  source_revision_kind = 'git-oid'
  rust_model_revision = $modelRevision.ToLowerInvariant()
  rust_model_revision_kind = 'rust-model-sha256'
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

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [string] $CacheRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Version = '7.25.0'
$Sha256 = '41ce4f6b07f196676439d710759fa1ced7a08066d06ff1bf314681470289efae'
$Url = "https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/$Version/openapi-generator-cli-$Version.jar"

if ([string]::IsNullOrWhiteSpace($CacheRoot)) {
    $CacheRoot = [Environment]::GetEnvironmentVariable('ACYCLIC_TOOL_CACHE')
}
if ([string]::IsNullOrWhiteSpace($CacheRoot)) {
    if ($IsWindows) {
        $CacheRoot = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'acyclic/tool-cache'
    } else {
        $CacheRoot = Join-Path ([Environment]::GetFolderPath('UserProfile')) '.cache/acyclic/tool-cache'
    }
}

$CacheRoot = [IO.Path]::GetFullPath($CacheRoot)
$jar = Join-Path $CacheRoot "openapi-generator-cli-$Version.jar"
New-Item -ItemType Directory -Force -Path $CacheRoot | Out-Null

function Test-PinnedJar([string] $Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $false }
    return ((Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() -eq $Sha256)
}

if (-not (Test-PinnedJar $jar)) {
    $partial = "$jar.$PID.download"
    try {
        Invoke-WebRequest -Uri $Url -OutFile $partial -UseBasicParsing
        if (-not (Test-PinnedJar $partial)) {
            throw "OpenAPI Generator $Version checksum mismatch"
        }
        Move-Item -LiteralPath $partial -Destination $jar -Force
    } finally {
        if (Test-Path -LiteralPath $partial) { Remove-Item -LiteralPath $partial -Force }
    }
}

if (-not (Test-PinnedJar $jar)) { throw "Pinned OpenAPI Generator jar is unavailable: $jar" }
Write-Output $jar

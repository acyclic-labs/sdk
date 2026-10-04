[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [Parameter(Mandatory = $true)] [string] $AuthorityManifest,
    [Parameter(Mandatory = $true)] [string] $OutputRoot,
    [Parameter(Mandatory = $true)] [string] $TargetOutput,
    [Parameter(Mandatory = $true)] [string] $Request,
    [string] $GoBin = $env:SDK_GO_BIN,
    [string] $Protoc = $env:SDK_PROTOC_BIN,
    [string] $ProtocGenGo = $env:SDK_PROTOC_GEN_GO_BIN,
    [string] $ProtocGenGoGrpc = $env:SDK_PROTOC_GEN_GO_GRPC_BIN,
    [string] $GoVersion = 'go1.27.1',
    [string] $ProtocVersion = 'libprotoc 36.2',
    [string] $ProtocGenGoVersion = 'v1.36.10',
    [string] $ProtocGenGoGrpcVersion = '1.5.1'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-RequiredFile([string] $Path, [string] $Label) {
    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw "$Label must be supplied explicitly or through its SDK_*_BIN environment variable"
    }
    $resolved = [System.IO.Path]::GetFullPath($Path)
    if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
        throw "$Label does not exist: $resolved"
    }
    return $resolved
}

$source = [System.IO.Path]::GetFullPath($SourceRoot)
$authority = Resolve-RequiredFile $AuthorityManifest 'Rust authority manifest'
$request = Resolve-RequiredFile $Request 'generation request'
$go = Resolve-RequiredFile $GoBin 'Go toolchain'
$protoc = Resolve-RequiredFile $Protoc 'protoc'
$genGo = Resolve-RequiredFile $ProtocGenGo 'protoc-gen-go'
$genGrpc = Resolve-RequiredFile $ProtocGenGoGrpc 'protoc-gen-go-grpc'
$target = [System.IO.Path]::GetFullPath($TargetOutput)
$output = [System.IO.Path]::GetFullPath($OutputRoot)

if (-not (Test-Path -LiteralPath $source -PathType Container)) {
    throw "Rust source root does not exist: $source"
}
$targetRelative = [System.IO.Path]::GetRelativePath($output, $target)
if ([System.IO.Path]::IsPathRooted($targetRelative) -or $targetRelative -eq '..' -or $targetRelative.StartsWith('..' + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::Ordinal)) {
    throw "staged Go output must remain inside the generation output root: $target"
}
$goProject = Join-Path $source 'go'
if (-not (Test-Path -LiteralPath (Join-Path $goProject 'cmd/sdk-go-producer/main.go') -PathType Leaf)) {
    throw "Rust source checkout has no Go producer: $goProject"
}

$arguments = @(
    'run', './cmd/sdk-go-producer',
    '--source-root', $source,
    '--authority', $authority,
    '--request', $request,
    '--output', $target,
    '--protoc', $protoc,
    '--protoc-gen-go', $genGo,
    '--protoc-gen-go-grpc', $genGrpc,
    '--go-version', $GoVersion,
    '--protoc-version', $ProtocVersion,
    '--protoc-gen-go-version', $ProtocGenGoVersion,
    '--protoc-gen-go-grpc-version', $ProtocGenGoGrpcVersion
)

Push-Location $goProject
try {
    & $go @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Rust-authority Go producer failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

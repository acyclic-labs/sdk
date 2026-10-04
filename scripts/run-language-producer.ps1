[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [ValidateSet('ruby', 'php', 'dart', 'swift', 'cpp', 'bash', 'perl', 'powershell', 'ada', 'crystal', 'nim', 'r')] [string] $TargetId,
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [Parameter(Mandatory = $true)] [string] $WireRoot,
    [Parameter(Mandatory = $true)] [string] $AuthorityManifest,
    [Parameter(Mandatory = $true)] [string] $OutputRoot,
    [Parameter(Mandatory = $true)] [string] $TargetOutput,
    [Parameter(Mandatory = $true)] [string] $Request
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Require-Command([string] $Name) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($null -eq $command) { throw "$Name is required for the $TargetId producer" }
    return $command.Source
}

function Require-File([string] $Path, [string] $Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "$Label was not found: $Path" }
}

function Resolve-PinnedTool([string] $Name, [string[]] $Candidates) {
    $explicit = [Environment]::GetEnvironmentVariable("ACYCLIC_$($Name.ToUpperInvariant())")
    $paths = @()
    if ($explicit) { $paths += $explicit }
    $paths += $Candidates
    foreach ($candidate in $paths) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    throw "$Name is required for the $TargetId producer; set ACYCLIC_$($Name.ToUpperInvariant()) or provision the pinned release toolchain"
}

function Copy-Tree([string] $Source, [string] $Destination) {
    if (Test-Path -LiteralPath $Destination) { Remove-Item -LiteralPath $Destination -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    Get-ChildItem -LiteralPath $Source -Force | Copy-Item -Destination $Destination -Recurse -Force
}

New-Item -ItemType Directory -Force -Path $TargetOutput | Out-Null
$wireManifest = Join-Path $WireRoot 'rust-authority.json'
Require-File $wireManifest 'Rust authority manifest'
Require-File $Request 'generation request'
$toolPaths = [System.Collections.Generic.List[string]]::new()

switch ($TargetId) {
    'ruby' {
        $ruby = Require-Command 'ruby'
        $toolPaths.Add($ruby)
        $input = Join-Path $OutputRoot '.producer-input/ruby'
        Copy-Tree (Join-Path $SourceRoot 'ruby') $input
        & $ruby (Join-Path $input 'generate.rb') '--schema-root' $WireRoot '--manifest' $wireManifest
        if ($LASTEXITCODE -ne 0) { throw "Ruby producer failed with exit code $LASTEXITCODE" }
        Copy-Tree (Join-Path $input 'generated') (Join-Path $TargetOutput 'generated')
    }
    'php' {
        $php = Require-Command 'php'
        $toolPaths.Add($php)
        $input = Join-Path $OutputRoot '.producer-input/php'
        Copy-Tree (Join-Path $SourceRoot 'php') $input
        & $php (Join-Path $input 'tools/generate.php') '--schema-root' $WireRoot '--manifest' $wireManifest
        if ($LASTEXITCODE -ne 0) { throw "PHP producer failed with exit code $LASTEXITCODE" }
        Copy-Tree (Join-Path $input 'src') (Join-Path $TargetOutput 'src')
    }
    'dart' {
        $dart = Require-Command 'dart'
        $toolPaths.Add($dart)
        $input = Join-Path $OutputRoot '.producer-input/dart'
        Copy-Tree (Join-Path $SourceRoot 'dart') $input
        & $dart 'run' (Join-Path $input 'tool/generate.dart') '--schema-root' $WireRoot '--manifest' $wireManifest
        if ($LASTEXITCODE -ne 0) { throw "Dart producer failed with exit code $LASTEXITCODE" }
        Copy-Tree (Join-Path $input 'lib/src/generated') (Join-Path $TargetOutput 'lib/src/generated')
    }
    'swift' {
        $protoc = Resolve-PinnedTool 'protoc' @(
            (Join-Path $SourceRoot 'build/protobuf-36.2/bin/protoc.exe'),
            'Q:\sdk\build\protobuf-36.2\bin\protoc.exe'
        )
        $swift = Resolve-PinnedTool 'protoc-gen-swift' @(
            (Join-Path $SourceRoot 'build/swift-build/swift-protobuf-consumer-241/plugins/cache/SwiftProtobufPlugin.exe'),
            'Q:\sdk\build\swift-build\swift-protobuf-consumer-241\plugins\cache\SwiftProtobufPlugin.exe'
        )
        $grpc = Resolve-PinnedTool 'protoc-gen-grpc-swift' @(
            (Join-Path $SourceRoot 'build/swift-build/swift-protobuf-consumer-241/plugins/cache/GRPCProtobufPlugin.exe'),
            'Q:\sdk\build\swift-build\swift-protobuf-consumer-241\plugins\cache\GRPCProtobufPlugin.exe'
        )
        $toolPaths.Add($protoc)
        $toolPaths.Add($swift)
        $toolPaths.Add($grpc)
        & (Join-Path $SourceRoot 'swift/Generate.ps1') -Protoc $protoc -SwiftPlugin $swift -GrpcSwiftPlugin $grpc -ProtoRoot $WireRoot -OutputDirectory $TargetOutput -WritePackageManifest
        if ($LASTEXITCODE -ne 0) { throw "Swift producer failed with exit code $LASTEXITCODE" }
    }
    'cpp' {
        $protoc = Resolve-PinnedTool 'protoc' @(
            (Join-Path $SourceRoot 'build/protobuf-36.2/bin/protoc.exe'),
            'Q:\sdk\build\protobuf-36.2\bin\protoc.exe'
        )
        $plugin = Resolve-PinnedTool 'grpc_cpp_plugin' @(
            (Join-Path $SourceRoot 'build/grpc-install-1.80.0-vs-clean/bin/grpc_cpp_plugin.exe'),
            'Q:\sdk\build\grpc-install-1.80.0-vs-clean\bin\grpc_cpp_plugin.exe'
        )
        $toolPaths.Add($protoc)
        $toolPaths.Add($plugin)
        & (Join-Path $SourceRoot 'cpp/Generate.ps1') -Protoc $protoc -GrpcCppPlugin $plugin -ProtoRoot $WireRoot -OutputDirectory $TargetOutput
        if ($LASTEXITCODE -ne 0) { throw "C++ producer failed with exit code $LASTEXITCODE" }
    }
    { $_ -in @('bash', 'perl', 'powershell') } {
        $toolPaths.Add((Require-Command 'java'))
        & (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/produce-http-target.ps1') -TargetId $TargetId -SourceRoot $SourceRoot -AuthorityManifest $AuthorityManifest -OutputRoot $OutputRoot -TargetOutput $TargetOutput -Request $Request
        if ($LASTEXITCODE -ne 0) { throw "OpenAPI HTTP producer failed for $TargetId with exit code $LASTEXITCODE" }
    }
    { $_ -in @('ada', 'crystal') } {
        $toolPaths.Add((Require-Command 'java'))
        & (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/produce-ada-crystal.ps1') -TargetId $TargetId -SourceRoot $SourceRoot -AuthorityManifest $AuthorityManifest -OutputRoot $OutputRoot -TargetOutput $TargetOutput -Request $Request
        if ($LASTEXITCODE -ne 0) { throw "OpenAPI producer failed for $TargetId with exit code $LASTEXITCODE" }
    }
    { $_ -in @('nim', 'r') } {
        $toolPaths.Add((Require-Command 'java'))
        & (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/produce-nim-r.ps1') -TargetId $TargetId -SourceRoot $SourceRoot -AuthorityManifest $AuthorityManifest -OutputRoot $OutputRoot -TargetOutput $TargetOutput -Request $Request
        if ($LASTEXITCODE -ne 0) { throw "OpenAPI producer failed for $TargetId with exit code $LASTEXITCODE" }
    }
}

$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
$toolReceipt = [ordered]@{
    schema = 'acyclic.sdk.language-toolchain-receipt.v1'
    target = $TargetId
    source_revision = [string]$requestDocument.source.revision
    source_digest = [string]$requestDocument.source.digest
    tools = @($toolPaths | Sort-Object -Unique | ForEach-Object {
        [ordered]@{
            path = $_
            sha256 = (Get-FileHash -LiteralPath $_ -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    })
}
$toolReceipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $TargetOutput 'toolchain-receipt.json') -Encoding utf8NoBOM

Write-Host "Generated $TargetId Rust-authority package in $TargetOutput"

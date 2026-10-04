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

function Full-Path([string] $Path) {
    return [System.IO.Path]::GetFullPath($Path)
}

function Assert-SafeDestination([string] $Destination, [string] $Root, [string] $Source) {
    $rootPath = Full-Path $Root
    $destinationPath = Full-Path $Destination
    $rootPrefix = $rootPath.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
    if (-not $destinationPath.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "producer destination must be a child of OutputRoot: $destinationPath"
    }
    $sourcePath = Full-Path $Source
    if ($destinationPath.StartsWith($sourcePath.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase) -or
        $sourcePath.StartsWith($destinationPath.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase) -or
        $sourcePath.Equals($destinationPath, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "producer source and destination must be disjoint: $sourcePath / $destinationPath"
    }
    if (Test-Path -LiteralPath $Destination) {
        $reparse = Get-ChildItem -LiteralPath $Destination -Force -Recurse -ErrorAction Stop |
            Where-Object { ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 } |
            Select-Object -First 1
        if ($reparse) { throw "refusing to remove reparse point under producer destination: $($reparse.FullName)" }
        $item = Get-Item -LiteralPath $Destination -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "refusing to remove reparse-point producer destination: $Destination"
        }
    }
}

function Resolve-PinnedTool([string] $Name, [string[]] $Candidates) {
    $explicit = [Environment]::GetEnvironmentVariable("ACYCLIC_$($Name.ToUpperInvariant())")
    $expected = [Environment]::GetEnvironmentVariable("ACYCLIC_$($Name.ToUpperInvariant())_SHA256")
    $builtinHashes = @{
        ruby = '522bec55ce15ae724222207930222c4fd85b9688fec155093067872872e9b7bb'
        php = '2f372d8bcd4dd20ac60b223cadeb1c5ddb5994725dd244108cd67e25ed5bab96'
        dart = 'cc74095cd723739b6f9f0cd155ffa29d55da679fe7a188a0022c9048cf36e16c'
        protoc = '5a1b5350308309c9729ce4484a798981f281c10909144c4bdeFd6a37688e4b1f'.ToLowerInvariant()
        grpc_cpp_plugin = 'db8dc820af0e37a4adb410876148fb0b205c858a6d36ecee337b0b4a2d166a4a'
        grpc_php_plugin = '3f8afe91e921b9ff0c35aa7baaf755b6241d954552d1222beaf4eef2d87f32ee'
        'protoc-gen-dart' = 'abd4c73ecff068bfea97a1315c65a2f5b9aa64ca22c263ce568dea95d4161fc5'
        java = '7e8b8f4be1a64db6784d95c16d833f292a5d32b070442a764117f34dc2a001f9'
        'protoc-gen-swift' = 'b9d026472016eb8606f4bacd691c089b5fb23207e1f5fe31d5958749cee6802e'
    }
    if (-not $expected -and $builtinHashes.ContainsKey($Name)) { $expected = $builtinHashes[$Name] }
    $paths = @()
    if ($explicit) { $paths += $explicit }
    $paths += $Candidates
    foreach ($candidate in $paths) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            $resolved = (Resolve-Path -LiteralPath $candidate).Path
            if (-not $expected) { throw "No SHA-256 pin is configured for $Name; set ACYCLIC_$($Name.ToUpperInvariant())_SHA256" }
            $actual = (Get-FileHash -LiteralPath $resolved -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($actual -ne $expected.ToLowerInvariant()) { throw "$Name checksum mismatch for $resolved; expected $expected, got $actual" }
            return $resolved
        }
    }
    throw "$Name is required for the $TargetId producer; provision the pinned release toolchain and set ACYCLIC_$($Name.ToUpperInvariant()) plus its SHA-256 pin"
}

function Copy-Tree([string] $Source, [string] $Destination) {
    Assert-SafeDestination $Destination $OutputRoot $Source
    if (-not (Test-Path -LiteralPath $Source -PathType Container)) { throw "producer source directory was not found: $Source" }
    if (Test-Path -LiteralPath $Destination) { Remove-Item -LiteralPath $Destination -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    Get-ChildItem -LiteralPath $Source -Force | Copy-Item -Destination $Destination -Recurse -Force
}

function Copy-PackageTree([string] $Source, [string] $Destination, [string[]] $ExcludeDirectory) {
    Assert-SafeDestination $Destination $OutputRoot $Source
    if (-not (Test-Path -LiteralPath $Source -PathType Container)) { throw "package source directory was not found: $Source" }
    if (Test-Path -LiteralPath $Destination) { Remove-Item -LiteralPath $Destination -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    $items = Get-ChildItem -LiteralPath $Source -Force
    foreach ($item in $items) {
        if ($item.PSIsContainer -and $ExcludeDirectory -contains $item.Name) { continue }
        Copy-Item -LiteralPath $item.FullName -Destination $Destination -Recurse -Force
    }
}

New-Item -ItemType Directory -Force -Path $TargetOutput | Out-Null
$OutputRoot = Full-Path $OutputRoot
$TargetOutput = Full-Path $TargetOutput
Assert-SafeDestination $TargetOutput $OutputRoot $SourceRoot
$wireManifest = Join-Path $WireRoot 'rust-authority.json'
Require-File $wireManifest 'Rust authority manifest'
Require-File $Request 'generation request'
$toolPaths = [System.Collections.Generic.List[string]]::new()

switch ($TargetId) {
    'ruby' {
        $ruby = Resolve-PinnedTool 'ruby' @(
            (Join-Path $SourceRoot 'ruby/.toolchain/ruby.exe'),
            'Q:\sdk\ruby-cache\ruby\rubyinstaller-3.2.11-1-x64\bin\ruby.exe'
        )
        $rubyGemBin = 'Q:\sdk\ruby-cache\gems\bin'
        if (Test-Path -LiteralPath 'Q:\sdk\ruby-cache\gems' -PathType Container) {
            $env:GEM_HOME = 'Q:\sdk\ruby-cache\gems'
            $env:GEM_PATH = 'Q:\sdk\ruby-cache\gems'
        }
        $rubyBin = Split-Path -Parent $ruby
        $env:PATH = "$rubyGemBin;$rubyBin;$([Environment]::GetEnvironmentVariable('PATH'))"
        $toolPaths.Add($ruby)
        if (Test-Path -LiteralPath (Join-Path $rubyGemBin 'grpc_tools_ruby_protoc.bat') -PathType Leaf) {
            $toolPaths.Add((Resolve-Path -LiteralPath (Join-Path $rubyGemBin 'grpc_tools_ruby_protoc.bat')).Path)
        }
        $input = Join-Path $OutputRoot '.producer-input/ruby'
        Copy-Tree (Join-Path $SourceRoot 'ruby') $input
        & $ruby (Join-Path $input 'generate.rb') '--schema-root' $WireRoot '--manifest' $wireManifest
        if ($LASTEXITCODE -ne 0) { throw "Ruby producer failed with exit code $LASTEXITCODE" }
        Copy-PackageTree $input $TargetOutput @('.bundle', 'test')
    }
    'php' {
        $php = Resolve-PinnedTool 'php' @(
            (Join-Path $SourceRoot 'php/.toolchain/php.exe'),
            'Q:\sdk\php-cache\php-8.2.34\php.exe'
        )
        $protoc = Resolve-PinnedTool 'protoc' @(
            (Join-Path $SourceRoot 'build/protobuf-36.2/bin/protoc.exe'),
            'Q:\sdk\build\protobuf-36.2\bin\protoc.exe'
        )
        $phpPlugin = Resolve-PinnedTool 'grpc_php_plugin' @(
            (Join-Path $SourceRoot 'build/grpc-install-1.80.0-vs-clean/bin/grpc_php_plugin.exe'),
            'Q:\sdk\php-cache\grpc-build6\grpc_php_plugin.exe'
        )
        $env:PROTOC = $protoc
        $env:GRPC_PHP_PLUGIN = $phpPlugin
        $toolPaths.Add($php)
        $toolPaths.Add($protoc)
        $toolPaths.Add($phpPlugin)
        $input = Join-Path $OutputRoot '.producer-input/php'
        Copy-Tree (Join-Path $SourceRoot 'php') $input
        & $php (Join-Path $input 'tools/generate.php') '--schema-root' $WireRoot '--manifest' $wireManifest
        if ($LASTEXITCODE -ne 0) { throw "PHP producer failed with exit code $LASTEXITCODE" }
        Copy-PackageTree $input $TargetOutput @('tests')
    }
    'dart' {
        $dart = Resolve-PinnedTool 'dart' @(
            (Join-Path $SourceRoot 'dart/.toolchain/dart-sdk/bin/dart.exe'),
            'Q:\sdk\dart\.toolchain\dart-sdk\bin\dart.exe',
            'C:\Users\varun\.codex\worktrees\rust-sdk-docs-source\sdk\dart\.toolchain\dart-sdk\bin\dart.exe'
        )
        $protoc = Resolve-PinnedTool 'protoc' @(
            (Join-Path $SourceRoot 'build/protobuf-36.2/bin/protoc.exe'),
            'Q:\sdk\build\protobuf-36.2\bin\protoc.exe'
        )
        $dartPlugin = Resolve-PinnedTool 'protoc-gen-dart' @(
            (Join-Path $SourceRoot 'dart/.toolchain/protoc-gen-dart-shim.exe'),
            'Q:\sdk\dart\.toolchain\protoc-gen-dart-shim.exe',
            'C:\Users\varun\.codex\worktrees\rust-sdk-docs-source\sdk\dart\.toolchain\protoc-gen-dart-shim.exe'
        )
        $env:PROTOC = $protoc
        $env:PROTOC_GEN_DART = $dartPlugin
        $env:PUB_CACHE = Join-Path $SourceRoot 'dart/.pub-cache'
        $toolPaths.Add($dart)
        $toolPaths.Add($protoc)
        $toolPaths.Add($dartPlugin)
        $input = Join-Path $OutputRoot '.producer-input/dart'
        Copy-Tree (Join-Path $SourceRoot 'dart') $input
        & $dart 'run' (Join-Path $input 'tool/generate.dart') '--schema-root' $WireRoot '--manifest' $wireManifest
        if ($LASTEXITCODE -ne 0) { throw "Dart producer failed with exit code $LASTEXITCODE" }
        Copy-PackageTree $input $TargetOutput @('.dart_tool', '.pub-cache', '.toolchain', 'test')
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
        $java = Resolve-PinnedTool 'java' @(
            'C:\Program Files\Eclipse Adoptium\jdk-17.0.14.7-hotspot\bin\java.exe',
            (Join-Path $SourceRoot 'build/jdk-17/bin/java.exe')
        )
        $env:PATH = "$(Split-Path -Parent $java);$([Environment]::GetEnvironmentVariable('PATH'))"
        $toolPaths.Add($java)
        $jar = [string](& pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/ensure-openapi-generator.ps1') '-SourceRoot' $SourceRoot)
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw 'Pinned OpenAPI Generator bootstrap failed' }
        $toolPaths.Add((Resolve-Path -LiteralPath $jar).Path)
        & (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/produce-http-target.ps1') -TargetId $TargetId -SourceRoot $SourceRoot -AuthorityManifest $AuthorityManifest -OutputRoot $OutputRoot -TargetOutput $TargetOutput -Request $Request
        if ($LASTEXITCODE -ne 0) { throw "OpenAPI HTTP producer failed for $TargetId with exit code $LASTEXITCODE" }
    }
    { $_ -in @('ada', 'crystal') } {
        $java = Resolve-PinnedTool 'java' @('C:\Program Files\Eclipse Adoptium\jdk-17.0.14.7-hotspot\bin\java.exe', (Join-Path $SourceRoot 'build/jdk-17/bin/java.exe'))
        $env:PATH = "$(Split-Path -Parent $java);$([Environment]::GetEnvironmentVariable('PATH'))"
        $toolPaths.Add($java)
        $jar = [string](& pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/ensure-openapi-generator.ps1') '-SourceRoot' $SourceRoot)
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw 'Pinned OpenAPI Generator bootstrap failed' }
        $toolPaths.Add((Resolve-Path -LiteralPath $jar).Path)
        & (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/produce-ada-crystal.ps1') -TargetId $TargetId -SourceRoot $SourceRoot -AuthorityManifest $AuthorityManifest -OutputRoot $OutputRoot -TargetOutput $TargetOutput -Request $Request
        if ($LASTEXITCODE -ne 0) { throw "OpenAPI producer failed for $TargetId with exit code $LASTEXITCODE" }
    }
    { $_ -in @('nim', 'r') } {
        $java = Resolve-PinnedTool 'java' @('C:\Program Files\Eclipse Adoptium\jdk-17.0.14.7-hotspot\bin\java.exe', (Join-Path $SourceRoot 'build/jdk-17/bin/java.exe'))
        $env:PATH = "$(Split-Path -Parent $java);$([Environment]::GetEnvironmentVariable('PATH'))"
        $toolPaths.Add($java)
        $jar = [string](& pwsh '-NoProfile' '-File' (Join-Path $SourceRoot 'research/additional-languages/openapi-targets/ensure-openapi-generator.ps1') '-SourceRoot' $SourceRoot)
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $jar -PathType Leaf)) { throw 'Pinned OpenAPI Generator bootstrap failed' }
        $toolPaths.Add((Resolve-Path -LiteralPath $jar).Path)
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

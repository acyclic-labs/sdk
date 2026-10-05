[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Protoc,

    [Parameter(Mandatory = $true)]
    [string]$SwiftPlugin,

    [Parameter(Mandatory = $true)]
    [string]$GrpcSwiftPlugin,

    [Parameter(Mandatory = $true)]
    [string]$ProtoRoot,
    [string]$OutputDirectory = (Join-Path $PSScriptRoot "..\build\sdk-swift\generated"),
    [string]$ConsumerSource = (Join-Path $PSScriptRoot "consumer\main.swift"),
    [switch]$WritePackageManifest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedProtoc = "36.2"
$ExpectedSwiftProtobuf = "1.38.1"
$ExpectedGrpcSwift = "2.4.1"

function Require-File([string]$Path, [string]$Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label was not found: $Path"
    }
}

Require-File $Protoc "protoc"
Require-File $SwiftPlugin "protoc-gen-swift"
Require-File $GrpcSwiftPlugin "protoc-gen-grpc-swift"
if (-not (Test-Path -LiteralPath $ProtoRoot -PathType Container)) {
    throw "Proto root was not found: $ProtoRoot"
}
Require-File (Join-Path $ProtoRoot "rust-authority.json") "Rust authority manifest"

$protocVersion = (& $Protoc --version).Trim()
if ($protocVersion -ne "libprotoc $ExpectedProtoc") {
    throw "Expected libprotoc $ExpectedProtoc; found '$protocVersion'."
}

$swiftVersion = ((& $SwiftPlugin --version 2>$null) | Out-String).Trim()
if ($swiftVersion -and $swiftVersion -notmatch [regex]::Escape($ExpectedSwiftProtobuf)) {
    throw "Expected protoc-gen-swift $ExpectedSwiftProtobuf; found '$swiftVersion'."
}

$grpcVersion = ((& $GrpcSwiftPlugin --version 2>$null) | Out-String).Trim()
if ($grpcVersion -and $grpcVersion -notmatch [regex]::Escape($ExpectedGrpcSwift)) {
    throw "Expected protoc-gen-grpc-swift $ExpectedGrpcSwift; found '$grpcVersion'."
}

$protoFiles = @(Get-ChildItem -LiteralPath $ProtoRoot -Recurse -File -Filter *.proto | Sort-Object FullName)
if ($protoFiles.Count -eq 0) {
    throw "No .proto sources found under $ProtoRoot"
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
# Remove only prior generated Swift files so a rerun cannot mix layouts.
Get-ChildItem -LiteralPath $OutputDirectory -Recurse -File -Filter '*.swift' -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -ne 'Package.swift' } | Remove-Item -Force
$include = (Resolve-Path -LiteralPath $ProtoRoot).Path
$swift = (Resolve-Path -LiteralPath $SwiftPlugin).Path
$grpc = (Resolve-Path -LiteralPath $GrpcSwiftPlugin).Path
$protocArgs = @(
    "-I", $include,
    "--swift_out=$OutputDirectory",
    "--swift_opt=Visibility=Public",
    "--grpc-swift_out=$OutputDirectory",
    "--grpc-swift_opt=Visibility=Public",
    "--plugin=protoc-gen-swift=$swift",
    "--plugin=protoc-gen-grpc-swift=$grpc"
)
$protocArgs += $protoFiles | ForEach-Object { $_.FullName }

& $Protoc @protocArgs
if ($LASTEXITCODE -ne 0) {
    throw "Pinned Swift generation failed with exit code $LASTEXITCODE"
}

# SwiftPM and the Windows Swift driver identify source files by basename. The
# contract has versioned families with repeated names (for example objects/v1
# and objects/v2), so flatten generated files with a path-derived unique name
# before writing the package manifest. File contents and generated symbols are
# unchanged.
$generatedSwift = @(Get-ChildItem -LiteralPath $OutputDirectory -Recurse -File -Filter '*.swift' |
    Where-Object { $_.Name -ne 'Package.swift' })
foreach ($generated in $generatedSwift) {
    $relative = $generated.FullName.Substring((Resolve-Path -LiteralPath $OutputDirectory).Path.Length).TrimStart('\', '/')
    $flatName = ($relative -replace '[\\/]', '_')
    $destination = Join-Path $OutputDirectory $flatName
    if ($generated.FullName -ne $destination) {
        Move-Item -LiteralPath $generated.FullName -Destination $destination -Force
    }
}

Require-File $ConsumerSource "Swift fixture consumer source"
Copy-Item -LiteralPath $ConsumerSource -Destination (Join-Path $OutputDirectory "AcyclicFixtureConsumer.swift") -Force

# gRPC Swift 2.4.1 emits an RPC named `import` without Swift's required
# backticks. Preserve the wire method name while making the generated source
# legal Swift; the replacement is limited to declarations and member calls.
foreach ($generated in (Get-ChildItem -LiteralPath $OutputDirectory -File -Filter '*.grpc.swift')) {
    $text = Get-Content -LiteralPath $generated.FullName -Raw
    $text = $text -replace '\bfunc import\b', 'func `import`'
    $text = $text -replace '\.import\(', '.`import`('
    [IO.File]::WriteAllText($generated.FullName, $text, [Text.UTF8Encoding]::new($false))
}

$sourceRows = foreach ($proto in $protoFiles) {
    $relative = $proto.FullName.Substring($include.Length).TrimStart('\', '/') -replace '\\', '/'
    [ordered]@{ path = $relative; sha256 = (Get-FileHash -LiteralPath $proto.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
}
$sourceDigestText = ($sourceRows | ForEach-Object { "$($_.path) $($_.sha256)" }) -join "`n"
$sourceDigest = [Convert]::ToHexString(([System.Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($sourceDigestText)))).ToLowerInvariant()
$outputRoot = (Resolve-Path -LiteralPath $OutputDirectory).Path
$generatedRows = Get-ChildItem -LiteralPath $OutputDirectory -Recurse -File -Filter '*.swift' |
    Where-Object { $_.Name -ne 'Package.swift' -and $_.Name -ne 'AcyclicFixtureConsumer.swift' } |
    Sort-Object FullName | ForEach-Object {
    $relative = $_.FullName.Substring($outputRoot.Length).TrimStart('\', '/') -replace '\\', '/'
    [ordered]@{ path = $relative; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
}
$receipt = [ordered]@{
    schema_version = 1
    language = "swift"
    swift_protobuf_pin = $ExpectedSwiftProtobuf
    grpc_swift_pin = $ExpectedGrpcSwift
    protobuf_pin = $ExpectedProtoc
    source_digest = $sourceDigest
    proto_files = @($sourceRows)
    generated_files = @($generatedRows)
}
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $OutputDirectory "generation-receipt.json") -Encoding utf8NoBOM

if ($WritePackageManifest) {
    # GRPCCore and GRPCProtobuf are portable. Network transport selection is
    # intentionally left to the consuming application and its host platform.
    $sourceList = ($generatedRows | ForEach-Object {
        '        "' + $_.path + '"'
    }) -join ",`n"
    $manifest = @"
// swift-tools-version: 6.1
import PackageDescription

let package = Package(
  name: "AcyclicSDKGenerated",
  products: [
    .library(name: "AcyclicSDKGenerated", targets: ["AcyclicSDKGenerated"]),
    .executable(name: "AcyclicFixtureConsumer", targets: ["AcyclicFixtureConsumer"])
  ],
  dependencies: [
    .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.4.1"),
    .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", exact: "2.4.1"),
    .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", exact: "2.4.1")
  ],
  targets: [
    .target(
      name: "AcyclicSDKGenerated",
      dependencies: [
        .product(name: "GRPCCore", package: "grpc-swift-2"),
        .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf")
      ],
      path: ".",
      exclude: ["generation-receipt.json"],
      sources: [
$sourceList
      ]
    ),
    .executableTarget(
      name: "AcyclicFixtureConsumer",
      dependencies: [
        "AcyclicSDKGenerated",
        .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport")
      ],
      path: ".",
      sources: ["AcyclicFixtureConsumer.swift"]
    )
  ]
)
"@
    Set-Content -LiteralPath (Join-Path $OutputDirectory "Package.swift") -Value $manifest -Encoding utf8NoBOM
}

Write-Host "Generated $($protoFiles.Count) protobuf source files under $OutputDirectory"
Write-Host "Status: transport bindings only; run SwiftPM on a supported host and the conformance gates before packaging."

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Protoc,

    [Parameter(Mandatory = $true)]
    [string]$GrpcCppPlugin,

    [string]$ProtoRoot = (Join-Path $PSScriptRoot "..\proto"),
    [string]$OutputDirectory = (Join-Path $PSScriptRoot "..\build\sdk-cpp\generated"),

    [string]$AuthorityManifest = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ExpectedProtoc = "36.2"
$ExpectedGrpc = "1.80.0"

function Require-File([string]$Path, [string]$Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label was not found: $Path"
    }
}

Require-File $Protoc "protoc"
Require-File $GrpcCppPlugin "grpc_cpp_plugin"
if (-not (Test-Path -LiteralPath $ProtoRoot -PathType Container)) {
    throw "Proto root was not found: $ProtoRoot"
}

$protocVersion = (& $Protoc --version).Trim()
if ($protocVersion -ne "libprotoc $ExpectedProtoc") {
    throw "Expected libprotoc $ExpectedProtoc; found '$protocVersion'. Use the pinned generator; do not silently mix runtime generations."
}

# The official grpc_cpp_plugin does not expose --version and writes an
# "Unknown option: --version" diagnostic. Probe through cmd.exe so this
# expected capability difference is recorded under ErrorActionPreference=Stop.
$pluginProbe = (& cmd.exe /d /c ('"{0}" --version 2>&1' -f (Resolve-Path -LiteralPath $GrpcCppPlugin).Path) | Out-String).Trim()
$pluginVersion = if ($LASTEXITCODE -eq 0) { $pluginProbe } else { "unreported" }
if ($pluginVersion -ne "unreported" -and $pluginVersion -notmatch [regex]::Escape($ExpectedGrpc)) {
    throw "Expected grpc_cpp_plugin $ExpectedGrpc; found '$pluginVersion'. If this plugin does not expose --version, record its release in CI provenance before invoking this script."
}
$pluginSha256 = (Get-FileHash -LiteralPath $GrpcCppPlugin -Algorithm SHA256).Hash.ToLowerInvariant()

$protoFiles = @(Get-ChildItem -LiteralPath $ProtoRoot -Recurse -File -Filter *.proto | Sort-Object FullName)
if ($protoFiles.Count -eq 0) {
    throw "No .proto sources found under $ProtoRoot"
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$include = (Resolve-Path -LiteralPath $ProtoRoot).Path
$plugin = (Resolve-Path -LiteralPath $GrpcCppPlugin).Path
$protocArgs = @(
    "-I", $include,
    "--cpp_out=$OutputDirectory",
    "--grpc_out=$OutputDirectory",
    "--plugin=protoc-gen-grpc=$plugin"
)
$protocArgs += $protoFiles | ForEach-Object { $_.FullName }

& $Protoc @protocArgs
if ($LASTEXITCODE -ne 0) {
    throw "Pinned C++ generation failed with exit code $LASTEXITCODE"
}

if ($AuthorityManifest) {
    Require-File $AuthorityManifest "Rust authority manifest"
    Copy-Item -LiteralPath $AuthorityManifest -Destination (Join-Path $OutputDirectory "rust-authority.json") -Force
}

$sourceRows = foreach ($proto in $protoFiles) {
    $relative = $proto.FullName.Substring($include.Length).TrimStart('\', '/') -replace '\\', '/'
    $hash = (Get-FileHash -LiteralPath $proto.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    [ordered]@{ path = $relative; sha256 = $hash }
}
$sourceDigestText = ($sourceRows | ForEach-Object { "$($_.path) $($_.sha256)" }) -join "`n"
$sha256 = [System.Security.Cryptography.SHA256]::Create()
try {
    $sourceDigestBytes = $sha256.ComputeHash([Text.Encoding]::UTF8.GetBytes($sourceDigestText))
} finally {
    $sha256.Dispose()
}
$sourceDigest = (($sourceDigestBytes | ForEach-Object { $_.ToString("x2") }) -join "")
$outputRoot = (Resolve-Path -LiteralPath $OutputDirectory).Path
$generatedRows = Get-ChildItem -LiteralPath $OutputDirectory -Recurse -File -Include *.cc, *.h | Sort-Object FullName | ForEach-Object {
    $relative = $_.FullName.Substring($outputRoot.Length).TrimStart('\', '/') -replace '\\', '/'
    [ordered]@{ path = $relative; sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
}
$receipt = [ordered]@{
    schema_version = 1
    language = "cpp"
    protobuf_pin = $ExpectedProtoc
    grpc_pin = $ExpectedGrpc
    grpc_cpp_plugin_sha256 = $pluginSha256
    grpc_cpp_plugin_version = $pluginVersion
    source_digest = $sourceDigest
    proto_files = @($sourceRows)
    generated_files = @($generatedRows)
}
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $OutputDirectory "generation-receipt.json") -Encoding UTF8

Write-Host "Generated $($protoFiles.Count) protobuf source files under $OutputDirectory"
Write-Host "Source-bound generation receipt: $(Join-Path $OutputDirectory 'generation-receipt.json')"
Write-Host "Status: transport bindings only; run the CMake and conformance gates before packaging."

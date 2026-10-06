[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $OpenApi,
    [Parameter(Mandatory = $true)] [string] $Output,
    [string] $SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path,
    [string] $Jar = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Version = '7.25.0'
$ExpectedJarSha256 = '41ce4f6b07f196676439d710759fa1ced7a08066d06ff1bf314681470289efae'
$openApiPath = [IO.Path]::GetFullPath($OpenApi)
$outputPath = [IO.Path]::GetFullPath($Output)

if (-not (Test-Path -LiteralPath $openApiPath -PathType Leaf)) {
    throw "Rust-owned OpenAPI input does not exist: $openApiPath"
}

$documentText = Get-Content -LiteralPath $openApiPath -Raw
$document = $documentText | ConvertFrom-Json
if ($document.openapi -ne '3.0.3') {
    throw 'The Lua prototype only accepts the Rust OpenAPI 3.0.3 projection.'
}
$sourceMetadata = $document.PSObject.Properties['x-acyclic-source']
if ($null -eq $sourceMetadata) {
    throw 'Rust OpenAPI source metadata is missing.'
}
$wireAuthority = $sourceMetadata.Value.PSObject.Properties['wire_authority']
if ($null -eq $wireAuthority -or [string]::IsNullOrWhiteSpace([string]$wireAuthority.Value)) {
    throw 'OpenAPI source metadata does not identify the Rust wire authority.'
}

if ([string]::IsNullOrWhiteSpace($Jar)) {
    $Jar = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "acyclic/tool-cache/openapi-generator-cli-$Version.jar"
}
$jarPath = [IO.Path]::GetFullPath($Jar)
if (-not (Test-Path -LiteralPath $jarPath -PathType Leaf)) {
    throw "Pinned OpenAPI Generator jar is missing: $jarPath"
}
$jarSha256 = (Get-FileHash -LiteralPath $jarPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($jarSha256 -ne $ExpectedJarSha256) {
    throw "Pinned OpenAPI Generator checksum mismatch: $jarSha256"
}
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw 'Java is required for the pinned OpenAPI Generator jar.'
}

$sourceSha256 = (Get-FileHash -LiteralPath $openApiPath -Algorithm SHA256).Hash.ToLowerInvariant()
$sourceRevision = (& git -C $SourceRoot rev-parse HEAD 2>$null).Trim()
if ([string]::IsNullOrWhiteSpace($sourceRevision)) {
    throw "Could not determine the Rust source revision at $SourceRoot"
}
$decimalCount = [regex]::Matches($documentText, '"x-protobuf-json"\s*:\s*"decimal-string"').Count
$oneofCount = [regex]::Matches($documentText, '"x-protobuf-oneof"').Count
$presenceCount = [regex]::Matches($documentText, '"x-protobuf-presence"').Count

if (Test-Path -LiteralPath $outputPath) {
    Remove-Item -LiteralPath $outputPath -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

# Lua identifiers cannot contain the hyphens used by package display names.
# Keep this generator option Rust-stage data, rather than patching generated
# source after generation.
$packageName = 'acyclic_workers_lua'
& java -jar $jarPath generate -i $openApiPath -g lua -o $outputPath `
    --package-name $packageName `
    --additional-properties "packageName=$packageName,projectName=$packageName"
if ($LASTEXITCODE -ne 0) {
    throw "OpenAPI Generator Lua failed with exit code $LASTEXITCODE"
}

$rockspec = Get-ChildItem -LiteralPath $outputPath -File -Filter '*.rockspec' | Select-Object -First 1
if ($null -eq $rockspec) {
    throw 'OpenAPI Generator Lua did not emit a LuaRocks rockspec.'
}
$luaFiles = @(Get-ChildItem -LiteralPath $outputPath -Recurse -File -Filter '*.lua')
$luaText = ($luaFiles | ForEach-Object { Get-Content -LiteralPath $_.FullName -Raw }) -join "`n"
$invalidIdentifierCount = [regex]::Matches($luaText, '(?m)^local [^=\r\n]*-').Count
$extensionMentions = [regex]::Matches($luaText, 'x-protobuf').Count
$usesLuaHttp = $luaText.Contains('require "http.request"')
$usesDkjson = $luaText.Contains('require "dkjson"')
$usesBasexx = $luaText.Contains('require "basexx"')
$rockspecText = Get-Content -LiteralPath $rockspec.FullName -Raw
$rockspecHasDependencies = $rockspecText.Contains('"http"') -and $rockspecText.Contains('"dkjson"') -and $rockspecText.Contains('"basexx"')
$rockspecHasProductLicense = $rockspecText.Contains('license = "Apache-2.0"')

if ($invalidIdentifierCount -ne 0) {
    throw "Generated Lua contains $invalidIdentifierCount invalid local identifiers."
}
if (-not ($usesLuaHttp -and $usesDkjson -and $usesBasexx -and $rockspecHasDependencies)) {
    throw 'Generated Lua did not expose the expected lua-http/dkjson/basexx dependencies.'
}

$receipt = [ordered]@{
    schema = 'acyclic.sdk.lua.prototype-receipt.v1'
    source = [ordered]@{
        openapi_path = $openApiPath
        openapi_sha256 = $sourceSha256
        source_revision = $sourceRevision
        authority = 'rust'
        decimal_string_annotations = $decimalCount
        oneof_annotations = $oneofCount
        presence_annotations = $presenceCount
    }
    generator = [ordered]@{
        name = 'OpenAPI Generator'
        version = $Version
        generator_name = 'lua'
        jar_sha256 = $jarSha256
        output_package_name = $packageName
        output_file_count = @((Get-ChildItem -LiteralPath $outputPath -Recurse -File)).Count
        lua_file_count = $luaFiles.Count
    }
    capability_evidence = [ordered]@{
        generation = 'passed'
        installable_package_shape = if ($rockspecHasDependencies) { 'generated-but-runtime-unverified' } else { 'failed' }
        generated_lua_identifier_syntax = if ($invalidIdentifierCount -eq 0) { 'passed' } else { 'failed' }
        rust_extension_consumption = if ($extensionMentions -eq 0) { 'failed-no-extension-consumer' } else { 'passed' }
        uint64 = 'conditional-decimal-string-only-no-validation-proof'
        package_license_metadata = if ($rockspecHasProductLicense) { 'passed' } else { 'failed-upstream-template-unlicense' }
        grpc = 'excluded'
        streaming = 'excluded'
        embedded = 'excluded'
        runtime_execution = 'not-run-lua-and-luarocks-unavailable'
    }
    generated_rockspec = $rockspec.FullName
}
$receiptPath = Join-Path $outputPath 'lua-prototype-receipt.json'
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
Write-Output "Lua OpenAPI prototype generated: $outputPath"
Write-Output "Receipt: $receiptPath"

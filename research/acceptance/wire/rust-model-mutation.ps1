[CmdletBinding()]
param(
    [switch] $KeepWork
)

# Compile the real Rust exporters twice from detached source copies. The
# mutated copy changes one field JSON identity, one API description, and one
# HTTP route. Every assertion compares generated projections and immutable
# archive fixtures without modifying the checkout's Rust model.
$ErrorActionPreference = "Stop"

function Copy-ContractCrates([string] $SourceRoot, [string] $DestinationRoot) {
    $crateNames = @(
        "sdk-contract-wire",
        "sdk-contract-options",
        "sdk-contract-validation",
        "sdk-openapi-prototype"
    )
    foreach ($crateName in $crateNames) {
        $source = Join-Path $SourceRoot "rust/crates/$crateName"
        $destination = Join-Path $DestinationRoot "rust/crates/$crateName"
        New-Item -ItemType Directory -Force -Path $destination | Out-Null
        Copy-Item -Path (Join-Path $source "*") -Destination $destination -Recurse -Force
    }
}

function Copy-ModelFixtures([string] $SourceRoot, [string] $DestinationRoot) {
    $fixturePaths = @(
        "rust/crates/actors/src/generated/acyclic-actors-v1.bin",
        "rust/crates/workers/src/generated/acyclic-workers-v1.bin",
        "rust/crates/objects/src/generated/acyclic-objects-v2.bin",
        "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
        "rust/crates/inference/inference_descriptor.bin",
        "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
        "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
        "rust/crates/harness/src/generated/harness-archived-v2.bin"
    )
    foreach ($relative in $fixturePaths) {
        $source = Join-Path $SourceRoot $relative
        $destination = Join-Path $DestinationRoot $relative
        if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
            throw "Rust model fixture is missing from the source checkout: $source"
        }
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destination) | Out-Null
        Copy-Item -LiteralPath $source -Destination $destination -Force
    }
}

function Invoke-Cargo([string] $TargetDirectory, [string[]] $Arguments) {
    $previousTarget = $env:CARGO_TARGET_DIR
    try {
        $env:CARGO_TARGET_DIR = $TargetDirectory
        & cargo @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw "cargo exited with status ${LASTEXITCODE}: cargo $($Arguments -join ' ')"
        }
    } finally {
        if ($null -eq $previousTarget) {
            Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
        } else {
            $env:CARGO_TARGET_DIR = $previousTarget
        }
    }
}

function Get-Hash([string] $Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Assert-Same([string] $Left, [string] $Right, [string] $Label) {
    $leftHash = Get-Hash $Left
    $rightHash = Get-Hash $Right
    if ($leftHash -ne $rightHash) {
        throw "$Label changed unexpectedly: $Left ($leftHash) != $Right ($rightHash)"
    }
}

function Assert-Different([string] $Left, [string] $Right, [string] $Label) {
    $leftHash = Get-Hash $Left
    $rightHash = Get-Hash $Right
    if ($leftHash -eq $rightHash) {
        throw "$Label did not change after the Rust model mutation: $Left and $Right both hash $leftHash"
    }
}

function Assert-Contains([string] $Path, [string] $Needle, [string] $Label) {
    $content = Get-Content -LiteralPath $Path -Raw
    if (-not $content.Contains($Needle)) {
        throw "$Label is missing '$Needle' in $Path"
    }
}

function Assert-NotContains([string] $Path, [string] $Needle, [string] $Label) {
    $content = Get-Content -LiteralPath $Path -Raw
    if ($content.Contains($Needle)) {
        throw "$Label unexpectedly contains '$Needle' in $Path"
    }
}

function Replace-Once([string] $Content, [string] $Old, [string] $New, [string] $Label) {
    $count = ([regex]::Matches($Content, [regex]::Escape($Old))).Count
    if ($count -ne 1) {
        throw "$Label expected one source occurrence, found $count"
    }
    return $Content.Replace($Old, $New)
}

function Invoke-WireGeneration([string] $Root, [string] $Target, [string] $WireOutput, [string] $ProductOutput) {
    $manifest = Join-Path $Root "rust/crates/sdk-contract-wire/Cargo.toml"
    Invoke-Cargo $Target @(
        "run", "--locked", "--offline", "--manifest-path", $manifest,
        "--bin", "sdk-contract-wire", "--", "generate", "--out", $WireOutput
    )
    Invoke-Cargo $Target @(
        "run", "--locked", "--offline", "--manifest-path", $manifest,
        "--bin", "sdk-contract-wire", "--", "check", "--out", $WireOutput
    )
    Invoke-Cargo $Target @(
        "run", "--locked", "--offline", "--manifest-path", $manifest,
        "--bin", "sdk-contract-wire", "--", "generate-products", "--root", $Root,
        "--out", $ProductOutput
    )
    Invoke-Cargo $Target @(
        "run", "--locked", "--offline", "--manifest-path", $manifest,
        "--bin", "sdk-contract-wire", "--", "check-products", "--root", $Root,
        "--out", $ProductOutput
    )
}

function Invoke-OpenApiGeneration([string] $Root, [string] $Target, [string] $OutputRoot) {
    $manifest = Join-Path $Root "rust/crates/sdk-openapi-prototype/Cargo.toml"
    foreach ($family in @("actors", "workers", "stream", "objects", "inference")) {
        $output = Join-Path $OutputRoot "$family.json"
        Invoke-Cargo $Target @(
            "run", "--locked", "--offline", "--manifest-path", $manifest,
            "--bin", "sdk-openapi", "--", "--contract", $family, $output
        )
    }
}

function Get-ArchiveFixtures([string] $Root) {
    return @(Get-ChildItem -LiteralPath (Join-Path $Root "rust/crates/sdk-contract-wire/tests/fixtures") -Filter "*.descriptor.bin" -File)
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")).Path
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$workRoot = Join-Path ([System.IO.Path]::GetTempPath()) "acyclic-sdk-wire-rust-model-mutation-$stamp"
$baselineRoot = Join-Path $workRoot "baseline"
$mutatedRoot = Join-Path $workRoot "mutated"
$baselineWire = Join-Path $workRoot "outputs/baseline/wire"
$mutatedWire = Join-Path $workRoot "outputs/mutated/wire"
$baselineProducts = Join-Path $workRoot "outputs/baseline/products"
$mutatedProducts = Join-Path $workRoot "outputs/mutated/products"
$baselineOpenApi = Join-Path $workRoot "outputs/baseline/openapi"
$mutatedOpenApi = Join-Path $workRoot "outputs/mutated/openapi"
$baselineTarget = Join-Path $workRoot "target-baseline"
$mutatedTarget = Join-Path $workRoot "target-mutated"

try {
    New-Item -ItemType Directory -Force -Path $workRoot, $baselineWire, $mutatedWire,
        $baselineProducts, $mutatedProducts, $baselineOpenApi, $mutatedOpenApi | Out-Null

    Copy-ContractCrates $repoRoot $baselineRoot
    Copy-ContractCrates $repoRoot $mutatedRoot
    Copy-ModelFixtures $repoRoot $baselineRoot
    Copy-ModelFixtures $repoRoot $mutatedRoot

    Write-Host "Generating baseline projections from the copied Rust model..."
    Invoke-WireGeneration $baselineRoot $baselineTarget $baselineWire $baselineProducts
    Invoke-OpenApiGeneration $baselineRoot $baselineTarget $baselineOpenApi

    $sourcePath = Join-Path $mutatedRoot "rust/crates/sdk-contract-wire/src/lib.rs"
    $source = Get-Content -LiteralPath $sourcePath -Raw
    $source = Replace-Once $source '"memoryBytes"' '"memoryBytesMutation"' "field JSON identity mutation"
    $source = Replace-Once $source '"ActorLimits" => "Resource and checkpoint limits for an actor."' '"ActorLimits" => "Resource and checkpoint limits for an actor (mutation probe)."' "message documentation mutation"
    $source = Replace-Once $source 'path: "/v1/actors/create",' 'path: "/v1/actors/create-mutation",' "route mutation"
    Set-Content -LiteralPath $sourcePath -Value $source -NoNewline
    $baselineSourcePath = Join-Path $baselineRoot "rust/crates/sdk-contract-wire/src/lib.rs"
    if ((Get-Hash $baselineSourcePath) -eq (Get-Hash $sourcePath)) {
        throw "detached baseline and mutation Rust model copies unexpectedly have the same identity"
    }
    Assert-Contains $baselineSourcePath '"memoryBytes"' "detached baseline Rust model"
    Assert-NotContains $baselineSourcePath '"memoryBytesMutation"' "detached baseline Rust model"
    Assert-Contains $sourcePath '"memoryBytesMutation"' "detached mutated Rust model"

    Write-Host "Generating mutated projections from the detached Rust model copy..."
    Invoke-WireGeneration $mutatedRoot $mutatedTarget $mutatedWire $mutatedProducts
    Invoke-OpenApiGeneration $mutatedRoot $mutatedTarget $mutatedOpenApi

    $baselineActorsProto = Join-Path $baselineWire "actors/v1/actors.proto"
    $mutatedActorsProto = Join-Path $mutatedWire "actors/v1/actors.proto"
    $baselineActorsDescriptor = Join-Path $baselineWire "actors/v1/actors.fds.bin"
    $mutatedActorsDescriptor = Join-Path $mutatedWire "actors/v1/actors.fds.bin"
    $baselineAuthority = Join-Path $baselineWire "rust-authority.json"
    $mutatedAuthority = Join-Path $mutatedWire "rust-authority.json"
    $baselineGoldens = Join-Path $baselineWire "rust-family-goldens.json"
    $mutatedGoldens = Join-Path $mutatedWire "rust-family-goldens.json"

    Assert-Different $baselineActorsProto $mutatedActorsProto "derived Actors protobuf"
    Assert-Different $baselineActorsDescriptor $mutatedActorsDescriptor "derived Actors descriptor"
    Assert-Different $baselineAuthority $mutatedAuthority "Rust authority manifest"
    Assert-Different $baselineGoldens $mutatedGoldens "Rust family goldens"
    Assert-Contains $mutatedActorsProto "Resource and checkpoint limits for an actor (mutation probe)." "derived API documentation"

    $baselineActorsOpenApi = Join-Path $baselineOpenApi "actors.json"
    $mutatedActorsOpenApi = Join-Path $mutatedOpenApi "actors.json"
    Assert-Different $baselineActorsOpenApi $mutatedActorsOpenApi "derived Actors OpenAPI"
    Assert-Contains $mutatedActorsOpenApi "/v1/actors/create-mutation" "derived OpenAPI route"
    Assert-Contains $mutatedActorsOpenApi "memoryBytesMutation" "derived OpenAPI JSON identity"
    Assert-Contains $mutatedActorsOpenApi "Resource and checkpoint limits for an actor (mutation probe)." "derived OpenAPI documentation"

    $policyPaths = @(
        "ruby/lib/acyclic_sdk/generated_remote_policy.rb",
        "php/src/Acyclic/Runtime/GeneratedRemotePolicy.php",
        "dart/lib/src/generated_remote_policy.dart",
        "jvm/src/main/java/dev/acyclic/transport/GeneratedRemotePolicy.java",
        "dotnet/GeneratedRemotePolicy.cs"
    )
    foreach ($relative in $policyPaths) {
        Assert-Different (Join-Path $baselineProducts $relative) (Join-Path $mutatedProducts $relative) "generated policy $relative"
    }

    $baselineFixtures = @{}
    foreach ($fixture in Get-ArchiveFixtures $baselineRoot) {
        $baselineFixtures[$fixture.Name] = Get-Hash $fixture.FullName
    }
    foreach ($fixture in Get-ArchiveFixtures $mutatedRoot) {
        if (-not $baselineFixtures.ContainsKey($fixture.Name)) {
            throw "mutation probe lost immutable archive fixture $($fixture.Name)"
        }
        $actual = Get-Hash $fixture.FullName
        if ($actual -ne $baselineFixtures[$fixture.Name]) {
            throw "immutable archive fixture changed: $($fixture.Name)"
        }
    }
    if ($baselineFixtures.Count -ne (Get-ArchiveFixtures $mutatedRoot).Count) {
        throw "mutation probe changed the immutable archive fixture set"
    }

    foreach ($relative in @(
        "rust/crates/harness/src/generated/harness-archived-v2.bin",
        "rust/crates/inference/inference_descriptor.bin",
        "rust/crates/machines/src/generated/acyclic-machines-v1.bin"
    )) {
        Assert-Same (Join-Path $baselineProducts $relative) (Join-Path $mutatedProducts $relative) "immutable product archive $relative"
    }
    Assert-Same (Join-Path $baselineRoot "rust/crates/sdk-contract-wire/tests/fixtures/harness-v2.descriptor.bin") (Join-Path $baselineProducts "rust/crates/harness/src/generated/harness-archived-v2.bin") "Harness archive fixture projection"
    Assert-Same (Join-Path $baselineRoot "rust/crates/sdk-contract-wire/tests/fixtures/inference-v1.descriptor.bin") (Join-Path $baselineProducts "rust/crates/inference/inference_descriptor.bin") "Inference archive fixture projection"
    Assert-Same (Join-Path $baselineRoot "rust/crates/sdk-contract-wire/tests/fixtures/machines-v1.descriptor.bin") (Join-Path $baselineProducts "rust/crates/machines/src/generated/acyclic-machines-v1.bin") "Machines archive fixture projection"

    $summary = [ordered]@{
        work_root = $workRoot
        baseline = "compiled Rust model projections"
        mutated = "field identity, API docs, and route changed in detached Rust source"
        changed = @(
            "actors/v1/actors.proto",
            "actors/v1/actors.fds.bin",
            "rust-authority.json",
            "rust-family-goldens.json",
            "actors.json",
            "all generated remote policy facades"
        )
        immutable_archive_fixtures = $baselineFixtures.Keys | Sort-Object
    }
    $summary | ConvertTo-Json -Depth 4
    Write-Host "Rust-source mutation probe passed."
} finally {
    if (-not $KeepWork -and (Test-Path -LiteralPath $workRoot)) {
        Remove-Item -LiteralPath $workRoot -Recurse -Force
    } elseif (Test-Path -LiteralPath $workRoot) {
        Write-Host "Retained mutation probe artifacts at $workRoot"
    }
}

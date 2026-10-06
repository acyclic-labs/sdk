[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $SourceRoot,
    [Parameter(Mandatory = $true)] [string] $TargetOutput,
    [Parameter(Mandatory = $true)] [string] $Request
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Full-Path([string] $Path) {
    return [System.IO.Path]::GetFullPath($Path)
}

function Require-Command([string] $Name) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        throw "$Name is required for the Rust producer"
    }
    return $command.Source
}

$SourceRoot = Full-Path $SourceRoot
$TargetOutput = Full-Path $TargetOutput
if (-not (Test-Path -LiteralPath $SourceRoot -PathType Container)) {
    throw "Rust source root was not found: $SourceRoot"
}
if ($TargetOutput.Equals($SourceRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
    $TargetOutput.StartsWith($SourceRoot.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase) -or
    $SourceRoot.StartsWith($TargetOutput.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Rust producer source and destination must be disjoint: $SourceRoot / $TargetOutput"
}
if (-not (Test-Path -LiteralPath $Request -PathType Leaf)) {
    throw "Rust producer request was not found: $Request"
}

$cargo = Require-Command 'cargo'
$tar = Require-Command 'tar'
$requestDocument = Get-Content -LiteralPath $Request -Raw | ConvertFrom-Json
New-Item -ItemType Directory -Force -Path $TargetOutput | Out-Null
$productOutput = Join-Path $TargetOutput 'rust-products'
$cargoTarget = Join-Path $TargetOutput '.cargo-target'
$crateOutput = Join-Path $TargetOutput 'crates'
New-Item -ItemType Directory -Force -Path $productOutput, $cargoTarget, $crateOutput | Out-Null

$previousCargoTarget = [Environment]::GetEnvironmentVariable('CARGO_TARGET_DIR', 'Process')
$env:CARGO_TARGET_DIR = $cargoTarget
try {
    & $cargo run --manifest-path (Join-Path $SourceRoot 'rust/crates/sdk-contract-wire/Cargo.toml') `
        --locked --bin sdk-contract-wire -- generate-products --root $SourceRoot --out $productOutput
    if ($LASTEXITCODE -ne 0) {
        throw "Rust authority product generation failed with exit code $LASTEXITCODE"
    }

    # The workspace currently contains path dependencies between the family
    # crates. Cargo's registry package command strips those paths and therefore
    # requires every dependency to already be published. Build an exact source
    # closure instead: the extracted bundle remains a normal Cargo workspace,
    # so consumers can use the family crates without registry publication.
    $packages = @(
        'acyclic-actors',
        'acyclic-fs',
        'acyclic-harness',
        'acyclic-inference',
        'acyclic-inference-contract',
        'acyclic-machines',
        'acyclic-native-runtime',
        'acyclic-objects',
        'acyclic-stream',
        'acyclic-workers'
    )
    $bundleName = 'acyclic-rust-sdk-0.2.0'
    $bundleRoot = Join-Path $TargetOutput $bundleName
    New-Item -ItemType Directory -Force -Path (Join-Path $bundleRoot 'rust/crates'), (Join-Path $bundleRoot 'proto'), (Join-Path $bundleRoot '.cargo') | Out-Null
    Copy-Item -LiteralPath (Join-Path $SourceRoot 'Cargo.toml') -Destination $bundleRoot -Force
    Copy-Item -LiteralPath (Join-Path $SourceRoot 'Cargo.lock') -Destination $bundleRoot -Force
    Copy-Item -LiteralPath (Join-Path $SourceRoot 'proto') -Destination $bundleRoot -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $SourceRoot '.cargo/config.toml') -Destination (Join-Path $bundleRoot '.cargo/config.toml') -Force
    $closureCrates = @(
        'actors', 'filesystem', 'harness', 'inference', 'inference-contract',
        'machines', 'native-runtime', 'objects', 'stream', 'workers',
        'sdk-contract-options', 'sdk-contract-validation', 'sdk-contract-wire'
    )
    foreach ($crate in $closureCrates) {
        Copy-Item -LiteralPath (Join-Path $SourceRoot "rust/crates/$crate") -Destination (Join-Path $bundleRoot "rust/crates/$crate") -Recurse -Force
    }
    $manifestPath = Join-Path $bundleRoot 'Cargo.toml'
    $manifest = Get-Content -LiteralPath $manifestPath -Raw
    $members = ($closureCrates | ForEach-Object { '  "' + "rust/crates/$_" + '",' }) -join "`n"
    $manifest = [regex]::Replace($manifest, '(?s)members = \[.*?\]\r?\nresolver', "members = [`n$members`n]`nresolver", 1)
    Set-Content -LiteralPath $manifestPath -Value $manifest -Encoding utf8NoBOM
    & $cargo generate-lockfile --manifest-path $manifestPath --offline
    if ($LASTEXITCODE -ne 0) {
        throw "Rust source closure lockfile generation failed with exit code $LASTEXITCODE"
    }
    $archive = Join-Path $crateOutput "$bundleName.tar.gz"
    & $tar --options 'gzip:!timestamp' --format pax --mtime '1970-01-01' --uid 0 --gid 0 --uname root --gname root `
        -czf $archive -C $TargetOutput $bundleName
    if ($LASTEXITCODE -ne 0) {
        throw "Rust source package archive failed with exit code $LASTEXITCODE"
    }
    $hash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
    $archives = @([ordered]@{
        name = (Split-Path -Leaf $archive)
        path = [System.IO.Path]::GetRelativePath($TargetOutput, $archive).Replace('\', '/')
        sha256 = $hash
        format = 'cargo-workspace-source'
    })
    $packageManifestPath = Join-Path $crateOutput 'rust-family-packages.json'
    $packageManifest = [ordered]@{
        schema = 'acyclic.sdk.rust-package-manifest.v2'
        bundle = [System.IO.Path]::GetRelativePath($TargetOutput, $archive).Replace('\', '/')
        install = 'extract the archive and run cargo add --path rust/crates/<family>'
        default_remote_transport = 'native'
        packages = @($packages | ForEach-Object { [string]$_ })
        source_closure_crates = $closureCrates
    }
    $packageManifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $packageManifestPath -Encoding utf8NoBOM

    $receipt = [ordered]@{
        schema = 'acyclic.sdk.rust-producer-receipt.v1'
        source_revision = [string]$requestDocument.source.revision
        source_digest = [string]$requestDocument.source.digest
        generator = 'acyclic-sdk-contract-wire generate-products'
        packages = $archives
        family_crates = $packages
        qualification = 'authority-products; registry publishing and conformance receipts remain separate'
    }
    $receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $TargetOutput 'generation-receipt.json') -Encoding utf8NoBOM
}
finally {
    if ($null -eq $previousCargoTarget) {
        Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
    } else {
        $env:CARGO_TARGET_DIR = $previousCargoTarget
    }
}

Write-Host "Generated Rust authority products and installable Cargo workspace bundle in $TargetOutput"

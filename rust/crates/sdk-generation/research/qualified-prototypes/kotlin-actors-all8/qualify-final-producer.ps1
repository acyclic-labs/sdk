[CmdletBinding()]
param(
    [string]$SourceRoot = 'Q:\sdk\work\sdkgen-main-actual03bb',
    [string]$Jar = 'C:\Users\varun\.codex\worktrees\rust-source-foundation\kotlin-current-lock-20261007\consumer\target\acyclic-actors-uniffi-kotlin-0.2.0.jar',
    [string]$OutputDirectory = ''
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'audit'
}

function Require-File([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "required source file is missing: $Path" }
}
function Sha256([string]$Path) { (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash }

$sourceFiles = @(
    'Cargo.toml', 'Cargo.lock', 'rust/crates/actors/Cargo.toml',
    'rust/crates/actors/src/lib.rs', 'rust/crates/actors/src/domain.rs',
    'rust/crates/actors/src/client.rs', 'rust/crates/actors/src/codegen.rs',
    'rust/crates/actors/src/contract.rs', 'rust/crates/actors/src/contract_definitions.rs',
    'rust/crates/actors/build.rs'
)
$hashes = [ordered]@{}
foreach ($relative in $sourceFiles) {
    $path = Join-Path $SourceRoot ($relative -replace '/', '\')
    Require-File $path
    $hashes[$relative] = Sha256 $path
}

$domain = Get-Content -Raw -LiteralPath (Join-Path $SourceRoot 'rust/crates/actors/src/domain.rs')
$rootsMatch = [regex]::Match($domain, 'export_roots!\((?<body>[\s\S]*?)\);')
if (-not $rootsMatch.Success) { throw 'domain.rs does not expose the producer-owned export_roots! list' }
$semanticRoots = @($rootsMatch.Groups['body'].Value -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })

$client = Get-Content -Raw -LiteralPath (Join-Path $SourceRoot 'rust/crates/actors/src/client.rs')
$operations = @([regex]::Matches($client, 'operation!\(\s*(?<name>[a-z_]+),') | ForEach-Object { $_.Groups['name'].Value })
if ($operations.Count -ne 8) { throw "expected eight producer-owned operations, found $($operations.Count)" }

$lib = Get-Content -Raw -LiteralPath (Join-Path $SourceRoot 'rust/crates/actors/src/lib.rs')
$routes = [ordered]@{}
foreach ($match in [regex]::Matches($lib, '\("(?<name>[^"]+)",\s*"(?<route>[^"]+)"\)')) {
    $routes[$match.Groups['name'].Value] = $match.Groups['route'].Value
}
if ($routes.Count -ne 8) { throw "expected eight producer-owned HTTP routes, found $($routes.Count)" }

$jarSha = $null
$jarEntries = @()
if (Test-Path -LiteralPath $Jar -PathType Leaf) {
    $jarSha = Sha256 $Jar
    $jarEntries = @(jar tf $Jar)
}
$resourceEntries = [ordered]@{
    'linux-x86-64/' = 'linux-x86-64/libacyclic_actors_uniffi.so'
    'win32-x86-64/' = 'win32-x86-64/acyclic_actors_uniffi.dll'
    'darwin-aarch64/' = 'darwin-aarch64/libacyclic_actors_uniffi.dylib'
}
$resourcePresence = [ordered]@{}
foreach ($prefix in $resourceEntries.Keys) {
    $resourcePresence[$prefix] = $jarEntries -contains $resourceEntries[$prefix]
}

$sourceCommit = (git -C $SourceRoot rev-parse HEAD).Trim()
$dirty = (@(git -C $SourceRoot status --porcelain=v1)).Count -gt 0
$uniffiPresent = Test-Path -LiteralPath (Join-Path $SourceRoot 'rust/crates/actors-uniffi') -PathType Container
$receipt = [ordered]@{
    schema = 'acyclic.kotlin.jvm.final-producer-source-receipt.v1'
    source_root = $SourceRoot
    source_commit = $sourceCommit
    source_worktree_dirty = $dirty
    source_files_sha256 = $hashes
    final_semantic_producer = [ordered]@{
        domain_module = 'rust/crates/actors/src/domain.rs'
        semantic_export_roots = $semanticRoots
        operation_methods = $operations
        http_routes = $routes
        extraction = @('domain.rs: export_roots! macro used by export_typescript', 'client.rs: operation! macro declarations', 'lib.rs: HTTP_ROUTES')
    }
    current_cutover_gap = [ordered]@{
        actors_uniffi_present = $uniffiPresent
        required_integration = @('add a maintained UniFFI facade that depends on acyclic-actors by path', 'replace duplicate Kotlin type and operation tables with producer metadata extraction', 'hash the producer closure and Cargo lock at generation time', 'derive Maven version from Cargo metadata and retain standard JNA resource roots')
    }
    exact_jar = [ordered]@{
        path = $Jar
        sha256 = $jarSha
        standard_jna_resource_prefixes = $resourcePresence
        no_custom_loader_claim = $true
    }
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$jsonPath = Join-Path $OutputDirectory 'kotlin-final-producer-source.receipt.json'
$rawPath = Join-Path $OutputDirectory 'kotlin-final-producer-source.raw.txt'
$receipt | ConvertTo-Json -Depth 12 | Set-Content -Encoding UTF8 -LiteralPath $jsonPath
$routeText = (($routes.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ';')
$raw = @('KOTLIN_FINAL_PRODUCER_SOURCE_RECEIPT_V1', "source_root=$SourceRoot", "source_commit=$sourceCommit", "source_worktree_dirty=$dirty", "domain_sha256=$($hashes['rust/crates/actors/src/domain.rs'])", "client_sha256=$($hashes['rust/crates/actors/src/client.rs'])", "lib_sha256=$($hashes['rust/crates/actors/src/lib.rs'])", "semantic_roots=$($semanticRoots -join ',')", "operations=$($operations -join ',')", "routes=$routeText", "actors_uniffi_present=$uniffiPresent", "jar_path=$Jar", "jar_sha256=$jarSha", "jna_linux=$($resourcePresence['linux-x86-64/'])", "jna_windows=$($resourcePresence['win32-x86-64/'])", "jna_macos=$($resourcePresence['darwin-aarch64/'])", "receipt_json=$jsonPath")
$raw | Set-Content -Encoding UTF8 -LiteralPath $rawPath
$raw | Write-Output

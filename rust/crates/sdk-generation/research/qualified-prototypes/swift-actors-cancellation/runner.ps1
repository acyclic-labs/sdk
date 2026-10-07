param(
    [string]$UniFFISource = 'Q:\sdk\work\uniffi-swift-oss-prototype\source',
    [string]$ProducerSource = '',
    [string]$ProducerWorkspace = '',
    [string]$ProducerLockPath = '',
    [string]$FixtureOptions = 'Q:\sdk\work\root-pending-actors-fixture-options.json',
    [string]$WorkRoot = 'Q:\sdk\work\swift-actors-cancellation-repro-20261007',
    [string]$ProducerTargetDir = '',
    [switch]$BuildOnly,
    [ValidateSet('ActorsConformanceConsumer', 'ActorsAll8ConformanceConsumer')]
    [string]$Product = 'ActorsConformanceConsumer'
)
$ErrorActionPreference = 'Stop'
$artifact = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $artifact '..\..\..\..\..\..')).Path
if ([string]::IsNullOrWhiteSpace($ProducerSource)) {
    $ProducerSource = Join-Path $repoRoot 'rust\crates\actors-uniffi'
}
$ProducerSource = (Resolve-Path -LiteralPath $ProducerSource).Path
if ([string]::IsNullOrWhiteSpace($ProducerWorkspace)) {
    $ProducerWorkspace = $repoRoot
}
$ProducerWorkspace = (Resolve-Path -LiteralPath $ProducerWorkspace).Path
if ([string]::IsNullOrWhiteSpace($ProducerLockPath)) {
    $ProducerLockPath = Join-Path $artifact 'producer-source.lock.json'
} elseif (!(Split-Path -IsAbsolute $ProducerLockPath)) {
    $ProducerLockPath = Join-Path $artifact $ProducerLockPath
}
$producerLockPath = (Resolve-Path -LiteralPath $ProducerLockPath).Path
if (!(Test-Path -LiteralPath $producerLockPath)) { throw "Missing producer source lock: $producerLockPath" }
$producerLock = Get-Content -LiteralPath $producerLockPath -Raw | ConvertFrom-Json
$expectedProducerFingerprint = $producerLock.fingerprint
$expectedProducerRoot = (Resolve-Path (Join-Path $ProducerWorkspace ($producerLock.producerRoot -replace '/', '\'))).Path
if ($ProducerSource.TrimEnd('\') -ne $expectedProducerRoot.TrimEnd('\')) {
    throw "ProducerSource must resolve to the locked final producer: expected $expectedProducerRoot, got $ProducerSource"
}
$templates = @{
    'Async.swift' = '830FB89B3C97713CA2C1CF844BE63E0A1AD5D458AF7A3C87A3E2FF58A2B5E0C6'
    'macros.swift' = '5EB1C64482514E8962099B42BE14BBC2C0B375570869CFE391006A77802ABDB4'
    'Helpers.swift' = '932944185A48F7C99C81E0A08C3F89304515D7C20D025F552875D8BB816A7E44'
}
foreach ($name in $templates.Keys) {
    $path = Join-Path $UniFFISource "uniffi_bindgen\src\bindings\swift\templates\$name"
    if (!(Test-Path -LiteralPath $path)) { throw "Missing pinned UniFFI template: $path" }
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash
    if ($actual -ne $templates[$name]) { throw "Pinned template hash mismatch for ${name}: $actual" }
}
$semanticGeneratorFiles = @{
    'uniffi_bindgen/src/bindings/swift/templates/CustomType.swift' = '30ECF288B0E70584214BF693A1E8EECF41DC2C3217E1BFA43767EEB785D66C84'
    'uniffi_bindgen/src/bindings/swift/templates/EnumTemplate.swift' = 'C05A024CBF06479C8DE4832E251C131E02382A32B885837043E7074080E9BDA7'
    'uniffi_bindgen/src/bindings/swift/gen_swift/mod.rs' = '589B8FA6AF259FF12B29F4F00A62935BEA9FE37927905A78582BC86DB75C190C'
    'uniffi_bindgen/src/bindings/swift/mod.rs' = '37BBA61731E61B439D9DDB0DD2F9FD34EBF0B2005D52FAAE5081A28CDF7E2259'
    'runner/src/main.rs' = '450E0DAF89518F734E2DF933F49680DBC38D724CCBCDC1BF8F6DE09161ECF975'
}
foreach ($relative in $semanticGeneratorFiles.Keys) {
    $path = Join-Path $UniFFISource ($relative -replace '/', '\')
    if (!(Test-Path -LiteralPath $path)) { throw "Missing pinned semantic generator source: $path" }
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToUpperInvariant()
    if ($actual -ne $semanticGeneratorFiles[$relative]) { throw "Pinned semantic generator hash mismatch for ${relative}: $actual" }
}
if (!(Test-Path -LiteralPath (Join-Path $ProducerSource 'Cargo.toml'))) {
    throw "ProducerSource is not the maintained actors-uniffi crate: $ProducerSource"
}
$producerFiles = @(
    Get-ChildItem -LiteralPath $ProducerSource -File |
        Where-Object { $_.Name -in @('Cargo.toml', 'Cargo.lock', 'uniffi.toml') }
    Get-ChildItem -LiteralPath (Join-Path $ProducerSource 'src') -Recurse -File -Filter '*.rs'
    Get-ChildItem -LiteralPath (Join-Path $ProducerWorkspace 'rust\crates\actors') -Recurse -File |
        Where-Object { $_.Extension -in @('.rs', '.toml') }
)
if ($producerFiles.Count -eq 0) { throw "No maintained producer source files found under $ProducerSource" }
$producerLines = $producerFiles |
    Sort-Object FullName |
    ForEach-Object {
        $relative = $_.FullName.Substring($ProducerWorkspace.Length + 1).Replace('\', '/')
        $hash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToUpperInvariant()
        "$relative`t$hash"
    }
$producerManifest = (($producerLines -join "`n") + "`n")
$producerStream = [IO.MemoryStream]::new([Text.Encoding]::UTF8.GetBytes($producerManifest))
$producerFingerprint = (Get-FileHash -InputStream $producerStream -Algorithm SHA256).Hash.ToUpperInvariant()
if ($producerFingerprint -ne $expectedProducerFingerprint) {
    throw "Maintained producer source fingerprint mismatch: expected $expectedProducerFingerprint, got $producerFingerprint"
}
$expectedByPath = @{}
foreach ($entry in $producerLock.files) { $expectedByPath[$entry.path] = $entry.sha256.ToUpperInvariant() }
if ($expectedByPath.Count -ne $producerLines.Count) {
    throw "Maintained producer source file set mismatch: lock has $($expectedByPath.Count), source has $($producerLines.Count)"
}
foreach ($line in $producerLines) {
    $parts = $line -split "`t", 2
    if (!$expectedByPath.ContainsKey($parts[0]) -or $expectedByPath[$parts[0]] -ne $parts[1]) {
        throw "Maintained producer source file hash mismatch: $($parts[0])"
    }
}

# Build the native artifact from this exact producer source. A caller cannot
# accidentally pair generated Swift with a stale or unrelated cdylib.
$producerTarget = if ([string]::IsNullOrWhiteSpace($ProducerTargetDir)) { Join-Path $WorkRoot 'producer-target' } else { $ProducerTargetDir }
& cargo build --manifest-path (Join-Path $ProducerSource 'Cargo.toml') --release --target-dir $producerTarget
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$RustDll = Join-Path $producerTarget 'release\acyclic_actors_uniffi.dll'
if (!(Test-Path -LiteralPath $RustDll)) { throw "Producer build did not emit the Rust cdylib: $RustDll" }
$package = Join-Path $WorkRoot 'consumer'
$generated = Join-Path $WorkRoot 'generated'
New-Item -ItemType Directory -Force "$package\Sources\ActorsConformanceConsumer","$package\Sources\ActorsAll8ConformanceConsumer","$package\Generated\AcyclicActors","$package\Generated\acyclic_actorsFFI\include","$package\Generated\acyclic_actors_uniffiFFI\include","$package\Native\windows-x86_64","$WorkRoot\build" | Out-Null
Copy-Item (Join-Path $artifact 'consumer\Package.swift') $package -Force
Copy-Item (Join-Path $artifact 'consumer\Sources\ActorsConformanceConsumer\main.swift') "$package\Sources\ActorsConformanceConsumer\main.swift" -Force
Copy-Item (Join-Path $artifact 'consumer\Sources\ActorsAll8ConformanceConsumer\main.swift') "$package\Sources\ActorsAll8ConformanceConsumer\main.swift" -Force
cargo run --manifest-path (Join-Path $UniFFISource 'runner\Cargo.toml') --target-dir (Join-Path $WorkRoot 'bindgen-target') -- $RustDll $generated acyclic_actors_uniffi (Join-Path $ProducerSource 'uniffi.toml')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if (!(Test-Path -LiteralPath (Join-Path $generated 'acyclic_actors_uniffi.swift'))) {
    throw 'Pinned Swift bindgen did not emit the generated Swift source'
}
$generatedSwift = Get-Content (Join-Path $generated 'acyclic_actors.swift') -Raw
foreach ($type in @('ActorId', 'CodeSha256', 'PositiveU64')) {
    if ($generatedSwift -match "public typealias $type =") {
        throw "Final producer emitted $type as a builtin Swift alias; nominal qualification is blocked"
    }
    if ($generatedSwift -notmatch "public struct $type\s*:") {
        throw "Generated Swift is missing the Rust-owned nominal $type semantic object"
    }
}
if ($generatedSwift -notmatch 'public struct ActorLimits\s*:\s*Equatable') {
    throw 'Generated Swift is missing the Rust-owned ActorLimits semantic object'
}
if ($generatedSwift -match 'public var handlerTimeoutMillis') {
    throw 'Final producer emitted mutable ActorLimits fields; immutable qualification is blocked'
}
if ($generatedSwift -notmatch 'fileprivate case currentHead\(Bool') {
    throw 'CurrentHead remains publicly forgeable in generated Swift'
}
if ($generatedSwift -notmatch 'public static func currentHead\(\) throws -> Start') {
    throw 'Generated Swift is missing the producer-validated CurrentHead factory'
}
if (!(Test-Path -LiteralPath (Join-Path $generated 'acyclic_actors.swift'))) {
    throw 'Pinned Swift bindgen did not emit the semantic Swift source'
}
$semanticSwift = Get-Content (Join-Path $generated 'acyclic_actors.swift') -Raw
$serviceSwift = Get-Content (Join-Path $generated 'acyclic_actors_uniffi.swift') -Raw
$serviceMarker = 'public protocol ActorsClientProtocol'
$serviceOffset = $serviceSwift.IndexOf($serviceMarker)
if ($serviceOffset -lt 0) { throw 'Generated service Swift is missing the ActorsClient surface' }
$swiftParts = @($semanticSwift, $serviceSwift.Substring($serviceOffset))
[IO.File]::WriteAllText((Join-Path $package 'Generated\AcyclicActors\Actors.swift'), ($swiftParts -join "`n"), [Text.UTF8Encoding]::new($false))
Copy-Item "$generated\acyclic_actors_uniffiFFI.h" "$package\Generated\acyclic_actors_uniffiFFI\include\acyclic_actors_uniffiFFI.h" -Force
Copy-Item "$generated\acyclic_actors_uniffi.modulemap" "$package\Generated\acyclic_actors_uniffiFFI\module.modulemap" -Force
Copy-Item (Join-Path $artifact 'consumer\Sources\acyclic_actors_uniffiFFI\ffi_anchor.c') "$package\Generated\acyclic_actors_uniffiFFI\ffi_anchor.c" -Force
Copy-Item "$generated\acyclic_actorsFFI.h" "$package\Generated\acyclic_actorsFFI\include\acyclic_actorsFFI.h" -Force
Set-Content -LiteralPath "$package\Generated\acyclic_actorsFFI\module.modulemap" -Value @('module acyclic_actorsFFI {','    header "acyclic_actorsFFI.h"','    export *','}') -Encoding ascii
Set-Content -LiteralPath "$package\Generated\acyclic_actors_uniffiFFI\module.modulemap" -Value @('module acyclic_actors_uniffiFFI {','    header "acyclic_actors_uniffiFFI.h"','    export *','}') -Encoding ascii
Set-Content -LiteralPath "$package\Generated\acyclic_actorsFFI\ffi_anchor.c" -Value '#include "acyclic_actorsFFI.h"' -Encoding ascii
$nativeImportLib = Join-Path (Split-Path $RustDll) 'acyclic_actors_uniffi.dll.lib'
if (!(Test-Path -LiteralPath $nativeImportLib)) {
    $nativeImportLib = Join-Path (Split-Path $RustDll) 'acyclic_actors_uniffi.lib'
}
if (!(Test-Path -LiteralPath $nativeImportLib)) { throw "Producer build did not emit a Windows import library beside $RustDll" }
Copy-Item $nativeImportLib "$package\Native\windows-x86_64\acyclic_actors_uniffi.lib" -Force
Copy-Item $RustDll "$package\Native\windows-x86_64\acyclic_actors_uniffi.dll" -Force
swift build --package-path $package --scratch-path (Join-Path $WorkRoot 'build') --product $Product
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($BuildOnly) { exit 0 }
$env:ACTORS_FIXTURE_OPTIONS = (Resolve-Path -LiteralPath $FixtureOptions).Path
& (Join-Path $WorkRoot "build\out\products\debug-windows-x86_64\$Product.exe")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

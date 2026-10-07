param(
    [string]$UniFFISource = 'Q:\sdk\work\uniffi-swift-oss-prototype\source',
    [string]$RustDll = 'Q:\sdk\work\actors-uniffi-all8-check-current-20261007\debug\acyclic_actors_uniffi.dll',
    [string]$FixtureOptions = 'Q:\sdk\work\root-pending-actors-fixture-options.json',
    [string]$WorkRoot = 'Q:\sdk\work\swift-actors-cancellation-repro-20261007'
)
$ErrorActionPreference = 'Stop'
$artifact = Split-Path -Parent $MyInvocation.MyCommand.Path
$templates = @{
    'Async.swift' = '830FB89B3C97713CA2C1CF844BE63E0A1AD5D458AF7A3C87A3E2FF58A2B5E0C6'
    'macros.swift' = '5EB1C64482514E8962099B42BE14BBC2C0B375570869CFE391006A77802ABDB4'
    'Helpers.swift' = '932944185A48F7C99C81E0A08C3F89304515D7C20D025F552875D8BB816A7E44'
}
foreach ($name in $templates.Keys) {
    $path = Join-Path $UniFFISource "uniffi_bindgen\src\bindings\swift\templates\$name"
    if (!(Test-Path -LiteralPath $path)) { throw "Missing pinned UniFFI template: $path" }
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash
    if ($actual -ne $templates[$name]) { throw "Pinned template hash mismatch for $name: $actual" }
}
if (!(Test-Path -LiteralPath $RustDll)) { throw "Missing Rust cdylib: $RustDll" }
$package = Join-Path $WorkRoot 'consumer'
$generated = Join-Path $WorkRoot 'generated'
New-Item -ItemType Directory -Force "$package\Sources\ActorsConformanceConsumer","$package\Generated\AcyclicActors","$package\Generated\acyclic_actors_uniffiFFI\include","$package\Native\windows-x86_64","$WorkRoot\build" | Out-Null
Copy-Item (Join-Path $artifact 'consumer\Package.swift') $package -Force
Copy-Item (Join-Path $artifact 'consumer\Sources\ActorsConformanceConsumer\main.swift') "$package\Sources\ActorsConformanceConsumer\main.swift" -Force
cargo run --manifest-path (Join-Path $UniFFISource 'runner\Cargo.toml') --target-dir (Join-Path $WorkRoot 'bindgen-target') -- $RustDll $generated acyclic_actors_uniffi
Copy-Item "$generated\acyclic_actors_uniffi.swift" "$package\Generated\AcyclicActors\Actors.swift" -Force
Copy-Item "$generated\acyclic_actors_uniffiFFI.h" "$package\Generated\acyclic_actors_uniffiFFI\include\acyclic_actors_uniffiFFI.h" -Force
Copy-Item "$generated\acyclic_actors_uniffi.modulemap" "$package\Generated\acyclic_actors_uniffiFFI\module.modulemap" -Force
Copy-Item (Join-Path (Split-Path $RustDll) 'acyclic_actors_uniffi.lib') "$package\Native\windows-x86_64\acyclic_actors_uniffi.lib" -Force
Copy-Item $RustDll "$package\Native\windows-x86_64\acyclic_actors_uniffi.dll" -Force
swift build --package-path $package --scratch-path (Join-Path $WorkRoot 'build') --product ActorsConformanceConsumer
$env:ACTORS_FIXTURE_OPTIONS = (Resolve-Path -LiteralPath $FixtureOptions).Path
& (Join-Path $WorkRoot 'build\out\products\debug-windows-x86_64\ActorsConformanceConsumer.exe')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

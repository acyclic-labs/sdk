param(
    [string]$ReceiptPath = (Join-Path $PSScriptRoot 'minimal-patched-generator-receipt.json')
)

$ErrorActionPreference = 'Stop'
$receipt = Get-Content -LiteralPath $ReceiptPath -Raw | ConvertFrom-Json

function Assert-File($path, $expectedHash, $expectedBytes) {
    if (!(Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Missing producer output: $path"
    }
    $item = Get-Item -LiteralPath $path
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash
    if ($actualHash -ne $expectedHash) {
        throw "SHA-256 mismatch for ${path}: expected $expectedHash, got $actualHash"
    }
    if ($item.Length -ne $expectedBytes) {
        throw "Byte-count mismatch for ${path}: expected $expectedBytes, got $($item.Length)"
    }
}

Assert-File $receipt.generator.archive.path $receipt.generator.archive.sha256 $receipt.generator.archive.bytes
Assert-File (Join-Path $PSScriptRoot 'uniffi-python-typing.patch') $receipt.generator.patch.sha256 $receipt.generator.patch.bytes

$archiveRoot = Join-Path (Split-Path $receipt.generator.archive.path) 'uniffi_bindgen-0.31.0'
foreach ($entry in $receipt.generator.archive_source_files.PSObject.Properties) {
    $path = Join-Path $archiveRoot $entry.Name
    Assert-File $path $entry.Value ((Get-Item -LiteralPath $path).Length)
}

Assert-File (Join-Path $PSScriptRoot '..\..\..\..\actors-uniffi\src\lib.rs') `
    $receipt.rust_facade_source.observed_checkout.'rust/crates/actors-uniffi/src/lib.rs'.sha256 `
    $receipt.rust_facade_source.observed_checkout.'rust/crates/actors-uniffi/src/lib.rs'.bytes
Assert-File (Join-Path $PSScriptRoot 'installed-all8-remote-conformance.py') `
    $receipt.qualification.consumer_source.sha256 `
    $receipt.qualification.consumer_source.bytes
foreach ($entry in $receipt.qualification.installed_receipts.PSObject.Properties) {
    Assert-File (Join-Path $PSScriptRoot (Join-Path 'installed-receipts' (Split-Path $entry.Value.path -Leaf))) `
        $entry.Value.sha256 $entry.Value.bytes
}

$python = $receipt.generated_outputs.python_module
Assert-File $python.windows_path $python.sha256 $python.bytes
Assert-File $python.linux_path $python.sha256 $python.bytes
foreach ($entry in $receipt.generated_outputs.PSObject.Properties) {
    if ($entry.Name -ne 'python_module') {
        $output = $entry.Value
        Assert-File $output.path $output.sha256 $output.bytes
    }
}

$pythonWindows = $python.windows_path
$pythonLinux = $python.linux_path
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $pythonWindows).Hash -ne
    (Get-FileHash -Algorithm SHA256 -LiteralPath $pythonLinux).Hash) {
    throw 'Generated Python module differs between Windows and Linux producer outputs'
}

Write-Output 'minimal patched UniFFI Python receipt: PASS'

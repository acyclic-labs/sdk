param(
    [Parameter(Mandatory = $true)]
    [string] $PackagePath
)

$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

$temporaryPath = "$PackagePath.reproducible"
if (Test-Path -LiteralPath $temporaryPath) {
    Remove-Item -LiteralPath $temporaryPath -Force
}

$source = [System.IO.Compression.ZipFile]::OpenRead($PackagePath)
$destination = [System.IO.Compression.ZipFile]::Open($temporaryPath, [System.IO.Compression.ZipArchiveMode]::Create)
try {
    $entries = $source.Entries | Sort-Object FullName
    foreach ($entry in $entries) {
        $name = $entry.FullName
        if ($name -match '^package/services/metadata/core-properties/[^/]+\.psmdcp$') {
            $name = 'package/services/metadata/core-properties/acyclic-sdk-transport.psmdcp'
        }

        $input = New-Object System.IO.MemoryStream
        try {
            $entryStream = $entry.Open()
            try { $entryStream.CopyTo($input) } finally { $entryStream.Dispose() }
            $bytes = $input.ToArray()
        } finally {
            $input.Dispose()
        }

        if ($name -eq '_rels/.rels') {
            $text = [System.Text.Encoding]::UTF8.GetString($bytes)
            $text = [regex]::Replace($text, '/package/services/metadata/core-properties/[^\"]+\.psmdcp', '/package/services/metadata/core-properties/acyclic-sdk-transport.psmdcp')
            $text = [regex]::Replace($text, 'Id="R[0-9A-F]+"', 'Id="RCOREPROPERTIES"')
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
        }

        $outputEntry = $destination.CreateEntry($name, [System.IO.Compression.CompressionLevel]::Optimal)
        $outputEntry.LastWriteTime = [DateTimeOffset]::Parse('2026-01-01T00:00:00Z')
        $outputStream = $outputEntry.Open()
        try { $outputStream.Write($bytes, 0, $bytes.Length) } finally { $outputStream.Dispose() }
    }
} finally {
    $destination.Dispose()
    $source.Dispose()
}

Move-Item -LiteralPath $temporaryPath -Destination $PackagePath -Force

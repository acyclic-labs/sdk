[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $Root,
    [string] $Archive,
    [switch] $ValidateOnly
)

$ErrorActionPreference = 'Stop'

function Assert-PortableTree([string] $Tree) {
    $rootPath = [IO.Path]::GetFullPath($Tree).TrimEnd([IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $rootPath -PathType Container)) {
        throw "Generated output directory is missing: $rootPath"
    }

    # Generated sources must not bind the artifact to the checkout that produced it.
    # Keep this check deliberately narrow so URLs and documented semantic timestamps
    # remain valid while drive- and home-directory paths fail closed.
    $utf8 = [Text.UTF8Encoding]::new($false, $true)
    $textExtensions = @('.c', '.cs', '.go', '.h', '.hpp', '.java', '.jl', '.json', '.md', '.pl', '.pm', '.ps1', '.psd1', '.py', '.rb', '.sh', '.swift', '.ts', '.txt', '.yaml', '.yml')
    foreach ($file in @(Get-ChildItem -LiteralPath $rootPath -Recurse -File | Sort-Object FullName)) {
        if ($textExtensions -notcontains $file.Extension.ToLowerInvariant() -or $file.Length -gt 16MB) { continue }
        try { $content = $utf8.GetString([IO.File]::ReadAllBytes($file.FullName)) }
        catch { continue }
        if ($content -match '(?im)(?:(?<![A-Za-z0-9])[A-Z]:[\\/]|(?:^|[^\w])(?:/Users/|/home/|/private/var/)|(?:\\Users\\))') {
            throw "Generated output contains a machine-local path: $($file.FullName)"
        }
    }
}

Assert-PortableTree $Root
if ($ValidateOnly) {
    Write-Output "Portable generated tree validated: $([IO.Path]::GetFullPath($Root))"
    return
}
if ([string]::IsNullOrWhiteSpace($Archive)) { throw 'Archive is required unless -ValidateOnly is supplied' }

$rootPath = [IO.Path]::GetFullPath($Root).TrimEnd([IO.Path]::DirectorySeparatorChar)
$archivePath = [IO.Path]::GetFullPath($Archive)
$archiveParent = Split-Path -Parent $archivePath
if (-not (Test-Path -LiteralPath $archiveParent -PathType Container)) {
    New-Item -ItemType Directory -Force -Path $archiveParent | Out-Null
}
if ($archivePath.StartsWith($rootPath + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Archive must be outside the generated root: $archivePath"
}
if (Test-Path -LiteralPath $archivePath) { Remove-Item -LiteralPath $archivePath -Force }

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$stream = [IO.File]::Open($archivePath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
try {
    $zip = [IO.Compression.ZipArchive]::new($stream, [IO.Compression.ZipArchiveMode]::Create, $false)
    try {
        $base = $rootPath + [IO.Path]::DirectorySeparatorChar
        foreach ($file in @(Get-ChildItem -LiteralPath $rootPath -Recurse -File | Sort-Object FullName)) {
            $relative = $file.FullName.Substring($base.Length).Replace('\', '/')
            $entry = $zip.CreateEntry($relative, [IO.Compression.CompressionLevel]::Optimal)
            $entry.LastWriteTime = [DateTimeOffset]::new(1980, 1, 1, 0, 0, 0, [TimeSpan]::Zero)
            $input = [IO.File]::OpenRead($file.FullName)
            try {
                $output = $entry.Open()
                try { $input.CopyTo($output) } finally { $output.Dispose() }
            } finally { $input.Dispose() }
        }
    } finally { $zip.Dispose() }
} finally { $stream.Dispose() }

Write-Output "Deterministic archive $archivePath SHA256 $((Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash)"

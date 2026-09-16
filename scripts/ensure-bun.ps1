$ErrorActionPreference = "Stop"

$version = "1.4.2"
if ($env:PROCESSOR_ARCHITECTURE -ne "AMD64") {
    throw "unsupported Bun host architecture: $env:PROCESSOR_ARCHITECTURE"
}
$target = "bun-windows-x64"
$expected = "ce4c17497b2f29712a99d3d53f028de28cd42e3bacb8589599e7f000e49b6405"
$expectedBinary = "15277c59ccd6c6c20f8dc9716c2b59c1776320d606b6a8658f70be8799519ca4"
$expectedBinaryBytes = 86096984
if ([string]::IsNullOrWhiteSpace($env:TOOLS_DIR)) {
    throw "TOOLS_DIR must identify the architecture-scoped CI tool cache"
}

$directory = Join-Path $env:TOOLS_DIR "bun\$version\$target"
$bun = Join-Path $directory "bun.exe"
$binaryIsValid = (Test-Path -LiteralPath $bun -PathType Leaf) -and `
    ((Get-Item -LiteralPath $bun).Length -eq $expectedBinaryBytes) -and `
    ((Get-FileHash -Algorithm SHA256 -LiteralPath $bun).Hash.ToLowerInvariant() -eq $expectedBinary)
if (-not $binaryIsValid) {
    $temporary = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString("N"))
    $archive = Join-Path $temporary "$target.zip"
    $extracted = Join-Path $temporary "extracted"
    New-Item -ItemType Directory -Path $temporary | Out-Null
    try {
        $maximumBytes = 134217728L
        $client = [System.Net.Http.HttpClient]::new()
        try {
            $client.DefaultRequestHeaders.UserAgent.ParseAdd("acyclic-sdk-ci/1")
            $response = $client.GetAsync(
                "https://github.com/oven-sh/bun/releases/download/bun-v$version/$target.zip",
                [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
            ).GetAwaiter().GetResult()
            $response.EnsureSuccessStatusCode() | Out-Null
            if ($null -ne $response.Content.Headers.ContentLength -and `
                $response.Content.Headers.ContentLength -gt $maximumBytes) {
                throw "Bun archive exceeds 128 MiB"
            }
            $source = $response.Content.ReadAsStream()
            $destination = [System.IO.File]::Open(
                $archive,
                [System.IO.FileMode]::CreateNew,
                [System.IO.FileAccess]::Write,
                [System.IO.FileShare]::None
            )
            try {
                $buffer = [byte[]]::new(65536)
                $received = 0L
                while (($read = $source.Read($buffer, 0, $buffer.Length)) -gt 0) {
                    $received += $read
                    if ($received -gt $maximumBytes) {
                        throw "Bun archive exceeds 128 MiB"
                    }
                    $destination.Write($buffer, 0, $read)
                }
            }
            finally {
                $destination.Dispose()
                $source.Dispose()
                $response.Dispose()
            }
        }
        finally {
            $client.Dispose()
        }
        $observedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash.ToLowerInvariant()
        if ($observedHash -ne $expected) {
            throw "Bun archive digest mismatch: $observedHash"
        }
        Expand-Archive -LiteralPath $archive -DestinationPath $extracted
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
        Copy-Item -LiteralPath (Join-Path $extracted "$target\bun.exe") -Destination "$bun.tmp"
        Move-Item -LiteralPath "$bun.tmp" -Destination $bun -Force
    }
    finally {
        Remove-Item -LiteralPath $temporary -Recurse -Force -ErrorAction SilentlyContinue
    }
}
if ((Get-Item -LiteralPath $bun).Length -ne $expectedBinaryBytes -or `
    (Get-FileHash -Algorithm SHA256 -LiteralPath $bun).Hash.ToLowerInvariant() -ne $expectedBinary) {
    throw "Bun executable digest mismatch"
}

$env:PATH = "$directory;$env:PATH"
if ([string]::IsNullOrWhiteSpace($env:BUN_INSTALL_CACHE_DIR)) {
    $env:BUN_INSTALL_CACHE_DIR = Join-Path $env:TOOLS_DIR "bun\install-cache"
}
New-Item -ItemType Directory -Force -Path $env:BUN_INSTALL_CACHE_DIR | Out-Null
$observedVersion = (& $bun --version).Trim()
if ($LASTEXITCODE -ne 0 -or $observedVersion -ne $version) {
    throw "unexpected Bun version: $observedVersion"
}

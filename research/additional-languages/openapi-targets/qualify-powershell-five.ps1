[CmdletBinding()]
param(
    [string]$Root,
    [switch]$SkipGeneration
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = (Resolve-Path (Join-Path (Join-Path $scriptDir '..') '..')).Path
}
$version = '7.25.0'
$jarSha256 = '41CE4F6B07F196676439D710759FA1CED7A08066D06FF1BF314681470289EFAE'
$families = @('actors', 'workers', 'stream', 'objects', 'inference')
$target = Join-Path $Root 'research/additional-languages/target/powershell'
$packages = Join-Path $target 'packages'
$installed = Join-Path $target 'installed'
$artifacts = Join-Path $target 'artifacts'
$jar = Join-Path $Root "research/additional-languages/target/bash/openapi-generator-cli-$version.jar"
$fixture = Join-Path $scriptDir 'powershell-fixture.py'
$consumer = Join-Path $scriptDir 'powershell-consumer-five.ps1'
$adapt = Join-Path $scriptDir 'apply-powershell-byte-adaptation.ps1'
$port = 18767

New-Item -ItemType Directory -Force -Path $target, $packages, $installed, $artifacts | Out-Null
if (-not (Test-Path -LiteralPath $jar)) { throw "Missing pinned OAG jar: $jar" }
if ((Get-FileHash $jar -Algorithm SHA256).Hash -ne $jarSha256) { throw 'OAG jar checksum mismatch' }

if (-not $SkipGeneration) {
    foreach ($family in $families) {
        $cap = $family.Substring(0, 1).ToUpperInvariant() + $family.Substring(1)
        $name = "Acyclic${cap}Http"
        $output = Join-Path $packages $family
        if (Test-Path -LiteralPath $output) { Remove-Item -LiteralPath $output -Recurse -Force }
        java '-Xmx768m' -jar $jar generate -i (Join-Path $Root "research/additional-languages/target/bash/$family.json") -g powershell -o $output --package-name $name --additional-properties="packageName=$name,packageVersion=1.0.0,packageGuid=00000000-0000-0000-0000-000000000001,licenseUri=https://www.apache.org/licenses/LICENSE-2.0" | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "PowerShell package generation failed: $family" }
        if ($family -eq 'workers') { & pwsh -NoProfile -File $adapt -GeneratedRoot $output }
        & pwsh -NoProfile -File (Join-Path $output 'Build.ps1') | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "PowerShell module build failed: $family" }
    }
}

$hashes = [ordered]@{}
foreach ($family in $families) {
    $package = Join-Path $packages $family
    $manifest = Get-ChildItem $package -Recurse -Filter '*.psd1' | Select-Object -First 1
    if (-not $manifest) { throw "Missing built PowerShell manifest: $family" }
    $zip = Join-Path $artifacts "acyclic-$family-powershell-1.0.0.zip"
    Remove-Item -LiteralPath $zip -Force -ErrorAction SilentlyContinue
    Compress-Archive -Path (Join-Path $package '*') -DestinationPath $zip -CompressionLevel Optimal -Force
    $hashes[$family] = (Get-FileHash $zip -Algorithm SHA256).Hash
    $install = Join-Path $installed $family
    if (Test-Path -LiteralPath $install) { Remove-Item -LiteralPath $install -Recurse -Force }
    Expand-Archive -LiteralPath $zip -DestinationPath $install -Force
}

$server = Start-Process -FilePath 'python' -ArgumentList @('-u', $fixture, $port) -PassThru -RedirectStandardOutput (Join-Path $target 'fixture.out') -RedirectStandardError (Join-Path $target 'fixture.err')
try {
    Start-Sleep -Milliseconds 800
    foreach ($family in $families) {
        $manifest = Get-ChildItem (Join-Path $installed $family) -Recurse -Filter '*.psd1' | Select-Object -First 1
        & pwsh -NoProfile -File $consumer -Family $family -ModuleManifest $manifest.FullName
        if ($LASTEXITCODE -ne 0) { throw "PowerShell installed consumer failed: $family" }
    }
} finally {
    Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
}

$combined = Join-Path $artifacts 'acyclic-http-powershell-1.0.0.zip'
Remove-Item -LiteralPath $combined -Force -ErrorAction SilentlyContinue
Compress-Archive -Path (Join-Path $installed '*') -DestinationPath $combined -CompressionLevel Optimal -Force
$summary = ($hashes.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '; '
Write-Output "PowerShell five-family HTTP qualification passed; packages=$summary; combined=$((Get-FileHash $combined -Algorithm SHA256).Hash)"

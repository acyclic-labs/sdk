$ErrorActionPreference = 'Stop'
$producer = Join-Path $PSScriptRoot 'build-dotnet-embedded-package.ps1'
$realCargo = (Get-Command cargo.exe -ErrorAction Stop).Source
$scratch = Join-Path ([IO.Path]::GetTempPath()) ('acyclic-embedded-single-rid-' + [guid]::NewGuid().ToString('N'))

function global:cargo {
  param([Parameter(ValueFromRemainingArguments = $true)][object[]]$Arguments)
  if ($Arguments -contains 'build') {
    $targetIndex = [Array]::IndexOf($Arguments, '--target')
    $targetDirIndex = [Array]::IndexOf($Arguments, '--target-dir')
    $target = [string]$Arguments[$targetIndex + 1]
    $targetDir = [string]$Arguments[$targetDirIndex + 1]
    $file = if ($target -like '*windows*') { 'acyclic_sdk_embedded_prototype.dll' }
      elseif ($target -like '*apple*') { 'libacyclic_sdk_embedded_prototype.dylib' }
      else { 'libacyclic_sdk_embedded_prototype.so' }
    $binary = Join-Path (Join-Path (Join-Path $targetDir $target) 'release') $file
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $binary) | Out-Null
    [IO.File]::WriteAllBytes($binary, [Text.Encoding]::UTF8.GetBytes("synthetic-$target"))
    return
  }
  & $script:realCargo @Arguments
}

try {
  New-Item -ItemType Directory -Force -Path $scratch | Out-Null
  $output = Join-Path $scratch 'output'
  & $producer -Output $output -Target 'x86_64-pc-windows-msvc' -NativeOnly | Out-Host
  & $producer -Output $output -Target 'aarch64-pc-windows-msvc' -NativeOnly | Out-Host
  $manifest = Get-Content -LiteralPath (Join-Path $output 'native/native-manifest.json') -Raw | ConvertFrom-Json
  $records = @($manifest.assets)
  if ($records.Count -ne 2) { throw "single-RID staging expected two merged records, found $($records.Count)" }
  if ((@($records | Select-Object -ExpandProperty rid) | Sort-Object) -join ',' -ne 'win-arm64,win-x64') {
    throw 'single-RID staging did not preserve both Windows RID records'
  }
  Write-Output 'SINGLE_RID_STAGING=PASS'
}
finally {
  Remove-Item Function:\cargo -ErrorAction SilentlyContinue
  if (Test-Path -LiteralPath $scratch) { Remove-Item -LiteralPath $scratch -Recurse -Force }
}

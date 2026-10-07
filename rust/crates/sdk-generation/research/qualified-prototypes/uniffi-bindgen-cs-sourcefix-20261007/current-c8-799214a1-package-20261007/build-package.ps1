param(
  [Parameter(Mandatory=$true)][string]$ManagedSource,
  [Parameter(Mandatory=$true)][string]$WindowsNative,
  [Parameter(Mandatory=$true)][string]$LinuxNative,
  [string]$MacNative
)
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$stage = Join-Path $here '.build'
Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path "$stage\src", "$stage\native\win-x64", "$stage\native\linux-x64", "$stage\out" | Out-Null
Copy-Item "$here\Acyclic.Actors.csproj", "$here\README.md" $stage
Copy-Item $ManagedSource "$stage\src\Acyclic.Actors.cs"
Copy-Item $WindowsNative "$stage\native\win-x64\acyclic_actors_uniffi.dll"
Copy-Item $LinuxNative "$stage\native\linux-x64\libacyclic_actors_uniffi.so"
if ($MacNative) {
  New-Item -ItemType Directory -Force -Path "$stage\native\osx-arm64" | Out-Null
  Copy-Item $MacNative "$stage\native\osx-arm64\libacyclic_actors_uniffi.dylib"
  $proj = Get-Content "$stage\Acyclic.Actors.csproj" -Raw
  $proj = $proj.Replace('</ItemGroup>', '<None Include="native\osx-arm64\libacyclic_actors_uniffi.dylib" Pack="true" PackagePath="runtimes\osx-arm64\native\libacyclic_actors_uniffi.dylib" /></ItemGroup>')
  Set-Content "$stage\Acyclic.Actors.csproj" $proj -Encoding utf8
}
$sdk = if ($env:DOTNET) { $env:DOTNET } else { 'dotnet' }
& $sdk restore "$stage\Acyclic.Actors.csproj" --ignore-failed-sources
& $sdk pack "$stage\Acyclic.Actors.csproj" -c Release --no-restore
if ($LASTEXITCODE) { throw "dotnet pack failed with exit code $LASTEXITCODE" }

param(
  [string]$Workflow = (Join-Path $PSScriptRoot '..\.github\workflows\embedded-native-packaging.yml')
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $Workflow -PathType Leaf)) {
  throw "embedded workflow is missing: $Workflow"
}

$text = Get-Content -Raw -LiteralPath $Workflow
$requiredJobs = @(
  'dotnet-installed-consumer:',
  'dotnet-installed-consumer-musl:',
  'jvm-installed-consumer:',
  'jvm-installed-consumer-musl:',
  'platform-runtime-proof:'
)
foreach ($job in $requiredJobs) {
  if ($text.IndexOf($job, [StringComparison]::Ordinal) -lt 0) {
    throw "installed consumer job is missing: $job"
  }
}

$rids = @(
  'win-x64', 'win-arm64', 'linux-x64', 'linux-arm64',
  'linux-musl-x64', 'linux-musl-arm64', 'osx-x64', 'osx-arm64'
)
foreach ($rid in $rids) {
  if ($text.IndexOf("rid: $rid", [StringComparison]::Ordinal) -lt 0) {
    throw "embedded consumer matrix is missing RID: $rid"
  }
}

foreach ($needle in @(
  'name: embedded-native-packages-${{ github.run_id }}-${{ github.run_attempt }}',
  'name: Embedded native packages',
  'source_revision',
  'acyclic-embedded-jna-0.1.0.jar',
  'Acyclic.Sdk.Embedded.0.2.0-alpha.1.nupkg',
  'rust:1.98.1-alpine3.22@sha256:',
  'mcr.microsoft.com/dotnet/sdk@sha256:',
  'apk add --no-cache openjdk17 maven',
  'Installed Rust-backed .NET consumer passed.',
  'Rust value did not round-trip'
)) {
  if ($text.IndexOf($needle, [StringComparison]::Ordinal) -lt 0) {
    throw "installed consumer workflow lost required check or package boundary: $needle"
  }
}

if ($text -match 'acyclic\.embedded\.native\.path["''=]') {
  throw 'consumer workflow configures an explicit native path; platform selection must remain automatic'
}
if ($text.IndexOf('artifact-aggregate-only', [StringComparison]::Ordinal) -ge 0) {
  throw 'workflow retains the old aggregate-only qualification status'
}

Write-Output 'embedded installed consumer workflow checks passed'

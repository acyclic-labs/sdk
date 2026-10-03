[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$GeneratedRoot
)

$ErrorActionPreference = 'Stop'
$modelDir = Join-Path $GeneratedRoot 'lib/model'
$models = @(
    'acyclic_workers_v1_error_code.dart',
    'acyclic_workers_v1_job_state.dart'
)
foreach ($name in $models) {
    $path = Join-Path $modelDir $name
    if (-not (Test-Path -LiteralPath $path)) { throw "Generated Dart model is missing: $path" }
    $text = Get-Content -LiteralPath $path -Raw
    $class = if ($name -like '*error_code*') { 'AcyclicWorkersV1ErrorCode' } else { 'AcyclicWorkersV1JobState' }
    $constructorAnchor = "$class({`r`n  });"
    $constructorReplacement = "$class();"
    if (-not $text.Contains($constructorAnchor)) {
        $constructorAnchor = $constructorAnchor.Replace("`r`n", "`n")
    }
    if (-not $text.Contains($constructorAnchor)) { throw "Dart empty-constructor anchor changed: $path" }
    $text = $text.Replace($constructorAnchor, $constructorReplacement)
    $anchor = "  bool operator ==(Object other) => identical(this, other) || other is $class &&`r`n`r`n  @override`r`n  int get hashCode =>`r`n    // ignore: unnecessary_parenthesis`r`n"
    $replacement = "  bool operator ==(Object other) => identical(this, other) || other is $class;`r`n`r`n  @override`r`n  int get hashCode => runtimeType.hashCode;`r`n"
    if (-not $text.Contains($anchor)) {
        $anchor = $anchor.Replace("`r`n", "`n")
        $replacement = $replacement.Replace("`r`n", "`n")
    }
    if (-not $text.Contains($anchor)) { throw "Dart empty-enum anchor changed: $path" }
    Set-Content -LiteralPath $path -Value $text.Replace($anchor, $replacement) -NoNewline
}

$pubspec = Join-Path $GeneratedRoot 'pubspec.yaml'
if (-not (Test-Path -LiteralPath $pubspec)) { throw "Generated Dart pubspec is missing: $pubspec" }
$pubspecText = Get-Content -LiteralPath $pubspec -Raw
$licenseAnchor = "homepage: 'homepage'"
if (-not $pubspecText.Contains($licenseAnchor)) { throw 'Dart pubspec license anchor changed.' }
if (-not $pubspecText.Contains("license: 'Apache-2.0'")) {
    $pubspecText = $pubspecText.Replace($licenseAnchor, "$licenseAnchor`r`nlicense: 'Apache-2.0'")
}
$testAnchor = "  test: '>=1.21.6 <1.22.0'"
$testReplacement = "  test: '>=1.31.0 <2.0.0'"
if (-not $pubspecText.Contains($testAnchor)) { throw 'Dart test dependency anchor changed; refusing an unreviewed adaptation.' }
$pubspecText = $pubspecText.Replace($testAnchor, $testReplacement)
Set-Content -LiteralPath $pubspec -Value $pubspecText -NoNewline
$smoke = Join-Path $GeneratedRoot 'tool/rust_owned_package_smoke.dart'
if (-not (Test-Path -LiteralPath (Split-Path -Parent $smoke))) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $smoke) | Out-Null
}
@'
import 'package:openapi/api.dart';

void main() {
  final request = AcyclicWorkersV1InvokeDeploymentRequest(body: 'AQID');
  if (request.toJson()['body'] != 'AQID') {
    throw StateError('generated request bytes did not retain base64 JSON');
  }
  final response = AcyclicWorkersV1InvokeResponse(
    body: 'b2s=',
    resolvedRevision: '18446744073709551615',
    resolvedSha256: 'AQID',
    status: 200,
  );
  final decoded = AcyclicWorkersV1InvokeResponse.fromJson(response.toJson());
  if (decoded?.body != 'b2s=' ||
      decoded?.resolvedRevision != '18446744073709551615' ||
      decoded?.resolvedSha256 != 'AQID' ||
      decoded?.status != 200) {
    throw StateError('generated response JSON round trip changed bytes or uint64');
  }
  if (AcyclicWorkersV1ErrorCode().toJson().isNotEmpty ||
      AcyclicWorkersV1JobState().toJson().isNotEmpty) {
    throw StateError('generated empty enum models were not adapted');
  }
}
'@ | Set-Content -LiteralPath $smoke -NoNewline
Write-Output "Applied Rust-owned Workers Dart runtime/test adaptation to $GeneratedRoot"

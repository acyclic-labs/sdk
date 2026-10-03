[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string] $GeneratedRoot
)

$ErrorActionPreference = 'Stop'
$path = Join-Path $GeneratedRoot 'model/acyclic_workers_v1_job_observation.c'
if (-not (Test-Path -LiteralPath $path)) {
  throw "Rust-owned Workers C target is missing generated anchor: $path"
}
$text = Get-Content -LiteralPath $path -Raw
$old = @'
    acyclic_workers_v1_job_observation_t *result = acyclic_workers_v1_job_observation_create_internal (
        attempt_copy,
        cancellation_requested_copy,
        failure_code,
        job_id,
        resolved_sha256,
        result,
        state
        );
    if (!result) {
        free(attempt_copy);
        free(cancellation_requested_copy);
    }
    return result;
'@
$new = @'
    acyclic_workers_v1_job_observation_t *result_local_var = acyclic_workers_v1_job_observation_create_internal (
        attempt_copy,
        cancellation_requested_copy,
        failure_code,
        job_id,
        resolved_sha256,
        result,
        state
        );
    if (!result_local_var) {
        free(attempt_copy);
        free(cancellation_requested_copy);
    }
    return result_local_var;
'@
if (-not $text.Contains($old)) {
  throw "Refusing C adaptation: expected OAG result-name collision anchor was not found."
}
$updated = $text.Replace($old, $new)
Set-Content -LiteralPath $path -Value $updated -Encoding utf8
Write-Output "Applied Rust-owned Workers C compile adaptation to $GeneratedRoot"

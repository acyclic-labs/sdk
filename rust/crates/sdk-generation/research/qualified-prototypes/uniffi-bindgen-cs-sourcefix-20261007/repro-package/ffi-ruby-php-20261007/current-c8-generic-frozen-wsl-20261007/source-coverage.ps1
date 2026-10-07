param([string]$Root = $PSScriptRoot)
$records = Get-Content -Raw (Join-Path $Root "acyclic_actors.rb")
$ffi = Get-Content -Raw (Join-Path $Root "acyclic_actors_uniffi.rb")
$requiredRecords = @("class ActorId", "class CodeSha256", "class PositiveU64", "class CurrentHeadMarker", "class CurrentHeadMarker", "freeze", "field_0")
$requiredOps = @("def create_actor", "def update_actor", "def inspect_actor", "def add_subscription", "def remove_subscription", "def resume_subscription", "def checkpoint_actor", "def invoke_actor")
foreach ($needle in $requiredRecords) { if ($records.IndexOf($needle) -lt 0) { throw "missing records marker: $needle" } }
foreach ($needle in $requiredOps) { if ($ffi.IndexOf($needle) -lt 0) { throw "missing operation marker: $needle" } }
if ($ffi.IndexOf("uniffi_rust_call_async") -lt 0) { throw "missing maintained async scheduler" }
Write-Output "records_and_ffi_source_coverage=PASS"
Write-Output "nominal_types=ActorId,CodeSha256,PositiveU64,CurrentHeadMarker"
Write-Output "operations=8"
Write-Output "async_scheduler=uniffi_rust_call_async"
Write-Output "immutable_value_objects=freeze"

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('actors', 'workers', 'stream', 'objects', 'inference')]
    [string]$Family,
    [Parameter(Mandatory = $true)]
    [string]$ModuleManifest,
    [string]$BaseUrl = 'http://127.0.0.1:18767'
)

$ErrorActionPreference = 'Stop'
Import-Module -Force $ModuleManifest
Set-Configuration -BaseUrl $BaseUrl -AccessToken 'powershell-fixture-token'
$max = '18446744073709551615'

switch ($Family) {
    'actors' {
        $request = Initialize-AcyclicActorsV1InvokeActorRequest -ActorId 'actor-1' -Body 'AQID' -Method 'POST' -Url 'https://example.test'
        $result = Invoke-Actor -AcyclicActorsV1InvokeActorRequest $request -WithHttpInfo
        if ($result.StatusCode -ne 200 -or $result.Response.body -ne 'AQID') { throw 'Actors bytes/status mismatch' }
    }
    'workers' {
        $request = Initialize-AcyclicWorkersV1InvokeDeploymentRequest -Alias 'prod' -Body ([byte[]](1, 2, 3)) -Method 'POST' -Url 'https://example.test'
        if ($request.body -ne 'AQID') { throw 'Workers Rust-owned byte adaptation did not emit AQID' }
        $result = Invoke-Deployment -Alias 'prod' -AcyclicWorkersV1InvokeDeploymentRequest $request -WithHttpInfo
        if ($result.StatusCode -ne 200 -or [Convert]::ToBase64String($result.Response.body) -ne 'b2s=') { throw 'Workers bytes/status mismatch' }
        if ([Convert]::ToBase64String($result.Response.resolvedSha256) -ne 'AQID' -or $result.Response.resolvedRevision -ne $max) { throw 'Workers digest/uint64 mismatch' }
    }
    'stream' {
        $request = Initialize-AcyclicStreamV2ReadRequest -Path 'root' -Limit 1
        $result = & 'Read-' -AcyclicStreamV2ReadRequest $request -WithHttpInfo
        if ($result.StatusCode -ne 200 -or $result.Response.record.value -ne 'AQID' -or $result.Response.record.sequence -ne $max) { throw 'Stream record/uint64 mismatch' }
        $errorSeen = $false
        try { & 'Read-' -AcyclicStreamV2ReadRequest (Initialize-AcyclicStreamV2ReadRequest -Path 'root' -Limit 0) -WithHttpInfo | Out-Null } catch { $errorSeen = $true }
        if (-not $errorSeen) { throw 'Stream 503 did not raise generated API error' }
    }
    'objects' {
        $request = Initialize-AcyclicObjectsV2PutObjectRequest -Body 'AQID' -Complete $true
        $result = Send-Object -AcyclicObjectsV2PutObjectRequest $request -WithHttpInfo
        if ($result.StatusCode -ne 200 -or $result.Response.etag -ne 'etag-powershell' -or $result.Response.size -ne '3') { throw 'Objects response mismatch' }
    }
    'inference' {
        $request = Initialize-InferenceCustomerV1GenerateRunRequest -Context 'AQID' -MaximumOutput $max
        $result = Invoke-RunsGenerate -InferenceCustomerV1GenerateRunRequest $request -WithHttpInfo
        if ($result.StatusCode -ne 200 -or $result.Response.run.input -ne 'AQID' -or $result.Response.run.lastSequence -ne $max) { throw 'Inference bytes/uint64 mismatch' }
    }
}

Write-Output "powershell-consumer family=$Family status=passed"

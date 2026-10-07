param(
    [Parameter(Mandatory = $true)][string]$PackageRoot,
    [Parameter(Mandatory = $true)][string]$AllEightOptions,
    [Parameter(Mandatory = $true)][string]$PendingOptions
)

$ErrorActionPreference = 'Stop'
trap { Write-Output ("TRACE_ERROR line={0} message={1}" -f $_.InvocationInfo.ScriptLineNumber, $_.Exception.Message); throw }
$managed = Join-Path $PackageRoot 'lib/net8.0/Acyclic.Actors.dll'
$native = Join-Path $PackageRoot 'runtimes/win-x64/native'
$env:PATH = "$native;$env:PATH"
$assembly = [Reflection.Assembly]::LoadFrom($managed)

function New-Managed([string]$Name, [object[]]$Arguments = @()) {
    $type = $assembly.GetType("Acyclic.Actors.$Name", $true)
    $lastError = $null
    foreach ($constructor in $type.GetConstructors([Reflection.BindingFlags]'Public,Instance')) {
        if ($constructor.GetParameters().Count -ne $Arguments.Count) { continue }
        try { return $constructor.Invoke($Arguments) } catch { $lastError = $_.Exception.ToString() }
    }
    throw "constructor not found: $Name ($lastError)"
}

function Await([object]$Task) {
    return $Task.GetAwaiter().GetResult()
}

function Invoke-Method([object]$Target, [string]$Name, [object[]]$Arguments) {
    $method = $Target.GetType().GetMethods([Reflection.BindingFlags]'Public,Instance') |
        Where-Object Name -eq $Name | Select-Object -First 1
    if ($null -eq $method) { throw "method not found: $Name" }
    return Await ($method.Invoke($Target, $Arguments))
}

function Byte-Array([byte[]]$Bytes) { return $Bytes }

$all8 = Get-Content -LiteralPath $AllEightOptions -Raw | ConvertFrom-Json
$connect = $assembly.GetType('Acyclic.Actors.AcyclicActorsUniffiMethods').GetMethods([Reflection.BindingFlags]'Public,Static') | Where-Object Name -eq 'ConnectActorsWithCa' | Select-Object -First 1
$client = Await ($connect.Invoke($null, [object[]]@(
    $all8.endpoint,
    $all8.token,
    [Text.Encoding]::UTF8.GetBytes([string]$all8.caCertificate),
    $null,
    [Threading.CancellationToken]::None
)))
Write-Output 'CHECKPOINT_CONNECTED'

$actorId = New-Managed 'ActorId' ([object[]]@('actor-a'))
$digest = New-Managed 'CodeSha256' ([object[]]@(,([byte[]](1..32))))
$limits = New-Managed 'ActorLimits' ([object[]]@([UInt64]1, [UInt64]2, [UInt64]3))
$binding = New-Managed 'Binding' ([object[]]@('binding-a', 'capability-a', 'resource-a'))
$bindings = [Array]::CreateInstance($binding.GetType(), 1); $bindings.SetValue($binding, 0)
$cursor = New-Managed 'SubscriptionStart+Cursor' ([object[]]@([UInt64]9007199254740993))
$start = $cursor
$subscription = New-Managed 'SubscriptionSpec' ([object[]]@('subscription-a', 'events/input', $start, $true))
$subscriptions = [Array]::CreateInstance($subscription.GetType(), 1); $subscriptions.SetValue($subscription, 0)
$create = New-Managed 'CreateActorRequest' ([object[]]@($digest, 'eu', $bindings, $limits, $subscriptions, 'csharp-create-a'))
$update = New-Managed 'UpdateActorRequest' ([object[]]@($actorId, $digest, $bindings, $limits, [UInt64]0, 'csharp-update-a'))
$inspect = New-Managed 'InspectActorRequest' ([object[]]@($actorId))
$add = New-Managed 'AddSubscriptionRequest' ([object[]]@($actorId, $subscription, 'csharp-add-a'))
$remove = New-Managed 'RemoveSubscriptionRequest' ([object[]]@($actorId, 'subscription-a', 'csharp-remove-a'))
$resume = New-Managed 'ResumeSubscriptionRequest' ([object[]]@($actorId, 'subscription-a', 'csharp-resume-a'))
$checkpoint = New-Managed 'CheckpointActorRequest' ([object[]]@($actorId, 'checkpoint-a'))
$header = New-Managed 'Header' ([object[]]@('content-type', 'application/json'))
$headers = [Array]::CreateInstance($header.GetType(), 1); $headers.SetValue($header, 0)
$body = [byte[]][Text.Encoding]::UTF8.GetBytes('request-body')
$invoke = New-Managed 'InvokeActorRequest' ([object[]]@($actorId, 'POST', '/invoke', [object]$body, $headers))
Write-Output 'CHECKPOINT_REQUESTS'

Invoke-Method $client 'CreateActor' ([object[]]@($create, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'UpdateActor' ([object[]]@($update, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'InspectActor' ([object[]]@($actorId, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'InspectActorRequest' ([object[]]@($inspect, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'AddSubscription' ([object[]]@($add, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'RemoveSubscription' ([object[]]@($remove, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'ResumeSubscription' ([object[]]@($resume, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'CheckpointActor' ([object[]]@($checkpoint, $null, [Threading.CancellationToken]::None))
Invoke-Method $client 'InvokeActor' ([object[]]@($invoke, $null, [Threading.CancellationToken]::None))
Write-Output 'NUPKG_CORRECTED_ALL8_RUNTIME=PASS'
Write-Output 'CHECKPOINT_ALL8'

$pending = Get-Content -LiteralPath $PendingOptions -Raw | ConvertFrom-Json
$pendingConnect = $assembly.GetType('Acyclic.Actors.AcyclicActorsUniffiMethods').GetMethods([Reflection.BindingFlags]'Public,Static') | Where-Object Name -eq 'ConnectActorsWithCa' | Select-Object -First 1
$pendingClient = Await ($pendingConnect.Invoke($null, [object[]]@(
    $pending.endpoint,
    $pending.token,
    [Text.Encoding]::UTF8.GetBytes([string]$pending.caCertificate),
    $null,
    [Threading.CancellationToken]::None
)))
Write-Output 'CHECKPOINT_PENDING_CONNECTED'
$runtime = $assembly.GetType('Acyclic.Actors._UniFFIAsync')
$map = $runtime.GetField('_async_handle_map', [Reflection.BindingFlags]'Static,NonPublic').GetValue($null)
$storage = $map.GetType().GetField('_map', [Reflection.BindingFlags]'Instance,NonPublic').GetValue($map)
$state0 = (Invoke-RestMethod ($pending.controlEndpoint + '/state'))
$baseline = $storage.Count
$peaks = @()
$http = [Net.Http.HttpClient]::new()
for ($i = 0; $i -lt 3; $i++) {
    $id = New-Managed 'ActorId' ([object[]]@("pending-csharp-corrected-$i"))
    $cts = [Threading.CancellationTokenSource]::new()
    $operation = $pendingClient.GetType().GetMethod('InspectActor').Invoke($pendingClient, [object[]]@($id, $null, $cts.Token))
    $seen = $false
    for ($attempt = 0; $attempt -lt 200; $attempt++) {
        $current = Invoke-RestMethod ($pending.controlEndpoint + '/state')
        if ($current.started -ge ($i + 1) -and $current.active -eq 1) { $seen = $true; break }
        Start-Sleep -Milliseconds 10
    }
    if (-not $seen) { throw "pending server did not reach active gate $i" }
    $peaks += $storage.Count
    $cts.Cancel()
    $canceled = $false
    try { Await $operation | Out-Null } catch [OperationCanceledException] { $canceled = $_.Exception.CancellationToken -eq $cts.Token }
    if (-not $canceled) { throw 'request token did not produce OperationCanceledException' }
    $after = $null
    for ($attempt = 0; $attempt -lt 200; $attempt++) {
        $after = Invoke-RestMethod ($pending.controlEndpoint + '/state')
        if ($after.aborted -ge ($i + 1) -and $after.active -eq 0) { break }
        Start-Sleep -Milliseconds 10
    }
    if ($storage.Count -ne 0) { throw "continuation map leak: $($storage.Count)" }
    $cts.Dispose(); $id.Dispose()
}
Write-Output ("NUPKG_CORRECTED_CANCELLATION_RUNTIME=PASS baseline={0} peaks={1} final={2}/{3}/{4}" -f $baseline, ($peaks -join ','), $after.started, $after.aborted, $after.active)

$identity = $assembly.GetName()
$positive = $assembly.GetType('Acyclic.Actors.PositiveU64')
$raw = $positive.GetConstructors([Reflection.BindingFlags]'NonPublic,Instance') | % ToString
$public = $positive.GetConstructors([Reflection.BindingFlags]'Public,Instance') | % ToString
Write-Output ("NUPKG_CORRECTED_IDENTITY={0} VERSION={1} TYPES={2}" -f $identity.Name, $identity.Version, $assembly.GetTypes().Count)
Write-Output ("NUPKG_CORRECTED_POSITIVE_PUBLIC={0}" -f ($public -join ';'))
Write-Output ("NUPKG_CORRECTED_POSITIVE_RAW={0}" -f ($raw -join ';'))

param(
    [Parameter(Mandatory = $true)][string]$TracePath
)
$ErrorActionPreference = 'Stop'
$trace = Get-Content -LiteralPath $TracePath -Raw | ConvertFrom-Json
if ($null -eq $trace) { throw 'Trace is empty.' }
if ($trace -isnot [System.Array]) { $trace = @($trace) }

$admitted = @{}
$started = @{}
$completed = @{}
$messages = @{}
$waits = @{}
$published = @{}
$activeAgents = [System.Collections.Generic.HashSet[int]]::new()
$null = $activeAgents.Add(1)

function Require([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "trace conformance: $Message" }
}

function IsDirect([int]$Left, [int]$Right) {
    return (($Left -eq 1 -and ($Right -eq 2 -or $Right -eq 3)) -or
            ($Right -eq 1 -and ($Left -eq 2 -or $Left -eq 3)) -or
            ($Left -eq 2 -and $Right -eq 4) -or
            ($Right -eq 2 -and $Left -eq 4))
}

foreach ($event in $trace) {
    Require ($null -ne $event.kind) 'event kind is required.'
    switch ($event.kind) {
        'fork_admitted' {
            Require ($null -ne $event.operation_id) 'fork operation_id is required.'
            Require (-not $admitted.ContainsKey([string]$event.operation_id)) 'fork operation identity was reused.'
            Require ([int]$event.parent -ne [int]$event.child) 'fork parent and child must differ.'
            Require (IsDirect ([int]$event.parent) ([int]$event.child)) 'fork is not a direct-parent operation.'
            Require ([int]$event.depth -ge 0 -and [int]$event.depth -le 2) 'fork depth exceeds the bounded model.'
            Require ($activeAgents.Count -lt 4) 'fork exceeds the bounded session allocation.'
            Require ($null -ne $event.captured_generation) 'fork captured generation is required.'
            $admitted[[string]$event.operation_id] = $event
            $null = $activeAgents.Add([int]$event.child)
        }
        'model_started' {
            $op = [string]$event.operation_id
            Require ($admitted.ContainsKey($op)) 'model started without a fork admission.'
            Require (-not $started.ContainsKey($op)) 'model operation was started twice.'
            $started[$op] = $event
        }
        'agent_completed' {
            $op = [string]$event.operation_id
            Require ($started.ContainsKey($op)) 'agent completed without model start.'
            Require ([bool]$event.outcome_durable) 'agent completion lacks a durable outcome.'
            Require (-not $completed.ContainsKey($op)) 'agent completion was published twice.'
            $completed[$op] = $event
        }
        'message_admitted' {
            $id = [string]$event.message_id
            Require ($id.Length -gt 0) 'message identity is required.'
            Require (-not $messages.ContainsKey($id)) 'message identity was reused.'
            Require (IsDirect ([int]$event.sender) ([int]$event.recipient)) 'message authority is not direct parent/child.'
            $messages[$id] = [ordered]@{ event = $event; delivered = $false }
        }
        'message_delivered' {
            $id = [string]$event.message_id
            Require ($messages.ContainsKey($id)) 'message delivered without admission.'
            Require (-not $messages[$id].delivered) 'message was delivered more than once.'
            Require ([int]$event.delivery_index -eq 1) 'delivery order is not the first durable delivery.'
            $messages[$id].delivered = $true
        }
        'wait_observed' {
            $id = [string]$event.wait_id
            Require ($id.Length -gt 0) 'wait identity is required.'
            Require (-not $waits.ContainsKey($id)) 'wait identity was reused.'
            Require (IsDirect ([int]$event.waiter) ([int]$event.target)) 'wait target is outside direct-parent scope.'
            Require ([bool]$event.target_completed) 'wait observed a target without a durable completion.'
            $waits[$id] = $event
        }
        'workspace_published' {
            $op = [string]$event.operation_id
            Require ($completed.ContainsKey($op)) 'workspace publication lacks durable completion.'
            Require (IsDirect ([int]$event.parent) ([int]$event.child)) 'publication target is not the direct parent.'
            Require ([int]$event.captured_generation -eq [int]$event.current_generation) 'publication crossed a stale workspace generation.'
            Require (-not $published.ContainsKey($op)) 'workspace publication was repeated.'
            $published[$op] = $event
        }
        default { throw "trace conformance: unsupported event kind '$($event.kind)'." }
    }
}

Write-Output (ConvertTo-Json ([ordered]@{
    valid = $true
    event_count = $trace.Count
    fork_admissions = $admitted.Count
    model_starts = $started.Count
    completions = $completed.Count
    messages = $messages.Count
    waits = $waits.Count
    publications = $published.Count
}) -Compress)

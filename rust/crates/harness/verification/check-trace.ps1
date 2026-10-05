param(
    [Parameter(Mandatory = $true)][string]$TracePath
)
$ErrorActionPreference = 'Stop'

$trace = Get-Content -LiteralPath $TracePath -Raw | ConvertFrom-Json
if ($null -eq $trace) { throw 'Trace is empty.' }
if ($trace -isnot [System.Array]) { $trace = @($trace) }

# This is the finite event projection used by the scheduler/publication models:
# root=1, children=2/3/5, and grandchild=4 (child 2's child).
$parentOf = @{ 1 = 1; 2 = 1; 3 = 1; 4 = 2; 5 = 1 }
$depthOf = @{ 1 = 0; 2 = 1; 3 = 1; 4 = 2; 5 = 1 }
$sessionBudget = 3
$generationBound = 1

$admitted = @{}
$admittedByChild = @{}
$started = @{}
$completed = @{}
$completedAgents = @{}
$messages = @{}
$waits = @{}
$published = @{}
$pendingPublications = @{}
$activeAgents = [System.Collections.Generic.HashSet[int]]::new()
$null = $activeAgents.Add(1)
$workspaceGeneration = @{ 1 = 0; 2 = 0; 3 = 0; 4 = 0; 5 = 0 }

function Require([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "trace conformance: $Message" }
}

function RequireString([object]$Value, [string]$Name) {
    Require ($null -ne $Value) "$Name is required."
    Require ($Value -is [string]) "$Name must be a string."
    Require (-not [string]::IsNullOrWhiteSpace([string]$Value)) "$Name must not be empty."
    return [string]$Value
}

function RequireInt([object]$Value, [string]$Name, [int]$Minimum, [int]$Maximum) {
    Require ($null -ne $Value) "$Name is required."
    Require ($Value -is [byte] -or $Value -is [int16] -or $Value -is [int32] -or
             $Value -is [int64] -or $Value -is [decimal] -or $Value -is [double]) "$Name must be numeric."
    Require (-not ($Value -is [bool])) "$Name must not be boolean."
    $number = [int64]$Value
    Require ([double]$number -eq [double]$Value) "$Name must be an integer."
    Require ($number -ge $Minimum -and $number -le $Maximum) "$Name is outside the finite model bound."
    return [int]$number
}

function RequireBool([object]$Value, [string]$Name) {
    Require ($null -ne $Value) "$Name is required."
    Require ($Value -is [bool]) "$Name must be a JSON boolean."
    return [bool]$Value
}

function IsChild([int]$Parent, [int]$Child) {
    return ($parentOf.ContainsKey($Child) -and $Child -ne 1 -and $parentOf[$Child] -eq $Parent)
}

function IsCommunicationPair([int]$Left, [int]$Right) {
    return (IsChild $Left $Right) -or (IsChild $Right $Left)
}

foreach ($event in $trace) {
    $kind = RequireString $event.kind 'event kind'
    switch ($kind) {
        'fork_admitted' {
            $forkOp = RequireString $event.fork_operation_id 'fork fork_operation_id'
            $childOp = RequireString $event.child_operation_id 'fork child_operation_id'
            Require ($forkOp -ne $childOp) 'fork and child operation identities must be distinct.'
            Require (-not $admitted.ContainsKey($forkOp)) 'fork operation identity was reused.'
            Require (-not $admittedByChild.ContainsKey($childOp)) 'child operation identity was reused.'
            $parent = RequireInt $event.parent 'fork parent' 1 5
            $child = RequireInt $event.child 'fork child' 1 5
            $depth = RequireInt $event.depth 'fork depth' 0 2
            $capture = RequireInt $event.captured_generation 'fork captured_generation' 0 $generationBound
            Require ($activeAgents.Contains($parent)) 'fork parent is not an active agent.'
            Require (-not $completedAgents.ContainsKey($parent)) 'completed agent attempted a fork.'
            Require (IsChild $parent $child) 'fork is not a directed direct-parent operation.'
            Require (-not $activeAgents.Contains($child)) 'fork child identity was already allocated.'
            Require ($depth -eq $depthOf[$child]) 'fork depth does not match the retained agent identity.'
            Require ($capture -eq $workspaceGeneration[$parent]) 'fork captured a forged workspace generation.'
            Require ($activeAgents.Count -lt $sessionBudget) 'fork exceeds the finite session allocation budget.'
            $admitted[$forkOp] = [ordered]@{
                parent = $parent
                child = $child
                depth = $depth
                captured_generation = $capture
                child_operation_id = $childOp
            }
            $admittedByChild[$childOp] = $forkOp
            $null = $activeAgents.Add($child)
            if ($pendingPublications.ContainsKey($forkOp)) {
                $pending = $pendingPublications[$forkOp]
                Require ($pending.parent -eq $parent -and $pending.child -eq $child) 'publication identity does not match its fork admission.'
                Require ($pending.captured_generation -eq $capture) 'publication supplied a forged captured generation.'
                $published[$forkOp] = $pending.current_generation
                $pendingPublications.Remove($forkOp)
            }
        }
        'workspace_advanced' {
            $agent = RequireInt $event.agent 'workspace_advanced agent' 1 5
            $generation = RequireInt $event.generation 'workspace_advanced generation' 0 $generationBound
            Require ($activeAgents.Contains($agent)) 'workspace generation advanced for an unknown agent.'
            Require ($generation -eq ($workspaceGeneration[$agent] + 1)) 'workspace generation skipped or was replayed.'
            $workspaceGeneration[$agent] = $generation
        }
        'model_started' {
            $childOp = RequireString $event.child_operation_id 'model_started child_operation_id'
            $agent = RequireInt $event.agent 'model_started agent' 1 5
            Require ($admittedByChild.ContainsKey($childOp)) 'model started without a fork admission.'
            $forkOp = $admittedByChild[$childOp]
            Require ($published.ContainsKey($forkOp)) 'model started before workspace publication.'
            Require (-not $started.ContainsKey($childOp)) 'model operation was started twice.'
            Require ($agent -eq $admitted[$forkOp].child) 'model start agent does not match its fork admission.'
            $started[$childOp] = $agent
        }
        'agent_completed' {
            $childOp = RequireString $event.child_operation_id 'agent_completed child_operation_id'
            $agent = RequireInt $event.agent 'agent_completed agent' 1 5
            $durable = RequireBool $event.outcome_durable 'agent_completed outcome_durable'
            Require ($admittedByChild.ContainsKey($childOp)) 'agent completed without a fork admission.'
            $forkOp = $admittedByChild[$childOp]
            Require ($started.ContainsKey($childOp)) 'agent completed without model start.'
            Require (-not $completed.ContainsKey($childOp)) 'agent completion was published twice.'
            Require ($agent -eq $admitted[$forkOp].child) 'completion agent does not match its fork admission.'
            Require $durable 'agent completion lacks a durable outcome.'
            $completed[$childOp] = $agent
            $completedAgents[$agent] = $childOp
        }
        'message_admitted' {
            $id = RequireString $event.message_id 'message identity'
            $sender = RequireInt $event.sender 'message sender' 1 5
            $recipient = RequireInt $event.recipient 'message recipient' 1 5
            Require (-not $messages.ContainsKey($id)) 'message identity was reused.'
            Require ($activeAgents.Contains($sender) -and $activeAgents.Contains($recipient)) 'message uses an unknown agent.'
            Require (IsCommunicationPair $sender $recipient) 'message authority is not direct parent/child.'
            $messages[$id] = [ordered]@{ sender = $sender; recipient = $recipient; delivered = $false }
        }
        'message_delivered' {
            $id = RequireString $event.message_id 'delivered message identity'
            $index = RequireInt $event.delivery_index 'message delivery_index' 1 1
            Require ($messages.ContainsKey($id)) 'message delivered without admission.'
            Require (-not $messages[$id].delivered) 'message was delivered more than once.'
            Require ($index -eq 1) 'delivery order is not the first durable delivery.'
            $messages[$id].delivered = $true
        }
        'wait_observed' {
            $id = RequireString $event.wait_id 'wait identity'
            $waiter = RequireInt $event.waiter 'waiter' 1 5
            $target = RequireInt $event.target 'wait target' 1 5
            $targetOp = RequireString $event.target_operation_id 'wait target operation_id'
            $targetCompleted = RequireBool $event.target_completed 'wait target_completed'
            Require (-not $waits.ContainsKey($id)) 'wait identity was reused.'
            Require (IsCommunicationPair $waiter $target) 'wait target is outside direct-parent scope.'
            Require ($completed.ContainsKey($targetOp)) 'wait observed a target without durable completion.'
            Require ($completed[$targetOp] -eq $target) 'wait target does not match the completed operation.'
            Require $targetCompleted 'wait target_completed must be true for an observed completion.'
            $waits[$id] = $targetOp
        }
        'workspace_published' {
            $forkOp = RequireString $event.fork_operation_id 'workspace fork_operation_id'
            $publicationOp = RequireString $event.publication_operation_id 'workspace publication_operation_id'
            Require ($forkOp -ne $publicationOp) 'fork and publication operation identities must be distinct.'
            $parent = RequireInt $event.parent 'publication parent' 1 5
            $child = RequireInt $event.child 'publication child' 1 5
            $capture = RequireInt $event.captured_generation 'publication captured_generation' 0 $generationBound
            Require (IsChild $parent $child) 'publication target is not the directed direct parent.'
            $hasCurrent = $null -ne $event.PSObject.Properties['current_generation']
            if ($hasCurrent) {
                $current = RequireInt $event.current_generation 'publication current_generation' 0 $generationBound
                Require ($current -eq $workspaceGeneration[$parent]) 'publication supplied a forged current generation.'
                Require ($capture -eq $current) 'publication crossed a stale workspace generation.'
            } else {
                $current = $null
            }
            Require (-not $published.ContainsKey($forkOp) -and -not $pendingPublications.ContainsKey($forkOp)) 'workspace publication was repeated.'
            if ($admitted.ContainsKey($forkOp)) {
                Require ($admitted[$forkOp].parent -eq $parent -and $admitted[$forkOp].child -eq $child) 'publication identity does not match its fork admission.'
                Require ($capture -eq $admitted[$forkOp].captured_generation) 'publication supplied a forged captured generation.'
                $published[$forkOp] = $current
            } else {
                # A parent conversation can publish before the swarm registry
                # persists its prepared admission. Retain the exact bounded
                # witness and bind it when the admission record arrives.
                $pendingPublications[$forkOp] = [ordered]@{
                    parent = $parent
                    child = $child
                    captured_generation = $capture
                    current_generation = $current
                }
            }
        }
        default { throw "trace conformance: unsupported event kind '$kind'." }
    }
}

Require ($pendingPublications.Count -eq 0) 'workspace publication has no matching fork admission.'

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

-------------------------- MODULE ForkBoundary --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Children, UnsafeEarlyDispatch, UnsafeRefresh
VARIABLES parentRevision, pinnedRevision, captured, phase, exchangeCompleted

vars == <<parentRevision, pinnedRevision, captured, phase, exchangeCompleted>>

Init == /\ parentRevision = 0
        /\ pinnedRevision = 0
        /\ captured = [c \in Children |-> 0]
        /\ phase = [c \in Children |-> "absent"]
        /\ exchangeCompleted = FALSE

\* One completed model batch selects all children at a single capture boundary.
CaptureBatch == /\ \A c \in Children: phase[c] = "absent"
                /\ pinnedRevision' = parentRevision
                /\ captured' = [c \in Children |-> parentRevision]
                /\ phase' = [c \in Children |-> "admitted"]
                /\ UNCHANGED <<parentRevision, exchangeCompleted>>

\* Abstracts persistence of the entire ordered triggering tool exchange.
CompleteExchange == /\ ~exchangeCompleted
                    /\ \A c \in Children: phase[c] = "admitted"
                    /\ exchangeCompleted' = TRUE
                    /\ UNCHANGED <<parentRevision, pinnedRevision, captured, phase>>

Bind(c) == /\ exchangeCompleted /\ phase[c] = "admitted"
           /\ phase' = [phase EXCEPT ![c] = "bound"]
           /\ UNCHANGED <<parentRevision, pinnedRevision, captured, exchangeCompleted>>

Dispatch(c) == /\ phase[c] = "bound"
               /\ (UnsafeEarlyDispatch \/
                    (\A sibling \in Children: phase[sibling] \in {"bound", "started"}))
               /\ phase' = [phase EXCEPT ![c] = "started"]
               /\ UNCHANGED <<parentRevision, pinnedRevision, captured, exchangeCompleted>>

ParentChange == /\ parentRevision < 2
                /\ parentRevision' = parentRevision + 1
                /\ UNCHANGED <<pinnedRevision, captured, phase, exchangeCompleted>>

\* Negative control: resolving an inherited reference against mutable state.
RefreshCapture(c) == /\ UnsafeRefresh /\ phase[c] # "absent"
                     /\ captured[c] # parentRevision
                     /\ captured' = [captured EXCEPT ![c] = parentRevision]
                     /\ UNCHANGED <<parentRevision, pinnedRevision, phase, exchangeCompleted>>

Next == CaptureBatch \/ CompleteExchange \/ ParentChange \/
        (\E c \in Children: Bind(c) \/ Dispatch(c) \/ RefreshCapture(c))

Spec == Init /\ [][Next]_vars

TypeOK == /\ parentRevision \in 0..2 /\ pinnedRevision \in 0..2
          /\ captured \in [Children -> 0..2]
          /\ phase \in [Children -> {"absent", "admitted", "bound", "started"}]
          /\ exchangeCompleted \in BOOLEAN

DispatchRequiresCompleteBatch ==
    (\E c \in Children: phase[c] = "started") =>
        (exchangeCompleted /\ \A c \in Children: phase[c] \in {"bound", "started"})

InheritedCaptureRemainsPinned ==
    \A c \in Children: phase[c] # "absent" => captured[c] = pinnedRevision
=============================================================================

-------------------------- MODULE ForkBoundary --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Children, Selected, UnsafeEarlyDispatch, UnsafeRefresh

VARIABLES parentRevision, pinnedRevision, captured, publicationRevision,
          phase, exchangeCompleted

vars == <<parentRevision, pinnedRevision, captured, publicationRevision,
          phase, exchangeCompleted>>

Init == /\ parentRevision = 0
        /\ pinnedRevision = 0
        /\ captured = [c \in Children |-> 0]
        /\ publicationRevision = [c \in Children |-> 0]
        /\ phase = [c \in Children |->
                      IF c \in Selected THEN "absent" ELSE "unselected"]
        /\ exchangeCompleted = FALSE

\* A batch may contain a selected subset of the possible child plans. The
\* inherited boundary is captured once for exactly that subset.
CaptureBatch == /\ \A c \in Selected: phase[c] = "absent"
                /\ pinnedRevision' = parentRevision
                /\ captured' = [c \in Children |->
                                  IF c \in Selected THEN parentRevision ELSE 0]
                /\ publicationRevision' = [c \in Children |->
                                             IF c \in Selected THEN parentRevision ELSE 0]
                /\ phase' = [c \in Children |->
                               IF c \in Selected THEN "admitted" ELSE "unselected"]
                /\ UNCHANGED <<parentRevision, exchangeCompleted>>

\* Abstracts persistence of the entire ordered triggering tool exchange.
CompleteExchange == /\ ~exchangeCompleted
                    /\ \A c \in Selected: phase[c] = "admitted"
                    /\ exchangeCompleted' = TRUE
                    /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                                    publicationRevision, phase>>

Bind(c) == /\ c \in Selected
           /\ exchangeCompleted
           /\ phase[c] = "admitted"
           /\ phase' = [phase EXCEPT ![c] = "bound"]
           /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                          publicationRevision, exchangeCompleted>>

Dispatch(c) == /\ c \in Selected
               /\ phase[c] = "bound"
               /\ (UnsafeEarlyDispatch \/
                    (\A sibling \in Selected:
                       phase[sibling] \in {"bound", "started"}))
               /\ phase' = [phase EXCEPT ![c] = "started"]
               /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                              publicationRevision, exchangeCompleted>>

ParentChange == /\ parentRevision < 2
                /\ parentRevision' = parentRevision + 1
                /\ UNCHANGED <<pinnedRevision, captured, publicationRevision,
                               phase, exchangeCompleted>>

\* A proven publication rebind updates the parent stream revision used by a
\* plan. It never rewrites the immutable inherited model-input boundary.
Rebind(c) == /\ c \in Selected
             /\ phase[c] \in {"admitted", "bound"}
             /\ publicationRevision[c] # parentRevision
             /\ publicationRevision' =
                   [publicationRevision EXCEPT ![c] = parentRevision]
             /\ UNCHANGED <<parentRevision, pinnedRevision, captured, phase,
                            exchangeCompleted>>

\* Negative control: resolving an inherited reference against mutable state.
RefreshCapture(c) == /\ UnsafeRefresh
                     /\ c \in Selected
                     /\ phase[c] # "absent"
                     /\ captured[c] # parentRevision
                     /\ captured' = [captured EXCEPT ![c] = parentRevision]
                     /\ UNCHANGED <<parentRevision, pinnedRevision,
                                    publicationRevision, phase,
                                    exchangeCompleted>>

Next == CaptureBatch \/ CompleteExchange \/ ParentChange \/
        (\E c \in Children:
          Bind(c) \/ Dispatch(c) \/ Rebind(c) \/ RefreshCapture(c))

Spec == Init /\ [][Next]_vars

TypeOK == /\ Selected \subseteq Children
          /\ parentRevision \in 0..2
          /\ pinnedRevision \in 0..2
          /\ captured \in [Children -> 0..2]
          /\ publicationRevision \in [Children -> 0..2]
          /\ phase \in [Children ->
                         {"unselected", "absent", "admitted", "bound", "started"}]
          /\ exchangeCompleted \in BOOLEAN

DispatchRequiresCompleteBatch ==
    (\E c \in Selected: phase[c] = "started") =>
        (exchangeCompleted /\
         \A c \in Selected: phase[c] \in {"bound", "started"})

InheritedCaptureRemainsPinned ==
    \A c \in Selected: phase[c] # "absent" => captured[c] = pinnedRevision

PublicationRevisionNotAhead ==
    \A c \in Selected: publicationRevision[c] <= parentRevision

=============================================================================

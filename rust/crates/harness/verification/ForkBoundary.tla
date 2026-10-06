-------------------------- MODULE ForkBoundary --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Children, Selected, UnsafeEarlyDispatch, UnsafeRefresh,
          UnsafeEarlyChildRequest, UnsafeDuplicatePublication, UnsafeStaleOwner

VARIABLES parentRevision, pinnedRevision, captured, publicationRevision,
          preparedOwner, publishedOwner, publicationCount, requested, phase,
          boundaryCaptured, exchangeCompleted, ownerEpoch

vars == <<parentRevision, pinnedRevision, captured, publicationRevision,
          preparedOwner, publishedOwner, publicationCount, requested, phase,
          boundaryCaptured, exchangeCompleted, ownerEpoch>>

Init == /\ parentRevision = 0
        /\ pinnedRevision = 0
        /\ captured = [c \in Children |-> 0]
        /\ publicationRevision = [c \in Children |-> 0]
        /\ preparedOwner = [c \in Children |-> 0]
        /\ publishedOwner = [c \in Children |-> 0]
        /\ publicationCount = [c \in Children |-> 0]
        /\ requested = [c \in Children |-> FALSE]
        /\ phase = [c \in Children |->
                      IF c \in Selected THEN "absent" ELSE "unselected"]
        /\ boundaryCaptured = FALSE
        /\ exchangeCompleted = FALSE
        /\ ownerEpoch = 0

\* Capture the immutable inherited model-input boundary once. Plans are then
\* prepared one at a time against this same boundary.
CaptureBoundary == /\ ~boundaryCaptured
                  /\ pinnedRevision' = parentRevision
                  /\ boundaryCaptured' = TRUE
                  /\ UNCHANGED <<parentRevision, captured,
                                 publicationRevision, preparedOwner,
                                 publishedOwner, publicationCount, requested,
                                 phase, exchangeCompleted, ownerEpoch>>

\* Preparation is per selected child. The parent publication revision and
\* owner epoch used by a plan may later be checked independently of capture.
Prepare(c) == /\ c \in Selected
             /\ boundaryCaptured
             /\ phase[c] = "absent"
             /\ phase' = [phase EXCEPT ![c] = "prepared"]
             /\ captured' = [captured EXCEPT ![c] = pinnedRevision]
             /\ publicationRevision' =
                   [publicationRevision EXCEPT ![c] = parentRevision]
             /\ preparedOwner' = [preparedOwner EXCEPT ![c] = ownerEpoch]
             /\ UNCHANGED <<parentRevision, pinnedRevision, publishedOwner,
                            publicationCount, requested, boundaryCaptured,
                            exchangeCompleted, ownerEpoch>>

\* The ordered triggering exchange is complete only after every selected plan
\* has durable preparation evidence. This is separate from publication.
CompleteExchange == /\ ~exchangeCompleted
                    /\ boundaryCaptured
                    /\ \A c \in Selected: phase[c] = "prepared"
                    /\ exchangeCompleted' = TRUE
                    /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                                    publicationRevision, preparedOwner,
                                    publishedOwner, publicationCount, requested,
                                    phase, boundaryCaptured, ownerEpoch>>

\* Each prepared child is published sequentially. A stale owner is accepted
\* only by the explicit negative control, which records the wrong owner.
Publish(c) == /\ c \in Selected
              /\ exchangeCompleted
              /\ phase[c] = "prepared"
              /\ (UnsafeStaleOwner \/ preparedOwner[c] = ownerEpoch)
              /\ phase' = [phase EXCEPT ![c] = "published"]
              /\ publicationCount' =
                    [publicationCount EXCEPT ![c] = @ + 1]
              /\ publishedOwner' =
                    [publishedOwner EXCEPT
                       ![c] = IF UnsafeStaleOwner /\
                                      preparedOwner[c] # ownerEpoch
                                  THEN ownerEpoch
                                  ELSE preparedOwner[c]]
              /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                             publicationRevision, preparedOwner, requested,
                             boundaryCaptured, exchangeCompleted, ownerEpoch>>

\* A lost publication acknowledgement is a retry observation. The safe path
\* reuses the durable publication and leaves its physical count unchanged.
RetryPublish(c) == /\ c \in Selected
                  /\ phase[c] \in {"published", "bound", "started"}
                  /\ publicationCount[c] > 0
                  /\ publicationCount' =
                        (IF UnsafeDuplicatePublication
                         THEN [publicationCount EXCEPT ![c] = @ + 1]
                         ELSE publicationCount)
                  /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                                 publicationRevision, preparedOwner,
                                 publishedOwner, requested, phase,
                                 boundaryCaptured, exchangeCompleted,
                                 ownerEpoch>>

Bind(c) == /\ c \in Selected
           /\ exchangeCompleted
           /\ phase[c] = "published"
           /\ phase' = [phase EXCEPT ![c] = "bound"]
           /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                          publicationRevision, preparedOwner, publishedOwner,
                          publicationCount, requested, boundaryCaptured,
                          exchangeCompleted, ownerEpoch>>

\* A child request is admitted only once all selected children are bound. The
\* unsafe control models enqueueing a child from a merely published plan.
RequestChild(c) == /\ c \in Selected
                  /\ ~requested[c]
                  /\ phase[c] \in {"published", "bound"}
                  /\ (UnsafeEarlyChildRequest \/
                       (phase[c] = "bound" /\
                        \A sibling \in Selected:
                          phase[sibling] \in {"bound", "started"}))
                  /\ requested' = [requested EXCEPT ![c] = TRUE]
                  /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                                 publicationRevision, preparedOwner,
                                 publishedOwner, publicationCount, phase,
                                 boundaryCaptured, exchangeCompleted,
                                 ownerEpoch>>

Dispatch(c) == /\ c \in Selected
               /\ phase[c] = "bound"
               /\ (UnsafeEarlyDispatch \/
                    (\A sibling \in Selected:
                       phase[sibling] \in {"bound", "started"}))
               /\ phase' = [phase EXCEPT ![c] = "started"]
               /\ requested' = [requested EXCEPT ![c] = TRUE]
               /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                              publicationRevision, preparedOwner, publishedOwner,
                              publicationCount, boundaryCaptured,
                              exchangeCompleted, ownerEpoch>>

ParentChange == /\ parentRevision < 2
                /\ parentRevision' = parentRevision + 1
                /\ UNCHANGED <<pinnedRevision, captured, publicationRevision,
                               preparedOwner, publishedOwner, publicationCount,
                               requested, phase, boundaryCaptured,
                               exchangeCompleted, ownerEpoch>>

OwnerChange == /\ ownerEpoch < 2
               /\ ownerEpoch' = ownerEpoch + 1
               /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                              publicationRevision, preparedOwner, publishedOwner,
                              publicationCount, requested, phase,
                              boundaryCaptured, exchangeCompleted>>

\* A proven publication rebind updates only the parent stream revision used by
\* a prepared plan. It never rewrites the immutable inherited capture.
Rebind(c) == /\ c \in Selected
             /\ phase[c] = "prepared"
             /\ publicationRevision[c] # parentRevision
             /\ publicationRevision' =
                   [publicationRevision EXCEPT ![c] = parentRevision]
             /\ UNCHANGED <<parentRevision, pinnedRevision, captured,
                            preparedOwner, publishedOwner, publicationCount,
                            requested, phase, boundaryCaptured,
                            exchangeCompleted, ownerEpoch>>

\* Negative control: resolving an inherited reference against mutable state.
RefreshCapture(c) == /\ UnsafeRefresh
                     /\ c \in Selected
                     /\ phase[c] # "absent"
                     /\ captured[c] # parentRevision
                     /\ captured' = [captured EXCEPT ![c] = parentRevision]
                     /\ UNCHANGED <<parentRevision, pinnedRevision,
                                    publicationRevision, preparedOwner,
                                    publishedOwner, publicationCount, requested,
                                    phase, boundaryCaptured,
                                    exchangeCompleted, ownerEpoch>>

Next == CaptureBoundary \/ CompleteExchange \/ ParentChange \/ OwnerChange \/
        (\E c \in Children:
          Prepare(c) \/ Publish(c) \/ RetryPublish(c) \/ Bind(c) \/
          RequestChild(c) \/ Dispatch(c) \/ Rebind(c) \/ RefreshCapture(c))

Spec == Init /\ [][Next]_vars

TypeOK == /\ Selected \subseteq Children
          /\ parentRevision \in 0..2
          /\ pinnedRevision \in 0..2
          /\ ownerEpoch \in 0..2
          /\ captured \in [Children -> 0..2]
          /\ publicationRevision \in [Children -> 0..2]
          /\ preparedOwner \in [Children -> 0..2]
          /\ publishedOwner \in [Children -> 0..2]
          /\ publicationCount \in [Children -> 0..2]
          /\ requested \in [Children -> BOOLEAN]
          /\ phase \in [Children ->
                         {"unselected", "absent", "prepared", "published",
                          "bound", "started"}]
          /\ boundaryCaptured \in BOOLEAN
          /\ exchangeCompleted \in BOOLEAN

DispatchRequiresCompleteBatch ==
    (\E c \in Selected: phase[c] = "started") =>
        (exchangeCompleted /\
         \A c \in Selected: phase[c] \in {"bound", "started"})

ChildRequestRequiresCompleteBatch ==
    (\E c \in Selected: requested[c]) =>
        (exchangeCompleted /\
         \A c \in Selected: phase[c] \in {"bound", "started"})

InheritedCaptureRemainsPinned ==
    \A c \in Selected: phase[c] # "absent" => captured[c] = pinnedRevision

PublicationRevisionNotAhead ==
    \A c \in Selected: publicationRevision[c] <= parentRevision

AtMostOncePhysicalPublication ==
    \A c \in Selected: publicationCount[c] <= 1

PublicationOwnerMatchesPreparation ==
    \A c \in Selected: publicationCount[c] = 0 \/
        publishedOwner[c] = preparedOwner[c]
=============================================================================

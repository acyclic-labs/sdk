------------------------ MODULE SwarmIntegration ------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS UnsafeSibling, UnsafeGrandchild, UnsafeStaleApproval,
          UnsafeMismatchedApproval

Root == 1
Children == {2, 3, 4}
Agents == 1..4
Generations == 0..1
Statuses == {"private", "ready", "integrated", "discarded"}
ApprovalStates == {"none", "approved"}

Parent(a) == IF a = 2 THEN 1 ELSE IF a = 3 THEN 2 ELSE IF a = 4 THEN 1 ELSE 0
DirectParent(actor, child) == child \in Children /\ Parent(child) = actor
WritebackOperation(child) ==
    IF child = 2 THEN "writeback-2" ELSE IF child = 3 THEN "writeback-3" ELSE "writeback-4"
VARIABLES currentGeneration, status, capturedGeneration,
          integrationActor, integrationAction,
          approvalState, approvalTarget, approvalOperation,
          approvalAction, approvalGeneration, approvalTargetGeneration,
          writebackState, writebackActor, writebackGeneration,
          writebackTargetGeneration

vars == <<currentGeneration, status, capturedGeneration,
          integrationActor, integrationAction,
          approvalState, approvalTarget, approvalOperation,
          approvalAction, approvalGeneration, approvalTargetGeneration,
          writebackState, writebackActor, writebackGeneration,
          writebackTargetGeneration>>

Init == /\ currentGeneration = 0
       /\ status = [child \in Children |-> "private"]
       /\ capturedGeneration = [child \in Children |-> 0]
       /\ integrationActor = [child \in Children |-> 0]
       /\ integrationAction = [child \in Children |-> "none"]
       /\ approvalState = "none"
       /\ approvalTarget = 0
       /\ approvalOperation = "none"
       /\ approvalAction = "none"
       /\ approvalGeneration = 0
       /\ approvalTargetGeneration = 0
       /\ writebackState = [child \in Children |-> "absent"]
       /\ writebackActor = [child \in Children |-> 0]
       /\ writebackGeneration = [child \in Children |-> 0]
       /\ writebackTargetGeneration = [child \in Children |-> 0]

Fork(child) == /\ child \in Children
             /\ status[child] = "private"
             /\ status' = [status EXCEPT ![child] = "ready"]
             /\ capturedGeneration' = [capturedGeneration EXCEPT ![child] = currentGeneration]
             /\ UNCHANGED <<currentGeneration, integrationActor, integrationAction,
                              approvalState, approvalTarget, approvalOperation,
                               approvalAction, approvalGeneration,
                               approvalTargetGeneration, writebackState,
                               writebackActor, writebackGeneration,
                               writebackTargetGeneration>>

AdvanceGeneration == /\ currentGeneration = 0
                    /\ currentGeneration' = 1
                    /\ UNCHANGED <<status, capturedGeneration, integrationActor,
                                     integrationAction, approvalState, approvalTarget,
                                      approvalOperation, approvalAction, approvalGeneration,
                                      approvalTargetGeneration, writebackState,
                                      writebackActor, writebackGeneration,
                                      writebackTargetGeneration>>

IntegrationActorFor(child) == Parent(child)

IntegrateOrDiscard(child, action) == /\ child \in Children
    /\ status[child] = "ready"
    /\ action \in {"integrate", "discard"}
    /\ integrationAction' = [integrationAction EXCEPT ![child] = action]
    /\ integrationActor' = [integrationActor EXCEPT ![child] =
          (IF UnsafeSibling /\ child = 4 THEN 2 ELSE IntegrationActorFor(child))]
    /\ status' = [status EXCEPT ![child] =
          (IF action = "integrate" THEN "integrated" ELSE "discarded")]
    /\ UNCHANGED <<currentGeneration, capturedGeneration, approvalState,
                     approvalTarget, approvalOperation, approvalAction,
                     approvalGeneration, approvalTargetGeneration,
                     writebackState, writebackActor, writebackGeneration,
                     writebackTargetGeneration>>

ApproveWriteback(child) == /\ child \in Children
    /\ status[child] = "ready"
    /\ Parent(child) = Root
    /\ approvalState = "none"
    /\ approvalState' = "approved"
    /\ approvalTarget' = child
    /\ approvalOperation' =
          (IF UnsafeMismatchedApproval THEN WritebackOperation(3) ELSE WritebackOperation(child))
    /\ approvalAction' = (IF UnsafeMismatchedApproval THEN "discard" ELSE "writeback")
    /\ approvalGeneration' =
          (IF UnsafeStaleApproval THEN 1 - capturedGeneration[child]
           ELSE capturedGeneration[child])
    /\ approvalTargetGeneration' = currentGeneration
    /\ UNCHANGED <<currentGeneration, status, capturedGeneration, integrationActor,
                     integrationAction, writebackState, writebackActor,
                     writebackGeneration, writebackTargetGeneration>>

WritebackTarget(child) ==
    IF UnsafeGrandchild THEN 3 ELSE child

Writeback(child) == /\ child \in Children
    /\ child = WritebackTarget(child)
    /\ status[child] = "ready"
    /\ approvalState = "approved"
    /\ (UnsafeGrandchild \/ UnsafeStaleApproval \/ UnsafeMismatchedApproval
        \/ (approvalTarget = child
            /\ approvalOperation = WritebackOperation(child)
            /\ approvalAction = "writeback"
            /\ approvalGeneration = capturedGeneration[child]
            /\ approvalTargetGeneration = currentGeneration
            /\ currentGeneration = capturedGeneration[child]))
    /\ writebackState' = [writebackState EXCEPT ![child] = "written"]
    /\ writebackActor' = [writebackActor EXCEPT ![child] = Root]
    /\ writebackGeneration' = [writebackGeneration EXCEPT ![child] = capturedGeneration[child]]
    /\ writebackTargetGeneration' = [writebackTargetGeneration EXCEPT ![child] = currentGeneration]
    /\ UNCHANGED <<currentGeneration, status, capturedGeneration, integrationActor,
                     integrationAction, approvalState, approvalTarget,
                     approvalOperation, approvalAction, approvalGeneration,
                     approvalTargetGeneration>>

Next == AdvanceGeneration
     \/ (\E child \in Children: Fork(child))
     \/ (\E child \in Children, action \in {"integrate", "discard"}:
            IntegrateOrDiscard(child, action))
     \/ (\E child \in Children: ApproveWriteback(child))
     \/ (\E child \in Children: Writeback(child))

Spec == Init /\ [][Next]_vars

TypeOK == /\ currentGeneration \in Generations
          /\ status \in [Children -> Statuses]
          /\ capturedGeneration \in [Children -> Generations]
          /\ integrationActor \in [Children -> Agents \cup {0}]
          /\ integrationAction \in [Children -> {"none", "integrate", "discard"}]
          /\ approvalState \in ApprovalStates
          /\ approvalTarget \in Children \cup {0}
          /\ approvalOperation \in {"none", "writeback-2", "writeback-3", "writeback-4"}
          /\ approvalAction \in {"none", "writeback", "discard"}
          /\ approvalGeneration \in Generations
          /\ approvalTargetGeneration \in Generations
          /\ writebackState \in [Children -> {"absent", "written"}]
          /\ writebackActor \in [Children -> Agents \cup {0}]
          /\ writebackGeneration \in [Children -> Generations]
          /\ writebackTargetGeneration \in [Children -> Generations]

DirectIntegrationAuthority ==
    \A child \in Children:
        integrationAction[child] # "none" => DirectParent(integrationActor[child], child)

RootWritebackScope ==
    \A child \in Children:
        writebackState[child] = "written" => Parent(child) = Root

ApprovalBinding ==
    \A child \in Children:
        writebackState[child] = "written" =>
            /\ approvalTarget = child
            /\ Parent(approvalTarget) = Root
            /\ approvalOperation = WritebackOperation(child)
            /\ approvalAction = "writeback"
            /\ approvalGeneration = capturedGeneration[child]
            /\ approvalTargetGeneration = writebackTargetGeneration[child]

WritebackAtCapturedGeneration ==
    \A child \in Children:
        writebackState[child] = "written" =>
            /\ writebackGeneration[child] = capturedGeneration[child]
            /\ writebackTargetGeneration[child] = approvalTargetGeneration

=============================================================================

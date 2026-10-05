-------------------------- MODULE SwarmAuthority --------------------------
EXTENDS Naturals, FiniteSets

CONSTANT UnsafeAuthority

Agents == 1..4
Messages == 1..3
Waits == 1..3

Parent(a) == IF a = 1 THEN 1 ELSE IF a = 2 THEN 1 ELSE IF a = 3 THEN 1 ELSE 2
Direct(a, b) == a # b /\ (Parent(b) = a \/ Parent(a) = b)
MessageRecipient(m) == IF m = 1 THEN 1 ELSE IF m = 2 THEN 2 ELSE 1
MessageSender(m) == IF m = 1 THEN 2 ELSE IF m = 2 THEN 1 ELSE 1
Waiter(w) == IF w = 1 THEN 1 ELSE IF w = 2 THEN 2 ELSE 2
WaitTarget(w) == IF w = 1 THEN 2 ELSE IF w = 2 THEN 4 ELSE 3

VARIABLES messageState, waitState

vars == <<messageState, waitState>>

Init == /\ messageState = [m \in Messages |-> "absent"]
        /\ waitState = [w \in Waits |-> "absent"]

AdmitMessage(m) == /\ m \in Messages
                  /\ messageState[m] = "absent"
                  /\ (UnsafeAuthority \/ Direct(MessageSender(m), MessageRecipient(m)))
                  /\ messageState' = [messageState EXCEPT ![m] = "admitted"]
                  /\ UNCHANGED waitState

AdmitWait(w) == /\ w \in Waits
                /\ waitState[w] = "absent"
                /\ (UnsafeAuthority \/ Direct(Waiter(w), WaitTarget(w)))
                /\ waitState' = [waitState EXCEPT ![w] = "admitted"]
                /\ UNCHANGED messageState

DeliverMessage(m) == /\ m \in Messages
                    /\ messageState[m] = "admitted"
                    /\ messageState' = [messageState EXCEPT ![m] = "delivered"]
                    /\ UNCHANGED waitState

ObserveWait(w) == /\ w \in Waits
                  /\ waitState[w] = "admitted"
                  /\ waitState' = [waitState EXCEPT ![w] = "observed"]
                  /\ UNCHANGED messageState

Next == (\E m \in Messages: AdmitMessage(m) \/ DeliverMessage(m))
        \/ (\E w \in Waits: AdmitWait(w) \/ ObserveWait(w))

Spec == Init /\ [][Next]_vars

TypeOK == /\ messageState \in [Messages -> {"absent", "admitted", "delivered"}]
          /\ waitState \in [Waits -> {"absent", "admitted", "observed"}]

DirectMessageAuthority ==
    \A m \in Messages: messageState[m] # "absent" =>
        Direct(MessageSender(m), MessageRecipient(m))

SelfMessageAuthority ==
    messageState[3] # "absent" => MessageSender(3) # MessageRecipient(3)

DirectWaitAuthority ==
    \A w \in Waits: waitState[w] # "absent" =>
        Direct(Waiter(w), WaitTarget(w))

=============================================================================

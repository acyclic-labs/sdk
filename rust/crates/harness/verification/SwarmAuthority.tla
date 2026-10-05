-------------------------- MODULE SwarmAuthority --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS UnsafeAuthority, UnsafeTranscriptAuthority

Agents == 1..4
Messages == 1..4
Waits == 1..3

Parent(a) == IF a = 1 THEN 1 ELSE IF a = 2 THEN 1 ELSE IF a = 3 THEN 1 ELSE 2
Direct(a, b) == a # b /\ (Parent(b) = a \/ Parent(a) = b)
MessageSender(m) == IF m = 1 THEN 2 ELSE IF m = 2 THEN 1 ELSE IF m = 3 THEN 1 ELSE 3
MessageRecipient(m) == IF m = 1 THEN 1 ELSE IF m = 2 THEN 2 ELSE IF m = 3 THEN 1 ELSE 2
Waiter(w) == IF w = 1 THEN 1 ELSE IF w = 2 THEN 2 ELSE 2
WaitTarget(w) == IF w = 1 THEN 2 ELSE IF w = 2 THEN 4 ELSE 3

VARIABLES inherited, messageState, waitState

vars == <<inherited, messageState, waitState>>

Init == /\ inherited = [a \in Agents |-> FALSE]
        /\ messageState = [m \in Messages |-> "absent"]
        /\ waitState = [w \in Waits |-> "absent"]

InheritTranscript(a) == /\ a \in Agents \ {1}
                        /\ inherited[a] = FALSE
                        /\ inherited' = [inherited EXCEPT ![a] = TRUE]
                        /\ UNCHANGED <<messageState, waitState>>

AdmitMessage(m) == /\ m \in Messages
                  /\ messageState[m] = "absent"
                  /\ (UnsafeAuthority \/
                      (UnsafeTranscriptAuthority /\ inherited[MessageSender(m)]) \/
                      Direct(MessageSender(m), MessageRecipient(m)))
                  /\ messageState' = [messageState EXCEPT ![m] = "admitted"]
                  /\ UNCHANGED <<inherited, waitState>>

AdmitWait(w) == /\ w \in Waits
                /\ waitState[w] = "absent"
                /\ (UnsafeAuthority \/ Direct(Waiter(w), WaitTarget(w)))
                /\ waitState' = [waitState EXCEPT ![w] = "admitted"]
                /\ UNCHANGED <<inherited, messageState>>

DeliverMessage(m) == /\ m \in Messages
                    /\ messageState[m] = "admitted"
                    /\ messageState' = [messageState EXCEPT ![m] = "delivered"]
                    /\ UNCHANGED <<inherited, waitState>>

ObserveWait(w) == /\ w \in Waits
                  /\ waitState[w] = "admitted"
                  /\ waitState' = [waitState EXCEPT ![w] = "observed"]
                  /\ UNCHANGED <<inherited, messageState>>

Next == (\E a \in Agents \ {1}: InheritTranscript(a))
        \/ (\E m \in Messages: AdmitMessage(m) \/ DeliverMessage(m))
        \/ (\E w \in Waits: AdmitWait(w) \/ ObserveWait(w))

Spec == Init /\ [][Next]_vars

TypeOK == /\ inherited \in [Agents -> BOOLEAN]
          /\ messageState \in [Messages -> {"absent", "admitted", "delivered"}]
          /\ waitState \in [Waits -> {"absent", "admitted", "observed"}]

DirectMessageAuthority ==
    \A m \in Messages: messageState[m] # "absent" =>
        Direct(MessageSender(m), MessageRecipient(m))

SelfMessageAuthority ==
    messageState[3] # "absent" => MessageSender(3) # MessageRecipient(3)

TranscriptInheritanceDoesNotGrantAuthority ==
    \A m \in Messages:
        messageState[m] # "absent" =>
            ~(inherited[MessageSender(m)] /\
              ~Direct(MessageSender(m), MessageRecipient(m)))

DirectWaitAuthority ==
    \A w \in Waits: waitState[w] # "absent" =>
        Direct(Waiter(w), WaitTarget(w))

=============================================================================

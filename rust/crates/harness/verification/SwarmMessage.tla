--------------------------- MODULE SwarmMessage ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS UnsafeDuplicate, UnsafeOrphan

VARIABLES state, admitted, deliveryCount
vars == <<state, admitted, deliveryCount>>

Init == /\ state = "absent"
        /\ admitted = FALSE
        /\ deliveryCount = 0

Admit == /\ state = "absent"
         /\ state' = "admitted"
         /\ admitted' = TRUE
         /\ UNCHANGED deliveryCount

Deliver == /\ (state = "admitted" \/
                (UnsafeOrphan /\ state = "absent") \/
                (UnsafeDuplicate /\ state = "delivered"))
           /\ deliveryCount < 2
           /\ state' = "delivered"
           /\ deliveryCount' = deliveryCount + 1
           /\ UNCHANGED admitted

Next == Admit \/ Deliver
Spec == Init /\ [][Next]_vars

TypeOK == /\ state \in {"absent", "admitted", "delivered"}
          /\ admitted \in BOOLEAN
          /\ deliveryCount \in 0..2

DeliveredRequiresAdmission == deliveryCount > 0 => admitted
AtMostOnce == deliveryCount <= 1
=============================================================================

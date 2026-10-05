--------------------------- MODULE SwarmMessage ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS UnsafeDuplicate, UnsafeOrphan, UnsafeCancelDelivery,
          UnsafePublicationReplay

VARIABLES state, admitted, cancelled, deliveryCount, published, publicationCount
vars == <<state, admitted, cancelled, deliveryCount, published, publicationCount>>

Init == /\ state = "absent"
        /\ admitted = FALSE
        /\ cancelled = FALSE
        /\ deliveryCount = 0
        /\ published = FALSE
        /\ publicationCount = 0

Admit == /\ state = "absent"
         /\ state' = "admitted"
         /\ admitted' = TRUE
         /\ UNCHANGED <<cancelled, deliveryCount, published, publicationCount>>

Cancel == /\ ~cancelled
         /\ ~published
         /\ deliveryCount = 0
         /\ state \in {"absent", "admitted"}
         /\ state' = "cancelled"
         /\ cancelled' = TRUE
         /\ UNCHANGED <<admitted, deliveryCount, published, publicationCount>>

Deliver == /\ (state = "admitted" \/
                (UnsafeOrphan /\ state = "absent") \/
                (UnsafeDuplicate /\ state = "delivered") \/
                (UnsafeCancelDelivery /\ state = "cancelled" /\ admitted))
           /\ deliveryCount < 2
           /\ ~published
           /\ state' = "delivered"
           /\ deliveryCount' = deliveryCount + 1
           /\ UNCHANGED <<admitted, cancelled, published, publicationCount>>

Publish == /\ state = "delivered"
          /\ admitted
          /\ ~cancelled
          /\ ~published
          /\ state' = "published"
          /\ published' = TRUE
          /\ publicationCount' = publicationCount + 1
          /\ UNCHANGED <<admitted, cancelled, deliveryCount>>

RecoverPublish == /\ UnsafePublicationReplay
                 /\ state = "published"
                 /\ admitted
                 /\ ~cancelled
                 /\ published
                 /\ publicationCount < 2
                 /\ UNCHANGED <<state, admitted, cancelled, deliveryCount, published>>
                 /\ publicationCount' = publicationCount + 1

Next == Admit \/ Cancel \/ Deliver \/ Publish \/ RecoverPublish
Spec == Init /\ [][Next]_vars

TypeOK == /\ state \in {"absent", "admitted", "cancelled", "delivered", "published"}
          /\ admitted \in BOOLEAN
          /\ cancelled \in BOOLEAN
          /\ deliveryCount \in 0..2
          /\ published \in BOOLEAN
          /\ publicationCount \in 0..2

DeliveredRequiresAdmission == deliveryCount > 0 => admitted
AtMostOnce == deliveryCount <= 1
CancelledNeverDelivered == cancelled => deliveryCount = 0
PublicationRequiresDelivery == publicationCount > 0 =>
    /\ admitted
    /\ deliveryCount = 1
    /\ published
    /\ ~cancelled
AtMostOncePublication == publicationCount <= 1
=============================================================================

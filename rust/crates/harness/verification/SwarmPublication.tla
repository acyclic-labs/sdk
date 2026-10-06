------------------------ MODULE SwarmPublication ------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS UnsafeStalePublish, UnsafePublishWithoutReservation,
          UnsafeActivationWithoutAlias, UnsafePublishAfterResolverCancel

Parent == 1
Child == 2
GenerationValues == 0..1

VARIABLES parentGeneration, phase, capturedGeneration, published,
          publicationGeneration, budgetReserved, seedCommitted, aliasPresent,
          activationReserved, cancelled

vars == <<parentGeneration, phase, capturedGeneration, published,
           publicationGeneration, budgetReserved, seedCommitted, aliasPresent,
           activationReserved, cancelled>>

Init == /\ parentGeneration = 0
        /\ phase = "absent"
        /\ capturedGeneration = 0
        /\ published = FALSE
        /\ publicationGeneration = 0
        /\ budgetReserved = FALSE
        /\ seedCommitted = FALSE
        /\ aliasPresent = FALSE
        /\ activationReserved = FALSE
        /\ cancelled = FALSE

\* Child budget admission is the resolver's durable boundary. A failed
\* reservation cannot consume capacity after restart.
ReserveActivation == /\ phase = "absent"
                     /\ ~budgetReserved
                     /\ ~cancelled
                     /\ budgetReserved' = TRUE
                     /\ UNCHANGED <<parentGeneration, phase,
                                    capturedGeneration, published,
                                    publicationGeneration, seedCommitted,
                                    aliasPresent, activationReserved, cancelled>>

Fork == /\ phase = "absent"
        /\ (UnsafePublishWithoutReservation \/ budgetReserved)
        /\ (UnsafePublishAfterResolverCancel \/ ~cancelled)
        /\ phase' = "admitted"
        /\ capturedGeneration' = parentGeneration
        /\ seedCommitted' = TRUE
        /\ UNCHANGED <<parentGeneration, published, publicationGeneration,
                       budgetReserved, aliasPresent, activationReserved,
                       cancelled>>

\* Seed commit and Git compatibility alias publication are separate durable
\* effects. The safe state after Fork therefore permits seedCommitted with a
\* missing alias; activation remains fenced until registration succeeds.
RegisterAlias == /\ phase = "admitted"
                 /\ seedCommitted
                 /\ ~aliasPresent
                 /\ ~cancelled
                 /\ aliasPresent' = TRUE
                 /\ UNCHANGED <<parentGeneration, phase, capturedGeneration,
                                published, publicationGeneration,
                                budgetReserved, seedCommitted,
                                activationReserved, cancelled>>

\* A lost alias acknowledgement is retried against the same durable seed.
\* This is a stuttering transition when the alias is already present.
RetryAlias == /\ phase = "admitted"
              /\ seedCommitted
              /\ aliasPresent
              /\ UNCHANGED <<parentGeneration, phase, capturedGeneration,
                             published, publicationGeneration,
                             budgetReserved, seedCommitted, aliasPresent,
                             activationReserved, cancelled>>

ReserveChildActivation == /\ phase = "admitted"
                          /\ seedCommitted
                          /\ (UnsafeActivationWithoutAlias \/ aliasPresent)
                          /\ ~activationReserved
                          /\ ~cancelled
                          /\ activationReserved' = TRUE
                          /\ UNCHANGED <<parentGeneration, phase,
                                         capturedGeneration, published,
                                         publicationGeneration,
                                         budgetReserved, seedCommitted,
                                         aliasPresent, cancelled>>

Start == /\ phase = "admitted"
         /\ activationReserved
         /\ ~cancelled
         /\ phase' = "running"
         /\ UNCHANGED <<parentGeneration, capturedGeneration, published,
                        publicationGeneration, budgetReserved, seedCommitted,
                        aliasPresent, activationReserved, cancelled>>

Complete == /\ phase = "running"
            /\ phase' = "completed"
            /\ UNCHANGED <<parentGeneration, capturedGeneration, published,
                           publicationGeneration, budgetReserved, seedCommitted,
                           aliasPresent, activationReserved, cancelled>>

AdvanceParent == /\ parentGeneration = 0
                 /\ parentGeneration' = 1
                 /\ UNCHANGED <<phase, capturedGeneration, published,
                                publicationGeneration, budgetReserved,
                                seedCommitted, aliasPresent,
                                activationReserved, cancelled>>

Publish == /\ phase = "completed"
           /\ ~published
           /\ (UnsafeStalePublish \/ parentGeneration = capturedGeneration)
           /\ published' = TRUE
           /\ publicationGeneration' = parentGeneration
           /\ UNCHANGED <<parentGeneration, phase, capturedGeneration,
                          budgetReserved, seedCommitted, aliasPresent,
                          activationReserved, cancelled>>

\* Cancellation before seed commit releases the resolver reservation. The
\* unsafe control models publishing after that cancellation linearization; it
\* retains the reservation so the counterexample isolates cancellation rather
\* than also violating reservation admission.
CancelResolver == /\ phase = "absent"
                  /\ budgetReserved
                  /\ ~seedCommitted
                  /\ ~cancelled
                  /\ budgetReserved' = IF UnsafePublishAfterResolverCancel
                                           THEN TRUE ELSE FALSE
                  /\ cancelled' = TRUE
                  /\ UNCHANGED <<parentGeneration, phase, capturedGeneration,
                                 published, publicationGeneration,
                                 seedCommitted, aliasPresent,
                                 activationReserved>>

Next == ReserveActivation \/ Fork \/ RegisterAlias \/ RetryAlias \/
        ReserveChildActivation \/ Start \/ Complete \/ AdvanceParent \/
        Publish \/ CancelResolver
Spec == Init /\ [][Next]_vars

TypeOK == /\ parentGeneration \in GenerationValues
          /\ phase \in {"absent", "admitted", "running", "completed"}
          /\ capturedGeneration \in GenerationValues
          /\ published \in BOOLEAN
          /\ publicationGeneration \in GenerationValues
          /\ budgetReserved \in BOOLEAN
          /\ seedCommitted \in BOOLEAN
          /\ aliasPresent \in BOOLEAN
          /\ activationReserved \in BOOLEAN
          /\ cancelled \in BOOLEAN

PublicationRequiresReservation == seedCommitted => budgetReserved
ActivationRequiresAlias == activationReserved => aliasPresent
CancelledNeverPublishes == cancelled => ~seedCommitted
PublicationAtCapturedGeneration ==
    published => publicationGeneration = capturedGeneration
=============================================================================

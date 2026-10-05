------------------------ MODULE SwarmPublication ------------------------
EXTENDS Naturals, FiniteSets

CONSTANT UnsafeStalePublish

Parent == 1
Child == 2
GenerationValues == 0..1

VARIABLES parentGeneration, phase, capturedGeneration, published,
          publicationGeneration

vars == <<parentGeneration, phase, capturedGeneration, published,
           publicationGeneration>>

Init == /\ parentGeneration = 0
        /\ phase = "absent"
        /\ capturedGeneration = 0
        /\ published = FALSE
        /\ publicationGeneration = 0

Fork == /\ phase = "absent"
        /\ phase' = "admitted"
        /\ capturedGeneration' = parentGeneration
        /\ UNCHANGED <<parentGeneration, published, publicationGeneration>>

Start == /\ phase = "admitted"
         /\ phase' = "running"
         /\ UNCHANGED <<parentGeneration, capturedGeneration, published,
                        publicationGeneration>>

Complete == /\ phase = "running"
            /\ phase' = "completed"
            /\ UNCHANGED <<parentGeneration, capturedGeneration, published,
                           publicationGeneration>>

AdvanceParent == /\ parentGeneration = 0
                 /\ parentGeneration' = 1
                 /\ UNCHANGED <<phase, capturedGeneration, published,
                                publicationGeneration>>

Publish == /\ phase = "completed"
           /\ ~published
           /\ (UnsafeStalePublish \/ parentGeneration = capturedGeneration)
           /\ published' = TRUE
           /\ publicationGeneration' = parentGeneration
           /\ UNCHANGED <<parentGeneration, phase, capturedGeneration>>

Next == Fork \/ Start \/ Complete \/ AdvanceParent \/ Publish
Spec == Init /\ [][Next]_vars

TypeOK == /\ parentGeneration \in GenerationValues
          /\ phase \in {"absent", "admitted", "running", "completed"}
          /\ capturedGeneration \in GenerationValues
          /\ published \in BOOLEAN
          /\ publicationGeneration \in GenerationValues

PublicationAtCapturedGeneration ==
    published => publicationGeneration = capturedGeneration
=============================================================================

------------------------ MODULE ActivationRecovery ------------------------
EXTENDS Naturals

CONSTANTS Owners, UnsafeFailureRelease, UnsafeFalseSuccess, UnsafeRedispatch
VARIABLES claim, owner, mode, started, dispatched, outcome, reported, cancelled,
          dispatchEvidence

vars == <<claim, owner, mode, started, dispatched, outcome, reported, cancelled,
           dispatchEvidence>>

Init == /\ claim = FALSE
        /\ owner = 0
        /\ mode = "none"
        /\ started = FALSE
        /\ dispatched = 0
        /\ outcome = "pending"
        /\ reported = FALSE
        /\ cancelled = FALSE
        /\ dispatchEvidence = "unknown"

Acquire(w) == /\ ~claim /\ owner = 0 /\ ~started /\ ~cancelled
              /\ outcome = "pending"
              /\ claim' = TRUE /\ owner' = w /\ mode' = "fresh"
              /\ UNCHANGED <<started, dispatched, outcome, reported, cancelled,
                             dispatchEvidence>>

\* Admission is durable before a provider can receive a request.
Admit == /\ claim /\ owner # 0 /\ ~started /\ ~cancelled
         /\ started' = TRUE
         /\ UNCHANGED <<claim, owner, mode, dispatched, outcome, reported,
                        cancelled, dispatchEvidence>>

\* dispatched is the actual effect count. The durable evidence remains
\* unknown until reconciliation persists an observation.
Dispatch == /\ claim /\ owner # 0 /\ started
            /\ outcome = "pending" /\ ~cancelled
            /\ ((mode = "fresh" /\ dispatched = 0)
                \/ (UnsafeRedispatch /\ mode = "reconcile" /\ dispatched = 1))
            /\ dispatched' = dispatched + 1 /\ mode' = "reconcile"
            /\ UNCHANGED <<claim, owner, started, outcome, reported, cancelled,
                           dispatchEvidence>>

\* Only an available, authoritative pre-admission journal permits release.
ProvenUnstartedFailure == /\ claim /\ owner # 0 /\ ~started
                         /\ claim' = FALSE /\ owner' = 0 /\ mode' = "none"
                         /\ outcome' = "failed"
                         /\ UNCHANGED <<started, dispatched, reported, cancelled,
                                        dispatchEvidence>>

\* A failed open/setup cannot prove an absent admission. The unsafe setting
\* deliberately models releasing its claim without inspecting that journal.
UnavailableJournalFailure == /\ claim /\ owner # 0
                             /\ claim' = IF UnsafeFailureRelease THEN FALSE ELSE claim
                             /\ owner' = 0 /\ mode' = "none"
                             /\ UNCHANGED <<started, dispatched, outcome, reported,
                                            cancelled, dispatchEvidence>>

\* A crash after durable admission leaves the provider effect ambiguous. The
\* actual effect count may be zero or one while the observer still has no
\* durable result evidence.
Crash == /\ owner # 0 /\ owner' = 0 /\ mode' = "none"
         /\ outcome' = IF ~reported /\ started THEN "indeterminate" ELSE outcome
         /\ UNCHANGED <<claim, started, dispatched, reported, cancelled,
                        dispatchEvidence>>

\* A cold owner can reconcile an admitted operation, but cannot redispatch it.
Recover(w) == /\ claim /\ owner = 0 /\ started /\ ~cancelled
              /\ outcome \in {"pending", "indeterminate"}
              /\ owner' = w /\ mode' = "reconcile"
              /\ UNCHANGED <<claim, started, dispatched, outcome, reported,
                             cancelled, dispatchEvidence>>

PersistResult == /\ claim /\ owner # 0 /\ mode = "reconcile"
                 /\ started /\ outcome \in {"pending", "indeterminate"}
                 /\ ~cancelled
                 /\ (UnsafeFalseSuccess \/ dispatched = 1)
                 /\ outcome' = "success"
                 /\ dispatchEvidence' = IF dispatched = 1 THEN "started" ELSE "unknown"
                 /\ UNCHANGED <<claim, owner, mode, started, dispatched,
                                reported, cancelled>>

PersistFailure == /\ claim /\ owner # 0 /\ mode = "reconcile"
                  /\ started /\ dispatched = 1
                  /\ outcome \in {"pending", "indeterminate"} /\ ~cancelled
                  /\ outcome' = "failed"
                  /\ dispatchEvidence' = "started"
                  /\ UNCHANGED <<claim, owner, mode, started, dispatched,
                                 reported, cancelled>>

\* Recovery may prove that the admitted provider effect never started.
PersistNoDispatch == /\ claim /\ owner # 0 /\ mode = "reconcile"
                     /\ started /\ dispatched = 0
                     /\ outcome \in {"pending", "indeterminate"} /\ ~cancelled
                     /\ outcome' = "failed"
                     /\ dispatchEvidence' = "absent"
                     /\ UNCHANGED <<claim, owner, mode, started, dispatched,
                                    reported, cancelled>>

ReportSuccess == /\ outcome = "success" /\ ~cancelled /\ ~reported
                 /\ reported' = TRUE
                 /\ UNCHANGED <<claim, owner, mode, started, dispatched, outcome,
                                cancelled, dispatchEvidence>>

ReportFailure == /\ outcome = "failed" /\ ~cancelled /\ ~reported
                 /\ reported' = TRUE
                 /\ UNCHANGED <<claim, owner, mode, started, dispatched, outcome,
                                cancelled, dispatchEvidence>>

Cancel == /\ ~cancelled /\ ~reported
          /\ cancelled' = TRUE /\ owner' = 0 /\ mode' = "none"
          /\ UNCHANGED <<claim, started, dispatched, outcome, reported,
                         dispatchEvidence>>

Next == (\E w \in Owners: Acquire(w) \/ Recover(w))
        \/ Admit \/ Dispatch \/ ProvenUnstartedFailure
        \/ UnavailableJournalFailure \/ Crash \/ PersistResult
        \/ PersistFailure \/ PersistNoDispatch \/ ReportSuccess
        \/ ReportFailure \/ Cancel

Spec == Init /\ [][Next]_vars

TypeOK == /\ claim \in BOOLEAN /\ owner \in Owners \cup {0}
          /\ mode \in {"none", "fresh", "reconcile"}
          /\ started \in BOOLEAN /\ dispatched \in 0..2
          /\ outcome \in {"pending", "indeterminate", "success", "failed"}
          /\ reported \in BOOLEAN /\ cancelled \in BOOLEAN
          /\ dispatchEvidence \in {"unknown", "started", "absent"}

AdmittedClaimRetained ==
    (started /\ outcome \in {"pending", "indeterminate"}) => claim
DispatchRequiresAdmission == dispatched > 0 => started
TerminalReportHasOutcome == reported => outcome \in {"success", "failed"}
SuccessRequiresDurableResult ==
    (reported /\ outcome = "success") => dispatched = 1
FailureRequiresDurableResult ==
    (reported /\ outcome = "failed") =>
        (~started \/ dispatchEvidence \in {"started", "absent"})
CancellationNeverReportsSuccess == cancelled => ~reported
CancellationBeforeAdmissionHasNoDispatch == (cancelled /\ ~started) => dispatched = 0
CancellationAfterAdmissionRetainsClaim == (cancelled /\ started) => claim
RecoveryDoesNotRedispatch == dispatched <= 1
DurableEvidenceMatchesObservation ==
    (dispatchEvidence = "started" => dispatched = 1)
    /\ (dispatchEvidence = "absent" => dispatched = 0)
=============================================================================

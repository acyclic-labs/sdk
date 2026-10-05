------------------------ MODULE ActivationRecovery ------------------------
EXTENDS Naturals

CONSTANTS Owners, UnsafeFailureRelease
VARIABLES claim, owner, mode, started, dispatched, outcome, reported, cancelled

vars == <<claim, owner, mode, started, dispatched, outcome, reported, cancelled>>

Init == /\ claim = FALSE
        /\ owner = 0
        /\ mode = "none"
        /\ started = FALSE
        /\ dispatched = 0
        /\ outcome = "pending"
        /\ reported = FALSE
        /\ cancelled = FALSE

Acquire(w) == /\ ~claim /\ owner = 0 /\ ~started /\ ~cancelled
              /\ outcome = "pending"
              /\ claim' = TRUE /\ owner' = w /\ mode' = "fresh"
              /\ UNCHANGED <<started, dispatched, outcome, reported, cancelled>>

\* Admission is durable before a provider can receive a request.
Admit == /\ claim /\ owner # 0 /\ ~started /\ ~cancelled
         /\ started' = TRUE
         /\ UNCHANGED <<claim, owner, mode, dispatched, outcome, reported, cancelled>>

Dispatch == /\ claim /\ owner # 0 /\ started /\ mode = "fresh"
            /\ outcome = "pending" /\ ~cancelled
            /\ dispatched' = dispatched + 1 /\ mode' = "reconcile"
            /\ UNCHANGED <<claim, owner, started, outcome, reported, cancelled>>

\* Only an available, authoritative pre-admission journal permits release.
ProvenUnstartedFailure == /\ claim /\ owner # 0 /\ ~started
                         /\ claim' = FALSE /\ owner' = 0 /\ mode' = "none"
                         /\ outcome' = "failed"
                         /\ UNCHANGED <<started, dispatched, reported, cancelled>>

\* A failed open/setup cannot prove an absent admission. The unsafe setting
\* deliberately models releasing its claim without inspecting that journal.
UnavailableJournalFailure == /\ claim /\ owner # 0
                             /\ claim' = IF UnsafeFailureRelease THEN FALSE ELSE claim
                             /\ owner' = 0 /\ mode' = "none"
                             /\ UNCHANGED <<started, dispatched, outcome, reported, cancelled>>

Crash == /\ owner # 0 /\ owner' = 0 /\ mode' = "none"
         /\ outcome' = IF ~reported /\ dispatched = 1 THEN "indeterminate" ELSE outcome
         /\ UNCHANGED <<claim, started, dispatched, reported, cancelled>>

\* A cold owner can reconcile an admitted operation, but cannot redispatch it.
Recover(w) == /\ claim /\ owner = 0 /\ started /\ ~cancelled
              /\ outcome \in {"pending", "indeterminate"}
              /\ owner' = w /\ mode' = "reconcile"
              /\ UNCHANGED <<claim, started, dispatched, outcome, reported, cancelled>>

PersistResult == /\ claim /\ owner # 0 /\ mode = "reconcile"
                 /\ started /\ dispatched = 1
                 /\ outcome \in {"pending", "indeterminate"} /\ ~cancelled
                 /\ outcome' = "success"
                 /\ UNCHANGED <<claim, owner, mode, started, dispatched, reported, cancelled>>

PersistFailure == /\ claim /\ owner # 0 /\ mode = "reconcile"
                  /\ started /\ dispatched = 1
                  /\ outcome \in {"pending", "indeterminate"} /\ ~cancelled
                  /\ outcome' = "failed"
                  /\ UNCHANGED <<claim, owner, mode, started, dispatched, reported, cancelled>>

ReportSuccess == /\ outcome = "success" /\ ~cancelled /\ ~reported
                 /\ reported' = TRUE
                 /\ UNCHANGED <<claim, owner, mode, started, dispatched, outcome, cancelled>>

ReportFailure == /\ outcome = "failed" /\ ~cancelled /\ ~reported
                 /\ reported' = TRUE
                 /\ UNCHANGED <<claim, owner, mode, started, dispatched, outcome, cancelled>>

Cancel == /\ ~cancelled /\ ~reported
          /\ cancelled' = TRUE /\ owner' = 0 /\ mode' = "none"
          /\ UNCHANGED <<claim, started, dispatched, outcome, reported>>

Next == (\E w \in Owners: Acquire(w) \/ Recover(w))
        \/ Admit \/ Dispatch \/ ProvenUnstartedFailure
        \/ UnavailableJournalFailure \/ Crash \/ PersistResult
        \/ PersistFailure \/ ReportSuccess \/ ReportFailure \/ Cancel

Spec == Init /\ [][Next]_vars

TypeOK == /\ claim \in BOOLEAN /\ owner \in Owners \cup {0}
          /\ mode \in {"none", "fresh", "reconcile"}
          /\ started \in BOOLEAN /\ dispatched \in 0..1
          /\ outcome \in {"pending", "indeterminate", "success", "failed"}
          /\ reported \in BOOLEAN /\ cancelled \in BOOLEAN

AdmittedClaimRetained ==
    (started /\ outcome \in {"pending", "indeterminate"}) => claim
DispatchRequiresAdmission == dispatched > 0 => started
TerminalReportHasOutcome == reported => outcome \in {"success", "failed"}
SuccessRequiresDurableResult ==
    (reported /\ outcome = "success") => dispatched = 1
FailureRequiresDurableResult ==
    (reported /\ outcome = "failed") => (~started \/ dispatched = 1)
CancellationNeverReportsSuccess == cancelled => ~reported
=============================================================================

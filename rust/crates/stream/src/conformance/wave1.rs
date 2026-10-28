//! Provider-independent histories for wave-1 key and subtree-fork contracts.
//!
//! These checks use only public provider operations. They establish observable
//! semantics, not crash durability or internal allocation bounds. A caller must
//! supply a fresh namespace and await each selected history without other writers
//! in that namespace. Keys are deterministic within that namespace, so rerunning
//! against retained state is a setup failure rather than a new qualification.

use crate::{
    AppendOutcome, AppendRequest, CommitCondition, CommitMutation, CommitOutcome, CommitRequest,
    IdempotencyKey, IdempotencyOutcome, OwnedBytes, ReadRequest, StreamError, StreamPath,
    StreamProvider,
};
use futures::TryStreamExt as _;
use sha2::{Digest as _, Sha256};

mod agent;
pub mod coverage;
mod fork;

/// Exact command for rerunning the endpoint suite with the same environment.
pub const RECOVERY: &str =
    "build-slot --remote cargo run -p acyclic-stream --features http --example wave1-conformance";

/// Fixed histories; no provider-specific cases or in-memory fallback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Case {
    /// A rejected append key remains available for its identical later retry.
    AppendRetry,
    /// A rejected coordinated-commit key remains available for identical retry.
    CommitRetry,
    /// A zero-tail condition admits a first append on an absent path.
    AbsentZeroTail,
    /// The entire subtree is inherited, with private writes and bounded reads.
    SubtreeFork,
    /// A composed fork sees pre-commit heads of root and descendants.
    ComposedFork,
    /// Agent spawn: subtree fork, adoption record and companion writes in one commit.
    Spawn,
    /// A closed inbox refuses every later send without spending its key.
    ClosedInbox,
    /// Inherited paths follow privately; a missing path follow is `NotFound`.
    InheritedFollow,
    /// Fork-of-fork keeps every ancestor's adoption record and conversation cut.
    ForkLineage,
}

impl Case {
    /// Every gate history in stable order.
    pub const ALL: [Self; 9] = [
        Self::AppendRetry,
        Self::CommitRetry,
        Self::AbsentZeroTail,
        Self::SubtreeFork,
        Self::ComposedFork,
        Self::Spawn,
        Self::ClosedInbox,
        Self::InheritedFollow,
        Self::ForkLineage,
    ];

    /// Stable name used in receipts and case selection.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AppendRetry => "append_retry",
            Self::CommitRetry => "commit_retry",
            Self::AbsentZeroTail => "absent_zero_tail",
            Self::SubtreeFork => "subtree_fork",
            Self::ComposedFork => "composed_fork",
            Self::Spawn => "spawn",
            Self::ClosedInbox => "closed_inbox",
            Self::InheritedFollow => "inherited_follow",
            Self::ForkLineage => "fork_lineage",
        }
    }
}

/// Independently reported assertion, including setup and recovery checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Invariant {
    /// Paths and deterministic keys were unused before the history.
    FreshNamespace,
    /// The rejected request returned the exact expected conflict outcome.
    ExactConflict,
    /// A conflict retained no key observation.
    RejectedKeyUnspent,
    /// An identical request succeeded once its precondition became true.
    StableKeyRetry,
    /// Tail zero matched an absent stream.
    ZeroTailAbsence,
    /// Retrying after discarding the acknowledgement returned the original fact.
    ExactReplay,
    /// Recovery inspection returned the successful immutable intent.
    RecoveryMatchesReceipt,
    /// A successful key rejected different arguments with `IdempotencyMismatch`.
    ChangedIntentRejected,
    /// Replays and rejected mutations changed neither sequence nor record values.
    NoDuplicateRecords,
    /// Root and deep descendants belong to the inherited namespace cut.
    SubtreeMembership,
    /// Source and destination writes cannot modify each other's inherited cut.
    ForkIsolation,
    /// A composed fork inherits the source namespace before companion mutations.
    PreCommitCut,
    /// The retained composed envelope and replay are exactly the returned fact.
    CompositionReceipt,
    /// A refused spawn published none of its mutations; a committed one all.
    SpawnAtomicity,
    /// A missing-path follow is `NotFound` and creates nothing.
    MissingFollow,
    /// Descendants carry ancestor records with their original identities.
    Lineage,
}

/// A semantic violation is distinct from provider/setup/transport failure.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// The provider could not complete a particular operation.
    #[error("wave1 case={case:?} step={step} provider={source}; recovery: {RECOVERY}")]
    Provider {
        /// Selected history.
        case: Case,
        /// Exact failing operation.
        step: &'static str,
        /// Original typed provider error.
        source: StreamError,
    },
    /// The provider completed an operation but contradicted the contract.
    #[error("wave1 case={case:?} step={step} invariant={invariant:?}; recovery: {RECOVERY}")]
    Violation {
        /// Selected history.
        case: Case,
        /// Exact failing assertion.
        step: &'static str,
        /// Contract that was contradicted.
        invariant: Invariant,
    },
}

struct History<'a, P: ?Sized> {
    provider: &'a P,
    case: Case,
    path: StreamPath,
    key: IdempotencyKey,
}

impl<'a, P: StreamProvider + ?Sized> History<'a, P> {
    fn new(provider: &'a P, namespace: &StreamPath, case: Case) -> Result<Self, Failure> {
        let path = StreamPath::new(format!("{}/{}", namespace.as_str(), case.name()))
            .map_err(|source| provider_failure(case, "derive_path", source))?;
        let digest = Sha256::digest(path.as_str().as_bytes());
        let key = IdempotencyKey::new(OwnedBytes::from(digest.to_vec()))
            .map_err(|source| provider_failure(case, "derive_key", source))?;
        Ok(Self {
            provider,
            case,
            path,
            key,
        })
    }

    fn check(&self, step: &'static str, invariant: Invariant, holds: bool) -> Result<(), Failure> {
        if holds {
            Ok(())
        } else {
            Err(Failure::Violation {
                case: self.case,
                step,
                invariant,
            })
        }
    }

    fn result<T>(&self, step: &'static str, result: Result<T, StreamError>) -> Result<T, Failure> {
        result.map_err(|source| provider_failure(self.case, step, source))
    }

    async fn require_fresh(&self) -> Result<(), Failure> {
        match self.provider.tail(self.path.clone()).await {
            Err(StreamError::NotFound) => (),
            Ok(_) => return self.check("fresh_path", Invariant::FreshNamespace, false),
            Err(source) => return Err(provider_failure(self.case, "fresh_path", source)),
        }
        let observed = self.result(
            "fresh_key",
            self.provider.inspect_idempotency(self.key.clone()).await,
        )?;
        self.check("fresh_key", Invariant::FreshNamespace, observed.is_none())
    }

    async fn require_unspent(&self) -> Result<(), Failure> {
        let observed = self.result(
            "inspect_conflict",
            self.provider.inspect_idempotency(self.key.clone()).await,
        )?;
        self.check(
            "inspect_conflict",
            Invariant::RejectedKeyUnspent,
            observed.is_none(),
        )
    }

    async fn seed(&self) -> Result<(), Failure> {
        let digest = Sha256::digest(format!("{}:seed", self.path.as_str()).as_bytes());
        let key = self.result(
            "seed_key",
            IdempotencyKey::new(OwnedBytes::from(digest.to_vec())),
        )?;
        let seeded = self.result(
            "seed",
            self.provider
                .append(AppendRequest {
                    path: self.path.clone(),
                    records: vec![OwnedBytes::from_static(b"seed").into()],
                    if_tail: Some(0),
                    idempotency_key: key,
                })
                .await,
        )?;
        self.check("seed", Invariant::StableKeyRetry,
            matches!(seeded, AppendOutcome::Committed(ref receipt) if receipt.start == 0 && receipt.end == 1 && receipt.tail == 1))
    }

    async fn require_recovery(&self, expected: IdempotencyOutcome) -> Result<(), Failure> {
        let first = self.result(
            "inspect_success",
            self.provider.inspect_idempotency(self.key.clone()).await,
        )?;
        self.check(
            "inspect_success",
            Invariant::RecoveryMatchesReceipt,
            first.as_ref().is_some_and(|observation| {
                observation.idempotency_key == self.key
                    && observation.request_digest != [0; 32]
                    && observation.outcome == expected
            }),
        )?;
        let second = self.result(
            "inspect_success_again",
            self.provider.inspect_idempotency(self.key.clone()).await,
        )?;
        self.check(
            "inspect_success_again",
            Invariant::RecoveryMatchesReceipt,
            first == second,
        )
    }

    fn require_mismatch<T>(
        &self,
        step: &'static str,
        result: Result<T, StreamError>,
    ) -> Result<(), Failure> {
        match result {
            Err(StreamError::IdempotencyMismatch) => Ok(()),
            Ok(_) => self.check(step, Invariant::ChangedIntentRejected, false),
            Err(source) => Err(provider_failure(self.case, step, source)),
        }
    }

    async fn require_values(&self, expected: &[&[u8]]) -> Result<(), Failure> {
        let records = self.result(
            "read",
            self.provider
                .read(ReadRequest {
                    key: None,
                    path: self.path.clone(),
                    from: 0,
                    limit: 8,
                    at_least: Default::default(),
                })
                .await,
        )?;
        // take(9) prevents a broken finite reader from growing the oracle without bound.
        let records = self.result(
            "collect_read",
            futures::StreamExt::take(records, 9)
                .try_collect::<Vec<_>>()
                .await,
        )?;
        let values_match = records.len() == expected.len()
            && records
                .iter()
                .zip(expected)
                .enumerate()
                .all(|(sequence, (record, value))| {
                    usize::try_from(record.sequence) == Ok(sequence)
                        && record.value.as_ref() == *value
                });
        let tail = self.result("final_tail", self.provider.tail(self.path.clone()).await)?;
        self.check(
            "final_records",
            Invariant::NoDuplicateRecords,
            values_match && usize::try_from(tail) == Ok(expected.len()),
        )
    }
}

fn provider_failure(case: Case, step: &'static str, source: StreamError) -> Failure {
    Failure::Provider { case, step, source }
}

/// Runs every fixed history against this exact provider in a fresh namespace.
///
/// The returned cases completed all assertions. Callers must impose an outer
/// deadline appropriate to their executor; the endpoint runner uses nine seconds
/// per history. Receipt loss here discards a result and retries; it does not claim
/// a process crash or a transport fault was injected.
pub async fn verify<P: StreamProvider + ?Sized>(
    provider: &P,
    namespace: &StreamPath,
) -> Result<[Case; 9], Failure> {
    for case in Case::ALL {
        verify_case(provider, namespace, case).await?;
    }
    Ok(Case::ALL)
}

/// Runs one named history, unchanged for memory, local, browser and hosted use.
pub async fn verify_case<P: StreamProvider + ?Sized>(
    provider: &P,
    namespace: &StreamPath,
    case: Case,
) -> Result<(), Failure> {
    let history = History::new(provider, namespace, case)?;
    history.require_fresh().await?;
    match case {
        Case::AppendRetry => append_retry(&history).await,
        Case::CommitRetry => commit_retry(&history).await,
        Case::AbsentZeroTail => absent_zero_tail(&history).await,
        Case::SubtreeFork => fork::subtree(&history).await,
        Case::ComposedFork => fork::composed(&history).await,
        Case::Spawn => agent::spawn(&history).await,
        Case::ClosedInbox => agent::closed_inbox(&history).await,
        Case::InheritedFollow => agent::inherited_follow(&history).await,
        Case::ForkLineage => agent::fork_lineage(&history).await,
    }
}

async fn append_retry<P: StreamProvider + ?Sized>(history: &History<'_, P>) -> Result<(), Failure> {
    let request = AppendRequest {
        path: history.path.clone(),
        records: vec![OwnedBytes::from_static(b"intent").into()],
        if_tail: Some(1),
        idempotency_key: history.key.clone(),
    };
    let conflict = history.result(
        "reject_append",
        history.provider.append(request.clone()).await,
    )?;
    history.check(
        "reject_append",
        Invariant::ExactConflict,
        conflict == AppendOutcome::TailConflict { actual_tail: 0 },
    )?;
    history.require_unspent().await?;
    history.seed().await?;
    let success = history.result(
        "retry_append",
        history.provider.append(request.clone()).await,
    )?;
    history.check("retry_append", Invariant::StableKeyRetry,
        matches!(success, AppendOutcome::Committed(ref receipt) if receipt.start == 1 && receipt.end == 2 && receipt.tail == 2))?;
    // Treat the first response as lost; reconcile and retry the original request.
    history
        .require_recovery(IdempotencyOutcome::Append(success.clone()))
        .await?;
    let replay = history.result(
        "replay_append",
        history.provider.append(request.clone()).await,
    )?;
    history.check("replay_append", Invariant::ExactReplay, replay == success)?;
    let changed = AppendRequest {
        records: vec![OwnedBytes::from_static(b"changed").into()],
        ..request
    };
    history.require_mismatch("changed_append", history.provider.append(changed).await)?;
    history
        .require_recovery(IdempotencyOutcome::Append(success))
        .await?;
    history.require_values(&[b"seed", b"intent"]).await
}

fn commit_request<P: ?Sized>(
    history: &History<'_, P>,
    expected: u64,
    value: &'static [u8],
) -> CommitRequest {
    CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: history.path.clone(),
            expected,
        }],
        mutations: vec![CommitMutation::Append {
            path: history.path.clone(),
            records: vec![OwnedBytes::from_static(value).into()],
        }],
        idempotency_key: history.key.clone(),
    }
}

async fn commit_retry<P: StreamProvider + ?Sized>(history: &History<'_, P>) -> Result<(), Failure> {
    let request = commit_request(history, 1, b"intent");
    let conflict = history.result(
        "reject_commit",
        history.provider.commit(request.clone()).await,
    )?;
    history.check("reject_commit", Invariant::ExactConflict,
        matches!(conflict, CommitOutcome::Conflict(ref conflicts) if conflicts == &[crate::CommitConflict::Tail {
            path: history.path.clone(), expected: 1, actual: None,
        }]))?;
    history.require_unspent().await?;
    history.seed().await?;
    let success = history.result(
        "retry_commit",
        history.provider.commit(request.clone()).await,
    )?;
    history.check(
        "retry_commit",
        Invariant::StableKeyRetry,
        matches!(success, CommitOutcome::Committed(_)),
    )?;
    let envelope = match &success {
        CommitOutcome::Committed(receipt) => fork::facts(history, receipt).await?,
        CommitOutcome::Conflict(_) => {
            return Err(Failure::Violation {
                case: history.case,
                step: "committed_facts",
                invariant: Invariant::RecoveryMatchesReceipt,
            });
        }
    };
    history
        .require_recovery(IdempotencyOutcome::Commit(envelope.clone()))
        .await?;
    let replay = history.result("replay_commit", history.provider.commit(request).await)?;
    history.check("replay_commit", Invariant::ExactReplay, replay == success)?;
    history.require_mismatch(
        "changed_commit",
        history
            .provider
            .commit(commit_request(history, 1, b"changed"))
            .await,
    )?;
    history
        .require_recovery(IdempotencyOutcome::Commit(envelope))
        .await?;
    history.require_values(&[b"seed", b"intent"]).await
}

async fn absent_zero_tail<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let request = commit_request(history, 0, b"first");
    let success = history.result(
        "absent_tail_zero",
        history.provider.commit(request.clone()).await,
    )?;
    history.check(
        "absent_tail_zero",
        Invariant::ZeroTailAbsence,
        matches!(success, CommitOutcome::Committed(_)),
    )?;
    let envelope = match &success {
        CommitOutcome::Committed(receipt) => fork::facts(history, receipt).await?,
        CommitOutcome::Conflict(_) => {
            return Err(Failure::Violation {
                case: history.case,
                step: "committed_facts",
                invariant: Invariant::RecoveryMatchesReceipt,
            });
        }
    };
    history
        .require_recovery(IdempotencyOutcome::Commit(envelope.clone()))
        .await?;
    let replay = history.result("replay_zero", history.provider.commit(request).await)?;
    history.check("replay_zero", Invariant::ExactReplay, replay == success)?;
    history.require_values(&[b"first"]).await
}

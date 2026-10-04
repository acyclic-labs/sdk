//! Authenticated bridge between a provider-owned generation and one attached
//! native checkout.
//!
//! This module deliberately composes [`Source`] and the native materializer.
//! It does not copy a checkout into an adapter-owned store and it does not
//! infer an operating-system path from a [`VolumeRef`]. The source binding and
//! generation precondition is supplied as an immutable target while the
//! source root capability and identity remain owned by this handle.

use crate::native_host::HostRoot;
use crate::native_mount::{
    HostPathReplacement, HostPathRestore, MaterializationReceipt, MaterializeError,
    MaterializeOptions,
};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, Generation, GenerationId,
    IdempotencyKey, MaterializationJournal, OperationId, OperationReceipt, Source, SourceBinding,
    SourceError, SourceOptions, SourceState, WorkBudget, WorkCounters, Workspace, WorkspaceError,
};
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
use crate::{
    NativeWorkspacePublication, NativeWorkspacePublicationError,
    publish_native_generation_transition,
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

/// Fail-closed errors returned by [`HostCheckout`].
#[derive(Debug, Error)]
pub enum HostCheckoutError {
    /// The supplied workspace was not created by native source attachment.
    #[error("workspace has no attached native checkout source")]
    NoSource,
    /// Native source state or identity validation failed.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Exact cumulative work accounting overflowed or exceeded its bound.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Authenticated workspace or generation operation failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// The source generation changed since the caller recorded its approval
    /// precondition.
    #[error("attached checkout generation is stale: expected {expected:?}, actual {actual:?}")]
    StaleSource {
        /// Generation retained by the approval record.
        expected: GenerationId,
        /// Generation currently authenticated by the attached source.
        actual: GenerationId,
    },
    /// The materialization destination was not the exact attached checkout.
    #[error("materialization destination is not the attached checkout root")]
    DestinationMismatch,
    /// The requested paths are not a deterministic, non-overlapping set.
    #[error("restore paths must be sorted and non-overlapping")]
    InvalidPaths,
    /// The held checkout root could not be opened or retained.
    #[error("failed to retain the attached checkout root: {0}")]
    Host(#[from] std::io::Error),
}

/// Exact result of a bounded sequence of host-path replacements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCheckoutRestore {
    /// Per-path authenticated replacement results, in request order.
    pub outcomes: Vec<HostPathRestore>,
    /// Additive work performed by all replacements.
    pub work: WorkCounters,
}

/// Immutable, provider-bound target for one host restore operation.
///
/// The request is created by [`HostCheckout::new_restore_request`], so the
/// source root and identity are observations of the retained provider-owned
/// capability rather than caller-provided authorization. Harness persistence
/// should store this request's operation key and fingerprint with its durable
/// approval record and replay the same request after recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostRestoreRequest {
    operation: IdempotencyKey,
    source_generation: GenerationId,
    target_generation: GenerationId,
    source_root: PathBuf,
    root_identity: crate::NativeRootIdentity,
    paths: Vec<PathBuf>,
    replacement: HostPathReplacement,
    options: MaterializeOptions,
    fingerprint: crate::Digest,
}

/// The immutable request presented to the authority before a host checkout is
/// changed.  Keeping the request separate from its approval makes it
/// impossible for a caller to approve one operation and execute another one
/// by changing a path, generation, or retry identity between the two steps.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostCheckoutRootWritebackRequest {
    operation_id: OperationId,
    source_generation: GenerationId,
    target_generation: GenerationId,
    root: PathBuf,
    operation_directory: PathBuf,
    root_identity: [u8; 16],
    options_digest: Option<crate::Digest>,
    excluded_names_digest: Option<crate::Digest>,
    request_digest: crate::Digest,
}

impl HostCheckoutRootWritebackRequest {
    /// Creates a request after opening and identifying the destination root.
    /// The operation directory is retained as part of the immutable request;
    /// the native publisher still validates that it is outside the checkout
    /// and on the same volume immediately before staging.
    pub fn new(
        operation_id: OperationId,
        source_generation: GenerationId,
        target_generation: GenerationId,
        root: impl AsRef<Path>,
        operation_directory: impl AsRef<Path>,
    ) -> Result<Self, HostCheckoutError> {
        let root = root.as_ref().to_path_buf();
        let operation_directory = operation_directory.as_ref().to_path_buf();
        let root_handle = HostRoot::open(&root)?;
        let root_identity = root_handle.identity().to_bytes();
        let request_digest = root_writeback_request_fingerprint(
            operation_id,
            source_generation,
            target_generation,
            &root,
            &operation_directory,
            root_identity,
            None,
            None,
        );
        Ok(Self {
            operation_id,
            source_generation,
            target_generation,
            root,
            operation_directory,
            root_identity,
            options_digest: None,
            excluded_names_digest: None,
            request_digest,
        })
    }

    /// Creates a request whose publication policy is fully bound into the
    /// durable approval digest.  Callers performing native publication should
    /// use this constructor rather than the legacy request-only constructor.
    pub fn new_with_options(
        operation_id: OperationId,
        source_generation: GenerationId,
        target_generation: GenerationId,
        root: impl AsRef<Path>,
        operation_directory: impl AsRef<Path>,
        options: &MaterializeOptions,
        excluded_names: &[&str],
    ) -> Result<Self, HostCheckoutError> {
        let root = root.as_ref().to_path_buf();
        let operation_directory = operation_directory.as_ref().to_path_buf();
        let root_handle = HostRoot::open(&root)?;
        let root_identity = root_handle.identity().to_bytes();
        let options_digest = materialize_options_digest(options);
        let excluded_names_digest = excluded_names_digest(excluded_names);
        let request_digest = root_writeback_request_fingerprint(
            operation_id,
            source_generation,
            target_generation,
            &root,
            &operation_directory,
            root_identity,
            Some(options_digest),
            Some(excluded_names_digest),
        );
        Ok(Self {
            operation_id,
            source_generation,
            target_generation,
            root,
            operation_directory,
            root_identity,
            options_digest: Some(options_digest),
            excluded_names_digest: Some(excluded_names_digest),
            request_digest,
        })
    }

    /// Stable retry identity bound by the approval.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Generation expected to be represented by the host before publication.
    #[must_use]
    pub const fn source_generation(&self) -> GenerationId {
        self.source_generation
    }

    /// Generation selected for host publication.
    #[must_use]
    pub const fn target_generation(&self) -> GenerationId {
        self.target_generation
    }

    /// Destination checkout path captured in the request.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Service-owned staging and recovery directory captured in the request.
    #[must_use]
    pub fn operation_directory(&self) -> &Path {
        &self.operation_directory
    }

    /// Stable identity of the root opened while preparing the request.
    #[must_use]
    pub const fn root_identity(&self) -> [u8; 16] {
        self.root_identity
    }

    /// Digest covering every operation input and the observed root identity.
    #[must_use]
    pub const fn request_digest(&self) -> crate::Digest {
        self.request_digest
    }

    /// Asks the caller's authenticated Harness authority to admit this exact
    /// request.  Filesystem never stores or constructs an operator grant;
    /// the verifier resolves the durable interaction and binds it to the
    /// operation and request digest before this private intent is created.
    pub async fn authorize_with<V: RootWritebackApprovalVerifier + ?Sized>(
        &self,
        verifier: &V,
    ) -> Result<HostCheckoutRootWritebackIntent, HostCheckoutRootWritebackError> {
        verifier
            .verify(RootWritebackApprovalContext {
                operation_id: self.operation_id,
                request_digest: self.request_digest,
            })
            .await
            .map_err(|error| HostCheckoutRootWritebackError::ApprovalDenied(error.to_string()))?;
        Ok(HostCheckoutRootWritebackIntent {
            request: self.clone(),
        })
    }
}

/// One approved, immutable host writeback operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostCheckoutRootWritebackIntent {
    request: HostCheckoutRootWritebackRequest,
}

impl HostCheckoutRootWritebackIntent {
    /// Returns the immutable request bound to this intent.
    #[must_use]
    pub const fn request(&self) -> &HostCheckoutRootWritebackRequest {
        &self.request
    }

    /// Operation identity used by both the authority journal and the native
    /// materialization journal.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.request.operation_id
    }

    /// Checks that persisted request bytes still describe the same root and
    /// operation before they are admitted for replay.
    pub fn validate(&self) -> Result<(), HostCheckoutRootWritebackError> {
        let digest = root_writeback_request_fingerprint(
            self.request.operation_id,
            self.request.source_generation,
            self.request.target_generation,
            &self.request.root,
            &self.request.operation_directory,
            self.request.root_identity,
            self.request.options_digest,
            self.request.excluded_names_digest,
        );
        if digest != self.request.request_digest {
            return Err(HostCheckoutRootWritebackError::CorruptRequest);
        }
        Ok(())
    }

    /// Admits the operation in a durable CAS journal before host execution.
    pub async fn admit<S: RootWritebackJournalStore>(
        &self,
        store: &S,
    ) -> Result<RootWritebackJournal, RootWritebackJournalError<S::Error>> {
        self.validate()
            .map_err(RootWritebackJournalError::InvalidRequest)?;
        let journal = RootWritebackJournal {
            version: ROOT_WRITEBACK_JOURNAL_VERSION,
            revision: 1,
            intent: self.clone(),
            phase: RootWritebackPhase::Admitted,
            outcome: RootWritebackOutcome::None,
        };
        match store
            .load(self.operation_id())
            .await
            .map_err(RootWritebackJournalError::Store)?
        {
            Some(existing) => {
                if existing.version != ROOT_WRITEBACK_JOURNAL_VERSION || existing.intent != *self {
                    return Err(RootWritebackJournalError::Conflict);
                }
                Ok(existing)
            }
            None => {
                if store
                    .compare_and_swap(self.operation_id(), 0, journal.clone())
                    .await
                    .map_err(RootWritebackJournalError::Store)?
                {
                    Ok(journal)
                } else {
                    let existing = store
                        .load(self.operation_id())
                        .await
                        .map_err(RootWritebackJournalError::Store)?
                        .ok_or(RootWritebackJournalError::Contended)?;
                    if existing.intent == *self {
                        Ok(existing)
                    } else {
                        Err(RootWritebackJournalError::Conflict)
                    }
                }
            }
        }
    }

    /// Resolves an uncertain publication without replaying the physical
    /// effect. The verifier must authenticate the same durable
    /// Harness/operator interaction used for the original request; changing
    /// the journal is CAS fenced.
    pub async fn recover<S: RootWritebackJournalStore>(
        &self,
        store: &S,
        verifier: &impl RootWritebackApprovalVerifier,
        recovery: RootWritebackRecovery,
    ) -> Result<RootWritebackJournal, HostCheckoutRootWritebackError> {
        verifier
            .verify(RootWritebackApprovalContext {
                operation_id: self.request.operation_id,
                request_digest: self.request.request_digest,
            })
            .await
            .map_err(|error| HostCheckoutRootWritebackError::ApprovalDenied(error.to_string()))?;
        let current = store
            .load(self.operation_id())
            .await
            .map_err(|error| HostCheckoutRootWritebackError::Journal(error.to_string()))?
            .ok_or(HostCheckoutRootWritebackError::Conflict)?;
        if current.intent != *self || current.phase != RootWritebackPhase::Publishing {
            return Err(HostCheckoutRootWritebackError::Conflict);
        }
        let (phase, outcome) = match recovery {
            RootWritebackRecovery::Failed => {
                (RootWritebackPhase::Failed, RootWritebackOutcome::Failed)
            }
            RootWritebackRecovery::Conflicted => (
                RootWritebackPhase::Conflicted,
                RootWritebackOutcome::Conflict,
            ),
            RootWritebackRecovery::Cancelled => (
                RootWritebackPhase::Cancelled,
                RootWritebackOutcome::Cancelled,
            ),
        };
        let resolved = RootWritebackJournal {
            version: current.version,
            revision: current.revision.saturating_add(1),
            intent: self.clone(),
            phase,
            outcome,
        };
        if !store
            .compare_and_swap(self.operation_id(), current.revision, resolved.clone())
            .await
            .map_err(|error| HostCheckoutRootWritebackError::Journal(error.to_string()))?
        {
            return Err(HostCheckoutRootWritebackError::Conflict);
        }
        Ok(resolved)
    }

    /// Publishes through the existing journaled native publisher after the
    /// authority admission has been durably recorded. A restart can replay a
    /// `Publishing` journal with the same operation and exact staging paths.
    #[cfg(all(
        feature = "local",
        feature = "native-mount",
        not(target_arch = "wasm32")
    ))]
    pub async fn publish_native<A, O, S, V>(
        &self,
        from_generation: &Generation<A, O>,
        to_generation: &Generation<A, O>,
        store: &S,
        options: &MaterializeOptions,
        excluded_names: &[&str],
        budget: WorkBudget,
        cancellation: &CancellationToken,
        verifier: &V,
        root_handle: Arc<HostRoot>,
    ) -> Result<HostCheckoutRootWritebackResult, HostCheckoutRootWritebackError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        S: RootWritebackJournalStore,
        V: RootWritebackApprovalVerifier + ?Sized,
    {
        if from_generation.id() != self.request.source_generation
            || to_generation.id() != self.request.target_generation
            || options.destination != self.request.operation_directory.join("target")
            || self.request.options_digest != Some(materialize_options_digest(options))
            || self.request.excluded_names_digest != Some(excluded_names_digest(excluded_names))
        {
            return Err(HostCheckoutRootWritebackError::RequestMismatch);
        }
        verifier
            .verify(RootWritebackApprovalContext {
                operation_id: self.request.operation_id,
                request_digest: self.request.request_digest,
            })
            .await
            .map_err(|error| HostCheckoutRootWritebackError::ApprovalDenied(error.to_string()))?;
        if root_handle.identity().to_bytes() != self.request.root_identity {
            return Err(HostCheckoutRootWritebackError::RootIdentityMismatch);
        }
        let admitted = self
            .admit(store)
            .await
            .map_err(|error| HostCheckoutRootWritebackError::Journal(error.to_string()))?;
        if admitted.intent != *self {
            return Err(HostCheckoutRootWritebackError::Conflict);
        }
        if cancellation.is_cancelled() && !matches!(admitted.phase, RootWritebackPhase::Published) {
            let cancelled = RootWritebackJournal {
                version: admitted.version,
                revision: admitted.revision.saturating_add(1),
                intent: self.clone(),
                phase: RootWritebackPhase::Cancelled,
                outcome: RootWritebackOutcome::Cancelled,
            };
            let cancelled = store
                .compare_and_swap(self.operation_id(), admitted.revision, cancelled)
                .await
                .map_err(|error| HostCheckoutRootWritebackError::Journal(error.to_string()))?;
            if !cancelled {
                return Err(HostCheckoutRootWritebackError::Journal(
                    RootWritebackJournalError::<S::Error>::Contended.to_string(),
                ));
            }
            return Err(HostCheckoutRootWritebackError::Cancelled);
        }
        let publishing_revision = match admitted.phase {
            RootWritebackPhase::Published => {
                return Ok(HostCheckoutRootWritebackResult::AlreadyPublished);
            }
            RootWritebackPhase::Conflicted
            | RootWritebackPhase::Failed
            | RootWritebackPhase::Cancelled => {
                return Err(HostCheckoutRootWritebackError::Conflict);
            }
            RootWritebackPhase::Publishing => admitted.revision,
            RootWritebackPhase::Admitted => {
                let publishing = RootWritebackJournal {
                    version: admitted.version,
                    revision: admitted.revision.saturating_add(1),
                    intent: self.clone(),
                    phase: RootWritebackPhase::Publishing,
                    outcome: RootWritebackOutcome::Unknown,
                };
                if !store
                    .compare_and_swap(self.operation_id(), admitted.revision, publishing)
                    .await
                    .map_err(|error| HostCheckoutRootWritebackError::Journal(error.to_string()))?
                {
                    return Err(HostCheckoutRootWritebackError::Journal(
                        RootWritebackJournalError::<S::Error>::Contended.to_string(),
                    ));
                }
                admitted.revision.saturating_add(1)
            }
        };
        let publication = NativeWorkspacePublication {
            root: &self.request.root,
            operation_directory: &self.request.operation_directory,
            operation_id: self.request.operation_id,
            from: self.request.source_generation,
            to: self.request.target_generation,
            excluded_names,
            options,
            budget,
            cancellation,
            root_handle: Some(root_handle),
        };
        let result = publish_native_generation_transition(
            from_generation,
            to_generation,
            store.materialization_store(),
            publication,
        )
        .await
        .map_err(HostCheckoutRootWritebackError::Native);
        let journal = match result {
            Ok(journal) => journal,
            Err(error) => {
                if matches!(
                    &error,
                    HostCheckoutRootWritebackError::Native(
                        NativeWorkspacePublicationError::Native(
                            crate::NativeTreeMaterializationError::ExternalMutation(_)
                        )
                    ) | HostCheckoutRootWritebackError::Native(
                        NativeWorkspacePublicationError::Materialization(
                            crate::MaterializationError::Backend(
                                crate::NativeTreeMaterializationError::ExternalMutation(_)
                            )
                        )
                    )
                ) {
                    let conflict = RootWritebackJournal {
                        version: ROOT_WRITEBACK_JOURNAL_VERSION,
                        revision: publishing_revision.saturating_add(1),
                        intent: self.clone(),
                        phase: RootWritebackPhase::Conflicted,
                        outcome: RootWritebackOutcome::Conflict,
                    };
                    let _ = store
                        .compare_and_swap(self.operation_id(), publishing_revision, conflict)
                        .await;
                }
                return Err(error);
            }
        };
        let finished = RootWritebackJournal {
            version: ROOT_WRITEBACK_JOURNAL_VERSION,
            revision: publishing_revision.saturating_add(1),
            intent: self.clone(),
            phase: RootWritebackPhase::Published,
            outcome: RootWritebackOutcome::Applied,
        };
        if !store
            .compare_and_swap(self.operation_id(), publishing_revision, finished)
            .await
            .map_err(|error| HostCheckoutRootWritebackError::Journal(error.to_string()))?
        {
            return Err(HostCheckoutRootWritebackError::Journal(
                RootWritebackJournalError::<S::Error>::Contended.to_string(),
            ));
        }
        Ok(HostCheckoutRootWritebackResult::Applied(journal))
    }
}

/// Durable lifecycle for one root writeback operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RootWritebackPhase {
    /// Request and approval were persisted before dispatch.
    Admitted,
    /// Native staging or publication may have started.
    Publishing,
    /// The native publication journal reached its terminal applied state.
    Published,
    /// Recovery observed an external mutation and requires an explicit retry.
    Conflicted,
    /// Publication failed before a safe retry boundary was established.
    Failed,
    /// Publication was cancelled before completion.
    Cancelled,
}

/// Durable outcome attached to a root writeback phase.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RootWritebackOutcome {
    /// No terminal observation exists yet.
    None,
    /// The dispatch outcome is unknown and must be reconciled from journals.
    Unknown,
    /// The target reached the host checkout.
    Applied,
    /// An external mutation fenced publication.
    Conflict,
    /// The operation was cancelled before dispatch.
    Cancelled,
    /// The operation failed before its effect could be considered unknown.
    Failed,
}

/// Explicit operator resolution for a journal whose physical outcome is
/// unknown.  Recovery never silently re-dispatches an uncertain operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RootWritebackRecovery {
    /// Mark the operation as requiring a fresh approved request.
    Failed,
    /// Record an observed external mutation.
    Conflicted,
    /// Record an explicit cancellation.
    Cancelled,
}

/// Result of an idempotent host writeback request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostCheckoutRootWritebackResult {
    /// This invocation completed the native publication.
    Applied(MaterializationJournal),
    /// A prior invocation already reached the terminal published state.
    AlreadyPublished,
}

/// Durable root-writeback authority record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RootWritebackJournal {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic CAS revision.
    pub revision: u64,
    /// Immutable approved intent.
    pub intent: HostCheckoutRootWritebackIntent,
    /// Lifecycle phase.
    pub phase: RootWritebackPhase,
    /// Durable dispatch observation.
    pub outcome: RootWritebackOutcome,
}

const ROOT_WRITEBACK_JOURNAL_VERSION: u32 = 2;

/// Durable CAS storage required by [`HostCheckoutRootWritebackIntent`].
pub trait RootWritebackJournalStore: Send + Sync {
    /// Storage error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads the exact operation record.
    fn load(
        &self,
        operation_id: OperationId,
    ) -> impl std::future::Future<Output = Result<Option<RootWritebackJournal>, Self::Error>> + Send;

    /// Replaces `expected_revision`; zero creates a record.
    fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: RootWritebackJournal,
    ) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send;

    /// Materialization storage used by the native publication journal.
    #[cfg(all(
        feature = "local",
        feature = "native-mount",
        not(target_arch = "wasm32")
    ))]
    fn materialization_store(&self) -> &crate::LocalCoreStateStore;
}

/// Exact request identity presented to Harness's durable interaction authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootWritebackApprovalContext {
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Digest of every mutable publication input.
    pub request_digest: crate::Digest,
}

/// Harness-owned authority boundary for host writeback.
///
/// Implementations must resolve an authenticated owner interaction and verify
/// its exact operation and digest.  A missing verifier is a denial; the
/// filesystem has no local persistence fallback and cannot self-approve.
pub trait RootWritebackApprovalVerifier: Send + Sync {
    /// Verify one exact request before durable publication admission.
    fn verify<'a>(
        &'a self,
        context: RootWritebackApprovalContext,
    ) -> BoxFuture<'a, Result<(), String>>;
}

/// Root writeback authority errors.
#[derive(Debug, Error)]
pub enum RootWritebackJournalError<E: std::error::Error + 'static> {
    /// Durable storage failed.
    #[error("root writeback journal failed: {0}")]
    Store(E),
    /// A different request already owns the operation identity.
    #[error("root writeback operation identity is already bound to another request")]
    Conflict,
    /// The journal lost its initial CAS race.
    #[error("root writeback journal remained contended")]
    Contended,
    /// Persisted request bytes are invalid.
    #[error("root writeback request is invalid: {0}")]
    InvalidRequest(HostCheckoutRootWritebackError),
}

/// Errors raised before or during an approved root writeback.
#[derive(Debug, Error)]
pub enum HostCheckoutRootWritebackError {
    /// The Harness/operator authority did not authenticate this request.
    #[error("root writeback approval denied: {0}")]
    ApprovalDenied(String),
    /// Persisted request digest no longer matches its fields.
    #[error("root writeback request digest is invalid")]
    CorruptRequest,
    /// Generations or staging path differ from the approved request.
    #[error("root writeback request does not match the publication")]
    RequestMismatch,
    /// A previously-conflicted operation may not be retried silently.
    #[error("root writeback is conflicted and requires explicit recovery")]
    Conflict,
    /// Durable authority admission failed.
    #[error("root writeback journal failed: {0}")]
    Journal(String),
    /// The attached checkout was replaced after the request was approved.
    #[error("attached checkout root identity changed after approval")]
    RootIdentityMismatch,
    /// Native journaled publication failed.
    #[cfg(all(
        feature = "local",
        feature = "native-mount",
        not(target_arch = "wasm32")
    ))]
    #[error(transparent)]
    Native(#[from] NativeWorkspacePublicationError),
    /// Cancellation was observed before physical dispatch.
    #[error("root writeback was cancelled before dispatch")]
    Cancelled,
}

impl HostRestoreRequest {
    /// Stable retry identity retained by the durable approval record.
    #[must_use]
    pub const fn operation(&self) -> IdempotencyKey {
        self.operation
    }

    /// Source generation used as the conditional host baseline.
    #[must_use]
    pub const fn source_generation(&self) -> GenerationId {
        self.source_generation
    }

    /// Immutable generation selected for publication.
    #[must_use]
    pub const fn target_generation(&self) -> GenerationId {
        self.target_generation
    }

    /// Provider-owned destination root path captured with the request.
    #[must_use]
    pub fn source_root(&self) -> &Path {
        &self.source_root
    }

    /// Stable root identity captured with the request.
    #[must_use]
    pub const fn root_identity(&self) -> crate::NativeRootIdentity {
        self.root_identity
    }

    /// Exact sorted, non-overlapping paths admitted by this request.
    #[must_use]
    pub fn paths(&self) -> &[PathBuf] {
        &self.paths
    }

    /// Replacement mode fixed by the request fingerprint.
    #[must_use]
    pub const fn replacement(&self) -> HostPathReplacement {
        self.replacement
    }

    /// Materialization bounds and destination fixed by the request.
    #[must_use]
    pub const fn options(&self) -> &MaterializeOptions {
        &self.options
    }

    /// Fingerprint of every operation input, including paths and bounds.
    #[must_use]
    pub const fn fingerprint(&self) -> crate::Digest {
        self.fingerprint
    }
}

/// Native checkout handle retaining its provider-owned source binding.
///
/// `HostCheckout` is the narrow host boundary for local writeback. The
/// attached source remains the authority for root identity and current source
/// generation, and the held root capability remains inside this handle.
/// Durable approval records should retain the expected generation and call
/// [`Self::revalidate_with_key`] after host changes to acknowledge the source
/// watcher interval.
pub struct HostCheckout<A, O> {
    workspace: Workspace<A, O>,
    source: Source<A, O>,
    root: std::sync::Arc<HostRoot>,
}

impl<A, O> Clone for HostCheckout<A, O> {
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            source: self.source.clone(),
            root: std::sync::Arc::clone(&self.root),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> HostCheckout<A, O> {
    /// Attaches one exact host directory and returns its authenticated bridge.
    ///
    /// The initial baseline is captured by [`Fs::attach_directory`]. The
    /// source path and root identity are retained in the returned binding;
    /// this function never treats a provider-internal volume reference as a
    /// host path.
    pub async fn attach(
        fs: &Fs<A, O>,
        name: impl AsRef<str>,
        path: impl AsRef<Path>,
        options: SourceOptions,
    ) -> Result<Self, HostCheckoutError> {
        let workspace = fs.attach_directory(name, path, options).await?;
        Self::from_workspace(workspace).await
    }

    /// Wraps a workspace returned by [`Fs::attach_directory`].
    pub async fn from_workspace(workspace: Workspace<A, O>) -> Result<Self, HostCheckoutError> {
        let source = workspace
            .source()
            .cloned()
            .ok_or(HostCheckoutError::NoSource)?;
        let binding = source.binding().await;
        let root = HostRoot::open(&binding.source_root)?;
        if root.identity() != binding.root_identity {
            return Err(HostCheckoutError::Source(SourceError::BindingMismatch));
        }
        Ok(Self {
            workspace,
            source,
            root: std::sync::Arc::new(root),
        })
    }

    /// The provider-owned workspace handle backing this bridge.
    #[must_use]
    pub fn workspace(&self) -> &Workspace<A, O> {
        &self.workspace
    }

    /// The attached native source handle.
    #[must_use]
    pub fn source(&self) -> &Source<A, O> {
        &self.source
    }

    /// Reads the durable source binding without changing source state.
    pub async fn binding(&self) -> SourceBinding {
        self.source.binding().await
    }

    /// Revalidates the attached root and captures one bounded source interval.
    ///
    /// Tracking sources use their watcher interval. Pinned sources use a full
    /// rescan. A `NeedsRescan` or conflict result remains an error so callers
    /// cannot accidentally approve against an incomplete baseline.
    pub async fn revalidate_with_key(
        &self,
        key: IdempotencyKey,
    ) -> Result<SourceBinding, HostCheckoutError> {
        let before = self.source.binding().await;
        self.source.verify_root(&before.source_root).await?;
        let outcome = match self.source.reconcile_with_key(key).await {
            Err(SourceError::Pinned) => self.source.rescan_with_key(key).await?,
            result => result?,
        };
        match outcome {
            crate::ReconcileOutcome::Clean(_) => Ok(self.source.binding().await),
            crate::ReconcileOutcome::NeedsRescan(reason) => Err(HostCheckoutError::Source(
                SourceError::Engine(format!("source requires rescan: {reason:?}")),
            )),
            crate::ReconcileOutcome::Conflict => {
                Err(HostCheckoutError::Source(SourceError::Conflict))
            }
        }
    }

    /// Verifies the approval precondition and the destination root immediately
    /// before a host mutation.
    pub async fn prepare_publish(
        &self,
        expected_generation: GenerationId,
    ) -> Result<SourceBinding, HostCheckoutError> {
        let expected = self.source.binding().await;
        let actual = self.source.binding().await;
        if actual.generation_id != expected_generation {
            return Err(HostCheckoutError::StaleSource {
                expected: expected_generation,
                actual: actual.generation_id,
            });
        }
        if self.root.identity() != expected.root_identity {
            return Err(HostCheckoutError::Source(SourceError::BindingMismatch));
        }
        self.source.verify_root(&expected.source_root).await?;
        match self.source.state().await {
            SourceState::Clean | SourceState::Sealed => Ok(expected),
            state => Err(HostCheckoutError::Source(SourceError::Engine(format!(
                "source is not publishable in state {state:?}"
            )))),
        }
    }

    /// Captures an immutable, provider-bound restore target. The returned
    /// request is the only input Harness should persist for replay; its root
    /// identity cannot be supplied or substituted by the caller.
    pub async fn new_restore_request(
        &self,
        operation: IdempotencyKey,
        target_generation: GenerationId,
        paths: Vec<PathBuf>,
        replacement: HostPathReplacement,
        options: MaterializeOptions,
    ) -> Result<HostRestoreRequest, HostCheckoutError> {
        let binding = self
            .prepare_publish(self.source.binding().await.generation_id)
            .await?;
        validate_restore_paths(&paths)?;
        if options.destination != binding.source_root {
            return Err(HostCheckoutError::DestinationMismatch);
        }
        let fingerprint = restore_request_fingerprint(
            operation,
            binding.generation_id,
            target_generation,
            &binding.source_root,
            &paths,
            replacement,
            &options,
        );
        Ok(HostRestoreRequest {
            operation,
            source_generation: binding.generation_id,
            target_generation,
            source_root: binding.source_root,
            root_identity: binding.root_identity,
            paths,
            replacement,
            options,
            fingerprint,
        })
    }

    /// Replays one exact typed restore target. A mismatched operation,
    /// destination, generation, root identity, or fingerprint fails before
    /// any host mutation; durable journal/recovery state remains Harness's
    /// responsibility.
    pub async fn restore_request(
        &self,
        generation: &Generation<A, O>,
        request: &HostRestoreRequest,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<HostCheckoutRestore, HostCheckoutError> {
        let binding = self.prepare_publish(request.source_generation).await?;
        if request.target_generation != generation.id()
            || request.source_root != binding.source_root
            || request.root_identity != binding.root_identity
            || request.options.destination != binding.source_root
            || request.options.destination != self.root_binding_path().await?
            || restore_request_fingerprint(
                request.operation,
                request.source_generation,
                request.target_generation,
                &request.source_root,
                &request.paths,
                request.replacement,
                &request.options,
            ) != request.fingerprint
        {
            return Err(HostCheckoutError::Source(SourceError::BindingMismatch));
        }
        self.restore_paths(
            generation,
            request.source_generation,
            &request.paths,
            request.replacement,
            &request.options,
            budget,
            cancellation,
        )
        .await
    }

    /// Materializes an authenticated generation into a caller-owned empty
    /// staging directory. The attached source is not touched.
    pub async fn materialize_to_staging(
        &self,
        generation: &Generation<A, O>,
        options: &MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<MaterializationReceipt>, HostCheckoutError> {
        Ok(generation
            .materialize(options, budget, cancellation)
            .await?)
    }

    /// Applies selected authenticated paths to the attached checkout after an
    /// exact source-generation and root-identity check.
    ///
    /// This is intentionally path-scoped. Callers must compute the changed
    /// frontier from authenticated generations and must revalidate the source
    /// after the operation; concurrent host edits are therefore surfaced by
    /// the source watcher rather than silently merged by this adapter.
    pub async fn restore_paths(
        &self,
        generation: &Generation<A, O>,
        expected_generation: GenerationId,
        paths: &[PathBuf],
        replacement: HostPathReplacement,
        options: &MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<HostCheckoutRestore, HostCheckoutError> {
        let binding = self.prepare_publish(expected_generation).await?;
        if options.destination != binding.source_root {
            return Err(HostCheckoutError::DestinationMismatch);
        }
        validate_restore_paths(paths)?;
        if paths.is_empty() {
            return Ok(HostCheckoutRestore {
                outcomes: Vec::new(),
                work: WorkCounters::default(),
            });
        }
        let mut work = WorkCounters::default();
        let mut outcomes = Vec::with_capacity(paths.len());
        for path in paths {
            let remaining = budget.remaining(work)?;
            let (already_present, check_work) = self
                .source
                .host_paths_match_generation(
                    generation.id(),
                    std::slice::from_ref(path),
                    1,
                    options.maximum_extent_spans,
                    remaining,
                    cancellation,
                )
                .await?;
            work = work.checked_add(check_work)?;
            if already_present {
                outcomes.push(HostPathRestore::Restored);
                continue;
            }
            let remaining = budget.remaining(work)?;
            let check_work = self
                .source
                .verify_paths(
                    expected_generation,
                    std::slice::from_ref(path),
                    1,
                    options.maximum_extent_spans,
                    remaining,
                    cancellation,
                )
                .await?;
            work = work.checked_add(check_work)?;
            let remaining = budget.remaining(work)?;
            let verify_source = self.source.clone();
            let verify_path = path.clone();
            let verify_cancellation = cancellation.clone();
            let receipt = generation
                .restore_host_path_with_root_and_precondition(
                    path,
                    replacement,
                    options,
                    std::sync::Arc::clone(&self.root),
                    remaining,
                    cancellation,
                    move |check_budget| async move {
                        verify_source
                            .verify_paths(
                                expected_generation,
                                std::slice::from_ref(&verify_path),
                                1,
                                options.maximum_extent_spans,
                                check_budget,
                                &verify_cancellation,
                            )
                            .await
                            .map_err(|error| MaterializeError::Engine(error.to_string()))
                    },
                )
                .await?;
            work = work.checked_add(receipt.work)?;
            outcomes.push(receipt.value);
        }
        self.source.verify_root(&binding.source_root).await?;
        Ok(HostCheckoutRestore { outcomes, work })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> HostCheckout<A, O> {
    async fn root_binding_path(&self) -> Result<PathBuf, HostCheckoutError> {
        Ok(self.source.binding().await.source_root)
    }
}

fn validate_restore_paths(paths: &[PathBuf]) -> Result<(), HostCheckoutError> {
    if paths
        .windows(2)
        .any(|pair| pair[0] >= pair[1] || pair[1].starts_with(&pair[0]))
    {
        return Err(HostCheckoutError::InvalidPaths);
    }
    Ok(())
}

fn restore_request_fingerprint(
    operation: IdempotencyKey,
    source_generation: GenerationId,
    target_generation: GenerationId,
    source_root: &Path,
    paths: &[PathBuf],
    replacement: HostPathReplacement,
    options: &MaterializeOptions,
) -> crate::Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-host-restore-v2\0");
    hasher.update(&operation.into_bytes());
    hasher.update(source_generation.digest().as_bytes());
    hasher.update(target_generation.digest().as_bytes());
    hasher.update(source_root.to_string_lossy().as_bytes());
    hasher.update(&[replacement as u8]);
    hasher.update(options.destination.to_string_lossy().as_bytes());
    hasher.update(&options.maximum_directory_entries.to_le_bytes());
    hasher.update(&options.maximum_extent_spans.to_le_bytes());
    hasher.update(&options.transfer_bytes.to_le_bytes());
    for path in paths {
        let bytes = path.to_string_lossy();
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes.as_bytes());
    }
    crate::Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn root_writeback_request_fingerprint(
    operation_id: OperationId,
    source_generation: GenerationId,
    target_generation: GenerationId,
    root: &Path,
    operation_directory: &Path,
    root_identity: [u8; 16],
    options_digest: Option<crate::Digest>,
    excluded_names_digest: Option<crate::Digest>,
) -> crate::Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-host-root-writeback-v1\0");
    hasher.update(&operation_id.into_bytes());
    hasher.update(source_generation.digest().as_bytes());
    hasher.update(target_generation.digest().as_bytes());
    hash_path(&mut hasher, root);
    hash_path(&mut hasher, operation_directory);
    hasher.update(&root_identity);
    hasher.update(&[u8::from(options_digest.is_some())]);
    if let Some(digest) = options_digest {
        hasher.update(digest.as_bytes());
    }
    hasher.update(&[u8::from(excluded_names_digest.is_some())]);
    if let Some(digest) = excluded_names_digest {
        hasher.update(digest.as_bytes());
    }
    crate::Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn materialize_options_digest(options: &MaterializeOptions) -> crate::Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-materialize-options-v1\0");
    hash_path(&mut hasher, &options.destination);
    hasher.update(&options.maximum_directory_entries.to_le_bytes());
    hasher.update(&options.maximum_extent_spans.to_le_bytes());
    hasher.update(&options.transfer_bytes.to_le_bytes());
    crate::Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn excluded_names_digest(excluded_names: &[&str]) -> crate::Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-materialize-excluded-v1\0");
    for name in excluded_names {
        hasher.update(&(name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
    }
    crate::Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn hash_path(hasher: &mut blake3::Hasher, path: &Path) {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let words = path.as_os_str().encode_wide().collect::<Vec<_>>();
        hasher.update(&(words.len() as u64).to_le_bytes());
        for word in words {
            hasher.update(&word.to_le_bytes());
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    #[cfg(not(any(windows, unix)))]
    {
        let text = path.to_string_lossy();
        hasher.update(&(text.len() as u64).to_le_bytes());
        hasher.update(text.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Fs, LocalCoreStateStore, SourceMode};
    use tempfile::tempdir;

    #[tokio::test]
    async fn attached_binding_is_provider_owned_and_root_fenced()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        std::fs::write(root.path().join("tracked.txt"), b"before")?;
        let fs = Fs::memory();
        let checkout = HostCheckout::attach(
            &fs,
            "host-checkout",
            root.path(),
            SourceOptions {
                mode: SourceMode::Pinned,
                ..SourceOptions::default()
            },
        )
        .await?;
        let binding = checkout.binding().await;
        assert_eq!(binding.workspace_id, checkout.workspace().id());
        assert_eq!(binding.source_root, root.path());
        checkout.source().verify_root(root.path()).await?;
        let other = tempdir()?;
        assert!(matches!(
            checkout.source().verify_root(other.path()).await,
            Err(SourceError::BindingMismatch)
        ));
        Ok(())
    }

    #[cfg(all(
        feature = "local",
        feature = "native-mount",
        not(target_arch = "wasm32")
    ))]
    #[test]
    fn approved_writeback_rejects_replaced_root_identity() -> Result<(), Box<dyn std::error::Error>>
    {
        let parent = tempdir()?;
        let root = parent.path().join("checkout");
        let staging = parent.path().join("staging");
        std::fs::create_dir(&root)?;
        std::fs::create_dir(&staging)?;
        let options = MaterializeOptions {
            destination: staging.join("target"),
            maximum_directory_entries: 32,
            maximum_extent_spans: 32,
            transfer_bytes: 1024,
        };
        let request = HostCheckoutRootWritebackRequest::new_with_options(
            OperationId::from_bytes([0x11; 16]),
            GenerationId::new(crate::Digest::from_bytes([0x12; 32])),
            GenerationId::new(crate::Digest::from_bytes([0x13; 32])),
            &root,
            &staging,
            &options,
            &[".git"],
        )?;
        struct Allow;
        impl RootWritebackApprovalVerifier for Allow {
            fn verify<'a>(
                &'a self,
                _context: RootWritebackApprovalContext,
            ) -> BoxFuture<'a, Result<(), String>> {
                Box::pin(async { Ok(()) })
            }
        }
        let _intent = futures::executor::block_on(request.authorize_with(&Allow))?;
        let moved = parent.path().join("old-checkout");
        std::fs::rename(&root, &moved)?;
        std::fs::create_dir(&root)?;
        assert_ne!(
            HostRoot::open(&root)?.identity().to_bytes(),
            request.root_identity()
        );
        Ok(())
    }

    #[tokio::test]
    async fn denied_writeback_never_creates_an_intent_or_journal()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempdir()?;
        let root = parent.path().join("checkout");
        let staging = parent.path().join("staging");
        std::fs::create_dir(&root)?;
        std::fs::create_dir(&staging)?;
        let options = MaterializeOptions::native(staging.join("target"));
        let request = HostCheckoutRootWritebackRequest::new_with_options(
            OperationId::from_bytes([0x21; 16]),
            GenerationId::new(crate::Digest::from_bytes([0x22; 32])),
            GenerationId::new(crate::Digest::from_bytes([0x23; 32])),
            &root,
            &staging,
            &options,
            &[],
        )?;
        struct Deny;
        impl RootWritebackApprovalVerifier for Deny {
            fn verify<'a>(
                &'a self,
                _context: RootWritebackApprovalContext,
            ) -> BoxFuture<'a, Result<(), String>> {
                Box::pin(async { Err("denied".to_owned()) })
            }
        }
        assert!(matches!(
            request.authorize_with(&Deny).await,
            Err(HostCheckoutRootWritebackError::ApprovalDenied(_))
        ));
        assert!(!staging.join("target").exists());
        Ok(())
    }

    #[cfg(all(
        feature = "local",
        feature = "native-mount",
        not(target_arch = "wasm32")
    ))]
    #[tokio::test]
    async fn recovery_requires_authority_and_never_replays_publishing()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = tempdir()?;
        let root = parent.path().join("checkout");
        let staging = parent.path().join("staging");
        std::fs::create_dir(&root)?;
        std::fs::create_dir(&staging)?;
        let options = MaterializeOptions::native(staging.join("target"));
        let request = HostCheckoutRootWritebackRequest::new_with_options(
            OperationId::from_bytes([0x31; 16]),
            GenerationId::new(crate::Digest::from_bytes([0x32; 32])),
            GenerationId::new(crate::Digest::from_bytes([0x33; 32])),
            &root,
            &staging,
            &options,
            &[],
        )?;
        struct Allow;
        impl RootWritebackApprovalVerifier for Allow {
            fn verify<'a>(
                &'a self,
                _context: RootWritebackApprovalContext,
            ) -> BoxFuture<'a, Result<(), String>> {
                Box::pin(async { Ok(()) })
            }
        }
        struct Deny;
        impl RootWritebackApprovalVerifier for Deny {
            fn verify<'a>(
                &'a self,
                _context: RootWritebackApprovalContext,
            ) -> BoxFuture<'a, Result<(), String>> {
                Box::pin(async { Err("recovery denied".to_owned()) })
            }
        }
        let intent = request.authorize_with(&Allow).await?;
        let store = LocalCoreStateStore::open_owned(parent.path().join("state"))?;
        let admitted = intent.admit(&store).await?;
        let publishing = RootWritebackJournal {
            version: admitted.version,
            revision: admitted.revision + 1,
            intent: intent.clone(),
            phase: RootWritebackPhase::Publishing,
            outcome: RootWritebackOutcome::Unknown,
        };
        assert!(
            store
                .compare_and_swap(intent.operation_id(), admitted.revision, publishing)
                .await?
        );
        assert!(matches!(
            intent
                .recover(&store, &Deny, RootWritebackRecovery::Failed)
                .await,
            Err(HostCheckoutRootWritebackError::ApprovalDenied(_))
        ));
        assert!(matches!(
            intent
                .recover(&store, &Allow, RootWritebackRecovery::Conflicted)
                .await?
                .phase,
            RootWritebackPhase::Conflicted
        ));
        assert!(!root.join("published.txt").exists());
        Ok(())
    }
}

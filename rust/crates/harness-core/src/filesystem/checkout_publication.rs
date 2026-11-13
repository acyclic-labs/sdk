//! Bound native checkout publication, including original allocation custody.
use crate::{Error, Result};
use acyclic_fs::{
    Generation, LocalAuthorityBackend, LocalCoreStateStore, LocalObjectBackend,
    MaterializationJournal, MaterializeOptions, NativeAllocation, NativeMaterializationWork,
    NativeRootIdentity, NativeWorkspacePublication, OperationId,
};
use std::{
    future::Future,
    path::{Path, PathBuf},
};

type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;
mod approval;
pub use approval::{AuthorizedCheckout, CheckoutApproval};

/// One native checkout and its same-filesystem operation staging directory.
pub struct CheckoutPublication {
    root: PathBuf,
    directory: PathBuf,
    identity: NativeRootIdentity,
    operation: OperationId,
    _custody: NativeAllocation,
}
impl CheckoutPublication {
    /// Admits the host state layout before a caller changes its logical workspace.
    /// Uses the same native topology proof as publication without creating state.
    pub fn validate_state(
        root: &Path,
        identity: NativeRootIdentity,
        state: &LocalCoreStateStore,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let actual = acyclic_fs::materializer::validate_native_preparation_location(
            root,
            state.root(),
            work,
        )?;
        if actual != identity {
            return Err(
                acyclic_fs::NativeTreeMaterializationError::AllocationChanged {
                    expected: identity,
                    actual,
                }
                .into(),
            );
        }
        Ok(())
    }

    /// Retains paths on the caller's original native ledger.
    pub fn new(
        root: &Path,
        identity: NativeRootIdentity,
        operation: OperationId,
        state: &LocalCoreStateStore,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if !root.is_absolute() || !state.root().is_absolute() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native checkout and host state roots must be absolute",
            });
        }
        let mut root = work.path(root, None)?;
        let mut directory = state.native_preparation_directory(identity, operation, work)?;
        root.1.absorb(&mut directory.1)?;
        Ok(Self {
            root: root.0,
            directory: directory.0,
            identity,
            operation,
            _custody: root.1,
        })
    }
    /// Physical checkout bound into publication and approval.
    pub fn root(&self) -> &Path {
        &self.root
    }
    /// Operation staging directory bound into publication and approval.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    fn request<'a>(
        &'a self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        options: &'a MaterializeOptions,
        work: &'a NativeMaterializationWork,
    ) -> NativeWorkspacePublication<'a> {
        NativeWorkspacePublication {
            root: &self.root,
            source_root_identity: self.identity,
            operation_directory: &self.directory,
            operation_id: self.operation,
            from: from.id(),
            to: to.id(),
            excluded_names: &[],
            options,
            budget: work.budget(),
            cancellation: work.cancellation(),
        }
    }
    /// Prepares a native generation transition without publishing it.
    pub async fn prepare(
        &self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        work: &NativeMaterializationWork,
    ) -> Result<MaterializationJournal> {
        let prepared = acyclic_fs::materializer::prepare_native_generation_transition(
            from,
            to,
            state,
            self.request(from, to, options, work),
            work.clone(),
        )
        .await;
        match prepared {
            Ok(receipt) => Ok(receipt.value),
            Err((source, _)) => Err(source.into()),
        }
    }

    /// Resumes this operation's retained transition, independently of the
    /// workspace heads observed by a later delivery. The journal owns the
    /// original generation pair; callers must not reconstruct it from heads.
    pub(crate) async fn resume<F, Fut>(
        &self,
        source: &LocalGeneration,
        target: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        work: &NativeMaterializationWork,
        mut guard: F,
    ) -> Result<Option<MaterializationJournal>>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        guard().await?;
        let retained =
            acyclic_fs::load_native_materialization_journal(state, self.operation, work.clone())
                .await
                .map_err(|(cause, _)| Error::from(cause))?
                .value;
        let Some(journal) = retained else {
            return Ok(None);
        };
        let acyclic_fs::MaterializationIntent::GenerationTransition { from, to } =
            journal.plan.intent
        else {
            return Err(Error::Storage(
                "checkout publication is not a generation transition".into(),
            ));
        };
        if journal.root_identity != Some(self.identity.to_bytes()) {
            return Err(Error::Storage(
                "checkout publication belongs to another root".into(),
            ));
        }
        let from = source
            .workspace()
            .generation(from, work, work.cancellation())
            .await?;
        let to = target.generation(to, work, work.cancellation()).await?;
        self.publish(&from, &to, state, options, work, guard)
            .await
            .map(Some)
    }
    /// Publishes with the caller's guard and original native work ledger.
    pub(crate) async fn publish<F, Fut>(
        &self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        preparation: (&NativeMaterializationWork, Option<&MaterializationJournal>),
        mut guard: F,
    ) -> Result<MaterializationJournal>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        let (work, approved) = preparation;
        guard().await?;
        let actual = work.root_identity(&self.root)?;
        if actual != self.identity {
            return Err(
                acyclic_fs::NativeTreeMaterializationError::AllocationChanged {
                    expected: self.identity,
                    actual,
                }
                .into(),
            );
        }
        let retained =
            acyclic_fs::load_native_materialization_journal(state, self.operation, work.clone())
                .await;
        let journal = match retained {
            Ok(receipt) => receipt.value,
            Err((cause, _)) => return Err(cause.into()),
        };
        let journal = match (journal, approved) {
            (Some(journal), Some(approved)) => {
                if journal.plan() != approved.plan()
                    || journal.root_identity != approved.root_identity
                    || journal.preparation_identity != approved.preparation_identity
                    || journal.preimages() != approved.preimages()
                {
                    return Err(Error::Unauthorized(
                        "checkout differs from its approved preparation".into(),
                    ));
                }
                journal
            }
            (Some(journal), None) => journal,
            (None, Some(approved))
                if approved.plan().edits.is_empty()
                    && completed(approved, self.identity, from.id(), to.id())? =>
            {
                // This approval authorizes no physical effects. Host changes
                // after preparation cannot expand it into a different plan.
                let mut copy = approved.copy_empty().ok_or_else(|| {
                    Error::Unauthorized("checkout approval contains preimages".into())
                })?;
                copy.custody = Some(work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?);
                copy
            }
            (None, Some(_)) => {
                return Err(Error::Unauthorized(
                    "checkout lost its approved preparation".into(),
                ));
            }
            (None, None) => self.prepare(from, to, state, options, work).await?,
        };
        // A logical commit can precede its native acknowledgement. Every host
        // resumes the original transition, even when its current head is `to`.
        let original = if from.id() == to.id()
            && let acyclic_fs::MaterializationIntent::GenerationTransition {
                from: original,
                to: target,
            } = journal.plan().intent
            && target == to.id()
        {
            Some(
                from.workspace()
                    .generation(original, work, work.cancellation())
                    .await?,
            )
        } else {
            None
        };
        let from = original.as_ref().unwrap_or(from);
        if completed(&journal, self.identity, from.id(), to.id())? {
            // Completion belongs to the journal, so post-ack host edits cannot
            // be overwritten by reconstructing and replaying another plan.
            cleanup_directory(&self.directory)?;
            return Ok(journal);
        }
        let guard_cause = std::sync::Mutex::new(None);
        let result = acyclic_fs::publish_native_generation_transition(
            from,
            to,
            state,
            self.request(from, to, options, work),
            work.clone(),
            || {
                let future = guard();
                let guard_cause = &guard_cause;
                async move {
                    future.await.map_err(|cause| {
                        *guard_cause
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cause);
                        acyclic_fs::NativeWorkspacePublicationError::Native(
                            acyclic_fs::NativeTreeMaterializationError::Io(
                                std::io::ErrorKind::PermissionDenied.into(),
                            ),
                        )
                    })
                }
            },
        )
        .await;
        let journal = result.map(|receipt| receipt.value).map_err(|(source, _)| {
            guard_cause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| Error::from(source))
        })?;
        cleanup_directory(&self.directory)?;
        Ok(journal)
    }

    /// Ends replay custody after the enclosing publication is acknowledged.
    pub(crate) async fn finish(
        state: &LocalCoreStateStore,
        identity: NativeRootIdentity,
        operation: OperationId,
    ) -> Result<()> {
        // Remove staging before its replay evidence: a failed cleanup must
        // leave the completed marker available to protect later host edits.
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (directory, _custody) =
            state.native_preparation_directory(identity, operation, &work)?;
        cleanup_directory(&directory)?;
        state.remove_materialization_async(operation).await?;
        Ok(())
    }
}

fn completed(
    journal: &MaterializationJournal,
    root: NativeRootIdentity,
    from: acyclic_fs::GenerationId,
    to: acyclic_fs::GenerationId,
) -> Result<bool> {
    if journal.root_identity != Some(root.to_bytes())
        || journal.plan().intent
            != (acyclic_fs::MaterializationIntent::GenerationTransition { from, to })
    {
        return Err(Error::Storage(
            "checkout publication differs from its original generation transition".into(),
        ));
    }
    Ok(journal.phase == acyclic_fs::MaterializationPhase::Applied)
}

fn cleanup_directory(directory: &Path) -> Result<()> {
    let parent = directory
        .parent()
        .ok_or_else(|| Error::Invalid("native staging directory has no parent".into()))?;
    // Journals retain shared parent identities before creating their operation
    // directory. Even an empty parent can therefore belong to a preparation.
    // Only this operation's tree can be retired without invalidating a journal.
    match acyclic_native_runtime::remove_child_directory(parent, directory) {
        Err(cause) if cause.kind() != std::io::ErrorKind::NotFound => {
            Err(acyclic_fs::NativeTreeMaterializationError::Io(cause).into())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod transition_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkout_paths_retain_the_original_ledger_on_success_and_refusal() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let identity = NativeRootIdentity::from_bytes([0; 16]);
        let root = std::env::temp_dir().join("checkout-parent/checkout");
        let state = LocalCoreStateStore::new(std::env::temp_dir().join("host-state/core"));
        let publication =
            CheckoutPublication::new(&root, identity, OperationId::new(), &state, &work)?;
        assert_eq!(publication.root(), root.as_path());
        assert!(work.live_allocation_bytes() > baseline);
        drop(publication);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let refused =
            CheckoutPublication::new(Path::new(""), identity, OperationId::new(), &state, &work);
        assert!(matches!(
            refused,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ));
        drop(refused);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}

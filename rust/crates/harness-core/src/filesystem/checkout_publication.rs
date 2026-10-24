//! Bound native checkout publication, including original allocation custody.
use super::NativeDirectory;
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
const EXCLUDED_NAMES: &[&str] = &[".git", ".system", ".volumes"];
mod approval;

/// One native checkout and its same-filesystem operation staging directory.
pub struct CheckoutPublication {
    root: PathBuf,
    directory: PathBuf,
    identity: NativeRootIdentity,
    operation: OperationId,
    _custody: NativeAllocation,
}
impl CheckoutPublication {
    /// Retains paths on the caller's original native ledger.
    pub fn new(
        root: &Path,
        identity: NativeRootIdentity,
        operation: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if !root.is_absolute() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native checkout root must be absolute",
            });
        }
        let mut root = work.path(root, None)?;
        let mut directory =
            NativeDirectory(&root.0).publication_directory_with_native_work(operation, work)?;
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
            excluded_names: EXCLUDED_NAMES,
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
        let mut request = self.request(from, to, options, work);
        request.budget = work.remaining()?;
        let prepared = acyclic_fs::materializer::prepare_native_generation_transition(
            from, to, state, request,
        )
        .await;
        match prepared {
            Ok(receipt) => {
                work.spent(receipt.work)?;
                validate_plan(&receipt.value.plan)?;
                Ok(receipt.value)
            }
            Err((source, spent)) => {
                work.spent(spent)?;
                Err(source.into())
            }
        }
    }
    /// Publishes with the caller's guard and original native work ledger.
    pub async fn publish<F, Fut>(
        &self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        work: &NativeMaterializationWork,
        mut guard: F,
    ) -> Result<MaterializationJournal>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
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
        let retained = acyclic_fs::load_native_materialization_journal(
            state,
            self.operation,
            work.remaining()?,
            work.cancellation(),
        )
        .await;
        let journal = match retained {
            Ok(receipt) => {
                work.spent(receipt.work)?;
                receipt.value
            }
            Err((cause, spent)) => {
                work.spent(spent)?;
                return Err(cause.into());
            }
        };
        if let Some(journal) = &journal {
            validate_plan(&journal.plan)?;
        }
        // A logical commit can precede its native acknowledgement. Every host
        // resumes the original transition, even when its current head is `to`.
        let original = if let Some(journal) = &journal
            && from.id() == to.id()
            && let acyclic_fs::MaterializationIntent::GenerationTransition {
                from: original,
                to: target,
            } = journal.plan.intent
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
        if let Some(journal) = journal
            && completed(&journal, self.identity, from.id(), to.id())?
        {
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
        root: &Path,
        operation: OperationId,
    ) -> Result<()> {
        // Remove staging before its replay evidence: a failed cleanup must
        // leave the completed marker available to protect later host edits.
        let directory = NativeDirectory(root).publication_directory(operation)?;
        cleanup_directory(&directory)?;
        state.remove_materialization_async(operation).await?;
        Ok(())
    }
}

fn validate_plan(plan: &acyclic_fs::MaterializationPlan) -> Result<()> {
    use acyclic_fs::MaterializationEdit;
    let allowed = |path: &str| !EXCLUDED_NAMES.contains(&path.split('/').next().unwrap_or(""));
    for edit in &plan.edits {
        let valid = match edit {
            MaterializationEdit::Install { path, .. }
            | MaterializationEdit::Remove { path }
            | MaterializationEdit::SetMetadata { path, .. } => allowed(path),
            MaterializationEdit::Rename { from, to } => allowed(from) && allowed(to),
            _ => false,
        };
        if !valid {
            return Err(Error::Unauthorized(
                "checkout journal targets a reserved host path".into(),
            ));
        }
    }
    Ok(())
}

fn completed(
    journal: &MaterializationJournal,
    root: NativeRootIdentity,
    from: acyclic_fs::GenerationId,
    to: acyclic_fs::GenerationId,
) -> Result<bool> {
    if journal.root_identity != Some(root.to_bytes())
        || journal.plan.intent
            != (acyclic_fs::MaterializationIntent::GenerationTransition { from, to })
    {
        return Err(Error::Storage(
            "checkout publication differs from its original generation transition".into(),
        ));
    }
    Ok(journal.phase == acyclic_fs::MaterializationPhase::Applied)
}

fn cleanup_directory(directory: &Path) -> Result<()> {
    if !directory
        .try_exists()
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))?
    {
        return Ok(());
    }
    let parent = directory
        .parent()
        .ok_or_else(|| Error::Invalid("native staging directory has no parent".into()))?;
    acyclic_native_runtime::remove_child_directory(parent, directory)
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))
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
        let publication = CheckoutPublication::new(&root, identity, OperationId::new(), &work)?;
        assert_eq!(publication.root(), root.as_path());
        assert!(
            publication
                .directory()
                .starts_with(std::env::temp_dir().join("checkout-parent/.acyclic-m"))
        );
        assert!(work.live_allocation_bytes() > baseline);
        drop(publication);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let refused = CheckoutPublication::new(Path::new(""), identity, OperationId::new(), &work);
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

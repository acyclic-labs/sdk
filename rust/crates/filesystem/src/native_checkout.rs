//! Authenticated bridge between a provider-owned generation and one attached
//! native checkout.
//!
//! This module deliberately composes [`Source`] and the native materializer.
//! It does not copy a checkout into an adapter-owned store and it does not
//! infer an operating-system path from a [`VolumeRef`]. The source binding and
//! generation precondition must be retained by the caller and checked again
//! immediately before an approved host mutation.

use crate::native_mount::{
    HostPathExpectation, HostPathReplacement, HostPathRestore, MaterializationReceipt,
    MaterializeOptions,
};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, Generation, GenerationId,
    IdempotencyKey, OperationReceipt, Source, SourceBinding, SourceError, SourceOptions,
    SourceState, WorkBudget, WorkCounters, Workspace, WorkspaceError,
};
use std::path::{Path, PathBuf};
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
    /// The generation being restored belongs to another provider workspace.
    #[error("restore generation belongs to another workspace")]
    GenerationMismatch,
    /// The materialization destination was not the exact attached checkout.
    #[error("materialization destination is not the attached checkout root")]
    DestinationMismatch,
    /// A host entry changed at the conditional publication boundary.
    #[error("host path changed during conditional restore")]
    ConcurrentHostEdit,
    /// A host publication crossed its mutation boundary without a durable
    /// receipt; retained artifacts must be reconciled before retry.
    #[error("host restore publication is uncertain")]
    UncertainPublication,
    /// Root writeback uses only the sealed atomic native policy.
    #[error("native root writeback policy is not the sealed atomic policy")]
    InvalidRestorePolicy,
}

/// Exact result of a bounded sequence of host-path replacements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCheckoutRestore {
    /// Per-path authenticated replacement results, in request order.
    pub outcomes: Vec<HostPathRestore>,
    /// Additive work performed by all replacements.
    pub work: WorkCounters,
}

/// Native checkout handle retaining its provider-owned source binding.
///
/// `HostCheckout` is the narrow host boundary for local writeback. The
/// attached source remains the authority for root identity and current source
/// generation. Callers should persist [`SourceBinding`] (or the containing
/// approval record) before invoking [`Self::restore_paths`], and should call
/// [`Self::revalidate_with_key`] after host changes to acknowledge the source
/// watcher interval.
pub struct HostCheckout<A, O> {
    workspace: Workspace<A, O>,
    source: Source<A, O>,
}

impl<A, O> Clone for HostCheckout<A, O> {
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            source: self.source.clone(),
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
        Ok(Self { workspace, source })
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
    pub async fn prepare_publish(&self, expected: &SourceBinding) -> Result<(), HostCheckoutError> {
        let actual = self.source.binding().await;
        if actual.workspace_id != expected.workspace_id
            || actual.root_identity != expected.root_identity
            || actual.source_root != expected.source_root
        {
            return Err(HostCheckoutError::Source(SourceError::BindingMismatch));
        }
        if actual.generation_id != expected.generation_id {
            return Err(HostCheckoutError::StaleSource {
                expected: expected.generation_id,
                actual: actual.generation_id,
            });
        }
        self.source.verify_root(&expected.source_root).await?;
        match self.source.state().await {
            SourceState::Clean | SourceState::Sealed => Ok(()),
            state => Err(HostCheckoutError::Source(SourceError::Engine(format!(
                "source is not publishable in state {state:?}"
            )))),
        }
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
        expected: &SourceBinding,
        paths: &[PathBuf],
        replacement: HostPathReplacement,
        options: &MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<HostCheckoutRestore, HostCheckoutError> {
        if generation.workspace_id() != expected.workspace_id {
            return Err(HostCheckoutError::GenerationMismatch);
        }
        if options.destination != expected.source_root {
            return Err(HostCheckoutError::DestinationMismatch);
        }
        self.prepare_publish(expected).await?;
        let mut work = WorkCounters::default();
        let mut outcomes = Vec::with_capacity(paths.len());
        for path in paths {
            let remaining = budget.remaining(work)?;
            let receipt = generation
                .restore_host_path(path, replacement, options, remaining, cancellation)
                .await?;
            work = work.checked_add(receipt.work)?;
            outcomes.push(receipt.value);
        }
        self.source.verify_root(&expected.source_root).await?;
        Ok(HostCheckoutRestore { outcomes, work })
    }

    /// Revalidates the attached source under the supplied retry identity
    /// before restoring paths. A binding captured before a concurrent host
    /// edit is rejected as stale and cannot authorize the write.
    pub async fn restore_paths_after_revalidation(
        &self,
        generation: &Generation<A, O>,
        expected: &SourceBinding,
        reconciliation_key: IdempotencyKey,
        paths: &[PathBuf],
        replacement: HostPathReplacement,
        options: &MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<HostCheckoutRestore, HostCheckoutError> {
        if replacement != HostPathReplacement::Atomic
            || *options != MaterializeOptions::native(expected.source_root.clone())
        {
            return Err(HostCheckoutError::InvalidRestorePolicy);
        }
        if generation.workspace_id() != expected.workspace_id {
            return Err(HostCheckoutError::GenerationMismatch);
        }
        let actual = self.revalidate_with_key(reconciliation_key).await?;
        if actual != *expected {
            // A source generation can advance for an unrelated user edit.
            // Compare only the approved paths before deciding whether that
            // edit conflicts; this keeps unrelated user work intact while
            // still fencing an overlapping edit before any host mutation.
            let baseline = self.workspace.generation(expected.generation_id).await?;
            let current = self.workspace.generation(actual.generation_id).await?;
            for path in paths {
                if Self::path_state(&baseline, path).await?
                    != Self::path_state(&current, path).await?
                {
                    return Err(HostCheckoutError::StaleSource {
                        expected: expected.generation_id,
                        actual: actual.generation_id,
                    });
                }
            }
        }
        let mut work = WorkCounters::default();
        let mut outcomes = Vec::with_capacity(paths.len());
        for path in paths {
            cancellation
                .check()
                .map_err(|error| HostCheckoutError::Workspace(WorkspaceError::from(error)))?;
            let before = self
                .revalidate_with_key(path_reconciliation_key(reconciliation_key, path, b"before"))
                .await?;
            if before.workspace_id != expected.workspace_id
                || before.root_identity != expected.root_identity
                || before.source_root != expected.source_root
            {
                return Err(HostCheckoutError::Source(SourceError::BindingMismatch));
            }
            let current = self.workspace.generation(before.generation_id).await?;
            let target_state = Self::path_state(generation, path).await?;
            let current_state = Self::path_state(&current, path).await?;
            let target_expectation = host_path_expectation(target_state.as_ref())?;
            let target_on_host = crate::native_mount::host_path_matches_expectation(
                &options.destination,
                path,
                &target_expectation,
            )
            .map_err(|error| HostCheckoutError::Workspace(WorkspaceError::engine(error)))?;
            if current_state == target_state && target_on_host {
                outcomes.push(match target_state {
                    Some(_) => HostPathRestore::Restored,
                    None => HostPathRestore::Removed,
                });
                continue;
            }
            if current_state == target_state {
                return Err(HostCheckoutError::ConcurrentHostEdit);
            }
            if target_on_host {
                let observed = self
                    .revalidate_with_key(path_reconciliation_key(
                        reconciliation_key,
                        path,
                        b"already-published",
                    ))
                    .await?;
                let observed_generation = self.workspace.generation(observed.generation_id).await?;
                if Self::path_state(&observed_generation, path).await? == target_state {
                    outcomes.push(match target_state {
                        Some(_) => HostPathRestore::Restored,
                        None => HostPathRestore::Removed,
                    });
                    continue;
                }
                return Err(HostCheckoutError::ConcurrentHostEdit);
            }
            let expectation = host_path_expectation(current_state.as_ref())?;
            self.prepare_publish(&before).await?;
            let receipt = generation
                .restore_host_path_if_unchanged_with_operation(
                    path,
                    replacement,
                    options,
                    Some(&expectation),
                    path_reconciliation_key(reconciliation_key, path, b"publish"),
                    budget.remaining(work)?,
                    cancellation,
                )
                .await
                .map_err(|error| {
                    if matches!(error, WorkspaceError::Engine(ref message)
                        if message.contains("host path changed during conditional restore"))
                    {
                        HostCheckoutError::ConcurrentHostEdit
                    } else if matches!(error, WorkspaceError::Engine(ref message)
                        if message.contains("conditional host restore is unsupported"))
                    {
                        HostCheckoutError::InvalidRestorePolicy
                    } else if matches!(error, WorkspaceError::Engine(ref message)
                        if message.contains("conditional host restore has an uncertain publication"))
                    {
                        HostCheckoutError::UncertainPublication
                    } else {
                        HostCheckoutError::Workspace(error)
                    }
                })?;
            work = work.checked_add(receipt.work)?;
            let after = self
                .revalidate_with_key(path_reconciliation_key(reconciliation_key, path, b"after"))
                .await?;
            let after_generation = self.workspace.generation(after.generation_id).await?;
            if Self::path_state(&after_generation, path).await? != target_state {
                return Err(HostCheckoutError::ConcurrentHostEdit);
            }
            outcomes.push(receipt.value);
        }
        self.source.verify_root(&expected.source_root).await?;
        Ok(HostCheckoutRestore { outcomes, work })
    }

    async fn path_state(
        generation: &Generation<A, O>,
        path: &Path,
    ) -> Result<Option<(crate::WorkspaceStat, Option<Vec<u8>>)>, HostCheckoutError> {
        let mut canonical = path.to_string_lossy().into_owned();
        #[cfg(windows)]
        {
            canonical = canonical.replace('\\', "/");
        }
        if !canonical.starts_with('/') {
            canonical.insert(0, '/');
        }
        let stat = match generation.stat(&canonical).await {
            Ok(stat) => stat,
            Err(WorkspaceError::NotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let bytes = if stat.kind == crate::kernel::FileKind::Regular {
            Some(generation.read(&canonical, u64::MAX).await?.to_vec())
        } else {
            None
        };
        Ok(Some((stat, bytes)))
    }
}

fn host_path_expectation(
    state: Option<&(crate::WorkspaceStat, Option<Vec<u8>>)>,
) -> Result<HostPathExpectation, HostCheckoutError> {
    match state {
        Some((stat, bytes)) if stat.kind == crate::kernel::FileKind::Regular => {
            Ok(HostPathExpectation::present(stat.kind, bytes.as_deref()))
        }
        Some(_) => Err(HostCheckoutError::InvalidRestorePolicy),
        None => Ok(HostPathExpectation::absent()),
    }
}

fn path_reconciliation_key(key: IdempotencyKey, path: &Path, phase: &[u8]) -> IdempotencyKey {
    let mut input = Vec::with_capacity(96);
    input.extend_from_slice(b"acyclic.native-checkout.path-reconcile.v1\0");
    input.extend_from_slice(&key.into_bytes());
    input.extend_from_slice(phase);
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        input.extend_from_slice(path.as_os_str().as_bytes());
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        for unit in path.as_os_str().encode_wide() {
            input.extend_from_slice(&unit.to_le_bytes());
        }
    }
    #[cfg(not(any(unix, windows)))]
    input.extend_from_slice(path.to_string_lossy().as_bytes());
    let digest = blake3::hash(&input);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

fn post_reconciliation_key(key: IdempotencyKey) -> IdempotencyKey {
    let mut input = Vec::with_capacity(64);
    input.extend_from_slice(b"acyclic.native-checkout.post-reconcile.v1\0");
    input.extend_from_slice(&key.into_bytes());
    let digest = blake3::hash(&input);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Fs, SourceMode};
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

    #[tokio::test]
    async fn keyed_revalidation_fences_a_concurrent_host_edit()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        std::fs::write(root.path().join("tracked.txt"), b"before")?;
        let fs = Fs::memory();
        let checkout = HostCheckout::attach(
            &fs,
            "host-checkout-revalidation",
            root.path(),
            SourceOptions {
                mode: SourceMode::Pinned,
                ..SourceOptions::default()
            },
        )
        .await?;
        let approved = checkout.binding().await;
        std::fs::write(root.path().join("tracked.txt"), b"concurrent")?;

        let refreshed = checkout
            .revalidate_with_key(IdempotencyKey::from_bytes([7; 16]))
            .await?;
        assert_ne!(refreshed.generation_id, approved.generation_id);
        assert!(matches!(
            checkout.prepare_publish(&approved).await,
            Err(HostCheckoutError::StaleSource { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn safe_restore_rejects_a_host_edit_before_any_path_effect()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        std::fs::write(root.path().join("tracked.txt"), b"before")?;
        let fs = Fs::memory();
        let checkout = HostCheckout::attach(
            &fs,
            "host-checkout-safe-restore",
            root.path(),
            SourceOptions {
                mode: SourceMode::Pinned,
                ..SourceOptions::default()
            },
        )
        .await?;
        let approved = checkout.binding().await;
        let generation = checkout.workspace().head().await?;
        std::fs::write(root.path().join("tracked.txt"), b"user-edit")?;

        let error = checkout
            .restore_paths_after_revalidation(
                &generation,
                &approved,
                IdempotencyKey::from_bytes([8; 16]),
                &[PathBuf::from("tracked.txt")],
                HostPathReplacement::Atomic,
                &MaterializeOptions::native(root.path()),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect_err("a stale physical edit must fence restore");
        assert!(matches!(error, HostCheckoutError::StaleSource { .. }));
        assert_eq!(
            std::fs::read(root.path().join("tracked.txt"))?,
            b"user-edit"
        );
        Ok(())
    }

    #[tokio::test]
    async fn path_scoped_restore_preserves_an_unrelated_user_edit()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        std::fs::write(root.path().join("approved.txt"), b"before")?;
        std::fs::write(root.path().join("user.txt"), b"before")?;
        std::fs::write(root.path().join("user-delete.txt"), b"before")?;
        let fs = Fs::memory();
        let checkout = HostCheckout::attach(
            &fs,
            "host-checkout-unrelated-edit",
            root.path(),
            SourceOptions {
                mode: SourceMode::Pinned,
                ..SourceOptions::default()
            },
        )
        .await?;
        let approved = checkout.binding().await;
        let generation = checkout.workspace().head().await?;
        std::fs::write(root.path().join("user.txt"), b"user-change")?;

        let options = MaterializeOptions::native(root.path());
        let first = checkout
            .restore_paths_after_revalidation(
                &generation,
                &approved,
                IdempotencyKey::from_bytes([9; 16]),
                &[PathBuf::from("approved.txt")],
                HostPathReplacement::Atomic,
                &options,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(first.outcomes.len(), 1);
        assert_eq!(std::fs::read(root.path().join("user.txt"))?, b"user-change");
        assert_eq!(std::fs::read(root.path().join("approved.txt"))?, b"before");

        // A lost acknowledgement of the post-mutation reconciliation must be
        // replayable by its derived identity without another host mutation.
        let acknowledged = checkout
            .revalidate_with_key(post_reconciliation_key(IdempotencyKey::from_bytes([9; 16])))
            .await?;
        assert_eq!(acknowledged, checkout.binding().await);

        // A lost acknowledgement replays the same provider operation identity
        // and must keep the unrelated user edit intact.
        let replay = checkout
            .restore_paths_after_revalidation(
                &generation,
                &approved,
                IdempotencyKey::from_bytes([9; 16]),
                &[PathBuf::from("approved.txt")],
                HostPathReplacement::Atomic,
                &options,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(replay, first);
        assert_eq!(std::fs::read(root.path().join("user.txt"))?, b"user-change");

        // A deletion outside the approved path set is also user-owned and
        // must survive a later retry of the approved publication.
        std::fs::remove_file(root.path().join("user-delete.txt"))?;
        let deletion_retry = checkout
            .restore_paths_after_revalidation(
                &generation,
                &approved,
                IdempotencyKey::from_bytes([10; 16]),
                &[PathBuf::from("approved.txt")],
                HostPathReplacement::Atomic,
                &options,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(deletion_retry.outcomes.len(), 1);
        assert!(!root.path().join("user-delete.txt").exists());
        Ok(())
    }

    #[tokio::test]
    async fn post_restore_reconciliation_observes_an_edit_before_acknowledgement()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        std::fs::write(root.path().join("approved.txt"), b"before")?;
        std::fs::write(root.path().join("user.txt"), b"before")?;
        let fs = Fs::memory();
        let checkout = HostCheckout::attach(
            &fs,
            "host-checkout-post-reconcile",
            root.path(),
            SourceOptions::default(),
        )
        .await?;
        let approved = checkout.binding().await;
        assert_eq!(approved.source_root, std::fs::canonicalize(root.path())?);
        assert_eq!(approved.workspace_id, checkout.workspace().id());
        checkout
            .workspace()
            .write_text("/approved.txt", "agent")
            .await?;
        let generation = checkout.workspace().head().await?;
        assert_eq!(generation.workspace_id(), approved.workspace_id);
        let key = IdempotencyKey::from_bytes([11; 16]);
        let options = MaterializeOptions::native(root.path());
        assert_eq!(options.destination, approved.source_root);

        checkout
            .restore_paths(
                &generation,
                &approved,
                &[PathBuf::from("approved.txt")],
                HostPathReplacement::Atomic,
                &options,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        // This is the crash cut between physical publication and durable
        // acknowledgement. The post-reconcile operation must observe the
        // user edit and retain it rather than replaying the publication.
        std::fs::write(root.path().join("user.txt"), b"user-during-ack")?;
        let observed = checkout
            .revalidate_with_key(post_reconciliation_key(key))
            .await?;
        assert_ne!(observed.generation_id, approved.generation_id);
        assert_eq!(
            std::fs::read(root.path().join("user.txt"))?,
            b"user-during-ack"
        );
        assert_eq!(std::fs::read(root.path().join("approved.txt"))?, b"agent");
        Ok(())
    }
}

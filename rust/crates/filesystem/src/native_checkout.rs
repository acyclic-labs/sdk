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
    #[error("attached checkout generation is stale: expected {expected}, actual {actual}")]
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
}

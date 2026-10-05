//! Authenticated bridge between a provider-owned generation and one attached
//! native checkout.
//!
//! This module deliberately composes [`Source`] and the native materializer.
//! It does not copy a checkout into an adapter-owned store and it does not
//! infer an operating-system path from a [`VolumeRef`]. The source binding and
//! generation precondition must be retained by the caller and checked again
//! immediately before an approved host mutation.

use crate::native_mount::{
    HostPathReplacement, HostPathRestore, MaterializationReceipt, MaterializeOptions,
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
}

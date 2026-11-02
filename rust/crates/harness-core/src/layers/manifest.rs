//! Original immutable filesystem pins and their consuming commit conditions.
use acyclic_fs::{
    AsyncObjectStore, FilesystemCommit, Fs, Generation, OperationFailure, OperationReceipt,
    StreamAuthorityStore, WorkBudget, WorkCounters, WorkspaceError,
};
use acyclic_stream::StreamProvider;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// One complete configuration cut. Every position is an original retained
/// generation, including an empty configuration volume. A digest or decoded
/// manifest alone cannot construct a generation or supply its read authority.
///
/// The caller owns the pins; this view borrows them without allocating, copying
/// their closures or starting followers. Reads and evaluation use these exact
/// generations until the consuming commit conditions all of their heads.
pub struct Manifest<'a, A: 'static, O: 'static> {
    generations: [&'a Generation<A, O>; 6],
    digest: [u8; 32],
}

impl<'a, A, O> Manifest<'a, A, O> {
    /// Pins every configuration position in [`super::ORDER`]. The host selects
    /// these original sources; a subject cannot replace an issuer's source by
    /// writing a similarly named file in its own home.
    #[must_use]
    pub fn new(generations: [&'a Generation<A, O>; 6]) -> Self {
        let workspaces = generations.map(|generation| generation.workspace_id().into_bytes());
        let identities = generations.map(|generation| generation.id().to_bytes());
        let digest = super::stack_digest(
            super::ORDER
                .into_iter()
                .zip(workspaces.iter().zip(&identities))
                .flat_map(|(name, (workspace, generation))| {
                    [(name, workspace.as_slice()), (name, generation.as_slice())]
                }),
        );
        Self {
            generations,
            digest,
        }
    }

    /// Cached identity of the complete ordered cut. Comparing an unchanged
    /// manifest reads one fixed digest and does not reopen or parse any file.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Exact original sources, lowest first. Every settings fold, policy meet
    /// and import resolution must use this same cut.
    pub fn sources(&self) -> impl DoubleEndedIterator<Item = (&'static str, &'a Generation<A, O>)> {
        super::ORDER.into_iter().zip(self.generations)
    }
}

/// A refused manifest condition retains the original commit and all its
/// preparation, publication and allocation custody. It has not been submitted.
pub struct ManifestFailure {
    /// The same partially conditioned caller commit; no naked request escapes.
    pub commit: FilesystemCommit,
    /// Original failing step and aggregate work, including earlier layers.
    pub failure: OperationFailure<WorkspaceError>,
}

impl<P: StreamProvider + 'static, O: AsyncObjectStore + 'static>
    Manifest<'_, StreamAuthorityStore<P>, O>
{
    /// Adds every source-head condition to the SAME caller commit, spending
    /// one aggregate budget. Nothing is published here. The caller submits the
    /// returned commit once through the original filesystem submission path;
    /// its Rejected/Unknown outcomes and exact retry ownership stay unchanged.
    /// A source changed between reading and submission rejects the whole
    /// consuming commit, even when all changed data paths are unrelated.
    ///
    /// # Errors
    /// Original custody plus the exact source failure; no retry or polling.
    pub async fn condition(
        &self,
        filesystem: &Fs<StreamAuthorityStore<P>, O>,
        mut commit: FilesystemCommit,
        budget: WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<OperationReceipt<FilesystemCommit>, ManifestFailure> {
        let mut spent = WorkCounters::UNCHARGED;
        for (_, generation) in self.sources() {
            let remaining = match spent.remaining(budget) {
                Ok(remaining) => remaining,
                Err(error) => {
                    return Err(ManifestFailure {
                        commit,
                        failure: OperationFailure::new(error.into(), spent),
                    });
                }
            };
            match filesystem
                .require_head(generation, &mut commit, remaining, cancellation)
                .await
            {
                Ok(receipt) => match spent.checked_add(receipt.work) {
                    Ok(work) => spent = work,
                    Err(error) => {
                        return Err(ManifestFailure {
                            commit,
                            failure: OperationFailure::new(error.into(), spent),
                        });
                    }
                },
                Err(failure) => {
                    return Err(ManifestFailure {
                        commit,
                        failure: failure.map_with_prior_work(spent, std::convert::identity),
                    });
                }
            }
        }
        Ok(OperationReceipt {
            value: commit,
            work: spent,
        })
    }
}

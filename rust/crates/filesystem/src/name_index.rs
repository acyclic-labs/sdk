//! Generation-bound path-index pages are ordinary files in the indexed volume.
//! Publishing a page and its restart checkpoint is one filesystem transaction.
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Generation, GenerationId,
    IdempotencyKey, NameCheckpoint, NameCut, PublicationPermit, SearchError, SearchName,
    TransactionCommit, WorkBudget, Workspace, WorkspaceError,
};
mod checkpoint;
mod files;
mod refresh;
#[cfg(feature = "native-mount")]
mod refresh_native;
pub use refresh::NameIndexRefresh;

const ROOT: &str = "/.acyclic-sdk/name-index";
const MANIFEST: &str = "/.acyclic-sdk/name-index/manifest.json";
const MAXIMUM_INDEX_FILE_BYTES: u64 = crate::name_walk::NAME_PAGE_BYTES;

/// Versioned progress and restart state of one shared volume index.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameIndexManifest {
    /// Durable index format, independent of source implementation.
    pub schema: u32,
    /// Names are interpreted at this immutable source/authored cut.
    pub cut: NameCut,
    /// Last cut at which candidate bindings changed. Content-only source
    /// revisions preserve this identity so followers retain their name fold.
    pub names_cut: NameCut,
    /// Generation observed before this manifest's atomic publication.
    pub observed_generation: GenerationId,
    /// Number of committed, bounded pages.
    pub pages: u64,
    /// Directory continuations still awaiting metadata enumeration.
    pub pending_directories: usize,
    /// Only a terminal source page may set this bit.
    pub complete: bool,
    /// Metadata-only continuation after the last committed page.
    pub checkpoint: NameCheckpoint,
}

impl NameIndexManifest {
    /// Current on-volume format. Directory observations include stable source
    /// identities and source-bound journal evidence for atomic replay progress.
    pub const SCHEMA: u32 = 7;
}

/// A pinned file-query snapshot of the index. Reading pages never lists source
/// directories and cannot materialize source-backed file contents.
pub struct NameIndex<A, O> {
    generation: Generation<A, O>,
    files: files::IndexFiles,
    #[cfg(test)]
    checkpoint_policy_records: usize,
    #[cfg(test)]
    checkpoint_metadata_records: usize,
    #[cfg(test)]
    checkpoint_frontier_records: usize,
    /// Follow clients fold progress from this file and stream the numbered pages.
    pub manifest: NameIndexManifest,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> NameIndex<A, O> {
    /// Opens the manifest at one exact SDK generation, without following into
    /// the native source namespace even when the workspace is lazy.
    pub async fn open(workspace: &Workspace<A, O>) -> Result<Option<Self>, SearchError> {
        Self::open_reusing(workspace, None).await
    }

    /// Follows the current publication while retaining an already restored
    /// checkpoint when the generation is identical or a bounded authenticated
    /// file-identity diff proves that every index file and binding is unchanged.
    pub async fn open_reusing(
        workspace: &Workspace<A, O>,
        previous: Option<&Self>,
    ) -> Result<Option<Self>, SearchError> {
        let generation = workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        if let Some(previous) = previous
            && workspace.id() == previous.generation.workspace_id()
            && workspace
                .volume
                .fs
                .owns_workspace(&previous.generation.workspace())
            && (generation.id() == previous.generation.id()
                || files::bounded_diff(&previous.generation, &generation)
                    .await?
                    .is_some_and(|diff| previous.files.unchanged(&diff)))
        {
            return Ok(Some(Self {
                generation,
                files: previous.files.clone(),
                manifest: previous.manifest.clone(),
                #[cfg(test)]
                checkpoint_policy_records: 0,
                #[cfg(test)]
                checkpoint_metadata_records: 0,
                #[cfg(test)]
                checkpoint_frontier_records: 0,
            }));
        }
        let bytes = match generation
            .read(
                MANIFEST,
                MAXIMUM_INDEX_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => return Ok(None),
            Err(failure) => return Err(failure.error.into()),
        };
        let stored: checkpoint::StoredManifest = serde_json::from_slice(&bytes)?;
        if stored.schema != NameIndexManifest::SCHEMA {
            return Err(SearchError::Invalid(
                "unsupported pathname index schema".into(),
            ));
        }
        let manifest = stored.restore(&generation).await?;
        let files = files::IndexFiles::collect(&generation).await?;
        Ok(Some(Self {
            generation,
            files,
            manifest,
            #[cfg(test)]
            checkpoint_policy_records: 0,
            #[cfg(test)]
            checkpoint_metadata_records: 0,
            #[cfg(test)]
            checkpoint_frontier_records: 0,
        }))
    }

    /// Generation pin for a client reading this manifest's committed pages.
    pub fn generation(&self) -> Generation<A, O> {
        self.generation.clone()
    }

    /// Streams one already-indexed page. A large index has more pages, never a
    /// larger per-client candidate buffer or a maximum-file-count failure.
    pub async fn page(&self, page: u64) -> Result<Vec<SearchName>, SearchError> {
        if page >= self.manifest.pages {
            return Err(SearchError::Invalid(
                "pathname index page is not committed".into(),
            ));
        }
        let bytes = self
            .generation
            .read(
                &page_path(page),
                MAXIMUM_INDEX_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Compare-and-publish a bounded page and checkpoint together. Competing
    /// owners cannot overwrite progress derived from a different observed head.
    /// A caller retries a conflict by following the winning manifest.
    /// Supplying the previous pinned index persists only changed policies.
    pub async fn publish(
        workspace: &Workspace<A, O>,
        expected: &Generation<A, O>,
        mut manifest: NameIndexManifest,
        page: Option<&[SearchName]>,
        previous: Option<&Self>,
    ) -> Result<Option<Self>, SearchError> {
        manifest.observed_generation = expected.id();
        let Some(mut transaction) = begin_at(workspace, expected).await? else {
            return Ok(None);
        };
        let mut files = Vec::new();
        let path = page_path(manifest.pages.saturating_sub(1));
        if let Some(page) = page {
            let bytes = serde_json::to_vec(page)?;
            if bytes.len() as u64 > MAXIMUM_INDEX_FILE_BYTES {
                return Err(SearchError::Invalid(
                    "pathname index page exceeds its file budget".into(),
                ));
            }
            files.push((path.as_str(), bytes.into()));
        }
        transaction
            .write_batch(
                &["/.acyclic-sdk", ROOT],
                &files,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?;
        let (_policy_records, _metadata_records, _frontier_records) =
            checkpoint::write_manifest(&mut transaction, expected, &manifest, previous).await?;
        let outcome = transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        match outcome {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => {
                let files = files::IndexFiles::after_publication(&generation, previous).await?;
                Ok(Some(Self {
                    generation,
                    files,
                    manifest,
                    #[cfg(test)]
                    checkpoint_policy_records: _policy_records,
                    #[cfg(test)]
                    checkpoint_metadata_records: _metadata_records,
                    #[cfg(test)]
                    checkpoint_frontier_records: _frontier_records,
                }))
            }
            TransactionCommit::Conflict { .. } => Ok(None),
            TransactionCommit::Fenced | TransactionCommit::IdempotencyConflict => Err(
                SearchError::Invalid("pathname index publication was fenced".into()),
            ),
        }
    }
}

// An exact checkout pins its data root, but its publication head can be newer.
// Open the actual head and compare the data generation from that same checkout.
// Subsequent commits use its captured authority head, without a rebase retry.
async fn begin_at<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    expected: &Generation<A, O>,
) -> Result<Option<crate::Transaction<A, O>>, SearchError> {
    if workspace.id() != expected.workspace_id()
        || !workspace.volume.fs.owns_workspace(&expected.workspace())
    {
        return Err(WorkspaceError::ForeignGeneration.into());
    }
    let transaction = workspace
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    Ok((transaction.checkout.generation_id() == expected.id()).then_some(transaction))
}

fn page_path(page: u64) -> String {
    format!("{ROOT}/page-{page:016x}.json")
}

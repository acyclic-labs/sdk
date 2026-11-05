//! The project checkout's diff query: its changes since the checkout's first
//! complete import, the durable baseline the checkout capture retains. The
//! filesystem's changed paths between that baseline and the current head form
//! one publication, folded and paged by the shared [`DiffView`]; every
//! endpoint read goes through this harness's authenticated content reader.
//! Equal generations fold to the same projection, so a cursor resumes exactly
//! while the head is unchanged and is refused once it moves.
use super::{LocalHarness, storage};
use crate::{
    Error, Result,
    client::{DiffLimits, DiffView},
    context::{
        ContextRoot, DiffCursor, DiffFilePage, DiffPath, DiffPublication, PinnedContextPath,
    },
    resources::GenerationRef,
};
use acyclic_fs::{
    CancellationToken, ChangedPath, IdempotencyKey, WorkBudget,
    kernel::{FileKind, FileRecord},
};
use acyclic_harness_client::AgentPageRequest;
use std::collections::BTreeMap;

/// The largest file diffed line by line; a larger one shows only its size.
const DIFF_FILE_BYTES: u64 = 64 * 1024;

impl LocalHarness {
    /// One page of the project checkout's diff, at `page`'s cursor.
    ///
    /// # Errors
    /// Without a checkout, on a storage failure, or for a cursor of another
    /// projection.
    pub(super) async fn workspace_diff(
        &self,
        page: &AgentPageRequest,
    ) -> Result<Vec<DiffFilePage>> {
        let checkout = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("workspace diff requires a checkout".into()))?;
        let workspace = checkout.view().workspace();
        let cancellation = CancellationToken::new();
        let Some(baseline) = workspace
            .operation_generation(
                IdempotencyKey::from_bytes(
                    super::checkout_tools::capture::baseline_operation(workspace.id()).into_bytes(),
                ),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value
        else {
            // Nothing is captured before the first complete import.
            return Ok(Vec::new());
        };
        let head = workspace
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let changes = baseline
            .diff_to(&head, u32::MAX, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| storage(failure.error))?
            .value
            .0;
        let changed = changes
            .changed_paths(u32::MAX, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| storage(failure.error))?
            .value
            .0;
        let base = self.files.generation_ref_id(baseline.id(), None)?;
        let current = self.files.generation_ref_id(head.id(), None)?;
        let root = ContextRoot {
            volume: self.project.clone(),
            directory: String::new(),
        };
        let paths = publication_paths(&changed, &root, (&base, &current), &cancellation)?;
        let limits = DiffLimits {
            files: paths.len().max(1),
            changes: paths.len().max(1),
            file_bytes: DIFF_FILE_BYTES,
            render_bytes: usize::MAX,
            work: usize::MAX,
        };
        let mut view = DiffView::new(self.project.clone(), base.clone())?;
        view.apply(
            &DiffPublication {
                previous: base,
                generation: current,
                paths,
            },
            &*self.client_reader()?,
            limits,
        )
        .await?;
        fill(&view, page)
    }
}

/// The publication rows of `changed`: one per file path, sorted. A directory
/// has no content of its own; a file replacing one, or replaced by one, is
/// added or removed on its own row. Internal paths are never disclosed.
fn publication_paths(
    changed: &[ChangedPath],
    root: &ContextRoot,
    (base, current): (&GenerationRef, &GenerationRef),
    cancellation: &CancellationToken,
) -> Result<Vec<DiffPath>> {
    let pinned = |generation: &GenerationRef, path: &str| PinnedContextPath {
        root: root.clone(),
        generation: generation.clone(),
        path: path.to_owned(),
    };
    let mut paths = BTreeMap::new();
    for change in changed {
        let mut path = String::new();
        for component in change.path.components() {
            if !path.is_empty() {
                path.push('/');
            }
            path.push_str(
                &component
                    .unicode_text(WorkBudget::UNBOUNDED, cancellation)
                    .map_err(|_| Error::Invalid("changed path is not Unicode".into()))?
                    .value,
            );
        }
        let (before, after) = (file(change.before), file(change.after));
        if (before.is_none() && after.is_none()) || crate::conversation::is_internal_path(&path) {
            continue;
        }
        let entry = DiffPath {
            base: before.map(|_| pinned(base, &path)),
            current: after.map(|_| pinned(current, &path)),
            largest_file_bytes: before
                .iter()
                .chain(after.iter())
                .map(FileRecord::logical_file_bytes)
                .max(),
            matches_base: before.zip(after).is_some_and(|(before, after)| {
                before.kind == after.kind && before.payload == after.payload
            }),
            path: path.clone(),
        };
        paths.entry(path).or_insert(entry);
    }
    Ok(paths.into_values().collect())
}

/// A file endpoint: a directory has no content of its own.
fn file(record: Option<FileRecord>) -> Option<FileRecord> {
    record.filter(|record| record.kind != FileKind::Directory)
}

/// Fills one page from `view` at `page`'s cursor: whole fragments while rows
/// and encoded bytes remain, stopping at a file split by the page's credit.
fn fill(view: &DiffView, page: &AgentPageRequest) -> Result<Vec<DiffFilePage>> {
    let mut cursor = page
        .cursor()
        .map(serde_json::from_str::<DiffCursor>)
        .transpose()
        .map_err(|_| Error::Invalid("invalid diff page position".into()))?;
    let mut rows = usize::try_from(page.items()).unwrap_or(usize::MAX);
    let mut bytes = usize::try_from(page.bytes()).unwrap_or(usize::MAX);
    let mut items = Vec::new();
    while rows > 0 && bytes > 0 {
        // Escaping can grow patch text when encoded: offer half the remaining
        // credit, then charge the fragment's whole encoded size.
        let Some(fragment) = view.page(cursor.as_ref(), rows, (bytes / 2).max(1))? else {
            break;
        };
        let encoded = serde_json::to_vec(&fragment)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len();
        if encoded > bytes {
            if items.is_empty() {
                return Err(Error::Invalid(
                    "diff page credit cannot hold one fragment".into(),
                ));
            }
            break;
        }
        bytes -= encoded;
        rows = rows.saturating_sub(
            usize::from(!fragment.continued) + fragment.patch.split_inclusive('\n').count(),
        );
        cursor.clone_from(&fragment.next);
        let split = cursor.as_ref().is_some_and(|next| next.offset.is_some());
        items.push(fragment);
        if cursor.is_none() || split {
            break;
        }
    }
    Ok(items)
}

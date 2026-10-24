//! Removes head entries the reserved-name policy forbids.
//!
//! Demand, capture and publication never carry a reserved name, and the SDK
//! writes only its owned `.system` subtrees. Opening a binding still checks
//! the head itself, so trust never rests on anything a head can hold: every
//! entry under a reserved name outside an owned subtree is removed, and the
//! owned subtrees are kept without being read. Nothing is recorded, so no
//! bytes in a head, wherever they came from, can skip the check.
use crate::{Error, Result};
use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::kernel::FileKind;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, FileId, Generation, LazyWorkspace,
    LazyWorkspaceStore, WorkBudget, WorkspaceError,
};

const PAGE: u32 = 256;

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(format!("reserved-name reconciliation: {error}"))
}

/// Head entries under `name` the SDK does not own, every child before its
/// parent. Owned subtrees are neither listed nor descended into.
async fn forbidden<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    head: &Generation<A, O>,
    name: &str,
    cancellation: &CancellationToken,
) -> Result<Vec<(String, FileId)>> {
    let root = match head
        .stat(&format!("/{name}"), WorkBudget::UNBOUNDED, cancellation)
        .await
    {
        Ok(stat) => stat.value,
        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {
            return Ok(Vec::new());
        }
        Err(failure) => return Err(storage(failure.error)),
    };
    let mut entries = vec![(name.to_owned(), root.file_id, root.kind)];
    let mut index = 0;
    while index < entries.len() {
        let (directory, _, kind) = &entries[index];
        // A reserved root that holds owned subtrees is listed for its other
        // children; an owned subtree itself is SDK state.
        let owned = super::reserved::sdk_owned(directory, *kind);
        if *kind == FileKind::Directory && (!owned || directory.as_str() == name) {
            let directory = directory.clone();
            let mut after = None;
            loop {
                let page = head
                    .list_directory(
                        &format!("/{directory}"),
                        after.as_ref(),
                        PAGE,
                        WorkBudget::UNBOUNDED,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| storage(failure.error))?
                    .value
                    .0;
                for entry in &page.entries {
                    let child = entry
                        .name
                        .unicode_text(WorkBudget::UNBOUNDED, cancellation)
                        .map_err(|failure| storage(failure.error))?
                        .value;
                    entries.push((format!("{directory}/{child}"), entry.file_id, entry.kind));
                }
                after = page.entries.last().map(|entry| entry.name.clone());
                if !page.has_more || after.is_none() {
                    break;
                }
            }
        }
        index += 1;
    }
    // Breadth-first order lists parents first; reversed, children come first.
    Ok(entries
        .into_iter()
        .rev()
        .filter(|(path, _, kind)| !super::reserved::sdk_owned(path, *kind))
        .map(|(path, file_id, _)| (path, file_id))
        .collect())
}

/// Removes every entry of `view`'s head that the reserved-name policy
/// forbids. Removal is idempotent, so an interrupted open simply repeats it.
pub async fn reconcile_reserved_names<A, O, S>(
    view: &LazyWorkspace<A, O, NativeDemandSource, S>,
) -> Result<()>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    S: LazyWorkspaceStore,
{
    let cancellation = CancellationToken::new();
    let head = view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| storage(failure.error))?
        .value;
    let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
    for name in super::RESERVED_NAMES {
        for (relative, file_id) in forbidden(&head, name, &cancellation).await? {
            view.remove(&format!("/{relative}"), Some(file_id), &mut accounting)
                .await
                .map_err(storage)?;
        }
    }
    Ok(())
}

use super::*;
use crate::demand::{DemandSource, SourceNodeKind};
use crate::facade::FsWork as _;
use crate::lazy_workspace::NativeNameDelta;
use crate::{GitIgnorePolicy, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore};
use std::collections::{BTreeMap, BTreeSet};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> NameIndex<A, O> {
    /// Applies a bounded native delta without listing unchanged directories.
    /// Changed directories become resumable discovery roots in the manifest.
    pub async fn refresh_native<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        &self,
        lazy: &LazyWorkspace<A, O, D, S>,
        snapshot: LazySnapshotRef,
        delta: NativeNameDelta,
        ignore: &GitIgnorePolicy,
    ) -> Result<NameIndexRefresh<A, O>, SearchError> {
        let NameCut::Lazy(old) = &self.manifest.cut else {
            return Ok(NameIndexRefresh::Ineligible);
        };
        if delta.all
            || !self.manifest.complete
            || old.workspace_id != snapshot.workspace_id
            || old.source.identity != snapshot.source.identity
            || old.overlay != snapshot.overlay
            || old.shadows != snapshot.shadows
        {
            return Ok(NameIndexRefresh::Ineligible);
        }
        let cut = NameCut::Lazy(snapshot);
        let Some(mut checkpoint) = self.manifest.checkpoint.completed_at(cut.clone()) else {
            return Ok(NameIndexRefresh::Ineligible);
        };
        if let Some(journal) = delta.journal {
            checkpoint.record_journal_checkpoint(journal)?;
        }
        let stop = CancellationToken::new();
        let workspace = lazy.workspace();
        let expected = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .map_err(|failure| failure.error)?
            .value;
        if expected.id() != snapshot.authored_generation {
            return Ok(NameIndexRefresh::Conflict);
        }
        let observed = workspace
            .generation(
                self.manifest.observed_generation,
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let (diff, _) = match observed
            .diff_to(&expected, 512, WorkBudget::UNBOUNDED, &stop)
            .await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, WorkspaceError::JoinLimit) => {
                return Ok(NameIndexRefresh::Ineligible);
            }
            Err(failure) => return Err(failure.error.into()),
        };
        if diff.changes().truncated {
            return Ok(NameIndexRefresh::Ineligible);
        }
        let (paths, _allocation) = match diff.changed_paths(512, WorkBudget::UNBOUNDED, &stop).await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, WorkspaceError::ChangedPathLimit) => {
                return Ok(NameIndexRefresh::Ineligible);
            }
            Err(failure) => return Err(failure.error.into()),
        };
        for path in paths {
            if path
                .path
                .components()
                .iter()
                .any(|name| name.unicode_utf8_bytes().is_none())
            {
                return Ok(NameIndexRefresh::Ineligible);
            }
            if let Some(first) = path.path.components().first()
                && first
                    .unicode_text(WorkBudget::UNBOUNDED, &stop)
                    .map_err(|failure| WorkspaceError::from(failure.error))?
                    .value
                    != ".acyclic-sdk"
            {
                // Do not mix an unrelated authored edit into the native delta.
                return Ok(NameIndexRefresh::Ineligible);
            }
        }
        let mut leaves = BTreeMap::new();
        let mut subtrees = BTreeSet::new();
        for path in delta.paths {
            if subtrees
                .iter()
                .any(|root: &String| crate::name_walk::within_subtree(&path, root))
            {
                continue;
            }
            if path
                .split('/')
                .any(|part| matches!(part, ".git" | ".acyclic-sdk"))
            {
                continue;
            }
            if matches!(path.rsplit('/').next(), Some(".gitignore" | ".ignore")) {
                return Ok(NameIndexRefresh::Ineligible);
            }
            // A binding may have changed from a directory into a leaf, or
            // disappeared. Invalidate its old descendants in every case.
            checkpoint.forget_subtree(&path);
            subtrees.insert(path.clone());
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&stop);
            let parent = format!(
                "/{}",
                path.rsplit_once('/').map_or("", |(parent, _)| parent)
            );
            if !lazy
                .same_volume_at_snapshot(snapshot, &parent, &mut accounting)
                .await?
            {
                leaves.insert(path, None);
                continue;
            }
            let (entry, _, guard) = lazy
                .lookup_at_snapshot(snapshot, &format!("/{path}"), &mut accounting)
                .await?;
            let kind = entry.as_ref().map(|entry| entry.stat().kind);
            if let Some(entry) = entry {
                lazy.retire_lookup((entry, guard), &mut accounting)?;
            } else {
                crate::kernel::retire_allocation(guard, 0, &mut accounting)?;
            }
            if kind == Some(SourceNodeKind::Directory) {
                let eligible = checkpoint.eligible_directory(ignore, &path)
                    && lazy
                        .same_volume_at_snapshot(snapshot, &format!("/{path}"), &mut accounting)
                        .await?;
                if eligible {
                    let authored = match expected
                        .stat(&format!("/{path}"), WorkBudget::UNBOUNDED, &stop)
                        .await
                    {
                        Ok(receipt) => receipt.value.kind == crate::kernel::FileKind::Directory,
                        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => false,
                        Err(failure) => return Err(failure.error.into()),
                    };
                    checkpoint.discover_directory(&path, authored);
                    leaves.insert(
                        path.clone(),
                        Some(SearchName {
                            path,
                            regular: false,
                            directory: true,
                        }),
                    );
                }
                continue;
            }
            let name = kind
                .filter(|_| checkpoint.eligible(ignore, &path))
                .map(|kind| SearchName {
                    path: path.clone(),
                    regular: kind == SourceNodeKind::RegularFile,
                    directory: false,
                });
            leaves.insert(path, name);
        }
        self.publish_delta(
            workspace,
            &expected,
            cut,
            checkpoint,
            super::refresh::NameDelta { leaves, subtrees },
        )
        .await
    }
}

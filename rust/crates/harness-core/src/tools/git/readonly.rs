//! Exact Git reads without workspace publication, recovery, or operation leases.
use super::*;
use acyclic_fs::{NativeMaterializationWork, ReadOnlyGeneration};

type InspectedTree<'a> = ReadOnlyGeneration<'a, LocalAuthorityBackend, LocalObjectBackend>;

pub(super) fn work(cancellation: &CancellationToken) -> crate::Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(WorkBudget::UNBOUNDED, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

pub(super) async fn workspace_tree(view: View<'_>) -> crate::Result<GitTreeRef> {
    match view {
        View::Checkout(source) => source
            .inspection_snapshot(
                source.workspace(),
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .await
            .map(GitTreeRef::Lazy)
            .map_err(Error::from),
        View::Volume(volume) => {
            let cancellation = CancellationToken::new();
            let work = work(&cancellation)?;
            let generation = volume.head(&work, &cancellation).await?;
            Ok(GitTreeRef::exact(volume.id(), generation.id()))
        }
    }
}

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn status_read_only(
        &self,
        mut status: acyclic_fs::GitStatus,
    ) -> crate::Result<acyclic_fs::GitStatus> {
        let cancellation = CancellationToken::new();
        let work = work(&cancellation)?;
        let mut after = self
            .inspected_tree(status.workspace, &work, &cancellation, None)
            .await?;
        if let Some((_, view)) = self.inspection {
            after = after.capture_native(view, &work, &cancellation).await?;
        }
        if let Some(head) = status.head {
            let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
            let GitCommandOutput::Commits(commits) = repository
                .execute(
                    GitCommand::Show {
                        object: Some(head.to_hex()),
                    },
                    status.workspace,
                )
                .await
                .map_err(Error::from)?
            else {
                return Err(Error::Conflict("HEAD did not resolve to a commit".into()));
            };
            let commit = commits
                .first()
                .ok_or_else(|| Error::NotFound("HEAD commit".into()))?;
            let before = self
                .inspected_tree(commit.tree, &work, &cancellation, Some(&after))
                .await?;
            let changes = before
                .diff_to(
                    &after,
                    &self.repository,
                    &commit.tracked_paths,
                    &cancellation,
                )
                .await?;
            status.dirty = if changes.file_changes == 0 && changes.binding_changes == 0 {
                acyclic_fs::GitDirtyState::Clean
            } else {
                acyclic_fs::GitDirtyState::Dirty
            };
        } else {
            status.dirty = acyclic_fs::GitDirtyState::Dirty;
        }
        status.workspace = GitTreeRef::exact(self.current.id(), after.id());
        Ok(status)
    }

    async fn inspected_tree<'a>(
        &'a self,
        tree: GitTreeRef,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
        within: Option<&InspectedTree<'a>>,
    ) -> crate::Result<InspectedTree<'a>> {
        match tree {
            GitTreeRef::Exact(reference) => {
                let workspace = self.workspace(reference.workspace_id).await?;
                workspace
                    .generation(reference.generation, work, cancellation)
                    .await?;
                match within {
                    Some(within) => within
                        .related_generation(&workspace, reference.generation)
                        .map_err(Error::from),
                    None => self
                        .fs
                        .read_only_generation(&workspace, reference.generation, work)
                        .map_err(Error::from),
                }
            }
            GitTreeRef::Lazy(snapshot) => {
                let workspace = self.workspace(snapshot.workspace_id).await?;
                let View::Checkout(source) = self.lazy_current else {
                    return Err(Error::Unauthorized(
                        "volume repository refers to a foreign native source".into(),
                    ));
                };
                match within {
                    Some(within) => {
                        within
                            .related_snapshot(&workspace, source, snapshot, work, cancellation)
                            .await
                    }
                    None => {
                        self.fs
                            .read_only_snapshot(&workspace, source, snapshot, work, cancellation)
                            .await
                    }
                }
                .map_err(Error::from)
            }
        }
    }

    pub(super) async fn diff_read_only(
        &self,
        from: Option<&GitTreeRef>,
        to: &GitTreeRef,
        tracked: &BTreeSet<String>,
    ) -> crate::Result<GitFilesystemResult> {
        let cancellation = CancellationToken::new();
        let work = work(&cancellation)?;
        let mut after = self.inspected_tree(*to, &work, &cancellation, None).await?;
        if let Some((working, view)) = self.inspection
            && working == *to
        {
            after = after.capture_native(view, &work, &cancellation).await?;
        }
        let value = if let Some(from) = from {
            // The committed side stays immutable even when its reference equals
            // the workspace generation. Only the working side captures native edits.
            let before = self
                .inspected_tree(*from, &work, &cancellation, Some(&after))
                .await?;
            let changes = before
                .diff_to(&after, &self.repository, tracked, &cancellation)
                .await?;
            json!({ "from": hex::encode(before.id().digest().as_bytes()),
                "to": hex::encode(after.id().digest().as_bytes()),
                "fileChanges": changes.file_changes, "bindingChanges": changes.binding_changes })
        } else {
            json!({ "from": null, "to": hex::encode(after.id().digest().as_bytes()), "unborn": true })
        };
        Ok(GitFilesystemResult::Data {
            kind: "diff".to_owned(),
            value,
        })
    }

    pub(super) async fn show_file_read_only(
        &self,
        tree: GitTreeRef,
        path: &str,
    ) -> crate::Result<GitFilesystemResult> {
        let cancellation = CancellationToken::new();
        let work = work(&cancellation)?;
        let tree = self
            .inspected_tree(tree, &work, &cancellation, None)
            .await?;
        let bytes = tree
            .file_bytes(&format!("/{path}"), 16 * 1024 * 1024, &work, &cancellation)
            .await?;
        let value = match std::str::from_utf8(&bytes) {
            Ok(text) => json!({ "path": path, "text": text }),
            Err(_) => json!({ "path": path,
                "base64": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes) }),
        };
        Ok(GitFilesystemResult::Data {
            kind: "file".to_owned(),
            value,
        })
    }
}

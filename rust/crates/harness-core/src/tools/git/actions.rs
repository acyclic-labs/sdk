//! The Git filesystem actions the workspace executor performs.
use super::*;

/// Largest committed file `git show REV:PATH` returns, matching blame and grep.
const SHOWN_FILE_BYTES: u64 = 16 * 1024 * 1024;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    /// Captures the live workspace as a commit tree.
    pub(super) async fn capture_commit(
        &self,
        operation_id: OperationId,
        workspace_tree: &GitTreeRef,
        head_tree: Option<&GitTreeRef>,
        head_workspace_tree: Option<&GitTreeRef>,
        tracked_paths: &BTreeSet<String>,
    ) -> Result<GitFilesystemResult, Error> {
        if matches!(workspace_tree, GitTreeRef::Lazy(_)) {
            return Ok(GitFilesystemResult::Captured {
                tree: *workspace_tree,
                tracked_paths: tracked_paths.clone(),
                proof: None,
            });
        }
        let workspace_generation = self.generation(*workspace_tree).await?;
        if self.current.id() != workspace_generation.workspace_id()
            || self
                .current
                .head(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(Error::from)?
                .id()
                != workspace_generation.id()
        {
            return Err(Self::error("Git commit workspace changed before capture"));
        }
        let captured = match (head_workspace_tree, head_tree) {
            (Some(previous_live), Some(previous_capture)) => {
                let previous_live = self.generation(*previous_live).await?;
                let previous_capture = self.generation(*previous_capture).await?;
                capture_git_compatible_generation_incremental(
                    &self.current,
                    &previous_live,
                    &previous_capture,
                    &self.ignore,
                    tracked_paths,
                    operation_id,
                )
                .await
            }
            _ => {
                capture_git_compatible_generation(
                    &self.current,
                    &self.ignore,
                    tracked_paths,
                    operation_id,
                )
                .await
            }
        }
        .map_err(Error::from)?;
        let proof = captured
            .authenticated_proof(operation_id, &WorkspaceGraph::new(self.store.clone()))
            .await
            .map_err(Error::from)?;
        Ok(GitFilesystemResult::Captured {
            tree: GitTreeRef::exact(captured.generation.workspace_id(), captured.generation.id()),
            tracked_paths: captured.tracked_paths,
            proof,
        })
    }

    /// Forks a branch workspace from the unchanged source tree.
    pub(super) async fn fork_branch(
        &self,
        operation_id: OperationId,
        branch: &str,
        source_tree: &GitTreeRef,
        switch: bool,
    ) -> Result<GitFilesystemResult, Error> {
        let source_matches = match source_tree {
            GitTreeRef::Exact(source) => {
                self.current.id() == source.workspace_id
                    && self
                        .current
                        .head(
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                        .map_err(Error::from)?
                        .id()
                        == source.generation
            }
            GitTreeRef::Lazy(source) => {
                self.lazy_current.tree(false, self.permit).await? == GitTreeRef::Lazy(*source)
            }
        };
        if !source_matches {
            return Err(Self::error("Git branch source workspace changed"));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.repository_id.into_bytes());
        hasher.update(branch.as_bytes());
        let destination = format!(
            "git-branch-{}",
            hex::encode(&hasher.finalize().as_bytes()[..12])
        );
        let fork_generation = self
            .lazy_current
            .workspace()
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?
            .id();
        let workspace = self
            .lazy_current
            .fork(&destination, operation_id, self.permit)
            .await?;
        WorkspaceGraph::new(self.store.clone())
            .register_existing_child(
                self.lazy_current.workspace(),
                workspace.workspace(),
                fork_generation,
            )
            .await
            .map_err(Error::from)?;
        let workspace_id = workspace.workspace().id();
        if switch {
            *self
                .switched
                .lock()
                .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
                Some(workspace);
        }
        Ok(GitFilesystemResult::Forked { workspace_id })
    }

    /// Selects a related workspace for the rest of the command.
    pub(super) async fn switch_workspace(
        &self,
        workspace_id: &acyclic_fs::WorkspaceId,
    ) -> Result<GitFilesystemResult, Error> {
        let workspace = self.workspace(*workspace_id).await?;
        let lazy_workspace = self.lazy_current.related(workspace).await?;
        let tree = lazy_workspace.view().tree(false, self.permit).await?;
        *self
            .switched
            .lock()
            .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
            Some(lazy_workspace);
        Ok(GitFilesystemResult::Applied {
            tree: Some(tree),
            tracked_paths: None,
        })
    }

    /// Counts the Git-visible changes between two trees.
    pub(super) async fn diff(
        &self,
        from: Option<&GitTreeRef>,
        to: &GitTreeRef,
        tracked_paths: &BTreeSet<String>,
    ) -> Result<GitFilesystemResult, Error> {
        let to = self.generation(*to).await?;
        let value = if let Some(from) = from {
            let from = self.generation(*from).await?;
            let diff = git_compatible_diff_counts(
                &from,
                &to,
                &self.repository,
                tracked_paths,
                100_000,
                &CancellationToken::new(),
            )
            .await
            .map_err(Error::from)?;
            json!({
                "from": hex::encode(from.id().digest().as_bytes()),
                "to": hex::encode(to.id().digest().as_bytes()),
                "fileChanges": diff.file_changes,
                "bindingChanges": diff.binding_changes
            })
        } else {
            json!({
                "from": null,
                "to": hex::encode(to.id().digest().as_bytes()),
                "unborn": true
            })
        };
        Ok(GitFilesystemResult::Data {
            kind: "diff".to_owned(),
            value,
        })
    }

    /// Restores the live workspace, or only `paths`, to a tree.
    pub(super) async fn restore_generation(
        &self,
        operation_id: OperationId,
        tree: &GitTreeRef,
        paths: Option<&BTreeSet<String>>,
    ) -> Result<GitFilesystemResult, Error> {
        if let Some(paths) = paths {
            return self
                .restore_paths(
                    operation_id,
                    *tree,
                    &paths.iter().cloned().collect::<Vec<_>>(),
                )
                .await;
        }
        let source = self.generation(*tree).await?;
        if source.workspace_id() != self.current.id() {
            return Err(Self::error(
                "an exact workspace restore cannot use a foreign generation",
            ));
        }
        // Replay a committed restore before deriving paths from a head that
        // later writes may have moved.
        if let Some(replayed) = self.committed_restore(operation_id).await? {
            return Ok(replayed);
        }
        let current = self
            .current
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        // A whole-tree restore rewrites every changed path, nested ones
        // included, except the host's namespaces, whose state never rewinds.
        let paths = git_restore_paths(
            &current,
            &source,
            &self.ignore,
            u32::MAX,
            &CancellationToken::new(),
        )
        .await
        .map_err(Error::from)?;
        if paths.is_empty() {
            // The workspace already matches outside the host's namespaces.
            return Ok(GitFilesystemResult::Applied {
                tree: Some(GitTreeRef::exact(self.current.id(), current.id())),
                tracked_paths: None,
            });
        }
        self.restore_paths(operation_id, *tree, &paths).await
    }

    /// Applies a commit's paths to the live workspace.
    pub(super) async fn apply_commit(
        &self,
        operation_id: OperationId,
        base: Option<&GitTreeRef>,
        source: Option<&GitTreeRef>,
        paths: &BTreeSet<String>,
        tracked_paths: &BTreeSet<String>,
    ) -> Result<GitFilesystemResult, Error> {
        let base = match base {
            Some(base) => Some(self.generation(*base).await?),
            None => None,
        };
        let source = match source {
            Some(source) => Some(self.generation(*source).await?),
            None => None,
        };
        let current = self
            .current
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        // An applied commit, like a restore, never writes over a reserved
        // path or a directory holding one.
        let paths = git_paths_preserving_reserved(
            &current,
            source.as_ref(),
            &self.ignore,
            paths.iter().cloned(),
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(Error::from)?;
        let outcome = self
            .current
            .apply_paths_from(
                base.as_ref(),
                source.as_ref(),
                &paths,
                current.id(),
                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                self.permit,
            )
            .await
            .map_err(Error::from)?;
        let generation = match outcome {
            WorkspacePathApply::Applied(generation)
            | WorkspacePathApply::AlreadyApplied(generation)
            | WorkspacePathApply::NoChanges(generation) => generation,
            WorkspacePathApply::Conflicted(conflicts) => {
                return Err(Self::error(
                    serde_json::to_string(&json!({
                        "kind": "conflicts",
                        "conflicts": conflicts
                            .iter()
                            .map(|conflict| json!({
                                "path": conflict.path,
                                "kind": format!("{:?}", conflict.kind),
                            }))
                            .collect::<Vec<_>>()
                    }))
                    .map_err(json_error)?,
                ));
            }
            WorkspacePathApply::Stale(_) => {
                return Err(Self::error(
                    "Git commit application raced with another workspace writer",
                ));
            }
            WorkspacePathApply::Fenced => {
                return Err(Self::error("Git commit application was fenced"));
            }
            WorkspacePathApply::IdempotencyConflict => {
                return Err(Self::error(
                    "Git commit application retry identity was reused",
                ));
            }
        };
        let resulting_tracked = existing_tracked_paths(&generation, tracked_paths).await?;
        Ok(GitFilesystemResult::Applied {
            tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
            tracked_paths: Some(resulting_tracked),
        })
    }

    /// Lists a tree's Git-visible entries.
    pub(super) async fn archive(&self, tree: &GitTreeRef) -> Result<GitFilesystemResult, Error> {
        let generation = self.generation(*tree).await?;
        let entries = walk_git_tree(&generation, &self.ignore, None, 100_000)
            .await
            .map_err(Error::from)?;
        Ok(GitFilesystemResult::Data {
            kind: "archive".to_owned(),
            value: json!({
                "format": "acyclic-fs-manifest-v1",
                "generation": hex::encode(generation.id().digest().as_bytes()),
                "entries": entries
            }),
        })
    }

    /// Searches the Git-visible files of a tree.
    pub(super) async fn grep(
        &self,
        pattern: &str,
        path: Option<&str>,
        tree: &GitTreeRef,
    ) -> Result<GitFilesystemResult, Error> {
        let generation = self.generation(*tree).await?;
        let result = grep_git_generation(
            &generation,
            &self.ignore,
            pattern,
            path,
            100_000,
            16 * 1024 * 1024,
            100_000,
        )
        .await
        .map_err(Error::from)?;
        Ok(GitFilesystemResult::Data {
            kind: "grep".to_owned(),
            value: serde_json::to_value(result).map_err(json_error)?,
        })
    }

    /// Attributes the lines of `path` across `commits`.
    pub(super) async fn blame(
        &self,
        path: &str,
        commits: &[acyclic_fs::GitCommit],
    ) -> Result<GitFilesystemResult, Error> {
        let mut history = Vec::with_capacity(commits.len());
        for commit in commits {
            let generation = self.generation(commit.tree).await?;
            history.push((commit.clone(), generation));
        }
        let lines = blame_git_generations(&history, &self.ignore, path, 16 * 1024 * 1024)
            .await
            .map_err(Error::from)?;
        Ok(GitFilesystemResult::Data {
            kind: "blame".to_owned(),
            value: serde_json::to_value(lines).map_err(json_error)?,
        })
    }

    /// Removes, or lists, untracked files that are not ignored.
    pub(super) async fn clean(
        &self,
        operation_id: OperationId,
        dry_run: bool,
        tree: &GitTreeRef,
        tracked_paths: &BTreeSet<String>,
    ) -> Result<GitFilesystemResult, Error> {
        let generation = self.generation(*tree).await?;
        if generation.workspace_id() != self.current.id() {
            return Err(Self::error(
                "Git clean generation is not the live workspace",
            ));
        }
        let entries = walk_git_tree(&generation, &self.ignore, None, 100_000)
            .await
            .map_err(Error::from)?;
        let candidates: Vec<_> = entries
            .into_iter()
            .filter(|entry| {
                entry.kind != "directory"
                    && !tracked_paths.contains(&entry.path)
                    && self.ignore.eligible(&entry.path, false, false)
            })
            .map(|entry| entry.path)
            .collect();
        if dry_run || candidates.is_empty() {
            return Ok(GitFilesystemResult::Data {
                kind: "clean".to_owned(),
                value: json!({ "paths": candidates, "dryRun": dry_run }),
            });
        }
        let mut transaction = self
            .current
            .begin_transaction(
                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(Error::from)?;
        for path in &candidates {
            transaction
                .remove(
                    &format!("/{path}"),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(Error::from)?;
        }
        committed(
            transaction
                .commit(
                    None,
                    self.permit,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                    |_| async {
                        Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                            acyclic_fs::OperationReceipt {
                                value: (),
                                work: acyclic_fs::WorkCounters::default(),
                            },
                        )
                    },
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(Error::from)?,
        )
    }

    /// Lists which of `paths` the live tree ignores.
    pub(super) async fn check_ignore(
        &self,
        paths: &[String],
        tree: &GitTreeRef,
    ) -> Result<GitFilesystemResult, Error> {
        let generation = self.generation(*tree).await?;
        if generation.workspace_id() != self.current.id()
            || self
                .current
                .head(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(Error::from)?
                .id()
                != generation.id()
        {
            return Err(Self::error(
                "Git check-ignore live generation changed before inspection",
            ));
        }
        let ignored = paths
            .iter()
            .filter(|path| !self.ignore.eligible(path, false, false))
            .cloned()
            .collect::<Vec<_>>();
        Ok(GitFilesystemResult::Data {
            kind: "check-ignore".to_owned(),
            value: json!({ "paths": ignored }),
        })
    }

    /// Reads one committed file from an exact tree.
    pub(super) async fn show_file(
        &self,
        tree: &GitTreeRef,
        path: &str,
    ) -> Result<GitFilesystemResult, Error> {
        // Host namespaces are never part of a commit, even one captured
        // before they were kept out of the repository view.
        if self.ignore.reserves(path) {
            return Err(Error::NotFound(path.to_owned()));
        }
        let generation = self.generation(*tree).await?;
        let shown = format!("/{path}");
        let cancellation = acyclic_fs::CancellationToken::new();
        let kind = generation
            .stat(&shown, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value.kind)
            .map_err(|failure| Error::from(failure.error))?;
        // Git stores a link as its target, so showing one prints the target
        // and never follows it.
        let bytes = if kind == FileKind::SymbolicLink {
            generation
                .read_symbolic_link(&shown, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map(|receipt| receipt.value.bytes)
        } else {
            generation
                .read(
                    &shown,
                    SHOWN_FILE_BYTES,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map(|receipt| receipt.value)
        }
        .map_err(|failure| Error::from(failure.error))?;
        let value = match std::str::from_utf8(&bytes) {
            Ok(text) => json!({ "path": path, "text": text }),
            Err(_) => json!({
                "path": path,
                "base64": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes),
            }),
        };
        Ok(GitFilesystemResult::Data {
            kind: "file".to_owned(),
            value,
        })
    }

    /// Applies a Git patch to the live workspace.
    pub(super) async fn apply_patch(
        &self,
        operation_id: OperationId,
        patch: &[u8],
    ) -> Result<GitFilesystemResult, Error> {
        committed(
            apply_git_patch(
                &self.current,
                patch,
                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                self.permit,
            )
            .await
            .map_err(Error::from)?,
        )
    }
}

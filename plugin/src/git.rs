//! The `acyclic git` executor over managed workspaces.

use super::*;

#[derive(Debug)]
pub(crate) struct PluginGitError(pub(crate) String);

impl std::fmt::Display for PluginGitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PluginGitError {}

impl From<String> for PluginGitError {
    fn from(message: String) -> Self {
        Self(message)
    }
}

pub(crate) struct PluginGitExecutor<'a> {
    pub(crate) fs: &'a LocalFs,
    pub(crate) distributed: &'a LocalDistributedFs,
    pub(crate) current: LocalWorkspace,
    pub(crate) lazy_current: LocalLazyWorkspace,
    pub(crate) repository_id: acyclic_fs::WorkspaceId,
    pub(crate) ignore: GitIgnorePolicy,
    pub(crate) permit: PublicationPermit,
    pub(crate) lease: Option<OperationWindowLease>,
    pub(crate) merge_drivers: Arc<MergeDriverRegistry>,
    pub(crate) merge_cache: AsyncMutex<MemoryMergeResolutionCache>,
    pub(crate) switched: Mutex<Option<LocalLazyWorkspace>>,
}

impl PluginGitExecutor<'_> {
    pub(crate) fn error(message: impl Into<String>) -> PluginGitError {
        PluginGitError(message.into())
    }

    pub(crate) fn switched_workspace(&self) -> Result<Option<LocalLazyWorkspace>, PluginGitError> {
        self.switched
            .lock()
            .map_err(|_| Self::error("Git workspace switch lock is unavailable"))
            .map(|workspace| workspace.clone())
    }

    pub(crate) async fn workspace(
        &self,
        workspace_id: acyclic_fs::WorkspaceId,
    ) -> Result<LocalWorkspace, PluginGitError> {
        self.distributed
            .workspace(workspace_id)
            .await
            .map_err(display)
            .map_err(PluginGitError)
    }

    pub(crate) async fn exact(
        &self,
        tree: GitTreeRef,
        operation: &str,
    ) -> Result<GitGenerationRef, PluginGitError> {
        match tree {
            GitTreeRef::Exact(reference) => Ok(reference),
            GitTreeRef::Lazy(snapshot) => {
                let workspace = self.workspace(snapshot.workspace_id).await?;
                let lazy = self
                    .lazy_current
                    .open_related(workspace)
                    .await
                    .map_err(display)?;
                let snapshot_bytes = snapshot.id.into_bytes();
                let destination = format!("git-exact-{}", hex::encode(&snapshot_bytes[..12]));
                let exact = lazy
                    .exactify_snapshot(
                        snapshot,
                        &destination,
                        IdempotencyKey::from_bytes(snapshot_bytes[..16].try_into().map_err(
                            |_| Self::error(format!("{operation} snapshot identity is invalid")),
                        )?),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
                self.distributed
                    .lineage()
                    .register_existing_child(
                        lazy.workspace(),
                        &self
                            .fs
                            .open_workspace(&destination)
                            .await
                            .map_err(display)?,
                        snapshot.authored_generation,
                    )
                    .await
                    .map_err(display)?;
                Ok(GitGenerationRef {
                    workspace_id: exact.value.workspace_id(),
                    generation: exact.value.id(),
                })
            }
        }
    }

    /// Enforce the compare-and-swap boundary owned by the live workspace.
    ///
    /// The compatibility repository records a `GitTreeRef` before dispatch,
    /// but that record is only useful when the host adapter checks it against
    /// its current workspace immediately before an effect.  Keep this check
    /// in the production adapter as well as in the generic state machine: a
    /// provider call may race with another writer after the state machine has
    /// validated its input.
    pub(crate) async fn validate_workspace_tree(
        &self,
        workspace_tree: GitTreeRef,
    ) -> Result<(), PluginGitError> {
        match workspace_tree {
            GitTreeRef::Exact(reference) => {
                if reference.workspace_id != self.current.id() {
                    return Err(Self::error(
                        "Git workspace belongs to another live workspace",
                    ));
                }
                let current = self.current.head().await.map_err(display)?;
                if current.id() != reference.generation {
                    // A conflicted local join publishes a new working
                    // generation while its durable transition still points
                    // at the pre-conflict target. Continue/abort owns that
                    // transition and must validate the live workspace again
                    // at its own dispatch boundary; do not reject it merely
                    // because the retained target is older.
                    if self
                        .distributed
                        .git(self.repository_id)
                        .pending_transition()
                        .await
                        .map_err(display)?
                        .is_some_and(|pending| {
                            matches!(pending.mutation, acyclic_fs::GitPendingMutation::Join { .. })
                        })
                    {
                        return Ok(());
                    }
                    return Err(Self::error(
                        "Git workspace generation changed before the operation",
                    ));
                }
            }
            GitTreeRef::Lazy(snapshot) => {
                if snapshot.workspace_id != self.current.id() {
                    return Err(Self::error(
                        "Git snapshot belongs to another live workspace",
                    ));
                }
                let current = self.lazy_current.snapshot().await.map_err(display)?;
                if current != snapshot {
                    return Err(Self::error(
                        "Git workspace snapshot changed before the operation",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn expected_workspace_generation(
        &self,
        expected_workspace_tree: Option<GitTreeRef>,
    ) -> Result<acyclic_fs::GenerationId, PluginGitError> {
        if let Some(expected_workspace_tree) = expected_workspace_tree {
            self.validate_workspace_tree(expected_workspace_tree)
                .await?;
            return Ok(expected_workspace_tree.authored_generation());
        }
        Ok(self.current.head().await.map_err(display)?.id())
    }
}

pub(crate) fn git_requires_exact_workspace(argv: &[String]) -> bool {
    if matches!(
        argv,
        [command, option]
            if matches!(command.as_str(), "merge" | "rebase")
                && matches!(option.as_str(), "--continue" | "--abort")
    ) {
        return false;
    }
    matches!(
        argv.first().map(String::as_str),
        Some(
            "status"
                | "diff"
                | "blame"
                | "grep"
                | "clean"
                | "archive"
                | "check-ignore"
                // Transitions retain their target tree in the durable Git
                // journal. Capture a complete authored snapshot before
                // preparing one so `--abort` can restore the exact target
                // after a conflict, including when the root began lazy.
                | "merge"
                | "rebase"
                | "reset"
                | "stash"
        )
    )
}

impl GitFilesystemExecutor for PluginGitExecutor<'_> {
    type Error = PluginGitError;

    async fn validate_workspace_tree(&self, workspace_tree: GitTreeRef) -> Result<(), Self::Error> {
        PluginGitExecutor::validate_workspace_tree(self, workspace_tree).await
    }

    async fn validate(&self) -> Result<(), Self::Error> {
        let Some(lease) = &self.lease else {
            return Ok(());
        };
        let snapshot = self
            .distributed
            .operations()
            .snapshot(lease.workspace_id)
            .await
            .map_err(display)?;
        let active = match snapshot.phase {
            acyclic_fs::OperationWindowPhase::Active { leases, .. } => {
                leases.get(&lease.lease_id).is_some_and(|active| {
                    active.expires_at_millis == lease.expires_at_millis
                        && active.expires_at_millis > now_millis()
                })
            }
            acyclic_fs::OperationWindowPhase::Idle
            | acyclic_fs::OperationWindowPhase::Reconciling { .. } => false,
        };
        if active {
            Ok(())
        } else {
            Err(Self::error("Git compatibility command lease expired"))
        }
    }

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        match action {
            GitFilesystemAction::CaptureCommit {
                workspace_tree,
                head_tree,
                head_workspace_tree,
                tracked_paths,
                ..
            } => {
                if matches!(workspace_tree, GitTreeRef::Lazy(_)) {
                    return Ok(GitFilesystemResult::Captured {
                        tree: *workspace_tree,
                        tracked_paths: tracked_paths.clone(),
                        proof: None,
                    });
                }
                let workspace_generation = self.exact(*workspace_tree, "commit").await?;
                if self.current.id() != workspace_generation.workspace_id
                    || self.current.head().await.map_err(display)?.id()
                        != workspace_generation.generation
                {
                    return Err(Self::error("Git commit workspace changed before capture"));
                }
                let captured = match (head_workspace_tree.as_ref(), head_tree) {
                    (Some(previous_live), Some(previous_capture)) => {
                        let previous_live =
                            self.exact(*previous_live, "incremental commit").await?;
                        let previous_capture =
                            self.exact(*previous_capture, "incremental commit").await?;
                        let previous_live = self
                            .workspace(previous_live.workspace_id)
                            .await?
                            .generation(previous_live.generation)
                            .await
                            .map_err(display)?;
                        let previous_capture = self
                            .workspace(previous_capture.workspace_id)
                            .await?
                            .generation(previous_capture.generation)
                            .await
                            .map_err(display)?;
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
                .map_err(display)?;
                let proof = captured
                    .authenticated_proof(operation_id, &self.distributed.lineage())
                    .await
                    .map_err(display)?;
                Ok(GitFilesystemResult::Captured {
                    tree: GitTreeRef::exact(
                        captured.generation.workspace_id(),
                        captured.generation.id(),
                    ),
                    tracked_paths: captured.tracked_paths,
                    proof,
                })
            }
            GitFilesystemAction::ForkBranch {
                branch,
                source_tree,
                switch,
                ..
            } => {
                let source_matches = match source_tree {
                    GitTreeRef::Exact(source) => {
                        self.current.id() == source.workspace_id
                            && self.current.head().await.map_err(display)?.id() == source.generation
                    }
                    GitTreeRef::Lazy(source) => {
                        self.lazy_current.snapshot().await.map_err(display)? == *source
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
                    .head()
                    .await
                    .map_err(display)?
                    .id();
                let workspace = self
                    .lazy_current
                    .fork(
                        destination,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    )
                    .await
                    .map_err(display)?;
                self.distributed
                    .lineage()
                    .register_existing_child(
                        self.lazy_current.workspace(),
                        workspace.workspace(),
                        fork_generation,
                    )
                    .await
                    .map_err(display)?;
                if *switch {
                    *self
                        .switched
                        .lock()
                        .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
                        Some(workspace.clone());
                }
                Ok(GitFilesystemResult::Forked {
                    workspace_id: workspace.workspace().id(),
                })
            }
            GitFilesystemAction::SwitchWorkspace { workspace_id } => {
                let workspace = self.workspace(*workspace_id).await?;
                let lazy_workspace = self
                    .lazy_current
                    .open_related(workspace.clone())
                    .await
                    .map_err(display)?;
                let tree = GitTreeRef::Lazy(lazy_workspace.snapshot().await.map_err(display)?);
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
            GitFilesystemAction::Diff {
                from,
                to,
                tracked_paths,
            } => {
                let to = self.exact(*to, "diff").await?;
                let to_workspace = self.workspace(to.workspace_id).await?;
                let to = to_workspace
                    .generation(to.generation)
                    .await
                    .map_err(display)?;
                let value = if let Some(from) = from {
                    let from = self.exact(*from, "diff").await?;
                    let from_workspace = self.workspace(from.workspace_id).await?;
                    let from = from_workspace
                        .generation(from.generation)
                        .await
                        .map_err(display)?;
                    let diff = git_compatible_diff_counts(
                        &from,
                        &to,
                        tracked_paths,
                        100_000,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(display)?;
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
            GitFilesystemAction::RestoreGeneration {
                tree,
                paths,
                expected_workspace_tree,
            } => {
                let current_id = self
                    .expected_workspace_generation(*expected_workspace_tree)
                    .await?;
                // A whole-workspace restore must preserve the live workspace
                // identity.  `exact` intentionally materializes a lazy
                // snapshot in a fresh child, which is correct for read-only
                // inspection and path restores but cannot be used as the
                // target of `restore_generation`.  When an abort/stash target
                // is a lazy snapshot of this workspace, exactify it as an
                // immutable source and apply its complete semantic delta back
                // to the current workspace.  A lazy snapshot owned by another
                // workspace remains a foreign whole-workspace restore and is
                // rejected before materialization.
                let lazy_whole_restore = match tree {
                    GitTreeRef::Lazy(snapshot) => {
                        if snapshot.workspace_id != self.current.id() && paths.is_none() {
                            return Err(Self::error(
                                "an exact workspace restore cannot use a foreign lazy snapshot",
                            ));
                        }
                        paths.is_none()
                    }
                    GitTreeRef::Exact(_) => false,
                };
                let source_ref = self.exact(*tree, "restore").await?;
                let source_workspace = self.workspace(source_ref.workspace_id).await?;
                let source = source_workspace
                    .generation(source_ref.generation)
                    .await
                    .map_err(display)?;
                let generation = if let Some(paths) = paths {
                    match self
                        .current
                        .restore_paths_from_with_permit(
                            &source,
                            &paths.iter().cloned().collect::<Vec<_>>(),
                            current_id,
                            IdempotencyKey::from_bytes(operation_id.into_bytes()),
                            self.permit,
                        )
                        .await
                        .map_err(display)?
                    {
                        TransactionCommit::Committed(generation)
                        | TransactionCommit::AlreadyCommitted(generation) => generation,
                        TransactionCommit::Conflict { .. } => {
                            return Err(Self::error(
                                "Git restore raced with another workspace writer",
                            ));
                        }
                        TransactionCommit::Fenced => {
                            return Err(Self::error("Git restore was fenced"));
                        }
                        TransactionCommit::IdempotencyConflict => {
                            return Err(Self::error("Git restore retry identity was reused"));
                        }
                    }
                } else {
                    if !lazy_whole_restore && source_ref.workspace_id != self.current.id() {
                        return Err(Self::error(
                            "an exact workspace restore cannot use a foreign generation",
                        ));
                    }
                    if lazy_whole_restore {
                        let current = self.current.head().await.map_err(display)?;
                        let changed = current
                            .diff_to(&source, u32::MAX)
                            .await
                            .map_err(display)?
                            .changed_paths(u32::MAX)
                            .await
                            .map_err(display)?;
                        let paths = changed
                            .iter()
                            .map(|change| {
                                if change.path.is_root() {
                                    return Ok("/".to_owned());
                                }
                                let mut path = String::new();
                                for component in change.path.components() {
                                    path.push('/');
                                    path.push_str(component.unicode_text().ok_or_else(|| {
                                        Self::error(
                                            "Git restore cannot represent a non-Unicode path",
                                        )
                                    })?.as_ref());
                                }
                                Ok(path)
                            })
                            .collect::<Result<Vec<_>, PluginGitError>>()?;
                        match self
                            .current
                            .restore_paths_from_with_permit(
                                &source,
                                &paths,
                                current_id,
                                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                                self.permit,
                            )
                            .await
                            .map_err(display)?
                        {
                            TransactionCommit::Committed(generation)
                            | TransactionCommit::AlreadyCommitted(generation) => generation,
                            TransactionCommit::Conflict { .. } => {
                                return Err(Self::error(
                                    "Git restore raced with another workspace writer",
                                ));
                            }
                            TransactionCommit::Fenced => {
                                return Err(Self::error("Git restore was fenced"));
                            }
                            TransactionCommit::IdempotencyConflict => {
                                return Err(Self::error("Git restore retry identity was reused"));
                            }
                        }
                    } else {
                        match self
                            .current
                            .restore_generation_with_permit(
                                &source,
                                current_id,
                                IdempotencyKey::from_bytes(operation_id.into_bytes()),
                                self.permit,
                            )
                            .await
                            .map_err(display)?
                        {
                            WorkspaceRestore::Restored(generation)
                            | WorkspaceRestore::AlreadyRestored(generation)
                            | WorkspaceRestore::Current(generation) => generation,
                            WorkspaceRestore::Stale(_) => {
                                return Err(Self::error(
                                    "Git restore raced with another workspace writer",
                                ));
                            }
                            WorkspaceRestore::Fenced => {
                                return Err(Self::error("Git restore was fenced"));
                            }
                            WorkspaceRestore::IdempotencyConflict => {
                                return Err(Self::error("Git restore retry identity was reused"));
                            }
                        }
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::RestorePaths {
                tree,
                paths,
                expected_workspace_tree,
            } => {
                let current_id = self
                    .expected_workspace_generation(*expected_workspace_tree)
                    .await?;
                let source_ref = self.exact(*tree, "restore").await?;
                let source_workspace = self.workspace(source_ref.workspace_id).await?;
                let source = source_workspace
                    .generation(source_ref.generation)
                    .await
                    .map_err(display)?;
                let outcome = self
                    .current
                    .restore_paths_from_with_permit(
                        &source,
                        paths,
                        current_id,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        self.permit,
                    )
                    .await
                    .map_err(display)?;
                let generation = match outcome {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation,
                    TransactionCommit::Conflict { .. } => {
                        return Err(Self::error(
                            "Git path restore raced with another workspace writer",
                        ));
                    }
                    TransactionCommit::Fenced => {
                        return Err(Self::error("Git path restore was fenced"));
                    }
                    TransactionCommit::IdempotencyConflict => {
                        return Err(Self::error("Git path restore retry identity was reused"));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::Join {
                source_workspace,
                source_tree,
                target_tree,
                rebase,
                tracked_paths,
                ..
            } => {
                let expected_generation = self
                    .expected_workspace_generation(Some(*target_tree))
                    .await?;
                let source_workspace_handle = self.workspace(*source_workspace).await?;
                let (source, source_head) = if let Some(source_tree) = source_tree {
                    match *source_tree {
                        GitTreeRef::Exact(source_reference) => {
                            let current_source =
                                source_workspace_handle.head().await.map_err(display)?;
                            if source_reference.workspace_id != *source_workspace
                                || current_source.id() != source_reference.generation
                            {
                                return Err(Self::error(
                                    "Git join source changed while preparing the provider plan",
                                ));
                            }
                            let source_head = source_workspace_handle
                                .generation(source_reference.generation)
                                .await
                                .map_err(display)?;
                            (source_workspace_handle.clone(), source_head)
                        }
                        GitTreeRef::Lazy(snapshot) => {
                            if snapshot.workspace_id != *source_workspace {
                                return Err(Self::error(
                                    "Git join source changed while preparing the provider plan",
                                ));
                            }
                            let source_lazy = self
                                .lazy_current
                                .open_related(source_workspace_handle.clone())
                                .await
                                .map_err(display)?;
                            let current_snapshot = source_lazy.snapshot().await.map_err(display)?;
                            if current_snapshot.workspace_id != snapshot.workspace_id
                                || current_snapshot.authored_generation
                                    != snapshot.authored_generation
                                || current_snapshot.source.identity != snapshot.source.identity
                                || current_snapshot.overlay != snapshot.overlay
                                || current_snapshot.shadows != snapshot.shadows
                            {
                                return Err(Self::error(
                                    "Git join source changed while preparing the provider plan",
                                ));
                            }
                            // The validated lazy snapshot is now materialized as an
                            // immutable provider input. Its exactified child has a
                            // distinct workspace identity by design.
                            let source_reference = self
                                .exact(GitTreeRef::Lazy(snapshot), "git join source")
                                .await?;
                            let exact_source =
                                self.workspace(source_reference.workspace_id).await?;
                            let source_head = exact_source
                                .generation(source_reference.generation)
                                .await
                                .map_err(display)?;
                            (exact_source, source_head)
                        }
                    }
                } else {
                    // An unborn compatibility branch has no commit tree to
                    // persist, but its SDK workspace still has an exact fork
                    // generation. Pin that generation so a concurrent source
                    // writer cannot be folded into this join implicitly.
                    let source_head = source_workspace_handle.head().await.map_err(display)?;
                    (source_workspace_handle, source_head)
                };
                let mut builder = source.join_into(&self.current);
                if *rebase {
                    builder = builder.history(acyclic_fs::JoinHistory::Rebase);
                }
                let target_head = self
                    .current
                    .generation(expected_generation)
                    .await
                    .map_err(display)?;
                let plan = builder
                    .plan_pinned(source_head, target_head)
                    .await
                    .map_err(display)?;
                if plan.target_head() != expected_generation {
                    return Err(Self::error(
                        "Git join target changed while preparing the provider plan",
                    ));
                }
                let options = ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes(operation_id.into_bytes()),
                };
                let initial = plan
                    .apply_with_permit(options, self.permit)
                    .await
                    .map_err(display)?;
                let mut projected_conflicts = None;
                let outcome = if let JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                } = initial
                {
                    let typed = plan
                        .describe_conflicts(&conflicts, truncated)
                        .await
                        .map_err(display)?;
                    let typed_json = serde_json::to_string(&typed).map_err(display)?;
                    let candidate = {
                        let mut cache = self.merge_cache.lock().await;
                        resolve_merge_plan(typed, &self.merge_drivers, &mut *cache, false).map_err(
                            |error| {
                                Self::error(format!(
                                    "Git join has unresolved typed conflicts: {typed_json}; {error}"
                                ))
                            },
                        )?
                    };
                    let has_markers = candidate.resolutions.values().any(|resolution| {
                        matches!(resolution, acyclic_fs::MergeResolution::Text(text)
                            if acyclic_fs::text_merge::has_conflict_markers(text))
                    });
                    let outcome = plan
                        .apply_candidate_with_permit(options, &candidate, self.permit)
                        .await
                        .map_err(display)?;
                    if has_markers {
                        projected_conflicts = Some(typed_json);
                    }
                    outcome
                } else {
                    initial
                };
                let generation = match outcome {
                    JoinOutcome::Applied(application)
                    | JoinOutcome::AlreadyApplied(application) => application.into_generation(),
                    JoinOutcome::NoChanges(generation) => generation,
                    JoinOutcome::StaleTarget(_) => {
                        return Err(Self::error("Git join raced with another workspace writer"));
                    }
                    JoinOutcome::Conflicted {
                        conflicts,
                        truncated,
                    } => {
                        return Err(Self::error(format!(
                            "Git join has {} conflict(s); truncated={truncated}",
                            conflicts.len()
                        )));
                    }
                    JoinOutcome::Fenced => return Err(Self::error("Git join was fenced")),
                    JoinOutcome::IdempotencyConflict => {
                        return Err(Self::error("Git join retry identity was reused"));
                    }
                };
                if let Some(conflicts) = projected_conflicts {
                    return Err(Self::error(format!(
                        "Git join projected unresolved text conflicts into the workspace: {conflicts}. Edit them, then run `acyclic git merge --continue`, or run `acyclic git merge --abort`"
                    )));
                }
                let resulting_tracked = existing_tracked_paths(&generation, tracked_paths).await?;
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: Some(resulting_tracked),
                })
            }
            GitFilesystemAction::ApplyCommit {
                base,
                source,
                paths,
                tracked_paths,
                expected_workspace_tree,
                ..
            } => {
                let expected_generation = self
                    .expected_workspace_generation(*expected_workspace_tree)
                    .await?;
                let base_workspace = match base {
                    Some(base) => {
                        let base = self.exact(*base, "apply commit").await?;
                        Some(self.workspace(base.workspace_id).await?)
                    }
                    None => None,
                };
                let base = match (base, base_workspace.as_ref()) {
                    (Some(base), Some(workspace)) => {
                        let base = self.exact(*base, "apply commit").await?;
                        Some(
                            workspace
                                .generation(base.generation)
                                .await
                                .map_err(display)?,
                        )
                    }
                    _ => None,
                };
                let source_workspace = match source {
                    Some(source) => {
                        let source = self.exact(*source, "apply commit").await?;
                        Some(self.workspace(source.workspace_id).await?)
                    }
                    None => None,
                };
                let source = match (source, source_workspace.as_ref()) {
                    (Some(source), Some(workspace)) => {
                        let source = self.exact(*source, "apply commit").await?;
                        Some(
                            workspace
                                .generation(source.generation)
                                .await
                                .map_err(display)?,
                        )
                    }
                    _ => None,
                };
                let outcome = self
                    .current
                    .apply_paths_from_with_permit(
                        base.as_ref(),
                        source.as_ref(),
                        &paths.iter().cloned().collect::<Vec<_>>(),
                        expected_generation,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        self.permit,
                    )
                    .await
                    .map_err(display)?;
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
                            .map_err(display)?,
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
            GitFilesystemAction::Archive { tree } => {
                let reference = self.exact(*tree, "archive").await?;
                let workspace = self.workspace(reference.workspace_id).await?;
                let generation = workspace
                    .generation(reference.generation)
                    .await
                    .map_err(display)?;
                let entries = walk_git_tree(&generation, None, 100_000)
                    .await
                    .map_err(display)?;
                Ok(GitFilesystemResult::Data {
                    kind: "archive".to_owned(),
                    value: json!({
                        "format": "acyclic-fs-manifest-v1",
                        "generation": hex::encode(generation.id().digest().as_bytes()),
                        "entries": entries
                    }),
                })
            }
            GitFilesystemAction::Grep {
                pattern,
                path,
                tree,
            } => {
                let reference = self.exact(*tree, "grep").await?;
                let workspace = self.workspace(reference.workspace_id).await?;
                let generation = workspace
                    .generation(reference.generation)
                    .await
                    .map_err(display)?;
                let result = grep_git_generation(
                    &generation,
                    pattern,
                    path.as_deref(),
                    100_000,
                    16 * 1024 * 1024,
                    100_000,
                )
                .await
                .map_err(display)?;
                Ok(GitFilesystemResult::Data {
                    kind: "grep".to_owned(),
                    value: serde_json::to_value(result).map_err(display)?,
                })
            }
            GitFilesystemAction::Blame { path, commits } => {
                let mut history = Vec::with_capacity(commits.len());
                for commit in commits {
                    let reference = self.exact(commit.tree, "blame").await?;
                    let workspace = self.workspace(reference.workspace_id).await?;
                    let generation = workspace
                        .generation(reference.generation)
                        .await
                        .map_err(display)?;
                    history.push((commit.clone(), generation));
                }
                let lines = blame_git_generations(&history, path, 16 * 1024 * 1024)
                    .await
                    .map_err(display)?;
                Ok(GitFilesystemResult::Data {
                    kind: "blame".to_owned(),
                    value: serde_json::to_value(lines).map_err(display)?,
                })
            }
            GitFilesystemAction::Clean {
                dry_run,
                tree,
                tracked_paths,
            } => {
                let reference = self.exact(*tree, "clean").await?;
                self.validate_workspace_tree(*tree).await?;
                let generation = self
                    .current
                    .generation(reference.generation)
                    .await
                    .map_err(display)?;
                let entries = walk_git_tree(&generation, None, 100_000)
                    .await
                    .map_err(display)?;
                let candidates: Vec<_> = entries
                    .into_iter()
                    .filter(|entry| {
                        entry.kind != "directory"
                            && !tracked_paths.contains(&entry.path)
                            && self.ignore.eligible(&entry.path, false, false)
                    })
                    .map(|entry| entry.path)
                    .collect();
                if *dry_run || candidates.is_empty() {
                    return Ok(GitFilesystemResult::Data {
                        kind: "clean".to_owned(),
                        value: json!({ "paths": candidates, "dryRun": dry_run }),
                    });
                }
                let mut transaction = self
                    .current
                    .begin_transaction_if_current(
                        &generation,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    )
                    .await
                    .map_err(display)?;
                for path in &candidates {
                    transaction
                        .remove(&format!("/{path}"))
                        .await
                        .map_err(display)?;
                }
                let generation = match transaction
                    .commit_with_permit(self.permit)
                    .await
                    .map_err(display)?
                {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation,
                    TransactionCommit::Conflict { .. } => {
                        return Err(Self::error("Git clean raced with another workspace writer"));
                    }
                    TransactionCommit::Fenced => return Err(Self::error("Git clean was fenced")),
                    TransactionCommit::IdempotencyConflict => {
                        return Err(Self::error("Git clean retry identity was reused"));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::ApplyPatch {
                patch,
                expected_workspace_tree,
            } => {
                let expected_generation = self
                    .expected_workspace_generation(*expected_workspace_tree)
                    .await?;
                let expected = self
                    .current
                    .generation(expected_generation)
                    .await
                    .map_err(display)?;
                let generation = match apply_git_patch_with_permit_if_current(
                    &self.current,
                    &expected,
                    patch,
                    IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    self.permit,
                )
                .await
                .map_err(display)?
                {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => generation,
                    TransactionCommit::Conflict { .. } => {
                        return Err(Self::error(
                            "Git patch application raced with another workspace writer",
                        ));
                    }
                    TransactionCommit::Fenced => {
                        return Err(Self::error("Git patch application was fenced"));
                    }
                    TransactionCommit::IdempotencyConflict => {
                        return Err(Self::error(
                            "Git patch application retry identity was reused",
                        ));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::CheckIgnore { paths, tree } => {
                let reference = self.exact(*tree, "check-ignore").await?;
                if reference.workspace_id != self.current.id()
                    || self.current.head().await.map_err(display)?.id() != reference.generation
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
            unsupported => Err(Self::error(format!(
                "Git filesystem action is not yet available in the Codex adapter: {unsupported:?}"
            ))),
        }
    }
}

pub(crate) struct RootMaterializingGitExecutor<'a> {
    pub(crate) inner: PluginGitExecutor<'a>,
    pub(crate) root: &'a Path,
    pub(crate) store: &'a LocalCoreStateStore,
}

impl RootMaterializingGitExecutor<'_> {
    pub(crate) fn switched_workspace(&self) -> Result<Option<LocalLazyWorkspace>, PluginGitError> {
        self.inner.switched_workspace()
    }

    pub(crate) async fn materialize(
        &self,
        operation_id: OperationId,
        from: LocalGeneration,
    ) -> Result<(), PluginGitError> {
        let target = self.inner.switched_workspace()?.map_or_else(
            || self.inner.current.clone(),
            |lazy| lazy.workspace().clone(),
        );
        let to = target.head().await.map_err(display)?.id();
        if from.id() == to {
            return Ok(());
        }
        let operation_directory =
            root_materialization_directory(self.root, operation_id).map_err(PluginGitError)?;
        let options = MaterializeOptions {
            destination: operation_directory.join("target"),
            maximum_directory_entries: 4_096,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        };
        let cancellation = CancellationToken::new();
        let to_generation = target.head().await.map_err(display)?;
        acyclic_fs::publish_native_generation_transition(
            &from,
            &to_generation,
            self.store,
            acyclic_fs::NativeWorkspacePublication {
                root: self.root,
                operation_directory: &operation_directory,
                operation_id,
                from: from.id(),
                to,
                excluded_names: &[".git"],
                options: &options,
                budget: WorkBudget::UNBOUNDED,
                cancellation: &cancellation,
            },
        )
        .await
        .map_err(display)?;
        self.store
            .remove_materialization_async(operation_id)
            .await
            .map_err(display)?;
        remove_tree_checked(
            &root_materialization_root(self.root).map_err(PluginGitError)?,
            &operation_directory,
        )
        .map_err(display)?;
        Ok(())
    }
}

impl GitFilesystemExecutor for RootMaterializingGitExecutor<'_> {
    type Error = PluginGitError;

    async fn validate_workspace_tree(&self, workspace_tree: GitTreeRef) -> Result<(), Self::Error> {
        PluginGitExecutor::validate_workspace_tree(&self.inner, workspace_tree).await
    }

    async fn validate(&self) -> Result<(), Self::Error> {
        self.inner.validate().await
    }

    async fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        let from = self.inner.current.head().await.map_err(display)?;
        let result = self.inner.execute(operation_id, action).await;
        // Conflict projection is an intentional state transition: the join
        // executor leaves the Git transition pending and returns an error so
        // the caller can present the typed conflicts. Materialize that new
        // generation before propagating the error, otherwise the checkout
        // would never expose the files the user must edit.
        let materialized = self.materialize(operation_id, from).await;
        match (result, materialized) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), Ok(())) | (_, Err(error)) => Err(error),
        }
    }
}

pub(crate) fn git_transition_command(argv: &[String]) -> bool {
    matches!(
        argv,
        [command, option] if command == "merge" && matches!(option.as_str(), "--continue" | "--abort")
    ) || argv.first().is_some_and(|command| command == "add")
}

pub(crate) async fn run_git_command<E: GitFilesystemExecutor>(
    repository: &GitCompatRepository<LocalCoreStateStore>,
    argv: &[String],
    workspace_tree: GitTreeRef,
    patch: Option<&[u8]>,
    author: &str,
    authored_at_seconds: i64,
    executor: &E,
) -> Result<Option<GitCommandOutput>, String> {
    if !git_transition_command(argv)
        && repository
            .resume(executor)
            .await
            .map_err(display)?
            .is_some()
    {
        return Ok(None);
    }
    let output = if let Some(patch) = patch {
        repository
            .run(
                GitCommand::Apply {
                    patch: patch.to_vec(),
                },
                workspace_tree,
                executor,
            )
            .await
    } else {
        repository
            .run_argv(argv, workspace_tree, author, authored_at_seconds, executor)
            .await
    };
    output.map(Some).map_err(display)
}

pub(crate) async fn git_apply_patch(
    workspace: &LocalLazyWorkspace,
    root: &Path,
    argv: &[String],
) -> Result<Option<Vec<u8>>, String> {
    if argv.first().is_none_or(|command| command != "apply") {
        return Ok(None);
    }
    let [_, patch_path] = argv else {
        return Err("acyclic git apply requires exactly one patch file".to_owned());
    };
    if patch_path == "-" {
        return Err(
            "acyclic git apply does not accept stdin through the service; pass a patch file"
                .to_owned(),
        );
    }
    let patch_path = workspace_path_argument(root, patch_path)?;
    workspace
        .read(&patch_path, 16 * 1024 * 1024)
        .await
        .map(|bytes| Some(bytes.to_vec()))
        .map_err(display)
}

pub(crate) async fn git_workspace_tree(
    workspace: &LocalLazyWorkspace,
    argv: &[String],
    permit: Option<PublicationPermit>,
) -> Result<GitTreeRef, String> {
    if !git_requires_exact_workspace(argv) {
        return workspace
            .snapshot()
            .await
            .map(GitTreeRef::Lazy)
            .map_err(display);
    }
    let exact = match permit {
        Some(permit) => {
            workspace
                .exactify_with_permit(WorkBudget::UNBOUNDED, &CancellationToken::new(), permit)
                .await
        }
        None => {
            workspace
                .exactify(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
        }
    }
    .map_err(display)?;
    Ok(GitTreeRef::exact(
        workspace.workspace().id(),
        exact.value.id(),
    ))
}

pub(crate) async fn git_policy(
    workspace: &LocalLazyWorkspace,
) -> Result<(GitIgnorePolicy, Arc<MergeDriverRegistry>), String> {
    Ok((
        git_ignore_policy(workspace).await?,
        merge_drivers_for(workspace).await?,
    ))
}

pub(crate) async fn git_ignore_policy(
    workspace: &LocalLazyWorkspace,
) -> Result<GitIgnorePolicy, String> {
    let ignore_text = match workspace.read("/.gitignore", 1024 * 1024).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(display(error)),
    };
    Ok(GitIgnorePolicy::parse(&format!(
        "{ignore_text}\n{}",
        reserved_ignore_rules()
    )))
}

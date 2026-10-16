use super::*;

impl GitFilesystemExecutor for WorkspaceGitExecutor<'_> {
    type Error = Error;

    async fn validate(&self) -> Result<(), Self::Error> {
        let Some(lease) = &self.lease else {
            return Ok(());
        };
        let snapshot = OperationWindowCoordinator::new(self.fs.operation_window_store())
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
                        .map_err(display)?
                        .id()
                        != workspace_generation.id()
                {
                    return Err(Self::error("Git commit workspace changed before capture"));
                }
                let captured = match (head_workspace_tree.as_ref(), head_tree) {
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
                .map_err(display)?;
                let proof = captured
                    .authenticated_proof(operation_id, &WorkspaceGraph::new(self.store.clone()))
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
                            && self
                                .current
                                .head(
                                    acyclic_fs::WorkBudget::UNBOUNDED,
                                    &acyclic_fs::CancellationToken::new(),
                                )
                                .await
                                .map(|receipt| receipt.value)
                                .map_err(|failure| failure.error)
                                .map_err(display)?
                                .id()
                                == source.generation
                    }
                    GitTreeRef::Lazy(source) => {
                        self.lazy_current
                            .snapshot(
                                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                            )
                            .await
                            .map_err(display)?
                            == *source
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
                    .map_err(display)?
                    .id();
                let workspace = self
                    .lazy_current
                    .fork(
                        &destination,
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(display)?
                    .0;
                WorkspaceGraph::new(self.store.clone())
                    .register_existing_child(
                        self.lazy_current.workspace(),
                        workspace.workspace(),
                        fork_generation,
                    )
                    .await
                    .map_err(display)?;
                let workspace_id = workspace.workspace().id();
                if *switch {
                    *self
                        .switched
                        .lock()
                        .map_err(|_| Self::error("Git workspace switch lock is unavailable"))? =
                        Some(workspace);
                }
                Ok(GitFilesystemResult::Forked { workspace_id })
            }
            GitFilesystemAction::SwitchWorkspace { workspace_id } => {
                let workspace = self.workspace(*workspace_id).await?;
                let lazy_workspace = self
                    .lazy_current
                    .open_related(
                        workspace.clone(),
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .await
                    .map_err(display)?;
                let tree = GitTreeRef::Lazy(
                    lazy_workspace
                        .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
                        .await
                        .map_err(display)?,
                );
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
                let to = self.generation(*to).await?;
                let value = if let Some(from) = from {
                    let from = self.generation(*from).await?;
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
            GitFilesystemAction::RestoreGeneration { tree, paths } => {
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
                let current = self
                    .current
                    .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(display)?;
                let generation = match self
                    .current
                    .restore_generation(
                        &source,
                        current.id(),
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
                    WorkspaceRestore::Fenced => return Err(Self::error("Git restore was fenced")),
                    WorkspaceRestore::IdempotencyConflict => {
                        return Err(Self::error("Git restore retry identity was reused"));
                    }
                };
                Ok(GitFilesystemResult::Applied {
                    tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
                    tracked_paths: None,
                })
            }
            GitFilesystemAction::RestorePaths { tree, paths } => {
                self.restore_paths(operation_id, *tree, paths).await
            }
            GitFilesystemAction::Join {
                source_workspace,
                rebase,
                tracked_paths,
                ..
            } => {
                let source = self.workspace(*source_workspace).await?;
                let mut builder = source.join_into(&self.current);
                if *rebase {
                    builder = builder.history(acyclic_fs::JoinHistory::Rebase);
                }
                let plan = builder
                    .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| display(failure.error))?;
                let options = ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::from_bytes(operation_id.into_bytes()),
                };
                let initial = plan
                    .apply(options, BTreeMap::new(), self.permit)
                    .await
                    .map_err(display)?;
                let mut projected_conflicts = None;
                let outcome = if let JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                } = initial
                {
                    let typed = plan
                        .describe_conflicts(conflicts, truncated)
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
                        .apply_candidate(options, &candidate, self.permit)
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
                    | JoinOutcome::AlreadyApplied(application) => application.generation,
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
                ..
            } => {
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
                    .map_err(display)?;
                let outcome = self
                    .current
                    .apply_paths_from(
                        base.as_ref(),
                        source.as_ref(),
                        &paths.iter().cloned().collect::<Vec<_>>(),
                        current.id(),
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
                let generation = self.generation(*tree).await?;
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
                let generation = self.generation(*tree).await?;
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
                    let generation = self.generation(commit.tree).await?;
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
                let generation = self.generation(*tree).await?;
                if generation.workspace_id() != self.current.id() {
                    return Err(Self::error(
                        "Git clean generation is not the live workspace",
                    ));
                }
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
                    .begin_transaction(
                        IdempotencyKey::from_bytes(operation_id.into_bytes()),
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(display)?;
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
                        .map_err(display)?;
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
                        .map_err(display)?,
                )
            }
            GitFilesystemAction::ApplyPatch { patch } => committed(
                apply_git_patch(
                    &self.current,
                    patch,
                    IdempotencyKey::from_bytes(operation_id.into_bytes()),
                    self.permit,
                )
                .await
                .map_err(display)?,
            ),
            GitFilesystemAction::CheckIgnore { paths, tree } => {
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
                        .map_err(display)?
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
            unsupported => Err(Self::error(format!(
                "Git filesystem action is not yet available in the Codex adapter: {unsupported:?}"
            ))),
        }
    }
}

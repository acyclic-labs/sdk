use super::*;

/// Exact ignore-aware snapshot produced for one compatibility commit.
pub struct GitCapturedGeneration<A, O> {
    /// Immutable generation containing only compatibility-eligible paths.
    pub generation: Generation<A, O>,
    /// Initial generation of the capture workspace, when capture forked.
    pub(super) initial_generation: Option<crate::GenerationId>,
    /// Eligible non-directory paths represented by the snapshot.
    pub tracked_paths: BTreeSet<String>,
    pub(super) fork_parent: Option<Generation<A, O>>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> GitCapturedGeneration<A, O> {
    /// Issues a capture proof only after the SDK has durably recorded its
    pub async fn authenticated_proof<S: crate::WorkspaceLineageStore>(
        &self,
        operation_id: OperationId,
        graph: &crate::WorkspaceGraph<S>,
    ) -> Result<Option<GitCaptureProof>, GitCaptureAuthenticationError<S::Error>> {
        let proof = self.verified_proof(operation_id).await?;
        if let (Some(parent), Some(initial_generation)) = (&self.fork_parent, self.initial_generation) {
            graph
                .register_existing_child_at_generation(
                    &parent.workspace(),
                    &self.generation.workspace(),
                    parent.id(),
                    initial_generation,
                )
                .await?;
        }
        Ok(proof)
    }

    /// Authenticates this filtered snapshot against its original SDK fork.
    pub(super) async fn verified_proof(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<GitCaptureProof>, GitCaptureError> {
        let (parent, initial_generation) = match (&self.fork_parent, self.initial_generation) {
            (Some(parent), Some(initial_generation)) => (parent, initial_generation),
            (None, None) => return Ok(None),
            _ => return Err(GitCaptureError::Conflict),
        };
        let child = self.generation.workspace();
        let expected_suffix = format!("-{}", hex::encode(operation_id.into_bytes()));
        if !child.name().as_str().starts_with("git-capture-") || !child.name().as_str().ends_with(&expected_suffix) {
            return Err(GitCaptureError::Conflict);
        }
        let expected_child =
            parent.workspace().volume.fs.workspace_id(child.name().as_str()).map_err(WorkspaceError::from)?;
        if child.id() != expected_child {
            return Err(GitCaptureError::Conflict);
        }
        let initial = child
            .generation(initial_generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        if initial
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?
            .as_slice()
            != [parent.id()]
        {
            return Err(GitCaptureError::Conflict);
        }
        let result_parents = self
            .generation
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        if self.generation.id() != initial_generation && result_parents.as_slice() != [initial_generation] {
            return Err(GitCaptureError::Conflict);
        }
        Ok(Some(GitCaptureProof {
            fork_parent: GitTreeRef::exact(parent.workspace_id(), parent.id()),
            initial_generation,
            operation_id,
        }))
    }
}

/// Failure while producing an ignore-aware compatibility snapshot.
#[derive(Debug, Error)]
pub enum GitCaptureError {
    /// Workspace traversal, mutation, or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// A host-native name cannot be represented by Git's portable UTF-8 paths.
    #[error("Git compatibility cannot represent a non-UTF-8 path name")]
    NonPortableName,
    /// A deterministic capture workspace was concurrently changed.
    #[error("Git compatibility capture workspace changed concurrently")]
    Conflict,
}

/// Failure to authenticate and durably register one filtered Git capture.
#[derive(Debug, Error)]
pub enum GitCaptureAuthenticationError<E: std::error::Error + 'static> {
    /// The immutable captured generation did not match its SDK fork.
    #[error(transparent)]
    Capture(#[from] GitCaptureError),
    /// The direct-parent lineage could not be durably registered.
    #[error(transparent)]
    Lineage(#[from] crate::WorkspaceLineageError<E>),
}

pub(super) async fn load_git_ignore_policy<A, O>(
    generation: &Generation<A, O>,
    policies: &mut BTreeMap<String, GitIgnorePolicy>,
    directory: &str,
) -> Result<(), GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    if policies.contains_key(directory) {
        return Ok(());
    }
    let path = if directory.is_empty() { "/.gitignore".to_owned() } else { format!("/{directory}/.gitignore") };
    let policy = match generation
        .read(&path, 1024 * 1024, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    {
        Ok(contents) => {
            let contents = std::str::from_utf8(&contents).map_err(|_| GitCaptureError::NonPortableName)?;
            GitIgnorePolicy::parse(contents)
        }
        Err(WorkspaceError::NotFound) => GitIgnorePolicy::default(),
        Err(error) => return Err(error.into()),
    };
    policies.insert(directory.to_owned(), policy);
    Ok(())
}

pub(super) async fn scan_git_capture_entries<A, O>(
    live: &crate::Generation<A, O>,
) -> Result<(Vec<(String, bool)>, BTreeMap<String, GitIgnorePolicy>), GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let mut entries = Vec::new();
    let mut nested_policies = BTreeMap::new();
    let mut pending = vec!["/".to_owned()];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let page = live
                .list_directory(&directory, after.as_ref(), 1_024, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map(|receipt| receipt.value.0)
                .map_err(|failure| failure.error)?;
            for entry in &page.entries {
                let name = entry.name.unicode_text().ok_or(GitCaptureError::NonPortableName)?;
                let path = if directory == "/" {
                    name.to_string()
                } else {
                    format!("{}/{}", directory.trim_start_matches('/'), name)
                };
                let is_directory = entry.kind == FileKind::Directory;
                entries.push((path.clone(), is_directory));
                if is_directory {
                    pending.push(format!("/{path}"));
                } else if name == ".gitignore" && directory != "/" {
                    let contents = live
                        .read(&format!("/{path}"), 1024 * 1024, WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)?;
                    let contents = std::str::from_utf8(&contents).map_err(|_| GitCaptureError::NonPortableName)?;
                    nested_policies
                        .insert(directory.trim_start_matches('/').to_owned(), GitIgnorePolicy::parse(contents));
                }
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    Ok((entries, nested_policies))
}

/// Captures the exact eligible compatibility tree without changing the live
pub async fn capture_git_compatible_generation<A, O>(
    workspace: &Workspace<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    capture_git_compatible_generation_from(live, policy, already_tracked, operation_id).await
}

/// Captures an ignore-aware compatibility tree from one exact immutable
pub async fn capture_git_compatible_generation_at<A, O>(
    workspace: &Workspace<A, O>,
    generation: GenerationId,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace
        .generation(generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    capture_git_compatible_generation_from(live, policy, already_tracked, operation_id).await
}

pub(super) async fn capture_git_compatible_generation_from<A, O>(
    live: crate::Generation<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let (mut entries, nested_policies) = scan_git_capture_entries(&live).await?;
    entries.sort_by(|left, right| path_depth(&left.0).cmp(&path_depth(&right.0)).then_with(|| left.0.cmp(&right.0)));
    let mut excluded = Vec::<String>::new();
    let mut tracked_paths = BTreeSet::new();
    for (path, is_directory) in &entries {
        let parent_excluded = excluded.iter().any(|parent| is_path_below(path, parent));
        let tracked = already_tracked.contains(path);
        let tracked_descendant =
            *is_directory && already_tracked.iter().any(|candidate| is_path_below(candidate, path));
        if parent_excluded
            || (!git_path_eligible(policy, &nested_policies, path, *is_directory, tracked) && !tracked_descendant)
        {
            excluded.push(path.clone());
        } else if !is_directory {
            tracked_paths.insert(path.clone());
        }
    }
    if excluded.is_empty() {
        return Ok(GitCapturedGeneration {
            generation: live,
            initial_generation: None,
            tracked_paths,
            fork_parent: None,
        });
    }
    apply_git_capture(&live, Some(&live), None, &excluded, tracked_paths, operation_id).await
}

/// Incrementally captures only paths changed since the preceding live
#[allow(clippy::too_many_lines, reason = "eligibility and the single atomic path application form one operation")]
pub async fn capture_git_compatible_generation_incremental<A, O>(
    workspace: &Workspace<A, O>,
    previous_live: &Generation<A, O>,
    previous_capture: &Generation<A, O>,
    policy: &GitIgnorePolicy,
    already_tracked: &BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let live = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let changes = previous_live
        .diff_to(&live, u32::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let changed = changes
        .changed_paths(u32::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value.0)
        .map_err(|failure| failure.error)?;
    let mut portable = Vec::with_capacity(changed.len());
    for change in changed {
        let path = portable_changed_path(&change.path)?;
        if path.rsplit('/').next() == Some(".gitignore") {
            return capture_git_compatible_generation(workspace, policy, already_tracked, operation_id).await;
        }
        portable.push((path, change.before, change.after));
    }

    let mut eligible = Vec::new();
    let mut tracked_paths = already_tracked.clone();
    let mut before_policies = BTreeMap::new();
    let mut after_policies = BTreeMap::new();
    for (path, before, after) in portable {
        let kind = after.or(before).map(|record| record.kind);
        let is_directory = kind == Some(FileKind::Directory);
        let tracked = already_tracked.contains(&path);
        let tracked_descendant =
            is_directory && already_tracked.iter().any(|candidate| is_path_below(candidate, &path));
        let (generation, policies) =
            if after.is_some() { (&live, &mut after_policies) } else { (previous_live, &mut before_policies) };
        let allowed =
            git_path_eligible_with_root_at(generation, policies, policy, &path, is_directory, already_tracked).await?;
        if tracked || tracked_descendant || allowed {
            eligible.push(path.clone());
        }
        if !is_directory {
            if after.is_some() && (tracked || allowed) {
                tracked_paths.insert(path);
            } else {
                tracked_paths.remove(&path);
            }
        }
    }
    if eligible.is_empty() {
        return Ok(GitCapturedGeneration {
            generation: previous_capture.clone(),
            initial_generation: None,
            tracked_paths,
            fork_parent: None,
        });
    }

    apply_git_capture(previous_capture, Some(previous_live), Some(&live), &eligible, tracked_paths, operation_id).await
}

async fn apply_git_capture<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    parent: &Generation<A, O>,
    base: Option<&Generation<A, O>>,
    source: Option<&Generation<A, O>>,
    paths: &[String],
    tracked_paths: BTreeSet<String>,
    operation_id: OperationId,
) -> Result<GitCapturedGeneration<A, O>, GitCaptureError> {
    let bytes = operation_id.into_bytes();
    let workspace_hash = hex::encode(&parent.workspace_id().into_bytes()[..6]);
    let capture = parent
        .workspace()
        .fork(
            format!("git-capture-{workspace_hash}-{}", hex::encode(bytes)),
            crate::ForkOptions::from_generation(parent.clone(), IdempotencyKey::from_bytes(bytes)),
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            |_| {
                std::future::ready(Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let mut apply_bytes = bytes;
    apply_bytes[0] ^= 0xa5;
    let apply_key = IdempotencyKey::from_bytes(apply_bytes);
    let replay = capture
        .operation_generation(apply_key, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    let (generation, initial_generation) = if let Some(generation) = replay {
        let parents = generation
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let [initial_generation] = parents.as_slice() else {
            return Err(GitCaptureError::Conflict);
        };
        (generation, *initial_generation)
    } else {
        let head = capture
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        match capture
            .apply_paths_from(base, source, paths, head.id(), apply_key, crate::PublicationPermit::Unrestricted)
            .await?
        {
            crate::WorkspacePathApply::Applied(generation) => (generation, head.id()),
            crate::WorkspacePathApply::AlreadyApplied(generation) => {
                let parents = generation
                    .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)?;
                let [initial_generation] = parents.as_slice() else {
                    return Err(GitCaptureError::Conflict);
                };
                (generation, *initial_generation)
            }
            crate::WorkspacePathApply::NoChanges(generation) => (generation, head.id()),
            crate::WorkspacePathApply::Conflicted(_)
            | crate::WorkspacePathApply::Stale(_)
            | crate::WorkspacePathApply::Fenced
            | crate::WorkspacePathApply::IdempotencyConflict => return Err(GitCaptureError::Conflict),
        }
    };
    generation
        .pin(format!("capture-{}", hex::encode(bytes)), WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    Ok(GitCapturedGeneration {
        generation,
        initial_generation: Some(initial_generation),
        tracked_paths,
        fork_parent: Some(parent.clone()),
    })
}

pub(super) fn portable_changed_path(path: &crate::kernel::NamespacePath) -> Result<String, GitCaptureError> {
    let mut portable = String::new();
    for component in path.components() {
        if !portable.is_empty() {
            portable.push('/');
        }
        portable.push_str(&component.unicode_text().ok_or(GitCaptureError::NonPortableName)?);
    }
    Ok(portable)
}

pub(super) fn path_depth(path: &str) -> usize {
    path.split('/').count()
}

pub(super) fn is_path_below(candidate: &str, parent: &str) -> bool {
    candidate.strip_prefix(parent).is_some_and(|suffix| suffix.starts_with('/'))
}

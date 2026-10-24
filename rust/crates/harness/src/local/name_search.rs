//! Search requests and bounded result publications are ordinary followed files.
use super::*;
use crate::runtime::TaskStateProvider as _;
use crate::task_volume::{ChangeStream, FileKey, FileValue, Query, TaskFiles};
use crate::tools::git::View;
use crate::tui_files::NameSearchRequest;
use acyclic_fs::access::Action;
use futures::StreamExt as _;

impl LocalHarness {
    pub(crate) async fn bind_name_search(
        &self,
        completion: &mut crate::tui_files::CompletionFiles,
        input: &mut crate::tui::PromptInput,
    ) -> Result<()> {
        let Some(workspace) = self.workspace().cloned() else {
            return Ok(());
        };
        let files = Arc::clone(self.task_files());
        // Mount policies describe the host's view. The client must also hold
        // the signed content grant; a host-owned auxiliary mount alone must
        // never make another agent's filenames visible in this task.
        self.verifier.verify(&self.signed, &mut None)?;
        let verifier = self.verifier.clone();
        let signed = self.signed.clone();
        let runtime = Arc::clone(&self.runtime);
        let key = FileKey::parse("search/query.json").map_err(storage)?;
        let observation = files
            .inner
            .observe(Query::File(key), None, 1)
            .await
            .map_err(storage)?;
        let initial = observation
            .snapshot
            .and_then(|snapshot| snapshot.files.into_iter().next());
        let writer = Arc::clone(&files);
        let worker = tokio::spawn(async move {
            let mut changes = observation.changes;
            let mut query = initial
                .map(|file| serde_json::from_slice::<Option<NameSearchRequest>>(&file.bytes))
                .transpose()
                .map_err(storage)?
                .flatten();
            loop {
                let Some(active) = query.take() else {
                    query = request(&mut changes).await?;
                    continue;
                };
                let search = search(&workspace, &runtime, &writer, &active, &verifier, &signed);
                tokio::pin!(search);
                tokio::select! {
                    biased;
                    newer = request(&mut changes) => { query = newer?; }
                    done = &mut search => {
                        if done.is_err() {
                            // A failed source, admission or policy read cannot
                            // certify an empty namespace. Clear its candidates
                            // and end this request visibly partial; retain the
                            // query follower so another request can recover.
                            results(&writer, &active, &crate::tui::PathMatches::new(&active.query), 0, true, 1).await?;
                        }
                        query = request(&mut changes).await?;
                    }
                }
            }
        });
        completion.bind_search(input, files, worker)?;
        Ok(())
    }
}

async fn request(changes: &mut ChangeStream) -> Result<Option<NameSearchRequest>> {
    loop {
        let change = changes
            .next()
            .await
            .ok_or_else(|| Error::Storage("name query follow ended".into()))?
            .map_err(storage)?;
        if let Some(file) = change.file {
            return serde_json::from_slice(&file.bytes).map_err(storage);
        }
        if !change.removed.is_empty() {
            return Ok(None);
        }
    }
}

async fn publish(
    files: &TaskFiles<LocalStream>,
    path: &str,
    value: &impl serde::Serialize,
) -> Result<()> {
    let key = FileKey::parse(path).map_err(storage)?;
    let bytes = serde_json::to_vec(value).map_err(storage)?;
    loop {
        let snapshot = files
            .inner
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        match files
            .inner
            .append(
                snapshot.generation,
                &FileValue::new(key.clone(), bytes.clone()),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
        }
    }
}

async fn results(
    files: &TaskFiles<LocalStream>,
    request: &NameSearchRequest,
    best: &crate::tui::PathMatches,
    pending: usize,
    complete: bool,
    unavailable: usize,
) -> Result<()> {
    publish(
        files,
        "search/results.json",
        &serde_json::json!({"request":request.request,"query":request.query,"paths":best.paths().collect::<Vec<_>>()}),
    )
    .await?;
    publish(
        files,
        "search/progress.json",
        &serde_json::json!({"request":request.request,"query":request.query,"pending_dirs":pending,"complete":complete,"unavailable_entries":unavailable}),
    )
    .await
}

async fn search(
    workspace: &LocalWorkspace,
    runtime: &LocalTaskRuntime,
    files: &TaskFiles<LocalStream>,
    request: &NameSearchRequest,
    verifier: &AuthorityVerifier,
    signed: &Scope,
) -> Result<()> {
    let query = request.query.as_str();
    // The UI initially selects its authenticated owner, which names the
    // conversation's project before any execution admission exists. Only
    // that exact signed audience uses the project view directly; arbitrary
    // selected task IDs must still resolve an admission and its grants.
    let task = request.task.filter(|task| {
        let owner = verifier.audience();
        owner.kind != AggregateKind::Task || owner.id != task.to_string()
    });
    let admitted = match task {
        Some(task) => Some(runtime.task_host().observe_admission(task).await?),
        None => None,
    };
    let task_grants = admitted.as_ref().map(|record| &record.grants);
    let best = crate::tui::PathMatches::new(query);
    results(files, request, &best, 1, false, 0).await?;
    // Keep the selected branch alive for the complete query. Each mounted
    // volume is searched through its logical view and its own access policy.
    let selected = selected_view(workspace, runtime, task).await?;
    let mut volumes = Vec::new();
    for (prefix, mount) in workspace.mounts.mounts() {
        let view = if prefix.is_empty() {
            View::Checkout(selected.as_ref().unwrap_or(&workspace.view))
        } else if let Some(source) = &mount.view.source {
            source.workspace.view()
        } else {
            continue;
        };
        let names = match crate::tools::git::IndexedNames::open(view).await {
            Ok(names) => {
                names.prefer_directory_names(query)?;
                Some(names)
            }
            Err(_) => None,
        };
        let complete = names.is_none();
        volumes.push(VolumeSearch {
            names,
            prefix,
            mount,
            best: crate::tui::PathMatches::new(query),
            complete,
        });
    }
    if volumes.iter().all(|volume| volume.names.is_none()) {
        return results(files, request, &best, 0, true, volumes.len()).await;
    }
    let mut last = None;
    loop {
        // Poll all mounted indexes, including completed ones. An incomplete
        // large primary volume cannot starve an already indexed auxiliary
        // volume; a completed popup remains subscribed to later file changes.
        let (index, page) = {
            let mut next = futures::stream::FuturesUnordered::new();
            for (index, volume) in volumes.iter_mut().enumerate() {
                let complete = volume.complete;
                let Some(names) = &mut volume.names else {
                    continue;
                };
                next.push(async move {
                    let page = async {
                        if complete {
                            names.changed().await?;
                        }
                        names.next_page().await
                    }
                    .await;
                    (index, page)
                });
            }
            let Some(event) = next.next().await else {
                return Ok(());
            };
            event
        };
        verifier.verify(signed, &mut None)?;
        let volume = &mut volumes[index];
        let page = match page {
            Ok(page) => page,
            Err(_) => {
                // A failed source cannot certify an empty volume. Retain
                // healthy mounts, clear this source's stale names and report
                // one unavailable source until the next query retries it.
                volume.names = None;
                volume.best = crate::tui::PathMatches::new(query);
                None
            }
        };
        volume.complete = page.is_none();
        if let Some(page) = page {
            if page.reset {
                volume.best = crate::tui::PathMatches::new(query);
            }
            for name in page.names.into_iter().filter(|name| name.regular) {
                let now = acyclic_stream::UnixMillisClock::now_unix_millis(
                    &acyclic_stream::SystemUnixMillisClock,
                ) / 1000;
                let parent = name.path.rsplit_once('/').map_or("", |(parent, _)| parent);
                if volume.mount.check(Action::List, parent, now).is_err()
                    || volume.mount.check(Action::Read, &name.path, now).is_err()
                    || !authorized_name(signed, task_grants, &volume.mount.view.volume, &name.path)?
                {
                    continue;
                }
                let path = if volume.prefix.is_empty() {
                    name.path
                } else {
                    format!("{}/{}", volume.prefix, name.path)
                };
                volume.best.push(path);
            }
        }
        let mut combined = crate::tui::PathMatches::new(query);
        let mut pending = 0;
        let mut complete = true;
        let mut unavailable = 0;
        for volume in &volumes {
            for path in volume.best.paths() {
                combined.push(path.to_owned());
            }
            complete &= volume.complete;
            if let Some(names) = &volume.names {
                pending += names.pending_directories().await;
                unavailable += names.unavailable_entries().await;
            } else {
                unavailable += 1;
            }
        }
        let publication = (
            combined.paths().map(str::to_owned).collect::<Vec<_>>(),
            pending,
            complete,
            unavailable,
        );
        if last.as_ref() != Some(&publication) {
            results(files, request, &combined, pending, complete, unavailable).await?;
            last = Some(publication);
        }
        tokio::task::yield_now().await;
    }
}

struct VolumeSearch<'a> {
    names: Option<crate::tools::git::IndexedNames>,
    prefix: &'a str,
    mount: &'a crate::workspace::Mount<Option<Arc<workspace::MountedVolume>>>,
    best: crate::tui::PathMatches,
    complete: bool,
}

// Read durable fork/selection metadata without constructing a task executor or
// mounting its checkout. Search must not trigger recovery or content capture.
async fn selected_view(
    workspace: &LocalWorkspace,
    runtime: &LocalTaskRuntime,
    task: Option<TaskId>,
) -> Result<Option<workspace::View>> {
    let mut task = task;
    let mut visited = std::collections::BTreeSet::new();
    while let Some(candidate) = task {
        if !visited.insert(candidate) {
            return Err(Error::Conflict("task ancestry contains a cycle".into()));
        }
        let admission = runtime.task_host().observe_admission(candidate).await?;
        if let Some(view) = workspace.existing_task_view(candidate).await? {
            return Ok(Some(view));
        }
        task = admission.parent;
    }
    workspace
        .selected_view()
        .await
        .map(|(selected, _)| selected)
}

fn authorized_name(
    scope: &Scope,
    task_grants: Option<&Capabilities>,
    volume: &VolumeRef,
    path: &str,
) -> Result<bool> {
    if !name_allowed(scope.capabilities(), scope.agent().is_some(), volume, path)? {
        return Ok(false);
    }
    match task_grants {
        Some(grants) => name_allowed(grants, scope.agent().is_some(), volume, path),
        None => Ok(true),
    }
}

fn name_allowed(
    grants: &Capabilities,
    has_agent: bool,
    volume: &VolumeRef,
    path: &str,
) -> Result<bool> {
    if crate::conversation::is_internal_path(path) {
        return Ok(false);
    }
    if grants.contains(&volume.capability(VolumeOperation::Read, &mut None)?) {
        return Ok(true);
    }
    // Directory grants are owner-delegated private content authority. Match
    // component boundaries, so a grant for `src` cannot expose `src-private`.
    if volume.class() != VolumeClass::AgentPrivate || !has_agent {
        return Ok(false);
    }
    for prefix in std::iter::once("").chain(path.match_indices('/').map(|(end, _)| &path[..end])) {
        if grants.contains(&volume.directory_read_capability(prefix, &mut None)?) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn name_search_failed_volume_preserves_healthy_mount_results() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let project = super::super::tests::volume(directory.path()).await?;
        let harness = LocalHarness::open(
            super::super::tests::volume_config(
                directory.path(),
                project,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            )?
            .without_default_tools(),
        )
        .await?;
        let workspace = harness.workspace.as_ref().unwrap();
        assert!(
            workspace
                .mounts
                .named(Some("scratch"))?
                .1
                .view
                .source
                .is_some()
        );
        assert!(!workspace.path.join(".volumes").exists());
        let stop = acyclic_fs::CancellationToken::new();
        let mut transaction = workspace
            .view
            .workspace()
            .begin_transaction(
                acyclic_fs::IdempotencyKey::new(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .map_err(storage)?
            .value;
        transaction
            .create_dir_all(
                "/.acyclic-sdk/name-index",
                acyclic_fs::WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .map_err(storage)?;
        transaction
            .write_text("/.acyclic-sdk/name-index/manifest.json", "invalid JSON")
            .await
            .map_err(storage)?;
        let outcome = transaction
            .commit(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &stop,
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .map_err(storage)?
            .value;
        assert!(matches!(
            outcome,
            acyclic_fs::TransactionCommit::Committed(_)
        ));
        let scratch = workspace
            .filesystem
            .open_workspace(
                harness.scratch().storage_name(&mut None)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .map_err(storage)?
            .value;
        scratch
            .write_text("/healthy.rs", "candidate content")
            .await
            .map_err(storage)?;
        let mut input = crate::tui::PromptInput::new(crate::tui::Editor::new("@healthy"));
        let mut completion = crate::tui_files::CompletionFiles::observe_authorized(
            harness.task_files(),
            &mut input,
            None,
        )
        .await?;
        harness
            .bind_name_search(&mut completion, &mut input)
            .await?;
        completion.refresh_query(&mut input, None).await?;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let change = completion.read_change().await?;
                let complete = change.file.iter().chain(&change.files).any(|file| {
                    file.key.path() == "search/progress.json"
                        && serde_json::from_slice::<serde_json::Value>(&file.bytes)
                            .is_ok_and(|value| value["complete"] == true)
                });
                completion.apply_change(change, &mut input)?;
                if complete {
                    return Ok::<_, Error>(());
                }
            }
        })
        .await
        .map_err(storage)??;
        assert_eq!(input.completion.unavailable_entries(), 1);
        assert!(input.completion.accept(&mut input.editor, 1));
        assert_eq!(input.editor.text(), "@.volumes/scratch/healthy.rs ");
        assert!(!workspace.path.join(".volumes").exists());
        Ok(())
    }

    #[tokio::test]
    async fn name_search_failed_request_is_visibly_partial_and_the_next_request_runs() -> Result<()>
    {
        let directory = tempfile::tempdir().map_err(storage)?;
        let project = super::super::tests::volume(directory.path()).await?;
        let harness = LocalHarness::open(
            super::super::tests::volume_config(
                directory.path(),
                project,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            )?
            .without_default_tools(),
        )
        .await?;
        let mut input = crate::tui::PromptInput::new(crate::tui::Editor::new("@unavailable"));
        let mut completion = crate::tui_files::CompletionFiles::observe_authorized(
            harness.task_files(),
            &mut input,
            None,
        )
        .await?;
        harness
            .bind_name_search(&mut completion, &mut input)
            .await?;
        completion
            .refresh_query(&mut input, Some(TaskId::from_bytes([99; 16])))
            .await?;
        // A nonexistent admission fails in the production request worker.
        // Previously this stopped the follower with no terminal publication.
        async fn terminal(
            completion: &mut crate::tui_files::CompletionFiles,
            input: &mut crate::tui::PromptInput,
        ) -> Result<()> {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let change = completion.read_change().await?;
                    let complete = change.file.iter().chain(&change.files).any(|file| {
                        file.key.path() == "search/progress.json"
                            && serde_json::from_slice::<serde_json::Value>(&file.bytes)
                                .is_ok_and(|value| value["complete"] == true)
                    });
                    completion.apply_change(change, input)?;
                    if complete {
                        return Ok(());
                    }
                }
            })
            .await
            .map_err(storage)?
        }
        terminal(&mut completion, &mut input).await?;
        assert!(input.completion.indexing().is_none());
        assert_eq!(input.completion.unavailable_entries(), 1);
        assert!(
            input.completion.active(&input.editor),
            "partial empty popup was hidden"
        );
        input.editor = crate::tui::Editor::new("@recovered");
        input.refresh_completions();
        let owner = TaskId::parse(&harness.client_owner().id)?;
        // Negative control: GraphCoder selects this owner at startup, but it
        // has no execution admission. Treating it as an admitted task makes
        // the otherwise healthy popup permanently report an unavailable source.
        assert!(
            harness
                .runtime
                .task_host()
                .observe_admission(owner)
                .await
                .is_err()
        );
        harness
            .workspace
            .as_ref()
            .unwrap()
            .view
            .workspace()
            .write_text("/recovered.rs", "candidate content")
            .await?;
        completion.refresh_query(&mut input, Some(owner)).await?;
        terminal(&mut completion, &mut input).await?;
        assert_eq!(input.completion.unavailable_entries(), 0);
        assert!(input.completion.indexing().is_none());
        assert!(input.completion.accept(&mut input.editor, 1));
        assert_eq!(input.editor.text(), "@recovered.rs ");
        Ok(())
    }

    #[tokio::test]
    async fn name_search_selects_the_tasks_durable_fork_without_mounting_it() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let project = super::super::tests::volume(directory.path()).await?;
        let harness = LocalHarness::open(
            super::super::tests::volume_config(
                directory.path(),
                project,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            )?
            .without_default_tools(),
        )
        .await?;
        let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
            harness.submit(OperationId::new(), "search fork").await?
        else {
            panic!("search admission")
        };
        let workspace = harness.workspace.as_ref().unwrap();
        // Runtime tool binding initializes passive bookkeeping during open.
        // Clear this marker so an accidental executor construction by search
        // is observable independently of that initial binding.
        workspace.state.lock().await.initialized = false;
        assert!(
            selected_view(workspace, &harness.runtime, Some(task_id))
                .await?
                .is_none()
        );
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let (fork, _) = workspace
            .view
            .fork(
                &format!("task-{task_id}"),
                acyclic_fs::IdempotencyKey::from_bytes(task_id.into_bytes()),
                acyclic_fs::PublicationPermit::Unrestricted,
                &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                },
            )
            .await?;
        // Negative control: ordinary sparse task forks have no Git lineage
        // registration, so an ID-only Git resolver cannot open this view.
        assert!(
            acyclic_fs::WorkspaceResolver::resolve(
                &(
                    workspace.filesystem.clone(),
                    acyclic_fs::WorkspaceGraph::new(workspace.store.clone())
                ),
                fork.workspace().id(),
            )
            .await
            .is_err()
        );
        let selected = selected_view(workspace, &harness.runtime, Some(task_id))
            .await?
            .expect("saved task fork");
        assert_eq!(selected.workspace().id(), fork.workspace().id());
        // Negative control: the global selector still names the original view.
        assert!(workspace.selected_view().await?.0.is_none());
        assert_ne!(selected.workspace().id(), workspace.view.workspace().id());
        let switched = crate::tools::git::run(
            crate::tools::git::Workspace {
                filesystem: &workspace.filesystem,
                store: &workspace.store,
                view: crate::tools::git::View::Checkout(&fork),
                root: &workspace.path,
                repository: fork.workspace().id(),
                lease: None,
                checkout: None,
                reserved: &[],
                author: "test",
                authored_at_seconds: 1,
                read_allowed: &|_| true,
                search_progress: None,
            },
            &["switch".into(), "-c".into(), "popup-selected".into()],
        )
        .await?
        .switched
        .expect("selected task branch");
        let selected = selected_view(workspace, &harness.runtime, Some(task_id))
            .await?
            .expect("saved task branch");
        assert_eq!(selected.workspace().id(), switched.workspace().id());
        assert_ne!(selected.workspace().id(), fork.workspace().id());
        assert!(workspace.mounted.is_none());
        assert!(!workspace.state.lock().await.initialized);
        Ok(())
    }

    #[test]
    fn name_search_requires_the_clients_signed_content_grant() -> Result<()> {
        let agent = AgentId::from_bytes([1; 16]);
        let provider = crate::resources::ProviderRef::new(
            "search-test".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let private = VolumeRef::new(
            provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let issuer = AuthorityIssuer::new(
            "search-test",
            [7; 32],
            Authority {
                kind: AggregateKind::Agent,
                id: agent.to_string(),
            },
        );
        let issue = |grants| {
            issuer.issue(
                "search",
                &Capabilities::new(grants),
                Some(agent),
                None,
                &mut None,
            )
        };
        let directory = issue(vec![private.directory_read_capability("src", &mut None)?])?;
        assert!(authorized_name(&directory, None, &private, "src/file.rs")?);
        assert!(!authorized_name(
            &directory,
            None,
            &private,
            "src-private/file.rs"
        )?);
        assert!(!authorized_name(&directory, None, &private, "other.rs")?);
        let absent = issue(Vec::<String>::new())?;
        assert!(!authorized_name(&absent, None, &private, "src/file.rs")?);
        // Negative control: the host mount's policy alone permits this name.
        let root = std::env::current_dir().map_err(storage)?;
        let mounts = workspace::mounts::owner(&root, &private, (), [])?;
        assert!(
            mounts
                .named(None)?
                .1
                .check(Action::Read, "src/file.rs", 0)
                .is_ok()
        );
        let whole = issue(vec![private.capability(VolumeOperation::Read, &mut None)?])?;
        assert!(authorized_name(&whole, None, &private, "other.rs")?);
        assert!(!authorized_name(&whole, None, &private, ".system/secret")?);
        let task_grants =
            Capabilities::new(vec![private.directory_read_capability("src", &mut None)?]);
        assert!(authorized_name(
            &whole,
            Some(&task_grants),
            &private,
            "src/file.rs"
        )?);
        assert!(!authorized_name(
            &whole,
            Some(&task_grants),
            &private,
            "other.rs"
        )?);
        // Negative control: the client's broader grant alone exposes this name.
        assert!(authorized_name(&whole, None, &private, "other.rs")?);
        let broad_task =
            Capabilities::new(vec![private.capability(VolumeOperation::Read, &mut None)?]);
        assert!(!authorized_name(
            &absent,
            Some(&broad_task),
            &private,
            "src/file.rs"
        )?);
        Ok(())
    }
}

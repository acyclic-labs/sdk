//! Descendant discovery follows owner statuses; every edge is checked against
//! immutable admission before opening that child's private approval volume.
use super::*;
use crate::{
    TaskId,
    task_volume::{ChildFile, ChildStatus, Snapshot, TaskFiles},
};
use acyclic_stream::{BoxProviderStream, LocalStream};
use futures::stream::SelectAll;
use std::collections::{BTreeSet, VecDeque};

pub(super) enum Event {
    Snapshot(Query, Snapshot),
    Change(Change),
    Retire(TaskId, Arc<std::sync::Mutex<Vec<TaskId>>>),
}
impl Event {
    pub(super) fn publish(self, output: &mut impl Write) -> Result<()> {
        match self {
            Self::Snapshot(query, snapshot) => emit(
                output,
                super::super::jsonl::snapshot(&query, &snapshot.into_heads()?)?,
            ),
            Self::Change(change) => publish(output, &change),
            Self::Retire(task, tasks) => {
                tasks
                    .lock()
                    .map_err(|_| Error::Storage("approval discovery state poisoned".into()))?
                    .retain(|active| *active != task);
                Ok(())
            }
        }
    }
}
pub(super) struct Events {
    events: BoxProviderStream<'static, Result<Box<Event>>>,
    tasks: Arc<std::sync::Mutex<Vec<TaskId>>>,
}
impl Events {
    // Discovery authenticates descendants through the coordinator. Its stream
    // owns any in-flight metadata read, so every other coordinator wait must
    // keep polling it until that read releases its guard.
    pub(super) fn during<'a, T: 'a>(
        &'a mut self,
        work: impl Future<Output = T> + 'a,
        output: &'a mut impl Write,
    ) -> Pin<Box<dyn Future<Output = Result<T>> + 'a>> {
        // Erase the composed work before constructing the polling future. The
        // same boundary serves prompt, metadata and cancellation futures without
        // nesting their entire state machines in the CLI's native stack frame.
        let mut work: Pin<Box<dyn Future<Output = T> + 'a>> = Box::pin(work);
        Box::pin(async move {
            loop {
                tokio::select! {
                    result = work.as_mut() => return Ok(result),
                    event = self.next() => match event {
                        Some(event) => (*event?).publish(output)?,
                        None => return Err(Error::Storage("approval tree follow ended".into())),
                    },
                }
            }
        })
    }
    pub(super) async fn next(&mut self) -> Option<Result<Box<Event>>> {
        self.events.next().await
    }
    pub(super) fn tasks(&self) -> Result<Vec<TaskId>> {
        self.tasks
            .lock()
            .map(|tasks| tasks.clone())
            .map_err(|_| Error::Storage("approval discovery state poisoned".into()))
    }
}
struct Feed {
    task: TaskId,
    approval: bool,
    position: u64,
    changes: crate::task_volume::ChangeStream,
}

impl Stream for Feed {
    type Item = Result<(TaskId, bool, Change)>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.changes.as_mut().poll_next(cx).map(|change| {
            change.map(|change| {
                change.map_err(Error::from).map(|change| {
                    self.position = change.cursor.position;
                    (self.task, self.approval, change)
                })
            })
        })
    }
}

struct Tree {
    local: Arc<LocalHarness>,
    root: TaskId,
    initialized: bool,
    seen: BTreeSet<TaskId>,
    feeds: SelectAll<Feed>,
    statuses: VecDeque<(TaskId, FileValue)>,
    ready: VecDeque<Event>,
    tasks: Arc<std::sync::Mutex<Vec<TaskId>>>,
}
impl Tree {
    fn new(
        local: Arc<LocalHarness>,
        root: TaskId,
        tasks: Arc<std::sync::Mutex<Vec<TaskId>>>,
    ) -> Self {
        Self {
            local,
            root,
            initialized: false,
            seen: BTreeSet::new(),
            feeds: SelectAll::new(),
            statuses: VecDeque::new(),
            ready: VecDeque::new(),
            tasks,
        }
    }

    async fn watch(
        &mut self,
        task: TaskId,
        files: Arc<TaskFiles<LocalStream>>,
        root: bool,
    ) -> Result<()> {
        let snapshot = files.inner.child_lifecycle(None).await?;
        let position = snapshot.cursor.position;
        let changes = files.inner.follow(Query::Children, snapshot.cursor).await?;
        self.statuses
            .extend(snapshot.files.into_iter().map(|file| (task, file)));
        self.feeds.push(Feed {
            task,
            approval: false,
            position,
            changes,
        });
        if !root {
            let component = Component::new(task.to_string())?;
            let query = Query::Approvals(component.clone());
            let snapshot = files.inner.pending_approvals(&component).await?;
            let changes = files
                .inner
                .follow(query.clone(), snapshot.cursor.clone())
                .await?;
            self.feeds.push(Feed {
                task,
                approval: true,
                position: snapshot.cursor.position,
                changes,
            });
            self.ready.push_back(Event::Snapshot(query, snapshot));
        }
        self.seen.insert(task);
        if !root {
            // Publish discovery before its snapshot can be delivered. The same
            // authenticated set owns both following and disconnect detection.
            self.tasks
                .lock()
                .map_err(|_| Error::Storage("approval discovery state poisoned".into()))?
                .push(task);
        }
        Ok(())
    }

    fn deliver(&mut self, task: TaskId, approval: bool, change: Change) {
        if approval {
            if !change.is_checkpoint() {
                self.ready.push_back(Event::Change(change));
            }
        } else {
            self.statuses.extend(
                change
                    .file
                    .into_iter()
                    .chain(change.files)
                    .map(|file| (task, file)),
            );
        }
    }

    async fn retire(&mut self, task: TaskId, position: u64) -> Result<()> {
        // Detach the exact child's owned follows. Drain the accepted cut before
        // releasing them, so a status that wins the select cannot lose the
        // preceding final decision. Parent status follows can admit a later turn.
        let feeds = std::mem::take(&mut self.feeds).into_iter();
        for mut feed in feeds {
            if feed.task != task {
                self.feeds.push(feed);
                continue;
            }
            while feed.position < position {
                let (task, approval, change) = feed.next().await.ok_or_else(|| {
                    Error::Storage("settled approval follow ended before its cut".into())
                })??;
                self.deliver(task, approval, change);
            }
        }
        self.seen.remove(&task);
        // Disconnect observation releases this identity only after the caller
        // has delivered every final event queued above.
        self.ready
            .push_back(Event::Retire(task, self.tasks.clone()));
        Ok(())
    }
    async fn next(&mut self) -> Result<Event> {
        if !self.initialized {
            self.watch(self.root, self.local.task_files().clone(), true)
                .await?;
            self.initialized = true;
        }
        loop {
            if let Some(event) = self.ready.pop_front() {
                return Ok(event);
            }
            if let Some((parent, file)) = self.statuses.pop_front() {
                if matches!(
                    file.key,
                    FileKey::Child {
                        file: ChildFile::Status,
                        ..
                    }
                ) {
                    let status = ChildStatus::parse(&file.bytes)?;
                    let child = status.task;
                    if status.settled {
                        if self.seen.contains(&child)
                            && let Some(files) =
                                self.local.approval_child_files(parent, child).await?
                        {
                            self.retire(child, files.inner.head().await?.position)
                                .await?;
                        }
                        continue;
                    }
                    if !self.seen.contains(&child)
                        && let Some(files) = self.local.approval_child_files(parent, child).await?
                    {
                        self.watch(child, files, false).await?;
                    }
                }
                continue;
            }
            let (parent, approval, change) =
                self.feeds.next().await.ok_or_else(|| {
                    Error::Storage("approval tree lost its owner follow".into())
                })??;
            self.deliver(parent, approval, change);
        }
    }
}

pub(super) fn follow(local: Arc<LocalHarness>, root: TaskId) -> Events {
    let tasks = Arc::new(std::sync::Mutex::new(vec![root]));
    let tree = Tree::new(local, root, tasks.clone());
    let events = Box::pin(futures::stream::try_unfold(tree, |mut tree| async move {
        let event = tree.next().await?;
        Ok(Some((Box::new(event), tree)))
    }));
    Events { events, tasks }
}

#[cfg(all(test, not(acyclic_harness_loom), not(acyclic_loom)))]
mod tests {
    use super::*;
    use crate::{Admission, runtime::RuntimeTask};

    async fn fixture() -> Result<(tempfile::TempDir, Arc<LocalHarness>)> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let provider = Arc::new(
            crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
                crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                },
            ),
        );
        let model = crate::model::Model::new(
            "test".into(),
            "approval-discovery".into(),
            "1".into(),
            serde_json::Value::Null,
        )?;
        let local = Arc::new(
            LocalHarness::open(crate::test_support::local_harness_config(
                directory.path(),
                (model, provider),
            )?)
            .await?,
        );
        Ok((directory, local))
    }

    #[tokio::test]
    async fn settled_child_follows_drain_final_decisions_and_reopen_for_a_new_turn() -> Result<()> {
        let (_directory, local) = fixture().await?;
        let root = TaskId::from_bytes([1; 16]);
        let tasks = Arc::new(std::sync::Mutex::new(vec![root]));
        let mut tree = Tree::new(local.clone(), root, tasks.clone());
        let former_tasks = Arc::new(std::sync::Mutex::new(vec![root]));
        let mut former = Tree::new(local.clone(), root, former_tasks.clone());
        let files = local.task_files().clone();
        // Execute the former shape: watch each child and omit retirement.
        // Its two follows and disconnect identity survive every settlement.
        for turn in 0..24 {
            let child = TaskId::from_bytes([turn + 2; 16]);
            tree.watch(child, files.clone(), false).await?;
            former.watch(child, files.clone(), false).await?;
            tree.ready.clear();
            former.ready.clear();
            let request = Component::new(format!("turn-{turn}"))?;
            let key = |file| FileKey::Task {
                task: Component::new(child.to_string()).unwrap(),
                file,
            };
            let request_file = FileValue::new(
                key(TaskFile::ApprovalRequest(request.clone())),
                b"request".to_vec(),
            );
            let decision_file = FileValue::new(
                key(TaskFile::ApprovalDecision {
                    request: request.clone(),
                    principal: Component::new("human")?,
                }),
                b"decision".to_vec(),
            );
            let generation = files.inner.head().await?;
            let generation = files.inner.append(generation, &request_file).await?;
            let cut = files.inner.append(generation, &decision_file).await?;
            tree.retire(child, cut.position).await?;
            assert_eq!(tree.feeds.len(), 0);
            assert!(tree.seen.is_empty());
            assert_eq!(*tasks.lock().unwrap(), vec![root, child]);
            assert!(tree.ready.iter().any(|event| matches!(event,
                Event::Change(change) if change.file.iter().chain(&change.files).any(|file| file == &decision_file)
            )), "settlement lost the final decision before delivery");
            assert_eq!(
                files
                    .inner
                    .snapshot(
                        &Query::Approval {
                            task: Component::new(child.to_string())?,
                            request,
                        },
                        8
                    )
                    .await?
                    .files
                    .len(),
                2,
                "retirement deleted canonical history"
            );
            let mut output = Vec::new();
            while let Some(event) = tree.ready.pop_front() {
                event.publish(&mut output)?;
            }
            assert_eq!(*tasks.lock().unwrap(), vec![root]);
            // A later unsettled status for the same task must open fresh
            // follows; no historical tombstone can suppress the new turn.
            tree.watch(child, files.clone(), false).await?;
            assert_eq!(tree.feeds.len(), 2);
            assert_eq!(*tasks.lock().unwrap(), vec![root, child]);
            tree.retire(child, cut.position).await?;
            while let Some(event) = tree.ready.pop_front() {
                event.publish(&mut output)?;
            }
        }
        assert_eq!(former.feeds.len(), 48);
        assert_eq!(former.seen.len(), 24);
        assert_eq!(former_tasks.lock().unwrap().len(), 25);
        drop(tree);
        drop(former);
        super::super::super::disconnect_local(local).await?;
        Ok(())
    }

    async fn discovery_releases_gate(host_done: bool, broken: bool) -> Result<bool> {
        let (_directory, local) = fixture().await?;
        let Admission::Accepted(RuntimeTask::Durable { task_id: task, .. }) =
            local.submit(OperationId::new(), "parent").await?
        else {
            return Err(Error::Invalid("fixture admission was not accepted".into()));
        };
        let mut client = LocalFileClient::open(local.clone(), OperationId::new()).await?;
        drop(client.attachment.take());
        client.host.as_mut().await?;
        client.host = Box::pin(std::future::pending());
        client.host_done = host_done;
        let snapshot = local
            .task_files()
            .inner
            .snapshot(&Query::Root, MAXIMUM_FILES)
            .await?;
        let host = Arc::clone(local.test_coordinator());
        let discovery_host = host.clone();
        let (entered, locked) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        client.approvals = Events {
            events: Box::pin(
                futures::stream::once(async move {
                    discovery_host
                        .test_coordinator_barrier(entered, released)
                        .await;
                    Ok(Box::new(Event::Snapshot(Query::Root, snapshot)))
                })
                .chain(futures::stream::pending()),
            ),
            tasks: Arc::new(std::sync::Mutex::new(vec![task])),
        };
        assert!(futures::poll!(Box::pin(client.approvals.next()).as_mut()).is_pending());
        locked
            .await
            .map_err(|_| Error::Storage("fixture gate did not enter".into()))?;
        release
            .send(())
            .map_err(|()| Error::Storage("fixture gate lost its release".into()))?;
        // IO is ready, but only polling discovery releases the actual gate.
        // Probe once without a deadline; the parked-owner control is Pending.
        let mut output = Vec::new();
        let ready = if broken {
            tokio::task::unconstrained(host.terminal_status(task))
                .now_or_never()
                .map(|result| result.map(|status| status.is_none()))
        } else {
            tokio::task::unconstrained(
                client.while_driving(host.terminal_status(task), &mut output),
            )
            .now_or_never()
            .map(|result| result.and_then(|status| status.map(|status| status.is_none())))
        };
        let ready = ready.transpose()?.unwrap_or(false);
        if ready {
            assert!(!output.is_empty(), "discovery event was not published");
        }
        drop(client);
        drop(host);
        super::super::super::disconnect_local(local).await?;
        Ok(ready)
    }

    #[tokio::test]
    async fn approval_metadata_drives_discovery_before_and_after_host_settlement() -> Result<()> {
        for host_done in [false, true] {
            assert!(
                discovery_releases_gate(host_done, false).await?,
                "metadata parked approval discovery"
            );
            assert!(
                !discovery_releases_gate(host_done, true).await?,
                "old parked-owner control progressed"
            );
        }
        Ok(())
    }
}

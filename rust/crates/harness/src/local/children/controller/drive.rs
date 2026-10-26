use super::*;
use std::{collections::VecDeque, future::Future, pin::Pin};

impl LocalHarness {
    #[inline(never)]
    pub(in crate::local) fn advance_with_children<'a>(
        &'a self,
        task: TaskId,
        commands: &'a dyn TaskCommandHost,
        files: Arc<Files>,
        control: Option<crate::local::approvals::Inbox>,
    ) -> BoxProviderFuture<'a, Result<DriveState>> {
        Box::pin(async move {
            let signals = ChildSignals {
                inherited: commands.invocation_cancellation().cloned(),
                parent_done: acyclic_fs::CancellationToken::new(),
                parent_passive: acyclic_fs::CancellationToken::new(),
            };
            let children = self.initial_child_drivers(&files, &signals).await?;
            let scoped = draining::Commands::new(self, commands);
            let parent = self.advance_parent(task, &scoped, &control, &signals);
            coordination(|| self.drive_children(files, children, &scoped, &signals, parent)).await
        })
    }

    async fn drive_children<'a>(
        &'a self,
        files: Arc<Files>,
        mut children: ChildDrivers<'a>,
        scoped: &draining::Commands<'_>,
        signals: &ChildSignals,
        parent: impl Future<Output = Result<DriveState>> + acyclic_stream::ProviderTask,
    ) -> Result<DriveState> {
        let mut completions = BTreeSet::new();
        let inherited = &signals.inherited;
        let mut inherited_stopped = false;
        let cancellation = &scoped.cancellation;
        let drained = &scoped.drained;
        futures::pin_mut!(parent);
        let mut parent_done = false;
        let mut detached_approval = false;
        let mut pending = std::collections::VecDeque::new();
        macro_rules! observed {
            ($work:expr) => {
                during(
                    coordination(|| $work),
                    parent.as_mut(),
                    &mut parent_done,
                    &mut children.jobs,
                    &mut pending,
                )
                .await
            };
        }
        let result = coordination(|| async {
                loop {
                    if !inherited_stopped && inherited.as_ref().is_some_and(|token| token.is_cancelled()) {
                        inherited_stopped = true;
                        cancellation.cancel();
                        let mut after = None;
                        loop {
                            let Some(page) = observed!(files.inner.child_references_page(after.as_ref(), 64)).map_err(storage)? else { continue; };
                            if page.is_empty() { break; }
                            after = page.last().cloned();
                            for reference in page {
                            if !children.active.contains(&reference) && !matches!(desired(&reference, &children.values)?, ChildState::Cancelled | ChildState::Archived) {
                                children.active.insert(reference.clone());
                                let request = observed!(files.inner.child_request(&reference)).map_err(storage)?;
                                children.jobs.push(self.service_child(files.clone(), reference, request, Some(signals.clone())));
                            }
                        }
                        }
                    }
                    if cancellation.is_cancelled() && children.jobs.is_empty() { drained.cancel(); }
                    if parent_done && children.jobs.is_empty() && completions.is_empty() && pending.is_empty() {
                        return Ok(if detached_approval { DriveState::AwaitingApproval } else { DriveState::Quiescent });
                    }
                    let event = if let Some(event) = pending.pop_front() {
                        event
                    } else { tokio::select! {
                            biased;
                            change = coordination(|| follow::next(files.as_ref(), Query::Children, &mut children.cursor)) => Wake::File(change),
                            result = children.jobs.next(), if !children.jobs.is_empty() => Wake::child(result),
                            _ = inherited_cancelled(inherited.as_ref()),
                                if inherited.is_some() && !inherited_stopped => Wake::Inherited,
                            result = &mut parent, if !parent_done => Wake::Parent(result),
                        }
                    };
                    match event {
                        Wake::File(change) => {
                            for reference in children.accept_change(change, &mut completions)? {
                                if children.active.contains(&reference) || !needs_work_count(&reference, &children.values, inherited_stopped,
                                    observed!(files.inner.child_input_count(&reference)).map_err(storage)?)? { continue; }
                                let request = observed!(files.inner.child_request(&reference)).map_err(storage)?;
                                children.active.insert(reference.clone());
                                children.jobs.push(self.service_child(files.clone(), reference, request, Some(signals.clone())));
                            }
                        }
                        Wake::Child(result) => {
                            let ChildCompletion::Settled(reference, position) = result? else {
                                detached_approval = true;
                                continue;
                            };
                            completions.insert((position, reference));
                            for reference in release_completed(children.generation.position, &mut children.active, &mut completions) {
                                if needs_work_count(&reference, &children.values, inherited_stopped,
                                    observed!(files.inner.child_input_count(&reference)).map_err(storage)?)? {
                                    let request = observed!(files.inner.child_request(&reference)).map_err(storage)?;
                                    children.active.insert(reference.clone());
                                    children.jobs.push(self.service_child(files.clone(), reference, request, Some(signals.clone())));
                                }
                            }
                        }
                        Wake::Parent(result) => {
                            parent_done = true;
                            detached_approval |= result? == DriveState::AwaitingApproval;
                        }
                        Wake::Inherited => {}
                    }
                }
            }).await;
        drain_parent(
            result,
            parent.as_mut(),
            parent_done,
            &mut children.jobs,
            scoped,
            signals,
        )
        .await
    }
    async fn initial_child_drivers<'a>(
        &'a self,
        files: &Arc<Files>,
        signals: &ChildSignals,
    ) -> Result<ChildDrivers<'a>> {
        let snapshot = coordination(|| files.inner.child_lifecycle(None))
            .await
            .map_err(storage)?;
        let cursor = follow::Follow::new(snapshot.cursor);
        let generation = snapshot.generation;
        let values: BTreeMap<_, _> = snapshot
            .files
            .into_iter()
            .map(|file| (file.key.clone(), file))
            .collect();
        let mut active = BTreeSet::new();
        let jobs = FuturesUnordered::new();
        let mut after = None;
        loop {
            let Some(page) = coordination(|| files.inner.child_references_page(after.as_ref(), 64))
                .await
                .map_err(storage)?
            else {
                continue;
            };
            if page.is_empty() {
                break;
            }
            after = page.last().cloned();
            for reference in page {
                if needs_work_count(
                    &reference,
                    &values,
                    false,
                    coordination(|| files.inner.child_input_count(&reference))
                        .await
                        .map_err(storage)?,
                )? {
                    active.insert(reference.clone());
                    let request = coordination(|| files.inner.child_request(&reference))
                        .await
                        .map_err(storage)?;
                    jobs.push(self.service_child(
                        Arc::clone(files),
                        reference,
                        request,
                        Some(signals.clone()),
                    ));
                }
            }
        }
        Ok(ChildDrivers {
            cursor,
            generation,
            values,
            active,
            jobs,
        })
    }

    async fn advance_parent(
        &self,
        task: TaskId,
        scoped: &draining::Commands<'_>,
        control: &Option<crate::local::approvals::Inbox>,
        signals: &ChildSignals,
    ) -> Result<DriveState> {
        let state = crate::package::harness::worker::advance_while(
            self,
            task,
            scoped,
            control.as_ref(),
            || async {
                if scoped.cancellation.is_cancelled()
                    || scoped
                        .inner
                        .invocation_cancellation()
                        .is_some_and(|token| token.is_cancelled())
                {
                    return Ok(false);
                }
                match control {
                    None => Ok(true),
                    Some(crate::local::approvals::Inbox {
                        files, reference, ..
                    }) => Ok(super::super::commands::state(
                        files,
                        reference,
                        usize::try_from(self.limits.render_bytes).map_err(storage)?,
                    )
                    .await?
                        == ChildState::Running),
                }
            },
        )
        .await?;
        if state == DriveState::AwaitingApproval {
            signals.parent_passive.cancel();
        }
        match coordination(|| self.runtime.task_host().terminal_status(task)).await? {
            Some(crate::Outcome::Indeterminate {
                operation_id,
                cause,
            }) => return Err(cause.into_error(operation_id)),
            Some(_) => signals.parent_done.cancel(),
            None => {}
        }
        Ok(state)
    }
}

async fn inherited_cancelled(inherited: Option<&acyclic_fs::CancellationToken>) {
    if let Some(inherited) = inherited {
        inherited.cancelled().await;
    } else {
        std::future::pending::<()>().await;
    }
}

// Keep the coordination loop's storage/follow construction separate from the
// frame which owns parent cancellation and drains every child on failure.
#[inline(never)]
fn coordination<'a, T, F>(build: impl FnOnce() -> F) -> BoxProviderFuture<'a, T>
where
    F: Future<Output = T> + acyclic_stream::ProviderTask + 'a,
{
    Box::pin(build())
}

// This queue contains at most one completion per currently driven future. The
// accepted file cursor remains the authority for rescheduling and publication.
enum Wake {
    File(Option<crate::task_volume::Result<Change>>),
    Child(Result<ChildCompletion>),
    Parent(Result<DriveState>),
    Inherited,
}
impl Wake {
    fn child(result: Option<Result<ChildCompletion>>) -> Self {
        Self::Child(
            result.unwrap_or_else(|| Err(Error::Conflict("child worker disappeared".into()))),
        )
    }
}

// A paused child can own the coordinator mutex across an IO completion. Every
// metadata await must keep polling the parent and siblings which release it.
async fn during<T>(
    work: impl Future<Output = T>,
    mut parent: Pin<&mut impl Future<Output = Result<DriveState>>>,
    parent_done: &mut bool,
    jobs: &mut FuturesUnordered<BoxProviderFuture<'_, Result<ChildCompletion>>>,
    pending: &mut VecDeque<Wake>,
) -> T {
    futures::pin_mut!(work);
    loop {
        tokio::select! {
            biased;
            result = jobs.next(), if !jobs.is_empty() => pending.push_back(Wake::child(result)),
            result = parent.as_mut(), if !*parent_done => {
                *parent_done = true;
                pending.push_back(Wake::Parent(result));
            }
            result = &mut work => return result,
        }
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;

#[cfg(test)]
#[cfg(all(acyclic_harness_loom, acyclic_loom))]
mod models;

struct ChildDrivers<'a> {
    cursor: follow::Follow,
    generation: Generation,
    values: BTreeMap<FileKey, FileValue>,
    active: BTreeSet<Component>,
    jobs: FuturesUnordered<BoxProviderFuture<'a, Result<ChildCompletion>>>,
}

impl ChildDrivers<'_> {
    fn accept_change(
        &mut self,
        change: Option<crate::task_volume::Result<Change>>,
        completions: &mut BTreeSet<(u64, Component)>,
    ) -> Result<BTreeSet<Component>> {
        let change = change
            .ok_or_else(|| Error::Conflict("child folder follow ended".into()))?
            .map_err(storage)?;
        let mut changed = changed_references(&change);
        advance_fold(&mut self.values, &mut self.generation, change);
        changed.extend(release_completed(
            self.generation.position,
            &mut self.active,
            completions,
        ));
        Ok(changed)
    }
}

fn changed_references(change: &Change) -> BTreeSet<Component> {
    change
        .file
        .iter()
        .chain(&change.files)
        .filter_map(|file| match &file.key {
            FileKey::Child {
                reference,
                file: ChildFile::Request | ChildFile::Inbox(_) | ChildFile::Control,
            } => Some(reference.clone()),
            _ => None,
        })
        .collect()
}

async fn drain_parent(
    result: Result<DriveState>,
    mut parent: Pin<&mut (impl Future<Output = Result<DriveState>> + acyclic_stream::ProviderTask)>,
    parent_done: bool,
    jobs: &mut FuturesUnordered<BoxProviderFuture<'_, Result<ChildCompletion>>>,
    scoped: &draining::Commands<'_>,
    signals: &ChildSignals,
) -> Result<DriveState> {
    if result.is_ok() {
        return result;
    }
    signals.parent_done.cancel();
    coordination(|| {
        draining::finish(
            async {
                if parent_done {
                    Ok(())
                } else {
                    parent.as_mut().await.map(|_| ())
                }
            },
            jobs,
            &scoped.cancellation,
            &scoped.drained,
        )
    })
    .await;
    result
}

use super::*;
use std::{collections::VecDeque, future::Future, pin::Pin};

impl LocalHarness {
    pub(in crate::local) fn advance_with_children<'a>(
        &'a self,
        task: TaskId,
        commands: &'a dyn TaskCommandHost,
        files: Arc<Files>,
        control: Option<(Arc<Files>, Component)>,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let snapshot = files.inner.child_lifecycle(None).await.map_err(storage)?;
            let mut cursor = follow::Follow::new(snapshot.cursor);
            let mut generation = snapshot.generation;
            let mut values: BTreeMap<_, _> = snapshot
                .files
                .into_iter()
                .map(|file| (file.key.clone(), file))
                .collect();
            let mut active = BTreeSet::new();
            let mut completions = BTreeMap::new();
            let mut jobs = FuturesUnordered::new();
            let inherited = commands.invocation_cancellation().cloned();
            let mut inherited_stopped = false;
            for reference in files.inner.child_references().await.map_err(storage)? {
                if needs_work_count(
                    &reference,
                    &values,
                    false,
                    files
                        .inner
                        .child_input_count(&reference)
                        .await
                        .map_err(storage)?,
                )? {
                    active.insert(reference.clone());
                    let request = files
                        .inner
                        .child_request(&reference)
                        .await
                        .map_err(storage)?;
                    jobs.push(self.service_child(
                        files.clone(),
                        reference.clone(),
                        request.clone(),
                        inherited.clone(),
                    ));
                }
            }
            let cancellation = acyclic_fs::CancellationToken::new();
            let drained = acyclic_fs::CancellationToken::new();
            let scoped = draining::Commands {
                harness: self,
                inner: commands,
                cancellation: cancellation.clone(),
                drained: drained.clone(),
            };
            let parent = async {
                self.advance_while(task, &scoped, || async {
                    if cancellation.is_cancelled()
                        || commands
                            .invocation_cancellation()
                            .is_some_and(|token| token.is_cancelled())
                    {
                        return Ok(false);
                    }
                    match &control {
                        None => Ok(true),
                        Some((files, reference)) => Ok(super::super::commands::state(
                            files,
                            reference,
                            usize::try_from(self.limits.render_bytes).map_err(storage)?,
                        )
                        .await?
                            == ChildState::Running),
                    }
                })
                .await?;
                if let Some(crate::Outcome::Indeterminate {
                    operation_id,
                    cause,
                }) = self.runtime.task_host().terminal_status(task).await?
                {
                    return Err(cause.into_error(operation_id));
                }
                Ok(())
            };
            futures::pin_mut!(parent);
            let mut parent_done = false;
            let mut pending = std::collections::VecDeque::new();
            macro_rules! observed {
                ($work:expr) => {
                    during(
                        $work,
                        parent.as_mut(),
                        &mut parent_done,
                        &mut jobs,
                        &mut pending,
                    )
                    .await
                };
            }
            let result = async {
                loop {
                    if !inherited_stopped && inherited.as_ref().is_some_and(|token| token.is_cancelled()) {
                        inherited_stopped = true;
                        cancellation.cancel();
                        for reference in observed!(files.inner.child_references()).map_err(storage)? {
                            if !active.contains(&reference) && !matches!(desired(&reference, &values)?, ChildState::Cancelled | ChildState::Archived) {
                                active.insert(reference.clone());
                                let request = observed!(files.inner.child_request(&reference)).map_err(storage)?;
                                jobs.push(self.service_child(files.clone(), reference, request, inherited.clone()));
                            }
                        }
                    }
                    if cancellation.is_cancelled() && jobs.is_empty() { drained.cancel(); }
                    if parent_done && jobs.is_empty() && completions.is_empty() && pending.is_empty() {
                        return Ok(());
                    }
                    let event = match pending.pop_front() {
                        Some(event) => event,
                        None => tokio::select! {
                            biased;
                            change = follow::next(files.as_ref(), Query::Children, &mut cursor) => Wake::File(change),
                            result = jobs.next(), if !jobs.is_empty() => Wake::child(result),
                            _ = async { inherited.as_ref().unwrap().cancelled().await },
                                if inherited.is_some() && !inherited_stopped => Wake::Inherited,
                            result = &mut parent, if !parent_done => Wake::Parent(result),
                        },
                    };
                    match event {
                        Wake::File(change) => {
                            let change = change.ok_or_else(|| Error::Conflict("child folder follow ended".into()))?.map_err(storage)?;
                            let mut changed = BTreeSet::new();
                            for file in change.file.iter().chain(&change.files) {
                                if let FileKey::Child { reference, file: ChildFile::Request | ChildFile::Inbox(_) | ChildFile::Control } = &file.key {
                                    changed.insert(reference.clone());
                                }
                            }
                            advance_fold(&mut values, &mut generation, change);
                            changed.extend(release_completed(generation.position, &mut active, &mut completions));
                            for reference in changed {
                                if active.contains(&reference) || !needs_work_count(&reference, &values, inherited_stopped,
                                    observed!(files.inner.child_input_count(&reference)).map_err(storage)?)? { continue; }
                                let request = observed!(files.inner.child_request(&reference)).map_err(storage)?;
                                active.insert(reference.clone());
                                jobs.push(self.service_child(files.clone(), reference, request, inherited.clone()));
                            }
                        }
                        Wake::Child(result) => {
                            let (reference, position) = result?;
                            completions.insert(reference, position);
                            for reference in release_completed(generation.position, &mut active, &mut completions) {
                                if needs_work_count(&reference, &values, inherited_stopped,
                                    observed!(files.inner.child_input_count(&reference)).map_err(storage)?)? {
                                    let request = observed!(files.inner.child_request(&reference)).map_err(storage)?;
                                    active.insert(reference.clone());
                                    jobs.push(self.service_child(files.clone(), reference, request, inherited.clone()));
                                }
                            }
                        }
                        Wake::Parent(result) => {
                            parent_done = true;
                            result?;
                        }
                        Wake::Inherited => {}
                    }
                }
            }.await;
            if result.is_err() {
                draining::finish(
                    async {
                        if parent_done {
                            Ok(())
                        } else {
                            parent.as_mut().await
                        }
                    },
                    &mut jobs,
                    &cancellation,
                    &drained,
                )
                .await;
            }
            result
        })
    }
}

// This queue contains at most one completion per currently driven future. The
// accepted file cursor remains the authority for rescheduling and publication.
enum Wake {
    File(Option<crate::task_volume::Result<Change>>),
    Child(Result<(Component, u64)>),
    Parent(Result<()>),
    Inherited,
}
impl Wake {
    fn child(result: Option<Result<(Component, u64)>>) -> Self {
        Self::Child(
            result.unwrap_or_else(|| Err(Error::Conflict("child worker disappeared".into()))),
        )
    }
}

// A paused child can own the coordinator mutex across an IO completion. Every
// metadata await must keep polling the parent and siblings which release it.
async fn during<T>(
    work: impl Future<Output = T>,
    mut parent: Pin<&mut impl Future<Output = Result<()>>>,
    parent_done: &mut bool,
    jobs: &mut FuturesUnordered<BoxProviderFuture<'_, Result<(Component, u64)>>>,
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

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

#[cfg(all(test, acyclic_harness_loom, acyclic_loom))]
mod models;

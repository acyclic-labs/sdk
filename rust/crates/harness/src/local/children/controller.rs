use super::*;
use crate::{
    filesystem::TaskCommandHost,
    runtime::TaskStateProvider,
    task_volume::{
        Change, ChildFile, ChildRequest, ChildState, ChildStatus, Component, Control, FileKey,
        FileValue, Generation, Query, TaskFiles,
    },
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use futures::{StreamExt as _, stream::FuturesUnordered};
use std::collections::{BTreeMap, BTreeSet};

type Files = TaskFiles<acyclic_stream::LocalStream>;

mod delivery;
mod draining;
mod drive;
pub(super) mod follow;
mod inputs;
mod lease;
mod result_slot;
mod results;
mod stopping;

impl LocalHarness {
    pub(super) fn service_child<'a>(
        &'a self,
        files: Arc<Files>,
        reference: Component,
        request: FileValue,
        inherited: Option<acyclic_fs::CancellationToken>,
    ) -> BoxProviderFuture<'a, Result<(Component, u64)>> {
        Box::pin(async move {
            let FileKey::Child {
                reference: requested,
                file: ChildFile::Request,
            } = &request.key
            else {
                return Err(Error::Invalid("child controller requires a request".into()));
            };
            if requested != &reference {
                return Err(Error::Invalid("child controller reference differs".into()));
            }
            // Admission reads may wait on a live executor. Exclude another
            // controller before the first read, using admission's same identity.
            let operation =
                super::admission::child_operation(files.volume(), reference.as_str(), b"child")?;
            let driving = super::super::admission::serialized(&self.admission, async || {
                self.drivers.drive(operation)
            })
            .await;
            let _executing = driving.execute()?;
            let snapshot = files
                .inner
                .child_lifecycle(Some(&reference))
                .await
                .map_err(storage)?;
            let mut cursor = follow::Follow::new(snapshot.cursor);
            let mut generation = snapshot.generation;
            let mut values: BTreeMap<_, _> = snapshot
                .files
                .into_iter()
                .map(|file| (file.key.clone(), file))
                .collect();
            let mut inherited_stopped =
                inherited.as_ref().is_some_and(|token| token.is_cancelled());
            if inherited_stopped {
                stopping::cancel(
                    files.as_ref(),
                    &reference,
                    &mut cursor,
                    &mut values,
                    &mut generation,
                )
                .await?;
            }
            if !needs_work_count(
                &reference,
                &values,
                inherited_stopped,
                files
                    .inner
                    .child_input_count(&reference)
                    .await
                    .map_err(storage)?,
            )? {
                return Ok((reference, generation.position));
            }
            let child = self.admit_child_task(files.as_ref(), &request).await?;
            let parsed = ChildRequest::parse(&request.bytes, self.limits.file_bytes)?;
            let mut model = self.model.clone();
            if let Some(name) = parsed.model {
                model.name = name;
                model.validate()?;
            }
            let commands = self.runtime.commands().with_model(
                &model,
                self.provider.clone(),
                &self.compaction,
                None,
            )?;
            loop {
                follow::refresh(
                    files.as_ref(),
                    &reference,
                    &mut cursor,
                    &mut values,
                    &mut generation,
                )
                .await?;
                #[cfg(test)]
                #[cfg(not(acyclic_harness_loom))]
                super::cuts::follow_probe(files.as_ref(), &reference, &mut cursor).await?;
                let state = desired(&reference, &values)?;
                let previous = execution(&reference, &values)?;
                let inputs = files
                    .inner
                    .child_input_count(&reference)
                    .await
                    .map_err(storage)?;
                // A force-settled orphan has no completed receive checkpoint.
                // Its terminal scheduler outcome is authoritative: acknowledge
                // cancellation in files without reclaiming a released lease or
                // inventing a successful result for the interrupted receive.
                let cancelled_outcome = matches!(
                    self.runtime.task_host().terminal_status(child).await?,
                    Some(crate::Outcome::Cancelled)
                );
                let cancelled_suspension = if cancelled_outcome {
                    self.runtime
                        .task_host()
                        .cancelled_workflow_suspension(child, &mut None)
                        .await?
                } else {
                    None
                };
                if cancelled_outcome
                    && (!matches!(state, ChildState::Cancelled | ChildState::Archived)
                        || !stopping::cancelled_turn_is_quiet(
                            previous,
                            child,
                            results::published(&reference, &values),
                            cancelled_suspension.as_ref(),
                        ))
                {
                    stopping::cancel(
                        files.as_ref(),
                        &reference,
                        &mut cursor,
                        &mut values,
                        &mut generation,
                    )
                    .await?;
                    let status = ChildStatus {
                        task: child,
                        state: desired(&reference, &values)?,
                        settled: true,
                        inputs: previous.map_or(inputs, |status| status.inputs),
                    };
                    if publish(
                        files.as_ref(),
                        &reference,
                        status,
                        &mut values,
                        &mut generation,
                    )
                    .await?
                    {
                        return Ok((reference, generation.position));
                    }
                    continue;
                }
                let cancelled = matches!(state, ChildState::Cancelled | ChildState::Archived)
                    && cancelled_outcome;
                let suspension = if cancelled
                    || (state == ChildState::Running
                        && previous.is_some_and(|status| {
                            status.settled && status.inputs == inputs && status.task == child
                        })) {
                    let host = self.runtime.task_host();
                    if cancelled {
                        cancelled_suspension
                    } else {
                        host.workflow_suspension(child, &mut None).await?
                    }
                } else {
                    None
                };
                let idle_revision = suspension
                    .as_ref()
                    .filter(|slot| slot.waiting_command.is_some() && slot.input.is_none())
                    .map(|slot| slot.revision);
                let cancelled_checkpoint = cancelled
                    && stopping::cancelled_turn_is_quiet(
                        previous,
                        child,
                        results::published(&reference, &values),
                        suspension.as_ref(),
                    );
                if cancelled && !cancelled_checkpoint {
                    // The host may be newer than this fold. Refresh the bounded
                    // result reference before checking its retained receive cut.
                    follow::refresh(
                        files.as_ref(),
                        &reference,
                        &mut cursor,
                        &mut values,
                        &mut generation,
                    )
                    .await?;
                    if !stopping::cancelled_turn_is_quiet(
                        execution(&reference, &values)?,
                        child,
                        results::published(&reference, &values),
                        suspension.as_ref(),
                    ) {
                        return Err(Error::Indeterminate(OperationId::from_bytes(
                            child.into_bytes(),
                        )));
                    }
                    continue;
                }
                let passive_control = !cancelled
                    && state != ChildState::Running
                    && (previous.is_none() || !stopping::requested(state))
                    && previous.is_none_or(|status| status.settled)
                    && matches!(
                        self.runtime
                            .task_host()
                            .recover_work(child, &mut None)
                            .await?,
                        crate::distributed::WorkPull::Idle
                    );
                if cancelled_checkpoint
                    || passive_control
                    || (settled_turn(previous, child, inputs, idle_revision)
                        && values.contains_key(&FileKey::Child {
                            reference: reference.clone(),
                            file: ChildFile::Result(inputs),
                        }))
                {
                    if state != ChildState::Running {
                        if !cancelled
                            && matches!(state, ChildState::Cancelled | ChildState::Archived)
                        {
                            self.runtime.task_host().cancel(child).await?;
                            if !matches!(
                                self.runtime.task_host().terminal_status(child).await?,
                                Some(crate::Outcome::Cancelled)
                            ) {
                                return Err(Error::Indeterminate(OperationId::from_bytes(
                                    child.into_bytes(),
                                )));
                            }
                        }
                    }
                    let status = ChildStatus {
                        task: child,
                        state,
                        settled: true,
                        inputs: previous.map_or(1, |status| status.inputs),
                    };
                    if publish(
                        files.as_ref(),
                        &reference,
                        status,
                        &mut values,
                        &mut generation,
                    )
                    .await?
                    {
                        return Ok((reference, generation.position));
                    }
                    continue;
                }
                if state == ChildState::Running {
                    self.deliver_child_inputs(files.as_ref(), &reference, child, &request, inputs)
                        .await?;
                }
                let status = ChildStatus {
                    task: child,
                    state,
                    settled: false,
                    inputs,
                };
                if state == ChildState::Running
                    && !publish(
                        files.as_ref(),
                        &reference,
                        status,
                        &mut values,
                        &mut generation,
                    )
                    .await?
                {
                    continue;
                }
                #[cfg(test)]
                #[cfg(not(acyclic_harness_loom))]
                super::cuts::stop("status", OperationId::from_bytes(child.into_bytes()))?;
                let (child_files, context) = self.child_files(child, files.as_ref()).await?;
                let journal = self.runtime.workflow_journal_with_context(&context)?;
                drop(context);
                let cancellation = acyclic_fs::CancellationToken::new();
                if stopping::requested(state) {
                    cancellation.cancel();
                }
                let child_commands = super::commands::Commands {
                    harness: self,
                    inner: &commands,
                    files: files.as_ref(),
                    reference: &reference,
                    cancellation: cancellation.clone(),
                };
                {
                    let advance = self.advance_with_children(
                        child,
                        &child_commands,
                        child_files,
                        Some((files.clone(), reference.clone())),
                    );
                    let controls = async {
                        loop {
                            tokio::select! {
                                biased;
                                change = follow::next(files.as_ref(), Query::Child(reference.clone()), &mut cursor) => {
                                    let change = change.ok_or_else(|| Error::Conflict("child follow ended".into()))?.map_err(storage)?;
                                    let input = change.file.iter().chain(&change.files).any(|file| matches!(file.key,
                                        FileKey::Child { file: ChildFile::Request | ChildFile::Inbox(_), .. }));
                                    advance_fold(&mut values, &mut generation, change);
                                    let state = desired(&reference, &values)?;
                                    if stopping::requested(state) { cancellation.cancel(); }
                                    if input {
                                        loop {
                                            let state = desired(&reference, &values)?;
                                            if stopping::requested(state) { cancellation.cancel(); }
                                            if state != ChildState::Running { break; }
                                            let status = ChildStatus { task: child, state, settled: false,
                                                inputs: files.inner.child_input_count(&reference).await.map_err(storage)? };
                                            if inputs::publish(files.as_ref(), &reference, status, &mut values, &mut generation).await? {
                                                self.deliver_child_inputs(files.as_ref(), &reference, child, &request, status.inputs).await?;
                                                break;
                                            }
                                            follow::refresh(files.as_ref(), &reference, &mut cursor, &mut values, &mut generation).await?;
                                        }
                                    }
                                }
                                _ = async { inherited.as_ref().unwrap().cancelled().await },
                                    if inherited.is_some() && !inherited_stopped => {
                                    cancellation.cancel();
                                    stopping::cancel(files.as_ref(), &reference, &mut cursor, &mut values, &mut generation).await?;
                                    inherited_stopped = true;
                                }
                            }
                        }
                    };
                    stopping::drain(advance, controls, &cancellation).await?;
                }
                let (processed, results) = self
                    .child_processed(child, journal.as_ref(), &reference, &values)
                    .await?;
                for file in results {
                    loop {
                        if values.get(&file.key) == Some(&file) {
                            break;
                        }
                        match files.inner.append(generation, &file).await {
                            Ok(next) => {
                                generation = next;
                                values.insert(file.key.clone(), file.clone());
                                break;
                            }
                            Err(crate::task_volume::Error::Conflict(_)) => {
                                follow::refresh(
                                    files.as_ref(),
                                    &reference,
                                    &mut cursor,
                                    &mut values,
                                    &mut generation,
                                )
                                .await?;
                            }
                            Err(error) => return Err(storage(error)),
                        }
                    }
                }
                let state = desired(&reference, &values)?;
                if state == ChildState::Running
                    && processed
                        < files
                            .inner
                            .child_input_count(&reference)
                            .await
                            .map_err(storage)?
                {
                    continue;
                }
                if matches!(state, ChildState::Cancelled | ChildState::Archived) {
                    self.runtime.task_host().cancel(child).await?;
                    if !matches!(
                        self.runtime.task_host().terminal_status(child).await?,
                        Some(crate::Outcome::Cancelled)
                    ) {
                        return Err(Error::Indeterminate(OperationId::from_bytes(
                            child.into_bytes(),
                        )));
                    }
                }
                let status = ChildStatus {
                    task: child,
                    state,
                    settled: true,
                    inputs: if state == ChildState::Running {
                        processed
                    } else {
                        execution(&reference, &values)?
                            .map_or(inputs, |status| status.inputs)
                            .max(processed)
                    },
                };
                if publish(
                    files.as_ref(),
                    &reference,
                    status,
                    &mut values,
                    &mut generation,
                )
                .await?
                {
                    return Ok((reference, generation.position));
                }
            }
        })
    }
}

fn fold(values: &mut BTreeMap<FileKey, FileValue>, change: Change) {
    for key in change.removed {
        values.remove(&key);
    }
    for file in change.file.into_iter().chain(change.files) {
        if matches!(
            file.key,
            FileKey::Child {
                file: ChildFile::Request | ChildFile::Inbox(_),
                ..
            }
        ) {
            continue;
        }
        if let FileKey::Child {
            reference,
            file: ChildFile::Result(turn),
        } = &file.key
        {
            result_slot::evict(values, reference, *turn);
        }
        values.insert(file.key.clone(), file);
    }
}

pub(super) fn advance_fold(
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
    change: Change,
) {
    if change.cursor.position <= generation.position {
        return;
    }
    *generation = Generation {
        volume: change.cursor.volume,
        epoch: change.cursor.epoch,
        position: change.cursor.position,
        commit: change.cursor.commit,
    };
    fold(values, change);
}

fn desired(reference: &Component, values: &BTreeMap<FileKey, FileValue>) -> Result<ChildState> {
    values
        .get(&FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Control,
        })
        .map(|file| {
            Control::parse(&file.bytes)
                .map(|control| ChildState::Running.apply(control))
                .map_err(storage)
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

fn execution(
    reference: &Component,
    values: &BTreeMap<FileKey, FileValue>,
) -> Result<Option<ChildStatus>> {
    values
        .get(&FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Status,
        })
        .map(|file| ChildStatus::parse(&file.bytes).map_err(storage))
        .transpose()
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
fn input_count(reference: &Component, values: &BTreeMap<FileKey, FileValue>) -> u64 {
    values
        .keys()
        .filter(|key| {
            matches!(key, FileKey::Child { reference: candidate,
        file: ChildFile::Request | ChildFile::Inbox(_) } if candidate == reference)
        })
        .count() as u64
}

fn settled_turn(
    previous: Option<ChildStatus>,
    child: TaskId,
    inputs: u64,
    idle_revision: Option<u64>,
) -> bool {
    // Initial receive is revision one. Each completed receive/model pair adds
    // two revisions; a pause before the first message has no completed pair.
    let expected = inputs.checked_mul(2).and_then(|value| value.checked_add(1));
    if expected.is_none() || idle_revision != expected {
        return false;
    }
    previous.is_some_and(|status| {
        status.task == child
            && status.settled
            && status.inputs == inputs
            && matches!(
                status.state,
                ChildState::Running | ChildState::Paused | ChildState::Interrupted
            )
    })
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
fn needs_work(
    reference: &Component,
    values: &BTreeMap<FileKey, FileValue>,
    stopping: bool,
) -> Result<bool> {
    needs_work_count(reference, values, stopping, input_count(reference, values))
}

fn needs_work_count(
    reference: &Component,
    values: &BTreeMap<FileKey, FileValue>,
    stopping: bool,
    inputs: u64,
) -> Result<bool> {
    let state = desired(reference, values)?;
    if stopping && !matches!(state, ChildState::Cancelled | ChildState::Archived) {
        return Ok(true);
    }
    Ok(execution(reference, values)?.is_none_or(|status| {
        !status.settled
            || status.state != state
            || state == ChildState::Running
                && (status.inputs != inputs
                    || !values.contains_key(&FileKey::Child {
                        reference: reference.clone(),
                        file: ChildFile::Result(status.inputs),
                    }))
    }))
}

// Keep a worker active until the same file follow has observed its accepted
// completion. Mixing a callback's newer status with an older control snapshot
// can reschedule stale work or consume an inbox wakeup.
fn release_completed(
    position: u64,
    active: &mut BTreeSet<Component>,
    completions: &mut BTreeMap<Component, u64>,
) -> BTreeSet<Component> {
    let ready: BTreeSet<_> = completions
        .iter()
        .filter(|(_, accepted)| **accepted <= position)
        .map(|(reference, _)| reference.clone())
        .collect();
    for reference in &ready {
        completions.remove(reference);
        active.remove(reference);
    }
    ready
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;

async fn publish<P: StreamProvider + ?Sized>(
    files: &TaskFiles<P>,
    reference: &Component,
    status: ChildStatus,
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
) -> Result<bool> {
    let file = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Status,
        },
        serde_json::to_vec(&status).map_err(storage)?,
    );
    #[cfg(test)]
    acyclic_test_support::differential::record(("child-status-publish", 1u64));
    match files.inner.append(*generation, &file).await {
        Ok(next) => {
            *generation = next;
            values.insert(file.key.clone(), file);
            Ok(true)
        }
        Err(crate::task_volume::Error::Conflict(_)) => Ok(false),
        Err(error) => Err(storage(error)),
    }
}

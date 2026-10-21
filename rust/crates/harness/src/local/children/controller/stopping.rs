use super::*;

// Controls may fail after accepting a terminal intent. Keep the original effect
// future alive until its cancellation and final publications have drained.
pub(super) async fn drain(
    effect: impl Future<Output = Result<()>>,
    controls: impl Future<Output = Result<std::convert::Infallible>>,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<()> {
    futures::pin_mut!(effect, controls);
    match futures::future::select(controls, effect).await {
        futures::future::Either::Right((result, _)) => result,
        futures::future::Either::Left((result, effect)) => {
            let error = match result {
                Err(error) => error,
                Ok(never) => match never {},
            };
            cancellation.cancel();
            #[cfg(acyclic_harness_loom)]
            if crate::task_volume::model_checking::enabled(
                crate::task_volume::model_checking::Fault::DropActiveChild,
            ) {
                return Err(error);
            }
            effect.await?;
            Err(error)
        }
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod checkpoints;
#[cfg(test)]
#[cfg(all(acyclic_harness_loom, acyclic_fs_loom))]
mod models;

pub(in crate::local::children) fn requested(state: ChildState) -> bool {
    matches!(
        state,
        ChildState::Interrupted | ChildState::Cancelled | ChildState::Archived
    )
}

// A cancelled host has released its execution reservation. Its retained receive
// slot and immutable results prove the cut before a lost status acknowledgement;
// recovery must never claim a new execution lease merely to publish that ack.
pub(super) fn cancelled_turn_is_quiet(
    previous: Option<ChildStatus>,
    child: TaskId,
    published: u64,
    suspension: Option<&crate::scheduler::WorkflowSuspension>,
) -> bool {
    if previous.is_some_and(|status| {
        status.task != child || status.inputs == 0 || published > status.inputs
    }) || (previous.is_none() && published != 0)
    {
        return false;
    }
    match suspension {
        None => {
            published == 0
                && previous.is_none_or(|status| {
                    // An unstarted child has no receive slot. Its frozen cancellation
                    // acknowledgement already proves quiescence for a later archive.
                    status.state == ChildState::Cancelled && status.settled
                })
        }
        Some(slot) => {
            slot.waiting_command.is_some()
                && published.checked_mul(2).and_then(|n| n.checked_add(1)) == Some(slot.revision)
        }
    }
}

// Parent cancellation is another file write. Retry only after the existing
// child follow supplies the competing accepted cut; never resample or poll.
pub(in crate::local::children) async fn cancel(
    files: &Files,
    reference: &Component,
    cursor: &mut follow::Follow,
    values: &mut BTreeMap<FileKey, FileValue>,
    generation: &mut Generation,
) -> Result<()> {
    let control = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Control,
        },
        Control::Cancel.bytes().to_vec(),
    );
    loop {
        if matches!(
            desired(reference, values)?,
            ChildState::Cancelled | ChildState::Archived
        ) {
            return Ok(());
        }
        match files.write(*generation, &control).await {
            Ok(next) => {
                *generation = next;
                values.insert(control.key.clone(), control.clone());
                return Ok(());
            }
            Err(crate::task_volume::Error::Conflict(_)) => {
                let change = follow::next(files, Query::Child(reference.clone()), cursor)
                    .await
                    .ok_or_else(|| Error::Conflict("inherited control follow ended".into()))?
                    .map_err(storage)?;
                advance_fold(values, generation, change);
            }
            Err(error) => return Err(storage(error)),
        }
    }
}

// Original factories: retain their native branches and concrete frame layouts.
use super::*;

pub(super) fn append<'a>(
    journal: &'a Journal,
    operation: OperationId,
    key: &'a str,
    event: &'a ExecutionEvent,
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
    crate::conversation::BoundedContentFuture::new(io, || async move {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let local = PreparationIo::from_native_work(work)?;
            let result = async {
                journal
                    .append_validated(operation, None, key, event, Some(&local))
                    .await?;
                publish_chunk(journal, key, event, Some(&local)).await
            }
            .await;
            let retained = local.owned_bytes()?;
            drop(local.into_native_custody(retained)?);
            return result.map(|_| ());
        }
        let span = obs_span!("acyclic.harness.journal.append");
        traced(span, async move {
            journal
                .append_validated(operation, None, key, event, io)
                .await?;
            if io.is_some() {
                publish_chunk(journal, key, event, io).await?;
            }
            Ok(())
        })
        .await
    })
}

pub(super) fn append_if_tail<'a>(
    journal: &'a Journal,
    operation: OperationId,
    expected_tail: u64,
    key: &'a str,
    event: &'a ExecutionEvent,
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
    crate::conversation::BoundedContentFuture::new(io, || async move {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let local = PreparationIo::from_native_work(work)?;
            let result = journal
                .append_validated(operation, Some(expected_tail), key, event, Some(&local))
                .await;
            let retained = local.owned_bytes()?;
            drop(local.into_native_custody(retained)?);
            return result;
        }
        let span = obs_span!(
            "acyclic.harness.journal.append_if_tail",
            rev = expected_tail
        );
        traced(
            span,
            journal.append_validated(operation, Some(expected_tail), key, event, io),
        )
        .await
    })
}

pub(super) fn replay<'a>(
    journal: &'a Journal,
    operation: OperationId,
    after: u64,
    maximum: u32,
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
    crate::conversation::BoundedContentFuture::new(io, || async move {
        if io.is_none() {
            let span = obs_span!(
                "acyclic.harness.journal.replay",
                rev = after,
                items = crate::obs::Empty
            );
            return traced(span, async move {
                let records = journal.records(operation, after, maximum, io).await?;
                crate::obs::obs_record!("items" = records.len());
                Ok(records)
            })
            .await;
        }
        let records = journal.records(operation, after, maximum, io).await?;
        io.ok_or_else(|| {
            crate::executor::stock_boundary(io, crate::wire::ErrorCode::Invalid, "bounded branch")
        })?
        .remaining()?;
        Ok(records)
    })
}

pub(super) fn load<'a>(
    journal: &'a Journal,
    reference: &'a FileRef,
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
    crate::conversation::BoundedContentFuture::new(io, || async move {
        if io.is_none() {
            let span = obs_span!("acyclic.harness.journal.load", bytes = crate::obs::Empty);
            return traced(span, async move {
                let bytes = journal.load_content(reference, io).await?;
                crate::obs::obs_record!("bytes" = bytes.len());
                Ok(bytes)
            })
            .await;
        }
        journal.load_content(reference, io).await
    })
}

pub(super) fn stage<'a>(
    journal: &'a Journal,
    operation: OperationId,
    key: &'a str,
    bytes: &'a [u8],
    media: &'static str,
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
    #[cfg(not(target_arch = "wasm32"))]
    let native = match io.and_then(PreparationIo::original_native_work) {
        Some(work) => {
            work.remaining()?;
            let (owner, _) = journal.owner.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native staging requires an original dispatch owner",
            })?;
            owner.require_volume_grant(
                &journal.volume,
                VolumeOperation::Write,
                &mut Some(
                    &mut io
                        .ok_or_else(|| {
                            crate::executor::stock_boundary(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "native ledger",
                            )
                        })?
                        .history_io(),
                ),
            )?;
            Some((owner, work))
        }
        None => None,
    };
    crate::conversation::BoundedContentFuture::new(io, || async move {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((owner, work)) = native {
            let local = PreparationIo::from_native_work(work)?;
            owner
                .verify(true, &mut Some(&mut local.history_io()))
                .await?;
            let file = journal
                .stage_with_io(operation, key, bytes, media, Some(&local))
                .await?;
            owner
                .verify(true, &mut Some(&mut local.history_io()))
                .await?;
            let retained = file.logical_owned_bytes()?;
            io.ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "native ledger",
                )
            })?
            .absorb_native_custody(local.into_native_custody(retained)?)?;
            return Ok(file);
        }
        let span = obs_span!("acyclic.harness.journal.stage", bytes = bytes.len());
        traced(
            span,
            journal.stage_with_io(operation, key, bytes, media, io),
        )
        .await
    })
}

// Frozen pre-atomic publication used only by the old factory baseline.
use crate::task_volume::{Channel, Component, FileKey, FileValue, TaskFile};
pub(super) async fn publish_chunk(
    journal: &Journal,
    key: &str,
    event: &ExecutionEvent,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let (Some(files), Some((owner, _))) = (&journal.task_files, &journal.owner) else {
        return Ok(());
    };
    let ExecutionEvent::Model {
        step,
        purpose: crate::executor::ModelPurpose::Response,
        ..
    } = event
    else {
        return Ok(());
    };
    let Some(model) = journal.verify_event_refs_with_io(event, io).await? else {
        return Ok(());
    };
    let (channel, delta) = match model {
        ModelEvent::Content { delta } => (Channel::Text, delta),
        ModelEvent::Reasoning { delta } => (Channel::Reasoning, delta),
        _ => return Ok(()),
    };
    let sequence = key
        .rsplit(':')
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| Error::Invalid("model event has no canonical ordinal".into()))?;
    let value = FileValue {
        requester: None,
        publication: None,
        key: FileKey::Task {
            task: Component::new(owner.task_id().to_string())?,
            file: TaskFile::Chunk {
                execution: OperationId::from_bytes([0; 16]),
                turn: u64::from(*step),
                sequence,
            },
        },
        channel: Some(channel),
        bytes: delta.into_bytes().into(),
    };
    #[cfg(not(target_arch = "wasm32"))]
    let work = io.and_then(PreparationIo::original_native_work);
    #[cfg(target_arch = "wasm32")]
    let work = None;
    loop {
        let head = files.inner.head().await?;
        match files
            .inner
            .append_if(head, &value, Vec::new(), None, work)
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

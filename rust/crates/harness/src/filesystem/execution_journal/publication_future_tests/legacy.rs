// Original factories: retain their native branches and concrete frame layouts.
use super::*;

pub(super) fn append<'a>(
    journal: &'a Journal,
    operation: OperationId,
    key: &'a str,
    event: &'a ExecutionEvent,
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
    bounded(io, || async move {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let local = PreparationIo::from_native_work(work)?;
            let result = async {
                journal
                    .append_validated(operation, None, key, event, Some(&local))
                    .await?;
                journal.publish_chunk(key, event, Some(&local)).await
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
                journal.publish_chunk(key, event, io).await?;
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
    bounded(io, || async move {
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
    bounded(io, || async move {
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
    bounded(io, || async move {
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
    bounded(io, || async move {
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

// Observe the original concrete layouts without changing their futures, stores,
// admission order or allocations. Compiler layout changes need no byte table.
thread_local! {
    static OUTER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static INNER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn bounded<'a, T, F>(
    io: Option<&'a PreparationIo>,
    build: impl FnOnce() -> F,
) -> Result<crate::conversation::BoundedContentFuture<'a, T>>
where
    F: std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
{
    OUTER.set(std::mem::size_of::<F>());
    INNER.set(0);
    crate::conversation::BoundedContentFuture::new(io, build)
}

fn traced<'a, T: 'a>(
    span: crate::obs::OperationSpan,
    future: impl std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
) -> acyclic_stream::BoxProviderFuture<'a, Result<T>> {
    let future = crate::obs::traced(span, future);
    INNER.set(std::mem::size_of_val(future.as_ref().get_ref()));
    future
}

pub(super) fn frame_bytes() -> u64 {
    (OUTER.get() + INNER.get()) as u64
}

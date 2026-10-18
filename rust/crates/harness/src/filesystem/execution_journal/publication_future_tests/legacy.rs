// Original factories: retain their native branches and concrete frame layouts.
use super::*;

thread_local! {
    static FRAME_BYTES: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
    static TRACE_BYTES: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
    static UNMETERED_BYTES: std::cell::Cell<[usize; 5]> = const { std::cell::Cell::new([0; 5]) };
}

pub(super) fn expected_saved_bytes(mode: usize) -> usize {
    FRAME_BYTES.get()[mode] + TRACE_BYTES.get()[mode] - UNMETERED_BYTES.get()[mode]
}

fn unmetered_layout(mode: usize, future: impl std::future::Future) {
    let mut sizes = UNMETERED_BYTES.get();
    sizes[mode] = std::mem::size_of_val(&future);
    UNMETERED_BYTES.set(sizes);
}

// Retained independently from both production tracing adapters. The original
// allocates this frame; the expected unmetered representation embeds it inline.
fn trace_frame<'a, T: 'a>(
    span: crate::obs::OperationSpan,
    future: impl std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
) -> impl std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a {
    let context = span.clone();
    context.scope(async move {
        let result = future.await;
        span.record("outcome", if result.is_ok() { "ok" } else { "err" });
        if let Err(error) = &result {
            span.record("error.kind", error.kind());
        }
        result
    })
}

fn original_trace<'a, T: 'a>(
    mode: usize,
    span: crate::obs::OperationSpan,
    future: impl std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
) -> acyclic_stream::BoxProviderFuture<'a, Result<T>> {
    let future = trace_frame(span, future);
    let mut sizes = TRACE_BYTES.get();
    sizes[mode] = std::mem::size_of_val(&future);
    TRACE_BYTES.set(sizes);
    Box::pin(future)
}

fn original_frame<'a, T, F>(
    mode: usize,
    io: Option<&'a PreparationIo>,
    build: impl FnOnce() -> F,
) -> Result<crate::conversation::BoundedContentFuture<'a, T>>
where
    F: std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a,
{
    let mut sizes = FRAME_BYTES.get();
    sizes[mode] = std::mem::size_of::<F>();
    FRAME_BYTES.set(sizes);
    crate::conversation::BoundedContentFuture::new(io, build)
}

pub(super) fn append<'a>(
    journal: &'a Journal,
    operation: OperationId,
    key: &'a str,
    event: &'a ExecutionEvent,
    io: Option<&'a PreparationIo>,
) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
    unmetered_layout(0, async move {
        let span = obs_span!("acyclic.harness.journal.append");
        trace_frame(
            span,
            journal.append_validated(operation, None, key, event, None),
        )
        .await
        .map(|_| ())
    });
    original_frame(0, io, || async move {
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
        original_trace(0, span, async move {
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
    unmetered_layout(1, async move {
        let span = obs_span!(
            "acyclic.harness.journal.append_if_tail",
            rev = expected_tail
        );
        trace_frame(
            span,
            journal.append_validated(operation, Some(expected_tail), key, event, None),
        )
        .await
    });
    original_frame(1, io, || async move {
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
        original_trace(
            1,
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
    unmetered_layout(2, async move {
        let span = obs_span!(
            "acyclic.harness.journal.replay",
            rev = after,
            items = crate::obs::Empty
        );
        trace_frame(span, async move {
            let records = journal.records(operation, after, maximum, None).await?;
            crate::obs::obs_record!("items" = records.len());
            Ok(records)
        })
        .await
    });
    original_frame(2, io, || async move {
        if io.is_none() {
            let span = obs_span!(
                "acyclic.harness.journal.replay",
                rev = after,
                items = crate::obs::Empty
            );
            return original_trace(2, span, async move {
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
    unmetered_layout(3, async move {
        let span = obs_span!("acyclic.harness.journal.load", bytes = crate::obs::Empty);
        trace_frame(span, async move {
            let bytes = journal.load_content(reference, None).await?;
            crate::obs::obs_record!("bytes" = bytes.len());
            Ok(bytes)
        })
        .await
    });
    original_frame(3, io, || async move {
        if io.is_none() {
            let span = obs_span!("acyclic.harness.journal.load", bytes = crate::obs::Empty);
            return original_trace(3, span, async move {
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
    unmetered_layout(4, async move {
        let span = obs_span!("acyclic.harness.journal.stage", bytes = bytes.len());
        trace_frame(
            span,
            journal.stage_with_io(operation, key, bytes, media, None),
        )
        .await
    });
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
    original_frame(4, io, || async move {
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
        original_trace(
            4,
            span,
            journal.stage_with_io(operation, key, bytes, media, io),
        )
        .await
    })
}

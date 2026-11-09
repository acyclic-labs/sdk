use super::journal_progress::{Observer, fork_chain};
use acyclic_stream::{
    AppendRequest, CommitCondition, CommitMutation, CommitOutcome, CommitRequest, FollowRequest,
    IdempotencyKey, JournalStream, JournalStreamLimits, StreamError, StreamPath, StreamProvider,
    StreamProviderExt as _,
};
use futures::Stream as _;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use wasm_bindgen::{JsCast as _, prelude::*};

/// Cancels a public conditional commit after its real storage transaction, before acknowledgement.
#[wasm_bindgen(js_name = qualifyJournalCancellation)]
pub async fn qualify_journal_cancellation(
    journal: JsValue,
    hold_acknowledgement: js_sys::Function,
) -> Result<(), JsValue> {
    let check = |condition, message| {
        if condition {
            Ok(())
        } else {
            Err(JsValue::from_str(message))
        }
    };
    let stream_error = |error: StreamError| JsValue::from_str(error.code());
    let provider = JournalStream::open(journal.clone(), JournalStreamLimits::default())
        .await
        .map_err(stream_error)?;
    let peer = JournalStream::open(journal.clone(), JournalStreamLimits::default())
        .await
        .map_err(stream_error)?;
    let path = StreamPath::new("progress/root").map_err(stream_error)?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![b"initial".as_slice().into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(b"initial".to_vec()).map_err(stream_error)?,
        })
        .await
        .map_err(stream_error)?;
    let path = fork_chain(&peer, path).await.map_err(stream_error)?;
    let request = CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: 1,
        }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![b"accepted-before-cancel".as_slice().into()],
        }],
        idempotency_key: IdempotencyKey::new(b"cancelled-conditional".to_vec())
            .map_err(stream_error)?,
        deadline_unix_millis: None,
    };
    drop(provider.commit(request.clone()));
    let mut conflicting = request.clone();
    conflicting.conditions = vec![CommitCondition::Tail {
        path: path.clone(),
        expected: 0,
    }];
    check(
        matches!(
            peer.commit(conflicting).await.map_err(stream_error)?,
            CommitOutcome::Conflict(_)
        ),
        "pre-admission refusal lost its original condition",
    )?;
    check(
        peer.tail(path.clone()).await == Ok(1),
        "unstarted journal attempt retired a live authority",
    )?;
    let mut follow = peer
        .follow(FollowRequest::new(path.clone(), 1))
        .await
        .map_err(stream_error)?;
    let observer = Arc::new(Observer(AtomicBool::new(false)));
    let waker = futures::task::waker(observer.clone());
    let mut context = Context::from_waker(&waker);
    check(
        std::pin::Pin::new(&mut follow)
            .poll_next(&mut context)
            .is_pending(),
        "cancellation follow did not park",
    )?;
    let control = hold_acknowledgement
        .call0(&JsValue::NULL)?
        .dyn_into::<js_sys::Array>()?;
    let entered =
        wasm_bindgen_futures::JsFuture::from(control.get(0).dyn_into::<js_sys::Promise>()?);
    let release = control.get(1).dyn_into::<js_sys::Function>()?;
    {
        let write = Box::pin(provider.commit(request.clone()));
        match futures::future::select(write, entered).await {
            futures::future::Either::Right((Ok(_), write)) => drop(write),
            _ => {
                return Err(JsValue::from_str(
                    "conditional commit escaped its original durability acknowledgement",
                ));
            }
        }
    }
    check(
        observer.0.load(Ordering::Acquire),
        "cancelled journal append left its public follow parked",
    )?;
    check(
        matches!(
            std::pin::Pin::new(&mut follow).poll_next(&mut context),
            Poll::Ready(Some(Err(StreamError::Unavailable)))
        ),
        "cancelled journal append retained a live authority",
    )?;
    check(
        matches!(
            std::pin::Pin::new(&mut follow).poll_next(&mut context),
            Poll::Ready(None)
        ),
        "cancelled journal follow did not terminate",
    )?;
    check(
        peer.tail(path.clone()).await == Err(StreamError::Unavailable),
        "cancelled journal admitted a stale observer",
    )?;
    // Releasing the original host acknowledgement cannot revive its retired replica.
    wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&release.call0(&JsValue::NULL)?))
        .await?;
    check(
        peer.tail(path.clone()).await == Err(StreamError::Unavailable),
        "late journal acknowledgement revived a retired authority",
    )?;
    drop(follow);
    drop(peer);
    drop(provider);
    let reopened = JournalStream::open(journal, JournalStreamLimits::default())
        .await
        .map_err(stream_error)?;
    check(
        reopened.tail(path.clone()).await == Ok(2),
        "reopen lost the cancelled caller's original transaction",
    )?;
    check(
        matches!(
            reopened.commit(request).await.map_err(stream_error)?,
            CommitOutcome::Committed(_)
        ),
        "reopen lost the original conditional commit identity",
    )?;
    check(
        reopened.tail(path).await == Ok(2),
        "retry duplicated the original conditional commit",
    )?;
    Ok(())
}

use acyclic_stream::{
    AppendRequest, FollowRequest, ForkRequest, IdempotencyKey, JournalStream, JournalStreamLimits,
    RevisionCut, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
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

pub(super) struct Observer(pub(super) AtomicBool);
impl futures::task::ArcWake for Observer {
    fn wake_by_ref(observer: &Arc<Self>) {
        observer.0.store(true, Ordering::Release);
    }
}

pub(super) async fn fork_chain(
    provider: &JournalStream,
    mut path: StreamPath,
) -> Result<StreamPath, StreamError> {
    for destination in ["progress/child", "progress/grandchild"] {
        let destination = StreamPath::new(destination)?;
        provider
            .fork(ForkRequest {
                source: path,
                destination: destination.clone(),
                at_tail: None,
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::new(destination.as_str().as_bytes().to_vec())?,
            })
            .await?;
        path = destination;
    }
    Ok(path)
}

/// Observes terminal publication through the public provider and the host's real storage.
#[wasm_bindgen(js_name = qualifyJournalProgress)]
pub async fn qualify_journal_progress(
    journal: JsValue,
    remove_domain: js_sys::Function,
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
            records: vec![b"accepted".as_slice().into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(b"journal-progress".to_vec())
                .map_err(stream_error)?,
        })
        .await
        .map_err(stream_error)?;
    let path = fork_chain(&peer, path).await.map_err(stream_error)?;
    let mut follow = provider
        .follow(FollowRequest::new(path.clone(), 1))
        .await
        .map_err(stream_error)?;
    let mut unpolled = peer
        .follow(FollowRequest::new(path.clone(), 1))
        .await
        .map_err(stream_error)?;
    let follow_observer = Arc::new(Observer(AtomicBool::new(false)));
    let follow_waker = futures::task::waker(follow_observer.clone());
    check(
        std::pin::Pin::new(&mut follow)
            .poll_next(&mut Context::from_waker(&follow_waker))
            .is_pending(),
        "follow did not park",
    )?;
    peer.append(AppendRequest {
        path: StreamPath::new("progress/root").map_err(stream_error)?,
        records: vec![b"after-fork".as_slice().into()],
        if_tail: Some(1),
        idempotency_key: IdempotencyKey::new(b"journal-unrelated".to_vec())
            .map_err(stream_error)?,
    })
    .await
    .map_err(stream_error)?;
    check(
        !follow_observer.0.load(Ordering::Acquire),
        "unrelated publication woke retirement observer",
    )?;
    let restore_domain;
    {
        let mut revision = peer
            .wait_applied(&RevisionCut::default())
            .await
            .map_err(stream_error)?;
        revision.index += 1;
        let cut = RevisionCut::of(revision);
        let waiting = std::pin::pin!(provider.wait_applied(&cut));
        let mut waiting = waiting;
        let observer = Arc::new(Observer(AtomicBool::new(false)));
        let waker = futures::task::waker(observer.clone());
        let mut context = Context::from_waker(&waker);
        check(
            waiting.as_mut().poll(&mut context).is_pending(),
            "future revision did not park",
        )?;
        // The original FIFO replica lock joins the first wait's refresh; this is
        // an ordinary public read, not a readiness probe or a timing assumption.
        let observed = std::pin::pin!(peer.tail(path.clone()));
        check(
            matches!(
                futures::future::select(waiting.as_mut(), observed).await,
                futures::future::Either::Right((Ok(1), _))
            ),
            "future revision blocked its own refresh producer",
        )?;
        observer.0.store(false, Ordering::Release);
        check(
            waiting.as_mut().poll(&mut context).is_pending(),
            "future revision did not remain parked",
        )?;
        restore_domain = remove_domain
            .call0(&JsValue::NULL)?
            .dyn_into::<js_sys::Function>()?;
        check(
            peer.tail(path.clone()).await == Err(StreamError::Unavailable),
            "lost SQLite domain did not refuse public observation",
        )?;
        check(
            observer.0.load(Ordering::Acquire)
                && matches!(
                    waiting.as_mut().poll(&mut context),
                    Poll::Ready(Err(StreamError::Unavailable))
                ),
            "journal failure left a public revision parked",
        )?;
        check(
            follow_observer.0.load(Ordering::Acquire),
            "journal failure left a public follow parked",
        )?;
        for cursor in [&mut follow, &mut unpolled] {
            check(
                matches!(
                    std::pin::Pin::new(&mut *cursor).poll_next(&mut context),
                    Poll::Ready(Some(Err(StreamError::Unavailable)))
                ),
                "journal failure left a retained follow parked",
            )?;
            check(
                matches!(
                    std::pin::Pin::new(cursor).poll_next(&mut context),
                    Poll::Ready(None)
                ),
                "failed journal follow did not terminate",
            )?;
        }
        check(
            matches!(
                provider.follow(FollowRequest::new(path, 1)).await,
                Err(StreamError::Unavailable)
            ),
            "failed journal admitted a later follow",
        )?;
    }
    drop(follow);
    drop(unpolled);
    drop(peer);
    drop(provider);
    restore_domain.call0(&JsValue::NULL)?;
    let reopened = JournalStream::open(journal, JournalStreamLimits::default())
        .await
        .map_err(stream_error)?;
    check(
        reopened
            .tail(StreamPath::new("progress/grandchild").map_err(stream_error)?)
            .await
            == Ok(1),
        "reopened journal lost the original inherited cut",
    )?;
    check(
        reopened
            .tail(StreamPath::new("progress/root").map_err(stream_error)?)
            .await
            == Ok(2),
        "reopened journal lost its committed root",
    )?;
    Ok(())
}

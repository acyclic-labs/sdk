use super::{Mutex, StreamError, gate};
use std::{
    collections::VecDeque,
    future::poll_fn,
    task::{Context, Poll, Waker},
};

struct State<T> {
    frames: VecDeque<T>,
    closed: bool,
    reader: Option<Waker>,
}

pub(super) struct Outbound<T> {
    // Data reserves its original input credit before reaching this queue.
    // Controls must remain sendable without it, including the grants and
    // cancellations that let a stalled session make progress.
    // Lock order: never nested with another SDK lock, including closed's gate.
    // Wake callbacks and queued-value destruction happen after unlocking.
    state: Mutex<State<T>>,
    maximum_frame_bytes: usize,
    closed: gate::Gate,
}

impl<T> Outbound<T> {
    pub(super) fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            state: Mutex::new(State {
                frames: VecDeque::new(),
                closed: false,
                reader: None,
            }),
            maximum_frame_bytes,
            closed: gate::Gate::new(),
        }
    }
    pub(super) fn push(&self, value: T, bytes: usize) -> Result<(), StreamError> {
        let wake = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            if state.closed {
                return Err(StreamError::Unavailable);
            }
            if bytes > self.maximum_frame_bytes {
                return Err(StreamError::LimitExceeded);
            }
            state.frames.push_back(value);
            state.reader.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, StreamError>> {
        let wake = cx.waker().clone();
        let previous = {
            let Ok(mut state) = self.state.lock() else {
                return Poll::Ready(Err(StreamError::Unavailable));
            };
            if state.closed {
                return Poll::Ready(Err(StreamError::Unavailable));
            }
            if let Some(value) = state.frames.pop_front() {
                return Poll::Ready(Ok(value));
            }
            state.reader.replace(wake)
        };
        drop(previous);
        Poll::Pending
    }
    pub(super) async fn next(&self) -> Result<T, StreamError> {
        poll_fn(|cx| self.poll(cx)).await
    }
    pub(super) fn close(&self) -> Result<(), StreamError> {
        let (frames, wake) = {
            let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
            state.closed = true;
            (std::mem::take(&mut state.frames), state.reader.take())
        };
        self.closed.set(true)?;
        if let Some(wake) = wake {
            wake.wake();
        }
        drop(frames);
        Ok(())
    }
    pub(super) async fn cancelled(&self) -> Result<(), StreamError> {
        self.closed.changed(false).await
    }
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::session::{self, wire};
    use crate::{AppendRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProviderExt};
    use futures::{StreamExt, stream};
    use prost::Message;
    use std::future::Future as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct WakeCount(AtomicUsize);
    impl futures::task::ArcWake for WakeCount {
        fn wake_by_ref(owner: &Arc<Self>) {
            owner.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn queued_control_burst_preserves_delivery_and_close_wakes_readers()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let provider = Arc::new(MemoryStream::default());
        let path = StreamPath::new("queued/events")?;
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec!["published".into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let queue = Arc::new(Outbound::new(crate::MAX_COMMAND_BYTES));
        let push = |body| {
            let frame = wire::SessionFrame {
                body: Some(body),
                ..Default::default()
            };
            let bytes = frame.encoded_len();
            queue.push(frame, bytes)
        };
        push(wire::session_frame::Body::Hello(wire::SessionHello {
            version: session::VERSION,
            protocol: Some(session::protocol_handshake()),
            client_id: bytes::Bytes::from_static(b"queued-control-client"),
            credentials: vec![session::testing::credential()],
            receive_bytes: 1,
            receive_frames: 1,
            ..Default::default()
        }))?;
        // The transport has consumed nothing. Each grant is a valid independent
        // control frame; their count is unrelated to a logical record page.
        for _ in 0..=crate::MAX_ITEMS {
            push(wire::session_frame::Body::Window(wire::SessionWindow {
                bytes: crate::MAX_COMMAND_BYTES as u64,
                frames: 1,
            }))?;
        }
        push(wire::session_frame::Body::Interest(wire::SessionInterest {
            revision: 1,
            upsert: vec![wire::SessionSubscription {
                channel: Some(wire::SessionChannel {
                    channel_id: 1,
                    generation: 1,
                }),
                credential: 0,
                resume: Some(Default::default()),
                kind: Some(wire::session_subscription::Kind::Follow(
                    wire::SessionFollow {
                        resource: crate::wire_codec::session::stream_resource(path.as_str()),
                        delivery: wire::SessionDelivery::All as i32,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }],
            remove: Vec::new(),
        }))?;
        let credit = wire::SessionFrame {
            channel_id: 1,
            generation: 1,
            body: Some(wire::session_frame::Body::Window(wire::SessionWindow {
                bytes: crate::MAX_COMMAND_BYTES as u64,
                frames: 1,
            })),
        };
        let bytes = credit.encoded_len();
        queue.push(credit, bytes)?;
        let input = stream::unfold(queue.clone(), |queue| async move {
            queue.next().await.ok().map(|frame| (Ok(frame), queue))
        })
        .boxed();
        let mut output = session::serve(
            provider,
            session::testing::authority(),
            "node".into(),
            session::Limits::default(),
            input,
        )?;
        loop {
            let frame = output
                .next()
                .await
                .ok_or("session ended before delivery")??;
            if let Some(wire::session_frame::Body::Data(wire::SessionData {
                item: Some(wire::session_data::Item::Record(record)),
                ..
            })) = frame.into_wire().body
            {
                assert_eq!(record.sequence, 0);
                assert_eq!(record.value.as_ref(), b"published");
                break;
            }
        }
        let reader = queue.next();
        let closed = queue.cancelled();
        futures::pin_mut!(reader, closed);
        let reader_wakes = Arc::new(WakeCount::default());
        let closed_wakes = Arc::new(WakeCount::default());
        let reader_waker = futures::task::waker(reader_wakes.clone());
        let closed_waker = futures::task::waker(closed_wakes.clone());
        assert!(
            reader
                .as_mut()
                .poll(&mut Context::from_waker(&reader_waker))
                .is_pending()
        );
        assert!(
            closed
                .as_mut()
                .poll(&mut Context::from_waker(&closed_waker))
                .is_pending()
        );
        queue.close()?;
        assert!(reader_wakes.0.load(Ordering::SeqCst) > 0);
        assert!(closed_wakes.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(reader.await, Err(StreamError::Unavailable)));
        closed.await?;
        assert!(matches!(
            push(wire::session_frame::Body::Window(Default::default())),
            Err(StreamError::Unavailable)
        ));
        Ok(())
    }
}

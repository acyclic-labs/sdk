//! Fixed-capacity batch handoff from a blocking reader thread to an async
//! stream. Batches and their storage circulate through fixed arrays, so a
//! warm handoff never allocates.
use arrayvec::ArrayVec;
use std::io;
use std::task::{Context, Poll, Waker};

#[cfg(acyclic_harness_tui_loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(acyclic_harness_tui_loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

/// Queued batches before the reader waits for the stream.
const QUEUED: usize = 2;
/// Storage in circulation: queued batches, one being filled, the stream's
/// current batch, and the one it holds while swapping. The reader never lacks
/// storage, so nothing is allocated after construction.
pub(crate) const BUFFERS: usize = QUEUED + 3;

pub(crate) type Batch = io::Result<Vec<u8>>;

pub(crate) enum Message {
    Batch(Batch),
    Resized,
}

struct State {
    ready: ArrayVec<Batch, QUEUED>,
    spare: ArrayVec<Vec<u8>, BUFFERS>,
    waker: Option<Waker>,
    /// The stream is gone: the reader stops at its next send.
    closed: bool,
    /// The reader is gone: the stream ends once `ready` drains.
    finished: bool,
    resized: bool,
}

/// Lock order: this lock is a leaf. Wakers are woken after it is released.
pub(crate) struct Handoff {
    state: Mutex<State>,
    space: Condvar,
}

impl Handoff {
    pub(crate) fn new(batch: usize) -> Self {
        Self {
            state: Mutex::new(State {
                ready: ArrayVec::new(),
                spare: std::iter::repeat_with(|| vec![0; batch])
                    .take(BUFFERS)
                    .collect(),
                waker: None,
                closed: false,
                finished: false,
                resized: false,
            }),
            space: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reader: circulating storage, or `None` once the stream is gone.
    pub(crate) fn storage(&self) -> Option<Vec<u8>> {
        let mut state = self.lock();
        if state.closed {
            return None;
        }
        Some(state.spare.pop().unwrap_or_default())
    }

    /// Reader: queue a batch, waiting while the queue is full. False once the
    /// stream is gone.
    pub(crate) fn send(&self, batch: Batch) -> bool {
        let mut state = self.lock();
        while state.ready.is_full() && !state.closed {
            state = self
                .space
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if state.closed {
            return false;
        }
        state.ready.push(batch);
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// Reader: coalesce window changes without allocating or waiting for queue space.
    pub(crate) fn signal_resize(&self) {
        let mut state = self.lock();
        state.resized = true;
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Reader: no further batches.
    pub(crate) fn finish(&self) {
        let mut state = self.lock();
        state.finished = true;
        let waker = state.waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Stream: the next batch; `None` once the reader finished and the queue
    /// drained. The waker is registered under the same lock as the check.
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        let mut state = self.lock();
        // A continuous byte producer must not starve a pending window change.
        if std::mem::take(&mut state.resized) {
            return Poll::Ready(Some(Message::Resized));
        }
        if let Some(batch) = state.ready.pop_at(0) {
            drop(state);
            self.space.notify_one();
            return Poll::Ready(Some(Message::Batch(batch)));
        }
        if state.finished {
            return Poll::Ready(None);
        }
        match &mut state.waker {
            Some(waker) => waker.clone_from(cx.waker()),
            empty => *empty = Some(cx.waker().clone()),
        }
        Poll::Pending
    }

    /// Stream: return spent storage for the reader to fill again.
    pub(crate) fn recycle(&self, storage: Vec<u8>) {
        if storage.capacity() != 0 {
            let _ = self.lock().spare.try_push(storage);
        }
    }

    /// Stream: stop the reader at its next send.
    pub(crate) fn close(&self) {
        self.lock().closed = true;
        self.space.notify_all();
    }

    /// Negative control: close that never notifies a send waiting for space.
    #[cfg(acyclic_harness_tui_loom)]
    fn close_unnotified(&self) {
        self.lock().closed = true;
    }

    #[cfg(acyclic_harness_tui_loom)]
    fn register_split(&self, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        // Negative control: the empty check and the registration in separate
        // critical sections lose a send between them.
        let (empty, finished) = {
            let state = self.lock();
            (state.ready.is_empty() && !state.resized, state.finished)
        };
        if !empty {
            return self.poll_recv(cx);
        }
        if finished {
            return Poll::Ready(None);
        }
        loom::thread::yield_now();
        self.lock().waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(all(test, acyclic_harness_tui_loom))]
mod tests {
    use super::*;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    struct Flag(AtomicBool);
    impl std::task::Wake for Flag {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Unparks the stream thread. Only the correct handoff parks on it: a lost
    /// wake there would be a loom deadlock rather than an assertion.
    struct Unpark(loom::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }

    // The reader sends what the queue holds, then finishes; it never waits.
    // When the stream parks, it waits for the reader to exit: by then the
    // reader must have woken it, or the park would last forever.
    fn wake_model(broken: bool) {
        loom::model(move || {
            let handoff = Arc::new(Handoff::new(1));
            let reader = Arc::clone(&handoff);
            let mut thread = Some(loom::thread::spawn(move || {
                for key in 0..QUEUED as u8 {
                    let mut batch = reader.storage().unwrap();
                    batch[0] = key;
                    assert!(reader.send(Ok(batch)));
                }
                reader.signal_resize();
                reader.finish();
            }));
            let flag = std::sync::Arc::new(Flag(AtomicBool::new(false)));
            let waker = Waker::from(std::sync::Arc::clone(&flag));
            let mut cx = Context::from_waker(&waker);
            let mut received = Vec::new();
            let mut resizes = 0;
            loop {
                let next = if broken {
                    handoff.register_split(&mut cx)
                } else {
                    handoff.poll_recv(&mut cx)
                };
                match next {
                    Poll::Ready(Some(Message::Batch(batch))) => {
                        let batch = batch.unwrap();
                        received.push(batch[0]);
                        handoff.recycle(batch);
                    }
                    Poll::Ready(Some(Message::Resized)) => resizes += 1,
                    Poll::Ready(None) => break,
                    Poll::Pending => {
                        let reader = thread.take().expect("parked after the reader finished");
                        reader.join().unwrap();
                        assert!(flag.0.swap(false, Ordering::SeqCst), "park never woken");
                    }
                }
            }
            if let Some(reader) = thread {
                reader.join().unwrap();
            }
            assert_eq!(received, [0, 1]);
            assert_eq!(resizes, 1);
        });
    }

    #[test]
    fn every_batch_arrives_and_every_park_is_woken() {
        wake_model(false);
    }

    #[test]
    fn negative_control_detects_a_registration_split_from_its_check() {
        assert!(std::panic::catch_unwind(|| wake_model(true)).is_err());
    }

    fn resize_model(broken: bool) {
        loom::model(move || {
            let handoff = Arc::new(Handoff::new(1));
            let reader = Arc::clone(&handoff);
            let producer = loom::thread::spawn(move || {
                reader.signal_resize();
                reader.finish();
            });
            let flag = std::sync::Arc::new(Flag(AtomicBool::new(false)));
            let waker = Waker::from(std::sync::Arc::clone(&flag));
            let mut cx = Context::from_waker(&waker);
            let next = if broken {
                handoff.register_split(&mut cx)
            } else {
                handoff.poll_recv(&mut cx)
            };
            let pending = next.is_pending();
            producer.join().unwrap();
            if pending {
                assert!(flag.0.load(Ordering::SeqCst), "resize lost its wake");
                assert!(matches!(
                    handoff.poll_recv(&mut cx),
                    Poll::Ready(Some(Message::Resized))
                ));
            } else {
                assert!(matches!(next, Poll::Ready(Some(Message::Resized))));
            }
            assert!(matches!(handoff.poll_recv(&mut cx), Poll::Ready(None)));
        });
    }

    #[test]
    fn resize_and_finish_cannot_lose_the_waiting_stream() {
        resize_model(false);
    }

    #[test]
    fn negative_control_detects_resize_registration_split() {
        assert!(std::panic::catch_unwind(|| resize_model(true)).is_err());
    }

    fn resize_priority_model(bytes_first: bool) {
        loom::model(move || {
            let handoff = Handoff::new(1);
            for _ in 0..QUEUED {
                assert!(handoff.send(Ok(handoff.storage().unwrap())));
            }
            handoff.signal_resize();
            handoff.signal_resize();
            let mut cx = Context::from_waker(Waker::noop());
            let next = if bytes_first {
                // Negative control: always-ready bytes hide the resize flag.
                handoff.lock().ready.pop_at(0).map(Message::Batch)
            } else {
                match handoff.poll_recv(&mut cx) {
                    Poll::Ready(message) => message,
                    Poll::Pending => panic!("resize parked"),
                }
            };
            assert!(
                matches!(next, Some(Message::Resized)),
                "bytes starved resize"
            );
            for _ in 0..QUEUED {
                assert!(matches!(
                    handoff.poll_recv(&mut cx),
                    Poll::Ready(Some(Message::Batch(_)))
                ));
            }
            handoff.finish();
            assert!(
                matches!(handoff.poll_recv(&mut cx), Poll::Ready(None)),
                "resize did not coalesce"
            );
        });
    }

    #[test]
    fn resize_precedes_a_full_byte_queue_and_coalesces() {
        resize_priority_model(false);
    }

    #[test]
    fn negative_control_detects_byte_queue_starving_resize() {
        assert!(std::panic::catch_unwind(|| resize_priority_model(true)).is_err());
    }

    // The queue is full before the producer starts, so its send waits for
    // space that never comes; only the close can release it.
    fn close_model(broken: bool) {
        loom::model(move || {
            let handoff = Arc::new(Handoff::new(1));
            for _ in 0..QUEUED {
                let batch = handoff.storage().unwrap();
                assert!(handoff.send(Ok(batch)));
            }
            let producer = Arc::clone(&handoff);
            let thread = loom::thread::spawn(move || {
                if let Some(batch) = producer.storage() {
                    assert!(!producer.send(Ok(batch)), "sent past a full queue");
                }
            });
            if broken {
                handoff.close_unnotified();
            } else {
                handoff.close();
            }
            thread.join().unwrap();
        });
    }

    #[test]
    fn close_releases_a_send_blocked_on_a_full_queue() {
        close_model(false);
    }

    /// Runs the broken close model; loom reports its permanent wait as a
    /// deadlock and aborts, so the negative control below runs it in a child.
    #[test]
    fn unnotified_close_child() {
        if std::env::var_os("ACYCLIC_HANDOFF_UNNOTIFIED_CLOSE").is_some() {
            close_model(true);
        }
    }

    #[test]
    fn negative_control_detects_a_close_that_never_notifies_the_blocked_send() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "term::handoff::tests::unnotified_close_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("ACYCLIC_HANDOFF_UNNOTIFIED_CLOSE", "1")
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "the blocked send was released");
        assert!(stderr.contains("deadlock"), "{stderr}");
    }

    // The reader sends one batch more than the queue holds, so its last send
    // can wait for space; the stream takes the first batch and closes. The
    // waiting send must be released, either by the space or by the close.
    #[test]
    fn a_full_queue_waits_for_space_and_close_releases_it() {
        loom::model(|| {
            let handoff = Arc::new(Handoff::new(1));
            let reader = Arc::clone(&handoff);
            let thread = loom::thread::spawn(move || {
                for key in 0..=QUEUED as u8 {
                    let Some(mut batch) = reader.storage() else {
                        break;
                    };
                    batch[0] = key;
                    if !reader.send(Ok(batch)) {
                        break;
                    }
                }
                reader.finish();
            });
            let waker = Waker::from(std::sync::Arc::new(Unpark(loom::thread::current())));
            let mut cx = Context::from_waker(&waker);
            loop {
                match handoff.poll_recv(&mut cx) {
                    Poll::Ready(Some(Message::Batch(batch))) => {
                        assert_eq!(batch.unwrap()[0], 0);
                        break;
                    }
                    Poll::Ready(Some(Message::Resized)) => panic!("unexpected resize"),
                    Poll::Ready(None) => panic!("reader finished early"),
                    Poll::Pending => loom::thread::park(),
                }
            }
            handoff.close();
            thread.join().unwrap();
        });
    }
}

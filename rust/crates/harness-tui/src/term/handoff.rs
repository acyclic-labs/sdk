//! Fixed-capacity batch handoff from a blocking reader thread to an async
//! stream. Batches and their storage circulate through fixed arrays, so a
//! warm handoff never allocates.
use acyclic_native_runtime::input_batch::InputBatch;
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

pub(crate) type Batch = io::Result<InputBatch>;

struct State {
    ready: ArrayVec<Batch, QUEUED>,
    spare: ArrayVec<InputBatch, BUFFERS>,
    waker: Option<Waker>,
    /// The stream is gone: the reader stops at its next send.
    closed: bool,
    /// The reader is gone: the stream ends once `ready` drains.
    finished: bool,
}

/// Lock order: this lock is a leaf. Wakers are woken after it is released.
pub(crate) struct Handoff {
    state: Mutex<State>,
    space: Condvar,
    batch: usize,
}

impl Handoff {
    pub(crate) fn new(batch: usize) -> Self {
        Self {
            state: Mutex::new(State {
                ready: ArrayVec::new(),
                spare: std::iter::repeat_with(|| InputBatch::new(batch))
                    .take(BUFFERS)
                    .collect(),
                waker: None,
                closed: false,
                finished: false,
            }),
            space: Condvar::new(),
            batch,
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reader: circulating storage, or `None` once the stream is gone.
    pub(crate) fn storage(&self) -> Option<InputBatch> {
        let mut state = self.lock();
        if state.closed {
            return None;
        }
        let spare = state.spare.pop();
        drop(state);
        Some(spare.unwrap_or_else(|| InputBatch::new(self.batch)))
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
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<Batch>> {
        let mut state = self.lock();
        if let Some(batch) = state.ready.pop_at(0) {
            drop(state);
            self.space.notify_one();
            return Poll::Ready(Some(batch));
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
    pub(crate) fn recycle(&self, mut storage: InputBatch) {
        if !storage.storage_mut().is_empty() {
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
    fn register_split(&self, cx: &mut Context<'_>) -> Poll<Option<Batch>> {
        // Negative control: the empty check and the registration in separate
        // critical sections lose a send between them.
        let (empty, finished) = {
            let state = self.lock();
            (state.ready.is_empty(), state.finished)
        };
        if !empty {
            let batch = self.lock().ready.pop_at(0);
            self.space.notify_one();
            return Poll::Ready(batch);
        }
        if finished {
            return Poll::Ready(None);
        }
        loom::thread::yield_now();
        self.lock().waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(all(test, not(acyclic_harness_tui_loom)))]
mod reuse_tests {
    use super::*;

    #[test]
    fn recycled_batches_match_independent_vec_bytes_errors_and_order() {
        use super::*;
        const BATCH: usize = 1024;
        use std::{
            io,
            task::{Context, Poll, Waker},
        };
        acyclic_test_support::differential(
            || {
                let mut storage = vec![0; BATCH];
                let mut output = Vec::new();
                let mut trace = Vec::new();
                for read in [
                    Ok(b"a".as_slice()),
                    Ok(b"four".as_slice()),
                    Err(io::ErrorKind::PermissionDenied),
                ] {
                    storage.resize(BATCH, 0);
                    trace.push("read");
                    let result = read
                        .map(|bytes| {
                            storage[..bytes.len()].copy_from_slice(bytes);
                            storage.truncate(bytes.len());
                            storage.clone()
                        })
                        .map_err(|kind| (kind, io::Error::from(kind).to_string()));
                    trace.push("publish");
                    output.push(result);
                    trace.push("receive");
                }
                (output, trace)
            },
            || {
                let channel = Handoff::new(BATCH);
                let mut output = Vec::new();
                let mut trace = Vec::new();
                let mut cx = Context::from_waker(Waker::noop());
                for read in [
                    Ok(b"a".as_slice()),
                    Ok(b"four".as_slice()),
                    Err(io::ErrorKind::PermissionDenied),
                ] {
                    let mut storage = channel.storage().unwrap();
                    trace.push("read");
                    let result = read.map_err(io::Error::from).and_then(|bytes| {
                        storage.storage_mut()[..bytes.len()].copy_from_slice(bytes);
                        storage.finish(bytes.len())
                    });
                    assert!(channel.send(result));
                    trace.push("publish");
                    let Poll::Ready(Some(result)) = channel.poll_recv(&mut cx) else {
                        panic!("missing batch")
                    };
                    output.push(
                        result
                            .map(|storage| {
                                let bytes = storage.bytes().to_vec();
                                channel.recycle(storage);
                                bytes
                            })
                            .map_err(|error| (error.kind(), error.to_string())),
                    );
                    trace.push("receive");
                }
                (output, trace)
            },
        );
    }

    fn reinitialized_after_one_byte(broken: bool) -> usize {
        const SIZE: usize = 1024;
        let handoff = Handoff::new(SIZE);
        let mut storage = handoff.storage().unwrap();
        storage.storage_mut().fill(0xa5);
        // Darwin initializes its native condition variable on first notification.
        handoff.space.notify_one();
        let mut changed = 0;
        let allocations = acyclic_test_support::measure(|| {
            assert!(handoff.send(storage.finish(1)));
            let mut cx = Context::from_waker(Waker::noop());
            let Poll::Ready(Some(Ok(storage))) = handoff.poll_recv(&mut cx) else {
                panic!("published input disappeared");
            };
            assert_eq!(storage.bytes(), [0xa5]);
            handoff.recycle(storage);
            let mut storage = handoff.storage().unwrap();
            if broken {
                storage.storage_mut()[1..].fill(0);
            }
            assert_eq!(storage.storage_mut().len(), SIZE);
            changed = storage
                .storage_mut()
                .iter()
                .filter(|byte| **byte != 0xa5)
                .count();
        });
        assert_eq!(allocations.count_total, 0);
        handoff.finish();
        handoff.close();
        changed
    }

    #[test]
    fn recycled_one_byte_reads_initialize_exactly_zero_bytes() {
        assert_eq!(reinitialized_after_one_byte(false), 0);
    }

    #[test]
    fn negative_control_detects_reinitializing_the_recycled_tail() {
        assert_eq!(reinitialized_after_one_byte(true), 1023);
        assert!(
            std::panic::catch_unwind(|| assert_eq!(reinitialized_after_one_byte(true), 0)).is_err()
        );
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
                    batch.storage_mut()[0] = key;
                    assert!(reader.send(batch.finish(1)));
                }
                reader.finish();
            }));
            let flag = std::sync::Arc::new(Flag(AtomicBool::new(false)));
            let waker = Waker::from(std::sync::Arc::clone(&flag));
            let mut cx = Context::from_waker(&waker);
            let mut received = Vec::new();
            loop {
                let next = if broken {
                    handoff.register_split(&mut cx)
                } else {
                    handoff.poll_recv(&mut cx)
                };
                match next {
                    Poll::Ready(Some(batch)) => {
                        let batch = batch.unwrap();
                        received.push(batch.bytes()[0]);
                        handoff.recycle(batch);
                    }
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

    // The queue is full before the producer starts, so its send waits for
    // space that never comes; only the close can release it.
    fn close_model(broken: bool) {
        loom::model(move || {
            let handoff = Arc::new(Handoff::new(1));
            for _ in 0..QUEUED {
                let batch = handoff.storage().unwrap();
                assert!(handoff.send(batch.finish(1)));
            }
            let producer = Arc::clone(&handoff);
            let thread = loom::thread::spawn(move || {
                if let Some(batch) = producer.storage() {
                    assert!(!producer.send(batch.finish(1)), "sent past a full queue");
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
                    batch.storage_mut()[0] = key;
                    if !reader.send(batch.finish(1)) {
                        break;
                    }
                }
                reader.finish();
            });
            let waker = Waker::from(std::sync::Arc::new(Unpark(loom::thread::current())));
            let mut cx = Context::from_waker(&waker);
            loop {
                match handoff.poll_recv(&mut cx) {
                    Poll::Ready(Some(batch)) => {
                        assert_eq!(batch.unwrap().storage_mut()[0], 0);
                        break;
                    }
                    Poll::Ready(None) => panic!("reader finished early"),
                    Poll::Pending => loom::thread::park(),
                }
            }
            handoff.close();
            thread.join().unwrap();
        });
    }
}

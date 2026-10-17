//! Windows input: console input cannot complete through IOCP, so each stream
//! owns one persistent waiter. It waits on the console's close wake and the
//! console input handle together, reads through the console's input fence,
//! and hands filled batches over a bounded channel. Batch storage circulates
//! between the waiter and the stream, so a warm batch allocates nothing and
//! is decoded where the console wrote it.
use std::io;
use std::os::windows::io::AsRawHandle;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::thread::{self, JoinHandle};

use acyclic_native_runtime::terminal as ffi;

use super::super::Tty;
use super::super::handoff::{Handoff, Message};
use super::{Chunk, Events};
use crate::key_event::InputEvent;

const BATCH: usize = 1024;

/// The persistent waiter, its handoff and the stream's current batch.
struct Waiter {
    handoff: Arc<Handoff>,
    current: Vec<u8>,
    thread: Option<JoinHandle<()>>,
}

impl Waiter {
    /// `read` fills the slice and must return once the wake it waits on is
    /// signalled; a failed read ends the waiter after delivering its error.
    fn spawn(
        mut read: impl FnMut(&mut [u8]) -> io::Result<ffi::ConsoleRead> + Send + 'static,
    ) -> io::Result<Self> {
        let handoff = Arc::new(Handoff::new(BATCH));
        let reader = Arc::clone(&handoff);
        let thread = thread::Builder::new()
            .name("terminal-input".into())
            .spawn(move || pump(&mut read, &reader))?;
        Ok(Self {
            handoff,
            current: Vec::new(),
            thread: Some(thread),
        })
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Chunk<'_>>> {
        match ready!(self.handoff.poll_recv(cx)) {
            Some(Message::Batch(Ok(batch))) => {
                let spent = std::mem::replace(&mut self.current, batch);
                self.handoff.recycle(spent);
                Poll::Ready(Ok(Chunk::Bytes(&self.current)))
            }
            Some(Message::Batch(Err(error))) => Poll::Ready(Err(error)),
            Some(Message::Resized) => Poll::Ready(Ok(Chunk::Resized)),
            None => Poll::Ready(Ok(Chunk::Bytes(&[]))),
        }
    }

    /// Join after the console's input fence is closed: the waiter's next read
    /// is refused, and a send waiting for space returns once closed.
    fn join(&mut self) {
        self.handoff.close();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        self.join();
    }
}

/// Fill circulating storage and hand it over until a read fails or the
/// stream is gone.
fn pump(read: &mut impl FnMut(&mut [u8]) -> io::Result<ffi::ConsoleRead>, handoff: &Handoff) {
    while let Some(mut batch) = handoff.storage() {
        batch.resize(BATCH, 0);
        let batch = match read(&mut batch) {
            Ok(ffi::ConsoleRead::Resized) => {
                handoff.recycle(batch);
                handoff.signal_resize();
                continue;
            }
            Ok(ffi::ConsoleRead::Bytes(count)) => {
                batch.truncate(count);
                Ok(batch)
            }
            Err(error) => Err(error),
        };
        let failed = batch.is_err();
        if !handoff.send(batch) || failed {
            break;
        }
    }
    handoff.finish();
}

pub(super) struct Source {
    waiter: Waiter,
    tty: Tty,
}

impl Tty {
    /// Follow input through the decoder retained by negotiation. Must be
    /// called within a Tokio runtime with time enabled.
    pub fn events(mut self) -> io::Result<Events> {
        let reader = self.reader.try_clone()?;
        let console = Arc::clone(&self.console);
        let waiter = Waiter::spawn(move |bytes| {
            console.input.read(|| {
                ffi::console_read(
                    reader.as_raw_handle() as usize,
                    bytes,
                    None,
                    Some(&console.closed),
                )
            })
        })?;
        let decoder = std::mem::take(&mut self.decoder);
        Events::new(Source { waiter, tty: self }, decoder)
    }
}

impl Source {
    pub(super) fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Chunk<'_>>> {
        self.waiter.poll_read(cx)
    }

    pub(super) fn size(&self) -> io::Result<(u16, u16)> {
        crossterm::terminal::size()
    }

    pub(super) fn intercept(&mut self, _event: &InputEvent) -> io::Result<bool> {
        Ok(false)
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        // Restoration signals the wake, closes the input fence and waits for
        // the admitted read before undoing modes; then the waiter exits.
        let _ = self.tty.restore();
        self.waiter.join();
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::handoff::BUFFERS;
    use super::*;
    use std::future::Future;
    use std::pin::pin;
    use std::task::Waker;
    use std::time::{Duration, Instant};

    fn pending(future: impl Future) {
        let mut cx = Context::from_waker(Waker::noop());
        assert!(pin!(future).poll(&mut cx).is_pending());
    }

    #[test]
    fn dropping_a_waiting_stream_leaves_no_thread_blocked() -> io::Result<()> {
        let input = Arc::new(ffi::ConsoleWake::new()?);
        let close = Arc::new(ffi::ConsoleWake::new()?);
        let (handle, wake) = (Arc::clone(&input), Arc::clone(&close));
        let mut waiter = Waiter::spawn(move |bytes| {
            ffi::console_read(handle.as_raw_handle() as usize, bytes, None, Some(&wake))
        })?;
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            pending(std::future::poll_fn(|cx| {
                waiter.poll_read(cx).map_ok(|chunk| match chunk {
                    Chunk::Bytes(bytes) => bytes.len(),
                    Chunk::Resized => 0,
                })
            }));
        });
        close.signal()?;
        drop(waiter);
        // The close handshake: the waiter released its clones before join returned.
        assert_eq!(Arc::strong_count(&close), 1);
        assert_eq!(Arc::strong_count(&input), 1);
        Ok(())
    }

    #[test]
    fn negative_control_detects_spawn_blocking_reader_left_blocked() -> io::Result<()> {
        let input = Arc::new(ffi::ConsoleWake::new()?);
        let close = Arc::new(ffi::ConsoleWake::new()?);
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let handle = Arc::clone(&input);
        // The removed path: one blocking task per batch with a 24-hour deadline.
        let read = runtime.spawn_blocking(move || {
            ffi::console_read(
                handle.as_raw_handle() as usize,
                &mut [0; 1024],
                Some(Instant::now() + Duration::from_secs(86_400)),
                None,
            )
        });
        runtime.block_on(async { pending(read) });
        close.signal()?;
        assert_eq!(
            Arc::strong_count(&input),
            2,
            "blocked reader still holds input"
        );
        input.signal()?; // Release the blocked thread so the runtime can stop.
        runtime.shutdown_timeout(Duration::from_secs(5));
        assert_eq!(Arc::strong_count(&input), 1);
        Ok(())
    }

    struct Wakes(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn idle_waiter_has_zero_wakeups_and_reports_handoff_latency() -> io::Result<()> {
        let (keys, typed) = std::sync::mpsc::channel::<Vec<u8>>();
        let typed = std::sync::Mutex::new(typed);
        let mut waiter = Waiter::spawn(move |bytes| {
            let typed = typed.lock().map_err(|_| io::ErrorKind::Other)?;
            let key = typed.recv().map_err(|_| io::ErrorKind::NotConnected)?;
            bytes[..key.len()].copy_from_slice(&key);
            Ok(ffi::ConsoleRead::Bytes(key.len()))
        })?;
        // Rebound after the waiter: on unwind the sender drops first, so the
        // waiter's read ends and its join cannot hang.
        let keys = keys;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let wakes = Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);
        assert!(waiter.poll_read(&mut cx).is_pending());
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(200)).await });
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        // Measurement only: waiter wake to task readiness (the console wait
        // itself is the kernel's; this is the handoff this module adds).
        let mut samples = Vec::new();
        for _ in 0..200 {
            let start = Instant::now();
            keys.send(b"x".to_vec())
                .map_err(|_| io::ErrorKind::BrokenPipe)?;
            let count = runtime.block_on(std::future::poll_fn(|cx| {
                waiter.poll_read(cx).map_ok(|chunk| match chunk {
                    Chunk::Bytes(bytes) => bytes.len(),
                    Chunk::Resized => 0,
                })
            }))?;
            assert_eq!(count, 1);
            samples.push(start.elapsed());
        }
        samples.sort();
        eprintln!(
            "windows input handoff p50 {:?} p99 {:?} max {:?}; idle 200 ms: 0 wakeups",
            samples[100], samples[198], samples[199]
        );
        Ok(())
    }

    /// Runs the production pump on this thread against a stream polled on
    /// another. Returns the pump's allocations over all batches and the
    /// stream's over the batches after eight warm ones.
    fn measure_batches(recycle: bool) -> (u64, u64) {
        const WARM: u8 = 8;
        const MEASURED: u8 = 64;
        let handoff = Arc::new(Handoff::new(BATCH));
        let mut waiter = Waiter {
            handoff: Arc::clone(&handoff),
            current: Vec::new(),
            thread: None,
        };
        std::thread::scope(|scope| {
            let stream = scope.spawn(move || {
                let mut cx = Context::from_waker(Waker::noop());
                let mut next = |expected: u8| loop {
                    match waiter.poll_read(&mut cx) {
                        Poll::Ready(Ok(Chunk::Resized)) => panic!("unexpected resize"),
                        Poll::Ready(Ok(Chunk::Bytes(bytes))) => {
                            assert_eq!(bytes, [expected]);
                            if !recycle {
                                drop(std::mem::take(&mut waiter.current));
                            }
                            return;
                        }
                        Poll::Ready(Err(_)) => {}
                        Poll::Pending => std::thread::yield_now(),
                    }
                };
                (0..WARM).for_each(&mut next);
                acyclic_test_support::measure(|| (WARM..WARM + MEASURED).for_each(&mut next))
                    .count_total
            });
            let mut keys = 0..WARM + MEASURED;
            let mut read = |bytes: &mut [u8]| match keys.next() {
                Some(key) => {
                    bytes[0] = key;
                    Ok(ffi::ConsoleRead::Bytes(1))
                }
                None => Err(io::ErrorKind::Other.into()),
            };
            // The reader side over its whole life: storage is allocated once,
            // before the pump starts.
            let pumped = acyclic_test_support::measure(|| pump(&mut read, &handoff));
            let streamed = stream.join().unwrap_or(u64::MAX);
            (pumped.count_total, streamed)
        })
    }

    #[test]
    fn warm_batches_circulate_storage_without_allocating() {
        assert_eq!(measure_batches(true), (0, 0));
    }

    #[test]
    fn negative_control_detects_a_fresh_buffer_per_batch() {
        // Without recycling, every read after the circulating storage is
        // used up allocates: 72 batches plus the final failing read.
        let (pumped, _) = measure_batches(false);
        assert_eq!(pumped, (8 + 64 + 1 - BUFFERS) as u64);
    }
}

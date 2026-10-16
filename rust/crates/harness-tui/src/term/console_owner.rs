//! One console owner. No terminal-state lock is held during driver I/O.
use super::RestoreGuard;
use std::io::{self, Write};
use std::time::{Duration, Instant};

#[cfg(acyclic_harness_tui_loom)]
use loom::{
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::{self, JoinHandle},
};
#[cfg(not(acyclic_harness_tui_loom))]
use std::{
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::{self, JoinHandle},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Idle,
    Submitted,
    Completed,
    Cancelled,
}

struct State<W: Write> {
    guard: Option<RestoreGuard<W>>,
    phase: Phase,
    acknowledged: bool,
    closed: bool,
    restored: bool,
    worker: Option<Arc<JoinHandle<()>>>,
    generation: u64,
}

pub(crate) struct Owner<W: Write> {
    state: Mutex<State<W>>,
    wake: Condvar,
    cancel: fn(&JoinHandle<()>),
}

impl<W: Write + Send + 'static> Owner<W> {
    fn wait<'a>(
        &self,
        state: MutexGuard<'a, State<W>>,
        timeout: Duration,
    ) -> io::Result<MutexGuard<'a, State<W>>> {
        // Timer wakeups that leave the state unchanged are stutters. Model
        // acknowledgement notifications and explicit cancellation transitions;
        // real Windows tests exercise the bounded wall-clock waits.
        #[cfg(acyclic_harness_tui_loom)]
        {
            let _ = timeout;
            self.wake
                .wait(state)
                .map_err(|_| io::ErrorKind::Other.into())
        }
        #[cfg(not(acyclic_harness_tui_loom))]
        {
            self.wake
                .wait_timeout(state, timeout)
                .map(|value| value.0)
                .map_err(|_| io::ErrorKind::Other.into())
        }
    }
    pub(crate) fn new(guard: RestoreGuard<W>, cancel: fn(&JoinHandle<()>)) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                guard: Some(guard),
                phase: Phase::Completed,
                acknowledged: true,
                closed: false,
                restored: false,
                worker: None,
                generation: 0,
            }),
            wake: Condvar::new(),
            cancel,
        })
    }

    pub(crate) fn run<R: Send + 'static>(
        this: &Arc<Self>,
        deadline: Instant,
        action: impl FnOnce(&mut RestoreGuard<W>) -> io::Result<R> + Send + 'static,
    ) -> io::Result<R> {
        Self::execute(this, deadline, false, action)
    }

    fn execute<R: Send + 'static>(
        this: &Arc<Self>,
        deadline: Instant,
        restoring: bool,
        action: impl FnOnce(&mut RestoreGuard<W>) -> io::Result<R> + Send + 'static,
    ) -> io::Result<R> {
        // Lock order: owner -> result. Worker and waiter never acquire owner
        // while holding result. Driver calls and acknowledgement waits release
        // owner. Cancellation calls also run outside this lock and the caller.
        let mut state = this.state.lock().map_err(|_| io::ErrorKind::Other)?;
        while !state.acknowledged {
            if Instant::now() >= deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            let remaining = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(1));
            state = this.wait(state, remaining)?;
        }
        if !restoring && state.closed {
            return Err(io::ErrorKind::NotConnected.into());
        }
        if Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(io::ErrorKind::Other)?;
        state.phase = Phase::Idle;
        state.acknowledged = false;
        state.generation = generation;
        let result = Arc::new(Mutex::new(None));
        let output = Arc::clone(&result);
        let owner = Arc::clone(this);
        let worker = move || {
            let mut state = owner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.phase == Phase::Cancelled && !restoring {
                state.acknowledged = true;
                owner.wake.notify_all();
                return;
            }
            state.phase = Phase::Submitted;
            let Some(mut guard) = state.guard.take() else {
                *output.lock().unwrap_or_else(|error| error.into_inner()) =
                    Some(Err(io::Error::other("console owner lost writer")));
                state.phase = Phase::Cancelled;
                state.acknowledged = true;
                owner.wake.notify_all();
                return;
            };
            drop(state);
            // Deliberately outside the lock: cancellation may race this exact
            // submission. Restoration waits for the ack, not CancelIo's return.
            let value =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| action(&mut guard)))
                    .unwrap_or_else(|_| Err(io::Error::other("console worker panicked")));
            let mut state = owner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *output.lock().unwrap_or_else(|error| error.into_inner()) = Some(value);
            state.guard = Some(guard);
            if state.phase != Phase::Cancelled {
                state.phase = Phase::Completed;
            }
            state.acknowledged = true;
            owner.wake.notify_all();
        };
        #[cfg(acyclic_harness_tui_loom)]
        let spawned: io::Result<_> = Ok(thread::spawn(worker));
        #[cfg(not(acyclic_harness_tui_loom))]
        let spawned = thread::Builder::new()
            .name("terminal-console".into())
            .spawn(worker);
        match spawned {
            Ok(worker) => state.worker = Some(Arc::new(worker)),
            Err(error) => {
                state.phase = Phase::Cancelled;
                state.acknowledged = true;
                this.wake.notify_all();
                return Err(error);
            }
        }
        while !state.acknowledged {
            if Instant::now() >= deadline {
                state.phase = Phase::Cancelled;
                drop(state);
                Self::cancel_background(this, generation);
                return Err(io::ErrorKind::TimedOut.into());
            }
            state = this.wait(state, deadline.saturating_duration_since(Instant::now()))?;
        }
        let value = result.lock().map_err(|_| io::ErrorKind::Other)?.take();
        if state.closed && !restoring {
            return Err(io::ErrorKind::NotConnected.into());
        }
        if Instant::now() >= deadline || state.phase == Phase::Cancelled {
            return Err(io::ErrorKind::TimedOut.into());
        }
        value.ok_or_else(|| io::Error::from(io::ErrorKind::Interrupted))?
    }

    fn quiesce(&self, wait_for_ack: bool) -> io::Result<()> {
        self.quiesce_generation(wait_for_ack, None)
    }

    fn cancel_background(this: &Arc<Self>, generation: u64) {
        let owner = Arc::clone(this);
        #[cfg(acyclic_harness_tui_loom)]
        let _worker = thread::spawn(move || {
            let _ = owner.quiesce_generation(true, Some(generation));
        });
        #[cfg(not(acyclic_harness_tui_loom))]
        let _worker = thread::Builder::new()
            .name("terminal-cancel".into())
            .spawn(move || {
                let _ = owner.quiesce_generation(true, Some(generation));
            });
    }

    fn quiesce_generation(&self, wait_for_ack: bool, generation: Option<u64>) -> io::Result<()> {
        let mut state = self.state.lock().map_err(|_| io::ErrorKind::Other)?;
        if generation.is_some_and(|value| value != state.generation) {
            return Ok(());
        }
        state.phase = Phase::Cancelled;
        while wait_for_ack && !state.acknowledged {
            // Repeat cancellation across the pre-submission race. An idle
            // worker skips the action; a submitted worker must finish/abort.
            let worker = state.worker.clone();
            let current = state.generation;
            drop(state);
            if let Some(worker) = worker {
                (self.cancel)(&worker);
            }
            state = self.state.lock().map_err(|_| io::ErrorKind::Other)?;
            if current != state.generation {
                return Ok(());
            }
            if state.acknowledged {
                break;
            }
            state = self.wait(state, Duration::from_millis(1))?;
        }
        Ok(())
    }

    pub(crate) fn close(
        this: &Arc<Self>,
        os_restore: fn(&mut W) -> io::Result<()>,
    ) -> io::Result<()> {
        {
            let mut state = this.state.lock().map_err(|_| io::ErrorKind::Other)?;
            if state.closed {
                while !state.restored {
                    state = this.wake.wait(state).map_err(|_| io::ErrorKind::Other)?;
                }
                return Ok(());
            }
            state.closed = true;
        }
        this.quiesce(true)?;
        let result = Self::execute(
            this,
            Instant::now() + super::PROBE_TIMEOUT,
            true,
            move |guard| {
                // Cancellation can leave a partial query even with an empty undo
                // journal. Fence its worker first, then cancel the control string.
                let cancel = guard.writer().write_all(b"\x18");
                let dec = guard.restore().or_else(|_| guard.restore());
                let os = os_restore(guard.writer());
                cancel.and(dec).and(os)
            },
        );
        // Even if teardown output blocks, keep cancelling until it returns.
        // OS mode restoration runs after every attempted DEC undo, before ack.
        this.quiesce(true)?;
        let mut state = this.state.lock().map_err(|_| io::ErrorKind::Other)?;
        state.restored = true;
        this.wake.notify_all();
        result
    }
}

#[cfg(all(test, acyclic_harness_tui_loom))]
mod tests {
    use super::*;
    use crate::term::Capabilities;
    use loom::sync::atomic::{AtomicBool, Ordering};

    struct Console {
        restored: Arc<AtomicBool>,
        late: Arc<AtomicBool>,
        submitting: Option<loom::sync::mpsc::Sender<()>>,
    }
    impl Write for Console {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if let Some(submitting) = self.submitting.take() {
                submitting.send(()).unwrap_or_else(|error| panic!("{error}"));
            }
            thread::yield_now();
            if self.restored.load(Ordering::SeqCst) {
                self.late.store(true, Ordering::SeqCst);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn model(broken: bool) {
        loom::model(move || {
            let restored = Arc::new(AtomicBool::new(false));
            let late = Arc::new(AtomicBool::new(false));
            let (submitting, submitted) = loom::sync::mpsc::channel();
            let guard = RestoreGuard::new(
                Console {
                    restored: Arc::clone(&restored),
                    late: Arc::clone(&late),
                    submitting: Some(submitting),
                },
                &Capabilities::default(),
            );
            let owner = Owner::new(guard, |_| {});
            let writing = Arc::clone(&owner);
            let caller = thread::spawn(move || {
                let _ = Owner::run(
                    &writing,
                    Instant::now() + Duration::from_secs(10),
                    |guard| guard.writer().write_all(b"mode"),
                );
            });
            // Start at the reported race: the worker has passed its cancelled
            // check and may pause immediately before the native write. This
            // removes unrelated open/close-before-submit permutations.
            submitted.recv().unwrap_or_else(|error| panic!("{error}"));
            if broken {
                owner
                    .state
                    .lock()
                    .unwrap_or_else(|error| panic!("{error:?}"))
                    .closed = true;
                owner
                    .quiesce(false)
                    .unwrap_or_else(|error| panic!("{error}"));
                restored.store(true, Ordering::SeqCst);
            } else {
                Owner::close(&owner, |writer| {
                    writer.restored.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap_or_else(|error| panic!("{error}"));
            }
            caller
                .join()
                .unwrap_or_else(|error| std::panic::resume_unwind(error));
            assert!(
                !late.load(Ordering::SeqCst),
                "write after acknowledged restoration"
            );
        });
    }
    #[test]
    fn console_worker_acknowledges_before_restoration() {
        model(false);
    }
    #[test]
    fn negative_control_detects_unfenced_native_submission() {
        assert!(std::panic::catch_unwind(|| model(true)).is_err());
    }

    fn lock_model(broken: bool) {
        loom::model(move || {
            let owner = Owner::new(
                RestoreGuard::new(Vec::new(), &Capabilities::default()),
                |_| {},
            );
            let checking = Arc::clone(&owner);
            let free = Owner::run(
                &owner,
                Instant::now() + Duration::from_secs(10),
                move |_| {
                    if broken {
                        // Negative control: the old driver path retained owner
                        // state while waiting for its setup/restoration I/O.
                        let _held = checking
                            .state
                            .lock()
                            .unwrap_or_else(|error| panic!("{error:?}"));
                        Ok(checking.state.try_lock().is_ok())
                    } else {
                        Ok(checking.state.try_lock().is_ok())
                    }
                },
            );
            assert_eq!(free.ok(), Some(true));
        });
    }
    #[test]
    fn console_setup_and_restore_io_release_owner_state() {
        lock_model(false);
    }
    #[test]
    fn negative_control_detects_io_wait_under_owner_state() {
        assert!(std::panic::catch_unwind(|| lock_model(true)).is_err());
    }
}

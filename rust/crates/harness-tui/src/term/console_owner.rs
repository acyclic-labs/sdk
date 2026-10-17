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
    closing: bool,
    restored: bool,
    #[cfg(test)]
    fail_next_spawn: bool,
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
                closing: false,
                restored: false,
                #[cfg(test)]
                fail_next_spawn: false,
                worker: None,
                generation: 0,
            }),
            wake: Condvar::new(),
            cancel,
        })
    }

    /// Input may wait outside owner state, but must observe the same close fence.
    pub(crate) fn check_open(&self) -> io::Result<()> {
        if self.state.lock().map_err(|_| io::ErrorKind::Other)?.closed {
            Err(io::ErrorKind::NotConnected.into())
        } else {
            Ok(())
        }
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
        let result = Self::submit(this, &mut state, restoring, action)?;
        let generation = state.generation;
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

    fn submit<R: Send + 'static>(
        this: &Arc<Self>,
        state: &mut State<W>,
        restoring: bool,
        action: impl FnOnce(&mut RestoreGuard<W>) -> io::Result<R> + Send + 'static,
    ) -> io::Result<Arc<Mutex<Option<io::Result<R>>>>> {
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
            if restoring && value.is_ok() {
                state.restored = true;
            }
            *output.lock().unwrap_or_else(|error| error.into_inner()) = Some(value);
            state.guard = Some(guard);
            if state.phase != Phase::Cancelled {
                state.phase = Phase::Completed;
            }
            state.acknowledged = true;
            owner.wake.notify_all();
        };
        let spawn = || {
            #[cfg(acyclic_harness_tui_loom)]
            {
                Ok(thread::spawn(worker))
            }
            #[cfg(not(acyclic_harness_tui_loom))]
            {
                thread::Builder::new()
                    .name("terminal-console".into())
                    .spawn(worker)
            }
        };
        #[cfg(test)]
        let spawned = if std::mem::take(&mut state.fail_next_spawn) {
            Err(io::Error::other("injected console worker spawn failure"))
        } else {
            spawn()
        };
        #[cfg(not(test))]
        let spawned = spawn();
        match spawned {
            Ok(worker) => state.worker = Some(Arc::new(worker)),
            Err(error) => {
                state.phase = Phase::Cancelled;
                state.acknowledged = true;
                this.wake.notify_all();
                return Err(error);
            }
        }
        Ok(result)
    }

    fn quiesce(&self, wait_for_ack: bool) -> io::Result<()> {
        self.quiesce_generation::<true>(wait_for_ack, None)
    }

    fn cancel_background(this: &Arc<Self>, generation: u64) {
        let owner = Arc::clone(this);
        #[cfg(acyclic_harness_tui_loom)]
        let _worker = thread::spawn(move || {
            let _ = owner.quiesce_generation::<true>(true, Some(generation));
        });
        #[cfg(not(acyclic_harness_tui_loom))]
        let _worker = thread::Builder::new()
            .name("terminal-cancel".into())
            .spawn(move || {
                let _ = owner.quiesce_generation::<true>(true, Some(generation));
            });
    }

    fn quiesce_generation<const FENCE_WAKE: bool>(
        &self,
        wait_for_ack: bool,
        generation: Option<u64>,
    ) -> io::Result<()> {
        let mut state = self.state.lock().map_err(|_| io::ErrorKind::Other)?;
        if generation.is_some_and(|value| value != state.generation) {
            return Ok(());
        }
        state.phase = Phase::Cancelled;
        while wait_for_ack && !state.acknowledged {
            // A notified waiter may reacquire after replacement. Cancellation
            // still belongs to its original operation, never the new worker.
            if FENCE_WAKE && generation.is_some_and(|value| value != state.generation) {
                return Ok(());
            }
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
            while state.closing {
                state = this.wake.wait(state).map_err(|_| io::ErrorKind::Other)?;
            }
            if state.restored {
                return Ok(());
            }
            state.closed = true;
            state.closing = true;
        }
        let result = this.quiesce(true).and_then(|()| {
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
            this.quiesce(true).and(result)
        });
        let mut state = this.state.lock().map_err(|_| io::ErrorKind::Other)?;
        // Finishing an attempt does not imply restoration. Only the successful
        // restore action above may publish restored, after native completion.
        state.closing = false;
        this.wake.notify_all();
        result
    }
}

#[cfg(all(test, acyclic_harness_tui_loom))]
mod tests {
    use super::*;
    use crate::term::{Capabilities, ModeState};
    use loom::sync::atomic::{AtomicBool, Ordering};

    struct Console {
        restored: Arc<AtomicBool>,
        late: Arc<AtomicBool>,
        submitting: Option<loom::sync::mpsc::Sender<()>>,
    }
    impl Write for Console {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if let Some(submitting) = self.submitting.take() {
                submitting
                    .send(())
                    .unwrap_or_else(|error| panic!("{error}"));
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
            {
                let mut state = owner.state.lock().unwrap_or_else(|e| panic!("{e}"));
                Owner::submit(&owner, &mut state, false, |guard| {
                    guard.writer().write_all(b"mode")
                })
                .unwrap_or_else(|e| panic!("{e}"));
            }
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

    loom::lazy_static! {
        static ref CANCELLATIONS: Mutex<(Vec<thread::ThreadId>, Option<loom::sync::mpsc::Sender<()>>)> =
            Mutex::new((Vec::new(), None));
    }

    fn record_cancel(worker: &JoinHandle<()>) {
        let mut calls = CANCELLATIONS.lock().unwrap_or_else(|e| panic!("{e}"));
        calls.0.push(worker.thread().id());
        if let Some(started) = calls.1.take() {
            started.send(()).unwrap_or_else(|e| panic!("{e}"));
        }
    }

    fn handoff_model<const FENCE_WAKE: bool>() {
        loom::model(|| {
            let (started, cancelling) = loom::sync::mpsc::channel();
            CANCELLATIONS.lock().unwrap_or_else(|e| panic!("{e}")).1 = Some(started);
            let owner = Owner::new(
                RestoreGuard::new(Vec::new(), &Capabilities::default()),
                record_cancel,
            );
            // Cut at an in-flight native write: its guard is outside owner
            // state. The full worker/ack model above covers reaching this cut.
            let original = Arc::new(thread::spawn(|| {}));
            let original_id = original.thread().id();
            let guard = {
                let mut state = owner.state.lock().unwrap_or_else(|e| panic!("{e}"));
                state.generation = 1;
                state.phase = Phase::Submitted;
                state.acknowledged = false;
                state.worker = Some(original);
                state.guard.take()
            };
            let cancelling_owner = Arc::clone(&owner);
            let canceller = thread::spawn(move || {
                cancelling_owner
                    .quiesce_generation::<FENCE_WAKE>(true, Some(1))
                    .unwrap_or_else(|e| panic!("{e}"));
            });
            cancelling.recv().unwrap_or_else(|e| panic!("{e}"));
            // The old native write acknowledges. Publish a new operation before
            // the cancelled generation's waiter necessarily reacquires state.
            {
                let mut state = owner.state.lock().unwrap_or_else(|e| panic!("{e}"));
                state.guard = guard;
                state.acknowledged = true;
                owner.wake.notify_all();
            }
            {
                let mut state = owner.state.lock().unwrap_or_else(|e| panic!("{e}"));
                while !state.acknowledged {
                    state = owner.wake.wait(state).unwrap_or_else(|e| panic!("{e}"));
                }
                Owner::submit(&owner, &mut state, false, |_| Ok(()))
                    .unwrap_or_else(|e| panic!("{e}"));
            }
            canceller
                .join()
                .unwrap_or_else(|e| std::panic::resume_unwind(e));
            let mut state = owner.state.lock().unwrap_or_else(|e| panic!("{e}"));
            while !state.acknowledged {
                state = owner.wake.wait(state).unwrap_or_else(|e| panic!("{e}"));
            }
            drop(state);
            assert!(
                CANCELLATIONS
                    .lock()
                    .unwrap_or_else(|e| panic!("{e}"))
                    .0
                    .iter()
                    .all(|worker| *worker == original_id),
                "cancel reached replacement worker"
            );
        });
    }

    #[test]
    fn cancellation_wakeup_cannot_target_replacement_generation() {
        handoff_model::<true>();
    }

    #[test]
    fn negative_control_detects_cancellation_after_generation_handoff() {
        assert!(std::panic::catch_unwind(handoff_model::<false>).is_err());
    }

    fn retry_owner() -> Arc<Owner<Vec<u8>>> {
        let mut caps = Capabilities::default();
        caps.modes.insert(1049, ModeState::Reset);
        let mut guard = RestoreGuard::new(Vec::new(), &caps);
        guard.enable_mode(1049).unwrap_or_else(|e| panic!("{e}"));
        let owner = Owner::new(guard, |_| {});
        owner
            .state
            .lock()
            .unwrap_or_else(|e| panic!("{e}"))
            .fail_next_spawn = true;
        owner
    }

    fn restore_os(bytes: &mut Vec<u8>) -> io::Result<()> {
        bytes.extend_from_slice(b"OS restored");
        Ok(())
    }

    fn assert_restored(owner: &Owner<Vec<u8>>) {
        let mut state = owner.state.lock().unwrap_or_else(|e| panic!("{e}"));
        assert!(state.restored);
        assert!(!state.closing);
        let bytes = state.guard.as_mut().expect("writer returned").writer();
        assert!(bytes.windows(8).any(|b| b == b"\x1b[?1049l"));
        assert!(bytes.ends_with(b"OS restored"));
    }

    fn retry_model(broken: bool) {
        loom::model(move || {
            let owner = retry_owner();
            assert!(Owner::close(&owner, restore_os).is_err());
            {
                let mut state = owner.state.lock().unwrap_or_else(|e| panic!("{e}"));
                assert!(!state.restored);
                assert!(!state.closing);
                assert_eq!(
                    state.guard.as_mut().expect("writer retained").writer(),
                    b"\x1b[?1049h"
                );
                if broken {
                    // Negative control: the old close marked a failed spawn as
                    // restored even though its action never executed.
                    state.restored = true;
                }
            }
            // Tty::drop calls this same close even after explicit restore failed.
            Owner::close(&owner, restore_os).unwrap_or_else(|e| panic!("{e}"));
            assert_restored(&owner);
        });
    }

    #[test]
    fn failed_teardown_spawn_can_be_retried_by_close_or_drop() {
        retry_model(false);
    }

    #[test]
    fn negative_control_detects_restore_completion_without_action() {
        assert!(std::panic::catch_unwind(|| retry_model(true)).is_err());
    }

    #[test]
    fn failed_close_attempt_wakes_concurrent_retry() {
        loom::model(|| {
            let owner = retry_owner();
            let retrying = Arc::clone(&owner);
            let other = thread::spawn(move || Owner::close(&retrying, restore_os));
            let first = Owner::close(&owner, restore_os);
            let second = other
                .join()
                .unwrap_or_else(|e| std::panic::resume_unwind(e));
            assert_eq!(
                usize::from(first.is_err()) + usize::from(second.is_err()),
                1
            );
            assert_restored(&owner);
        });
    }
    fn input_close_model(broken: bool) {
        loom::model(move || {
            let owner = Owner::new(
                RestoreGuard::new(Vec::<u8>::new(), &Capabilities::default()),
                |_| {},
            );
            assert!(owner.check_open().is_ok());
            let closing = Arc::clone(&owner);
            let worker = thread::spawn(move || Owner::close(&closing, |_| Ok(())));
            let _ = owner.check_open(); // Either side of the concurrent close is valid.
            worker.join().unwrap().unwrap();
            let next_read = if broken { Ok(()) } else { owner.check_open() };
            assert!(next_read.is_err(), "input ignored the terminal close fence");
        });
    }
    #[test]
    fn input_observes_output_close_before_its_next_bounded_read() {
        input_close_model(false);
    }
    #[test]
    fn negative_control_detects_input_without_close_fence() {
        assert!(std::panic::catch_unwind(|| input_close_model(true)).is_err());
    }
}

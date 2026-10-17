use std::io::{self, Write};
use std::task::{Poll, Waker};

#[cfg(acyclic_harness_tui_loom)]
pub(crate) use loom::sync::{Arc, Mutex};
#[cfg(not(acyclic_harness_tui_loom))]
pub(crate) use std::sync::{Arc, Mutex};

use super::{Capabilities, RestoreGuard};

pub(crate) struct State<W: Write> {
    pub(crate) guard: RestoreGuard<W>,
    active: bool,
    closed: bool,
    restored: bool,
    /// The input stream parked on suspension or awaiting fd readiness. Close
    /// and resume take it under this lock, so neither transition is missed.
    input: Option<Waker>,
}

#[cfg(any(unix, acyclic_harness_tui_loom))]
pub(crate) type Owner<W> = Mutex<Option<Arc<Mutex<State<W>>>>>;

// Lock order: owner slot -> terminal state. Ordinary I/O only takes state;
// it never takes the owner slot. The dispatcher remains installed for life.
#[cfg(any(unix, acyclic_harness_tui_loom))]
pub(crate) fn attach<W: Write>(
    owner: &Owner<W>,
    create: impl FnOnce() -> io::Result<W>,
) -> io::Result<Arc<Mutex<State<W>>>> {
    let mut owner = owner.lock().map_err(|_| io::ErrorKind::Other)?;
    if owner.is_some() {
        return Err(io::ErrorKind::AlreadyExists.into());
    }
    let state = Arc::new(Mutex::new(State::new(create()?)));
    *owner = Some(Arc::clone(&state));
    Ok(state)
}

#[cfg(any(unix, acyclic_harness_tui_loom))]
pub(crate) fn detach<W: Write>(
    owner: &Owner<W>,
    state: &Arc<Mutex<State<W>>>,
    os_restore: impl FnOnce(&mut W) -> io::Result<()>,
) -> io::Result<()> {
    let mut owner = owner.lock().map_err(|_| io::ErrorKind::Other)?;
    if owner
        .as_ref()
        .is_none_or(|current| !Arc::ptr_eq(current, state))
    {
        return Ok(());
    }
    let result = restore(state, true, os_restore);
    // Failed restoration retains ownership; a new session must never inherit
    // an unfinished journal or race a later restoration attempt.
    if result.is_ok()
        && owner
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, state))
    {
        *owner = None;
    }
    result
}

#[cfg(any(unix, acyclic_harness_tui_loom))]
pub(crate) fn with_owner<W: Write, T>(
    owner: &Owner<W>,
    action: impl FnOnce(Option<&Arc<Mutex<State<W>>>>) -> T,
) -> io::Result<T> {
    let owner = owner.lock().map_err(|_| io::ErrorKind::Other)?;
    Ok(action(owner.as_ref()))
}

impl<W: Write> State<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            guard: RestoreGuard::new(writer, &Capabilities::default()),
            active: true,
            closed: false,
            restored: false,
            input: None,
        }
    }
}

// Lock order: owner slot, then this terminal-state lock. Never acquire the
// owner slot or a signal-iterator lock, or join the worker while holding state.
// Output, mode mutations and lifecycle callbacks ALL enter through these paths.
pub(crate) fn active<W: Write, T>(
    state: &Mutex<State<W>>,
    action: impl FnOnce(&mut RestoreGuard<W>) -> io::Result<T>,
) -> io::Result<T> {
    let mut state = state.lock().map_err(|_| io::ErrorKind::Other)?;
    if state.closed {
        return Err(io::ErrorKind::NotConnected.into());
    }
    if !state.active {
        return Err(io::ErrorKind::WouldBlock.into());
    }
    action(&mut state.guard)
}

/// Input entry: the close fence, the suspension check and waker registration
/// share one critical section with `restore(terminate)` and `resume`, so a
/// parked or readiness-waiting reader observes either transition.
#[cfg(any(unix, acyclic_harness_tui_loom))]
pub(crate) fn poll_input<W: Write, T>(
    state: &Mutex<State<W>>,
    waker: &Waker,
    action: impl FnOnce(&mut RestoreGuard<W>) -> io::Result<T>,
) -> Poll<io::Result<T>> {
    let mut state = match state.lock() {
        Ok(state) => state,
        Err(_) => return Poll::Ready(Err(io::ErrorKind::Other.into())),
    };
    if state.closed {
        return Poll::Ready(Err(io::ErrorKind::NotConnected.into()));
    }
    match &mut state.input {
        Some(current) => current.clone_from(waker),
        input => *input = Some(waker.clone()),
    }
    if !state.active {
        return Poll::Pending;
    }
    Poll::Ready(action(&mut state.guard))
}

pub(crate) fn restore<W: Write>(
    state: &Mutex<State<W>>,
    terminate: bool,
    os_restore: impl FnOnce(&mut W) -> io::Result<()>,
) -> io::Result<()> {
    // A panic while writing poisons the lock, but the journal is written
    // before every terminal byte, so it is exact at any unwinding point.
    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state.closed |= terminate;
    state.active = false;
    // Wake after unlocking: the reader re-enters through poll_input.
    let input = if terminate { state.input.take() } else { None };
    let result = if state.restored {
        Ok(())
    } else {
        let dec = state.guard.restore();
        let os = os_restore(state.guard.writer());
        let result = dec.and(os);
        state.restored = result.is_ok();
        result
    };
    drop(state);
    if let Some(input) = input {
        input.wake();
    }
    result
}

#[cfg(any(unix, acyclic_harness_tui_loom))]
pub(crate) fn resume<W: Write>(
    state: &Mutex<State<W>>,
    os_reapply: impl FnOnce(&mut W) -> io::Result<()>,
) -> io::Result<()> {
    let mut state = state.lock().map_err(|_| io::ErrorKind::Other)?;
    if !state.closed && !state.active {
        os_reapply(state.guard.writer())?;
        state.guard.reapply()?;
        state.active = true;
        state.restored = false;
        let input = state.input.take();
        drop(state);
        if let Some(input) = input {
            input.wake();
        }
    }
    Ok(())
}

#[cfg(all(test, not(acyclic_harness_tui_loom)))]
mod poison_tests {
    use super::*;

    #[test]
    fn restoration_survives_a_panic_while_writing() {
        let state = Mutex::new(State::new(Vec::<u8>::new()));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            active(&state, |guard| -> io::Result<()> {
                // Journaled before the first byte, as every frame is.
                guard.begin_frame(true);
                panic!("injected writer panic");
            })
        }));
        assert!(panicked.is_err());
        // Negative control: the previous `lock().map_err(..)?` refused this state.
        assert!(state.lock().is_err(), "the state lock is poisoned");
        assert!(restore(&state, true, |_| Ok(())).is_ok());
        let mut state = state
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(state.guard.writer().as_slice(), b"[?25h");
    }
}

#[cfg(all(test, acyclic_harness_tui_loom))]
mod terminal_lifecycle_model {
    use super::*;
    use crate::term::ModeState;

    // Fake OS state is acquired last, after owner -> state; never in reverse.
    struct Terminal {
        raw: Arc<Mutex<bool>>,
        broken_drop: bool,
    }

    impl Write for Terminal {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Drop for Terminal {
        fn drop(&mut self) {
            // Negative control reproduces the removed RawTty destructor.
            if self.broken_drop {
                *self.raw.lock().unwrap_or_else(|error| panic!("{error:?}")) = false;
            }
        }
    }

    fn reopen_model(broken_drop: bool) {
        loom::model(move || {
            let raw = Arc::new(Mutex::new(false));
            let owner = Arc::new(Mutex::new(None));
            let old = attach(&owner, || {
                *raw.lock().unwrap_or_else(|error| panic!("{error:?}")) = true;
                Ok(Terminal {
                    raw: Arc::clone(&raw),
                    broken_drop,
                })
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
            let reopening = Arc::clone(&owner);
            let new_raw = Arc::clone(&raw);
            let worker = loom::thread::spawn(move || {
                attach(&reopening, || {
                    *new_raw.lock().unwrap_or_else(|error| panic!("{error:?}")) = true;
                    Ok(Terminal {
                        raw: new_raw,
                        broken_drop: false,
                    })
                })
                .ok()
            });
            detach(&owner, &old, |tty| {
                *tty.raw.lock().unwrap_or_else(|error| panic!("{error:?}")) = false;
                Ok(())
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
            loom::thread::yield_now();
            drop(old);
            if let Some(new) = worker.join().unwrap_or_else(|error| panic!("{error:?}")) {
                active(&new, |_| {
                    assert!(
                        *raw.lock().unwrap_or_else(|error| panic!("{error:?}")),
                        "old destructor recooked new session"
                    );
                    Ok(())
                })
                .unwrap_or_else(|error| panic!("{error:?}"));
            }
        });
    }

    #[test]
    fn delayed_destructor_cannot_restore_over_reopened_terminal() {
        reopen_model(false);
    }

    #[test]
    fn negative_control_detects_restoration_after_owner_release() {
        assert!(std::panic::catch_unwind(|| reopen_model(true)).is_err());
    }

    #[test]
    fn failed_restoration_keeps_owner_until_successful_retry() {
        loom::model(|| {
            let owner = Mutex::new(None);
            let state =
                attach(&owner, || Ok(Vec::new())).unwrap_or_else(|error| panic!("{error:?}"));
            assert!(detach(&owner, &state, |_| Err(io::ErrorKind::Other.into())).is_err());
            assert!(attach(&owner, || Ok(Vec::new())).is_err());
            detach(&owner, &state, |_| Ok(())).unwrap_or_else(|error| panic!("{error:?}"));
            assert!(attach(&owner, || Ok(Vec::new())).is_ok());
        });
    }

    fn owner_model(broken: bool) {
        loom::model(move || {
            let owner = Arc::new(Mutex::new(None));
            let state = match attach(&owner, || Ok(Vec::new())) {
                Ok(state) => state,
                Err(_) => unreachable!(),
            };
            let signal_owner = Arc::clone(&owner);
            let signal_state = Arc::clone(&state);
            let signal = loom::thread::spawn(move || {
                assert!(
                    with_owner(&signal_owner, |owner| {
                        if owner.is_none() {
                            let state = match signal_state.lock() {
                                Ok(state) => state,
                                Err(_) => unreachable!(),
                            };
                            assert!(!state.active, "default signal action before restoration");
                        }
                    })
                    .is_ok()
                );
            });
            if broken {
                // Negative control: detach before restore permits default
                // termination while the terminal still belongs to this owner.
                if let Ok(mut slot) = owner.lock() {
                    *slot = None;
                }
                loom::thread::yield_now();
                assert!(restore(&state, true, |_| Ok(())).is_ok());
            } else {
                assert!(detach(&owner, &state, |_| Ok(())).is_ok());
            }
            assert!(signal.join().is_ok());
            assert!(with_owner(&owner, |owner| assert!(owner.is_none())).is_ok());
            assert!(attach(&owner, || Ok(Vec::new())).is_ok());
            assert!(attach(&owner, || Ok(Vec::new())).is_err());
        });
    }

    #[test]
    fn detach_restores_before_default_signal_and_allows_reopen() {
        owner_model(false);
    }

    #[test]
    fn negative_control_detects_early_owner_detach() {
        assert!(std::panic::catch_unwind(|| owner_model(true)).is_err());
    }

    fn model(broken: bool) {
        loom::model(move || {
            let mut state = State::new(Vec::new());
            let caps = Capabilities {
                modes: [(2004, ModeState::Reset)].into(),
                ..Capabilities::default()
            };
            assert!(state.guard.detected(&caps).is_ok());
            let shared = Arc::new(Mutex::new(state));
            let input = Arc::clone(&shared);
            let worker = loom::thread::spawn(move || {
                if broken {
                    // Negative control: checking the close fence outside the
                    // mutation's critical section permits a post-close enable.
                    let permitted = input.lock().is_ok_and(|state| !state.closed);
                    loom::thread::yield_now();
                    if permitted && let Ok(mut state) = input.lock() {
                        assert!(state.guard.enable_mode(2004).is_ok());
                    }
                } else {
                    let _ = active(&input, |guard| guard.enable_mode(2004));
                }
            });
            assert!(restore(&shared, true, |_| Ok(())).is_ok());
            assert!(worker.join().is_ok());
            let mut state = match shared.lock() {
                Ok(state) => state,
                Err(_) => unreachable!(),
            };
            let bytes = state.guard.writer();
            let last_set = bytes.windows(8).rposition(|b| b == b"\x1b[?2004h");
            let last_reset = bytes.windows(8).rposition(|b| b == b"\x1b[?2004l");
            assert!(
                last_set.is_none() || last_reset > last_set,
                "mode enabled after terminal close"
            );
            assert!(state.closed);
        });
    }

    #[test]
    fn close_fences_every_mode_write() {
        model(false);
    }

    #[test]
    fn negative_control_detects_split_close_fence() {
        assert!(std::panic::catch_unwind(|| model(true)).is_err());
    }

    #[test]
    fn suspend_resume_and_close_cannot_reenable_closed_terminal() {
        loom::model(|| {
            let state = Arc::new(Mutex::new(State::new(Vec::new())));
            let resumed = Arc::clone(&state);
            let worker = loom::thread::spawn(move || {
                assert!(restore(&resumed, false, |_| Ok(())).is_ok());
                assert!(resume(&resumed, |_| Ok(())).is_ok());
            });
            assert!(restore(&state, true, |_| Ok(())).is_ok());
            assert!(worker.join().is_ok());
            assert!(active(&state, |_| Ok(())).is_err());
        });
    }
    fn split_drop_model(broken: bool) {
        loom::model(move || {
            let owner = Arc::new(Mutex::new(None));
            let old = attach(&owner, || Ok(Vec::<u8>::new())).unwrap();
            detach(&owner, &old, |_| Ok(())).unwrap();
            let closing_owner = Arc::clone(&owner);
            let delayed = loom::thread::spawn(move || {
                detach(&closing_owner, &old, |_| Ok(())).unwrap();
                if broken {
                    // Negative control removes the identity fence on delayed close.
                    *closing_owner.lock().unwrap() = None;
                }
            });
            let fresh = attach(&owner, || Ok(Vec::<u8>::new())).unwrap();
            delayed.join().unwrap();
            assert!(
                owner
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &fresh)),
                "delayed split-half close detached the new owner"
            );
        });
    }

    #[test]
    fn delayed_split_half_cannot_detach_a_new_terminal() {
        split_drop_model(false);
    }
    #[test]
    fn negative_control_detects_unfenced_split_half_close() {
        assert!(std::panic::catch_unwind(|| split_drop_model(true)).is_err());
    }
    fn stale_restore_model(broken: bool) {
        loom::model(move || {
            let raw = Arc::new(Mutex::new(true));
            let old = Mutex::new(State::new(Vec::<u8>::new()));
            restore(&old, true, |_| {
                *raw.lock().unwrap() = false;
                Ok(())
            })
            .unwrap();
            *raw.lock().unwrap() = true; // The next owner has entered raw mode.
            restore(&old, false, |_| {
                *raw.lock().unwrap() = false;
                Ok(())
            })
            .unwrap();
            if broken {
                *raw.lock().unwrap() = false;
            } // Removed successful-restore fence.
            assert!(
                *raw.lock().unwrap(),
                "old terminal repeated OS restoration after handoff"
            );
        });
    }
    #[test]
    fn completed_restore_cannot_mutate_a_reopened_terminal() {
        stale_restore_model(false);
    }
    #[test]
    fn negative_control_detects_repeated_os_restoration() {
        assert!(std::panic::catch_unwind(|| stale_restore_model(true)).is_err());
    }

    struct Flag(loom::sync::atomic::AtomicBool);
    impl std::task::Wake for Flag {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.store(true, loom::sync::atomic::Ordering::SeqCst);
        }
    }

    // The reader polls once against a descriptor that is not ready while the
    // other thread closes the terminal or resumes it from suspension.
    fn input_wake_model(close: bool, broken: bool) {
        loom::model(move || {
            let state = Arc::new(Mutex::new(State::new(Vec::<u8>::new())));
            if !close {
                restore(&state, false, |_| Ok(())).unwrap();
            }
            let flag = std::sync::Arc::new(Flag(loom::sync::atomic::AtomicBool::new(false)));
            let waker = Waker::from(std::sync::Arc::clone(&flag));
            let other = Arc::clone(&state);
            let transition = loom::thread::spawn(move || {
                if close {
                    restore(&other, true, |_| Ok(())).unwrap();
                } else {
                    resume(&other, |_| Ok(())).unwrap();
                }
            });
            let parked = if broken {
                // Negative control: the fence check and the waker registration
                // in separate critical sections lose a transition between them.
                let waiting = {
                    let state = state.lock().unwrap();
                    !state.closed && (close || !state.active)
                };
                loom::thread::yield_now();
                if waiting {
                    state.lock().unwrap().input = Some(waker.clone());
                }
                waiting
            } else {
                match poll_input(&state, &waker, |_| {
                    Err::<(), _>(io::ErrorKind::WouldBlock.into())
                }) {
                    Poll::Pending => true,
                    // Waiting on readiness: only close must wake it.
                    Poll::Ready(Err(error)) => close && error.kind() == io::ErrorKind::WouldBlock,
                    Poll::Ready(Ok(())) => false,
                }
            };
            transition.join().unwrap();
            assert!(
                !parked || flag.0.load(loom::sync::atomic::Ordering::SeqCst),
                "terminal transition lost the parked reader's wakeup"
            );
        });
    }

    #[test]
    fn close_wakes_a_reader_waiting_for_readiness() {
        input_wake_model(true, false);
    }
    #[test]
    fn negative_control_detects_close_lost_between_check_and_registration() {
        assert!(std::panic::catch_unwind(|| input_wake_model(true, true)).is_err());
    }
    #[test]
    fn resume_wakes_a_reader_parked_on_suspension() {
        input_wake_model(false, false);
    }
    #[test]
    fn negative_control_detects_resume_lost_between_check_and_registration() {
        assert!(std::panic::catch_unwind(|| input_wake_model(false, true)).is_err());
    }
}

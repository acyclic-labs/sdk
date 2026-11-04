use std::io::{self, Write};

#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Mutex};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Mutex};

use super::{Capabilities, RestoreGuard};

pub(crate) struct State<W: Write> {
    pub(crate) guard: RestoreGuard<W>,
    active: bool,
    closed: bool,
    restored: bool,
    changed: Option<fn(&mut W) -> io::Result<()>>,
}

#[cfg(any(unix, loom))]
pub(crate) type Owner<W> = Mutex<Option<Arc<Mutex<State<W>>>>>;

// Lock order: owner slot -> terminal state. Ordinary I/O only takes state;
// it never takes the owner slot. The dispatcher remains installed for life.
#[cfg(any(unix, loom))]
pub(crate) fn attach<W: Write>(
    owner: &Owner<W>,
    create: impl FnOnce() -> io::Result<State<W>>,
) -> io::Result<Arc<Mutex<State<W>>>> {
    let mut owner = owner.lock().map_err(|_| io::ErrorKind::Other)?;
    if owner.is_some() {
        return Err(io::ErrorKind::AlreadyExists.into());
    }
    let state = Arc::new(Mutex::new(create()?));
    *owner = Some(Arc::clone(&state));
    Ok(state)
}

#[cfg(any(unix, loom))]
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

#[cfg(any(unix, loom))]
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
            changed: None,
        }
    }

    pub(crate) fn notifying(writer: W, changed: fn(&mut W) -> io::Result<()>) -> Self {
        Self {
            changed: Some(changed),
            ..Self::new(writer)
        }
    }

    pub(crate) fn is_active(&self) -> io::Result<bool> {
        if self.closed {
            Err(io::ErrorKind::NotConnected.into())
        } else {
            Ok(self.active)
        }
    }

    fn notify(&mut self) -> io::Result<()> {
        if let Some(changed) = self.changed {
            changed(self.guard.writer())?;
        }
        Ok(())
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
    if !state.is_active()? {
        return Err(io::ErrorKind::WouldBlock.into());
    }
    action(&mut state.guard)
}

// Lock order is the ordinary active path: terminal state only. Observation,
// empty-frame acknowledgement and device presentation share this lock, so no
// signal callback can intervene after the comparison and before the write.
pub(crate) fn present_if_current<W: Write>(
    state: &Mutex<State<W>>,
    frame: &mut Vec<u8>,
    effects: super::FrameEffects,
    expected: Option<u64>,
) -> io::Result<Option<u64>> {
    active(state, |guard| {
        super::Present::present_if_current(guard, frame, effects, expected)
    })
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
    // Publish under the same lock as the state change, including a close
    // after an earlier successful suspension restoration.
    let changed = state.notify();
    if state.restored {
        return changed;
    }
    let dec = state.guard.restore();
    let os = os_restore(state.guard.writer());
    let result = dec.and(os);
    state.restored = result.is_ok();
    result.and(changed)
}

#[cfg(any(unix, loom))]
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
        state.notify()?;
    }
    Ok(())
}

#[cfg(test)]
#[cfg(not(loom))]
mod poison_tests {
    use super::*;

    #[test]
    fn restoration_survives_a_panic_while_writing() {
        let state = Mutex::new(State::new(Vec::<u8>::new()));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            active(&state, |guard| -> io::Result<()> {
                // Journaled before the first byte, as every frame is.
                guard.begin_frame(crate::term::FrameEffects {
                    cursor_hidden: true,
                    margins: false,
                });
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

#[cfg(test)]
#[cfg(loom)]
mod terminal_lifecycle_model {
    use super::*;
    use crate::term::ModeState;

    struct InputWake {
        pending: Mutex<bool>,
        changed: loom::sync::Condvar,
    }

    struct Notifier(Arc<InputWake>);
    impl Write for Notifier {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Notifier {
        fn changed(&mut self) -> io::Result<()> {
            *self.0.pending.lock().map_err(|_| io::ErrorKind::Other)? = true;
            self.0.changed.notify_one();
            Ok(())
        }
    }

    /// The native control removes the sole notification as its negative.
    /// This model explores acknowledgement, suspension and final close on
    /// the same production state mutex and transition callbacks.
    #[test]
    fn lifecycle_notification_survives_acknowledgement_before_close() {
        loom::model(|| {
            assert!(notification_model().is_ok());
        });
    }

    fn notification_model() -> io::Result<()> {
        let wake = Arc::new(InputWake {
            pending: Mutex::new(false),
            changed: loom::sync::Condvar::new(),
        });
        let state = Arc::new(Mutex::new(State::notifying(
            Notifier(Arc::clone(&wake)),
            Notifier::changed,
        )));
        let reading = Arc::clone(&state);
        let reader = loom::thread::spawn(move || -> io::Result<()> {
            loop {
                {
                    let state = reading.lock().map_err(|_| io::ErrorKind::Other)?;
                    *wake.pending.lock().map_err(|_| io::ErrorKind::Other)? = false;
                    if state.is_active().is_err() {
                        return Ok(());
                    }
                }
                let mut pending = wake.pending.lock().map_err(|_| io::ErrorKind::Other)?;
                while !*pending {
                    pending = wake
                        .changed
                        .wait(pending)
                        .map_err(|_| io::ErrorKind::Other)?;
                }
            }
        });
        restore(&state, false, |_| Ok(()))?;
        restore(&state, true, |_| Ok(()))?;
        reader.join().map_err(|_| io::ErrorKind::Other)?
    }

    // Fake OS state is acquired last, after owner -> state; never in reverse.
    struct Terminal {
        raw: Arc<Mutex<bool>>,
    }

    impl Write for Terminal {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn reopen_model() {
        loom::model(|| {
            let raw = Arc::new(Mutex::new(false));
            let owner = Arc::new(Mutex::new(None));
            let old = attach(&owner, || {
                *raw.lock().unwrap_or_else(|error| panic!("{error:?}")) = true;
                Ok(State::new(Terminal {
                    raw: Arc::clone(&raw),
                }))
            })
            .unwrap_or_else(|error| panic!("{error:?}"));
            let reopening = Arc::clone(&owner);
            let new_raw = Arc::clone(&raw);
            let worker = loom::thread::spawn(move || {
                attach(&reopening, || {
                    *new_raw.lock().unwrap_or_else(|error| panic!("{error:?}")) = true;
                    Ok(State::new(Terminal { raw: new_raw }))
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
        reopen_model();
    }

    #[test]
    fn failed_restoration_keeps_owner_until_successful_retry() {
        loom::model(|| {
            let owner = Mutex::new(None);
            let state = attach(&owner, || Ok(State::new(Vec::new())))
                .unwrap_or_else(|error| panic!("{error:?}"));
            assert!(detach(&owner, &state, |_| Err(io::ErrorKind::Other.into())).is_err());
            assert!(
                attach(&owner, || Ok(State::new(Vec::new()))).is_err(),
                "failed restoration released the owner"
            );
            detach(&owner, &state, |_| Ok(())).unwrap_or_else(|error| panic!("{error:?}"));
            assert!(attach(&owner, || Ok(State::new(Vec::new()))).is_ok());
        });
    }

    fn owner_model() {
        loom::model(|| {
            let owner = Arc::new(Mutex::new(None));
            let state = match attach(&owner, || Ok(State::new(Vec::new()))) {
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
            assert!(detach(&owner, &state, |_| Ok(())).is_ok());
            assert!(signal.join().is_ok());
            assert!(with_owner(&owner, |owner| assert!(owner.is_none())).is_ok());
            assert!(attach(&owner, || Ok(State::new(Vec::new()))).is_ok());
            assert!(attach(&owner, || Ok(State::new(Vec::new()))).is_err());
        });
    }

    #[test]
    fn detach_restores_before_default_signal_and_allows_reopen() {
        owner_model();
    }

    fn model() {
        loom::model(|| {
            let mut state = State::new(Vec::new());
            let caps = Capabilities {
                modes: [(2004, ModeState::Reset)].into(),
                ..Capabilities::default()
            };
            assert!(state.guard.detected(&caps).is_ok());
            let shared = Arc::new(Mutex::new(state));
            let input = Arc::clone(&shared);
            let worker = loom::thread::spawn(move || {
                let _ = active(&input, |guard| guard.enable_mode(2004));
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
        model();
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
    fn split_drop_model() {
        loom::model(|| {
            let owner = Arc::new(Mutex::new(None));
            let old = attach(&owner, || Ok(State::new(Vec::<u8>::new()))).unwrap();
            detach(&owner, &old, |_| Ok(())).unwrap();
            let closing_owner = Arc::clone(&owner);
            let delayed = loom::thread::spawn(move || {
                detach(&closing_owner, &old, |_| Ok(())).unwrap();
            });
            let fresh = attach(&owner, || Ok(State::new(Vec::<u8>::new()))).unwrap();
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
        split_drop_model();
    }
    fn stale_restore_model() {
        loom::model(|| {
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
            assert!(
                *raw.lock().unwrap(),
                "old terminal repeated OS restoration after handoff"
            );
        });
    }
    #[test]
    fn completed_restore_cannot_mutate_a_reopened_terminal() {
        stale_restore_model();
    }
}

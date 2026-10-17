//! Two first callers use the production cache and instrumented owned mutex.
#[cfg(test)]
use super::*;
use std::cell::Cell;

loom::thread_local! { static UNLOCKED: Cell<bool> = Cell::new(false); }
loom::thread_local! { static EARLY_CLOSE: Cell<bool> = Cell::new(false); }
pub(super) fn unlocked() -> bool {
    UNLOCKED.with(Cell::get)
}

pub(super) fn early_close() -> bool {
    EARLY_CLOSE.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom::{
        sync::atomic::{AtomicUsize, Ordering},
        thread,
    };
    use std::task::{Context, Poll, Waker};

    fn complete<F: Future>(future: F) -> F::Output {
        match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("workspace opening did not finish"),
        }
    }

    fn race(broken: bool) {
        let state = StateArc::new(Mutex::new(Opened::<usize, ()>::default()));
        let opened = StateArc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let state = StateArc::clone(&state);
                let opened = StateArc::clone(&opened);
                thread::spawn(move || {
                    UNLOCKED.with(|value| value.set(broken));
                    let handle = complete(open_once(&state, "same-workspace".into(), async || {
                        thread::yield_now();
                        opened.fetch_add(1, Ordering::SeqCst);
                        Ok((StateArc::new(7), ()))
                    }))
                    .unwrap();
                    assert_eq!(*handle, 7);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(opened.load(Ordering::SeqCst), 1, "workspace opened twice");
        let state = complete(lock_owned(state));
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.resources.len(), 1);
    }

    #[test]
    fn overlapping_clients_open_one_provider_and_retain_its_worker() {
        loom::model(|| race(false));
        let failure = std::panic::catch_unwind(|| loom::model(|| race(true)))
            .expect_err("unlocked negative control was not detected");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(message.contains("workspace opened twice"), "{message}");
    }

    fn drain(broken: bool) {
        EARLY_CLOSE.with(|value| value.set(broken));
        let state = StateArc::new(Mutex::new(
            Opened::<(), loom::thread::JoinHandle<()>>::default(),
        ));
        let completed = StateArc::new(AtomicUsize::new(0));
        let closed = StateArc::new(AtomicUsize::new(0));
        {
            let mut opened = complete(lock_owned(StateArc::clone(&state)));
            opened.entries.insert("workspace".into(), StateArc::new(()));
            for _ in 0..2 {
                let completed = StateArc::clone(&completed);
                let provider = StateArc::clone(opened.entries.get("workspace").unwrap());
                opened.resources.push(thread::spawn(move || {
                    completed.fetch_add(1, Ordering::SeqCst);
                    drop(provider);
                }));
            }
        }
        complete(close_after_workers(
            &state,
            async |worker| {
                worker.join().unwrap();
                Ok(())
            },
            async |value| {
                take_owner(value)?;
                // Reentering the real registry lock detects retirement under
                // that lock; close must also follow every worker completion.
                let opened = lock_owned(StateArc::clone(&state)).await;
                assert!(opened.entries.is_empty());
                assert_eq!(
                    completed.load(Ordering::SeqCst),
                    2,
                    "close preceded worker drain"
                );
                closed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ))
        .unwrap();
        assert_eq!(closed.load(Ordering::SeqCst), 1, "provider was not closed");
    }

    #[test]
    fn shutdown_joins_effects_before_closing_providers_without_the_registry_lock() {
        loom::model(|| drain(false));
        let failure = std::panic::catch_unwind(|| loom::model(|| drain(true)))
            .expect_err("early close negative control was not detected");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("close preceded worker drain")
                || message.contains("workspace still has an execution owner"),
            "{message}"
        );
    }
}

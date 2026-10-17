//! The real retry driver over atomic provider CAS outcomes. Filesystem-internal
//! locks and IO are provider contracts, not modeled synchronization here.
use std::cell::Cell;
loom::thread_local! {static SKIP:Cell<bool> = Cell::new(false);}
pub(super) fn skip_retry() -> bool {
    SKIP.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use crate::sync::{Arc, SyncMutex};

    #[derive(Default)]
    struct State {
        revision: u64,
        files: [bool; 2],
    }
    struct Writer {
        state: Arc<SyncMutex<State>>,
        index: usize,
    }

    fn attempt(writer: &mut Writer) -> Result<BoundedContentFuture<'_, Option<()>>> {
        BoundedContentFuture::new(None, || async move {
            let expected = writer.state.lock().unwrap().revision;
            loom::thread::yield_now();
            // The sole provider-state lock represents an atomic CAS. Drop it
            // before the driver retries or another async operation is polled.
            let mut state = writer.state.lock().unwrap();
            if state.revision != expected {
                return Ok(None);
            }
            state.revision += 1;
            state.files[writer.index] = true;
            Ok(Some(()))
        })
    }

    fn model(broken: bool) {
        let state = Arc::new(SyncMutex::new(State::default()));
        let workers: Vec<_> = (0..2)
            .map(|index| {
                let state = Arc::clone(&state);
                loom::thread::spawn(move || {
                    SKIP.with(|mode| mode.set(broken));
                    loom::future::block_on(finish(&mut Writer { state, index }, attempt))
                })
            })
            .collect();
        let outcomes: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        let files = state.lock().unwrap().files;
        drop(state);
        assert!(
            outcomes.iter().all(Result::is_ok),
            "independent publication lost: {outcomes:?}"
        );
        assert_eq!(files, [true, true]);
    }

    #[test]
    fn independent_publications_retry_without_losing_either_file() {
        loom::model(|| model(false));
    }

    #[test]
    #[should_panic(expected = "independent publication lost")]
    fn negative_control_detects_omitting_a_definite_conflict_retry() {
        loom::model(|| model(true));
    }

    #[test]
    fn fencing_pinned_conflicts_and_uncertain_errors_never_retry() {
        for kind in ["fenced", "pinned conflict", "unknown commit"] {
            loom::model(move || {
                let mut calls = 0;
                let result = loom::future::block_on(finish(&mut calls, |calls| {
                    BoundedContentFuture::new(None, || async move {
                        *calls += 1;
                        Err::<Option<()>, _>(Error::Conflict(kind.into()))
                    })
                }));
                assert_eq!(calls, 1);
                assert!(matches!(result,Err(Error::Conflict(message)) if message == kind));
            });
        }
    }
}

use super::*;
use crate::verification::{block_on, check_with_bound};

fn race<const ORDERED: bool>() {
    check_with_bound(Some(2), move || {
        let stream = query_model::fixture(false)?;
        let keeper = std::mem::ManuallyDrop::new(stream.clone());
        let mut actors = Vec::new();
        for path in ["a", "b"] {
            actors.push(loom::thread::spawn({
                let stream = stream.clone();
                move || -> Result<(), StreamError> {
                    block_on(deferring_durability(stream.append(AppendRequest {
                        path: StreamPath::new(path)?,
                        records: vec![crate::OwnedBytes::from_static(b"record")],
                        if_tail: Some(0),
                        idempotency_key: None,
                    })))?;
                    if ORDERED {
                        stream.inner.flush_through(None)?;
                    } else {
                        stream.inner.flush_under_device(None)?;
                    }
                    Ok(())
                }
            }));
        }
        for actor in actors {
            match actor.join() {
                Ok(result) => result?,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        let journal = stream.inner.journal.lock().unwrap();
        assert_eq!(journal.appended, 2);
        assert_eq!(journal.synced, 2);
        assert_eq!(journal.group_start, journal.bytes);
        drop(journal);
        drop(stream);
        drop(std::mem::ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}

/// Exhausts two real mutations and competing flushes at two preemptions.
pub fn group_seals_follow_successful_predecessor_flushes() {
    race::<true>();
}
/// Omitting only the device gate permits the next seal before publication.
pub fn negative_control_group_seal_overtakes_flush() {
    race::<false>();
}

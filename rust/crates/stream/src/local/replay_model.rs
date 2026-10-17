use super::*;
use crate::verification::{block_on, check};

// Only the durability handoff is changed by replay elision. Provider identity
// and frame counts are checked by replay_work's shrinking properties. Exercise
// the new journal promotion against the actual flush and watch publication.
// Visibility still encloses promotion in production; no model-only lock is used.
fn race<const REQUIRE: bool>() {
    check(move || {
        let stream = query_model::fixture(false)?;
        let keeper = std::mem::ManuallyDrop::new(stream.clone());
        block_on(deferring_durability(stream.append(AppendRequest {
            path: StreamPath::new("replay-work")?,
            records: vec![crate::OwnedBytes::from_static(b"one")],
            if_tail: Some(0),
            idempotency_key: None,
        })))?;
        let (file, durability) = {
            let journal = stream.inner.journal.lock().unwrap();
            (journal.sync.clone(), journal.limits.durability)
        };
        let flusher = loom::thread::spawn({
            let stream = stream.clone();
            move || stream.inner.flush_through(1, &file, durability, None)
        });
        stream.require_accepted::<REQUIRE>()?;
        assert_eq!(
            stream.inner.required()?,
            1,
            "replay omitted the original durability requirement"
        );
        flusher.join().map_err(|_| "flusher panicked")?;
        block_on(stream.await_publication(stream.inner.required()?))?;
        let journal = stream.inner.journal.lock().unwrap();
        assert_eq!(journal.appended, 1);
        assert_eq!(journal.synced, 1);
        drop(journal);
        drop(stream);
        drop(std::mem::ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}

/// Exhausts the replay durability handoff against a competing flush.
pub fn replay_and_flush_preserve_durability() {
    race::<true>();
}

/// Detects acknowledgement without requiring the original deferred frame.
pub fn negative_control_acknowledges_an_unflushed_original() {
    race::<false>();
}

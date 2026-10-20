use super::*;
use crate::verification::block_on;

// Only the durability handoff is changed by replay elision. Provider identity
// and frame counts are checked by replay_work's shrinking properties. Exercise
// the new journal promotion against the actual flush and watch publication.
// Visibility still encloses promotion in production; no model-only lock is used.
/// Runs the production replay durability handoff against one competing flush.
pub fn replay_durability_case<const REQUIRE: bool>()
-> std::result::Result<(u64, u64, u64), Box<dyn std::error::Error + Send + Sync>> {
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
    let required = stream.inner.required()?;
    flusher.join().map_err(|_| "flusher panicked")?;
    block_on(stream.await_durable(stream.inner.required()?))?;
    let journal = stream.inner.journal.lock().unwrap();
    let actual = (required, journal.appended, journal.synced);
    drop(journal);
    drop(stream);
    drop(std::mem::ManuallyDrop::into_inner(keeper));
    Ok(actual)
}

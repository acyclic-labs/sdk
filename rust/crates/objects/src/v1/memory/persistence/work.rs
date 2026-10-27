//! Test-only observation and exact scan reversions inside the batch commit path.
use super::*;
use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Visits {
    pub encoded: usize,
    pub relocated: usize,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Scan {
    #[default]
    None,
    Encoding,
    Relocation,
}

thread_local! {
    static VISITS: Cell<Visits> = const { Cell::new(Visits { encoded: 0, relocated: 0 }) };
    static SCAN: Cell<Scan> = const { Cell::new(Scan::None) };
    static MEASURING: Cell<bool> = const { Cell::new(false) };
}

// Receipt and journal allocations include protobuf timestamps. Fix the clock
// only within measured test batches so variable-width nanos cannot affect the
// exact byte oracle. Other threads and ordinary tests retain the real clock.
pub(in crate::v1::memory) fn timestamp() -> Option<prost_types::Timestamp> {
    MEASURING.get().then_some(prost_types::Timestamp {
        seconds: 1_800_000_000,
        nanos: 123_456_789,
    })
}

pub(in crate::v1::memory) fn encoded() {
    let mut visits = VISITS.get();
    visits.encoded += 1;
    VISITS.set(visits);
}

pub(super) fn relocated() {
    let mut visits = VISITS.get();
    visits.relocated += 1;
    VISITS.set(visits);
}

pub(super) fn measure<T>(scan: Scan, run: impl FnOnce() -> T) -> (T, Visits) {
    struct Restore(Scan, bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            SCAN.set(self.0);
            MEASURING.set(self.1);
        }
    }
    let restore = Restore(SCAN.replace(scan), MEASURING.replace(true));
    let before = VISITS.get();
    let result = run();
    let after = VISITS.get();
    drop(restore);
    (
        result,
        Visits {
            encoded: after.encoded - before.encoded,
            relocated: after.relocated - before.relocated,
        },
    )
}

pub(super) fn restore_encoding(
    state: &State,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    if SCAN.get() == Scan::Encoding {
        // The former difference() loop encoded every part before checking
        // whether its receipt had changed. Keep the real codec and allocations.
        for upload in state.uploads.values() {
            for (_, body) in upload.parts.values() {
                let _ = bodies(body, publication)?;
            }
        }
    }
    Ok(())
}

pub(super) fn restore_relocation(
    state: &mut State,
    moves: &crate::body::LocalBodyRelocations,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    if SCAN.get() == Scan::Relocation {
        // The former place() loop took mutable persistent-tree paths even for
        // already-persisted parts absent from the delta.
        for (id, _) in &state.uploads.clone() {
            let upload = state.uploads.get_mut(id).ok_or(Error::from(Unavailable))?;
            for (number, _) in &upload.parts.clone() {
                let (_, body) = upload
                    .parts
                    .get_mut(number)
                    .ok_or(Error::from(Unavailable))?;
                if let Some(moved) = publication.relocated(body, moves)? {
                    *body = moved;
                }
            }
        }
    }
    Ok(())
}

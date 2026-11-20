use std::cell::{Cell, RefCell};

pub type Event = (&'static str, u64, u64, u64);

// An observer for a single-thread block_on test, never shared with the meter's
// other callers. No lock or production synchronization path is introduced.
std::thread_local! {
    static EVENTS: RefCell<Option<Vec<Event>>> = const { RefCell::new(None) };
    static READ_FIRST: Cell<bool> = const { Cell::new(false) };
}

pub struct RecordingOrder(bool);
impl Drop for RecordingOrder {
    fn drop(&mut self) {
        READ_FIRST.set(self.0);
    }
}
pub fn reorder_recordings() -> RecordingOrder {
    RecordingOrder(READ_FIRST.replace(true))
}
pub fn recordings_reordered() -> bool {
    READ_FIRST.get()
}

pub fn begin() {
    EVENTS.with(|events| assert!(events.replace(Some(Vec::new())).is_none()));
}

pub fn finish() -> Vec<Event> {
    EVENTS.with(|events| {
        let events = events.take();
        assert!(events.is_some(), "trace was started");
        events.unwrap_or_default()
    })
}

pub fn record(name: &'static str, a: u64, b: u64, c: u64) {
    EVENTS.with(|events| {
        if let Some(events) = events.borrow_mut().as_mut() {
            events.push((name, a, b, c));
        }
    });
}

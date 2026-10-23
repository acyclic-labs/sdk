use super::{AtomicU64, Mutex, Ordering, StreamError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Position {
    pub(super) next: u64,
    pub(super) opaque: Vec<u8>,
    pub(super) preceding: Option<[u8; 32]>,
}
struct State {
    position: Position,
    generation: u64,
}
pub(super) struct Progress {
    // Lock order: this is the only cursor lock. Never nest it with another
    // lock or hold it across an await; sequence, opaque cursor and checked
    // preceding commit form one cut. Received-but-unconsumed records never enter it.
    state: Mutex<State>,
    cancelled: AtomicU64,
}
impl Progress {
    #[cfg(any(test, acyclic_stream_loom))]
    pub(super) fn new(from: u64) -> Self {
        Self::with_preceding(from, None)
    }
    #[cfg(any(test, acyclic_stream_loom))]
    pub(super) fn with_preceding(from: u64, preceding: Option<[u8; 32]>) -> Self {
        Self::from_cursor(
            &crate::wire::SessionCursor {
                next: from,
                opaque: Default::default(),
            },
            preceding,
        )
    }
    pub(super) fn from_cursor(
        cursor: &crate::wire::SessionCursor,
        preceding: Option<[u8; 32]>,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                position: Position {
                    next: cursor.next,
                    opaque: cursor.opaque.to_vec(),
                    preceding,
                },
                generation: 1,
            }),
            cancelled: AtomicU64::new(0),
        }
    }
    pub(super) fn cancel(&self) {
        self.cancelled.store(1, Ordering::Release);
    }
    pub(super) fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire) != 0
    }
    pub(super) fn next(&self) -> Result<u64, StreamError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .position
            .next)
    }
    pub(super) fn fence(&self) -> Result<u64, StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        Ok(state.generation)
    }
    pub(super) fn position(&self) -> Result<(u64, Vec<u8>), StreamError> {
        let position = self.subscription()?;
        Ok((position.next, position.opaque))
    }
    pub(super) fn subscription(&self) -> Result<Position, StreamError> {
        let state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        Ok(state.position.clone())
    }
    pub(super) fn consume_record(
        &self,
        generation: u64,
        sequence: u64,
        commit: [u8; 32],
    ) -> Result<bool, StreamError> {
        self.consume_cut(generation, sequence, &[], Some(commit), false)
    }
    pub(super) fn current(&self, generation: u64) -> Result<bool, StreamError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .generation
            == generation)
    }
    pub(super) fn consume_progress(
        &self,
        generation: u64,
        next: u64,
        preceding: [u8; 32],
    ) -> Result<bool, StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        if generation != state.generation || next < state.position.next {
            return Ok(false);
        }
        state.position.next = next;
        state.position.opaque.clear();
        if state.position.preceding.is_some() {
            state.position.preceding = Some(preceding);
        }
        Ok(true)
    }
    pub(super) fn consume_opaque(
        &self,
        generation: u64,
        sequence: u64,
        opaque: &[u8],
    ) -> Result<bool, StreamError> {
        // A child cut counts provider discoveries, including children hidden
        // by authority. Only consuming an allowed child advances this cursor.
        self.consume_cut(generation, sequence, opaque, None, true)
    }
    pub(super) fn consume_latest(
        &self,
        generation: u64,
        sequence: u64,
    ) -> Result<bool, StreamError> {
        self.consume_cut(generation, sequence, &[], None, true)
    }
    fn consume_cut(
        &self,
        generation: u64,
        sequence: u64,
        opaque: &[u8],
        commit: Option<[u8; 32]>,
        conflated: bool,
    ) -> Result<bool, StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        if generation != state.generation || sequence < state.position.next {
            return Ok(false);
        }
        if sequence > state.position.next && !conflated {
            return Err(StreamError::Unavailable);
        }
        if state.position.preceding.is_some() && commit.is_none() {
            return Err(StreamError::Unavailable);
        }
        state.position.next = sequence.checked_add(1).ok_or(StreamError::OutOfRange)?;
        state.position.opaque = opaque.to_vec();
        if state.position.preceding.is_some() {
            state.position.preceding = commit;
        }
        Ok(true)
    }
}

pub(super) fn allocate(counter: &AtomicU64) -> Result<u64, StreamError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(2)
        })
        .map_err(|_| StreamError::Capacity)
}

#[cfg(acyclic_stream_loom)]
#[path = "progress/model.rs"]
mod model;
#[cfg(test)]
#[cfg(not(acyclic_stream_loom))]
#[path = "progress/tests.rs"]
mod tests;
#[cfg(acyclic_stream_loom)]
pub use model::verify_latest_progress;

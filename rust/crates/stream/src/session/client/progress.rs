use super::{AtomicU64, Mutex, Ordering, StreamError};

pub(super) struct Progress {
    // Lock order: this is the only cursor lock. Never nest it with another
    // lock or hold it across an await; position and opaque cursor move together.
    state: Mutex<(u64, u64, Vec<u8>)>,
    cancelled: AtomicU64,
}
impl Progress {
    pub(super) fn new(from: u64) -> Self {
        Self {
            state: Mutex::new((from, 1, Vec::new())),
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
        Ok(self.state.lock().map_err(|_| StreamError::Unavailable)?.0)
    }
    pub(super) fn fence(&self) -> Result<u64, StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        state.1 = state.1.checked_add(1).ok_or(StreamError::Capacity)?;
        Ok(state.1)
    }
    pub(super) fn position(&self) -> Result<(u64, Vec<u8>), StreamError> {
        let state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        Ok((state.0, state.2.clone()))
    }
    pub(super) fn consume(&self, generation: u64, sequence: u64) -> Result<bool, StreamError> {
        self.consume_opaque(generation, sequence, &[])
    }
    pub(super) fn consume_opaque(
        &self,
        generation: u64,
        sequence: u64,
        opaque: &[u8],
    ) -> Result<bool, StreamError> {
        let mut state = self.state.lock().map_err(|_| StreamError::Unavailable)?;
        if generation != state.1 || sequence < state.0 {
            return Ok(false);
        }
        if sequence > state.0 {
            return Err(StreamError::Unavailable);
        }
        state.0 = state.0.checked_add(1).ok_or(StreamError::OutOfRange)?;
        state.2 = opaque.to_vec();
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

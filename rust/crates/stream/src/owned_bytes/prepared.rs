use super::*;

/// Writable fixed storage allocated before the physical action.
#[must_use]
pub struct PreparedOwnedBytes<G: Send + Sync + 'static> {
    guard: Arc<GuardBundle<G>>,
}
impl<G: Send + Sync + 'static> PreparedOwnedBytes<G> {
    /// Transfers fully initialized storage and its original owner.
    pub fn new(
        mut data: Vec<u8>,
        guard: G,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        if data.len() != data.capacity() {
            return Err(PreparedOwnedBytesFailure {
                data,
                original_guard: guard,
                error: PreparedOwnedBytesError::NotFullyInitialized,
            });
        }
        let mut guard = Some(guard);
        GuardBundle::new(&mut data, &mut guard, &meter)
            .map(|guard| Self { guard })
            .map_err(|error| PreparedOwnedBytesFailure {
                data,
                original_guard: guard.expect("rejected preparation retains its owner"),
                error: PreparedOwnedBytesError::Admission(error),
            })
    }
    /// Initialized writable bound.
    #[must_use]
    pub fn maximum_len(&self) -> usize {
        self.guard.data.len()
    }
    /// Borrows the exclusively owned initialized bytes.
    pub fn writable(&mut self) -> &mut [u8] {
        &mut Arc::get_mut(&mut self.guard)
            .expect("unpublished storage is unique")
            .data
    }
    /// Publishes a checked prefix without allocating.
    pub fn freeze(self, actual_len: usize) -> Result<OwnedBytes, Self> {
        if actual_len > self.maximum_len() {
            return Err(self);
        }
        Ok(OwnedBytes {
            retained_capacity: Some(self.maximum_len()),
            range: 0..actual_len,
            data: Data::Native(self.guard),
        })
    }
}
impl<G: Send + Sync + 'static> AsMut<[u8]> for PreparedOwnedBytes<G> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.writable()
    }
}
impl<G: Send + Sync + 'static> std::fmt::Debug for PreparedOwnedBytes<G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedOwnedBytes")
            .field("maximum_len", &self.maximum_len())
            .finish_non_exhaustive()
    }
}
/// Invalid initialized storage or the original admission failure.
#[derive(Debug)]
pub enum PreparedOwnedBytesError {
    /// Initialized length does not cover actual capacity.
    NotFullyInitialized,
    /// Original admission cause.
    Admission(io::Error),
}
/// Failure retaining the payload until its original owner can retire.
#[must_use]
pub struct PreparedOwnedBytesFailure<G> {
    pub(super) data: Vec<u8>,
    pub(super) error: PreparedOwnedBytesError,
    pub(super) original_guard: G,
}
impl<G> PreparedOwnedBytesFailure<G> {
    /// Borrows the original cause.
    #[must_use]
    pub const fn error(&self) -> &PreparedOwnedBytesError {
        &self.error
    }
    /// Drops physical storage before returning its original cause.
    #[must_use]
    pub fn into_error(self) -> PreparedOwnedBytesError {
        let Self {
            data,
            error,
            original_guard,
        } = self;
        drop(data);
        drop(original_guard);
        error
    }
    /// Actual retained vector capacity.
    #[must_use]
    pub fn retained_capacity(&self) -> usize {
        self.data.capacity()
    }
}
impl<G> std::fmt::Debug for PreparedOwnedBytesFailure<G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedOwnedBytesFailure")
            .field("error", &self.error)
            .field("retained_capacity", &self.retained_capacity())
            .finish_non_exhaustive()
    }
}

use super::*;

/// Writable fixed storage allocated before the physical action.
#[must_use]
pub struct PreparedOwnedBytes<G: Send + Sync + 'static> {
    guard: Arc<GuardBundle<G>>,
}
impl<G: Send + Sync + 'static> PreparedOwnedBytes<G> {
    pub(crate) fn new_buffer(
        data: Vec<u8>,
        guard: G,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        GuardBundle::new(data, guard, meter).map(|guard| Self { guard })
    }

    pub(crate) fn buffer(&self) -> &[u8] {
        &self.guard.data
    }

    fn buffer_mut(&mut self) -> &mut Vec<u8> {
        let Some(guard) = Arc::get_mut(&mut self.guard) else {
            unreachable!("prepared storage is never shared before freeze")
        };
        &mut guard.data
    }

    pub(crate) fn retained_capacity(&self) -> usize {
        self.guard.data.capacity()
    }

    pub(crate) fn extend_within_capacity(&mut self, bytes: &[u8]) -> Option<()> {
        let length = self.buffer().len().checked_add(bytes.len())?;
        if length > self.retained_capacity() {
            return None;
        }
        self.buffer_mut().extend_from_slice(bytes);
        Some(())
    }

    /// Transfers fully initialized storage and its original owner.
    pub fn new(
        data: Vec<u8>,
        guard: G,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> Result<Self, PreparedOwnedBytesFailure<G>> {
        if data.len() != data.capacity() {
            return Err(PreparedOwnedBytesFailure {
                data,
                original_guard: guard,
                error: PreparedOwnedBytesError::NotFullyInitialized,
            });
        }
        Self::new_buffer(data, guard, meter)
    }
    /// Initialized writable bound.
    #[must_use]
    pub fn maximum_len(&self) -> usize {
        self.buffer().len()
    }
    /// Borrows the exclusively owned initialized bytes.
    ///
    /// # Panics
    /// Panics if unpublished storage has been shared.
    pub fn writable(&mut self) -> &mut [u8] {
        self.buffer_mut().as_mut_slice()
    }
    /// Publishes a checked prefix without allocating.
    pub fn freeze(self, actual_len: usize) -> Result<OwnedBytes, Self> {
        if actual_len > self.maximum_len() {
            return Err(self);
        }
        Ok(OwnedBytes {
            retained_capacity: Some(self.retained_capacity()),
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

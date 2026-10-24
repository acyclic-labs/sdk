//! Exclusive edits retain the allocation owner established by the producer.
use super::*;

enum Storage {
    Ordinary(Vec<u8>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(Arc<GuardBundle<dyn Send + Sync>>),
}

/// Unshared, complete byte storage whose native owner cannot be substituted.
/// Freezing transfers the same allocation back to immutable bytes.
#[must_use]
pub struct UniqueOwnedBytes {
    storage: Storage,
}

impl OwnedBytes {
    /// Recovers exclusively owned complete storage, preserving any original
    /// native guard. Shared storage, slices and borrowed bytes remain immutable.
    pub fn try_into_unique(self) -> Result<UniqueOwnedBytes, Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Self {
            data: Data::Native(mut guard),
            range,
            retained_capacity,
        } = self
        {
            if range.start != 0
                || range.end != guard.data.len()
                || Arc::get_mut(&mut guard).is_none()
            {
                return Err(Self {
                    data: Data::Native(guard),
                    range,
                    retained_capacity,
                });
            }
            return Ok(UniqueOwnedBytes {
                storage: Storage::Native(guard),
            });
        }
        self.try_into_ordinary_vec().map(|data| UniqueOwnedBytes {
            storage: Storage::Ordinary(data),
        })
    }
}

impl UniqueOwnedBytes {
    /// Borrows initialized bytes without exposing the ownership guard.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        match &self.storage {
            Storage::Ordinary(data) => data,
            #[cfg(not(target_arch = "wasm32"))]
            Storage::Native(guard) => &guard.data,
        }
    }

    /// Actual capacity owned by the original allocation.
    #[must_use]
    pub fn capacity(&self) -> usize {
        match &self.storage {
            Storage::Ordinary(data) => data.capacity(),
            #[cfg(not(target_arch = "wasm32"))]
            Storage::Native(guard) => guard.data.capacity(),
        }
    }

    /// Admits a real growth attempt and the relocating allocation's full peak
    /// before calling the allocator. A denial preserves all initialized bytes.
    pub fn reserve_capacity(&mut self, capacity: usize) -> std::io::Result<()> {
        if capacity <= self.capacity() {
            #[cfg(not(target_arch = "wasm32"))]
            if let Storage::Native(guard) = &self.storage {
                guard.metadata.meter().check()?;
            }
            return Ok(());
        }
        match &mut self.storage {
            Storage::Ordinary(data) => data
                .try_reserve_exact(capacity - data.len())
                .map_err(std::io::Error::other),
            #[cfg(not(target_arch = "wasm32"))]
            Storage::Native(guard) => {
                let guard = exclusive(guard);
                let meter = Arc::clone(guard.metadata.meter());
                meter.check()?;
                let old = guard.data.capacity();
                let copied = guard.data.len() as u64;
                meter.admit_codec(0, copied, 0)?;
                let mut growth = ReadAllocation::new(&meter, capacity)?;
                guard
                    .data
                    .try_reserve_exact(capacity - guard.data.len())
                    .map_err(|error| meter.allocation_failed(error))?;
                // The producer's existing claim continues to cover `old`
                // bytes of the relocated allocation; only the delta joins the
                // header's claim. Reconcile before any fallible observation.
                let actual = guard.data.capacity();
                let recorded = growth.reconcile_capacity(actual);
                growth.release(old)?;
                guard.metadata.absorb(&mut growth)?;
                recorded?;
                meter.record_codec(0, copied, 0)
            }
        }
    }

    /// Replaces an initialized suffix within already admitted capacity.
    /// The operation checks cancellation and copy allowance before mutation.
    pub fn replace_suffix(&mut self, prefix: usize, suffix: &[u8]) -> std::io::Result<()> {
        let length = prefix
            .checked_add(suffix.len())
            .ok_or(std::io::ErrorKind::InvalidInput)?;
        if prefix > self.as_slice().len() || length > self.capacity() {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        match &mut self.storage {
            Storage::Ordinary(data) => {
                data.truncate(prefix);
                data.extend_from_slice(suffix);
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Storage::Native(guard) => {
                let guard = exclusive(guard);
                guard.metadata.meter().check()?;
                guard
                    .metadata
                    .meter()
                    .admit_codec(0, suffix.len() as u64, 0)?;
                guard.data.truncate(prefix);
                guard.data.extend_from_slice(suffix);
                guard
                    .metadata
                    .meter()
                    .record_codec(0, suffix.len() as u64, 0)
            }
        }
    }

    /// Publishes the current initialized bytes without allocating or checking
    /// cancellation, including when a preceding observation reported failure.
    #[must_use]
    pub fn freeze(self) -> OwnedBytes {
        match self.storage {
            Storage::Ordinary(data) => data.into(),
            #[cfg(not(target_arch = "wasm32"))]
            Storage::Native(guard) => OwnedBytes {
                range: 0..guard.data.len(),
                retained_capacity: Some(guard.data.capacity()),
                data: Data::Native(guard),
            },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn exclusive(guard: &mut Arc<GuardBundle<dyn Send + Sync>>) -> &mut GuardBundle<dyn Send + Sync> {
    Arc::get_mut(guard).unwrap_or_else(|| unreachable!("unique storage never exposes an alias"))
}

impl std::fmt::Debug for UniqueOwnedBytes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UniqueOwnedBytes")
            .field("length", &self.as_slice().len())
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

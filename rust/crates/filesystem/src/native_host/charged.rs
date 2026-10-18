//! Native storage owned together with the ledger guard that paid for it.
use crate::{DemandError, NativeAllocation};
use std::mem::ManuallyDrop;

/// A value and the native ledger guard charged for its storage. Dropping it
/// on any path frees the value's storage before the guard refunds the
/// ledger, so the shared ledger never falls below the bytes still live.
pub(crate) struct Charged<T> {
    value: ManuallyDrop<T>,
    guard: Option<NativeAllocation>,
}

impl<T> Charged<T> {
    pub(crate) fn new(value: T, guard: Option<NativeAllocation>) -> Self {
        Self {
            value: ManuallyDrop::new(value),
            guard,
        }
    }

    /// Moves the value and guard out together; nothing is freed or refunded.
    fn into_parts(self) -> (T, Option<NativeAllocation>) {
        let mut this = ManuallyDrop::new(self);
        let guard = this.guard.take();
        // SAFETY: `this` is never dropped, so the value is taken exactly once.
        let value = unsafe { ManuallyDrop::take(&mut this.value) };
        (value, guard)
    }

    /// Converts the value under the same guard. `convert` consumes the old
    /// value, so its storage is moved into the result or freed before an
    /// error refunds the guard; errors must not own charged storage.
    pub(crate) fn map<U, E>(
        self,
        convert: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<Charged<U>, E> {
        let (value, guard) = self.into_parts();
        match convert(value) {
            Ok(value) => Ok(Charged::new(value, guard)),
            Err(error) => {
                drop(guard);
                Err(error)
            }
        }
    }

    /// Places `item`'s value into this one and takes over its charge. If the
    /// charges cannot combine, the item's storage is freed before its refund.
    pub(crate) fn absorb<U>(
        &mut self,
        item: Charged<U>,
        place: impl FnOnce(&mut T, U),
    ) -> Result<(), DemandError> {
        let (value, guard) = item.into_parts();
        if let Some(mut incoming) = guard {
            match &mut self.guard {
                Some(guard) => {
                    if let Err(error) = guard.absorb(&mut incoming) {
                        drop(value);
                        drop(incoming);
                        return Err(crate::native_name::original_name_error(error));
                    }
                }
                None => self.guard = Some(incoming),
            }
        }
        place(&mut self.value, value);
        Ok(())
    }
}

impl<T> Charged<Vec<T>> {
    /// Empty storage for `capacity` values, charged to `original`.
    pub(crate) fn buffer(
        capacity: usize,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, DemandError> {
        let (values, guard) = crate::native_host::buffer::<T, _>(capacity, &mut &*original)
            .map_err(crate::native_name::original_name_error)?;
        Ok(Self::new(values, Some(guard)))
    }
}

impl Charged<Vec<u8>> {
    /// Publishes the bytes with their guard into a reserved byte carrier.
    pub(crate) fn into_owned_bytes(
        self,
        header: acyclic_native_runtime::ReadAllocationReservation,
    ) -> Result<acyclic_stream::OwnedBytes, acyclic_stream::PreparedOwnedBytesError> {
        acyclic_stream::OwnedBytes::from_vec_with_reserved_guard(self.into_parts(), header)
            .map_err(|failure| failure.into_error())
    }
}

impl<T> std::ops::Deref for Charged<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Charged<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> Drop for Charged<T> {
    fn drop(&mut self) {
        #[cfg(test)]
        freeing(&mut self.guard);
        // SAFETY: the value is dropped once, here, and never used again.
        unsafe { ManuallyDrop::drop(&mut self.value) };
        drop(self.guard.take());
    }
}

#[cfg(test)]
thread_local! {
    static REFUND_FIRST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Runs `run` with charged storage refunded before it is freed: the
/// negative control for the ledger property.
#[cfg(test)]
pub(crate) fn refunding_first<T>(run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            REFUND_FIRST.set(false);
        }
    }
    REFUND_FIRST.set(true);
    let _reset = Reset;
    run()
}

/// Records `("free", ledger live bytes, charge)` just before charged storage
/// is freed.
#[cfg(test)]
fn freeing(guard: &mut Option<NativeAllocation>) {
    let Some((ledger, charged)) = guard
        .as_ref()
        .map(|guard| (guard.work().clone(), guard.bytes()))
    else {
        return;
    };
    if REFUND_FIRST.get() {
        drop(guard.take());
    }
    acyclic_test_support::differential::record::<(&'static str, u64, u64)>((
        "free",
        ledger.live_allocation_bytes(),
        charged,
    ));
}

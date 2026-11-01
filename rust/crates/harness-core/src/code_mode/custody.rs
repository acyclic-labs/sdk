//! Rust frames funded by the same original owner as the interpreter heap.
use super::*;
use std::ops::Deref;

// Payload ownership follows the last actual Rust/JS closure reference rather
// than the caller's stack frame. No second pool or allocation index is created.
pub(super) struct Retained<T> {
    value: T,
    _custody: crate::NativeCustody,
}

impl<T> Deref for Retained<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl CodeRuntime {
    pub(super) fn retained_arc<T>(&self, value: T) -> Result<Arc<Retained<T>>> {
        #[cfg(feature = "filesystem")]
        let custody = self.claim_rust(
            acyclic_native_runtime::arc_allocation_bytes::<Retained<T>>() as u64,
            1,
        )?;
        #[cfg(not(feature = "filesystem"))]
        let custody = None;
        Ok(Arc::new(Retained {
            value,
            _custody: custody,
        }))
    }

    fn claim_rust(&self, bytes: u64, operations: u64) -> Result<crate::NativeCustody> {
        #[cfg(feature = "filesystem")]
        if let Some(allocator) = &self.native_allocator {
            return allocator.claim(bytes, operations).map(Some);
        }
        let _ = (bytes, operations);
        Ok(None)
    }
}

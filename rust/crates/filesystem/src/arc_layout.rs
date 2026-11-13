use std::alloc::{Layout, LayoutError};

pub(crate) const fn allocation_bytes(payload: Layout) -> Result<usize, LayoutError> {
    match Layout::new::<[usize; 2]>().extend(payload) {
        Ok((layout, _)) => Ok(layout.pad_to_align().size()),
        Err(error) => Err(error),
    }
}

/// Exact shared-owner allocation layout, including its header and alignment.
/// This layout is available to every operation owner, including WebAssembly.
pub fn shared_allocation_bytes<T>() -> Result<u64, LayoutError> {
    allocation_bytes(Layout::new::<T>()).map(|bytes| bytes as u64)
}

use std::alloc::{Layout, LayoutError};

pub(crate) const fn allocation_bytes(payload: Layout) -> Result<usize, LayoutError> {
    match Layout::new::<[usize; 2]>().extend(payload) {
        Ok((layout, _)) => Ok(layout.pad_to_align().size()),
        Err(error) => Err(error),
    }
}

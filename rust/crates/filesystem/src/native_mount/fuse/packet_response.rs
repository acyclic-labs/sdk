//! One response representation for vectored transports and contiguous callers.

use super::NativeMountError;
use std::io::IoSlice;

pub(super) struct PacketResponse<'a> {
    buffer: &'a mut [u8],
    length: Option<usize>,
    payload: &'a [u8],
}

impl<'a> PacketResponse<'a> {
    pub(super) fn new(
        buffer: &'a mut [u8],
        length: Option<usize>,
        payload: Option<&'a [u8]>,
    ) -> Result<Self, NativeMountError> {
        let payload = payload.unwrap_or_default();
        if length.is_some_and(|length| length > buffer.len() || payload.len() > length)
            || (length.is_none() && !payload.is_empty())
        {
            return Err(libc::EPROTO.into());
        }
        Ok(Self {
            buffer,
            length,
            payload,
        })
    }

    pub(super) fn slices(&self) -> Option<[IoSlice<'_>; 2]> {
        self.length.map(|length| {
            [
                IoSlice::new(&self.buffer[..length - self.payload.len()]),
                IoSlice::new(self.payload),
            ]
        })
    }

    /// The public raw-packet entry promises contiguous bytes. Physical
    /// transports instead borrow slices while the original reply owns payload.
    pub(super) fn contiguous(self) -> Option<&'a [u8]> {
        let length = self.length?;
        self.buffer[length - self.payload.len()..length].copy_from_slice(self.payload);
        Some(&self.buffer[..length])
    }
}

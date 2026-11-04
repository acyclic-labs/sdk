use super::*;

impl OwnedBytes {
    /// Transfers the visible range to a transport without copying payload bytes.
    /// Ordinary storage reuses its existing `Bytes` owner. Native storage admits
    /// one fixed control allocation on its original meter before transferring
    /// both the backing and its custody. Transport clones and nonempty slices
    /// retain that owner until their final retirement.
    pub fn try_into_bytes(self) -> std::io::Result<Bytes> {
        match self {
            Self {
                data: Data::Plain(bytes),
                range,
                retained_capacity,
            } => Ok(
                if retained_capacity.is_some() && range != (0..bytes.len()) {
                    bytes.slice(range)
                } else {
                    bytes
                },
            ),
            #[cfg(not(target_arch = "wasm32"))]
            bytes => {
                let control = ReadAllocation::new(
                    bytes.data.meter().ok_or(std::io::ErrorKind::InvalidData)?,
                    acyclic_auth::operation::bytes_owner_allocation_bytes::<NativeTransport>(),
                )?;
                Ok(Bytes::from_owner(NativeTransport {
                    bytes,
                    _control: control,
                }))
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeTransport {
    bytes: OwnedBytes,
    _control: ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for NativeTransport {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

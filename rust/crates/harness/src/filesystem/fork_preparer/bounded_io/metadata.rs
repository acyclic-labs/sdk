//! Admitted metadata copies and stable journal identities.
use super::*;

pub(in crate::filesystem) fn failure(
    io: Option<&PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(in crate::filesystem) fn clone_text(text: &str, io: Option<&PreparationIo>) -> Result<String> {
    crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("{text}"), work))
}

pub(in crate::filesystem) fn hex_key(bytes: &[u8], io: Option<&PreparationIo>) -> Result<String> {
    let Some(io) = io else {
        return Ok(hex::encode(bytes));
    };
    let capacity = bytes
        .len()
        .checked_mul(2)
        .ok_or_else(|| io.invalid("hex key size overflow"))?;
    let copied = WorkCounters {
        bytes_copied: capacity as u64,
        ..Default::default()
    };
    io.retain_owned(capacity as u64)?;
    io.preflight(copied)?;
    io.allocation_attempt(capacity as u64)?;
    let mut text = String::new();
    text.try_reserve_exact(capacity)
        .map_err(Error::Allocation)?;
    for byte in bytes {
        text.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
        text.push(b"0123456789abcdef"[(byte & 15) as usize] as char);
    }
    io.retain_owned((text.capacity() - capacity) as u64)?;
    io.charge(copied)?;
    Ok(text)
}

pub(in crate::filesystem) fn reserve<T>(
    values: &mut Vec<T>,
    capacity: usize,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if capacity <= values.capacity() {
        return Ok(());
    }
    let old = values.capacity();
    let bytes = (capacity - old)
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| {
            failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "vector capacity overflow",
            )
        })? as u64;
    let copied = WorkCounters {
        bytes_copied: (values.len() * std::mem::size_of::<T>()) as u64,
        ..Default::default()
    };
    if let Some(io) = io {
        io.retain_owned(bytes)?;
        io.preflight(copied)?;
        io.allocation_attempt(bytes)?;
    }
    values
        .try_reserve_exact(capacity - values.len())
        .map_err(Error::Allocation)?;
    if let Some(io) = io {
        io.retain_owned(((values.capacity() - capacity) * std::mem::size_of::<T>()) as u64)?;
        io.charge(copied)?;
    }
    Ok(())
}

pub(in crate::filesystem) fn vector<T>(
    capacity: usize,
    io: Option<&PreparationIo>,
) -> Result<Vec<T>> {
    let mut values = Vec::new();
    reserve(&mut values, capacity, io)?;
    Ok(values)
}

pub(in crate::filesystem) fn clone_bytes(
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    let output = crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let output = crate::core::copy_copy_vector(bytes, &mut quota)?;
        quota.transfer();
        Ok::<_, Error>(output)
    })?;
    source_bytes_from_vec(output, io)
}

pub(in crate::filesystem) fn source_bytes_from_vec(
    bytes: Vec<u8>,
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io
        && let Some(work) = io.original_native_work()
    {
        let custody = match io.take_native_custody(bytes.capacity() as u64) {
            Ok(custody) => custody,
            Err(error) => {
                drop(bytes);
                return Err(error);
            }
        };
        return acyclic_stream::OwnedBytes::from_vec_with_guard(
            bytes,
            custody,
            work.runtime_meter(),
        )
        .map_err(|failure| Error::NativeWork {
            cause: match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(cause) => {
                    work.runtime_error(cause)
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout
                }
            },
            custody: None,
        });
    }
    let _ = io;
    Ok(bytes.into())
}

pub(in crate::filesystem) fn reference_key(
    file: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<String> {
    match io {
        Some(io) => io.capability(file, "file:read:"),
        None => file.read_capability(),
    }
}

pub(in crate::filesystem) fn item(io: Option<&PreparationIo>, bytes: u64) -> Result<()> {
    if let Some(io) = io {
        io.charged(WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
        io.retain_owned(bytes)?;
    }
    Ok(())
}

pub(in crate::filesystem) fn digest(
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<blake3::Hash> {
    let hashed = WorkCounters {
        bytes_hashed: bytes.len() as u64,
        ..Default::default()
    };
    if let Some(io) = io {
        io.preflight(hashed)?;
    }
    let digest = blake3::hash(bytes);
    if let Some(io) = io {
        io.charge(hashed)?;
    }
    Ok(digest)
}

pub(in crate::filesystem) fn operation_key(
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<acyclic_fs::IdempotencyKey> {
    let hashed = WorkCounters {
        bytes_hashed: key.as_str().len() as u64,
        ..Default::default()
    };
    if let Some(io) = io {
        io.preflight(hashed)?;
    }
    let key = crate::filesystem::filesystem_key(key);
    if let Some(io) = io {
        io.charge(hashed)?;
    }
    Ok(key)
}

pub(in crate::filesystem) fn content_workspace(
    provider: &ProviderRef,
    volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let name = volume_storage_name(volume, io)?;
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}

pub(in crate::filesystem) fn volume_storage_name(
    volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<String> {
    let Some(io) = io else {
        return volume.storage_name(&mut None);
    };
    let encoded = io.encode(volume, io.remaining()?.bytes_encoded, true)?;
    let hold = io.guard_bytes(&encoded)?;
    let hash = digest(&encoded, Some(io))?;
    let encoding = WorkCounters {
        bytes_encoded: if io.original_native_work().is_some() {
            64
        } else {
            0
        },
        ..Default::default()
    };
    io.preflight(encoding)?;
    let mut label = [0_u8; 72];
    label[..8].copy_from_slice(b"harness-");
    label[8..].copy_from_slice(hash.to_hex().as_bytes());
    io.charge(encoding)?;
    drop(encoded);
    drop(hold);
    clone_text(
        std::str::from_utf8(&label).expect("ASCII storage name"),
        Some(io),
    )
}

impl PreparationIo {
    pub(crate) fn capability<T: Serialize>(&self, value: &T, prefix: &str) -> Result<String> {
        let _hold;
        let encoded = self.encode(value, self.remaining()?.bytes_encoded, true)?;
        _hold = self.guard_bytes(&encoded)?;
        let hash = digest(&encoded, Some(self))?;
        crate::executor::with_history_io(Some(self), |work| crate::contract::preparation_format(format_args!("{prefix}{}", hash.to_hex()), work))
    }
    pub(crate) fn contains_capability<T: Serialize>(
        &self,
        grants: &crate::Capabilities,
        value: &T,
        prefix: &str,
    ) -> Result<bool> {
        let text = self.capability(value, prefix)?;
        let held = self.guard_owned(text.capacity() as u64)?;
        let granted = grants.contains(&text);
        drop(text);
        drop(held);
        Ok(granted)
    }
}

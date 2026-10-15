use super::{MAXIMUM_RESPONSE_BYTES, await_io, transport_error};
use crate::{NativeCustody, NativeWork, Result};
use futures::io::{AsyncRead, AsyncReadExt as _};

pub(super) async fn read(
    mut reader: impl AsyncRead + Unpin,
    length: Option<u64>,
    work: NativeWork<'_>,
) -> Result<(Vec<u8>, NativeCustody)> {
    let mut custody = crate::tool::edit::allocation(work, std::mem::size_of::<Vec<u8>>())?;
    let mut bytes = Vec::new();
    let mut buffer = [0; 8192];
    while length != Some(bytes.len() as u64) {
        // The extra byte distinguishes an exactly full response from an
        // oversized response without trusting Content-Length.
        let mut maximum = buffer.len().min(MAXIMUM_RESPONSE_BYTES + 1 - bytes.len());
        if let Some(length) = length {
            maximum = maximum.min((length - bytes.len() as u64) as usize);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let remaining = work.remaining()?;
            maximum = maximum
                .min(remaining.source_bytes_read.min(remaining.bytes_copied / 2) as usize)
                .max(1);
            // Admit both copies: reader -> buffer and buffer -> result.
            // At zero allowance this fails before the reader is polled.
            work.admit(acyclic_fs::WorkCounters {
                source_bytes_read: maximum as u64,
                bytes_copied: maximum as u64 * 2,
                items_examined: 1,
                ..Default::default()
            })?;
        }
        let count = await_io(reader.read(&mut buffer[..maximum]), work)
            .await?
            .map_err(|_| transport_error(work, "cannot read search response"))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                source_bytes_read: count as u64,
                bytes_copied: count as u64,
                ..Default::default()
            })?;
        }
        if count == 0 {
            break;
        }
        if count > MAXIMUM_RESPONSE_BYTES - bytes.len() {
            return Err(crate::search::rejection(
                work,
                "search response exceeds byte limit",
            ));
        }
        let next = bytes.len() + count;
        crate::tool::edit::reserve_vec(&mut custody, &mut bytes, next)?;
        crate::tool::edit::charge(&custody, count as u64, 1)?;
        bytes.extend_from_slice(&buffer[..count]);
    }
    Ok((bytes, custody))
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests;

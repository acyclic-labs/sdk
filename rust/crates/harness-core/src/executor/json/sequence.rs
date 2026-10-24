//! Canonical array hashing with one element's scratch storage at a time.
use super::*;
use std::io::Write;

type Digests = ([u8; 32], Option<[u8; 32]>);

/// The prefix digest includes its own closing bracket. Both digests are exactly
/// those of the ordinary canonical serializer, including for an empty array.
pub(crate) fn canonical_sequence_digests<T: Serialize>(
    values: &[T],
    prefix: Option<usize>,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<Digests> {
    let mut digest = blake3::Hasher::new();
    let mut selected = None;
    let mut written = 0_u64;
    framing(&mut digest, b"[", 1, bounds, code, &mut written, work)?;
    if prefix == Some(0) {
        selected = Some(close_prefix(&digest, work)?);
    }
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            framing(&mut digest, b",", 0, bounds, code, &mut written, work)?;
        }
        let item_bounds = JsonBounds {
            maximum_bytes: written
                .checked_add(1)
                .and_then(|framed| bounds.maximum_bytes.checked_sub(framed))
                .ok_or_else(|| boundary("JSON exceeds admitted output", code))?,
            maximum_depth: bounds
                .maximum_depth
                .checked_sub(1)
                .ok_or_else(|| boundary("JSON nesting limit exceeded", code))?,
            ..bounds
        };
        // The observed serializer already meters every emitted byte. Its
        // allocation scope ends here, before the next historical message.
        if let Some(observer) = work.as_deref_mut() {
            let bytes = serialization::canonical_update_with_work(
                value,
                item_bounds,
                code,
                observer,
                &mut digest,
            )?;
            written = written
                .checked_add(bytes)
                .ok_or_else(|| boundary("JSON byte count overflow", code))?;
        } else {
            let mut value =
                serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
            value.sort_all_objects();
            let mut output = HashOutput {
                digest: &mut digest,
                written: &mut written,
                maximum: bounds.maximum_bytes.saturating_sub(1),
                buffer: arrayvec::ArrayVec::new(),
            };
            serde_json::to_writer(&mut output, &value)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            output.finish();
        }
        if prefix == Some(index + 1) {
            selected = Some(close_prefix(&digest, work)?);
        }
    }
    framing(&mut digest, b"]", 0, bounds, code, &mut written, work)?;
    Ok((*digest.finalize().as_bytes(), selected))
}

fn close_prefix(
    digest: &blake3::Hasher,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<[u8; 32]> {
    if let Some(observer) = work.as_deref_mut() {
        observer.hashed_bytes(1)?;
    }
    let mut closed = digest.clone();
    closed.update(b"]");
    Ok(*closed.finalize().as_bytes())
}

fn framing(
    digest: &mut blake3::Hasher,
    bytes: &[u8],
    items: u64,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    written: &mut u64,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<()> {
    *written = written
        .checked_add(bytes.len() as u64)
        .filter(|size| *size <= bounds.maximum_bytes)
        .ok_or_else(|| boundary("JSON exceeds admitted output", code))?;
    if let Some(observer) = work.as_deref_mut() {
        observer.codec_charge(JsonCodecCharge::Work {
            bytes: bytes.len() as u64,
            items,
        })?;
        observer.hashed_bytes(bytes.len() as u64)?;
    }
    digest.update(bytes);
    Ok(())
}

struct HashOutput<'a> {
    digest: &'a mut blake3::Hasher,
    written: &'a mut u64,
    maximum: u64,
    buffer: arrayvec::ArrayVec<u8, 4096>,
}
impl HashOutput<'_> {
    fn finish(&mut self) {
        self.digest.update(&self.buffer);
        self.buffer.clear();
    }
}
impl Write for HashOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.write_all(bytes)?;
        Ok(bytes.len())
    }
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        *self.written = self
            .written
            .checked_add(bytes.len() as u64)
            .filter(|size| *size <= self.maximum)
            .ok_or_else(|| std::io::Error::other("JSON exceeds admitted output"))?;
        if self.buffer.try_extend_from_slice(bytes).is_err() {
            self.finish();
            if bytes.len() >= self.buffer.capacity() {
                self.digest.update(bytes);
            } else {
                self.buffer
                    .try_extend_from_slice(bytes)
                    .map_err(|_| std::io::ErrorKind::WriteZero)?;
            }
        }
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;

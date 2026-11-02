//! Protected operation payloads transferred without copying their backing.

use zeroize::Zeroizing;

/// Transfers collected bytes into protected storage. Reclaims the original
/// backing when `Bytes` can transfer it, and copies otherwise. Only the returned
/// allocation is scrubbed; other transport owners retain their own backing.
/// This supplies neither authority nor allocation credit.
pub fn protect_bytes(bytes: bytes::Bytes) -> Zeroizing<Vec<u8>> {
    Zeroizing::new(bytes.into())
}

/// Ensures room for `capacity` bytes without releasing an unscrubbed backing.
/// Unlike ordinary vector reservation, growth allocates a separate replacement,
/// copies the initialized bytes once, then scrubs and releases the old storage.
/// Allocation failure preserves the original bytes and capacity. A sufficient
/// capacity performs no allocation or copy.
///
/// The caller must admit the full replacement while retaining the old backing's
/// credit before calling this operation, then retire the old credit afterwards.
/// This supplies neither capacity credit nor authority. Subsequent writes must
/// stay within the reserved capacity to avoid ordinary vector reallocation.
pub fn reserve_protected_bytes(
    bytes: &mut Zeroizing<Vec<u8>>,
    capacity: usize,
) -> Result<(), std::collections::TryReserveError> {
    if capacity > bytes.capacity() {
        let mut replacement = Zeroizing::new(Vec::new());
        replacement.try_reserve_exact(capacity)?;
        replacement.extend_from_slice(bytes);
        *bytes = replacement;
    }
    Ok(())
}

/// An owned operation payload, scrubbed when its final transport owner retires.
///
/// Moving this value transfers the original allocation. It supplies neither
/// authority nor allocation credit; an issuing operation can transfer its
/// original allocation custody with [`Self::with_custody`].
/// Transport libraries may make their own internal copies; this owner only
/// guarantees scrubbing of the allocation it receives.
pub struct ProtectedBytes<C = ()> {
    // Field order scrubs and destroys the payload before retiring its custody.
    bytes: Zeroizing<Vec<u8>>,
    _custody: C,
}

impl ProtectedBytes {
    /// Serializes one JSON payload directly into protected storage. Partial
    /// output and replaced backings are scrubbed on success or serialization
    /// failure. This is one serializer pass, with geometric buffer growth and
    /// no final payload copy; it supplies neither authority nor capacity credit.
    pub fn from_json<T: serde::Serialize + ?Sized>(value: &T) -> serde_json::Result<Self> {
        // Match serde_json::to_vec's initial capacity, without its unprotected
        // reallocations. This is an initial allocation, never a payload limit.
        let mut writer = JsonWriter(Zeroizing::new(Vec::with_capacity(128)));
        serde_json::to_writer(&mut writer, value)?;
        Ok(Self::new(writer.0))
    }

    /// Takes the original protected allocation without copying its bytes.
    pub fn new(bytes: Zeroizing<Vec<u8>>) -> Self {
        Self {
            bytes,
            _custody: (),
        }
    }

    /// Retains the original payload's allocation custody without allocating or
    /// copying. The protected bytes are destroyed before this custody retires.
    /// This does not admit capacity or replace the operation's authority/stop.
    pub fn with_custody<C>(self, custody: C) -> ProtectedBytes<C> {
        ProtectedBytes {
            bytes: self.bytes,
            _custody: custody,
        }
    }
}

impl<C: Send + 'static> ProtectedBytes<C> {
    /// Transfer the same allocation to a transport. Clones and slices of
    /// the returned bytes retain this owner until the last reference retires.
    pub fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from_owner(self)
    }
}

impl<C> AsRef<[u8]> for ProtectedBytes<C> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl<C> ProtectedBytes<C> {
    /// Original backing capacity, for transferring its existing allocation credit.
    /// This does not admit capacity or change payload ownership.
    pub fn capacity(&self) -> usize {
        self.bytes.capacity()
    }
}

struct JsonWriter(Zeroizing<Vec<u8>>);

impl std::io::Write for JsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .0
            .len()
            .checked_add(bytes.len())
            .ok_or(std::io::ErrorKind::OutOfMemory)?;
        if next > self.0.capacity() {
            let capacity = next.max(self.0.capacity().saturating_mul(2));
            reserve_protected_bytes(&mut self.0, capacity).map_err(std::io::Error::other)?;
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

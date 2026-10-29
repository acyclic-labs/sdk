use super::OwnedBytes;

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static HASHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Bytes this thread has hashed into content digests.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn hashed_on_this_thread() -> u64 {
    HASHED.with(std::cell::Cell::get)
}

/// The BLAKE3-256 content digest of `bytes`: the one hash every content
/// address in the SDK uses.
#[must_use]
pub fn content_digest(bytes: &[u8]) -> [u8; 32] {
    #[cfg(any(test, feature = "test-support"))]
    HASHED.with(|hashed| hashed.set(hashed.get() + bytes.len() as u64));
    *blake3::hash(bytes).as_bytes()
}

/// Immutable bytes and the content digest one hash of them proved.
///
/// The only constructor hashes, so a seal never carries a digest its bytes
/// do not have; every layer that receives one reuses the digest instead of
/// hashing again. Bytes from outside the process are still verified where
/// they arrive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedBytes {
    bytes: OwnedBytes,
    digest: [u8; 32],
}

impl SealedBytes {
    /// Hashes `bytes` once.
    #[must_use]
    pub fn new(bytes: OwnedBytes) -> Self {
        Self {
            digest: content_digest(&bytes),
            bytes,
        }
    }

    /// The content digest of the bytes.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// The sealed bytes.
    #[must_use]
    pub const fn bytes(&self) -> &OwnedBytes {
        &self.bytes
    }

    /// The sealed bytes, giving up the seal.
    #[must_use]
    pub fn into_bytes(self) -> OwnedBytes {
        self.bytes
    }
}

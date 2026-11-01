//! Protected operation payloads transferred without copying their backing.

use zeroize::Zeroizing;

/// An owned operation payload, scrubbed when its final transport owner retires.
///
/// Moving this value transfers the original allocation. It supplies neither
/// authority nor allocation credit; the issuing operation retains those owners.
/// Transport libraries may make their own internal copies; this owner only
/// guarantees scrubbing of the allocation it receives.
pub struct ProtectedBytes(Zeroizing<Vec<u8>>);

impl ProtectedBytes {
    /// Takes the original protected allocation without copying its bytes.
    pub fn new(bytes: Zeroizing<Vec<u8>>) -> Self {
        Self(bytes)
    }

    /// Transfer the same allocation to a transport. Clones and slices of
    /// the returned bytes retain this owner until the last reference retires.
    pub fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for ProtectedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

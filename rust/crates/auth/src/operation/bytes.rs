//! Protected operation payloads transferred without copying their backing.

use zeroize::Zeroizing;

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

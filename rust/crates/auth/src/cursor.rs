//! Stateless confidential continuations, bound to their caller and query.
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};

/// A process-owned cursor key. Restarting its owner invalidates old cursors;
/// callers restart the query rather than retaining server-side page state.
pub struct CursorKey(XChaCha20Poly1305);

impl CursorKey {
    /// Generate an independent key without persisting it in user data.
    ///
    /// # Errors
    /// Returns an error if the OS randomness source is unavailable.
    pub fn new() -> Result<Self, crate::Error> {
        let mut key = zeroize::Zeroizing::new([0; 32]);
        getrandom::fill(&mut *key).map_err(|_| crate::Error::Invalid)?;
        Ok(Self(XChaCha20Poly1305::new((&*key).into())))
    }

    /// Number of ciphertext bytes for a plaintext of this length.
    pub const fn sealed_len(plaintext: usize) -> usize {
        plaintext + 1 + 24 + 16
    }

    /// Encrypt a continuation. `context` identifies the caller and exact query;
    /// it must be identical on resume. The nonce is independently random.
    ///
    /// # Errors
    /// Returns an error if nonce generation or encryption fails.
    pub fn seal(&self, context: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, crate::Error> {
        let mut nonce = [0; 24];
        getrandom::fill(&mut nonce).map_err(|_| crate::Error::Invalid)?;
        let ciphertext = self
            .0
            .encrypt(
                &XNonce::from(nonce),
                Payload {
                    msg: plaintext,
                    aad: context,
                },
            )
            .map_err(|_| crate::Error::Invalid)?;
        let mut sealed = Vec::with_capacity(Self::sealed_len(plaintext.len()));
        sealed.push(1);
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&ciphertext);
        Ok(sealed)
    }

    /// Authenticate and decrypt a v1 continuation, refusing another query,
    /// another owner, malformed ciphertext or a changed version.
    ///
    /// # Errors
    /// Returns an error when the token or its query binding is invalid.
    pub fn open(&self, context: &[u8], sealed: &[u8]) -> Result<Vec<u8>, crate::Error> {
        if sealed.len() < Self::sealed_len(0) || sealed.first() != Some(&1) {
            return Err(crate::Error::Invalid);
        }
        let mut nonce = [0; 24];
        nonce.copy_from_slice(sealed.get(1..25).ok_or(crate::Error::Invalid)?);
        self.0
            .decrypt(
                &XNonce::from(nonce),
                Payload {
                    msg: sealed.get(25..).ok_or(crate::Error::Invalid)?,
                    aad: context,
                },
            )
            .map_err(|_| crate::Error::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_cursors_reject_changed_queries_owners_and_bytes() -> Result<(), crate::Error> {
        let key = CursorKey::new()?;
        let cursor = key.seal(b"caller/query", b"private continuation")?;
        assert_eq!(key.open(b"caller/query", &cursor)?, b"private continuation");
        assert!(key.open(b"other/query", &cursor).is_err());
        assert!(CursorKey::new()?.open(b"caller/query", &cursor).is_err());
        for index in 0..cursor.len() {
            let mut changed = cursor.clone();
            changed[index] ^= 1;
            assert!(key.open(b"caller/query", &changed).is_err());
            assert!(key.open(b"caller/query", &cursor[..index]).is_err());
        }
        Ok(())
    }
}

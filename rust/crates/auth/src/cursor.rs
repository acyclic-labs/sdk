//! Stateless confidential continuations, bound to their caller and query.
use chacha20poly1305::{
    Tag, XChaCha20Poly1305, XNonce,
    aead::{AeadInOut, KeyInit},
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
        let mut sealed = vec![0; Self::sealed_len(plaintext.len())];
        self.seal_into(context, plaintext, &mut sealed)?;
        Ok(sealed)
    }

    /// Encrypt into exactly [`Self::sealed_len`] caller-owned bytes, without
    /// allocating. The caller retains its allocation owner through publication.
    ///
    /// # Errors
    /// Refuses a differently sized output, unavailable randomness or encryption failure.
    pub fn seal_into(
        &self,
        context: &[u8],
        plaintext: &[u8],
        sealed: &mut [u8],
    ) -> Result<(), crate::Error> {
        if sealed.len().checked_sub(Self::sealed_len(0)) != Some(plaintext.len()) {
            return Err(crate::Error::Invalid);
        }
        let mut nonce = [0; 24];
        getrandom::fill(&mut nonce).map_err(|_| crate::Error::Invalid)?;
        let (header, payload) = sealed.split_at_mut(25);
        let (body, tag) = payload.split_at_mut(plaintext.len());
        body.copy_from_slice(plaintext);
        let authenticated = self
            .0
            .encrypt_inout_detached(&XNonce::from(nonce), context, body.into())
            .map_err(|_| crate::Error::Invalid)?;
        header[0] = 1;
        header[1..].copy_from_slice(&nonce);
        tag.copy_from_slice(&authenticated);
        Ok(())
    }

    /// Authenticate and decrypt a v1 continuation, refusing another query,
    /// another owner, malformed ciphertext or a changed version.
    ///
    /// # Errors
    /// Returns an error when the token or its query binding is invalid.
    pub fn open(&self, context: &[u8], sealed: &[u8]) -> Result<Vec<u8>, crate::Error> {
        if sealed.first() != Some(&1) {
            return Err(crate::Error::Invalid);
        }
        let length = sealed
            .len()
            .checked_sub(Self::sealed_len(0))
            .ok_or(crate::Error::Invalid)?;
        let mut plaintext = vec![0; length];
        self.open_into(context, sealed, &mut plaintext)?;
        Ok(plaintext)
    }

    /// Authenticate into exactly `sealed.len() - sealed_len(0)` caller-owned
    /// bytes, without allocating. Output is usable only after success.
    ///
    /// # Errors
    /// Refuses another query, owner, version, malformed token or output length.
    pub fn open_into(
        &self,
        context: &[u8],
        sealed: &[u8],
        plaintext: &mut [u8],
    ) -> Result<(), crate::Error> {
        if sealed.len().checked_sub(Self::sealed_len(0)) != Some(plaintext.len()) || sealed[0] != 1
        {
            return Err(crate::Error::Invalid);
        }
        let mut nonce = [0; 24];
        nonce.copy_from_slice(&sealed[1..25]);
        let (body, tag) = sealed[25..].split_at(plaintext.len());
        let tag = Tag::try_from(tag).map_err(|_| crate::Error::Invalid)?;
        plaintext.copy_from_slice(body);
        self.0
            .decrypt_inout_detached(&XNonce::from(nonce), context, plaintext.into(), &tag)
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
        let mut output = [0; b"private continuation".len()];
        key.open_into(b"caller/query", &cursor, &mut output)?;
        assert_eq!(&output, b"private continuation");
        let mut sealed = [0; CursorKey::sealed_len(b"private continuation".len())];
        key.seal_into(b"caller/query", &output, &mut sealed)?;
        assert_eq!(key.open(b"caller/query", &sealed)?, output);
        assert!(key.seal_into(b"caller/query", &output, &mut []).is_err());
        assert!(key.open_into(b"caller/query", &cursor, &mut []).is_err());
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

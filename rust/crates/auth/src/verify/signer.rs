//! Root minting for runtime custodians. Workloads receive only delegated tips.
use super::*;

/// Non-cloneable certified account leaf signer, retained only by the runtime.
/// Minted capabilities contain public certificates and a distinct append tip;
/// they never retain this key or expose account minting authority.
pub struct CertifiedSigner {
    certificates: CertificateChain,
    key: SigningKey,
}

impl CertifiedSigner {
    /// Takes custody of a certified leaf key. Trust is checked at every mint.
    ///
    /// # Errors
    /// Fails when the final certificate does not name the supplied key.
    pub fn new(certificates: CertificateChain, key: SigningKey) -> Result<Self, VerifyError> {
        let leaf = certificates.keys.last().ok_or(VerifyError::Malformed)?;
        if leaf.key.public_key.as_slice() != key.verifying_key().as_bytes() {
            return Err(VerifyError::Signature);
        }
        Ok(Self { certificates, key })
    }

    /// Mints block zero within the current authenticated certificate ceilings.
    /// The caller supplies a fresh, distinct workload tip. All signatures,
    /// certificate ancestry, revocations, time bounds and caveats are checked
    /// before append authority is returned.
    ///
    /// # Errors
    /// Returns the same fail-closed errors as capability verification.
    pub fn mint(
        &self,
        caveats: Vec<Caveat>,
        next: SigningKey,
        trust: &(impl Trust + ?Sized),
        now_unix_seconds: u64,
    ) -> Result<VerifiedCapability, VerifyError> {
        let block = Block {
            certificates: Some(self.certificates.clone()),
            caveats,
            next_public_key: next.verifying_key().to_bytes().to_vec(),
        };
        let payload = BlockProto::from(block).encode_to_vec();
        let signature = self.key.sign(&block_transcript(0, &[0; 32], &payload));
        let signed = SignedBlockProto {
            payload,
            signature: signature.to_bytes().to_vec(),
        };
        let secret = Zeroizing::new(next.to_bytes());
        drop(next);
        let encoded = wire::encode(&[signed], 18, secret.as_ref(), VERSION);
        verify(&encoded, trust, now_unix_seconds)
    }
}

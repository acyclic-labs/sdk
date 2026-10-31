//! Authenticated immutable chains. Trust and revocation are supplied by the owner.
use crate::{BIRTH_DOMAIN, KEY_DOMAIN, SEAL_DOMAIN, VERSION, authority, contract::*, crypto::*};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;
use std::collections::HashSet;
use zeroize::Zeroizing;

mod certificates;
mod current;
mod effective;
pub use certificates::CertificateVerifier;
mod signatures;
mod signer;
mod wire;
pub use current::{TrustEvidence, TrustEvidenceStorage};
pub use effective::{Effective, ScopedFilter, ScopedPin};
pub use signer::CertifiedSigner;

/// Authentication, structural admission or authority failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    /// Canonical wire or semantic admission failed.
    #[error("malformed capability")]
    Malformed,
    /// Version must be checked before interpreting any authority.
    #[error("unsupported capability version {0}")]
    Version(u32),
    /// Includes invalid/weak public keys, tampering and invalid tip proofs.
    #[error("invalid capability signature or proof")]
    Signature,
    /// The trust owner could not provide authenticated current state.
    #[error("capability trust state unavailable")]
    Trust,
    /// A dependency of the verifier (a keyring, KMS or database) could not
    /// answer. Retryable, and never a verdict on the credential.
    #[error("credential verifier unavailable")]
    Unavailable,
    /// An ancestor key, epoch or block has been revoked.
    #[error("capability revoked")]
    Revoked,
    /// The current time lies outside the effective half-open interval.
    #[error("capability is not currently valid")]
    Time,
    /// A tip key would expose an ancestor's signing authority.
    #[error("capability reuses an ancestor signing key")]
    RepeatedKey,
    /// A sealed token has no append authority.
    #[error("capability is sealed")]
    Sealed,
    /// The owner policy or capability intersection denied the request.
    #[error(transparent)]
    Denied(#[from] authority::Deny),
}
impl From<crate::Error> for VerifyError {
    fn from(error: crate::Error) -> Self {
        match error {
            crate::Error::UnsupportedVersion(version) => Self::Version(version),
            _ => Self::Malformed,
        }
    }
}

/// Authenticated account keyring state, including still-trusted historical issuers.
pub struct AccountState<'a> {
    /// Authorized account issuer roots; never workload or online leaf keys.
    pub issuer_keys: &'a [BlockId],
    /// Minimum epoch accepted for every online ancestor certificate.
    pub minimum_key_epoch: u64,
    /// Earliest verification time supported by retained revocation evidence.
    /// A checkpoint that retires authority must advance this floor atomically.
    /// Historical authentication before it refuses, even with an older token.
    pub not_before_unix_seconds: u64,
}

/// Every relevant ancestor must be checked against current revocation state.
#[derive(Clone, Copy, Debug)]
pub enum RevocationCheck<'a> {
    /// An online certified ancestor.
    Key {
        /// Account-scoped key identity.
        id: &'a str,
        /// Issuance epoch.
        epoch: u64,
    },
    /// A block in this chain or an online certificate's parent link.
    Block(&'a BlockId),
}

/// Read-only authenticated trust snapshot. Implementations must fail closed on
/// missing/stale state; metering and network refresh belong outside these hooks.
pub trait Trust {
    /// Still-trusted environment issuers, including historical Birth signers.
    fn environment_keys(&self, environment: &str) -> Result<&[BlockId], VerifyError>;
    /// Authenticated current account keyring, anchored in environment trust.
    fn account(&self, account: &str) -> Result<AccountState<'_>, VerifyError>;
    /// Check key/block revocations; errors are denials, never soft failures.
    fn revoked(&self, account: &str, check: RevocationCheck<'_>) -> Result<bool, VerifyError>;
}

enum Proof {
    Append(SigningKey),
    Sealed([u8; 64]),
}

/// Immutable authenticated chain. No account or issuer signing key is retained.
/// Only its tip can sign an append; neither fields nor a mutable wire view are exposed.
pub struct VerifiedCapability {
    blocks: Vec<Block>,
    signed: Vec<SignedBlockProto>,
    ids: Vec<BlockId>,
    issuers: Vec<BlockId>,
    environment_issuer: BlockId,
    signing_keys: HashSet<BlockId>,
    proof: Proof,
    principal: Principal,
    limits: Limits,
    not_before: u64,
    expires_at: u64,
}

fn decode_blocks(envelope: &wire::Envelope) -> Result<Vec<BlockProto>, VerifyError> {
    if envelope.version != VERSION {
        return Err(VerifyError::Version(envelope.version));
    }
    if envelope.blocks.is_empty() {
        return Err(VerifyError::Malformed);
    }
    envelope
        .blocks
        .iter()
        .map(|signed| {
            BlockProto::decode(signed.payload.as_slice()).map_err(|_| VerifyError::Malformed)
        })
        .collect()
}

fn admit_blocks(wire: Vec<BlockProto>) -> Result<Vec<Block>, VerifyError> {
    let blocks = wire
        .into_iter()
        .map(Block::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    if !authority::valid_chain(&blocks) {
        return Err(VerifyError::Malformed);
    }
    Ok(blocks)
}

fn verify_proof(
    proof: wire::Proof,
    signer: ed25519_dalek::VerifyingKey,
    previous: &BlockId,
    message: &mut Vec<u8>,
) -> Result<Proof, VerifyError> {
    match proof {
        wire::Proof::Secret(secret) => {
            let seed: &[u8; 32] = secret
                .as_slice()
                .try_into()
                .map_err(|_| VerifyError::Malformed)?;
            let key = SigningKey::from_bytes(seed);
            if !authentication_eq(key.verifying_key().as_bytes(), signer.as_bytes()) {
                return Err(VerifyError::Signature);
            }
            Ok(Proof::Append(key))
        }
        wire::Proof::Sealed(signature) => {
            message.clear();
            message.extend_from_slice(SEAL_DOMAIN);
            message.extend_from_slice(previous);
            verify_signature(&signer, message, &signature)?;
            Ok(Proof::Sealed(signature))
        }
    }
}

/// Verify signatures before semantic certificate subset admission. The returned
/// authority must still be checked against current trust/time at each admission.
pub fn verify(
    bytes: &[u8],
    trust: &(impl Trust + ?Sized),
    now_unix_seconds: u64,
) -> Result<VerifiedCapability, VerifyError> {
    CertificateVerifier::default().verify(bytes, trust, now_unix_seconds)
}

fn verify_certified(
    bytes: &[u8],
    trust: &(impl Trust + ?Sized),
    now_unix_seconds: u64,
    certification: &mut CertificateVerifier,
) -> Result<VerifiedCapability, VerifyError> {
    let envelope = wire::decode(bytes)?;
    let wire = decode_blocks(&envelope)?;
    let certificates = wire
        .first()
        .ok_or(VerifyError::Malformed)?
        .certificates
        .as_ref()
        .ok_or(VerifyError::Malformed)?;
    let roots = certification.authenticate(certificates, trust, now_unix_seconds)?;
    let environment = roots.environment;
    let issuers = roots.issuers.clone();
    let mut seen = certification.signing_keys(certificates, trust)?;
    let mut message = Vec::new();
    let leaf = certificates
        .keys
        .last()
        .and_then(|cert| cert.key.as_ref())
        .ok_or(VerifyError::Malformed)?;
    let mut signer = public_key(&leaf.public_key)?;
    let mut previous = [0; 32];
    let mut ids = Vec::with_capacity(wire.len());
    // Each id commits to its signing transcript, signature, and predecessor.
    // Block zero includes the exact certificates and their leaf key. Matching
    // an authenticated endpoint therefore proves every signed ancestor, while
    // retaining neither input-sized storage nor a workload's append secret.
    for (index, signed) in envelope.blocks.iter().enumerate() {
        block_message(&mut message, index as u64, &previous, &signed.payload);
        previous = block_id(&message, &signed.signature);
        ids.push(previous);
    }
    let authenticated_through = certification.workloads.authenticated_through(&ids);
    previous = [0; 32];
    for (index, (block, signed)) in wire.iter().zip(&envelope.blocks).enumerate() {
        if index >= authenticated_through {
            block_message(&mut message, index as u64, &previous, &signed.payload);
            verify_signature(&signer, &message, &signed.signature)?;
        }
        previous = *ids.get(index).ok_or(VerifyError::Malformed)?;
        signer = public_key(&block.next_public_key)?;
        if !seen.insert(signer.to_bytes()) {
            return Err(VerifyError::RepeatedKey);
        }
        if block.encode_to_vec() != signed.payload {
            return Err(VerifyError::Malformed);
        }
    }
    let proof = verify_proof(envelope.proof, signer, &previous, &mut message)?;
    // No expensive claims/subset conversion happens until every signature is valid.
    let blocks = admit_blocks(wire)?;
    let mut verified = VerifiedCapability {
        blocks,
        signed: envelope.blocks,
        ids,
        issuers,
        environment_issuer: environment,
        signing_keys: seen,
        proof,
        principal: Principal {
            kind: PrincipalKind::Anonymous,
            id: String::new(),
            lineage: vec![],
            groups: vec![],
            roles: vec![],
            attributes: vec![],
        },
        limits: effective::unbounded_limits(),
        not_before: 0,
        expires_at: u64::MAX,
    };
    verified.evaluate();
    verified.check(trust, now_unix_seconds)?;
    certification.workloads.retain(&verified.ids);
    Ok(verified)
}

impl VerifiedCapability {
    /// Whether this chain has irreversibly discarded its append authority.
    pub fn is_sealed(&self) -> bool {
        matches!(self.proof, Proof::Sealed(_))
    }

    /// Account authenticated by the environment birth certificate.
    pub fn account_id(&self) -> Result<&str, VerifyError> {
        self.blocks
            .first()
            .and_then(|block| block.certificates.as_ref())
            .map(|certificates| certificates.birth.birth.account_id.as_str())
            .ok_or(VerifyError::Malformed)
    }

    /// Borrow effective principal, conjunctive grant/filter sets and numeric minima.
    pub fn effective(&self) -> Effective<'_> {
        Effective { capability: self }
    }
    /// Oldest-to-tip revocation identities; revoking any entry invalidates descendants.
    pub fn block_ids(&self) -> &[BlockId] {
        &self.ids
    }

    /// Every chain and certificate ancestor whose revocation invalidates this workload.
    pub fn revocation_ids(&self) -> impl Iterator<Item = &[u8]> {
        self.ids.iter().map(|id| id.as_slice()).chain(
            self.blocks
                .first()
                .into_iter()
                .flat_map(|block| block.certificates.iter())
                .flat_map(|certificates| &certificates.keys)
                .flat_map(|certificate| &certificate.key.parent_revocation_ids)
                .map(Vec::as_slice),
        )
    }

    /// Recheck the current trust snapshot and clock without repeating signatures.
    pub fn check(
        &self,
        trust: &(impl Trust + ?Sized),
        now_unix_seconds: u64,
    ) -> Result<(), VerifyError> {
        current::check(self, trust, now_unix_seconds)
    }

    /// Authorize the explicit scope against fresh revocations/time and the
    /// owner's policy result. Immutable structural admission is reused; every
    /// request fact and owner decision is evaluated again. Deferred scopes
    /// retain mandatory provider checks.
    pub fn authorize(
        &self,
        trust: &(impl Trust + ?Sized),
        request: &authority::Request<'_>,
        policy: authority::PolicyDecision,
        scope: authority::Scope,
    ) -> Result<(), VerifyError> {
        self.check(trust, request.now_unix_seconds)?;
        authority::permits_current(&self.blocks, request, policy, scope)
            .map_err(VerifyError::Denied)
    }

    /// Append a conjunctive block. The caller supplies fresh cryptographic entropy;
    /// reusing any ancestor key is rejected, including the account's online leaf.
    pub fn attenuate(
        &self,
        caveats: Vec<Caveat>,
        next_secret: &[u8; 32],
    ) -> Result<Self, VerifyError> {
        let Proof::Append(signer) = &self.proof else {
            return Err(VerifyError::Sealed);
        };
        let next = SigningKey::from_bytes(next_secret);
        let public = next.verifying_key().to_bytes();
        if self.signing_keys.contains(&public) {
            return Err(VerifyError::RepeatedKey);
        }
        let blocks = authority::attenuate(&self.blocks, caveats, public.to_vec())?;
        let block = blocks.last().ok_or(VerifyError::Malformed)?;
        let payload = BlockProto::from(block.clone()).encode_to_vec();
        let previous = self.ids.last().ok_or(VerifyError::Malformed)?;
        let message = block_transcript(self.blocks.len() as u64, previous, &payload);
        let signature = signer.sign(&message).to_bytes();
        let mut signed = self.signed.clone();
        signed.push(SignedBlockProto {
            payload,
            signature: signature.to_vec(),
        });
        let mut ids = self.ids.clone();
        ids.push(block_id(&message, &signature));
        let mut signing_keys = self.signing_keys.clone();
        signing_keys.insert(public);
        let mut child = Self {
            blocks,
            signed,
            ids,
            issuers: self.issuers.clone(),
            environment_issuer: self.environment_issuer,
            signing_keys,
            proof: Proof::Append(next),
            principal: self.principal.clone(),
            limits: self.limits.clone(),
            not_before: self.not_before,
            expires_at: self.expires_at,
        };
        child.evaluate();
        Ok(child)
    }

    /// Consume append authority and replace it with a final tip signature.
    pub fn seal(mut self) -> Result<Self, VerifyError> {
        if self.is_sealed() {
            return Err(VerifyError::Sealed);
        }
        self.proof = Proof::Sealed(self.seal_signature()?);
        Ok(self)
    }

    fn seal_signature(&self) -> Result<[u8; 64], VerifyError> {
        match &self.proof {
            Proof::Sealed(signature) => Ok(*signature),
            Proof::Append(signer) => {
                let mut message = [0; SEAL_DOMAIN.len() + 32];
                message[..SEAL_DOMAIN.len()].copy_from_slice(SEAL_DOMAIN);
                message[SEAL_DOMAIN.len()..]
                    .copy_from_slice(self.ids.last().ok_or(VerifyError::Malformed)?);
                Ok(signer.sign(&message).to_bytes())
            }
        }
    }

    /// Export public verification evidence without exporting append authority.
    /// The custodian retains its original immutable capability; the recipient
    /// receives the same authenticated blocks with only a final tip signature.
    /// This does not attest a new admission or authorize a different request.
    pub fn encode_sealed(&self) -> Result<Zeroizing<Vec<u8>>, VerifyError> {
        Ok(wire::encode(
            &self.signed,
            26,
            &self.seal_signature()?,
            VERSION,
        ))
    }

    /// Exact encoded length of the public seal, before allocating its output.
    pub fn sealed_encoded_len(&self) -> usize {
        wire::encoded_len(&self.signed, 64, VERSION)
    }

    /// Exact encoded length, without allocating or exposing the append secret.
    pub fn encoded_len(&self) -> usize {
        let proof_len = match &self.proof {
            Proof::Append(_) => 32,
            Proof::Sealed(_) => 64,
        };
        wire::encoded_len(&self.signed, proof_len, VERSION)
    }

    /// Encodes this capability, zeroizing the returned append secret on drop.
    pub fn encode(&self) -> Zeroizing<Vec<u8>> {
        match &self.proof {
            Proof::Append(key) => {
                let secret = Zeroizing::new(key.to_bytes());
                wire::encode(&self.signed, 18, secret.as_slice(), VERSION)
            }
            Proof::Sealed(signature) => wire::encode(&self.signed, 26, signature, VERSION),
        }
    }
}

#[cfg(test)]
mod authorization_tests;

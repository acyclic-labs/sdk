//! Authenticated immutable chains. Trust and revocation are supplied by the owner.
use crate::{BIRTH_DOMAIN, KEY_DOMAIN, SEAL_DOMAIN, VERSION, authority, contract::*, crypto::*};
use ed25519_dalek::{Signer, SigningKey};
use prost::Message;
use std::collections::HashSet;
use zeroize::Zeroizing;

mod effective;
mod signer;
mod wire;
pub use effective::{Effective, ScopedFilter};
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

/// Verify signatures before semantic certificate subset admission. The returned
/// authority must still be checked against current trust/time at each admission.
pub fn verify(
    bytes: &[u8],
    trust: &(impl Trust + ?Sized),
    now_unix_seconds: u64,
) -> Result<VerifiedCapability, VerifyError> {
    let envelope = wire::decode(bytes)?;
    if envelope.version != VERSION {
        return Err(VerifyError::Version(envelope.version));
    }
    if envelope.blocks.is_empty() {
        return Err(VerifyError::Malformed);
    }
    let wire = envelope
        .blocks
        .iter()
        .map(|signed| {
            BlockProto::decode(signed.payload.as_slice()).map_err(|_| VerifyError::Malformed)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let certificates = wire[0]
        .certificates
        .as_ref()
        .ok_or(VerifyError::Malformed)?;
    let birth = certificates.birth.as_ref().ok_or(VerifyError::Malformed)?;
    let claims = birth.birth.as_ref().ok_or(VerifyError::Malformed)?;
    let environments = trust.environment_keys(&claims.environment)?;
    let mut message = Vec::new();
    claims_message(&mut message, BIRTH_DOMAIN, claims)?;
    let environment = environments
        .iter()
        .find(|issuer| {
            public_key(*issuer)
                .is_ok_and(|key| verify_signature(&key, &message, &birth.signature).is_ok())
        })
        .copied()
        .ok_or(VerifyError::Signature)?;
    if claims.issued_at_unix_seconds > now_unix_seconds {
        return Err(VerifyError::Time);
    }
    let account = trust.account(&claims.account_id)?;
    let mut issuers = Vec::with_capacity(certificates.keys.len());
    let mut seen = HashSet::new();
    for issuer in environments {
        seen.insert(public_key(issuer)?.to_bytes());
    }
    seen.insert(public_key(&claims.genesis_public_key)?.to_bytes());
    for issuer in account.issuer_keys {
        seen.insert(public_key(issuer)?.to_bytes());
    }
    for certificate in &certificates.keys {
        let key = certificate.key.as_ref().ok_or(VerifyError::Malformed)?;
        claims_message(&mut message, KEY_DOMAIN, key)?;
        let issuer = account
            .issuer_keys
            .iter()
            .find(|issuer| {
                public_key(*issuer).is_ok_and(|key| {
                    verify_signature(&key, &message, &certificate.signature).is_ok()
                })
            })
            .ok_or(VerifyError::Signature)?;
        issuers.push(*issuer);
        seen.insert(public_key(&key.public_key)?.to_bytes());
    }
    let leaf = certificates
        .keys
        .last()
        .and_then(|cert| cert.key.as_ref())
        .ok_or(VerifyError::Malformed)?;
    let mut signer = public_key(&leaf.public_key)?;
    let mut previous = [0; 32];
    let mut ids = Vec::with_capacity(wire.len());
    for (index, (block, signed)) in wire.iter().zip(&envelope.blocks).enumerate() {
        block_message(&mut message, index as u64, &previous, &signed.payload);
        verify_signature(&signer, &message, &signed.signature)?;
        previous = block_id(&message, &signed.signature);
        ids.push(previous);
        signer = public_key(&block.next_public_key)?;
        if !seen.insert(signer.to_bytes()) {
            return Err(VerifyError::RepeatedKey);
        }
        if block.encode_to_vec() != signed.payload {
            return Err(VerifyError::Malformed);
        }
    }
    let proof = match envelope.proof {
        wire::Proof::Secret(secret) => {
            let seed: &[u8; 32] = secret
                .as_slice()
                .try_into()
                .map_err(|_| VerifyError::Malformed)?;
            let key = SigningKey::from_bytes(seed);
            if key.verifying_key() != signer {
                return Err(VerifyError::Signature);
            }
            Proof::Append(key)
        }
        wire::Proof::Sealed(signature) => {
            message.clear();
            message.extend_from_slice(SEAL_DOMAIN);
            message.extend_from_slice(&previous);
            verify_signature(&signer, &message, &signature)?;
            Proof::Sealed(signature)
        }
    };
    // No expensive claims/subset conversion happens until every signature is valid.
    let blocks = wire
        .into_iter()
        .map(Block::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    if !authority::valid_chain(&blocks) {
        return Err(VerifyError::Malformed);
    }
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
    Ok(verified)
}

impl VerifiedCapability {
    /// Borrow effective principal, conjunctive grant/filter sets and numeric minima.
    pub fn effective(&self) -> Effective<'_> {
        Effective { capability: self }
    }
    /// Oldest-to-tip revocation identities; revoking any entry invalidates descendants.
    pub fn block_ids(&self) -> &[BlockId] {
        &self.ids
    }

    /// Recheck the current trust snapshot and clock without repeating signatures.
    pub fn check(
        &self,
        trust: &(impl Trust + ?Sized),
        now_unix_seconds: u64,
    ) -> Result<(), VerifyError> {
        if now_unix_seconds < self.not_before || now_unix_seconds >= self.expires_at {
            return Err(VerifyError::Time);
        }
        let certificates = self.blocks[0]
            .certificates
            .as_ref()
            .ok_or(VerifyError::Malformed)?;
        let id = &certificates.birth.birth.account_id;
        let environments = trust.environment_keys(&certificates.birth.birth.environment)?;
        if !environments.contains(&self.environment_issuer) {
            return Err(VerifyError::Trust);
        }
        let account = trust.account(id)?;
        if self.blocks.iter().any(|block| {
            environments
                .iter()
                .chain(account.issuer_keys)
                .any(|key| block.next_public_key.as_slice() == key)
        }) {
            return Err(VerifyError::RepeatedKey);
        }
        if !self
            .issuers
            .iter()
            .all(|issuer| account.issuer_keys.contains(issuer))
        {
            return Err(VerifyError::Trust);
        }
        for cert in &certificates.keys {
            if cert.key.key_epoch < account.minimum_key_epoch
                || trust.revoked(
                    id,
                    RevocationCheck::Key {
                        id: &cert.key.key_id,
                        epoch: cert.key.key_epoch,
                    },
                )?
            {
                return Err(VerifyError::Revoked);
            }
            for parent in &cert.key.parent_revocation_ids {
                let parent = parent
                    .as_slice()
                    .try_into()
                    .map_err(|_| VerifyError::Malformed)?;
                if trust.revoked(id, RevocationCheck::Block(parent))? {
                    return Err(VerifyError::Revoked);
                }
            }
        }
        for block in &self.ids {
            if trust.revoked(id, RevocationCheck::Block(block))? {
                return Err(VerifyError::Revoked);
            }
        }
        Ok(())
    }

    /// Authorize against fresh revocations/time and the owner's policy result.
    pub fn authorize(
        &self,
        trust: &(impl Trust + ?Sized),
        request: &authority::Request<'_>,
        policy: authority::PolicyDecision,
    ) -> Result<(), VerifyError> {
        self.check(trust, request.now_unix_seconds)?;
        authority::permits(&self.blocks, request, policy).map_err(VerifyError::Denied)
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
        let Proof::Append(signer) = std::mem::replace(&mut self.proof, Proof::Sealed([0; 64]))
        else {
            return Err(VerifyError::Sealed);
        };
        let mut message = SEAL_DOMAIN.to_vec();
        message.extend_from_slice(self.ids.last().ok_or(VerifyError::Malformed)?);
        self.proof = Proof::Sealed(signer.sign(&message).to_bytes());
        Ok(self)
    }

    /// Sensitive transport bytes. The returned buffer is scrubbed when dropped.
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

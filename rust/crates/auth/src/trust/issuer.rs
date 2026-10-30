//! A locally run Trust: the same records and certificates the hosted Trust
//! publishes, so local, self-hosted and hosted verifiers run one follower.
use crate::{
    BIRTH_DOMAIN, KEY_DOMAIN, KEYRING_DOMAIN,
    contract::*,
    crypto::{BlockId, block_id, claims_message},
    revocation,
    verify::VerifyError,
};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use prost::Message;

/// Environment custody: signs account Births and account keyrings.
pub struct Issuer {
    environment: String,
    key: SigningKey,
}

/// One account's root custody and the frontiers of its two record chains.
pub struct AccountRoot {
    id: String,
    root: SigningKey,
    keyring: (u64, BlockId),
    revocations: (u64, BlockId),
    minimum_key_epoch: u64,
}

/// What Trust certifies for one runtime signing key.
pub struct Certification<'a> {
    /// Account-unique key identity; never reused.
    pub key_id: &'a str,
    /// The runtime's leaf public key.
    pub public_key: &'a VerifyingKey,
    /// Validity, at most 30 days.
    pub validity: Validity,
    /// The runtime's grant ceiling.
    pub grants: GrantSet,
    /// The runtime's attested identity.
    pub principal: Principal,
}

fn record(kind: TrustRecordKind) -> Vec<u8> {
    TrustRecordProto::from(TrustRecord { record: kind }).encode_to_vec()
}

fn sign(
    key: &SigningKey,
    domain: &[u8],
    claims: &impl Message,
) -> Result<(Vec<u8>, BlockId), VerifyError> {
    let mut message = Vec::new();
    claims_message(&mut message, domain, claims)?;
    let signature = key.sign(&message).to_bytes().to_vec();
    let digest = block_id(&message, &signature);
    Ok((signature, digest))
}

impl Issuer {
    /// Environment custody. `key` is also the follower's pinned anchor.
    #[must_use]
    pub fn new(environment: impl Into<String>, key: SigningKey) -> Self {
        Self {
            environment: environment.into(),
            key,
        }
    }

    /// The environment name.
    #[must_use]
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// The pinned anchor a [`super::Follower`] of this issuer starts from.
    #[must_use]
    pub fn anchor(&self) -> Vec<BlockId> {
        vec![self.key.verifying_key().to_bytes()]
    }

    /// Creates `id` under `root`: the account's first keyring and its first
    /// revocation checkpoint, both at `now`, in Stream order.
    ///
    /// # Errors
    /// When a record cannot be encoded.
    pub fn genesis(
        &self,
        id: impl Into<String>,
        root: SigningKey,
        now: u64,
    ) -> Result<(AccountRoot, Vec<Vec<u8>>), VerifyError> {
        let mut account = AccountRoot {
            id: id.into(),
            root,
            keyring: (0, [0; 32]),
            revocations: (0, [0; 32]),
            minimum_key_epoch: 1,
        };
        let keyring = self.keyring(&mut account, now)?;
        let checkpoint = RevocationPublication::Checkpoint(RevocationCheckpoint {
            account_id: account.id.clone(),
            sequence: 1,
            retired_before_unix_seconds: 0,
            minimum_key_epoch: 0,
            keys: vec![],
            blocks: vec![],
            issued_at_unix_seconds: now,
        });
        let checkpoint = account.publish(checkpoint);
        Ok((account, vec![keyring, checkpoint]))
    }

    /// The next keyring of `account`, attesting its current root and epoch.
    ///
    /// # Errors
    /// When the record cannot be encoded.
    pub fn keyring(&self, account: &mut AccountRoot, now: u64) -> Result<Vec<u8>, VerifyError> {
        let snapshot = KeyringSnapshot {
            account_id: account.id.clone(),
            sequence: account.keyring.0 + 1,
            previous_digest: account.keyring.1.to_vec(),
            minimum_key_epoch: account.minimum_key_epoch,
            revoked_key_ids: vec![],
            revocations: vec![],
            root_public_key: account.root.verifying_key().to_bytes().to_vec(),
            issued_at_unix_seconds: now,
        };
        let (signature, digest) = sign(
            &self.key,
            KEYRING_DOMAIN,
            &KeyringSnapshotProto::from(snapshot.clone()),
        )?;
        account.keyring = (snapshot.sequence, digest);
        Ok(record(TrustRecordKind::Keyring(SignedKeyringSnapshot {
            snapshot,
            signature,
        })))
    }

    /// Certifies one runtime leaf key under `account`'s current root: the
    /// environment-signed Birth and the root-signed key certificate a
    /// [`crate::verify::CertifiedSigner`] takes with the leaf's private key.
    ///
    /// # Errors
    /// When the claims are invalid.
    pub fn certify(
        &self,
        account: &AccountRoot,
        certification: Certification<'_>,
    ) -> Result<CertificateChain, VerifyError> {
        let birth = Birth {
            account_id: account.id.clone(),
            environment: self.environment.clone(),
            genesis_public_key: account.root.verifying_key().to_bytes().to_vec(),
            issued_at_unix_seconds: certification.validity.not_before_unix_seconds,
        };
        let key = CertifiedKey {
            account_id: account.id.clone(),
            key_id: certification.key_id.into(),
            public_key: certification.public_key.to_bytes().to_vec(),
            key_epoch: account.minimum_key_epoch,
            validity: certification.validity,
            grants: certification.grants,
            principal: certification.principal,
            parent_key_id: None,
            parent_revocation_ids: vec![],
        };
        Ok(CertificateChain {
            birth: BirthCertificate {
                signature: sign(&self.key, BIRTH_DOMAIN, &BirthProto::from(birth.clone()))?.0,
                birth,
            },
            keys: vec![KeyCertificate {
                signature: sign(
                    &account.root,
                    KEY_DOMAIN,
                    &CertifiedKeyProto::from(key.clone()),
                )?
                .0,
                key,
            }],
        })
    }
}

impl Issuer {
    /// An environment-signed spend allowance for `account`, carried as a
    /// caveat and verified offline in the same chain.
    ///
    /// # Errors
    /// When the allowance cannot be encoded.
    pub fn allowance(
        &self,
        account: &str,
        spend_micros: u64,
        expires_at_unix_seconds: u64,
    ) -> Result<SignedAllowance, VerifyError> {
        let allowance = Allowance {
            account_id: account.into(),
            spend_micros,
            expires_at_unix_seconds,
        };
        let signature = sign(
            &self.key,
            crate::ALLOWANCE_DOMAIN,
            &AllowanceProto::from(allowance.clone()),
        )?
        .0;
        Ok(SignedAllowance {
            allowance,
            signature,
        })
    }
}

impl AccountRoot {
    /// Account identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Revokes exactly these blocks (each with its retention deadline) and
    /// every descendant, as the next revocation delta.
    #[must_use]
    pub fn revoke(&mut self, blocks: &[(BlockId, u64)], now: u64) -> Vec<u8> {
        self.publish(RevocationPublication::Delta(RevocationDelta {
            account_id: self.id.clone(),
            sequence: self.revocations.0 + 1,
            previous_digest: self.revocations.1.to_vec(),
            issued_at_unix_seconds: now,
            keys: vec![],
            blocks: blocks
                .iter()
                .map(|(id, expires)| Revocation {
                    id: id.to_vec(),
                    expires_at_unix_seconds: *expires,
                    reason: String::new(),
                    label: None,
                })
                .collect(),
            revoke_all_before_epoch: None,
        }))
    }

    /// Raises the epoch floor the next keyring attests; certificates below it
    /// are refused once that keyring applies.
    pub fn set_minimum_key_epoch(&mut self, epoch: u64) {
        self.minimum_key_epoch = epoch;
    }

    fn publish(&mut self, publication: RevocationPublication) -> Vec<u8> {
        let transcript = revocation::transcript(&publication);
        let signature = self.root.sign(&transcript).to_bytes().to_vec();
        let sequence = match &publication {
            RevocationPublication::Delta(delta) => delta.sequence,
            RevocationPublication::Checkpoint(checkpoint) => checkpoint.sequence,
        };
        self.revocations = (sequence, block_id(&transcript, &signature));
        record(TrustRecordKind::Revocation(SignedRevocation {
            publication,
            signature,
        }))
    }
}

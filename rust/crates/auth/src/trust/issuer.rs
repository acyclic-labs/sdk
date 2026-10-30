//! Signing custody for trust records: the same records and certificates the
//! hosted Trust publishes, so local, self-hosted and hosted verifiers run one
//! follower. Every record custody produces is first checked against its
//! frontier through the follower's validation, so custody never publishes a
//! record a follower would refuse. Frontiers are built only by replaying
//! signed records, never from raw values, so they cannot be forged.
use super::follower::{Account, Applied, EnvironmentFrontier, decode};
use crate::{
    BIRTH_DOMAIN, ENVIRONMENT_DOMAIN, KEY_DOMAIN, KEYRING_DOMAIN, POSSESSION_DOMAIN,
    contract::*,
    crypto::{BlockId, claims_message, public_key, verify_signature},
    revocation,
    verify::VerifyError,
};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use prost::Message;

/// Environment custody: signs environment keyrings, Births, the keyrings
/// that set an account root, and spend allowances. It never needs an
/// account root's private key.
pub struct Issuer {
    environment: String,
    key: SigningKey,
}

/// One account's public keyring and revocation frontiers, as a follower
/// holds them: what the next record of each chain must extend.
pub struct KeyringFrontier {
    id: String,
    state: Account,
}

/// One account root's custody over its frontier. It needs no environment
/// key: the root alone signs its key certificates, its revocations and the
/// keyrings that narrow it (key revocation, epoch floor).
pub struct AccountRoot {
    root: SigningKey,
    frontier: KeyringFrontier,
}

/// What Trust certifies for one runtime signing key. Renewal certifies the
/// same public key again with a new validity.
pub struct Certification<'a> {
    /// Account-unique key identity; never reused for another key.
    pub key_id: &'a str,
    /// The runtime's leaf public key.
    pub public_key: VerifyingKey,
    /// Validity, at most 30 days.
    pub validity: Validity,
    /// The runtime's grant ceiling.
    pub grants: GrantSet,
    /// The runtime's attested identity.
    pub principal: Principal,
    /// Blocks (or keys) this certificate is revoked with: a key certified
    /// for one delegated credential dies with the capability it derives from.
    pub parent_revocation_ids: Vec<Vec<u8>>,
}

fn record(kind: TrustRecordKind) -> Vec<u8> {
    TrustRecordProto::from(TrustRecord { record: kind }).encode_to_vec()
}

fn sign(key: &SigningKey, domain: &[u8], claims: &impl Message) -> Result<Vec<u8>, VerifyError> {
    let mut message = Vec::new();
    claims_message(&mut message, domain, claims)?;
    Ok(key.sign(&message).to_bytes().to_vec())
}

impl Issuer {
    /// Environment custody. `key` must be a current environment key; at
    /// genesis it is the follower's pinned anchor.
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

    /// This custodian's public key: the pinned anchor of a new environment.
    #[must_use]
    pub fn anchor(&self) -> Vec<BlockId> {
        vec![self.key.verifying_key().to_bytes()]
    }

    /// The next environment keyring, replacing the environment keys with
    /// `keys` at `now`, signed by this custodian, which `frontier` must
    /// currently trust. `frontier` advances only when the record applies.
    ///
    /// # Errors
    /// When this key is not current in `frontier`, `keys` is empty, or `now`
    /// precedes the frontier.
    pub fn environment_keyring(
        &self,
        frontier: &mut EnvironmentFrontier,
        keys: &[BlockId],
        now: u64,
    ) -> Result<Vec<u8>, VerifyError> {
        let (sequence, digest) = frontier.frontier();
        let keyring = EnvironmentKeyring {
            environment: frontier.environment().to_owned(),
            sequence: sequence.checked_add(1).ok_or(VerifyError::Trust)?,
            previous_digest: digest.to_vec(),
            keys: keys.iter().map(|key| key.to_vec()).collect(),
            issued_at_unix_seconds: now,
        };
        let signature = sign(
            &self.key,
            ENVIRONMENT_DOMAIN,
            &EnvironmentKeyringProto::from(keyring.clone()),
        )?;
        let signed = SignedEnvironmentKeyring { keyring, signature };
        frontier.apply(&signed, now)?;
        Ok(record(TrustRecordKind::Environment(signed)))
    }

    /// Creates `id` under `root`: the account's first keyring (epoch 1) and
    /// its first revocation checkpoint, both at `now`, in Stream order.
    ///
    /// # Errors
    /// When a record cannot be encoded.
    pub fn genesis(
        &self,
        id: impl Into<String>,
        root: SigningKey,
        now: u64,
    ) -> Result<(AccountRoot, Vec<Vec<u8>>), VerifyError> {
        let id = id.into();
        let frontier = KeyringFrontier::resume(id.clone(), &[], &[])?;
        let keyring = self.keyring_for(
            &frontier,
            &root.verifying_key(),
            frontier.minimum_key_epoch(),
            now,
        )?;
        let (account, checkpoint) = AccountRoot::genesis(id, root, &self.anchor(), &keyring, now)?;
        Ok((account, vec![keyring, checkpoint]))
    }

    /// The next keyring of `account`, held publicly in `frontier`: it sets
    /// `root` as the account's only root at `minimum_key_epoch`, keeping
    /// every unexpired revocation. The root's private key is not needed.
    /// The caller publishes it, then applies it to `frontier`.
    ///
    /// # Errors
    /// When the record would not apply to `frontier`, such as a lowered
    /// epoch or an earlier `now`.
    pub fn keyring_for(
        &self,
        frontier: &KeyringFrontier,
        root: &VerifyingKey,
        minimum_key_epoch: u64,
        now: u64,
    ) -> Result<Vec<u8>, VerifyError> {
        let snapshot = frontier.next_keyring(root, minimum_key_epoch, now)?;
        let signature = sign(
            &self.key,
            KEYRING_DOMAIN,
            &KeyringSnapshotProto::from(snapshot.clone()),
        )?;
        let record = TrustRecordKind::Keyring(SignedKeyringSnapshot {
            snapshot,
            signature,
        });
        frontier.check(&record, &[self.key.verifying_key().to_bytes()], now)?;
        Ok(self::record(record))
    }

    /// [`Self::keyring_for`] `account`'s own root and epoch, applied to its
    /// frontier.
    ///
    /// # Errors
    /// When the record would not apply.
    pub fn keyring(&self, account: &mut AccountRoot, now: u64) -> Result<Vec<u8>, VerifyError> {
        let record = self.keyring_for(
            &account.frontier,
            &account.root.verifying_key(),
            account.minimum_key_epoch(),
            now,
        )?;
        account
            .frontier
            .apply(&record, &[self.key.verifying_key().to_bytes()], now)?;
        Ok(record)
    }

    /// The environment-signed Birth of `account` with genesis key `root`,
    /// issued at `issued_at`: what [`AccountRoot::certify`] extends.
    ///
    /// # Errors
    /// When the claims cannot be encoded.
    pub fn birth(
        &self,
        account: &str,
        root: &VerifyingKey,
        issued_at: u64,
    ) -> Result<BirthCertificate, VerifyError> {
        let birth = Birth {
            account_id: account.into(),
            environment: self.environment.clone(),
            genesis_public_key: root.to_bytes().to_vec(),
            issued_at_unix_seconds: issued_at,
        };
        Ok(BirthCertificate {
            signature: sign(&self.key, BIRTH_DOMAIN, &BirthProto::from(birth.clone()))?,
            birth,
        })
    }

    /// Signs `account`'s Birth, then [`AccountRoot::certify`]: the one
    /// certification path when this process holds both keys.
    ///
    /// # Errors
    /// When the claims cannot be encoded.
    pub fn certify(
        &self,
        account: &AccountRoot,
        certification: Certification<'_>,
    ) -> Result<CertificateChain, VerifyError> {
        let birth = self.birth(
            account.id(),
            &account.root.verifying_key(),
            certification.validity.not_before_unix_seconds,
        )?;
        account.certify(&birth, certification)
    }

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
        products: Vec<String>,
    ) -> Result<SignedAllowance, VerifyError> {
        let allowance = Allowance {
            account_id: account.into(),
            spend_micros,
            expires_at_unix_seconds,
            products,
        };
        let signature = sign(
            &self.key,
            crate::ALLOWANCE_DOMAIN,
            &AllowanceProto::from(allowance.clone()),
        )?;
        Ok(SignedAllowance {
            allowance,
            signature,
        })
    }
}

impl KeyringFrontier {
    /// Replays account `id`'s own Stream records in order through the
    /// follower's validation; no records is a new account. `environment` is
    /// the environment's public keys, which authenticate the keyrings that
    /// set a root.
    ///
    /// # Errors
    /// A gap, a foreign signer or account, or a rollback.
    pub fn resume(
        id: impl Into<String>,
        environment: &[BlockId],
        records: &[&[u8]],
    ) -> Result<Self, VerifyError> {
        let mut frontier = Self {
            id: id.into(),
            state: Account::default(),
        };
        for record in records {
            frontier.apply(record, environment, u64::MAX)?;
        }
        Ok(frontier)
    }

    /// Applies the account's next Stream record at `now`, exactly as a
    /// follower does. A refusal changes nothing.
    ///
    /// # Errors
    /// A gap, a foreign signer or account, a rollback, or a future record.
    pub fn apply(
        &mut self,
        record: &[u8],
        environment: &[BlockId],
        now: u64,
    ) -> Result<Applied, VerifyError> {
        let record = decode(record)?.record;
        self.state
            .apply(&self.id, &record, environment, now)
            .map(|(applied, _)| applied)
    }

    /// Account identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The current root, if a keyring has applied.
    #[must_use]
    pub fn root(&self) -> Option<BlockId> {
        self.state
            .keyring
            .as_ref()
            .and_then(|keyring| keyring.root.first().copied())
    }

    /// The current keyring's epoch floor; 1 before the first keyring.
    #[must_use]
    pub fn minimum_key_epoch(&self) -> u64 {
        self.state
            .keyring
            .as_ref()
            .map_or(1, |keyring| keyring.minimum_key_epoch)
    }

    /// Whether `record` would apply, without changing this frontier: only
    /// the keyring chain is involved.
    fn check(
        &self,
        record: &TrustRecordKind,
        environment: &[BlockId],
        now: u64,
    ) -> Result<(), VerifyError> {
        let mut scratch = Account::default();
        scratch.keyring = self.state.keyring.clone();
        scratch
            .apply(&self.id, record, environment, now)
            .map(|_| ())
    }

    fn next_keyring(
        &self,
        root: &VerifyingKey,
        minimum_key_epoch: u64,
        now: u64,
    ) -> Result<KeyringSnapshot, VerifyError> {
        let (sequence, digest, mut revoked_key_ids, mut revocations) = match &self.state.keyring {
            Some(current) => (
                current.sequence.checked_add(1).ok_or(VerifyError::Trust)?,
                current.digest,
                current.revoked_keys.iter().cloned().collect::<Vec<_>>(),
                current
                    .revoked_blocks
                    .iter()
                    .filter(|(_, expires)| **expires > now)
                    .map(|(id, expires)| Revocation {
                        id: id.to_vec(),
                        expires_at_unix_seconds: *expires,
                        reason: String::new(),
                        label: None,
                    })
                    .collect::<Vec<_>>(),
            ),
            None => (1, [0; 32], vec![], vec![]),
        };
        // Canonical order, so equal keyrings encode identically.
        revoked_key_ids.sort_unstable();
        revocations.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        Ok(KeyringSnapshot {
            account_id: self.id.clone(),
            sequence,
            previous_digest: digest.to_vec(),
            minimum_key_epoch,
            revoked_key_ids,
            revocations,
            root_public_key: root.to_bytes().to_vec(),
            issued_at_unix_seconds: now,
        })
    }
}

/// Verifies `proof` that the holder of `root` signed `birth`, which must
/// name `root` as its genesis key: what the environment checks before it
/// signs a keyring setting a customer-held root.
///
/// # Errors
/// A Birth naming another key, or a proof over another Birth or by another
/// key.
pub fn verify_possession(birth: &Birth, root: &[u8], proof: &[u8]) -> Result<(), VerifyError> {
    if birth.genesis_public_key != root {
        return Err(VerifyError::Signature);
    }
    let mut message = Vec::new();
    claims_message(
        &mut message,
        POSSESSION_DOMAIN,
        &BirthProto::from(birth.clone()),
    )?;
    verify_signature(&public_key(root)?, &message, proof)
}

impl AccountRoot {
    /// Creates custody of `id` under a root only its holder has: replays the
    /// account's first keyring, environment-signed over an empty frontier
    /// (see [`Issuer::keyring_for`]), through the follower's validation,
    /// requires it to name `root`, then signs the first revocation
    /// checkpoint. Returns the custody and the checkpoint to publish after
    /// the keyring.
    ///
    /// # Errors
    /// A keyring that does not apply to a new account or names another root.
    pub fn genesis(
        id: impl Into<String>,
        root: SigningKey,
        environment: &[BlockId],
        keyring: &[u8],
        now: u64,
    ) -> Result<(Self, Vec<u8>), VerifyError> {
        let mut frontier = KeyringFrontier::resume(id, &[], &[])?;
        frontier.apply(keyring, environment, now)?;
        if frontier.root() != Some(root.verifying_key().to_bytes()) {
            return Err(VerifyError::Trust);
        }
        let mut account = Self { root, frontier };
        let checkpoint = account.publish(
            RevocationPublication::Checkpoint(RevocationCheckpoint {
                account_id: account.frontier.id.clone(),
                sequence: 1,
                retired_before_unix_seconds: 0,
                minimum_key_epoch: 0,
                keys: vec![],
                blocks: vec![],
                issued_at_unix_seconds: now,
            }),
            now,
        )?;
        Ok((account, checkpoint))
    }

    /// `root`'s proof that it holds the key `birth` names, for
    /// [`verify_possession`]. It precedes the account's custody: the
    /// environment checks it before the first keyring exists.
    ///
    /// # Errors
    /// When the Birth cannot be encoded.
    pub fn possession_proof(root: &SigningKey, birth: &Birth) -> Result<Vec<u8>, VerifyError> {
        sign(root, POSSESSION_DOMAIN, &BirthProto::from(birth.clone()))
    }

    /// Restores custody of `id` under `root`: [`KeyringFrontier::resume`],
    /// then requires `root` to be the current root with a revocation
    /// checkpoint applied.
    ///
    /// # Errors
    /// A gap, a foreign signer or account, a rollback, or a replay that does
    /// not end with `root` current and a revocation checkpoint.
    pub fn resume(
        id: impl Into<String>,
        root: SigningKey,
        environment: &[BlockId],
        records: &[&[u8]],
    ) -> Result<Self, VerifyError> {
        let frontier = KeyringFrontier::resume(id, environment, records)?;
        if frontier.root() != Some(root.verifying_key().to_bytes())
            || frontier.state.revocations.is_none()
        {
            return Err(VerifyError::Trust);
        }
        Ok(Self { root, frontier })
    }

    /// Account identity.
    #[must_use]
    pub fn id(&self) -> &str {
        self.frontier.id()
    }

    /// The public frontier this custody extends.
    #[must_use]
    pub const fn frontier(&self) -> &KeyringFrontier {
        &self.frontier
    }

    /// The current keyring's epoch floor, which new key certificates carry.
    #[must_use]
    pub fn minimum_key_epoch(&self) -> u64 {
        self.frontier.minimum_key_epoch()
    }

    /// Certifies one runtime leaf key at the current epoch under `birth`,
    /// which must name this account and this root as genesis. The root
    /// signs only the key certificate; the Birth may come from an
    /// environment custodian that never sees this root's private key.
    ///
    /// # Errors
    /// A Birth of another account or root, or claims that cannot be encoded.
    pub fn certify(
        &self,
        birth: &BirthCertificate,
        certification: Certification<'_>,
    ) -> Result<CertificateChain, VerifyError> {
        if birth.birth.account_id != self.id()
            || birth.birth.genesis_public_key != self.root.verifying_key().to_bytes()
        {
            return Err(VerifyError::Trust);
        }
        let key = CertifiedKey {
            account_id: self.id().into(),
            key_id: certification.key_id.into(),
            public_key: certification.public_key.to_bytes().to_vec(),
            key_epoch: self.minimum_key_epoch(),
            validity: certification.validity,
            grants: certification.grants,
            principal: certification.principal,
            parent_key_id: None,
            parent_revocation_ids: certification.parent_revocation_ids,
        };
        Ok(CertificateChain {
            birth: birth.clone(),
            keys: vec![KeyCertificate {
                signature: sign(
                    &self.root,
                    KEY_DOMAIN,
                    &CertifiedKeyProto::from(key.clone()),
                )?,
                key,
            }],
        })
    }

    /// Revokes exactly these blocks (each with its retention deadline) and
    /// every descendant, as the next revocation delta.
    ///
    /// # Errors
    /// When the delta would not apply, such as `now` preceding the frontier.
    pub fn revoke(&mut self, blocks: &[(BlockId, u64)], now: u64) -> Result<Vec<u8>, VerifyError> {
        self.delta(
            vec![],
            blocks
                .iter()
                .map(|(id, expires)| Revocation {
                    id: id.to_vec(),
                    expires_at_unix_seconds: *expires,
                    reason: String::new(),
                    label: None,
                })
                .collect(),
            now,
        )
    }

    /// Revokes exactly these key identities (each with the retirement time
    /// covering everything it issued) and their descendants, as the next
    /// revocation delta.
    ///
    /// # Errors
    /// When the delta would not apply.
    pub fn revoke_keys(&mut self, keys: &[(&str, u64)], now: u64) -> Result<Vec<u8>, VerifyError> {
        self.delta(
            keys.iter()
                .map(|(key_id, expires)| KeyRevocation {
                    key_id: (*key_id).into(),
                    expires_at_unix_seconds: *expires,
                })
                .collect(),
            vec![],
            now,
        )
    }

    /// Refuses every key certified below `epoch`: the next keyring, signed by
    /// this root alone. The one "revoke everything before" mechanism.
    ///
    /// # Errors
    /// When `epoch` is not above the current floor or the keyring would not
    /// apply.
    pub fn raise_minimum_epoch(&mut self, epoch: u64, now: u64) -> Result<Vec<u8>, VerifyError> {
        if epoch <= self.minimum_key_epoch() {
            return Err(VerifyError::Trust);
        }
        let snapshot = self
            .frontier
            .next_keyring(&self.root.verifying_key(), epoch, now)?;
        let signature = sign(
            &self.root,
            KEYRING_DOMAIN,
            &KeyringSnapshotProto::from(snapshot.clone()),
        )?;
        self.extend(
            TrustRecordKind::Keyring(SignedKeyringSnapshot {
                snapshot,
                signature,
            }),
            now,
        )
    }

    fn delta(
        &mut self,
        keys: Vec<KeyRevocation>,
        blocks: Vec<Revocation>,
        now: u64,
    ) -> Result<Vec<u8>, VerifyError> {
        let (sequence, digest) = self
            .frontier
            .state
            .revocations
            .as_ref()
            .ok_or(VerifyError::Trust)?
            .frontier();
        self.publish(
            RevocationPublication::Delta(RevocationDelta {
                account_id: self.id().into(),
                sequence: sequence.checked_add(1).ok_or(VerifyError::Trust)?,
                previous_digest: digest.to_vec(),
                issued_at_unix_seconds: now,
                keys,
                blocks,
                revoke_all_before_epoch: None,
            }),
            now,
        )
    }

    fn publish(
        &mut self,
        publication: RevocationPublication,
        now: u64,
    ) -> Result<Vec<u8>, VerifyError> {
        let transcript = revocation::transcript(&publication);
        let signature = self.root.sign(&transcript).to_bytes().to_vec();
        self.extend(
            TrustRecordKind::Revocation(SignedRevocation {
                publication,
                signature,
            }),
            now,
        )
    }

    /// Applies a record this root just signed to its frontier through the
    /// follower's validation, then returns it for publication. Root-signed
    /// records need no environment key.
    fn extend(&mut self, record: TrustRecordKind, now: u64) -> Result<Vec<u8>, VerifyError> {
        self.frontier
            .state
            .apply(&self.frontier.id, &record, &[], now)?;
        Ok(self::record(record))
    }
}

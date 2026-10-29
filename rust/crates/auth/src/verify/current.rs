//! Immutable authentication evidence with a single current-trust check.
use super::*;

/// Heap storage needed to copy compact public evidence from an authenticated owner.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TrustEvidenceStorage<'a> {
    allocation_bytes: [u64; 6],
    keys: &'a [KeyCertificate],
    /// Fixed-size identities copied into the compact representation.
    pub copied_bytes: u64,
    /// Total retained heap storage, including buffers moved from the token.
    pub owned_bytes: u64,
}

impl TrustEvidenceStorage<'_> {
    /// Each nonempty allocation, in advance of copying any public evidence.
    pub fn allocation_bytes(&self) -> impl Iterator<Item = u64> + '_ {
        self.allocation_bytes
            .into_iter()
            .chain(
                self.keys
                    .iter()
                    .map(|certificate| certificate.key.key_id.len() as u64),
            )
            .filter(|bytes| *bytes != 0)
    }
}

#[derive(Clone, Debug)]
struct Key {
    id: String,
    epoch: u64,
}

/// Authenticated facts needed to recheck current trust and time.
///
/// This value has no append key, signatures, grants or authorization methods.
/// Its private fields can only be obtained from a verified capability.
#[derive(Clone, Debug)]
pub struct TrustEvidence {
    environment: String,
    account: String,
    environment_issuer: BlockId,
    issuers: Vec<BlockId>,
    ids: Vec<BlockId>,
    keys: Vec<Key>,
    next_keys: Vec<BlockId>,
    not_before: u64,
    expires_at: u64,
}

struct Certificate<'a> {
    id: &'a str,
    epoch: u64,
    parents: &'a [Vec<u8>],
}

struct View<'a, C, N> {
    environment: &'a str,
    account: &'a str,
    environment_issuer: &'a BlockId,
    issuers: &'a [BlockId],
    ids: &'a [BlockId],
    certificates: C,
    next_keys: N,
    not_before: u64,
    expires_at: u64,
}

impl<'a, C, N> View<'a, C, N>
where
    C: Iterator<Item = Certificate<'a>>,
    N: Iterator<Item = &'a [u8]>,
{
    fn preserved_by(self, descendant: &VerifiedCapability) -> Result<bool, VerifyError> {
        let certificates = descendant.certificates()?;
        if self.environment != certificates.birth.birth.environment
            || self.account != certificates.birth.birth.account_id
            || self.environment_issuer != &descendant.environment_issuer
            || self.issuers != descendant.issuers
            || self.not_before > descendant.not_before
            || self.expires_at < descendant.expires_at
        {
            return Ok(false);
        }
        let mut inherited = descendant.revocation_ids();
        for id in self.ids {
            if !inherited.by_ref().any(|actual| actual == id) {
                return Ok(false);
            }
        }
        let mut keys = certificates.keys.iter();
        for certificate in self.certificates {
            let Some(key) = keys.next() else {
                return Ok(false);
            };
            if certificate.id != key.key.key_id || certificate.epoch != key.key.key_epoch {
                return Ok(false);
            }
            for id in certificate.parents {
                if !inherited.by_ref().any(|actual| actual == id) {
                    return Ok(false);
                }
            }
        }
        Ok(keys.next().is_none())
    }

    fn check(mut self, trust: &(impl Trust + ?Sized), now: u64) -> Result<(), VerifyError> {
        if now < self.not_before || now >= self.expires_at {
            return Err(VerifyError::Time);
        }
        let environments = trust.environment_keys(self.environment)?;
        if !environments.contains(self.environment_issuer) {
            return Err(VerifyError::Trust);
        }
        let account = trust.account(self.account)?;
        if now < account.not_before_unix_seconds {
            return Err(VerifyError::Time);
        }
        if self.next_keys.any(|next| {
            environments
                .iter()
                .chain(account.issuer_keys)
                .any(|key| next == key)
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
        for certificate in self.certificates {
            if certificate.epoch < account.minimum_key_epoch
                || trust.revoked(
                    self.account,
                    RevocationCheck::Key {
                        id: certificate.id,
                        epoch: certificate.epoch,
                    },
                )?
            {
                return Err(VerifyError::Revoked);
            }
            for parent in certificate.parents {
                let parent = parent
                    .as_slice()
                    .try_into()
                    .map_err(|_| VerifyError::Malformed)?;
                if trust.revoked(self.account, RevocationCheck::Block(parent))? {
                    return Err(VerifyError::Revoked);
                }
            }
        }
        for block in self.ids {
            if trust.revoked(self.account, RevocationCheck::Block(block))? {
                return Err(VerifyError::Revoked);
            }
        }
        Ok(())
    }
}

fn slots<T>(capacity: usize) -> Option<usize> {
    capacity.checked_mul(std::mem::size_of::<T>())
}

fn bytes(value: Option<usize>) -> Result<u64, VerifyError> {
    value
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(VerifyError::Malformed)
}

impl VerifiedCapability {
    /// Project the presented authority's public identity and every revocation
    /// dependency. The authenticated platform supplies its namespace; a token
    /// cannot choose that binding. This snapshot never admits later use without
    /// the original owner's current-trust, audience and expiry checks.
    pub fn caller_authority(&self, namespace: &[u8]) -> Result<CallerAuthority, VerifyError> {
        if namespace.is_empty() {
            return Err(VerifyError::Malformed);
        }
        let principal = self.effective().principal();
        Ok(CallerAuthority {
            principal: CallerIdentity {
                account: self.account_id()?.as_bytes().to_vec(),
                ns: namespace.to_vec(),
                id: principal
                    .lineage
                    .last()
                    .unwrap_or(&principal.id)
                    .as_bytes()
                    .to_vec(),
            },
            expires_at_unix_seconds: self.expires_at,
            revocation: CallerRevocation {
                keys: self
                    .certificates()?
                    .keys
                    .iter()
                    .map(|certificate| CallerKeyRevocation {
                        key_id: certificate.key.key_id.clone(),
                        key_epoch: certificate.key.key_epoch,
                    })
                    .collect(),
                blocks: self.revocation_ids().map(<[u8]>::to_vec).collect(),
            },
        })
    }

    /// Whether a same-key descendant retains this admission's environment,
    /// issuers, key identities/epochs, half-open validity and every revocation
    /// dependency. The canonical issuer preserves ancestor order; extra block
    /// dependencies may narrow the descendant. One pass, with no allocation.
    /// This does not authorize new resource grants or an acting identity.
    pub fn preserved_by(&self, descendant: &Self) -> Result<bool, VerifyError> {
        self.trust_view()?.preserved_by(descendant)
    }

    fn certificates(&self) -> Result<&CertificateChain, VerifyError> {
        self.blocks
            .first()
            .and_then(|block| block.certificates.as_ref())
            .ok_or(VerifyError::Malformed)
    }

    fn trust_view(
        &self,
    ) -> Result<
        View<'_, impl Iterator<Item = Certificate<'_>>, impl Iterator<Item = &[u8]>>,
        VerifyError,
    > {
        let certificates = self.certificates()?;
        Ok(View {
            environment: &certificates.birth.birth.environment,
            account: &certificates.birth.birth.account_id,
            environment_issuer: &self.environment_issuer,
            issuers: &self.issuers,
            ids: &self.ids,
            certificates: certificates.keys.iter().map(|certificate| Certificate {
                id: &certificate.key.key_id,
                epoch: certificate.key.key_epoch,
                parents: &certificate.key.parent_revocation_ids,
            }),
            next_keys: self
                .blocks
                .iter()
                .map(|block| block.next_public_key.as_slice()),
            not_before: self.not_before,
            expires_at: self.expires_at,
        })
    }

    fn evidence_id_count(&self) -> Option<usize> {
        self.certificates()
            .ok()?
            .keys
            .iter()
            .try_fold(self.ids.len(), |count, certificate| {
                count.checked_add(certificate.key.parent_revocation_ids.len())
            })
    }

    /// Preflights every allocation and byte copied into public trust evidence.
    ///
    /// # Errors
    /// Fails if the checked storage sizes overflow.
    pub fn trust_evidence_storage(&self) -> Result<TrustEvidenceStorage<'_>, VerifyError> {
        let certificates = self.certificates()?;
        let allocation_bytes = [
            bytes(slots::<Key>(certificates.keys.len()))?,
            bytes(self.evidence_id_count().and_then(slots::<BlockId>))?,
            bytes(slots::<BlockId>(self.blocks.len()))?,
            bytes(slots::<BlockId>(self.issuers.len()))?,
            bytes(Some(certificates.birth.birth.environment.len()))?,
            bytes(Some(certificates.birth.birth.account_id.len()))?,
        ];
        let mut storage = TrustEvidenceStorage {
            allocation_bytes,
            keys: &certificates.keys,
            copied_bytes: 0,
            owned_bytes: 0,
        };
        let owned_bytes = storage
            .allocation_bytes()
            .try_fold(0u64, u64::checked_add)
            .ok_or(VerifyError::Malformed)?;
        storage.owned_bytes = owned_bytes;
        storage.copied_bytes = owned_bytes;
        Ok(storage)
    }

    /// Copies only public trust facts from an immutable authenticated owner.
    /// No append key, policy graph or signature is retained by the result.
    /// Budgeted callers admit every `trust_evidence_storage` allocation first.
    ///
    /// # Errors
    /// Fails if the authenticated certificate chain or an identity is malformed.
    pub fn trust_evidence(&self) -> Result<TrustEvidence, VerifyError> {
        let count = self.evidence_id_count().ok_or(VerifyError::Malformed)?;
        let certificates = self.certificates()?;
        let mut ids = Vec::with_capacity(count);
        ids.extend_from_slice(&self.ids);
        let mut keys = Vec::with_capacity(certificates.keys.len());
        for certificate in &certificates.keys {
            for parent in &certificate.key.parent_revocation_ids {
                ids.push(
                    parent
                        .as_slice()
                        .try_into()
                        .map_err(|_| VerifyError::Malformed)?,
                );
            }
            keys.push(Key {
                id: certificate.key.key_id.clone(),
                epoch: certificate.key.key_epoch,
            });
        }
        let mut next_keys = Vec::with_capacity(self.blocks.len());
        for block in &self.blocks {
            next_keys.push(
                block
                    .next_public_key
                    .as_slice()
                    .try_into()
                    .map_err(|_| VerifyError::Malformed)?,
            );
        }
        Ok(TrustEvidence {
            environment: certificates.birth.birth.environment.clone(),
            account: certificates.birth.birth.account_id.clone(),
            environment_issuer: self.environment_issuer,
            issuers: self.issuers.clone(),
            ids,
            keys,
            next_keys,
            not_before: self.not_before,
            expires_at: self.expires_at,
        })
    }
}

impl TrustEvidence {
    /// The same-key admission preservation check without retaining the parent's
    /// bearer, append key or policy graph. See `VerifiedCapability::preserved_by`.
    pub fn preserved_by(&self, descendant: &VerifiedCapability) -> Result<bool, VerifyError> {
        self.view().preserved_by(descendant)
    }

    fn view(&self) -> View<'_, impl Iterator<Item = Certificate<'_>>, impl Iterator<Item = &[u8]>> {
        View {
            environment: &self.environment,
            account: &self.account,
            environment_issuer: &self.environment_issuer,
            issuers: &self.issuers,
            ids: &self.ids,
            certificates: self.keys.iter().map(|key| Certificate {
                id: &key.id,
                epoch: key.epoch,
                parents: &[],
            }),
            next_keys: self.next_keys.iter().map(<[u8; 32]>::as_slice),
            not_before: self.not_before,
            expires_at: self.expires_at,
        }
    }

    /// Rechecks current issuers, epochs, every revocation ancestor and validity.
    ///
    /// # Errors
    /// Returns current-trust/time errors if this evidence has become invalid.
    pub fn check(
        &self,
        trust: &(impl Trust + ?Sized),
        now_unix_seconds: u64,
    ) -> Result<(), VerifyError> {
        self.view().check(trust, now_unix_seconds)
    }

    /// Counts actual heap capacities retained by this owner.
    ///
    /// # Errors
    /// Fails if the checked storage size overflows.
    pub fn owned_bytes(&self) -> Result<u64, VerifyError> {
        let base = slots::<Key>(self.keys.capacity())
            .and_then(|size| size.checked_add(slots::<BlockId>(self.next_keys.capacity())?))
            .and_then(|size| size.checked_add(self.environment.capacity()))
            .and_then(|size| size.checked_add(self.account.capacity()))
            .and_then(|size| size.checked_add(slots::<BlockId>(self.issuers.capacity())?))
            .and_then(|size| size.checked_add(slots::<BlockId>(self.ids.capacity())?));
        bytes(base.and_then(|base| {
            self.keys
                .iter()
                .try_fold(base, |size, key| size.checked_add(key.id.capacity()))
        }))
    }
}

pub(super) fn check(
    capability: &VerifiedCapability,
    trust: &(impl Trust + ?Sized),
    now: u64,
) -> Result<(), VerifyError> {
    capability.trust_view()?.check(trust, now)
}

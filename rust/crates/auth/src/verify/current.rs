//! Immutable authentication evidence with a single current-trust check.
use super::*;

/// Heap storage needed when consuming a token into compact trust evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrustEvidenceStorage {
    /// New vector storage allocated by the conversion.
    pub allocation_bytes: [u64; 3],
    /// Fixed-size identities copied into the compact representation.
    pub copied_bytes: u64,
    /// Total retained heap storage, including buffers moved from the token.
    pub owned_bytes: u64,
}

#[derive(Clone, Debug)]
struct Key {
    id: String,
    epoch: u64,
}

/// Authenticated facts needed to recheck current trust and time.
///
/// This value has no append key, signatures, grants or authorization methods.
/// Its private fields can only be obtained by consuming a verified capability.
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
    fn check(mut self, trust: &(impl Trust + ?Sized), now: u64) -> Result<(), VerifyError> {
        if now < self.not_before || now >= self.expires_at {
            return Err(VerifyError::Time);
        }
        let environments = trust.environment_keys(self.environment)?;
        if !environments.contains(self.environment_issuer) {
            return Err(VerifyError::Trust);
        }
        let account = trust.account(self.account)?;
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

    /// Preflights new allocations, copied identities and retained buffers.
    ///
    /// # Errors
    /// Fails if the checked storage sizes overflow.
    pub fn trust_evidence_storage(&self) -> Result<TrustEvidenceStorage, VerifyError> {
        let certificates = self.certificates()?;
        let allocation_bytes = [
            bytes(slots::<Key>(certificates.keys.len()))?,
            bytes(self.evidence_id_count().and_then(slots::<BlockId>))?,
            bytes(slots::<BlockId>(self.blocks.len()))?,
        ];
        let copied_bytes = allocation_bytes[1]
            .checked_add(allocation_bytes[2])
            .ok_or(VerifyError::Malformed)?;
        let allocated = allocation_bytes
            .into_iter()
            .try_fold(0u64, u64::checked_add)
            .ok_or(VerifyError::Malformed)?;
        let base = usize::try_from(allocated)
            .ok()
            .and_then(|size| size.checked_add(certificates.birth.birth.environment.capacity()))
            .and_then(|size| size.checked_add(certificates.birth.birth.account_id.capacity()))
            .and_then(|size| size.checked_add(slots::<BlockId>(self.issuers.capacity())?));
        let owned = base.and_then(|base| {
            certificates
                .keys
                .iter()
                .try_fold(base, |size, certificate| {
                    size.checked_add(certificate.key.key_id.capacity())
                })
        });
        Ok(TrustEvidenceStorage {
            allocation_bytes,
            copied_bytes,
            owned_bytes: bytes(owned)?,
        })
    }

    /// Discards append authority and immutable policy material. Fixed-size
    /// revocation and next-key identities occupy flat vectors; certificate
    /// parents do not retain a second graph of individually allocated buffers.
    /// Budgeted callers admit `trust_evidence_storage` before conversion and
    /// account for the resulting actual `owned_bytes` before transfer.
    ///
    /// # Errors
    /// Fails if the authenticated certificate chain or an identity is malformed.
    pub fn into_trust_evidence(mut self) -> Result<TrustEvidence, VerifyError> {
        let count = self.evidence_id_count().ok_or(VerifyError::Malformed)?;
        let certificates = self
            .blocks
            .first_mut()
            .and_then(|block| block.certificates.take())
            .ok_or(VerifyError::Malformed)?;
        let mut ids = Vec::with_capacity(count);
        ids.extend(self.ids);
        let mut keys = Vec::with_capacity(certificates.keys.len());
        for certificate in certificates.keys {
            for parent in certificate.key.parent_revocation_ids {
                ids.push(
                    parent
                        .as_slice()
                        .try_into()
                        .map_err(|_| VerifyError::Malformed)?,
                );
            }
            keys.push(Key {
                id: certificate.key.key_id,
                epoch: certificate.key.key_epoch,
            });
        }
        let mut next_keys = Vec::with_capacity(self.blocks.len());
        for block in self.blocks {
            next_keys.push(
                block
                    .next_public_key
                    .as_slice()
                    .try_into()
                    .map_err(|_| VerifyError::Malformed)?,
            );
        }
        Ok(TrustEvidence {
            environment: certificates.birth.birth.environment,
            account: certificates.birth.birth.account_id,
            environment_issuer: self.environment_issuer,
            issuers: self.issuers,
            ids,
            keys,
            next_keys,
            not_before: self.not_before,
            expires_at: self.expires_at,
        })
    }
}

impl TrustEvidence {
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

//! Immutable authentication evidence with a single current-trust check.
use super::*;

/// Heap storage needed to copy compact public evidence from an authenticated owner.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TrustEvidenceStorage<'a> {
    allocation_bytes: [u64; 8],
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
            .chain(
                self.keys
                    .iter()
                    .flat_map(|certificate| &certificate.key.trust_requirements)
                    .flat_map(|requirement| requirement_allocations(requirement, false)),
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
    environment_issuers: Vec<BlockId>,
    issuers: Vec<BlockId>,
    ids: Vec<BlockId>,
    keys: Vec<Key>,
    next_keys: Vec<BlockId>,
    not_before: u64,
    expires_at: u64,
    requirements: Vec<TrustRequirement>,
}

#[derive(Clone, Copy)]
enum Ids<'a> {
    Fixed(&'a [BlockId]),
    Wire(&'a [Vec<u8>]),
}

impl<'a> Ids<'a> {
    fn len(self) -> usize {
        match self {
            Self::Fixed(ids) => ids.len(),
            Self::Wire(ids) => ids.len(),
        }
    }
    fn iter(self) -> impl Iterator<Item = &'a [u8]> {
        let (fixed, wire): (&[BlockId], &[Vec<u8>]) = match self {
            Self::Fixed(ids) => (ids, &[]),
            Self::Wire(ids) => (&[], ids),
        };
        fixed
            .iter()
            .map(<[u8; 32]>::as_slice)
            .chain(wire.iter().map(Vec::as_slice))
    }
}

struct Certificate<'a> {
    id: &'a str,
    epoch: u64,
    parents: &'a [Vec<u8>],
}

struct View<'a, C, N> {
    environment: &'a str,
    account: &'a str,
    environment_issuers: Ids<'a>,
    issuers: Ids<'a>,
    ids: Ids<'a>,
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
    fn requirement(self) -> Result<TrustRequirement, VerifyError>
    where
        C: Clone + ExactSizeIterator,
    {
        let count = self
            .certificates
            .clone()
            .try_fold(self.ids.len(), |count, certificate| {
                count.checked_add(certificate.parents.len())
            })
            .ok_or(VerifyError::Malformed)?;
        let mut blocks = Vec::with_capacity(count);
        blocks.extend(self.ids.iter().map(<[u8]>::to_vec));
        let keys = self
            .certificates
            .map(|certificate| {
                blocks.extend(certificate.parents.iter().cloned());
                CallerKeyRevocation {
                    key_id: certificate.id.into(),
                    key_epoch: certificate.epoch,
                }
            })
            .collect();
        Ok(TrustRequirement {
            version: 1,
            environment: self.environment.into(),
            account: self.account.into(),
            environment_issuers: self
                .environment_issuers
                .iter()
                .map(<[u8]>::to_vec)
                .collect(),
            issuers: self.issuers.iter().map(<[u8]>::to_vec).collect(),
            blocks,
            keys,
            next_keys: self.next_keys.map(<[u8]>::to_vec).collect(),
            validity: Validity {
                not_before_unix_seconds: self.not_before,
                expires_at_unix_seconds: self.expires_at,
            },
        })
    }
    fn preserved_by(self, descendant: &VerifiedCapability) -> Result<bool, VerifyError> {
        let certificates = descendant.certificates()?;
        if self.environment != certificates.birth.birth.environment
            || self.account != certificates.birth.birth.account_id
            || !descendant.environment_issuers.iter().all(|key| {
                self.environment_issuers
                    .iter()
                    .any(|original| original == key)
            })
            || !self
                .issuers
                .iter()
                .eq(descendant.issuers.iter().map(<[u8; 32]>::as_slice))
            || self.not_before > descendant.not_before
            || self.expires_at < descendant.expires_at
        {
            return Ok(false);
        }
        let mut inherited = descendant.revocation_ids();
        for id in self.ids.iter() {
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
        if !self
            .environment_issuers
            .iter()
            .all(|key| environments.iter().any(|actual| actual.as_slice() == key))
        {
            return Err(VerifyError::Trust);
        }
        let account = trust.account(self.account)?;
        if now < account.not_before_unix_seconds {
            return Err(VerifyError::Time);
        }
        if self.next_keys.any(|next| {
            environments
                .iter()
                .chain(account.issuer_keys.iter())
                .any(|key| next == key)
        }) {
            return Err(VerifyError::RepeatedKey);
        }
        if !self.issuers.iter().all(|issuer| {
            account
                .issuer_keys
                .iter()
                .any(|actual| actual.as_slice() == issuer)
        }) {
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
        for block in self.ids.iter() {
            let block = block.try_into().map_err(|_| VerifyError::Malformed)?;
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

fn requirement_allocations(
    requirement: &TrustRequirement,
    capacity: bool,
) -> impl Iterator<Item = u64> + '_ {
    let strings = [&requirement.environment, &requirement.account].into_iter()
        .chain(requirement.keys.iter().map(|key| &key.key_id))
        .map(move |value| if capacity { value.capacity() } else { value.len() } as u64);
    let vectors = [
        &requirement.environment_issuers,
        &requirement.issuers,
        &requirement.blocks,
        &requirement.next_keys,
    ];
    let slots = vectors.into_iter().map(move |values| {
        (if capacity {
            values.capacity()
        } else {
            values.len()
        } * std::mem::size_of::<Vec<u8>>()) as u64
    });
    let ids = vectors.into_iter().flat_map(|values| values.iter())
        .map(move |value| if capacity { value.capacity() } else { value.len() } as u64);
    let keys = (if capacity {
        requirement.keys.capacity()
    } else {
        requirement.keys.len()
    } * std::mem::size_of::<CallerKeyRevocation>()) as u64;
    strings.chain(slots).chain(ids).chain(std::iter::once(keys))
}

fn check_requirement(
    requirement: &TrustRequirement,
    trust: &(impl Trust + ?Sized),
    now: u64,
) -> Result<(), VerifyError> {
    View {
        environment: &requirement.environment,
        account: &requirement.account,
        environment_issuers: Ids::Wire(&requirement.environment_issuers),
        issuers: Ids::Wire(&requirement.issuers),
        ids: Ids::Wire(&requirement.blocks),
        certificates: requirement.keys.iter().map(|key| Certificate {
            id: &key.key_id,
            epoch: key.key_epoch,
            parents: &[],
        }),
        next_keys: requirement.next_keys.iter().map(Vec::as_slice),
        not_before: requirement.validity.not_before_unix_seconds,
        expires_at: requirement.validity.expires_at_unix_seconds,
    }
    .check(trust, now)
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
        let mut inherited = descendant.trust_requirements();
        Ok(self.trust_view()?.preserved_by(descendant)?
            && self
                .trust_requirements()
                .all(|requirement| inherited.by_ref().any(|actual| actual == requirement)))
    }

    fn trust_requirements(&self) -> impl Iterator<Item = &TrustRequirement> + Clone {
        self.blocks
            .first()
            .into_iter()
            .flat_map(|block| &block.certificates)
            .flat_map(|certificates| &certificates.keys)
            .flat_map(|certificate| &certificate.key.trust_requirements)
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
        View<
            '_,
            impl ExactSizeIterator<Item = Certificate<'_>> + Clone,
            impl Iterator<Item = &[u8]>,
        >,
        VerifyError,
    > {
        let certificates = self.certificates()?;
        Ok(View {
            environment: &certificates.birth.birth.environment,
            account: &certificates.birth.birth.account_id,
            environment_issuers: Ids::Fixed(&self.environment_issuers),
            issuers: Ids::Fixed(&self.issuers),
            ids: Ids::Fixed(&self.ids),
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
            bytes(slots::<BlockId>(self.environment_issuers.len()))?,
            bytes(Some(certificates.birth.birth.environment.len()))?,
            bytes(Some(certificates.birth.birth.account_id.len()))?,
            bytes(
                certificates
                    .keys
                    .iter()
                    .try_fold(0usize, |count, certificate| {
                        count.checked_add(certificate.key.trust_requirements.len())
                    })
                    .and_then(slots::<TrustRequirement>),
            )?,
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
        let requirement_count = certificates
            .keys
            .iter()
            .try_fold(0usize, |count, key| {
                count.checked_add(key.key.trust_requirements.len())
            })
            .ok_or(VerifyError::Malformed)?;
        let mut requirements = Vec::with_capacity(requirement_count);
        requirements.extend(self.trust_requirements().cloned());
        Ok(TrustEvidence {
            environment: certificates.birth.birth.environment.clone(),
            account: certificates.birth.birth.account_id.clone(),
            environment_issuers: self.environment_issuers.clone(),
            issuers: self.issuers.clone(),
            ids,
            keys,
            next_keys,
            not_before: self.not_before,
            expires_at: self.expires_at,
            requirements,
        })
    }

    /// Project complete signed dependencies directly from the authenticated owner.
    /// No intermediate public-evidence graph or bearer copy is constructed.
    ///
    /// # Errors
    /// Fails on malformed ancestry or an overflowing allocation size.
    pub fn requirements(&self) -> Result<Vec<TrustRequirement>, VerifyError> {
        project_requirements(self.trust_view()?, self.trust_requirements())
    }
}

fn project_requirements<'a, C, N>(
    view: View<'a, C, N>,
    inherited: impl Iterator<Item = &'a TrustRequirement> + Clone,
) -> Result<Vec<TrustRequirement>, VerifyError>
where
    C: ExactSizeIterator<Item = Certificate<'a>> + Clone,
    N: Iterator<Item = &'a [u8]>,
{
    let count = inherited
        .clone()
        .count()
        .checked_add(1)
        .ok_or(VerifyError::Malformed)?;
    let mut requirements = Vec::with_capacity(count);
    requirements.push(view.requirement()?);
    requirements.extend(inherited.cloned());
    Ok(requirements)
}

impl TrustEvidence {
    /// Complete public requirements for a newly signed contribution. The root
    /// and already inherited dependencies are flattened, never recursively nested.
    ///
    /// # Errors
    /// Fails if the checked storage size overflows.
    pub fn requirements(&self) -> Result<Vec<TrustRequirement>, VerifyError> {
        project_requirements(self.view(), self.requirements.iter())
    }

    /// Register the original trust owner's publications for this authenticated
    /// account and its environment before rechecking this evidence. The owner
    /// must be the same source used by `check`; this transfers no authority.
    pub fn changed(
        &self,
        source: &(impl crate::trust::ChangeSource + ?Sized),
    ) -> crate::trust::Changed {
        if self.requirements.is_empty() {
            return source.changed_for(&self.account);
        }
        let registrations = std::iter::once(source.changed_for(&self.account))
            .chain(
                self.requirements
                    .iter()
                    .map(|requirement| source.changed_for(&requirement.account)),
            )
            .collect::<Vec<_>>();
        Box::pin(async move { futures::future::select_all(registrations).await.0 })
    }

    /// The same-key admission preservation check without retaining the parent's
    /// bearer, append key or policy graph. See `VerifiedCapability::preserved_by`.
    pub fn preserved_by(&self, descendant: &VerifiedCapability) -> Result<bool, VerifyError> {
        let mut inherited = descendant.trust_requirements();
        Ok(self.view().preserved_by(descendant)?
            && self
                .requirements
                .iter()
                .all(|requirement| inherited.by_ref().any(|actual| actual == requirement)))
    }

    fn view(
        &self,
    ) -> View<'_, impl ExactSizeIterator<Item = Certificate<'_>> + Clone, impl Iterator<Item = &[u8]>>
    {
        View {
            environment: &self.environment,
            account: &self.account,
            environment_issuers: Ids::Fixed(&self.environment_issuers),
            issuers: Ids::Fixed(&self.issuers),
            ids: Ids::Fixed(&self.ids),
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
        self.view().check(trust, now_unix_seconds)?;
        for requirement in &self.requirements {
            check_requirement(requirement, trust, now_unix_seconds)?;
        }
        Ok(())
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
            .and_then(|size| {
                size.checked_add(slots::<BlockId>(self.environment_issuers.capacity())?)
            })
            .and_then(|size| size.checked_add(slots::<BlockId>(self.ids.capacity())?));
        let base = base.and_then(|base| {
            base.checked_add(slots::<TrustRequirement>(self.requirements.capacity())?)
        });
        let base = bytes(base.and_then(|base| {
            self.keys
                .iter()
                .try_fold(base, |size, key| size.checked_add(key.id.capacity()))
        }))?;
        self.requirements
            .iter()
            .flat_map(|requirement| requirement_allocations(requirement, true))
            .try_fold(base, u64::checked_add)
            .ok_or(VerifyError::Malformed)
    }
}

pub(super) fn check(
    capability: &VerifiedCapability,
    trust: &(impl Trust + ?Sized),
    now: u64,
) -> Result<(), VerifyError> {
    capability.trust_view()?.check(trust, now)?;
    for requirement in capability.trust_requirements() {
        check_requirement(requirement, trust, now)?;
    }
    Ok(())
}

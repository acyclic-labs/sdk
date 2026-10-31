//! One owner's latest immutable certificate authentication, never a decision.
use super::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// One owner's latest immutable certificate authentication. Every verification
/// still authenticates changed workload bytes and checks current trust,
/// revocations and time. Exact public signed ancestry shares bounded evidence.
#[derive(Default)]
pub struct CertificateVerifier {
    latest: Option<Arc<CertifiedRoots>>,
    pub(super) workloads: signatures::WorkloadAuthentication,
}

pub(super) struct CertifiedRoots {
    digest: [u8; 32],
    pub(super) environment: BlockId,
    pub(super) issuers: Vec<BlockId>,
}

impl CertificateVerifier {
    /// Verifies a workload while retaining its exact public certificate evidence.
    /// No authorization decision or workload append key is retained here.
    ///
    /// # Errors
    /// Rejects malformed, forged, revoked, expired or wrong-trust capabilities.
    pub fn verify(
        &mut self,
        bytes: &[u8],
        trust: &(impl Trust + ?Sized),
        timestamp: u64,
    ) -> Result<VerifiedCapability, VerifyError> {
        acyclic_observability::operation!("acyclic.auth.verify", VerifyError::kind, || {
            verify_certified(bytes, trust, timestamp, self)
        })
    }

    pub(super) fn authenticate(
        &mut self,
        certificates: &CertificateChainProto,
        trust: &(impl Trust + ?Sized),
        timestamp: u64,
    ) -> Result<Arc<CertifiedRoots>, VerifyError> {
        let birth = certificates.birth.as_ref().ok_or(VerifyError::Malformed)?;
        let claims = birth.birth.as_ref().ok_or(VerifyError::Malformed)?;
        if claims.issued_at_unix_seconds > timestamp {
            return Err(VerifyError::Time);
        }
        let environments = trust.environment_keys(&claims.environment)?;
        let account = trust.account(&claims.account_id)?;
        let digest = Sha256::digest(certificates.encode_to_vec()).into();
        if let Some(roots) = self.latest.as_ref()
            && roots.digest == digest
        {
            if !environments.contains(&roots.environment)
                || !roots
                    .issuers
                    .iter()
                    .all(|key| account.issuer_keys.contains(key))
            {
                return Err(VerifyError::Trust);
            }
            return Ok(roots.clone());
        }
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
        let mut issuers = Vec::with_capacity(certificates.keys.len());
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
        }
        let roots = Arc::new(CertifiedRoots {
            digest,
            environment,
            issuers,
        });
        self.latest = Some(roots.clone());
        Ok(roots)
    }

    // Reconstruct from CURRENT trust even on a certification hit: newly trusted
    // issuer keys must still be unavailable as workload append keys.
    pub(super) fn signing_keys(
        &self,
        certificates: &CertificateChainProto,
        trust: &(impl Trust + ?Sized),
    ) -> Result<HashSet<BlockId>, VerifyError> {
        let birth = certificates
            .birth
            .as_ref()
            .and_then(|birth| birth.birth.as_ref())
            .ok_or(VerifyError::Malformed)?;
        let mut seen = HashSet::new();
        for key in trust.environment_keys(&birth.environment)? {
            seen.insert(public_key(key)?.to_bytes());
        }
        seen.insert(public_key(&birth.genesis_public_key)?.to_bytes());
        for key in trust.account(&birth.account_id)?.issuer_keys {
            seen.insert(public_key(key)?.to_bytes());
        }
        for certificate in &certificates.keys {
            let key = certificate.key.as_ref().ok_or(VerifyError::Malformed)?;
            seen.insert(public_key(&key.public_key)?.to_bytes());
        }
        Ok(seen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trust::Snapshot;

    #[test]
    fn a_certified_signer_owns_authentication_but_rechecks_every_mint() -> Result<(), VerifyError> {
        let (certificates, trust) = fixture(1);
        let make = || {
            CertifiedSigner::new(
                certificates.clone().try_into()?,
                SigningKey::from_bytes(&[3; 32]),
            )
        };
        let owner = make()?;
        let mint = |owner: &CertifiedSigner, trust: &Snapshot, time| {
            owner.mint(Vec::new(), SigningKey::from_bytes(&[4; 32]), trust, time)
        };
        let count = crate::crypto::signature_verifications;
        let before = count();
        mint(&owner, &trust, 100)?;
        assert_eq!(count() - before, 3);
        let before = count();
        mint(&owner, &trust, 101)?;
        assert_eq!(
            count() - before,
            0,
            "the exact block and its public ancestry are already authenticated"
        );
        // Execute the former fresh-owner path as the physical-work control.
        let before = count();
        mint(&make()?, &trust, 101)?;
        assert_eq!(count() - before, 3);
        for field in 0..4 {
            let mut changed = trust.clone();
            match field {
                0 => changed.environment_keys.clear(),
                1 => changed.issuer_keys.clear(),
                2 => changed.minimum_key_epoch = 2,
                _ => {
                    changed.revoked_key_ids.insert("key".into());
                }
            }
            assert!(mint(&owner, &changed, 101).is_err());
        }
        assert!(mint(&owner, &trust, 0).is_err());
        assert!(mint(&owner, &trust, 1000).is_err());
        mint(&owner, &trust, 101)?;
        Ok(())
    }

    fn fixture(timestamp: u64) -> (CertificateChainProto, Snapshot) {
        let environment = SigningKey::from_bytes(&[1; 32]);
        let issuer = SigningKey::from_bytes(&[2; 32]);
        let birth = Birth {
            account_id: "account".into(),
            environment: "test".into(),
            genesis_public_key: issuer.verifying_key().to_bytes().to_vec(),
            issued_at_unix_seconds: timestamp,
        };
        let certified = CertifiedKey {
            account_id: "account".into(),
            key_id: "key".into(),
            public_key: SigningKey::from_bytes(&[3; 32])
                .verifying_key()
                .to_bytes()
                .to_vec(),
            key_epoch: 1,
            validity: Validity {
                not_before_unix_seconds: 1,
                expires_at_unix_seconds: 1000,
            },
            grants: GrantSet { grants: vec![] },
            principal: Principal {
                kind: PrincipalKind::Service,
                id: "runtime".into(),
                lineage: vec![],
                groups: vec![],
                roles: vec![],
                attributes: vec![],
            },
            parent_key_id: None,
            parent_revocation_ids: vec![],
        };
        let mut message = Vec::new();
        // These two messages only encode fixture-owned bounded protobuf values.
        message.extend_from_slice(BIRTH_DOMAIN);
        message.extend_from_slice(&BirthProto::from(birth.clone()).encode_to_vec());
        let birth_signature = environment.sign(&message).to_bytes().to_vec();
        message.clear();
        message.extend_from_slice(KEY_DOMAIN);
        message.extend_from_slice(&CertifiedKeyProto::from(certified.clone()).encode_to_vec());
        let key_signature = issuer.sign(&message).to_bytes().to_vec();
        (
            CertificateChainProto::from(CertificateChain {
                birth: BirthCertificate {
                    birth,
                    signature: birth_signature,
                },
                keys: vec![KeyCertificate {
                    key: certified,
                    signature: key_signature,
                }],
            }),
            Snapshot {
                environment: "test".into(),
                environment_keys: vec![environment.verifying_key().to_bytes()],
                account: "account".into(),
                issuer_keys: vec![issuer.verifying_key().to_bytes()],
                minimum_key_epoch: 1,
                revoked_key_ids: Default::default(),
                revoked_block_ids: Default::default(),
            },
        )
    }

    #[test]
    fn sibling_admissions_share_only_authenticated_public_ancestry() -> Result<(), VerifyError> {
        let (certificates, trust) = fixture(1);
        let signer =
            CertifiedSigner::new(certificates.try_into()?, SigningKey::from_bytes(&[3; 32]))?;
        let root = signer.mint(Vec::new(), SigningKey::from_bytes(&[4; 32]), &trust, 100)?;
        let parent = root.attenuate(Vec::new(), &[5; 32])?;
        let first = parent.attenuate(Vec::new(), &[6; 32])?.seal()?;
        let second = parent.attenuate(Vec::new(), &[7; 32])?.seal()?;
        let mut owner = CertificateVerifier::default();
        owner.verify(&first.encode(), &trust, 100)?;
        let count = crate::crypto::signature_verifications;
        let before = count();
        owner.verify(&second.encode(), &trust, 100)?;
        assert_eq!(count() - before, 2, "new sibling block and sealing proof");
        let before = count();
        owner.verify(&second.encode(), &trust, 100)?;
        assert_eq!(count() - before, 1, "the proof is checked on every use");
        // Execute the former complete-block verification, retaining the same
        // certificate owner so the control isolates workload ancestry alone.
        owner.workloads = Default::default();
        let before = count();
        owner.verify(&first.encode(), &trust, 100)?;
        assert_eq!(count() - before, 4, "all three blocks plus the seal");
        owner.verify(&root.encode(), &trust, 100)?;
        owner.verify(&second.encode(), &trust, 100)?;
        Ok(())
    }

    #[test]
    fn authenticated_ancestry_cannot_hide_a_forged_ancestor_behind_a_valid_tip()
    -> Result<(), VerifyError> {
        let (certificates, trust) = fixture(1);
        let signer =
            CertifiedSigner::new(certificates.try_into()?, SigningKey::from_bytes(&[3; 32]))?;
        let root = signer.mint(Vec::new(), SigningKey::from_bytes(&[4; 32]), &trust, 100)?;
        let child = root.attenuate(Vec::new(), &[5; 32])?;
        let bytes = child.encode();
        let mut owner = CertificateVerifier::default();
        owner.verify(&bytes, &trust, 100)?;
        for index in 0..child.block_ids().len() {
            let mut forged = wire::decode(&bytes)?;
            let block = forged.blocks.get_mut(index).ok_or(VerifyError::Malformed)?;
            *block.signature.first_mut().ok_or(VerifyError::Malformed)? ^= 1;
            let wire::Proof::Secret(tip) = forged.proof else {
                return Err(VerifyError::Malformed);
            };
            // The final private-key proof remains valid. A stale seal must
            // not accidentally be the reason this forgery is refused.
            assert!(
                owner
                    .verify(
                        &wire::encode(&forged.blocks, 18, &tip, VERSION),
                        &trust,
                        100
                    )
                    .is_err(),
                "ancestor signature was skipped behind a valid final tip"
            );
        }
        owner.verify(&bytes, &trust, 100)?;
        Ok(())
    }

    #[test]
    fn authenticated_ancestry_rejects_changed_bytes_and_current_trust() -> Result<(), VerifyError> {
        let (certificates, trust) = fixture(1);
        let signer =
            CertifiedSigner::new(certificates.try_into()?, SigningKey::from_bytes(&[3; 32]))?;
        let root = signer.mint(Vec::new(), SigningKey::from_bytes(&[4; 32]), &trust, 100)?;
        let child = root.attenuate(Vec::new(), &[5; 32])?.seal()?;
        let bytes = child.encode();
        let mut owner = CertificateVerifier::default();
        owner.verify(&bytes, &trust, 100)?;
        for index in 0..child.block_ids().len() {
            for payload in [false, true] {
                let mut forged = wire::decode(&bytes)?;
                let block = forged.blocks.get_mut(index).ok_or(VerifyError::Malformed)?;
                let value = if payload {
                    &mut block.payload
                } else {
                    &mut block.signature
                };
                *value.last_mut().ok_or(VerifyError::Malformed)? ^= 1;
                let wire::Proof::Sealed(proof) = forged.proof else {
                    return Err(VerifyError::Malformed);
                };
                assert!(
                    owner
                        .verify(
                            &wire::encode(&forged.blocks, 26, &proof, VERSION),
                            &trust,
                            100
                        )
                        .is_err()
                );
            }
        }
        let mut forged = wire::decode(&bytes)?;
        let wire::Proof::Sealed(ref mut proof) = forged.proof else {
            return Err(VerifyError::Malformed);
        };
        *proof.first_mut().ok_or(VerifyError::Malformed)? ^= 1;
        assert!(
            owner
                .verify(
                    &wire::encode(&forged.blocks, 26, proof, VERSION),
                    &trust,
                    100
                )
                .is_err()
        );
        for id in child.block_ids() {
            let mut revoked = trust.clone();
            revoked.revoked_block_ids.insert(*id);
            assert!(owner.verify(&bytes, &revoked, 100).is_err());
        }
        let mut expanded = trust.clone();
        expanded
            .environment_keys
            .push(SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes());
        assert_eq!(
            owner.verify(&bytes, &expanded, 100).err(),
            Some(VerifyError::RepeatedKey)
        );
        for change in 0..4 {
            let mut changed = trust.clone();
            match change {
                0 => changed.environment_keys.clear(),
                1 => changed.issuer_keys.clear(),
                2 => changed.minimum_key_epoch = 2,
                _ => {
                    changed.revoked_key_ids.insert("key".into());
                }
            }
            assert!(owner.verify(&bytes, &changed, 100).is_err());
        }
        assert!(owner.verify(&bytes, &trust, 0).is_err());
        assert!(owner.verify(&bytes, &trust, 1000).is_err());
        owner.verify(&bytes, &trust, 100)?;
        Ok(())
    }

    #[test]
    fn exact_certifications_have_one_owner_and_changed_signatures_or_trust_are_refused()
    -> Result<(), VerifyError> {
        let (certificates, trust) = fixture(1);
        let mut owner = CertificateVerifier::default();
        let first = owner.authenticate(&certificates, &trust, 100)?;
        let second = owner.authenticate(&certificates, &trust, 100)?;
        assert!(Arc::ptr_eq(&first, &second));
        let duplicate = CertificateVerifier::default().authenticate(&certificates, &trust, 100)?;
        assert!(!Arc::ptr_eq(&first, &duplicate));
        for birth in [false, true] {
            let mut forged = certificates.clone();
            let signature = if birth {
                &mut forged
                    .birth
                    .as_mut()
                    .ok_or(VerifyError::Malformed)?
                    .signature
            } else {
                &mut forged
                    .keys
                    .first_mut()
                    .ok_or(VerifyError::Malformed)?
                    .signature
            };
            *signature.first_mut().ok_or(VerifyError::Malformed)? ^= 1;
            assert!(owner.authenticate(&forged, &trust, 100).is_err());
        }
        let mut foreign = trust.clone();
        foreign.environment_keys.clear();
        assert!(owner.authenticate(&certificates, &foreign, 100).is_err());
        foreign = trust.clone();
        foreign.issuer_keys.clear();
        assert!(owner.authenticate(&certificates, &foreign, 100).is_err());
        assert!(owner.authenticate(&certificates, &trust, 0).is_err());
        Ok(())
    }

    #[test]
    fn certification_replacement_drops_the_previous_owner() -> Result<(), VerifyError> {
        let (first_bytes, trust) = fixture(1);
        let (next_bytes, _) = fixture(2);
        let mut owner = CertificateVerifier::default();
        let first = owner.authenticate(&first_bytes, &trust, 100)?;
        let previous = Arc::downgrade(&first);
        owner.authenticate(&next_bytes, &trust, 100)?;
        drop(first);
        assert!(previous.upgrade().is_none());
        Ok(())
    }
}

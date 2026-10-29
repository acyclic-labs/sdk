//! Compact evidence preserves current trust checks without retaining authority.
use super::signed;
use acyclic_auth::{
    contract::*,
    verify::{self, VerifiedCapability},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn guest_evidence_has_no_append_authority_but_retains_current_revocation() -> TestResult {
    for sealed in [false, true] {
        let parent = fixture(sealed)?;
        let mut encoded = None;
        let allocations = acyclic_test_support::measure(|| {
            encoded = Some(parent.encode_sealed());
        });
        let encoded = encoded.ok_or("export did not run")??;
        // Only the output buffer allocates; signing has a fixed stack transcript.
        assert_eq!(allocations.count_total, 1);
        let evidence = verify::verify(&encoded, &signed::Snapshot::default(), signed::NOW)?;
        assert!(evidence.is_sealed());
        assert_eq!(evidence.block_ids(), parent.block_ids());
        assert!(parent.preserved_by(&evidence)?);
        assert!(matches!(
            evidence.attenuate(Vec::new(), &[7; 32]),
            Err(verify::VerifyError::Sealed)
        ));
        // The host's original append authority is not consumed by the export.
        if !sealed {
            assert!(parent.attenuate(Vec::new(), &[7; 32]).is_ok());
        }
        for id in parent.revocation_ids() {
            let mut trust = signed::Snapshot::default();
            trust.revoked_blocks.push(id.try_into()?);
            assert!(matches!(
                evidence.check(&trust, signed::NOW),
                Err(verify::VerifyError::Revoked)
            ));
        }
    }
    Ok(())
}

#[test]
fn same_key_descendants_preserve_every_admission_dependency_without_allocating() -> TestResult {
    let parent = fixture(false)?;
    let evidence = parent.trust_evidence()?;
    let envelope = acyclic_auth::authority::decode_capability(&parent.encode())?;
    let root = acyclic_auth::authority::decode_block(
        &envelope.blocks.first().ok_or("missing root")?.payload,
    )?;
    let certificates = root.certificates.ok_or("missing certificates")?;
    let ids = parent
        .revocation_ids()
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let caller = parent.caller_authority(b"customers")?;
    assert_eq!(caller.principal.account, parent.account_id()?.as_bytes());
    assert_eq!(caller.principal.ns, b"customers");
    let principal = parent.effective().principal();
    assert_eq!(
        caller.principal.id,
        principal.lineage.last().unwrap_or(&principal.id).as_bytes()
    );
    assert_eq!(
        caller.expires_at_unix_seconds,
        parent.effective().expires_at_unix_seconds()
    );
    assert_eq!(caller.revocation.blocks, ids);
    assert_eq!(
        caller.revocation.keys,
        certificates
            .keys
            .iter()
            .map(|certificate| CallerKeyRevocation {
                key_id: certificate.key.key_id.clone(),
                key_epoch: certificate.key.key_epoch,
            })
            .collect::<Vec<_>>()
    );
    assert!(parent.caller_authority(b"").is_err());
    for changed in 0..6 {
        let mut certificates = certificates.clone();
        for certificate in &mut certificates.keys {
            certificate.key.parent_revocation_ids.clear();
        }
        let leaf = &mut certificates.keys.last_mut().ok_or("missing leaf")?.key;
        leaf.parent_revocation_ids = ids.clone();
        match changed {
            1 => leaf.key_id = "different-leaf".into(),
            2 => leaf.key_epoch += 1,
            3 => {
                leaf.parent_revocation_ids.remove(1);
            }
            _ => {}
        }
        let wire = signed::root(|block| {
            block.certificates = Some(certificates.clone());
            block.caveats.extend([
                Caveat {
                    kind: CaveatKind::NotBeforeUnixSeconds(if changed == 5 { 0 } else { 50 }),
                },
                Caveat {
                    kind: CaveatKind::ExpiresAtUnixSeconds(if changed == 4 { 300 } else { 200 }),
                },
            ]);
        });
        // Every candidate is genuinely signed and valid now, including each
        // negative control. Current validity alone cannot establish inheritance.
        let descendant = verify::verify(&wire, &signed::Snapshot::default(), signed::NOW)?;
        let mut result = None;
        let allocations = acyclic_test_support::measure(|| {
            result = Some((
                parent.preserved_by(&descendant),
                evidence.preserved_by(&descendant),
            ));
        });
        let (full, compact) = result.ok_or("comparison did not run")?;
        assert_eq!(full?, changed == 0, "full dependency projection: {changed}");
        assert_eq!(
            compact?,
            changed == 0,
            "compact dependency projection: {changed}"
        );
        assert_eq!(allocations.count_total, 0);
    }
    let narrowed = parent
        .attenuate(
            vec![Caveat {
                kind: CaveatKind::ExpiresAtUnixSeconds(150),
            }],
            &[7; 32],
        )?
        .seal()?;
    assert!(parent.preserved_by(&narrowed)?);
    assert!(evidence.preserved_by(&narrowed)?);
    Ok(())
}

fn fixture(sealed: bool) -> Result<VerifiedCapability, verify::VerifyError> {
    let mut prepared = false;
    let wire = signed::root(|block| {
        let Some(certificates) = block.certificates.as_mut() else {
            return;
        };
        let Some(leaf) = certificates.keys.first_mut() else {
            return;
        };
        let mut ancestor = leaf.clone();
        ancestor.key.key_id = "ancestor".into();
        ancestor.key.public_key = signed::key(6).verifying_key().to_bytes().to_vec();
        ancestor.key.parent_revocation_ids = vec![vec![121; 32]];
        leaf.key.parent_key_id = Some("ancestor".into());
        leaf.key.parent_revocation_ids = vec![vec![121; 32], vec![122; 32]];
        certificates.keys.insert(0, ancestor);
        prepared = true;
    });
    if !prepared {
        return Err(verify::VerifyError::Malformed);
    }
    let capability = verify::verify(&wire, &signed::Snapshot::default(), signed::NOW)?.attenuate(
        vec![
            Caveat {
                kind: CaveatKind::NotBeforeUnixSeconds(50),
            },
            Caveat {
                kind: CaveatKind::ExpiresAtUnixSeconds(200),
            },
        ],
        &[5; 32],
    )?;
    if sealed {
        capability.seal()
    } else {
        Ok(capability)
    }
}

#[test]
fn compact_trust_matches_full_authentication_for_every_snapshot_change() -> TestResult {
    for sealed in [false, true] {
        let capability = fixture(sealed)?;
        let wire = capability.encode();
        let ids = capability.block_ids().to_vec();
        let [first, last] = ids.as_slice() else {
            return Err("fixture must authenticate exactly two blocks".into());
        };
        let evidence =
            verify::verify(&wire, &signed::Snapshot::default(), signed::NOW)?.trust_evidence()?;
        for (ancestor, (key, parent, next_seed)) in
            [("ancestor", [121; 32], 4), ("key", [122; 32], 5)]
                .into_iter()
                .enumerate()
        {
            for mask in 0..1024 {
                let mut trust = signed::Snapshot::default();
                if mask & 1 != 0 {
                    trust.available = false;
                }
                if mask & 2 != 0 {
                    trust.environments.clear();
                }
                if mask & 4 != 0 {
                    trust.issuers.clear();
                }
                if mask & 8 != 0 {
                    trust.minimum_epoch = u64::MAX;
                }
                if mask & 16 != 0 {
                    trust.revoked_keys.push(key.into());
                }
                if mask & 32 != 0 {
                    trust.revoked_blocks.push(parent);
                }
                if mask & 64 != 0 {
                    trust.revoked_blocks.push(*first);
                }
                if mask & 128 != 0 {
                    trust.revoked_blocks.push(*last);
                }
                let next = signed::key(next_seed).verifying_key().to_bytes();
                if mask & 256 != 0 {
                    trust.environments.push(next);
                }
                if mask & 512 != 0 {
                    trust.issuers.push(next);
                }
                for now in [0, 49, 50, 100, 199, 200, 1000] {
                    let expected = mask == 0 && (50..200).contains(&now);
                    assert_eq!(
                        evidence.check(&trust, now).is_ok(),
                        expected,
                        "compact trust diverged: sealed={sealed} ancestor={ancestor} mask={mask} now={now}"
                    );
                    assert_eq!(capability.check(&trust, now).is_ok(), expected);
                    assert_eq!(
                        verify::verify(&wire, &trust, now).is_ok(),
                        expected,
                        "full authentication diverged: sealed={sealed} ancestor={ancestor} mask={mask} now={now}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn compact_trust_storage_counts_actual_allocations_and_every_owned_buffer() -> TestResult {
    for sealed in [false, true] {
        let capability = fixture(sealed)?;
        let storage = capability.trust_evidence_storage()?;
        let mut converted = None;
        let allocation = acyclic_test_support::measure(|| {
            converted = Some(capability.trust_evidence());
        });
        let evidence = converted.ok_or("conversion did not run")??;
        assert_eq!(
            allocation.count_total,
            storage.allocation_bytes().count() as u64,
            "conversion added unplanned allocations"
        );
        assert_eq!(
            allocation.bytes_total,
            storage.allocation_bytes().sum::<u64>()
        );
        // The former three-vector preflight omits copied certificate names
        // and issuer storage when the authenticated capability has an owner.
        assert!(allocation.count_total > 3);
        assert!(allocation.bytes_total > storage.allocation_bytes().take(3).sum::<u64>());
        let reported_owned = evidence.owned_bytes()?;
        let cloned = evidence.clone();
        for evidence in [evidence, cloned] {
            let owned = evidence.owned_bytes()?;
            let released = acyclic_test_support::measure(|| drop(evidence));
            assert_eq!(
                u64::try_from(-released.bytes_current)?,
                owned,
                "compact evidence omitted retained heap storage"
            );
        }
        assert_eq!(reported_owned, storage.owned_bytes);
    }
    Ok(())
}

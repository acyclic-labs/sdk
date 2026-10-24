//! Compact evidence preserves current trust checks without retaining authority.
use super::signed;
use acyclic_auth::{
    contract::*,
    verify::{self, VerifiedCapability},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

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
        let evidence = verify::verify(&wire, &signed::Snapshot::default(), signed::NOW)?
            .into_trust_evidence()?;
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
            converted = Some(capability.into_trust_evidence());
        });
        let evidence = converted.ok_or("conversion did not run")??;
        assert_eq!(
            allocation.count_total, 3,
            "conversion added unplanned allocations"
        );
        assert_eq!(
            allocation.bytes_total,
            storage.allocation_bytes.into_iter().sum::<u64>()
        );
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

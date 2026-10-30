//! Follower proofs: swap-then-wake, fence on a lost follow, anti-rollback, and
//! a deterministic simulation of lossy delivery with a negative control.
use super::*;
use crate::trust::{AccountRoot, Certification, Issuer};
use crate::verify::{self, CertifiedSigner};
use ed25519_dalek::SigningKey;
use futures::FutureExt;

const NOW: u64 = 1_000;

fn key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}

struct World {
    issuer: Issuer,
    root: AccountRoot,
    genesis: Vec<Vec<u8>>,
    signer: CertifiedSigner,
}

fn world() -> World {
    let issuer = Issuer::new("test", key(1));
    let (root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let leaf = key(3);
    let chain = issuer
        .certify(
            &root,
            Certification {
                key_id: "runtime",
                public_key: &leaf.verifying_key(),
                validity: Validity {
                    not_before_unix_seconds: NOW,
                    expires_at_unix_seconds: NOW + 10_000,
                },
                grants: GrantSet {
                    grants: vec![Grant {
                        kind: GrantKind::Resource(ResourceGrant {
                            resource: ResourcePattern {
                                name: "streams:/a".into(),
                                prefix: true,
                            },
                            actions: vec!["streams/read".into()],
                        }),
                    }],
                },
                principal: Principal {
                    kind: PrincipalKind::Service,
                    id: "runtime".into(),
                    lineage: vec![],
                    groups: vec![],
                    roles: vec![],
                    attributes: vec![],
                },
            },
        )
        .unwrap();
    World {
        issuer,
        root,
        genesis,
        signer: CertifiedSigner::new(chain, leaf).unwrap(),
    }
}

fn follower(world: &World) -> Follower {
    let follower = Follower::new("test", world.issuer.anchor());
    for record in &world.genesis {
        assert_eq!(follower.apply(record, NOW), Ok(Applied::Changed));
    }
    follower.resume();
    follower.serve("account");
    follower
}

/// Credential `index`: its bytes and its root block id.
fn credential(world: &World, trust: &Follower, index: u8) -> (Vec<u8>, BlockId) {
    let capability = world
        .signer
        .mint(vec![], key(100 + index), trust, NOW)
        .unwrap();
    (capability.encode().to_vec(), capability.block_ids()[0])
}

#[test]
fn revocation_refuses_the_next_verify() {
    let mut world = world();
    let trust = follower(&world);
    let (bytes, id) = credential(&world, &trust, 0);
    let record = world.root.revoke(&[(id, NOW + 10_000)], NOW);
    // Negative control: before the record applies, the credential verifies.
    assert!(verify::verify(&bytes, &trust, NOW).is_ok());
    assert_eq!(trust.apply(&record, NOW), Ok(Applied::Changed));
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Revoked)
    );
    // Redelivery is a duplicate and changes nothing.
    assert_eq!(trust.apply(&record, NOW), Ok(Applied::Duplicate));
}

#[test]
fn swap_precedes_wake_and_lost_follow_fences() {
    let mut world = world();
    let trust = follower(&world);
    let (bytes, id) = credential(&world, &trust, 0);
    let mut wake = trust.changed();
    assert!((&mut wake).now_or_never().is_none());
    trust
        .apply(&world.root.revoke(&[(id, NOW + 10_000)], NOW), NOW)
        .unwrap();
    // The wake is ready and the swapped trust is already visible.
    assert_eq!(wake.now_or_never(), Some(Ok(())));
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Revoked)
    );

    let (other, _) = credential(&world, &trust, 1);
    let closed = trust.changed();
    trust.fence();
    assert_eq!(closed.now_or_never(), Some(Err(SourceClosed)));
    assert_eq!(trust.check_open(), Err(SourceClosed));
    assert_eq!(trust.changed().now_or_never(), Some(Err(SourceClosed)));
    assert_eq!(
        verify::verify(&other, &trust, NOW).err(),
        Some(VerifyError::Unavailable)
    );
    trust.resume();
    // Still fenced until the account's Stream is caught up again.
    assert_eq!(
        verify::verify(&other, &trust, NOW).err(),
        Some(VerifyError::Trust)
    );
    trust.serve("account");
    assert!(verify::verify(&other, &trust, NOW).is_ok());
}

#[test]
fn rollback_and_gaps_are_refused_and_fence_the_account() {
    let mut world = world();
    let trust = follower(&world);
    let (bytes, id) = credential(&world, &trust, 0);
    // A gap: delta 3 without delta 2.
    let _skipped = world.root.revoke(&[([7; 32], NOW + 1)], NOW);
    let gapped = world.root.revoke(&[(id, NOW + 1)], NOW);
    assert_eq!(trust.apply(&gapped, NOW), Err(VerifyError::Trust));
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Trust)
    );

    // An epoch floor that goes backwards is refused.
    let mut world = self::world();
    let trust = follower(&world);
    world.root.set_minimum_key_epoch(5);
    trust
        .apply(&world.issuer.keyring(&mut world.root, NOW).unwrap(), NOW)
        .unwrap();
    world.root.set_minimum_key_epoch(4);
    let lowered = world.issuer.keyring(&mut world.root, NOW).unwrap();
    assert_eq!(trust.apply(&lowered, NOW), Err(VerifyError::Trust));

    // A publication time that goes backwards is refused.
    let mut world = self::world();
    let trust = follower(&world);
    let earlier = world.issuer.keyring(&mut world.root, NOW - 1).unwrap();
    assert_eq!(trust.apply(&earlier, NOW), Err(VerifyError::Trust));

    // A future-dated record is refused; protected time never goes backwards.
    let mut world = self::world();
    let trust = follower(&world);
    let future = world.issuer.keyring(&mut world.root, NOW + 5).unwrap();
    assert_eq!(trust.apply(&future, NOW), Err(VerifyError::Time));
    assert_eq!(trust.protected_now(NOW - 500), NOW);
    assert_eq!(trust.protected_now(NOW + 7), NOW + 7);
    assert_eq!(trust.protected_now(NOW), NOW + 7);
}

#[test]
fn forged_records_are_refused() {
    let world = world();
    let trust = Follower::new("test", world.issuer.anchor());
    let forger = Issuer::new("test", key(9));
    let (_, forged) = forger.genesis("account", key(2), NOW).unwrap();
    assert_eq!(trust.apply(&forged[0], NOW), Err(VerifyError::Signature));
    assert_eq!(trust.apply(b"\xff", NOW), Err(VerifyError::Malformed));
}

#[test]
fn forwarding_appends_one_audience_block_offline() {
    let world = world();
    let trust = follower(&world);
    let capability = world.signer.mint(vec![], key(100), &trust, NOW).unwrap();
    let hop = capability.forward("streams:/a/b", &[50; 32]).unwrap();
    let forwarded = verify::verify(&hop, &trust, NOW).unwrap();
    assert!(forwarded.is_sealed());
    assert_eq!(forwarded.block_ids().len(), 2);
    assert_eq!(forwarded.block_ids()[0], capability.block_ids()[0]);
    // The holder keeps its append authority for the next hop; a sealed hop
    // cannot be forwarded again.
    assert!(capability.forward("streams:/a/c", &[51; 32]).is_ok());
    assert_eq!(
        forwarded.forward("streams:/a/c", &[52; 32]).err(),
        Some(VerifyError::Sealed)
    );
}

#[test]
fn allowances_verify_offline_against_environment_keys() {
    let world = world();
    let trust = follower(&world);
    let allowance = world.issuer.allowance("account", 500, NOW + 100).unwrap();
    let capability = world
        .signer
        .mint(
            vec![Caveat {
                kind: CaveatKind::Allowance(allowance.clone()),
            }],
            key(100),
            &trust,
            NOW,
        )
        .unwrap();
    let verified = verify::verify(&capability.encode(), &trust, NOW).unwrap();
    assert_eq!(verified.effective().expires_at_unix_seconds(), NOW + 100);
    // Negative controls: a forged signature and another account refuse.
    let mut forged = allowance.clone();
    forged.allowance.spend_micros = 5_000;
    let foreign = world.issuer.allowance("other", 500, NOW + 100).unwrap();
    for caveat in [forged, foreign] {
        let minted = world.signer.mint(
            vec![Caveat {
                kind: CaveatKind::Allowance(caveat),
            }],
            key(101),
            &trust,
            NOW,
        );
        assert!(minted.is_err());
    }
}

/// Deterministic xorshift; no dependency, same schedule on every platform.
struct Schedule(u64);
impl Schedule {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

/// One simulated follow over a lossy transport. `eager` is the negative
/// control: it resumes on reconnect before catching up to the observed tail.
fn simulate(seed: u64, eager: bool) -> Result<(), String> {
    let mut world = world();
    let trust = Follower::new("test", world.issuer.anchor());
    let mut stream = world.genesis.clone();
    let credentials = (0..4)
        .map(|index| {
            let probe = follower(&world);
            credential(&world, &probe, index)
        })
        .collect::<Vec<_>>();
    // Stream index of each credential's revocation, once appended.
    let mut revoked_at = [None::<usize>; 4];
    let mut schedule = Schedule(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let (mut next, mut tail, mut connected, mut changed) = (0, 0, false, 0);
    for _ in 0..400 {
        match schedule.next(6) {
            0 => {
                let index = schedule.next(4) as usize;
                if revoked_at[index].is_none() {
                    revoked_at[index] = Some(stream.len());
                    stream.push(
                        world
                            .root
                            .revoke(&[(credentials[index].1, NOW + 10_000)], NOW),
                    );
                }
            }
            1 | 2 if connected && next < stream.len() => {
                match trust.apply(&stream[next], NOW) {
                    Ok(Applied::Changed) => changed += 1,
                    Ok(Applied::Duplicate) => {}
                    Err(error) => return Err(format!("record {next} refused: {error}")),
                }
                next += 1;
                if next >= tail {
                    trust.resume();
                    trust.serve("account");
                }
            }
            3 if connected => {
                connected = false;
                trust.fence();
            }
            4 if !connected => {
                connected = true;
                tail = stream.len();
                // A reconnect may redeliver up to two applied records.
                next = next.saturating_sub(schedule.next(3) as usize);
                if eager || next >= tail {
                    trust.resume();
                    trust.serve("account");
                }
            }
            _ => {
                let index = schedule.next(4) as usize;
                let result = verify::verify(&credentials[index].0, &trust, NOW);
                let open = trust.check_open().is_ok();
                if !open && !matches!(result, Err(VerifyError::Unavailable)) {
                    return Err("fenced trust admitted or refused as a verdict".into());
                }
                // Serving trust must include everything up to the tail
                // observed at the last (re)connect: no stale admission.
                let stale = revoked_at[index].is_some_and(|at| at < tail);
                if open && stale && result.is_ok() {
                    return Err(format!("credential {index} admitted under stale trust"));
                }
                if open && revoked_at[index].is_some_and(|at| at < next) && result.is_ok() {
                    return Err(format!(
                        "credential {index} admitted after its revocation applied"
                    ));
                }
            }
        }
    }
    // Nothing is lost: reconnect and drain; every record changed trust once.
    trust.fence();
    for record in &stream[next..] {
        if trust.apply(record, NOW) == Ok(Applied::Changed) {
            changed += 1;
        }
    }
    trust.resume();
    trust.serve("account");
    if changed != stream.len() {
        return Err(format!(
            "{changed} of {} records changed trust",
            stream.len()
        ));
    }
    for (index, at) in revoked_at.iter().enumerate() {
        if at.is_some() && verify::verify(&credentials[index].0, &trust, NOW).is_ok() {
            return Err(format!("credential {index} survived its revocation"));
        }
    }
    Ok(())
}

#[test]
fn simulated_lossy_follow_never_admits_stale_trust_and_loses_nothing() {
    for seed in 0..512 {
        simulate(seed, false).unwrap_or_else(|error| panic!("seed {seed}: {error}"));
    }
    // Negative control: resuming before catch-up admits stale trust.
    assert!(
        (0..512).any(|seed| simulate(seed, true).is_err()),
        "the checker must catch a follower that serves before catching up"
    );
}

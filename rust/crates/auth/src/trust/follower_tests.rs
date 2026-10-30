//! Follower proofs: swap-then-wake, fence on a lost follow, anti-rollback, and
//! a deterministic simulation of lossy delivery with a negative control.
use super::*;
use crate::trust::{AccountRoot, Certification, Issuer, KeyringFrontier};
use crate::verify::{self, CertifiedSigner};
use ed25519_dalek::{Signer, SigningKey};
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

fn certification(leaf: &SigningKey) -> Certification<'_> {
    Certification {
        key_id: "runtime",
        public_key: leaf.verifying_key(),
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
        parent_revocation_ids: vec![],
    }
}

fn world() -> World {
    let issuer = Issuer::new("test", key(1));
    let (root, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    let leaf = key(3);
    let chain = issuer.certify(&root, certification(&leaf)).unwrap();
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
    let record = world.root.revoke(&[(id, NOW + 10_000)], NOW).unwrap();
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
        .apply(&world.root.revoke(&[(id, NOW + 10_000)], NOW).unwrap(), NOW)
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

/// `record`, a keyring, with `edit` applied and re-signed by `signer`: a
/// validly signed record custody itself would never produce.
fn resign(record: &[u8], signer: u8, edit: impl FnOnce(&mut KeyringSnapshot)) -> Vec<u8> {
    let TrustRecordKind::Keyring(mut signed) = decode(record).unwrap().record else {
        panic!("not a keyring");
    };
    edit(&mut signed.snapshot);
    let mut message = Vec::new();
    claims_message(
        &mut message,
        KEYRING_DOMAIN,
        &KeyringSnapshotProto::from(signed.snapshot.clone()),
    )
    .unwrap();
    signed.signature = key(signer).sign(&message).to_bytes().to_vec();
    TrustRecordProto::from(TrustRecord {
        record: TrustRecordKind::Keyring(signed),
    })
    .encode_to_vec()
}

fn slices(records: &[Vec<u8>]) -> Vec<&[u8]> {
    records.iter().map(Vec::as_slice).collect()
}

#[test]
fn rollback_and_gaps_are_refused_and_fence_the_account() {
    let mut world = world();
    let trust = follower(&world);
    let (bytes, id) = credential(&world, &trust, 0);
    // A gap: delta 3 without delta 2.
    let _skipped = world.root.revoke(&[([7; 32], NOW + 1)], NOW).unwrap();
    let gapped = world.root.revoke(&[(id, NOW + 1)], NOW).unwrap();
    assert_eq!(trust.apply(&gapped, NOW), Err(VerifyError::Trust));
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Trust)
    );

    // Custody refuses to lower its own floor.
    let mut world = self::world();
    let trust = follower(&world);
    trust
        .apply(&world.root.raise_minimum_epoch(5, NOW).unwrap(), NOW)
        .unwrap();
    assert_eq!(
        world.root.raise_minimum_epoch(4, NOW).err(),
        Some(VerifyError::Trust)
    );
    let next = world.issuer.keyring(&mut world.root, NOW).unwrap();
    // A floor or a publication time that goes backwards is refused, though
    // validly signed; so is a future record. The valid record applies.
    for rolled in [
        resign(&next, 1, |keyring| keyring.minimum_key_epoch = 4),
        resign(&next, 1, |keyring| keyring.issued_at_unix_seconds = NOW - 1),
    ] {
        assert_eq!(trust.apply(&rolled, NOW), Err(VerifyError::Trust));
    }
    let future = resign(&next, 1, |keyring| keyring.issued_at_unix_seconds = NOW + 5);
    assert_eq!(trust.apply(&future, NOW), Err(VerifyError::Time));
    assert_eq!(trust.apply(&next, NOW), Ok(Applied::Changed));

    // Protected time never goes backwards.
    assert_eq!(trust.protected_now(NOW - 500), NOW);
    assert_eq!(trust.protected_now(NOW + 7), NOW + 7);
    assert_eq!(trust.protected_now(NOW), NOW + 7);
}

#[test]
fn a_root_alone_narrows_but_never_rotates() {
    let mut world = world();
    let trust = follower(&world);
    let (bytes, _) = credential(&world, &trust, 0);
    let revoked = world
        .root
        .revoke_keys(&[("runtime", NOW + 10_000)], NOW)
        .unwrap();
    // Negative control: the record is what refuses the credential.
    assert!(verify::verify(&bytes, &trust, NOW).is_ok());
    trust.apply(&revoked, NOW).unwrap();
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Revoked)
    );

    let mut world = self::world();
    let trust = follower(&world);
    let (bytes, _) = credential(&world, &trust, 0);
    let raised = world.root.raise_minimum_epoch(2, NOW).unwrap();
    assert!(verify::verify(&bytes, &trust, NOW).is_ok());
    trust.apply(&raised, NOW).unwrap();
    assert!(verify::verify(&bytes, &trust, NOW).is_err());
    // A keyring the root signs that sets another root is refused; the
    // environment may set it.
    let next = world.issuer.keyring(&mut world.root, NOW).unwrap();
    let rotate = |keyring: &mut KeyringSnapshot| {
        keyring.root_public_key = key(8).verifying_key().to_bytes().to_vec();
    };
    assert_eq!(
        trust.apply(&resign(&next, 2, rotate), NOW),
        Err(VerifyError::Signature)
    );
    assert_eq!(
        trust.apply(&resign(&next, 1, rotate), NOW),
        Ok(Applied::Changed)
    );
}

#[test]
fn replay_restores_custody_and_refuses_gaps_foreign_signers_and_rollback() {
    let mut world = world();
    let mut stream = world.genesis.clone();
    stream.push(world.root.revoke(&[([7; 32], NOW + 50)], NOW).unwrap());
    stream.push(world.root.raise_minimum_epoch(3, NOW).unwrap());
    stream.push(world.root.revoke_keys(&[("old", NOW + 50)], NOW).unwrap());
    let anchor = world.issuer.anchor();
    let replay = |records: &[Vec<u8>], root: u8| {
        AccountRoot::resume("account", key(root), &anchor, &slices(records))
    };
    // The restored custody extends the same Stream: its next record applies
    // to a follower of the original and equals the original's own.
    let mut restored = replay(&stream, 2).unwrap();
    assert_eq!(restored.minimum_key_epoch(), 3);
    let trust = follower(&world);
    for record in &stream[2..] {
        trust.apply(record, NOW).unwrap();
    }
    let next = restored.revoke(&[([8; 32], NOW + 50)], NOW).unwrap();
    assert_eq!(trust.apply(&next, NOW), Ok(Applied::Changed));
    assert_eq!(
        world.root.revoke(&[([8; 32], NOW + 50)], NOW).unwrap(),
        next
    );
    // Negative controls: a gap, a foreign root, a foreign environment, a
    // record of another account, and a rollback all refuse.
    let mut gapped = stream.clone();
    gapped.remove(2);
    assert!(replay(&gapped, 2).is_err());
    assert!(replay(&stream, 9).is_err());
    let foreign = [key(9).verifying_key().to_bytes()];
    assert!(AccountRoot::resume("account", key(2), &foreign, &slices(&stream)).is_err());
    assert!(AccountRoot::resume("other", key(2), &anchor, &slices(&stream)).is_err());
    let next = world.issuer.keyring(&mut world.root, NOW).unwrap();
    let mut rolled_back = stream.clone();
    rolled_back.push(resign(&next, 1, |keyring| keyring.minimum_key_epoch = 1));
    assert!(replay(&rolled_back, 2).is_err());
    assert!(KeyringFrontier::resume("account", &anchor, &slices(&rolled_back)).is_err());
    assert!(KeyringFrontier::resume("account", &anchor, &slices(&gapped)).is_err());
}

#[test]
fn a_public_frontier_rotates_a_root_the_issuer_never_holds() {
    let world = world();
    let anchor = world.issuer.anchor();
    let mut frontier =
        KeyringFrontier::resume("account", &anchor, &slices(&world.genesis)).unwrap();
    let customer = key(6);
    let rotated = world
        .issuer
        .keyring_for(&frontier, &customer.verifying_key(), 1, NOW)
        .unwrap();
    // Negative control: a lowered epoch is refused before signing leaves.
    assert!(
        world
            .issuer
            .keyring_for(&frontier, &customer.verifying_key(), 0, NOW)
            .is_err()
    );
    frontier.apply(&rotated, &anchor, NOW).unwrap();
    let trust = follower(&world);
    trust.apply(&rotated, NOW).unwrap();
    // The customer root alone resumes, revokes and certifies.
    let mut stream = world.genesis.clone();
    stream.push(rotated);
    let mut root = AccountRoot::resume("account", key(6), &anchor, &slices(&stream)).unwrap();
    assert_eq!(frontier.root(), Some(customer.verifying_key().to_bytes()));
    let birth = world
        .issuer
        .birth("account", &customer.verifying_key(), NOW)
        .unwrap();
    let leaf = key(3);
    let signer =
        CertifiedSigner::new(root.certify(&birth, certification(&leaf)).unwrap(), leaf).unwrap();
    let capability = signer.mint(vec![], key(100), &trust, NOW).unwrap();
    let block = capability.block_ids()[0];
    assert!(verify::verify(&capability.encode(), &trust, NOW).is_ok());
    trust
        .apply(&root.revoke(&[(block, NOW + 50)], NOW).unwrap(), NOW)
        .unwrap();
    assert_eq!(
        verify::verify(&capability.encode(), &trust, NOW).err(),
        Some(VerifyError::Revoked)
    );
    // Negative controls: a Birth of another account or another root.
    for birth in [
        world
            .issuer
            .birth("other", &customer.verifying_key(), NOW)
            .unwrap(),
        world
            .issuer
            .birth("account", &key(2).verifying_key(), NOW)
            .unwrap(),
    ] {
        assert_eq!(
            root.certify(&birth, certification(&key(3))).err(),
            Some(VerifyError::Trust)
        );
    }
}

#[test]
fn environment_rotation_replays_and_refuses_retired_signers() {
    let issuer = Issuer::new("test", key(1));
    let successor = Issuer::new("test", key(5));
    let mut frontier = EnvironmentFrontier::genesis("test", issuer.anchor());
    let first = issuer
        .environment_keyring(&mut frontier, &successor.anchor(), NOW)
        .unwrap();
    // The retired anchor can no longer sign; its successor can.
    let mut copy = frontier.clone();
    assert!(
        issuer
            .environment_keyring(&mut copy, &issuer.anchor(), NOW)
            .is_err()
    );
    let second = successor
        .environment_keyring(&mut frontier, &issuer.anchor(), NOW + 1)
        .unwrap();
    let trust = Follower::new("test", issuer.anchor());
    for record in [&first, &second] {
        assert_eq!(trust.apply(record, NOW + 1), Ok(Applied::Changed));
    }
    let restored =
        EnvironmentFrontier::resume("test", issuer.anchor(), &[&first, &second]).unwrap();
    assert_eq!(restored.frontier(), frontier.frontier());
    assert_eq!(restored.keys(), issuer.anchor().as_slice());
    // Negative controls: a gap, a foreign anchor, an account record.
    assert!(EnvironmentFrontier::resume("test", issuer.anchor(), &[&second]).is_err());
    assert!(EnvironmentFrontier::resume("test", successor.anchor(), &[&first]).is_err());
    let (_, genesis) = issuer.genesis("account", key(2), NOW).unwrap();
    assert!(EnvironmentFrontier::resume("test", issuer.anchor(), &[&genesis[0]]).is_err());
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
    let allowance = world
        .issuer
        .allowance("account", 500, NOW + 100, vec![])
        .unwrap();
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
    let foreign = world
        .issuer
        .allowance("other", 500, NOW + 100, vec![])
        .unwrap();
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
    // Stream index of each credential's revocation, and of the epoch floor
    // that refuses every credential, once appended.
    let mut revoked_at = [None::<usize>; 4];
    let mut floor_at = None::<usize>;
    let mut schedule = Schedule(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let (mut next, mut tail, mut connected, mut changed) = (0, 0, false, 0);
    for _ in 0..400 {
        match schedule.next(6) {
            0 => match schedule.next(8) {
                // An unrelated key revocation changes trust but no verdict.
                0 => stream.push(
                    world
                        .root
                        .revoke_keys(&[("retired", NOW + 50)], NOW)
                        .unwrap(),
                ),
                1 if floor_at.is_none() => {
                    floor_at = Some(stream.len());
                    stream.push(world.root.raise_minimum_epoch(2, NOW).unwrap());
                }
                _ => {
                    let index = schedule.next(4) as usize;
                    if revoked_at[index].is_none() {
                        revoked_at[index] = Some(stream.len());
                        stream.push(
                            world
                                .root
                                .revoke(&[(credentials[index].1, NOW + 10_000)], NOW)
                                .unwrap(),
                        );
                    }
                }
            },
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
                let revoked_before = |bound: usize| {
                    revoked_at[index]
                        .into_iter()
                        .chain(floor_at)
                        .any(|at| at < bound)
                };
                let stale = revoked_before(tail);
                if open && stale && result.is_ok() {
                    return Err(format!("credential {index} admitted under stale trust"));
                }
                if open && revoked_before(next) && result.is_ok() {
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
        if (at.is_some() || floor_at.is_some())
            && verify::verify(&credentials[index].0, &trust, NOW).is_ok()
        {
            return Err(format!("credential {index} survived its revocation"));
        }
    }
    // Replay restores custody exactly: its next record extends the Stream.
    let mut restored =
        AccountRoot::resume("account", key(2), &world.issuer.anchor(), &slices(&stream))
            .map_err(|error| format!("replay refused: {error}"))?;
    let next = restored
        .revoke_keys(&[("probe", NOW + 50)], NOW)
        .map_err(|error| format!("restored custody refused: {error}"))?;
    if trust.apply(&next, NOW) != Ok(Applied::Changed) {
        return Err("restored custody diverged from the Stream".into());
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

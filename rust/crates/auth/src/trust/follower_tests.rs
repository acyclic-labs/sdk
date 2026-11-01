//! Follower proofs: swap-then-wake, fence on a lost follow, anti-rollback, and
//! a deterministic simulation of lossy delivery with a negative control.
use super::*;
use crate::trust::{AccountRoot, Certification, Issuer, KeyringFrontier};
use crate::verify::{self, CertifiedSigner};
use ed25519_dalek::{Signer, SigningKey};
use futures::FutureExt;

const NOW: u64 = 1_000;

thread_local! {
    static VERIFYING_KEYRING: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static VERIFYING_ENVIRONMENT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) fn environment_verifying() {
    let interleave = VERIFYING_ENVIRONMENT.with(|slot| slot.borrow_mut().take());
    if let Some(interleave) = interleave {
        interleave();
    }
}

pub(super) fn keyring_verifying() {
    let interleave = VERIFYING_KEYRING.with(|slot| slot.borrow_mut().take());
    if let Some(interleave) = interleave {
        interleave();
    }
}

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
    let fenced = trust.changed();
    // A withdrawal must progress even while account storage is unavailable.
    // This held write lock is a negative control for the former population walk.
    let accounts = trust.accounts.write().unwrap();
    assert!(trust.accounts.try_write().is_err());
    trust.fence(FenceCause::LostFollow);
    // A fence wakes waiters to recheck; only a close is an error.
    assert_eq!(fenced.now_or_never(), Some(Ok(())));
    assert_eq!(trust.check_open(), Err(SourceClosed));
    let mut waiting = trust.changed();
    assert!((&mut waiting).now_or_never().is_none());
    assert_eq!(
        verify::verify(&other, &trust, NOW).err(),
        Some(VerifyError::Unavailable)
    );
    drop(accounts);
    trust.resume();
    // A waiter registered while fenced is woken by the resume.
    assert_eq!(waiting.now_or_never(), Some(Ok(())));
    // Still fenced until the account's Stream is caught up again.
    assert_eq!(
        verify::verify(&other, &trust, NOW).err(),
        Some(VerifyError::Trust)
    );
    trust.serve("account");
    assert!(verify::verify(&other, &trust, NOW).is_ok());

    // An account read cannot accept an old generation even if the environment
    // has resumed again before that read finishes. The original account lock
    // remains held by with_account throughout this real withdrawal.
    assert_eq!(
        trust.with_account("account", |_, _| {
            trust.fence(FenceCause::LostFollow);
            trust.resume();
        }),
        Err(VerifyError::Unavailable)
    );
    assert_eq!(
        verify::verify(&other, &trust, NOW).err(),
        Some(VerifyError::Trust)
    );
    trust.serve("account");
    assert!(verify::verify(&other, &trust, NOW).is_ok());

    // Reconnecting, including repeated disconnects, invalidates every earlier
    // acknowledgement without destroying authenticated replay frontiers.
    let (_, genesis) = world.issuer.genesis("second", key(4), NOW).unwrap();
    for record in genesis {
        trust.apply(&record, NOW).unwrap();
    }
    trust.serve("second");
    assert!(trust.account("second").is_ok());
    let earlier = trust.generation.load(Ordering::Relaxed);
    trust.connect();
    trust.fence(FenceCause::LostFollow);
    trust.connect();
    trust.serve("account");
    // Complete the original acknowledgement phase of an older serve call
    // after the new call. It must not withdraw the new call's progress.
    trust.acknowledge("account", earlier);
    trust.resume();
    assert!(verify::verify(&other, &trust, NOW).is_ok());
    assert!(trust.initialized("second").unwrap());
    assert_eq!(trust.account("second").err(), Some(VerifyError::Trust));
    trust.serve("second");
    assert!(trust.account("second").is_ok());

    // A wrapping generation would resurrect an earlier acknowledgement.
    // Exhaustion therefore closes the same original publication owner.
    trust.generation.store(u64::MAX, Ordering::Relaxed);
    let ended = trust.changed();
    trust.connect();
    assert_eq!(ended.now_or_never(), Some(Err(SourceClosed)));
    assert_eq!(
        *trust.state().borrow(),
        FollowerState::Closed(FenceCause::GenerationExhausted)
    );
    trust.resume();
    trust.serve("account");
    assert_eq!(
        verify::verify(&other, &trust, NOW).err(),
        Some(VerifyError::Unavailable)
    );
}

#[test]
fn account_events_preserve_exact_dependencies_without_unrelated_wakes() {
    use std::{
        sync::atomic::AtomicUsize,
        task::{Context, Poll},
    };

    #[derive(Default)]
    struct Wakes(AtomicUsize);
    impl futures::task::ArcWake for Wakes {
        fn wake_by_ref(owner: &Arc<Self>) {
            owner.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let mut world = world();
    let trust = follower(&world);
    let (mut second, genesis) = world.issuer.genesis("second", key(4), NOW).unwrap();
    for record in genesis {
        trust.apply(&record, NOW).unwrap();
    }
    trust.serve("second");
    let (first_bytes, first_id) = credential(&world, &trust, 0);
    let first = verify::verify(&first_bytes, &trust, NOW)
        .unwrap()
        .trust_evidence()
        .unwrap();
    let leaf = key(5);
    let signer = CertifiedSigner::new(
        world.issuer.certify(&second, certification(&leaf)).unwrap(),
        leaf,
    )
    .unwrap();
    let token = signer.mint(vec![], key(6), &trust, NOW).unwrap();
    let other = token.trust_evidence().unwrap();
    let mut first_event = first.changed(&trust);
    let mut other_event = other.changed(&trust);
    let first_wakes = Arc::new(Wakes::default());
    let other_wakes = Arc::new(Wakes::default());
    let first_waker = futures::task::waker_ref(&first_wakes);
    let other_waker = futures::task::waker_ref(&other_wakes);
    assert_eq!(
        first_event
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker)),
        Poll::Pending
    );
    assert_eq!(
        other_event
            .as_mut()
            .poll(&mut Context::from_waker(&other_waker)),
        Poll::Pending
    );
    let unscoped = trust.changed();
    let record = second
        .revoke(&[(token.block_ids()[0], NOW + 10_000)], NOW)
        .unwrap();
    assert_eq!(trust.apply(&record, NOW), Ok(Applied::Changed));
    assert_eq!(first_wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(other_wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(other_event.now_or_never(), Some(Ok(())));
    assert_eq!(unscoped.now_or_never(), Some(Ok(())));
    assert_eq!(other.check(&trust, NOW), Err(VerifyError::Revoked));
    assert!(first.check(&trust, NOW).is_ok());
    let mut other_event = other.changed(&trust);
    assert!((&mut other_event).now_or_never().is_none());
    assert_eq!(trust.apply(&record, NOW), Ok(Applied::Duplicate));
    assert!((&mut other_event).now_or_never().is_none());
    assert_eq!(first_wakes.0.load(Ordering::Relaxed), 0);

    // Subscription alone cannot allocate or admit a new account.
    let accounts = trust.accounts.read().unwrap().len();
    assert_eq!(
        trust.changed_for("absent").now_or_never(),
        Some(Err(SourceClosed))
    );
    assert_eq!(trust.accounts.read().unwrap().len(), accounts);

    // Original account refusal, recovery and catch-up all reach the same key.
    trust.refuse("account");
    assert_eq!(first_event.now_or_never(), Some(Ok(())));
    assert!(first.check(&trust, NOW).is_err());
    assert!((&mut other_event).now_or_never().is_none());
    let released = first.changed(&trust);
    trust.release("account");
    assert_eq!(released.now_or_never(), Some(Ok(())));
    assert!(first.check(&trust, NOW).is_ok());
    let served = first.changed(&trust);
    trust.serve("account");
    assert_eq!(served.now_or_never(), Some(Ok(())));
    assert!((&mut other_event).now_or_never().is_none());

    // Registration precedes the check even when publication precedes polling.
    let revoked = first.changed(&trust);
    trust
        .apply(
            &world.root.revoke(&[(first_id, NOW + 10_000)], NOW).unwrap(),
            NOW,
        )
        .unwrap();
    assert_eq!(revoked.now_or_never(), Some(Ok(())));
    assert_eq!(first.check(&trust, NOW), Err(VerifyError::Revoked));
    assert!((&mut other_event).now_or_never().is_none());

    // Advancing the original protected clock is a genuine global dependency.
    let time_changed = first.changed(&trust);
    let clock_record = second.revoke(&[([88; 32], NOW + 10_000)], NOW + 1).unwrap();
    trust.apply(&clock_record, NOW + 1).unwrap();
    assert_eq!(time_changed.now_or_never(), Some(Ok(())));
    assert_eq!(other_event.now_or_never(), Some(Ok(())));
    assert_eq!(trust.protected_now(NOW), NOW + 1);
    let other_event = other.changed(&trust);

    let rotated = first.changed(&trust);
    let mut frontier = EnvironmentFrontier::genesis("test", world.issuer.anchor());
    let successor = Issuer::new("test", key(7));
    let rotation = world
        .issuer
        .environment_keyring(&mut frontier, &successor.anchor(), NOW)
        .unwrap();
    trust.apply(&rotation, NOW).unwrap();
    assert_eq!(rotated.now_or_never(), Some(Ok(())));
    assert_eq!(other_event.now_or_never(), Some(Ok(())));

    // Global withdrawal never locks or scans accounts to notify their waits.
    let fenced = first.changed(&trust);
    let other_fenced = other.changed(&trust);
    let accounts = trust.accounts.write().unwrap();
    trust.fence(FenceCause::LostFollow);
    assert_eq!(fenced.now_or_never(), Some(Ok(())));
    assert_eq!(other_fenced.now_or_never(), Some(Ok(())));
    drop(accounts);
    let mut resumed = first.changed(&trust);
    assert!((&mut resumed).now_or_never().is_none());
    trust.resume();
    assert_eq!(resumed.now_or_never(), Some(Ok(())));
    let closed = first.changed(&trust);
    trust.close(FenceCause::Shutdown);
    assert_eq!(closed.now_or_never(), Some(Err(SourceClosed)));
    assert_eq!(
        first.changed(&trust).now_or_never(),
        Some(Err(SourceClosed))
    );

    // A retained account operation cannot keep the global producer alive.
    let trust = follower(&world);
    let retained = trust.find_account("account").unwrap().unwrap();
    let mut lost = first.changed(&trust);
    assert!((&mut lost).now_or_never().is_none());
    drop(trust);
    assert_eq!(lost.now_or_never(), Some(Err(SourceClosed)));
    drop(retained);
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
    let duplicate = resign(&next, 1, |keyring| {
        keyring.revocations = vec![
            Revocation {
                id: id.to_vec(),
                expires_at_unix_seconds: NOW + 500,
                reason: String::new(),
                label: None,
            },
            Revocation {
                id: id.to_vec(),
                expires_at_unix_seconds: NOW + 1,
                reason: String::new(),
                label: None,
            },
        ];
    });
    assert_eq!(trust.apply(&duplicate, NOW), Err(VerifyError::Malformed));
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
fn a_customer_root_proves_possession_then_creates_its_account() {
    let issuer = Issuer::new("test", key(1));
    let anchor = issuer.anchor();
    let customer = key(6);
    let public = customer.verifying_key().to_bytes();
    let birth = issuer
        .birth("customer", &customer.verifying_key(), NOW)
        .unwrap()
        .birth;
    let proof = AccountRoot::possession_proof(&customer, &birth).unwrap();
    crate::trust::verify_possession(&birth, &public, &proof).unwrap();
    // Negative controls: a proof over another Birth, by another key, or for
    // a Birth naming another key.
    let mut other = birth.clone();
    other.account_id = "other".into();
    assert!(crate::trust::verify_possession(&other, &public, &proof).is_err());
    let forged = AccountRoot::possession_proof(&key(7), &birth).unwrap();
    assert!(crate::trust::verify_possession(&birth, &public, &forged).is_err());
    assert!(
        crate::trust::verify_possession(&birth, &key(7).verifying_key().to_bytes(), &forged)
            .is_err()
    );

    // The environment signs the first keyring without the root's key; the
    // root alone signs the checkpoint, and custody then resumes.
    let empty = KeyringFrontier::resume("customer", &[], &[]).unwrap();
    let keyring = issuer
        .keyring_for(&empty, &customer.verifying_key(), 1, NOW)
        .unwrap();
    // Negative control: a keyring naming another root is refused.
    assert_eq!(
        AccountRoot::genesis("customer", key(7), &anchor, &keyring, NOW).err(),
        Some(VerifyError::Trust)
    );
    let (_, checkpoint) =
        AccountRoot::genesis("customer", customer.clone(), &anchor, &keyring, NOW).unwrap();
    assert!(AccountRoot::resume("customer", customer, &anchor, &[&keyring, &checkpoint]).is_ok());
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

    // Real follower admission must release its environment guard before the
    // canonical signature check. An independently signed successor can win
    // there; the obsolete proof cannot replace its current frontier afterward.
    let trust = Arc::new(Follower::new("test", issuer.anchor()));
    trust.apply(&first, NOW + 1).unwrap();
    trust.resume();
    let mut competing = EnvironmentFrontier::resume("test", issuer.anchor(), &[&first]).unwrap();
    let winner = successor
        .environment_keyring(
            &mut competing,
            &[key(7).verifying_key().to_bytes()],
            NOW + 1,
        )
        .unwrap();
    let verifying = trust.clone();
    VERIFYING_ENVIRONMENT.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            // Fail on a lock regression before an independent read/write could
            // hang. The real check still uses its original captured signers.
            assert!(verifying.environment.try_write().is_ok());
            assert!(verifying.accounts.try_write().is_ok());
            assert!(verifying.environment_keys("test").is_ok());
            assert_eq!(verifying.apply(&winner, NOW + 1), Ok(Applied::Changed));
        }));
    });
    assert_eq!(trust.apply(&second, NOW + 1), Err(VerifyError::Unavailable));
    VERIFYING_ENVIRONMENT.with(|slot| assert!(slot.borrow().is_none()));
    {
        let current = trust.environment.read().unwrap();
        assert_eq!(current.frontier(), competing.frontier());
        assert_eq!(current.keys(), competing.keys());
    }
    assert_eq!(trust.check_open(), Err(SourceClosed));
    trust.resume();
    assert_eq!(trust.apply(&second, NOW + 1), Ok(Applied::Duplicate));
    assert_eq!(trust.environment.read().unwrap().keys(), competing.keys());
}

#[test]
fn account_publication_does_not_hold_global_storage_or_accept_a_retired_signer() {
    let world = world();
    let trust = Arc::new(follower(&world));
    let (mut other, records) = world.issuer.genesis("other", key(9), NOW).unwrap();
    for record in records {
        trust.apply(&record, NOW).unwrap();
    }
    trust.serve("other");
    let leaf = key(10);
    let signer = CertifiedSigner::new(
        world.issuer.certify(&other, certification(&leaf)).unwrap(),
        leaf,
    )
    .unwrap();
    let token = signer.mint(vec![], key(11), trust.as_ref(), NOW).unwrap();
    assert!(token.check(trust.as_ref(), NOW).is_ok());

    let frontier =
        KeyringFrontier::resume("account", &world.issuer.anchor(), &slices(&world.genesis))
            .unwrap();
    let old = world
        .issuer
        .keyring_for(&frontier, &key(2).verifying_key(), 1, NOW)
        .unwrap();
    let account = trust.find_account("account").unwrap().unwrap();
    let revoked = other
        .revoke(&[(token.block_ids()[0], NOW + 10_000)], NOW)
        .unwrap();
    let successor = Issuer::new("test", key(5));
    let mut environment = EnvironmentFrontier::genesis("test", world.issuer.anchor());
    let rotation = world
        .issuer
        .environment_keyring(&mut environment, &successor.anchor(), NOW)
        .unwrap();
    let sequence = account
        .state
        .read()
        .unwrap()
        .keyring
        .as_ref()
        .unwrap()
        .sequence;
    let verifying = trust.clone();
    VERIFYING_KEYRING.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            // Interleave at the real signature boundary inside account_record.
            // The local lock is held. Try-lock assertions fail immediately if
            // either global lock regresses, before an independent writer waits.
            assert!(account.state.try_write().is_err());
            assert!(verifying.accounts.try_write().is_ok());
            assert!(verifying.environment.try_write().is_ok());
            let mut changed = verifying.changed();
            assert!((&mut changed).now_or_never().is_none());
            assert_eq!(verifying.apply(&revoked, NOW), Ok(Applied::Changed));
            assert_eq!(changed.now_or_never(), Some(Ok(())));
            assert_eq!(
                token.check(verifying.as_ref(), NOW).err(),
                Some(VerifyError::Revoked)
            );
            assert_eq!(verifying.apply(&rotation, NOW), Ok(Applied::Changed));
        }));
    });
    // Real admission authenticates against its retained old keys, then refuses
    // publication because the interleaved rotation changed that revision.
    assert_eq!(trust.apply(&old, NOW), Err(VerifyError::Unavailable));
    VERIFYING_KEYRING.with(|slot| assert!(slot.borrow().is_none()));
    assert_eq!(
        trust
            .find_account("account")
            .unwrap()
            .unwrap()
            .state
            .read()
            .unwrap()
            .keyring
            .as_ref()
            .unwrap()
            .sequence,
        sequence
    );
    assert_eq!(trust.apply(&old, NOW), Err(VerifyError::Signature));
    assert_eq!(trust.account("account").err(), Some(VerifyError::Trust));

    // The same original frontier advances through the current signer's real
    // writer; no state rollback, history copy, or replacement account is needed.
    let current = successor
        .keyring_for(&frontier, &key(2).verifying_key(), 1, NOW)
        .unwrap();
    assert_eq!(trust.apply(&current, NOW), Ok(Applied::Changed));
    assert!(trust.account("account").is_ok());
    assert_eq!(trust.apply(&current, NOW), Ok(Applied::Duplicate));
    // The account-local lock still composes the global generation cut: a read
    // spanning a full disconnect/resume cannot return its pre-disconnect data.
    assert_eq!(
        trust.with_account("account", |_, _| {
            trust.fence(FenceCause::LostFollow);
            trust.resume();
        }),
        Err(VerifyError::Unavailable)
    );
    assert_eq!(trust.account("account").err(), Some(VerifyError::Trust));
    trust.serve("account");
    assert!(trust.account("account").is_ok());
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
                trust.fence(FenceCause::LostFollow);
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
    trust.fence(FenceCause::LostFollow);
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

#[test]
fn a_close_resolves_every_wait_with_its_cause_and_is_final() {
    let world = world();
    let trust = follower(&world);
    let mut state = trust.state();
    assert_eq!(*state.borrow_and_update(), FollowerState::Serving);
    trust.fence(FenceCause::LostFollow);
    let mut fenced = trust.changed();
    assert!((&mut fenced).now_or_never().is_none());
    trust.close(FenceCause::Shutdown);
    assert_eq!(fenced.now_or_never(), Some(Err(SourceClosed)));
    assert!(state.has_changed().unwrap());
    assert_eq!(
        *state.borrow_and_update(),
        FollowerState::Closed(FenceCause::Shutdown)
    );
    assert_eq!(trust.changed().now_or_never(), Some(Err(SourceClosed)));
    // Concurrent resume/serve cannot reopen the single closed state.
    std::thread::scope(|threads| {
        threads.spawn(|| trust.resume());
        threads.spawn(|| trust.serve("account"));
    });
    // Closed is final: a resume neither reopens trust nor changes the state.
    trust.resume();
    assert_eq!(trust.check_open(), Err(SourceClosed));
    assert_eq!(
        *trust.state().borrow(),
        FollowerState::Closed(FenceCause::Shutdown)
    );
}

#[test]
fn retained_records_do_not_release_an_account_before_its_revocations() {
    let mut world = world();
    let trust = follower(&world);
    let (bytes, id) = credential(&world, &trust, 0);
    let keyring = world.issuer.keyring(&mut world.root, NOW).unwrap();
    let revoked = world.root.revoke(&[(id, NOW + 10_000)], NOW).unwrap();
    assert!(verify::verify(&bytes, &trust, NOW).is_ok());
    trust.refuse("account");
    trust.apply_queued("account", &keyring, NOW).unwrap();
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Trust)
    );
    trust.apply_queued("account", &revoked, NOW).unwrap();
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Trust)
    );
    trust.release("account");
    assert_eq!(
        verify::verify(&bytes, &trust, NOW).err(),
        Some(VerifyError::Revoked)
    );
}

/// Waiters registered in any state, interleaved with every transition: each
/// resolves on the next transition, `Ok` unless closed. `late` is the negative
/// control: a waiter that subscribes when first polled, after the transition.
fn interleave(seed: u64, late: bool) -> Result<(), String> {
    let world = world();
    let trust = Arc::new(follower(&world));
    trust.connect();
    let mut schedule = Schedule(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut waits: Vec<Changed> = Vec::new();
    let mut closed = false;
    for step in 0..300 {
        match schedule.next(7) {
            0 | 1 => waits.push(if late {
                let trust = Arc::clone(&trust);
                Box::pin(async move { trust.changed().await })
            } else {
                trust.changed()
            }),
            choice => {
                match choice {
                    2 => trust.fence(FenceCause::LostFollow),
                    3 => trust.connect(),
                    4 => trust.resume(),
                    5 => trust.serve("account"),
                    _ if schedule.next(40) == 0 => {
                        trust.close(FenceCause::Shutdown);
                        closed = true;
                    }
                    _ => continue,
                }
                for wait in waits.drain(..) {
                    match (wait.now_or_never(), closed) {
                        (Some(Ok(())), false) | (Some(Err(SourceClosed)), true) => {}
                        (outcome, _) => {
                            return Err(format!("step {step}: waiter resolved {outcome:?}"));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn simulated_waits_in_every_state_wake_on_the_next_transition() {
    for seed in 0..256 {
        interleave(seed, false).unwrap_or_else(|error| panic!("seed {seed}: {error}"));
    }
    // Negative control: subscribing after the transition loses the wake.
    assert!(
        (0..256).any(|seed| interleave(seed, true).is_err()),
        "the checker must catch a waiter that registers after the transition"
    );
}

#[test]
fn a_removed_allowance_signer_refuses_cached_admission() {
    let world = world();
    let trust = follower(&world);
    let second = Issuer::new("test", key(9));
    let mut frontier = EnvironmentFrontier::genesis("test", world.issuer.anchor());
    let both = [world.issuer.anchor()[0], second.anchor()[0]];
    let record = world
        .issuer
        .environment_keyring(&mut frontier, &both, NOW)
        .unwrap();
    assert_eq!(trust.apply(&record, NOW), Ok(Applied::Changed));
    let mint = |issuer: &Issuer, next: u8| {
        let allowance = issuer.allowance("account", 500, NOW + 100, vec![]).unwrap();
        world
            .signer
            .mint(
                vec![Caveat {
                    kind: CaveatKind::Allowance(allowance),
                }],
                key(next),
                &trust,
                NOW,
            )
            .unwrap()
    };
    // Born under the first key; its allowance is signed by the second.
    let dependent = mint(&second, 100);
    let appended = dependent
        .attenuate(
            vec![Caveat {
                kind: CaveatKind::Allowance(
                    second.allowance("account", 250, NOW + 50, vec![]).unwrap(),
                ),
            }],
            &[102; 32],
        )
        .unwrap();
    assert!(appended.check(&trust, NOW).is_ok());
    let mut forged = second.allowance("account", 250, NOW + 50, vec![]).unwrap();
    forged.signature[0] ^= 1;
    for allowance in [
        forged,
        second.allowance("foreign", 250, NOW + 50, vec![]).unwrap(),
    ] {
        assert!(
            dependent
                .attenuate(
                    vec![Caveat {
                        kind: CaveatKind::Allowance(allowance),
                    }],
                    &[103; 32],
                )
                .is_err()
        );
    }
    // Control: the same chain with an allowance by the surviving key.
    let control = mint(&world.issuer, 101);
    assert!(dependent.check(&trust, NOW).is_ok());
    let record = world
        .issuer
        .environment_keyring(&mut frontier, &both[..1], NOW)
        .unwrap();
    assert_eq!(trust.apply(&record, NOW), Ok(Applied::Changed));
    assert_eq!(dependent.check(&trust, NOW).err(), Some(VerifyError::Trust));
    assert_eq!(appended.check(&trust, NOW).err(), Some(VerifyError::Trust));
    assert_eq!(
        verify::verify(&dependent.encode(), &trust, NOW).err(),
        Some(VerifyError::Signature)
    );
    assert!(control.check(&trust, NOW).is_ok());
    assert!(verify::verify(&control.encode(), &trust, NOW).is_ok());
}

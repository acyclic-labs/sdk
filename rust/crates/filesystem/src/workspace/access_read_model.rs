//! Immutable reads use the production authority CAS and access publication.
use super::access_write_model::{AUTHORITY, head, publish, ready};
use crate::{
    AsyncAuthorityStore, AuthorityWork, CancellationToken, Digest, Epoch, GenerationId, Head,
    MemoryAuthorityStore, ObjectId, ObjectKind, VolumeConfig, WorkBudget,
    access::{
        AccessView, Action, Capability, Context, Generations, Grant, PathTrie, Policy, Principal,
        PrincipalKind, Resource, ResourcePattern,
    },
    kernel::MetadataField,
};
use std::sync::Arc;

fn revision(head: Head) -> MetadataField<ObjectId> {
    MetadataField::Value(ObjectId {
        kind: ObjectKind::Metadata,
        digest: head.digest,
    })
}

fn view(head: Head, allowed: bool) -> PathTrie {
    let mut policy: Policy = serde_json::from_slice(crate::access::INITIAL_POLICY).unwrap();
    policy.version = head.sequence.get();
    if !allowed {
        policy.layers.clear();
    }
    let principal = Principal {
        kind: PrincipalKind::Service,
        id: "model".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: "**".into(),
            },
        }]],
        expires_at: None,
    };
    PathTrie::compile(
        &policy,
        &principal,
        Resource {
            config: VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            lineage: &[],
            volume: "model",
            generation: head.sequence.get(),
            head: head.sequence.get(),
            path: "",
        },
        Context {
            now: 0,
            approvals: 0,
        },
        &capability,
    )
    .unwrap()
    .with_policy_revision(GenerationId::new(head.digest), revision(head))
}

fn model(check_snapshot: bool) {
    loom::model(move || {
        // Both synchronization sites are production crate::sync primitives.
        // Backend locks are released before access publication is acquired.
        let store = Arc::new(MemoryAuthorityStore::default());
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        ready(|| store.create_authority(AUTHORITY, Epoch::GENESIS, &mut work, None::<&mut ()>))
            .unwrap();
        assert!(publish(&store, head(&store), 1));
        let initial = head(&store);
        let views = Arc::new(AccessView::new(view(initial, true)));
        let writer_store = Arc::clone(&store);
        let writer_views = Arc::clone(&views);
        let writer = loom::thread::spawn(move || {
            assert!(publish(&writer_store, initial, 2));
            writer_views
                .publish(view(head(&writer_store), false))
                .unwrap();
        });
        let permissions = views.current().unwrap();
        let snapshot = head(&store);
        // Each immutable digest names its policy and content together. Reading
        // commit 2 represents disclosing its denied bytes, without modeling
        // unrelated filesystem codecs or replacing the authority with a mock.
        let served = (!check_snapshot
            || permissions
                .matches_policy_revision(GenerationId::new(snapshot.digest), revision(snapshot)))
            && permissions.decide(Action::Read, "", 0).allowed;
        writer
            .join()
            .expect("commit and publication finish without deadlock");
        if served {
            assert_eq!(
                snapshot.sequence.get(),
                1,
                "snapshot authorized under another policy"
            );
            assert_ne!(snapshot.digest, Digest::ZERO);
        }
        assert_eq!(head(&store).sequence.get(), 2, "committed state was lost");
    });
}

#[test]
fn access_read_model_binds_committed_bytes_to_their_policy_snapshot() {
    model(true);
}

#[test]
#[should_panic(expected = "snapshot authorized under another policy")]
fn access_read_model_detects_a_missing_snapshot_fence() {
    model(false);
}

fn disclosure_model(fence: bool) {
    loom::model(move || {
        let store = Arc::new(MemoryAuthorityStore::default());
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        ready(|| store.create_authority(AUTHORITY, Epoch::GENESIS, &mut work, None::<&mut ()>))
            .unwrap();
        assert!(publish(&store, head(&store), 1));
        let initial = head(&store);
        let permissions = view(initial, true);
        let writer = loom::thread::spawn({
            let store = store.clone();
            move || assert!(publish(&store, initial, 2))
        });
        // Storage has returned the selected immutable bytes. The disclosure
        // fence observes authority after that read, through the real backend.
        let current = head(&store);
        let served = (!fence
            || permissions
                .matches_policy_revision(GenerationId::new(current.digest), revision(current)))
            && permissions.decide(Action::Read, "", 0).allowed;
        writer.join().unwrap();
        if served {
            assert_eq!(current.sequence.get(), 1, "revoked disclosure");
        }
    });
}

#[test]
fn completed_reads_check_current_policy_at_disclosure() {
    disclosure_model(true);
}

#[test]
#[should_panic(expected = "revoked disclosure")]
fn missing_disclosure_fence_negative_control() {
    disclosure_model(false);
}

fn policy_observation_model(confirm: bool) {
    loom::model(move || {
        let store = Arc::new(MemoryAuthorityStore::default());
        let cancel = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        ready(|| store.create_authority(AUTHORITY, Epoch::GENESIS, &mut work, None::<&mut ()>))
            .unwrap();
        assert!(publish(&store, head(&store), 1));
        let initial = head(&store);
        let writer = loom::thread::spawn({
            let store = store.clone();
            move || assert!(publish(&store, initial, 2))
        });
        let observed = head(&store);
        // Metadata IO belongs to observed. A later head check is the return cut.
        loom::thread::yield_now();
        let current = head(&store);
        let returned = !confirm || crate::facade::policy_head_is_current(observed, current);
        writer.join().unwrap();
        if returned {
            assert_eq!(observed, current, "stale policy observation returned");
        }
    });
}

#[test]
fn policy_metadata_io_cannot_outlive_its_authority_observation() {
    policy_observation_model(true);
}

#[test]
#[should_panic(expected = "stale policy observation returned")]
fn missing_policy_head_confirmation_negative_control() {
    policy_observation_model(false);
}

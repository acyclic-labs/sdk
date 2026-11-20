//! The admitted local root the hosting test binaries serve: one counted memory
//! stream, one memory object store, one platform.
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{EmbeddedCapabilities, LogicalObjectStore};
use acyclic_harness_core::{
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{TurnExecutors, TurnLimits},
};
use acyclic_harness_hosting::{Bound, Placement, Turns, hosting_test_support::provider};
use acyclic_objects::Blobs;
use acyclic_stream::{MemoryStream, testing::Counted};
use std::sync::Arc;

type Fixture<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The one admitted local root the hosting test binaries serve: a counted
/// memory stream and memory objects behind the object-store trait, so every
/// test binary instantiates the hosting stack once.
pub type TestBound = Bound<Counted<MemoryStream>, LogicalObjectStore<dyn Blobs>>;

/// One admitted local root over a counted memory stream and memory objects,
/// whose turns run through `executors`. The counted stream is returned so a
/// test can assert the exact requests its scenario makes.
pub fn binding(
    executors: Arc<dyn TurnExecutors>,
    approval_policy: crate::approvals::ApprovalPolicy,
) -> Fixture<(TestBound, Arc<Counted<MemoryStream>>)> {
    let stream = Arc::new(Counted::new(MemoryStream::default()));
    let objects: Arc<dyn Blobs> = Arc::new(acyclic_objects::MemoryObjects::memory());
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(objects),
                EmbeddedCapabilities::MEMORY,
            ),
            provider: provider()?,
            within: ActorReference::new(b"account", b"agents", b"owner")?,
        },
        Turns {
            executors,
            limits: TurnLimits {
                maximum_bytes: Some(64 * 1024),
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(SystemClock),
            approval_policy,
        },
    )?;
    Ok((bound, stream))
}

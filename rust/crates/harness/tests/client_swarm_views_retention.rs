//! Logical status slices cannot retain an uncharged transport backing allocation.
use acyclic_harness::client::{ClientViewLimits, ClientViews, QueryViewLimits, SwarmUpdate};
use acyclic_stream::{StreamPath, wire};
use bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Backing {
    bytes: Vec<u8>,
    retired: Arc<AtomicBool>,
}
impl AsRef<[u8]> for Backing {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for Backing {
    fn drop(&mut self) {
        self.retired.store(true, Ordering::SeqCst);
    }
}

fn probe(keep_transport_slice: bool) {
    let retired = Arc::new(AtomicBool::new(false));
    let backing = Bytes::from_owner(Backing {
        bytes: vec![b'a'; 1024],
        retired: retired.clone(),
    });
    // The broken variant retains a source slice instead of its compact value.
    let broken = keep_transport_slice.then(|| backing.slice(..3));
    let data = wire::SessionData {
        after: Some(wire::SessionCursor {
            next: 1,
            opaque: backing.slice(6..9),
        }),
        accepted: 1,
        durable: 0,
        item: Some(wire::session_data::Item::LatestValue(
            wire::SessionLatestValue {
                key: backing.slice(..3),
                value: backing.slice(3..6),
                attention: true,
            },
        )),
    };
    drop(backing);
    let root = StreamPath::new("swarm").unwrap();
    let agent = StreamPath::new("swarm/a").unwrap();
    let mut views = ClientViews::new(
        root,
        ClientViewLimits {
            agents: 1,
            selected: 0,
            overview_values: 1,
            overview_bytes: 9,
            query: QueryViewLimits {
                files: 0,
                segments: 0,
                bytes: 0,
            },
        },
    )
    .unwrap();
    views
        .apply(SwarmUpdate::Data {
            agent: agent.clone(),
            overview: true,
            data,
        })
        .unwrap();
    assert!(
        retired.load(Ordering::SeqCst),
        "a retained status slice pinned the uncharged transport backing"
    );
    assert_eq!(
        views
            .status(&agent)
            .unwrap()
            .values()
            .next()
            .unwrap()
            .value
            .as_ref(),
        b"aaa"
    );
    drop(broken);
}

#[test]
fn status_retains_only_the_budgeted_key_value_and_cursor() {
    probe(false);
}

#[test]
fn negative_control_retaining_a_transport_slice_is_detected() {
    assert!(std::panic::catch_unwind(|| probe(true)).is_err());
}

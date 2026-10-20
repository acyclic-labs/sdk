//! Layout-derived demand never changes fidelity for an already followed agent.
use acyclic_harness::client::{ClientInterests, InterestTier};
use acyclic_stream::{StreamPath, wire::SessionDelivery};
use std::collections::BTreeSet;

#[path = "../client_interest_continuity.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod continuity;
#[path = "../client_interest_discovery.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod discovery;
#[path = "../client_interest_wire.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod platform;
#[path = "../support/quiescent.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod quiescent;
#[path = "../support/swarm_session.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod runtime;
#[path = "../client_interest_session.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod session;

#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
fn client(
    limits: acyclic_harness::client::SwarmClientLimits,
) -> acyclic_harness::Result<acyclic_harness::client::SwarmClient> {
    use acyclic_harness::client::{ClientViewLimits, QueryViewLimits, SwarmClient};
    SwarmClient::new(
        path("swarm"),
        limits,
        ClientViewLimits {
            agents: limits.agents,
            selected: limits.selected,
            overview_values: limits.agents * 2,
            overview_bytes: limits.agents as u64 * 256,
            query: QueryViewLimits {
                files: limits.snapshot_files,
                segments: limits.snapshot_files,
                bytes: 4096,
            },
        },
    )
}

fn path(value: &str) -> StreamPath {
    StreamPath::new(value).unwrap()
}

fn layout(
    visible: &[&str],
    nearby: &[&str],
    maximum: usize,
) -> acyclic_harness::Result<ClientInterests> {
    ClientInterests::from_layout(
        path("swarm"),
        visible.iter().map(|value| path(value)),
        nearby.iter().map(|value| path(value)),
        maximum,
    )
}

#[test]
fn layout_cases_keep_overview_and_normalize_prefetch() {
    for (visible, nearby, maximum, rendered, followed) in [
        (vec![], vec![], 0, vec![], vec![]),
        (
            vec!["swarm/a", "swarm/a"],
            vec!["swarm/a", "swarm/b"],
            2,
            vec!["swarm/a"],
            vec!["swarm/a", "swarm/b"],
        ),
        (
            vec!["swarm/b"],
            vec!["swarm/a"],
            2,
            vec!["swarm/b"],
            vec!["swarm/a", "swarm/b"],
        ),
        (vec!["swarm"], vec![], 1, vec!["swarm"], vec!["swarm"]),
    ] {
        let interests = layout(&visible, &nearby, maximum).unwrap();
        assert_eq!(interests.root(), &path("swarm"));
        assert_eq!(
            interests
                .visible()
                .map(StreamPath::as_str)
                .collect::<Vec<_>>(),
            rendered
        );
        assert_eq!(
            interests
                .full_fidelity()
                .map(StreamPath::as_str)
                .collect::<Vec<_>>(),
            followed
        );
        assert_eq!(
            interests.tier(&path("swarm/unseen")),
            Some(InterestTier::Overview)
        );
        assert_eq!(interests.tier(&path("swarm-other/a")), None);
        for agent in visible {
            assert_eq!(interests.tier(&path(agent)), Some(InterestTier::InView));
        }
        for agent in nearby {
            if !rendered.contains(&agent) {
                assert_eq!(interests.tier(&path(agent)), Some(InterestTier::Prefetch));
            }
        }
    }
    assert_eq!(InterestTier::Overview.delivery(), SessionDelivery::Latest);
    for tier in [InterestTier::InView, InterestTier::Prefetch] {
        assert_eq!(tier.delivery(), SessionDelivery::All);
    }
    for (visible, nearby, maximum) in [
        (vec!["swarmish/a"], vec![], 1),
        (vec![], vec!["other/a"], 1),
        (vec!["swarm/a"], vec!["swarm/b"], 1),
        (vec!["swarm/a"], vec![], 0),
    ] {
        assert!(layout(&visible, &nearby, maximum).is_err());
    }
}

#[test]
fn prefetch_to_visible_reuses_the_existing_snapshot_and_follow() {
    let before = layout(&["swarm/a"], &["swarm/b"], 2).unwrap();
    let after = layout(&["swarm/b"], &["swarm/a"], 2).unwrap();
    let changes = after.changes(&before).unwrap();
    assert_eq!(changes.subscribe().count(), 0);
    assert_eq!(changes.unsubscribe().count(), 0);
    let next = layout(&["swarm/b"], &["swarm/c"], 2).unwrap();
    let changes = next.changes(&after).unwrap();
    assert_eq!(
        changes
            .subscribe()
            .map(StreamPath::as_str)
            .collect::<Vec<_>>(),
        ["swarm/c"]
    );
    assert_eq!(
        changes
            .unsubscribe()
            .map(StreamPath::as_str)
            .collect::<Vec<_>>(),
        ["swarm/a"]
    );
    let other = ClientInterests::from_layout(path("other"), [], [], 0).unwrap();
    assert!(other.changes(&next).is_err());
}

#[test]
fn bounds_stop_input_before_retaining_an_unbounded_layout() {
    let mut visited = 0;
    let selected = (0..1000).map(|index| {
        visited += 1;
        path(&format!("swarm/{index}"))
    });
    assert!(ClientInterests::from_layout(path("swarm"), selected, [], 2).is_err());
    assert_eq!(
        visited, 3,
        "do not collect input before enforcing the bound"
    );
}

fn selected(ids: &[u8]) -> impl Iterator<Item = StreamPath> + '_ {
    ids.iter().map(|id| path(&format!("swarm/{id}")))
}

fn ids<'a>(paths: impl Iterator<Item = &'a StreamPath>) -> BTreeSet<u8> {
    paths
        .map(|value| {
            value
                .as_str()
                .strip_prefix("swarm/")
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect()
}

fn check_changes(
    before: &BTreeSet<u8>,
    after: &BTreeSet<u8>,
    subscribe: &BTreeSet<u8>,
    unsubscribe: &BTreeSet<u8>,
) -> bool {
    *subscribe == after.difference(before).copied().collect()
        && *unsubscribe == before.difference(after).copied().collect()
}

#[test]
fn negative_control_promotion_resubscribe_is_rejected() {
    let before = layout(&[], &["swarm/1"], 1).unwrap();
    let after = layout(&["swarm/1"], &[], 1).unwrap();
    let changes = after.changes(&before).unwrap();
    let full = BTreeSet::from([1]);
    assert!(check_changes(
        &full,
        &full,
        &ids(changes.subscribe()),
        &ids(changes.unsubscribe())
    ));
    // Broken transport treats a rendering-tier change as a new subscription.
    assert!(!check_changes(
        &full,
        &full,
        &BTreeSet::from([1]),
        &BTreeSet::new()
    ));
}

proptest::proptest! {
    // Property: demand is exactly the bounded union of layout selections; visible
    // entries take precedence, and all unselected agents remain at overview.
    #[test]
    fn tiers_equal_layout_membership(
        visible in proptest::collection::vec(0u8..32, 0..64),
        nearby in proptest::collection::vec(0u8..32, 0..64),
        maximum in 0usize..33,
    ) {
        let rendered: BTreeSet<_> = visible.iter().copied().collect();
        let prefetched: BTreeSet<_> = nearby.iter().copied().collect();
        let full: BTreeSet<_> = rendered.union(&prefetched).copied().collect();
        let result = ClientInterests::from_layout(path("swarm"), selected(&visible), selected(&nearby), maximum);
        if full.len() > maximum {
            proptest::prop_assert!(result.is_err());
        } else {
            let interests = result.unwrap();
            proptest::prop_assert_eq!(ids(interests.visible()), rendered.clone());
            proptest::prop_assert_eq!(ids(interests.full_fidelity()), full);
            for id in 0..32 {
                let expected = if rendered.contains(&id) { InterestTier::InView }
                    else if prefetched.contains(&id) { InterestTier::Prefetch }
                    else { InterestTier::Overview };
                proptest::prop_assert_eq!(interests.tier(&path(&format!("swarm/{id}"))), Some(expected));
            }
        }
    }

    // Property: applying the borrowed subscription diff reconstructs the new
    // lossless set exactly, independent of the previous rendering tier.
    #[test]
    fn changes_are_exact_set_differences(
        old_visible in proptest::collection::vec(0u8..16, 0..24),
        old_nearby in proptest::collection::vec(0u8..16, 0..24),
        new_visible in proptest::collection::vec(0u8..16, 0..24),
        new_nearby in proptest::collection::vec(0u8..16, 0..24),
    ) {
        let previous = ClientInterests::from_layout(path("swarm"), selected(&old_visible), selected(&old_nearby), 16).unwrap();
        let current = ClientInterests::from_layout(path("swarm"), selected(&new_visible), selected(&new_nearby), 16).unwrap();
        let before: BTreeSet<_> = old_visible.into_iter().chain(old_nearby).collect();
        let after: BTreeSet<_> = new_visible.into_iter().chain(new_nearby).collect();
        let changes = current.changes(&previous).unwrap();
        proptest::prop_assert!(check_changes(&before, &after, &ids(changes.subscribe()), &ids(changes.unsubscribe())));
    }
}

#[path = "../support/swarm_status.rs"]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod status;

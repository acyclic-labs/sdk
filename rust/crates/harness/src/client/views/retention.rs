//! Retained allocation depends on final values, never their replacement history.
use super::*;
use proptest::{
    prelude::*,
    test_runner::{Config, TestError, TestRunner},
};

fn data(next: u64, key: Bytes, length: usize) -> wire::SessionData {
    wire::SessionData {
        after: Some(wire::SessionCursor {
            next,
            opaque: Bytes::from_static(b"cursor"),
        }),
        accepted: next,
        durable: next,
        item: Some(wire::session_data::Item::LatestValue(
            wire::SessionLatestValue {
                key,
                value: vec![7; length].into(),
                attention: false,
            },
        )),
    }
}
fn retained(sizes: (usize, usize, usize, usize), history: bool, broken: bool) -> i64 {
    let (old, current, key_bytes, other) = sizes;
    let mut kept = None;
    let allocation = acyclic_test_support::measure(|| {
        let path = |s| StreamPath::new(s).unwrap();
        let mut views = ClientViews::new(
            path("swarm"),
            ClientViewLimits {
                agents: 1,
                selected: 0,
                overview_values: 2,
                overview_bytes: (old + current + other + 2 * key_bytes + 12) as u64,
                query: QueryViewLimits {
                    files: 0,
                    segments: 0,
                    bytes: 0,
                },
            },
        )
        .unwrap();
        let agent = path("swarm/a");
        let key: Bytes = vec![1; key_bytes].into();
        let mut previous = None;
        if history {
            views
                .apply(SwarmUpdate::Data {
                    agent: agent.clone(),
                    overview: true,
                    data: data(1, key.clone(), old),
                })
                .unwrap();
            if broken {
                previous = views.agents[&agent]
                    .status
                    .values
                    .first_key_value()
                    .map(|(key, _)| key.clone());
            }
        }
        views
            .apply(SwarmUpdate::Data {
                agent: agent.clone(),
                overview: true,
                data: data(2, key.clone(), current),
            })
            .unwrap();
        if let Some(previous) = previous {
            // Restore precisely insert's old-key behavior, sharing the retired
            // body allocation despite the new value and reduced accounting.
            let values = &mut views.agents.get_mut(&agent).unwrap().status.values;
            let (_, value) = values.remove_entry(&key).unwrap();
            values.insert(previous, value);
        }
        views
            .apply(SwarmUpdate::Data {
                agent,
                overview: true,
                data: data(3, vec![2; key_bytes].into(), other),
            })
            .unwrap();
        kept = Some(views);
    });
    drop(kept);
    allocation.bytes_current
}
fn property(
    sizes: (usize, usize, usize, usize),
    broken: bool,
) -> std::result::Result<(), TestCaseError> {
    prop_assert_eq!(
        retained(sizes, true, broken),
        retained(sizes, false, false),
        "replacements pinned a retired allocation"
    );
    Ok(())
}
fn sizes() -> impl Strategy<Value = (usize, usize, usize, usize)> {
    (
        2048_usize..16384,
        128_usize..512,
        1_usize..128,
        0_usize..256,
    )
}
proptest! {
    #[test]
    fn status_replacement_retains_exactly_the_direct_final_state(sizes in sizes()) {
        property(sizes, false)?;
    }
}
#[test]
fn negative_control_preserving_the_old_map_key_fails_and_shrinks() {
    let mut runner = TestRunner::new(Config {
        cases: 32,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&sizes(), |sizes| property(sizes, true));
    assert!(
        matches!(result, Err(TestError::Fail(_, (2048, 128, 1, 0)))),
        "pinning control escaped or did not shrink: {result:?}"
    );
}

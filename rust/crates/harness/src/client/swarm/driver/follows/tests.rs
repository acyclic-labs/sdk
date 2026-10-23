//! Shrinking oracle exercises the production membership, view and snapshot retirement.
use super::*;
use crate::{
    client::{ClientViewLimits, ClientViews, QueryViewLimits, SwarmUpdate},
    task_volume::{Generation, HeadSnapshot, Query, SCHEMA},
};
use proptest::{
    prelude::*,
    test_runner::{Config, TestCaseResult, TestError, TestRunner},
};
use std::sync::Arc;

struct Unpolled;
impl acyclic_stream::session::runtime::Executor for Unpolled {
    fn spawn(&self, _: acyclic_stream::BoxProviderFuture<'static, ()>) {}
    fn sleep(&self, _: u32) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        Box::pin(futures::future::pending())
    }
}
fn path(id: usize) -> StreamPath {
    StreamPath::new(format!("swarm/a{id}")).unwrap()
}
fn check(commands: &[u8], broken: bool) -> TestCaseResult {
    let session = Client::in_process(
        Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_stream::session::testing::authority(),
        Arc::new(Unpolled),
    );
    let mut follows = Follows::new(session, Box::pin(futures::stream::pending()));
    let root = StreamPath::new("swarm").unwrap();
    let mut views = ClientViews::new(
        root.clone(),
        ClientViewLimits {
            agents: 2,
            selected: 2,
            overview_values: 2,
            overview_bytes: 66,
            query: QueryViewLimits {
                files: 1,
                segments: 1,
                bytes: 128,
            },
        },
    )
    .unwrap();
    let mut snapshots = super::super::snapshots::Snapshots::new(2);
    let mut active = std::collections::BTreeSet::new();
    let mut next_id = 0;
    for command in commands {
        if command % 2 == 0 && active.len() < 2 {
            let agent = path(next_id);
            next_id += 1;
            active.insert(agent.clone());
            let demand =
                ClientInterests::from_layout(root.clone(), active.iter().cloned(), [], 2).unwrap();
            snapshots.layout(&demand).unwrap();
            views.apply(SwarmUpdate::Layout(demand.clone())).unwrap();
            prop_assert!(
                follows.discover(&demand, agent.clone(), 2).is_ok(),
                "retired member still consumes subscription capacity"
            );
            prop_assert!(
                views
                    .apply(SwarmUpdate::Agent {
                        agent: agent.clone()
                    })
                    .is_ok(),
                "retired member still consumes view capacity"
            );
            let cursor = Cursor {
                volume: [1; 32],
                epoch: uuid::Uuid::nil(),
                schema: SCHEMA,
                query: *blake3::hash(&serde_json::to_vec(&Query::Root).unwrap()).as_bytes(),
                position: 1,
                accepted: 1,
                durable: 1,
                commit: [2; 32],
            };
            follows.content(agent.clone(), &cursor).unwrap();
            views
                .apply(SwarmUpdate::Snapshot {
                    agent: agent.clone(),
                    snapshot: HeadSnapshot {
                        files: vec![],
                        generation: Generation {
                            volume: cursor.volume,
                            epoch: cursor.epoch,
                            position: cursor.position,
                            commit: cursor.commit,
                        },
                        cursor,
                    },
                })
                .unwrap();
            let data = wire::SessionData {
                after: Some(wire::SessionCursor {
                    next: 1,
                    opaque: Default::default(),
                }),
                accepted: 1,
                durable: 1,
                item: Some(wire::session_data::Item::LatestValue(
                    wire::SessionLatestValue {
                        key: b"k".to_vec().into(),
                        value: vec![1; 32].into(),
                        attention: false,
                    },
                )),
            };
            prop_assert!(
                views
                    .apply(SwarmUpdate::Data {
                        agent: agent.clone(),
                        overview: true,
                        data
                    })
                    .is_ok(),
                "retired status charge leaked"
            );
            snapshots
                .start(agent.clone(), futures::future::pending())
                .unwrap();
        } else if command % 2 == 1
            && let Some(agent) = active.pop_first()
        {
            if !broken {
                snapshots.retire(&agent).unwrap();
                follows.retire(&agent);
                views.apply(SwarmUpdate::Retired { agent }).unwrap();
            }
        }
        prop_assert_eq!(
            follows.agents.len(),
            active.len(),
            "retired membership was retained"
        );
        prop_assert_eq!(
            follows.content.len(),
            active.len(),
            "retired content was retained"
        );
        prop_assert_eq!(
            views
                .agents()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            active.clone()
        );
    }
    Ok(())
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn archive_churn_reclaims_subscriptions_views_and_accounting(commands in prop::collection::vec(any::<u8>(), 0..80)) {
        check(&commands, false)?;
    }
}
#[test]
fn missing_retirement_mutant_is_rejected_and_shrinks() {
    let strategy = prop::collection::vec(any::<u8>(), 1..80);
    let mut runner = TestRunner::new(Config {
        cases: 64,
        ..Config::default()
    });
    assert!(matches!(
        runner.run(&strategy, |commands| check(&commands, true)),
        Err(TestError::Fail(_, _))
    ));
    assert!(check(&[0, 0, 1, 0], true).is_err());
}

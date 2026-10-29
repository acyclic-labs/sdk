//! Server-owned tree/page fixture exercising the production client ownership.
//! Transport is deliberately unpolled: these tests measure display admission,
//! fold memory and cancellation of lazy interests, not network credit.
use super::*;
use crate::{AgentId, AgentPath};
use acyclic_stream::{runtime::Executor, session::transport};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Scheduler {
    tasks: Mutex<Vec<BoxProviderFuture<'static, ()>>>,
    sleeps: AtomicUsize,
}

impl Executor for Scheduler {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        self.tasks.lock().unwrap().push(future);
    }
    fn sleep(&self, _: u32) -> BoxProviderFuture<'static, ()> {
        self.sleeps.fetch_add(1, Ordering::Relaxed);
        Box::pin(futures::future::pending())
    }
}

struct Unpolled;
impl transport::Transport for Unpolled {
    fn session(
        &self,
        _: transport::Requests,
    ) -> BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        panic!("display admission must not eagerly open a transport")
    }
}

fn client() -> (WireSession, Arc<Scheduler>) {
    let scheduler = Arc::new(Scheduler::default());
    let pool = Client::with_transport(
        Arc::new(Unpolled),
        Arc::new(|_| Box::pin(async { Err(StreamError::Unavailable) })),
        scheduler.clone(),
    );
    (
        WireSession::with_view_policy(
            pool,
            DemandPolicy::default(),
            StreamViewPolicy::new(4, 8192).unwrap(),
        ),
        scheduler,
    )
}

/// The population lives on the server side of the fixture; the client keeps
/// no population/status map.
struct Tree {
    children: Vec<AgentPath>,
}
impl Tree {
    fn new(total: usize) -> Self {
        let root = AgentPath::root(&AgentId::from_bytes([0; 16]));
        Self {
            children: (1..=total)
                .map(|id| {
                    root.child(&AgentId::from_bytes((id as u128).to_be_bytes()))
                        .unwrap()
                })
                .collect(),
        }
    }
    fn visible(
        &self,
        start: usize,
    ) -> (
        Vec<(StreamPath, wire::SessionSubscription)>,
        Vec<wire::SessionSubscription>,
    ) {
        let mut streams = Vec::new();
        let mut resources = Vec::new();
        for agent in &self.children[start..(start + 4).min(self.children.len())] {
            for path in [agent.conversation().unwrap(), agent.status().unwrap()] {
                streams.push((
                    path.clone(),
                    follow(wire_codec::session::stream_resource(path.as_str())),
                ));
            }
            resources.push(follow(format!("fs:/volume/{}/output", agent.id().as_str())));
        }
        (streams, resources)
    }
}

fn follow(resource: String) -> wire::SessionSubscription {
    wire::SessionSubscription {
        resume: Some(wire::SessionCursor {
            next: 0,
            opaque: Bytes::new(),
        }),
        kind: Some(wire::session_subscription::Kind::Follow(
            wire::SessionFollow {
                resource,
                delivery: wire::SessionDelivery::All as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn fill(view: &mut StreamView) {
    // Server committed history is delivered incrementally, never retained as
    // a whole transcript. Exercise eviction in the actual StreamView reducer.
    for sequence in view.next_sequence()..view.next_sequence() + 16 {
        let frame = wire::SessionFrame {
            body: Some(wire::session_frame::Body::Data(wire::SessionData {
                item: Some(wire::session_data::Item::Record(wire::Record {
                    sequence,
                    value: Bytes::from_static(&[42; 1024]),
                    commit_id: Bytes::from_static(&[1; 32]),
                    ..Default::default()
                })),
                after: Some(wire::SessionCursor {
                    next: sequence + 1,
                    opaque: Bytes::new(),
                }),
                accepted: sequence + 1,
                durable: sequence + 1,
                ..Default::default()
            })),
            ..Default::default()
        };
        view.apply(&frame, sequence + 1).unwrap();
    }
    assert_eq!(view.records().len(), 4);
}

#[test]
fn viewport_refusal_is_atomic_and_legacy_resource_retention_fails_oracle() {
    let tree = Tree::new(64);
    let (mut session, scheduler) = client();
    let (streams, resources) = tree.visible(0);
    session.replace_interests(streams, resources).unwrap();
    let old_path = session.views.keys().next().unwrap().clone();
    fill(&mut session.views.get_mut(&old_path).unwrap().view);
    let old_bytes = session.retained_bytes();
    let old_next = session.view(&old_path).unwrap().next_sequence();
    let revision = session.revision();
    let (streams, mut resources) = tree.visible(32);
    resources.extend((0..5).map(|id| follow(format!("fs:/volume/excess{id}/output"))));
    assert!(matches!(
        session.replace_interests(streams, resources),
        Err(WireSessionError::Demand(DemandError::SelectedCapacity))
    ));
    assert_eq!(session.revision(), revision);
    assert_eq!(session.followed_interests(), 12);
    assert_eq!(session.retained_bytes(), old_bytes);
    assert_eq!(session.view(&old_path).unwrap().next_sequence(), old_next);
    // Negative control: the old stream-only replacement retains off-screen
    // tool panes. The identical current-resource oracle rejects that shape.
    let (streams, resources) = tree.visible(32);
    let expected: Vec<_> = resources
        .iter()
        .map(|sub| validate_resource_subscription(sub).unwrap().0)
        .collect();
    session.replace_streams(streams).unwrap();
    assert!(!session.resources.keys().all(|key| expected.contains(key)));
    assert_eq!(session.resources.len(), 4);
    session
        .replace_interests(tree.visible(32).0, resources)
        .unwrap();
    assert!(session.resources.keys().all(|key| expected.contains(key)));
    eprintln!("off-screen retained tool panes: legacy=4 complete_viewport=0");
    scheduler.tasks.lock().unwrap().clear();
}

#[test]
fn agent_viewport_releases_output_panes_on_scroll_out() {
    let tree = Tree::new(64);
    let (session, scheduler) = client();
    let selection = |index: usize| crate::AgentSelection {
        agent: tree.children[index].clone(),
        conversation_from: 0,
        status_from: 0,
    };
    let mut client =
        crate::AgentClient::new(session.client().clone(), tree.children[0].clone(), 0, 0, 0)
            .unwrap();
    client
        .set_viewport(selection(0), (1..4).map(selection), tree.visible(0).1)
        .unwrap();
    assert_eq!(client.residency(), (12, 12, 0));
    client
        .set_viewport(selection(32), (33..36).map(selection), tree.visible(32).1)
        .unwrap();
    assert_eq!(client.residency(), (12, 12, 0));
    for old in tree.visible(0).1 {
        let (name, _) = validate_resource_subscription(&old).unwrap();
        assert!(client.resource_view(&name).is_err());
    }
    client
        .set_viewport(selection(32), (33..36).map(selection), std::iter::empty())
        .unwrap();
    assert_eq!(client.residency(), (8, 8, 0));
    scheduler.tasks.lock().unwrap().clear();
}

#[test]
fn terminal_viewport_interests_preserve_output_until_explicit_reopen() {
    for resource in [false, true] {
        for failed in [false, true] {
            let tree = Tree::new(64);
            let (mut session, scheduler) = client();
            let (streams, resources) = tree.visible(0);
            let path = streams[0].0.clone();
            let request = if resource {
                resources[0].clone()
            } else {
                streams[0].1.clone()
            };
            let name = validate_resource_subscription(&resources[0]).unwrap().0;
            session.replace_interests(streams, resources).unwrap();
            let terminal = if failed {
                vec![Err(StreamError::Unavailable)]
            } else {
                Vec::new()
            };
            let follow = Subscription {
                state: SubscriptionState::Open(Box::pin(futures::stream::iter(terminal))),
            };
            let mut context = Context::from_waker(std::task::Waker::noop());
            if resource {
                fill(&mut session.resource_views.get_mut(&name).unwrap().view);
                session.resources.insert(name.clone(), follow);
                assert!(session.poll_resource(&name, &mut context).is_ready());
                assert!(!session.is_resource_following(&name));
            } else {
                fill(&mut session.views.get_mut(&path).unwrap().view);
                session.follows.insert(path.clone(), follow);
                assert!(session.poll_stream(&path, &mut context).is_ready());
                assert!(!session.is_following(&path));
            }
            let view = |session: &WireSession| {
                let view = if resource {
                    session.resource_view(&name)
                } else {
                    session.view(&path)
                }
                .unwrap();
                (
                    view.retained_bytes(),
                    view.next_sequence(),
                    view.revision(),
                    view.record(0).unwrap().value.as_ptr(),
                )
            };
            let retained = view(&session);
            // Negative-control oracle: the old live-map membership check calls
            // this terminal interest new despite its populated retained fold.
            let would_reopen = if resource {
                !session.resources.contains_key(&name)
            } else {
                !session.follows.contains_key(&path)
            };
            assert!(would_reopen && retained.0 > 0);
            for _ in 0..2 {
                let (streams, resources) = tree.visible(0);
                session.replace_interests(streams, resources).unwrap();
                assert_eq!(view(&session), retained);
                assert_eq!(session.followed_interests(), 11);
                assert_eq!(session.retained_views(), 12);
            }
            // Retained ownership still binds the credential, even without a
            // live follow. A replacement cannot relabel another user's output.
            let revision = session.revision();
            let (mut streams, mut resources) = tree.visible(0);
            if resource {
                resources[0].credential = 1;
            } else {
                streams[0].1.credential = 1;
            }
            assert!(matches!(
                session.replace_interests(streams, resources),
                Err(WireSessionError::InvalidSubscription)
            ));
            assert_eq!(session.revision(), revision);
            assert_eq!(view(&session), retained);
            let (mut streams, mut resources) = tree.visible(0);
            let filter = Some(wire::SessionFilter {
                keys: vec![wire::KeyRange {
                    start: Bytes::from_static(b"restricted"),
                    end: Bytes::from_static(b"restricted\0"),
                }],
                ..Default::default()
            });
            if resource {
                resources[0].filter = filter;
            } else {
                streams[0].1.filter = filter;
            }
            assert!(matches!(
                session.replace_interests(streams, resources),
                Err(WireSessionError::InvalidSubscription)
            ));
            assert_eq!(session.revision(), revision);
            assert_eq!(view(&session), retained);
            if resource {
                session.begin_resource(request).unwrap();
                assert_eq!(session.resource_view(&name).unwrap().retained_bytes(), 0);
            } else {
                session.begin_stream(path.clone(), request).unwrap();
                assert_eq!(session.view(&path).unwrap().retained_bytes(), 0);
            }
            assert_eq!(session.followed_interests(), 12);
            scheduler.tasks.lock().unwrap().clear();
        }
    }
}

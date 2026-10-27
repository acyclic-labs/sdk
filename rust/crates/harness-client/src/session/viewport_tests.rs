//! Server-owned tree/page fixture exercising the production client ownership.
//! Transport is deliberately unpolled: these tests measure display admission,
//! fold memory and cancellation of lazy interests, not network credit.
use super::*;
use crate::{AgentId, AgentPath, PageDirection, PageWindow, PageWindowPolicy, WindowPage};
use acyclic_stream::{runtime::Executor, session::client::transport};
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

struct Page {
    rows: Vec<usize>,
    before: Arc<usize>,
    after: Arc<usize>,
    total: usize,
}
impl WindowPage for Page {
    type Item = usize;
    type Key = usize;
    type Cursor = usize;
    type Generation = u64;
    fn items(&self) -> &[usize] {
        &self.rows
    }
    fn key(item: &usize) -> &usize {
        item
    }
    fn before(&self) -> &Arc<usize> {
        &self.before
    }
    fn after(&self) -> &Arc<usize> {
        &self.after
    }
    fn generation(&self) -> &u64 {
        &1
    }
    fn has_more_before(&self) -> bool {
        *self.before > 0
    }
    fn has_more_after(&self) -> bool {
        *self.after + 1 < self.total
    }
    fn retained_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>()
            + self.rows.capacity() * std::mem::size_of::<usize>()
            + 2 * (std::mem::size_of::<usize>() * 3)) as u64
    }
}

/// The population lives on the server side of the fixture. Only page() results
/// enter PageWindow; there is no client-side population/status map.
struct Tree {
    children: Vec<AgentPath>,
    fetched: usize,
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
            fetched: 0,
        }
    }
    fn page(&mut self, start: usize) -> Page {
        let end = (start + 32).min(self.children.len());
        self.fetched += 1;
        Page {
            rows: (start..end).collect(),
            before: Arc::new(start),
            after: Arc::new(end - 1),
            total: self.children.len(),
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
fn hundred_thousand_agent_tree_has_bounded_viewport() {
    let started = std::time::Instant::now();
    let mut tree = Tree::new(100_000);
    let mut pages = PageWindow::new(PageWindowPolicy::new(2, 64, 4096).unwrap());
    pages.reset(tree.page(0)).unwrap();
    let (mut session, scheduler) = client();
    let mut peak_bytes = 0;
    for start in (0..100_000).step_by(32) {
        if start > 0 {
            let mut request = pages.begin(PageDirection::After).unwrap();
            assert_eq!(request.as_ref().unwrap().cursor().copied(), Some(start - 1));
            pages.finish(&mut request, Some(tree.page(start))).unwrap();
        }
        assert!(
            pages.len() <= 64 && pages.page_count() <= 2 && pages.retained_page_bytes() <= 4096
        );
        let (streams, resources) = tree.visible(start);
        let expected: Vec<_> = resources
            .iter()
            .map(|sub| validate_resource_subscription(sub).unwrap().0)
            .collect();
        session.replace_interests(streams, resources).unwrap();
        assert_eq!(
            session.retained_bytes(),
            0,
            "scroll-out must evict old payloads"
        );
        assert_eq!(session.followed_interests(), 12);
        assert_eq!(session.retained_views(), 12);
        assert!(session.resources.keys().all(|key| expected.contains(key)));
        for view in session
            .views
            .values_mut()
            .chain(session.resource_views.values_mut())
        {
            fill(view);
        }
        assert!(session.retained_bytes() <= 12 * 8192);
        peak_bytes = peak_bytes.max(session.retained_bytes());
        // Exactly one shared pool task, no work or timers per idle agent.
        assert_eq!(scheduler.tasks.lock().unwrap().len(), 1);
        assert_eq!(scheduler.sleeps.load(Ordering::Relaxed), 0);
    }
    assert_eq!(tree.fetched, 3125);
    session.clear().unwrap();
    assert_eq!(
        (
            session.followed_interests(),
            session.retained_views(),
            session.retained_bytes()
        ),
        (0, 0, 0)
    );
    eprintln!(
        "100000 agents: pages<=2 rows<=64 follows=12 display_bytes_peak={peak_bytes} idle_follows=0 elapsed={:?}",
        started.elapsed()
    );
}

#[test]
fn viewport_refusal_is_atomic_and_legacy_resource_retention_fails_oracle() {
    let tree = Tree::new(64);
    let (mut session, _) = client();
    let (streams, resources) = tree.visible(0);
    session.replace_interests(streams, resources).unwrap();
    let revision = session.revision();
    let (streams, mut resources) = tree.visible(32);
    resources.extend(std::iter::repeat_n(
        follow("fs:/volume/excess/output".into()),
        17,
    ));
    assert!(session.replace_interests(streams, resources).is_err());
    assert_eq!(session.revision(), revision);
    assert_eq!(session.followed_interests(), 12);
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
}

#[test]
fn agent_viewport_releases_output_panes_on_scroll_out() {
    let tree = Tree::new(64);
    let (session, _) = client();
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
}

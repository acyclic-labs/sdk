//! Views driven by the original provider's cuts and wake events.
#![cfg(feature = "agents")]

use acyclic_harness_client::explorer::{Explorer, Row};
use acyclic_harness_client::reader::{Driver, Op, Outcome, Priority, Reader, ReaderPolicy, Want};
use acyclic_harness_core::agent::{AgentId, AgentPath, AgentRecord, Phase, StatusRecord};
use acyclic_stream::{
    AppendRequest, ForkRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProviderExt as _,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Wake, Waker};

#[derive(Default)]
struct Ready(AtomicBool);

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn drive(reader: &mut Reader, driver: &mut Driver<MemoryStream>, ready: &Arc<Ready>, want: &Want) {
    let waker = Waker::from(ready.clone());
    let mut cx = Context::from_waker(&waker);
    ready.wake_by_ref();
    while ready.0.swap(false, Ordering::SeqCst) {
        let before = reader.generation();
        driver.step(reader, 0, &mut cx);
        if reader.generation() != before {
            reader.plan([(want.clone(), Priority::Visible)]);
            ready.wake_by_ref();
        }
    }
}

fn render(explorer: &mut Explorer, driver: &mut Driver<MemoryStream>, ready: &Arc<Ready>) {
    let waker = Waker::from(ready.clone());
    let mut cx = Context::from_waker(&waker);
    driver.attach(explorer.reader(), 0);
    explorer.frame(0);
    ready.wake_by_ref();
    while ready.0.swap(false, Ordering::SeqCst) {
        let before = explorer.reader_ref().generation();
        driver.step(explorer.reader(), 0, &mut cx);
        if explorer.reader_ref().generation() != before {
            // A completed provider event is the reason to render again.
            explorer.frame(0);
            ready.wake_by_ref();
        }
    }
}

#[test]
fn clock_jumps_and_extreme_scroll_preserve_original_provider_progress() {
    futures::executor::block_on(async {
        let provider = MemoryStream::default();
        let mut explorer = Explorer::new(ReaderPolicy::default()).unwrap();
        let mut expected = 0u128;
        for (id, elapsed) in [(1u128, u64::MAX), (2, 0), (3, u64::MAX), (4, 1)] {
            let agent = AgentPath::root(&AgentId::from_bytes(id.to_be_bytes()));
            let stream = agent.status().unwrap();
            provider
                .append(AppendRequest {
                    idempotency_key: IdempotencyKey::new(stream.to_string()).unwrap(),
                    path: stream.clone(),
                    records: vec![
                        StatusRecord {
                            phase: Phase::Idle,
                            cursor: 0,
                            conversation: 0,
                        }
                        .encode()
                        .unwrap()
                        .into(),
                    ],
                    if_tail: Some(0),
                })
                .await
                .unwrap();
            explorer.reader().plan([(
                Want::Last {
                    stream: stream.clone(),
                    count: 1,
                },
                Priority::Visible,
            )]);
            let [op]: [Op; 1] = explorer.reader().poll(0).try_into().unwrap();
            assert!(matches!(&op, Op::Head { stream: path, .. } if path == &stream));
            let original = provider.head(stream.clone()).await.unwrap();
            explorer
                .reader()
                .complete(op.id(), Outcome::Head(original), elapsed);
            expected = if expected == 0 {
                u128::from(elapsed)
            } else {
                (expected * 7 + u128::from(elapsed)) / 8
            };
            assert_eq!(u128::from(explorer.reader_ref().rtt_ms()), expected);
            assert_eq!(explorer.reader_ref().cache().tail(&stream), Some(1));
            assert!(explorer.reader_ref().cache().record(&stream, 0).is_some());
        }
        explorer.set_viewport(1, 1, 1);
        for velocity in [i64::MIN, i64::MAX] {
            explorer.scroll(velocity);
            explorer.frame(0);
        }
    });
}

#[test]
fn explorer_prefetch_and_expansion_keep_original_provider_cuts() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let root = AgentPath::root(&AgentId::from_bytes([1; 16]));
        let child = root.child(&AgentId::from_bytes([2; 16])).unwrap();
        for agent in [&root, &child] {
            for (path, records) in [
                (
                    StreamPath::new(agent.as_str()).unwrap(),
                    vec![b"entry".as_slice().into()],
                ),
                (
                    agent.conversation().unwrap(),
                    vec![b"first".as_slice().into(), b"second".as_slice().into()],
                ),
                (
                    agent.status().unwrap(),
                    vec![
                        StatusRecord {
                            phase: Phase::Idle,
                            cursor: 0,
                            conversation: 2,
                        }
                        .encode()
                        .unwrap()
                        .into(),
                    ],
                ),
            ] {
                provider
                    .append(AppendRequest {
                        idempotency_key: IdempotencyKey::new(path.to_string()).unwrap(),
                        path,
                        records,
                        if_tail: Some(0),
                    })
                    .await
                    .unwrap();
            }
        }
        let mut driver = Driver::new(provider.clone());
        let ready = Arc::new(Ready::default());
        let mut explorer = Explorer::new(ReaderPolicy::default()).unwrap();
        explorer.set_viewport(2, 0, 16);
        render(&mut explorer, &mut driver, &ready);
        assert!(matches!(
            explorer.rows(),
            [(
                _,
                Row::Agent {
                    status: Some(_),
                    conversation: Some(2),
                    ..
                }
            )]
        ));
        assert!(
            explorer
                .reader_ref()
                .cache()
                .listing(&root.children().unwrap(), 0)
                .is_some()
        );
        // More logical expansion intents than the former hidden limit. Only
        // the existing visible branch has provider data or consumes credit.
        for index in 0..300u128 {
            explorer.expand(
                &root
                    .child(&AgentId::from_bytes((index + 10).to_be_bytes()))
                    .unwrap(),
            );
        }
        explorer.expand(&child);
        explorer.expand(&root);
        render(&mut explorer, &mut driver, &ready);
        assert_eq!(explorer.expanded(), 302);
        assert!(explorer.rows().iter().any(|(_, row)| matches!(row,
            Row::Agent { path, expanded: true, status: Some(_), .. } if path == &child)));
        explorer.collapse(&child);
        explorer.open(child.clone(), 2);
        render(&mut explorer, &mut driver, &ready);
        assert_eq!(explorer.expanded(), 301);
        assert_eq!(
            explorer
                .history()
                .iter()
                .map(|row| row.value.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [b"first".as_slice(), b"second"]
        );
        let snapshot = explorer.reader_ref().snapshot().unwrap();
        let mut restored = Explorer::new(ReaderPolicy::default()).unwrap();
        restored.reader().restore(&snapshot).unwrap();
        restored.frame(0);
        assert!(matches!(
            restored.rows(),
            [(
                _,
                Row::Agent {
                    status: Some(_),
                    conversation: Some(2),
                    ..
                }
            )]
        ));
        assert!(
            !restored.reader().poll(0).is_empty(),
            "restored listing must revalidate"
        );
    });
}

#[test]
fn finite_windows_retain_their_end_and_resume_the_original_follow() {
    futures::executor::block_on(async {
        let provider = Arc::new(MemoryStream::default());
        let source = StreamPath::new("windows/source").unwrap();
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![
                    b"first".as_slice().into(),
                    b"second".as_slice().into(),
                    b"third".as_slice().into(),
                ],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(b"window source".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        let child = StreamPath::new("windows/child").unwrap();
        provider
            .fork(ForkRequest {
                source,
                destination: child.clone(),
                at_tail: Some(3),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::new(b"window fork".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        for read_limit in 1..=3 {
            let mut reader = Reader::new(ReaderPolicy {
                read_limit,
                ..ReaderPolicy::default()
            })
            .unwrap();
            let mut driver = Driver::new(provider.clone());
            let ready = Arc::new(Ready::default());
            driver.attach(&mut reader, 0);
            // Cold selection must learn the actual head/origin before a read.
            let want = Want::Range {
                stream: child.clone(),
                from: 0,
                to: 40,
            };
            reader.plan([(want.clone(), Priority::Visible)]);
            drive(&mut reader, &mut driver, &ready, &want);
            assert_eq!(reader.cache().tail(&child), Some(3));
            assert!(reader.cache().has_range(&child, 0, 3));
            assert!(!reader.cache().has_range(&child, 0, 4));
            assert!(reader.poll(0).is_empty());
            let issued = reader.issued();
            let want = Want::Range {
                stream: child.clone(),
                from: 3,
                to: 40,
            };
            reader.plan([(want.clone(), Priority::Visible)]);
            drive(&mut reader, &mut driver, &ready, &want);
            assert_eq!(
                reader.issued(),
                issued,
                "cached rows fabricated a longer history"
            );
            reader.set_follows([child.clone()]);
            drive(&mut reader, &mut driver, &ready, &want);
            let head = provider.head(child.clone()).await.unwrap();
            let mut restored = Reader::new(ReaderPolicy::default()).unwrap();
            restored.restore(&reader.snapshot().unwrap()).unwrap();
            assert_eq!(
                restored.follow_anchor(&child, 3),
                Some(head.last.unwrap().commit_id)
            );
            reader.set_follows([]);
            drive(&mut reader, &mut driver, &ready, &want);
            assert_eq!(reader.follows().count(), 0);
        }
    });
}

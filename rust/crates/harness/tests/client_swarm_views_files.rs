//! Live and reopened renderers fold the same canonical files, never model events.
use acyclic_harness::{
    client::{ClientInterests, ClientViewLimits, ClientViews, QueryViewLimits, SwarmUpdate},
    task_volume::{Channel, FileKey, HeadFile, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, ReadRequest, StreamPath, StreamProvider, wire, wire_codec};
use futures::StreamExt as _;
use serde_json::json;
use std::sync::Arc;

fn path(value: &str) -> StreamPath {
    StreamPath::new(value).unwrap()
}
fn key(path: &str) -> FileKey {
    FileKey::parse(path).unwrap()
}
fn empty() -> ClientViews {
    let mut views = ClientViews::new(
        path("swarm"),
        ClientViewLimits {
            agents: 1,
            selected: 1,
            overview_values: 8,
            overview_bytes: 8192,
            query: QueryViewLimits {
                files: 64,
                segments: 128,
                bytes: 65_536,
            },
        },
    )
    .unwrap();
    views
        .apply(SwarmUpdate::Layout(
            ClientInterests::from_layout(path("swarm"), [path("swarm/a")], [], 1).unwrap(),
        ))
        .unwrap();
    views
}
fn bytes(file: &HeadFile) -> Vec<u8> {
    file.segments
        .iter()
        .flat_map(|segment| segment.iter().copied())
        .collect()
}
struct Fixture {
    provider: Arc<MemoryStream>,
    volume: TaskVolume<MemoryStream>,
    views: ClientViews,
}
impl Fixture {
    async fn new() -> Self {
        let provider = Arc::new(MemoryStream::default());
        let volume = TaskVolume::open(provider.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        let mut views = empty();
        views
            .apply(SwarmUpdate::Snapshot {
                agent: path("swarm/a"),
                snapshot: volume.heads(&Query::Root, None, 64).await.unwrap(),
            })
            .unwrap();
        Self {
            provider,
            volume,
            views,
        }
    }
    async fn append(&mut self, write: Write) -> wire::SessionData {
        let cut = self.views.cursor(&path("swarm/a")).unwrap().position;
        self.volume
            .append(self.volume.head().await.unwrap(), &write)
            .await
            .unwrap();
        let record = self
            .provider
            .read(ReadRequest {
                path: path("swarm/a/files"),
                from: cut,
                limit: 1,
            })
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        let cursor = self
            .volume
            .heads(&Query::Root, None, 64)
            .await
            .unwrap()
            .cursor;
        let data = wire::SessionData {
            after: Some(wire::SessionCursor {
                next: record.sequence + 1,
                ..Default::default()
            }),
            accepted: cursor.accepted,
            durable: cursor.durable,
            item: Some(wire::session_data::Item::Record(wire_codec::record_wire(
                record,
            ))),
        };
        self.views
            .apply(SwarmUpdate::Data {
                agent: path("swarm/a"),
                overview: false,
                data: data.clone(),
            })
            .unwrap();
        data
    }
    async fn reopened(&self) -> ClientViews {
        let mut views = empty();
        let reopened = TaskVolume::open_existing(self.provider.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        views
            .apply(SwarmUpdate::Snapshot {
                agent: path("swarm/a"),
                snapshot: reopened.heads(&Query::Root, None, 64).await.unwrap(),
            })
            .unwrap();
        views
    }
}
async fn projection(chunks: &[String], tokens: &[u16], structured: bool, omit_file: bool) {
    let mut fixture = Fixture::new().await;
    let invocation = json!({"call_id":"call", "name":"bash", "arguments":{"command":"pwd"}});
    let call = key(".tasks/t/turns/7/tools/call/request.json");
    fixture
        .append(Write::new(
            call.clone(),
            serde_json::to_vec(&invocation).unwrap(),
        ))
        .await;
    for (index, chunk) in chunks.iter().enumerate() {
        fixture
            .append(Write::new(
                key(&format!(".tasks/t/turns/7/tools/call/stdout/{index}")),
                chunk.as_bytes(),
            ))
            .await;
        let mut write = Write::new(
            key(&format!(".tasks/t/turns/7/chunks/{index}")),
            chunk.as_bytes(),
        );
        write.channel = Some(Channel::Text);
        fixture.append(write).await;
    }
    let mut reasoning = Write::new(
        key(&format!(".tasks/t/turns/7/chunks/{}", chunks.len())),
        b"thinking".as_slice(),
    );
    reasoning.channel = Some(Channel::Reasoning);
    fixture.append(reasoning).await;
    let output = if structured {
        acyclic_harness::model::ToolResultContent::Json {
            value: json!({"cwd":"/workspace", "exit_code":0}),
        }
    } else {
        acyclic_harness::model::ToolResultContent::Parts {
            parts: vec![acyclic_harness::model::ModelDataPart::Text {
                text: chunks.concat(),
            }],
        }
    };
    let output = serde_json::to_value(output).unwrap();
    let result = key(".tasks/t/turns/7/tools/call/result.json");
    fixture
        .append(Write::new(
            result.clone(),
            serde_json::to_vec(&output).unwrap(),
        ))
        .await;
    let total: u64 = tokens.iter().map(|n| u64::from(*n)).sum();
    let completion = key(".tasks/t/turns/7/completion.json");
    fixture
        .append(Write::new(
            completion.clone(),
            serde_json::to_vec(&json!({"tokens":total})).unwrap(),
        ))
        .await;
    // An older turn's canonical completion must not erase the current turn.
    let old = fixture
        .append(Write::new(
            key(".tasks/t/turns/1/completion.json"),
            b"{}".as_slice(),
        ))
        .await;
    let current = fixture
        .views
        .file(&path("swarm/a"), &key(".tasks/t/turns/7/reasoning.md"))
        .unwrap();
    assert_eq!(bytes(current), b"thinking");
    let replayed = fixture.reopened().await;
    let text = replayed.file(&path("swarm/a"), &key(".tasks/t/turns/7/text.md"));
    if chunks.is_empty() {
        assert!(text.is_none());
    } else {
        assert_eq!(bytes(text.unwrap()), chunks.concat().as_bytes());
    }
    for (index, chunk) in chunks.iter().enumerate() {
        let output = replayed
            .file(
                &path("swarm/a"),
                &key(&format!(".tasks/t/turns/7/tools/call/stdout/{index}")),
            )
            .unwrap();
        assert_eq!(bytes(output), chunk.as_bytes());
    }
    let expected: Vec<_> = replayed.files(&path("swarm/a")).cloned().collect();
    let actual: Vec<_> = fixture
        .views
        .files(&path("swarm/a"))
        .filter(|f| !omit_file || f.key != call)
        .cloned()
        .collect();
    assert_eq!(actual, expected, "live and reopened files differ");
    for (key, value) in [
        (call, invocation),
        (result, output),
        (completion, json!({"tokens":total})),
    ] {
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes(
                replayed.file(&path("swarm/a"), &key).unwrap()
            ))
            .unwrap(),
            value
        );
    }
    assert_eq!(
        fixture.views.cursor(&path("swarm/a")),
        replayed.cursor(&path("swarm/a"))
    );
    let before = fixture.views.clone();
    assert!(
        fixture
            .views
            .apply(SwarmUpdate::Data {
                agent: path("swarm/a"),
                overview: false,
                data: old
            })
            .is_err()
    );
    assert_eq!(
        fixture.views, before,
        "delayed replay changed the current files"
    );
}
#[test]
fn streaming_tools_usage_and_turn_isolation_survive_reopen() {
    for structured in [false, true] {
        futures::executor::block_on(projection(
            &["first".into(), " second".into()],
            &[2, 3],
            structured,
            false,
        ));
    }
}
proptest::proptest! {
    // Property: exact file contents and cuts are independent of chunk boundaries,
    // provider step usage and output shape, on live follow and after reopening.
    #[test]
    fn file_replay_equals_live_for_arbitrary_chunking_and_usage(
        chunks in proptest::collection::vec(".{0,16}", 0..5),
        tokens in proptest::collection::vec(0u16..1000, 0..5),
        structured in proptest::bool::ANY,
    ) { futures::executor::block_on(projection(&chunks, &tokens, structured, false)); }
}
#[test]
fn negative_control_omitting_a_canonical_file_is_detected() {
    assert!(
        std::panic::catch_unwind(|| futures::executor::block_on(projection(&[], &[], false, true)))
            .is_err()
    );
}

async fn notifications(rows: &[(u8, u8, String)], omit: bool) {
    use acyclic_harness::client::NotificationFileView;
    let mut fixture = Fixture::new().await;
    let mut expected = std::collections::BTreeMap::new();
    for (index, (owner, event, detail)) in rows.iter().enumerate() {
        let task = format!("t{owner}");
        let id = format!("n{index}");
        let mut record =
            super::notification_files::encoded(*event, index as u64 + 1, detail.clone());
        record.task = task.clone();
        let invalid = event & 0x80 != 0;
        let payload = if invalid {
            b"{invalid".to_vec()
        } else {
            serde_json::to_vec(&record).unwrap()
        };
        fixture
            .append(Write::new(
                key(&format!(".tasks/{task}/notifications/{id}.json")),
                payload,
            ))
            .await;
        expected.insert(
            (task, id),
            if invalid {
                NotificationFileView::Invalid
            } else {
                NotificationFileView::Present {
                    notification: record,
                }
            },
        );
        for view in [&fixture.views, &fixture.reopened().await] {
            let actual = view
                .notifications()
                .filter(|_| !omit)
                .map(|row| {
                    assert_eq!(row.cursor, view.cursor(&path("swarm/a")).unwrap());
                    (
                        (row.task.as_str().to_owned(), row.id.as_str().to_owned()),
                        row.notification.clone(),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(actual, expected);
        }
    }
    fixture
        .views
        .apply(SwarmUpdate::Layout(
            ClientInterests::from_layout(path("swarm"), [], [], 1).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        fixture.views.notifications().count(),
        0,
        "unselected content retained notifications"
    );
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn notification_files_are_identical_on_live_follow_and_reopen(
        rows in proptest::collection::vec((0u8..3, proptest::num::u8::ANY, ".{0,24}"), 0..12),
    ) { futures::executor::block_on(notifications(&rows, false)); }
}
#[test]
fn notification_file_fold_control_cannot_drop_settle_goal_or_approval() {
    assert!(
        std::panic::catch_unwind(|| futures::executor::block_on(notifications(
            &[(0, 0, "settled".into()), (1, 4, "goal blocked".into())],
            true,
        )))
        .is_err()
    );
    futures::executor::block_on(notifications(
        &[
            (0, 0, "settled".into()),
            (1, 4, "goal blocked".into()),
            (2, 5, "approval requested".into()),
        ],
        false,
    ));
}

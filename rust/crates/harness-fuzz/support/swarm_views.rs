//! Independent state oracle over real task publications and canonical session data.
use acyclic_harness::{
    client::{
        ClientInterests, ClientViewLimits, ClientViews, InterestTier, QueryViewLimits, SwarmUpdate,
    },
    task_volume::{Channel, FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, ReadRequest, StreamPath, StreamProvider, wire, wire_codec};
use bytes::Bytes;
use futures::StreamExt as _;
use prost::Message as _;
use std::{collections::BTreeMap, sync::Arc};

fn path(value: &str) -> StreamPath {
    StreamPath::new(value).unwrap()
}
fn layout(visible: bool, selected: bool) -> ClientInterests {
    ClientInterests::from_layout(
        path("swarm"),
        if visible {
            vec![path("swarm/a")]
        } else {
            vec![]
        },
        if selected && !visible {
            vec![path("swarm/a")]
        } else {
            vec![]
        },
        1,
    )
    .unwrap()
}
fn chunk(sequence: u64, bytes: &[u8]) -> Write {
    let mut write = Write::new(
        FileKey::parse(&format!(".tasks/t/turns/1/chunks/{sequence}")).unwrap(),
        bytes.to_vec(),
    );
    write.channel = Some(Channel::Text);
    write
}
async fn data(provider: &MemoryStream, sequence: u64) -> wire::SessionData {
    let mut read = provider
        .read(ReadRequest {
            path: path("swarm/a/files"),
            from: sequence,
            limit: 1,
        })
        .await
        .unwrap();
    let record = read.next().await.unwrap().unwrap();
    wire::SessionData {
        after: Some(wire::SessionCursor {
            next: sequence + 1,
            ..Default::default()
        }),
        accepted: sequence + 1,
        durable: 0,
        item: Some(wire::session_data::Item::Record(wire_codec::record_wire(
            record,
        ))),
    }
}
fn status(next: u64, key: &'static [u8], value: &[u8]) -> wire::SessionData {
    wire::SessionData {
        after: Some(wire::SessionCursor {
            next,
            opaque: Bytes::from(vec![next as u8]),
        }),
        accepted: next,
        durable: 0,
        item: Some(wire::session_data::Item::LatestValue(
            wire::SessionLatestValue {
                key: Bytes::from_static(key),
                value: Bytes::copy_from_slice(value),
                attention: true,
            },
        )),
    }
}
fn check_watermark(views: &ClientViews, position: u64, accepted: u64, durable: u64) {
    let cursor = views.cursor(&path("swarm/a")).unwrap();
    assert_eq!(
        cursor.position, position,
        "watermarks cannot consume file data"
    );
    assert!(cursor.accepted >= accepted);
    assert!(
        cursor.durable >= durable,
        "standalone durability must reach the view"
    );
}

pub fn exercise(input: &[u8], skip_watermark: bool) {
    check_discovery(input, false);
    exercise_fault(input, skip_watermark, false);
}

/// Discovery is idempotent and visible before any agent publishes status.
pub fn check_discovery(input: &[u8], drop_updates: bool) {
    let mut views = ClientViews::new(
        path("swarm"),
        ClientViewLimits {
            agents: 8,
            selected: 0,
            overview_values: 0,
            overview_bytes: 0,
            query: QueryViewLimits {
                files: 0,
                segments: 0,
                bytes: 0,
            },
        },
    )
    .unwrap();
    let mut expected = std::collections::BTreeSet::new();
    for id in input {
        let agent = path(&format!("swarm/a{}", id % 8));
        expected.insert(agent.clone());
        if !drop_updates {
            views.apply(SwarmUpdate::Agent { agent }).unwrap();
        }
    }
    assert_eq!(
        views
            .agents()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        expected
    );
    for agent in views.agents() {
        assert_eq!(views.status(agent).unwrap().values().count(), 0);
        assert!(views.cursor(agent).is_none());
    }
}

#[cfg(test)]
pub fn exercise_without_attention(input: &[u8]) {
    exercise_fault(input, false, true);
}

fn exercise_fault(input: &[u8], skip_watermark: bool, drop_attention: bool) {
    futures::executor::block_on(async {
        let bytes = &input[..input.len().min(32)];
        let provider = Arc::new(MemoryStream::default());
        let volume = TaskVolume::open(provider.clone(), path("swarm/a/files"))
            .await
            .unwrap();
        let limits = ClientViewLimits {
            agents: 4,
            selected: 1,
            overview_values: 4,
            overview_bytes: 4096,
            query: QueryViewLimits {
                files: 16,
                segments: 16,
                bytes: 4096,
            },
        };
        let mut views = ClientViews::new(path("swarm"), limits).unwrap();
        views
            .apply(SwarmUpdate::Layout(layout(false, true)))
            .unwrap();
        for agent in ["swarm", "elsewhere/a", "swarm-sibling/a"] {
            assert!(
                views
                    .apply(SwarmUpdate::Agent { agent: path(agent) })
                    .is_err()
            );
            assert_eq!(views.agents().count(), 0);
        }
        let wrong_selection = volume
            .heads(&Query::File(FileKey::Goal), None, 16)
            .await
            .unwrap();
        assert!(
            views
                .apply(SwarmUpdate::Snapshot {
                    agent: path("swarm/a"),
                    snapshot: wrong_selection,
                })
                .is_err(),
            "selected content must use the complete root query"
        );
        assert_eq!(views.agents().count(), 0);
        let snapshot = volume.heads(&Query::Root, None, 16).await.unwrap();
        let cut = snapshot.cursor.position;
        views
            .apply(SwarmUpdate::Snapshot {
                agent: path("swarm/a"),
                snapshot,
            })
            .unwrap();
        volume
            .append(volume.head().await.unwrap(), &chunk(1, bytes))
            .await
            .unwrap();
        let original = data(&provider, cut).await;
        let decoded = wire::SessionData::decode(original.encode_to_vec().as_slice())
            .expect("generated frame must decode");
        assert_eq!(decoded, original);
        views
            .apply(SwarmUpdate::Data {
                agent: path("swarm/a"),
                overview: false,
                data: decoded,
            })
            .unwrap();
        volume
            .append(volume.head().await.unwrap(), &chunk(2, b"tail"))
            .await
            .unwrap();
        if !skip_watermark {
            views
                .apply(SwarmUpdate::Watermark {
                    agent: path("swarm/a"),
                    overview: false,
                    watermark: wire::SessionWatermark {
                        accepted: cut + 2,
                        durable: cut + 1,
                    },
                })
                .unwrap();
        }
        check_watermark(&views, cut + 1, cut + 2, cut + 1);
        // Data already queued before a control watermark retains its older
        // metadata, but its delivery must preserve the newer confirmation.
        views
            .apply(SwarmUpdate::Data {
                agent: path("swarm/a"),
                overview: false,
                data: data(&provider, cut + 1).await,
            })
            .unwrap();
        check_watermark(&views, cut + 2, cut + 2, cut + 1);
        let text = FileKey::parse(".tasks/t/turns/1/text.md").unwrap();
        let file = views.file(&path("swarm/a"), &text).unwrap();
        assert_eq!(file.segments.len(), 2);
        assert_eq!(file.segments[0].as_ref(), bytes);
        assert_eq!(file.segments[1].as_ref(), b"tail");
        let pointer = file.segments[0].as_ptr();
        let cursor = views.cursor(&path("swarm/a")).unwrap().clone();
        views
            .apply(SwarmUpdate::Layout(layout(true, true)))
            .unwrap();
        assert_eq!(
            views.layout().tier(&path("swarm/a")),
            Some(InterestTier::InView)
        );
        assert_eq!(views.cursor(&path("swarm/a")), Some(&cursor));
        assert_eq!(
            views.file(&path("swarm/a"), &text).unwrap().segments[0].as_ptr(),
            pointer
        );

        // Conflated entries at the same cut are distinct keys. A later cut can
        // update both, including the second key after the global cursor moves.
        let mut expected = BTreeMap::new();
        for (next, key, value) in [
            (1, b"a".as_slice(), bytes),
            (1, b"b".as_slice(), b"old".as_slice()),
            (2, b"a".as_slice(), b"new".as_slice()),
            (2, b"b".as_slice(), bytes),
        ] {
            let original = status(next, key, value);
            let mut decoded = wire::SessionData::decode(original.encode_to_vec().as_slice())
                .expect("generated latest value must decode");
            assert_eq!(decoded, original);
            if drop_attention {
                let Some(wire::session_data::Item::LatestValue(value)) = decoded.item.as_mut()
                else {
                    unreachable!()
                };
                value.attention = false;
            }
            views
                .apply(SwarmUpdate::Data {
                    agent: path("swarm/a"),
                    overview: true,
                    data: decoded,
                })
                .unwrap();
            expected.insert(key.to_vec(), (value.to_vec(), true));
            let actual: BTreeMap<_, _> = views
                .status(&path("swarm/a"))
                .unwrap()
                .values()
                .map(|value| (value.key.to_vec(), (value.value.to_vec(), value.attention)))
                .collect();
            assert_eq!(actual, expected);
        }
        views
            .apply(SwarmUpdate::Watermark {
                agent: path("swarm/a"),
                overview: true,
                watermark: wire::SessionWatermark {
                    accepted: 2,
                    durable: 2,
                },
            })
            .unwrap();
        assert_eq!(
            views.status(&path("swarm/a")).unwrap().watermarks().durable,
            2
        );
        if let Ok(data) = wire::SessionData::decode(input) {
            assert_eq!(
                wire::SessionData::decode(data.encode_to_vec().as_slice()).unwrap(),
                data
            );
            let mut status_views = ClientViews::new(path("swarm"), limits).unwrap();
            let expected = match &data.item {
                Some(wire::session_data::Item::LatestValue(value)) => Some(value.clone()),
                _ => None,
            };
            let after = data.after.clone();
            let watermarks = (data.accepted, data.durable);
            if status_views
                .apply(SwarmUpdate::Data {
                    agent: path("swarm/a"),
                    overview: true,
                    data: data.clone(),
                })
                .is_ok()
            {
                let actual = status_views.status(&path("swarm/a")).unwrap();
                assert_eq!(actual.cursor(), after.as_ref());
                assert_eq!(
                    (actual.watermarks().accepted, actual.watermarks().durable),
                    watermarks
                );
                assert_eq!(
                    actual.values().collect::<Vec<_>>(),
                    vec![expected.as_ref().unwrap()]
                );
            } else {
                assert!(
                    status_views.status(&path("swarm/a")).is_none(),
                    "invalid status cannot partially insert an agent"
                );
            }
            let before = views.cursor(&path("swarm/a")).unwrap().clone();
            if views
                .apply(SwarmUpdate::Data {
                    agent: path("swarm/a"),
                    overview: false,
                    data,
                })
                .is_err()
            {
                assert_eq!(views.cursor(&path("swarm/a")), Some(&before));
            }
        }
        views
            .apply(SwarmUpdate::Layout(layout(false, false)))
            .unwrap();
        assert!(views.cursor(&path("swarm/a")).is_none());
        assert_eq!(views.status(&path("swarm/a")).unwrap().values().count(), 2);
    });
}

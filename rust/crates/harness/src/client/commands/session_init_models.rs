use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn race<const RETRY: bool>(before_seed: bool) {
    loom::model(move || {
        loom::future::block_on(Box::pin(async {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("command-init-model").unwrap(),
                )
                .await
                .unwrap(),
            );
            let mut commands =
                CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1; 16]), 8192, 32)
                    .await
                    .unwrap();
            let request = files::FileValue::new(
                files::FileKey::Session {
                    session: component(commands.session.to_string()).unwrap(),
                    file: files::TaskFile::Request(
                        component(OperationId::from_bytes([2; 16]).to_string()).unwrap(),
                    ),
                },
                br#"{"kind":"prompt","text":"hello","source":null,"mentions":[]}"#.to_vec(),
            );
            let seed = ui("config", "extra.json").unwrap();
            // Both linearization orders at the real snapshot/create handoff.
            // Publication runs on a Loom thread over the real instrumented
            // task-volume/provider locks. No substitute mutex or inbox exists.
            if before_seed {
                let writer = volume.clone();
                let request = request.clone();
                loom::thread::spawn(move || {
                    loom::future::block_on(Box::pin(async {
                        let cut = writer.head().await.unwrap();
                        writer.append(cut, &request).await.unwrap();
                    }))
                })
                .join()
                .unwrap();
            }
            commands
                .initialize_file::<RETRY>(seed.clone(), b"{}".to_vec())
                .await
                .unwrap();
            if !before_seed {
                let cut = volume.head().await.unwrap();
                volume.append(cut, &request).await.unwrap();
            }
            let restored = CommandSession::open(volume.clone(), commands.session, 8192, 32)
                .await
                .unwrap();
            assert_eq!(
                restored.files.get(&request.key),
                Some(&request),
                "startup lost a request"
            );
            assert_eq!(restored.files.get(&seed).unwrap().bytes.as_ref(), b"{}");
        }));
    });
}

#[test]
fn initialization_preserves_a_request_on_both_sides_of_seed_publication() {
    race::<true>(false);
    race::<true>(true);
}

#[test]
fn fail_on_initialization_contention_is_detected_negative_control() {
    assert!(std::panic::catch_unwind(|| race::<false>(true)).is_err());
}

fn peer_edit<const MATCH_KEY: bool>(same_file: bool) {
    loom::model(move || {
        loom::future::block_on(Box::pin(async {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("command-key-model").unwrap(),
                )
                .await
                .unwrap(),
            );
            let mut commands =
                CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1; 16]), 8192, 32)
                    .await
                    .unwrap();
            let key = ui("config", "extra.json").unwrap();
            commands
                .initialize_file::<true>(key.clone(), b"{}".to_vec())
                .await
                .unwrap();
            let expected = commands.generation;
            let peer_key = if same_file {
                key.clone()
            } else {
                ui("config", "peer.json").unwrap()
            };
            let peer = files::FileValue::new(peer_key, br#"{"peer":true}"#.to_vec());
            volume.append(expected, &peer).await.unwrap();
            let result = commands
                .publish_rebased::<true, MATCH_KEY>(
                    files::FileValue::new(key.clone(), b"null".to_vec()),
                    false,
                )
                .await;
            if same_file {
                assert!(
                    matches!(result, Err(Error::Conflict(_))),
                    "overwrote a concurrent edit"
                );
            } else {
                result.unwrap();
                let snapshot = volume.snapshot(&files::Query::File(key), 1).await.unwrap();
                assert_eq!(snapshot.files[0].bytes.as_ref(), b"null");
            }
            let snapshot = volume
                .snapshot(&files::Query::File(peer.key.clone()), 1)
                .await
                .unwrap();
            assert_eq!(snapshot.files, [peer]);
        }));
    });
}

#[test]
fn a_same_file_edit_is_never_overwritten_by_rebasing() {
    peer_edit::<true>(false);
    peer_edit::<true>(true);
}

#[test]
fn omitting_the_file_comparison_is_detected_negative_control() {
    assert!(std::panic::catch_unwind(|| peer_edit::<false>(true)).is_err());
}

fn input_handoff<const RETRY: bool, const MERGE_LAYOUT: bool>(
    peer_first: bool,
    prompt: bool,
    layout_peer: bool,
) {
    loom::model(move || {
        loom::future::block_on(Box::pin(async {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("input-handoff-model").unwrap(),
                )
                .await
                .unwrap(),
            );
            let mut input =
                CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1; 16]), 8192, 32)
                    .await
                    .unwrap();
            let operation = OperationId::from_bytes([4; 16]);
            let peer = if layout_peer {
                files::FileValue::new(
                    input
                        .state_key(CommandFileRoot::Session, "layout.json")
                        .unwrap(),
                    br#"{"peer":true}"#.to_vec(),
                )
            } else {
                files::FileValue::new(ui("config", "peer.json").unwrap(), b"true".to_vec())
            };
            let publish_peer = || {
                let volume = volume.clone();
                let mut peer = peer.clone();
                loom::thread::spawn(move || {
                    loom::future::block_on(Box::pin(async move {
                        let cut = volume.head().await.unwrap();
                        if layout_peer {
                            let snapshot = volume
                                .snapshot(&files::Query::File(peer.key.clone()), 1)
                                .await
                                .unwrap();
                            let mut layout: Value =
                                serde_json::from_slice(&snapshot.files[0].bytes).unwrap();
                            layout["peer"] = json!(true);
                            peer.bytes = serde_json::to_vec(&layout).unwrap().into();
                        }
                        volume.append(cut, &peer).await.unwrap();
                    }))
                })
                .join()
                .unwrap();
            };
            // The session read its generation above. Force both sides of the
            // real read/write handoff, with the actual instrumented provider.
            if peer_first {
                publish_peer();
            }
            let resolution = if prompt {
                CommandResolution::Prompt {
                    text: "hello".into(),
                    source: None,
                    mentions: Vec::new(),
                }
            } else {
                CommandResolution::Submit {
                    request: CommandRequest::Quit,
                }
            };
            let result = if MERGE_LAYOUT {
                input
                    .publish_input_resolution_at::<RETRY>(resolution.clone(), operation)
                    .await
            } else {
                input
                    .publish_resolution_with_retry::<RETRY>(resolution.clone(), operation)
                    .await
            }
            .unwrap();
            assert_eq!(result, operation);
            if !peer_first {
                publish_peer();
            }
            input.refresh_files().await.unwrap();
            let snapshot = volume.snapshot(&files::Query::Root, 32).await.unwrap();
            if layout_peer {
                assert_eq!(
                    input
                        .state(CommandFileRoot::Session, "layout.json")
                        .unwrap()
                        .unwrap()["peer"],
                    true
                );
            } else {
                assert!(snapshot.files.contains(&peer));
            }
            if prompt {
                let requests: Vec<_> = snapshot
                    .files
                    .iter()
                    .filter(|file| {
                        matches!(
                            file.key,
                            files::FileKey::Session {
                                file: files::TaskFile::Request(_),
                                ..
                            }
                        )
                    })
                    .collect();
                assert_eq!(requests.len(), 1, "retry duplicated a submission");
                assert_eq!(
                    requests[0].key,
                    files::FileKey::Session {
                        session: component(input.session.to_string()).unwrap(),
                        file: files::TaskFile::Request(input.request_id(operation).unwrap()),
                    }
                );
                // Replay the same publication identity; it is already accepted.
                input
                    .publish_resolution_with_retry::<true>(resolution, operation)
                    .await
                    .unwrap();
                assert_eq!(volume.head().await.unwrap(), snapshot.generation);
            } else {
                assert_eq!(
                    input
                        .state(CommandFileRoot::Session, "layout.json")
                        .unwrap()
                        .unwrap()["exit_requested"],
                    true
                );
            }
        }));
    });
}

#[test]
fn input_publications_survive_both_read_write_handoffs_without_new_identities() {
    for prompt in [false, true] {
        for peer_first in [false, true] {
            for layout_peer in [false, true] {
                input_handoff::<true, true>(peer_first, prompt, layout_peer);
            }
        }
    }
}
#[test]
fn fail_on_input_contention_is_detected_negative_control() {
    for prompt in [false, true] {
        assert!(
            std::panic::catch_unwind(|| input_handoff::<false, true>(true, prompt, false)).is_err()
        );
    }
}

#[test]
fn retaining_the_old_quit_layout_is_detected_negative_control() {
    assert!(std::panic::catch_unwind(|| input_handoff::<true, false>(true, false, true)).is_err());
}

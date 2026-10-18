use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

fn withdrawn<const REBASE: bool>(peer_first: bool) {
    loom::model(move || {
        loom::future::block_on(Box::pin(async {
            let volume = Arc::new(
                files::TaskVolume::open(
                    Arc::new(MemoryStream::default()),
                    StreamPath::new("confirmation-cut").unwrap(),
                )
                .await
                .unwrap(),
            );
            let mut commands =
                CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1; 16]), 8192, 32)
                    .await
                    .unwrap();
            let confirmation =
                json!({"operation":OperationId::from_bytes([2;16]),"request":{"kind":"clear_all"}});
            commands
                .write_state(
                    CommandFileRoot::Session,
                    "layout.json",
                    &json!({"confirmation":confirmation}),
                )
                .await
                .unwrap();
            let layout = commands
                .state_key(CommandFileRoot::Session, "layout.json")
                .unwrap();
            let request = files::FileKey::Session {
                session: component(commands.session.to_string()).unwrap(),
                file: files::TaskFile::Request(
                    component(OperationId::from_bytes([2; 16]).to_string()).unwrap(),
                ),
            };
            // This is the generation checked by confirm_request. Force both sides
            // of the real checked-cut/create handoff, with a withdrawal in between.
            let withdraw = || {
                let volume = volume.clone();
                let layout = layout.clone();
                loom::thread::spawn(move || {
                    loom::future::block_on(Box::pin(async move {
                        let cut = volume.head().await.unwrap();
                        volume
                            .append(cut, &files::FileValue::new(layout, b"{}".to_vec()))
                            .await
                            .unwrap();
                    }))
                })
                .join()
                .unwrap();
            };
            if peer_first {
                withdraw();
            }
            let bytes = serde_json::to_vec(&CommandRequest::ClearAll).unwrap();
            let result = if REBASE {
                commands
                    .publish_rebased::<true, true>(
                        files::FileValue::new(request.clone(), bytes),
                        true,
                    )
                    .await
            } else {
                commands.publish(request.clone(), bytes, true).await
            };
            if !peer_first {
                withdraw();
            }
            let saved = volume
                .snapshot(&files::Query::File(request), 1)
                .await
                .unwrap();
            if peer_first {
                assert!(
                    matches!(result, Err(Error::Conflict(_))),
                    "withdrawn confirmation admitted a deletion"
                );
                assert!(saved.files.is_empty());
            } else {
                result.unwrap();
                assert_eq!(saved.files.len(), 1);
            }
        }))
    });
}
#[test]
fn confirmation_publication_is_fenced_by_the_checked_layout_cut() {
    withdrawn::<false>(true);
    withdrawn::<false>(false);
}
#[test]
fn rebasing_past_withdrawal_is_detected_negative_control() {
    assert!(std::panic::catch_unwind(|| withdrawn::<true>(true)).is_err());
}

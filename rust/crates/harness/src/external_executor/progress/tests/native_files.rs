//! Real file-delivery drain over the shared instrumented push-provider model.
use super::*;
use crate::{
    client::jsonl,
    task_volume::{Change, Cursor, FileKey, FileValue, SCHEMA},
};

fn model(skip_drain: bool) {
    shuttle::check_dfs(
        move || {
            future::block_on(async {
                // Same sole provider-state lock and wake discipline as Changes.
                let state = Arc::new(Mutex::new(State::default()));
                let producer = state.clone();
                let writer = future::spawn(async move {
                    future::yield_now().await;
                    let wake = {
                        let mut state = producer.lock().unwrap();
                        state.revision = 1;
                        state.wake.take()
                    };
                    if let Some(wake) = wake {
                        wake.wake();
                    }
                });
                let file = FileValue::new(
                    FileKey::parse("workspace/native/manifest.json").unwrap(),
                    b"cut".to_vec(),
                );
                let delivered = file.clone();
                let mut changes = Changes { state, cursor: 0 }.map(move |event| {
                    event.unwrap();
                    Ok(Change {
                        file: Some(delivered.clone()),
                        files: Vec::new(),
                        removed: Vec::new(),
                        cursor: Cursor {
                            volume: [0; 32],
                            epoch: uuid::Uuid::nil(),
                            schema: SCHEMA,
                            query: [0; 32],
                            position: 1,
                            accepted: 1,
                            durable: 0,
                            commit: [1; 32],
                        },
                    })
                });
                let mut through = 0;
                let mut output = Vec::new();
                if !skip_drain {
                    crate::external_executor::native::files::drain(
                        &mut through,
                        1,
                        &mut changes,
                        &mut output,
                    )
                    .await
                    .unwrap();
                }
                assert_eq!(
                    through, 1,
                    "acknowledged before following its accepted file"
                );
                let change = jsonl::parse_change(std::str::from_utf8(&output).unwrap()).unwrap();
                assert_eq!(change.file, Some(file));
                assert_eq!(change.cursor.position, 1);
                assert_eq!(
                    change.cursor.durable, 0,
                    "delivery waited for a durability watermark"
                );
                writer.await.unwrap();
            });
        },
        None,
    );
}

#[test]
fn accepted_file_is_followed_before_the_next_upload_is_acknowledged() {
    model(false);
}

#[test]
#[should_panic(expected = "acknowledged before following its accepted file")]
fn negative_control_detects_acknowledgement_without_following() {
    model(true);
}

//! Provider follow and task-volume locks are the actual instrumented SDK paths.
use std::cell::Cell;

loom::thread_local! {static FROM_TAIL: Cell<bool> = Cell::new(false);}
loom::thread_local! {static KEEP_INVALID: Cell<bool> = Cell::new(false);}
pub(super) fn from_tail() -> bool {
    FROM_TAIL.with(Cell::get)
}

pub(super) fn keep_invalid() -> bool {
    KEEP_INVALID.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use crate::{
        external_executor::{ExternalHost, request},
        task_volume::{Component, FileKey, TaskFile, TaskVolume},
    };
    use acyclic_stream::MemoryStream;
    use futures::StreamExt;
    use serde_json::json;

    const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

    fn publish(files: &TaskVolume<MemoryStream>, request: &ExternalRequest) {
        let file = request
            .to_file(Component::new(request.task_id().to_string()).unwrap())
            .unwrap();
        loom::future::block_on(Box::pin(async {
            files
                .append(files.head().await.unwrap(), &file)
                .await
                .unwrap();
        }));
    }

    fn model(broken: bool, invalid: Option<bool>) {
        FROM_TAIL.with(|value| value.set(broken));
        KEEP_INVALID.with(|value| value.set(invalid == Some(true)));
        let provider = Arc::new(MemoryStream::default());
        let files = Arc::new(
            loom::future::block_on(Box::pin(TaskVolume::open(
                provider.clone(),
                StreamPath::new("request-model").unwrap(),
            )))
            .unwrap(),
        );
        let attach = request(
            ExternalHost::Pi,
            "session_start",
            json!({"sessionId":"model","cwd":"/project"}),
        )
        .unwrap();
        publish(&files, &attach);
        if invalid.is_some() {
            let mut rejected = attach.to_file(Component::new(attach.task_id().to_string()).unwrap()).unwrap();
            if let FileKey::Task {file, ..} = &mut rejected.key {
                *file = TaskFile::Request(Component::new("rejected").unwrap());
            }
            rejected.bytes = b"{malformed".as_slice().into();
            loom::future::block_on(Box::pin(async {
                files.append(files.head().await.unwrap(), &rejected).await.unwrap();
            }));
        }
        let next = request(
            ExternalHost::Pi,
            "tool_call",
            json!({
                "sessionId":"model", "toolCallId":"tool", "toolName":"read","input":{"path":"file"},
            }),
        )
        .unwrap();
        let writer = files.clone();
        let value = next.clone();
        let producer = loom::thread::spawn(move || {
            let file = value
                .to_file(Component::new(value.task_id().to_string()).unwrap())
                .unwrap();
            loom::future::block_on(Box::pin(async {
                writer
                    .append(writer.head().await.unwrap(), &file)
                    .await
                    .unwrap();
            }));
        });
        let mut accepted =
            loom::future::block_on(Box::pin(follow(provider.as_ref(), files.path().clone())))
                .unwrap();
        producer.join().unwrap();
        let sentinel = request(ExternalHost::Pi, "tool_call", json!({
            "sessionId":"model", "toolCallId":"sentinel", "toolName":"read","input":{"path":"file"},
        })).unwrap();
        publish(&files, &sentinel);
        let first = loom::future::block_on(Box::pin(accepted.next()))
            .unwrap()
            .unwrap();
        let second = if first == attach {
            Some(
                loom::future::block_on(Box::pin(accepted.next()))
                    .unwrap()
                    .expect("malformed file stopped a valid request"),
            )
        } else {
            None
        };
        // All modeled resources are cleaned up before a negative-control panic.
        drop(accepted);
        drop(files);
        drop(provider);
        assert_eq!(first, attach, "request replay lost its accepted prefix");
        assert_eq!(second, Some(next));
    }

    #[test]
    fn replay_and_follow_do_not_lose_a_concurrent_request() {
        loom::model(|| model(false, None));
    }

    #[test]
    #[should_panic(expected = "request replay lost its accepted prefix")]
    fn negative_control_detects_starting_after_the_accepted_prefix() {
        loom::model(|| model(true, None));
    }

    #[test]
    fn malformed_files_cannot_hide_a_concurrent_valid_request() {
        loom::model(|| model(false, Some(false)));
    }

    #[test]
    #[should_panic(expected = "malformed file stopped a valid request")]
    fn negative_control_detects_promoting_a_file_error_to_the_provider() {
        loom::model(|| model(false, Some(true)));
    }
}

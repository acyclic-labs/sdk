use super::super::{
    PromptInput, Viewport,
    ratatui::{Terminal, backend::TestBackend},
};
use super::*;
use crate::{
    OperationId, TaskId,
    client::{CommandSession, TaskFileClient},
    task_volume::{Component, FileKey, FileValue, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

async fn frame_allocations<const SCAN: bool>(completed: u8) -> (u64, u64) {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("busy-frame").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([17; 16]);
    let pending = OperationId::from_bytes([255; 16]).to_string();
    for i in 0..=completed {
        let operation = if i == completed {
            pending.clone()
        } else {
            OperationId::from_bytes([i + 1; 16]).to_string()
        };
        if i < completed {
            assert!(
                operation < pending,
                "pending identity must follow retained requests"
            );
        }
        let request = FileKey::Session {
            session: Component::new(session.to_string()).unwrap(),
            file: TaskFile::Request(Component::new(&operation).unwrap()),
        };
        let bytes = serde_json::to_vec(&crate::client::CommandResolution::Prompt {
            text: "turn".into(),
            source: None,
            mentions: vec![],
        })
        .unwrap();
        volume
            .append(
                volume.head().await.unwrap(),
                &FileValue::new(request, bytes),
            )
            .await
            .unwrap();
        if i < completed {
            let result = FileKey::Session {
                session: Component::new(session.to_string()).unwrap(),
                file: TaskFile::ExecutionResult(Component::new(operation).unwrap()),
            };
            let bytes = serde_json::to_vec(
                &serde_json::json!({"sequence":u64::from(i)+1,"ok":{"text":"done\n\n"}}),
            )
            .unwrap();
            volume
                .append(volume.head().await.unwrap(), &FileValue::new(result, bytes))
                .await
                .unwrap();
        }
    }
    let files = CommandSession::open(volume.clone(), session, 65536, 4096)
        .await
        .unwrap();
    assert!(files.pending_execution().unwrap());
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: TaskId::from_bytes([18; 16]).to_string(),
    };
    let mut client = TaskFileClient::observe(volume.as_ref(), &owner, 4096, 4 * 1024 * 1024)
        .await
        .unwrap();
    client.follow_commands(&files).await.unwrap();
    let mut renderer = ClientRenderer::default();
    let mut input = PromptInput::default();
    let mut prompt = crate::client::PromptFiles::default();
    let mut viewport = Viewport::default();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    let mut frame = |now| {
        renderer.update_files(&client, &owner);
        renderer.configure(client.views(), &owner, &files).unwrap();
        if SCAN {
            renderer.busy = files.pending_execution_scan_control().unwrap();
        }
        prompt.refresh(&files, &mut input).unwrap();
        input.refresh_completions();
        input.busy = renderer.is_busy();
        renderer.layout(&files, &input, 80, 20).unwrap();
        let screen = renderer.screen_for(&files).unwrap();
        terminal
            .draw(|frame| {
                let scroll = screen.tail_scroll(frame.area(), &input);
                assert!(
                    screen
                        .render(frame, &input, &mut viewport, scroll, now)
                        .animation
                );
            })
            .unwrap();
    };
    frame(0);
    let allocations = acyclic_test_support::measure(|| frame(crate::tui::Thinking::FRAME_MS));
    (allocations.count_total, allocations.bytes_total)
}
async fn model<const SCAN: bool>() {
    let n = frame_allocations::<SCAN>(16).await;
    let two_n = frame_allocations::<SCAN>(32).await;
    assert_eq!(
        n, two_n,
        "complete animation frame must be independent of retained turns"
    );
}
#[test]
fn complete_animation_frames_have_equal_n_and_2n_allocations() {
    futures::executor::block_on(model::<false>());
}
#[test]
#[should_panic(expected = "complete animation frame must be independent of retained turns")]
fn retained_request_scan_negative_control() {
    futures::executor::block_on(model::<true>());
}

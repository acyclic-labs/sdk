use super::*;
use crate::{
    TaskId,
    client::{CommandFileRoot, CommandSession, TaskFileClient, file_tui_inputs, input_burst},
    task_volume::{FileKey, FileValue, Query, TaskVolume},
    tui::{
        ClientRenderer, Editor, PromptInput, Screen, Viewport,
        key_event::{InputEvent, KeyModifiers, MouseEvent, MouseEventKind},
        ratatui::{Terminal, backend::TestBackend, layout::Rect},
    },
    tui_files::CompletionFiles,
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::FutureExt;
use std::sync::Arc;

struct NoFiles;
impl crate::conversation::ContentResidencyVerifier for NoFiles {}

async fn model<const BROKEN: bool>(motions: usize) {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("burst-mux").unwrap(),
        )
        .await
        .unwrap(),
    );
    let task = TaskId::from_bytes([9; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let mut commands =
        CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([8; 16]), 65536, 128)
            .await
            .unwrap();
    commands
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &serde_json::json!({"selected_task":task.to_string()}),
        )
        .await
        .unwrap();
    let mut client = TaskFileClient::observe_task(volume.as_ref(), &owner, task, 128, 65536)
        .await
        .unwrap();
    client.follow_commands(&commands).await.unwrap();
    let mut prompt = PromptInput::new(Editor::new("draft"));
    let mut completions = CompletionFiles::observe(volume.as_ref(), &mut prompt, None, 128)
        .await
        .unwrap();
    let mut observed = volume.observe(Query::Inputs, None, 128).await.unwrap();
    client
        .views_mut()
        .query_snapshot(Query::Inputs, observed.snapshot.take().unwrap(), 128, 65536)
        .unwrap();
    let mut documents = observed.changes;
    let mut renderer = ClientRenderer::default();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    let mut draws = 0;
    let mut render = |prompt: &PromptInput| {
        terminal
            .draw(|frame| {
                draws += 1;
                Screen::default().render(frame, prompt, &mut Viewport::default(), 0, 0);
            })
            .unwrap();
    };
    let mut input = futures::stream::iter([Ok((0..motions)
        .map(|_| {
            InputEvent::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: 3,
                row: 1,
                modifiers: KeyModifiers::NONE,
            })
        })
        .collect())])
    .chain(futures::stream::pending());
    let mut batch = next_event::<_, BROKEN>(
        &mut client,
        &mut input,
        &mut completions,
        &mut prompt,
        &mut commands,
        false,
        &mut renderer,
        &mut documents,
    )
    .await
    .unwrap()
    .unwrap();
    let effects = input_burst(&mut batch, 8, |event| {
        prompt.event(event, Rect::new(0, 19, 80, 1), &mut Viewport::default(), 0)
    });
    file_tui_inputs(&mut commands, &NoFiles, effects, 8, 10000)
        .await
        .unwrap();
    render(&prompt);
    loop {
        let ready = tokio::task::unconstrained(next_event::<_, BROKEN>(
            &mut client,
            &mut input,
            &mut completions,
            &mut prompt,
            &mut commands,
            false,
            &mut renderer,
            &mut documents,
        ))
        .now_or_never();
        match ready {
            Some(result) => {
                result.unwrap();
                render(&prompt);
            }
            None => break,
        }
    }
    // Command display selects outcomes, so layout updates are checkpoints in
    // every external follow. The input publication has already drawn once.
    assert_eq!(draws, 1, "checkpoint-only wakes must not draw");
    assert_eq!(crate::client::tui_scroll(&commands).unwrap(), motions * 3);
    // A real matching completion publication still wakes.
    let cut = volume.snapshot(&Query::Root, 128).await.unwrap().generation;
    volume
        .append(
            cut,
            &FileValue::new(
                FileKey::parse("commands/extra.json").unwrap(),
                br#"[{"name":"extra"}]"#.to_vec(),
            ),
        )
        .await
        .unwrap();
    assert!(
        tokio::task::unconstrained(next_event::<_, BROKEN>(
            &mut client,
            &mut input,
            &mut completions,
            &mut prompt,
            &mut commands,
            false,
            &mut renderer,
            &mut documents
        ))
        .now_or_never()
        .unwrap()
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn wheel_bursts_draw_only_for_input_and_visible_publications() {
    for motions in [32, 64] {
        model::<false>(motions).await;
    }
}

#[tokio::test]
#[should_panic(expected = "checkpoint-only wakes must not draw")]
async fn negative_control_redrawing_every_checkpoint_is_detected() {
    model::<true>(32).await;
}

//! Production owner, canonical chunks, live projection and cancellation.
use super::*;
use crate::{
    client::{self, CommandSession},
    task_volume::{Component, FileKey, Query, TaskFile},
    tui::{
        ClientRenderer, PromptInput, Viewport,
        ratatui::{Terminal, backend::TestBackend, style::Modifier},
    },
};
use futures::FutureExt as _;

#[tokio::test]
async fn canonical_chunks_render_before_completion_and_cancel_settles() -> Result<()> {
    streaming(true).await
}

#[tokio::test]
async fn negative_control_missing_runtime_binding_has_no_intermediate_frame() -> Result<()> {
    streaming(false).await
}

async fn streaming(bound: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let (started, boundary) = tokio::sync::oneshot::channel();
    let started = Mutex::new(Some(started));
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "**intermediate answer**".into(),
            }),
            ScriptedEvent::Pending {},
        ])])
        .with_context_capacity(capacity())
        .with_dispatch_observer(move |_| {
            if let Some(started) = started.lock().unwrap().take() {
                let _ = started.send(());
            }
        }),
    );
    let mut local =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    if !bound {
        Arc::get_mut(&mut local.runtime)
            .expect("unshared runtime")
            .bind_task_files(None);
    }
    let session = OperationId::new();
    let authority = local.client_owner();
    let mut ui = client::local_ui(&local, &session.to_string(), &authority).await?;
    let reader = local.client_reader()?;
    ui.submit("initial prompt", reader.as_ref()).await?;
    let mut owner = CommandSession::open_local(
        &local,
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        4096,
    )
    .await?;
    let request = owner.next_request().await?;
    let mut views =
        client::TaskFileClient::observe(local.task_files().as_ref(), &authority, 4096, 65_536)
            .await?;
    {
        let run = client::run_local_command(&mut owner, &local, &request);
        futures::pin_mut!(run);
        tokio::select! {
            result = &mut run => panic!("turn ended before streaming: {result:?}"),
            result = boundary => result.expect("model dispatch"),
        }
        if bound {
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if let Some(task)=ui.active_task() {
                    views.follow_task(local.task_files().as_ref(),task).await?;
                }
                let mut renderer = ClientRenderer::default();
                renderer.update(views.views(), &authority);
                let mut terminal =
                    Terminal::new(TestBackend::new(80, 20)).map_err(super::super::storage)?;
                terminal
                    .draw(|frame| {
                        renderer.screen().render(
                            frame,
                            &PromptInput::default(),
                            &mut Viewport::default(),
                            0,
                            0,
                        );
                    })
                    .map_err(super::super::storage)?;
                let cells = &terminal.backend().buffer().content;
                let text: String = cells.iter().map(|cell| cell.symbol()).collect();
                if text.contains("intermediate answer") {
                    assert!(!text.contains("**intermediate answer**"));
                    let start = cells
                        .windows(19)
                        .position(|window| {
                            window.iter().map(|cell| cell.symbol()).collect::<String>()
                                == "intermediate answer"
                        })
                        .unwrap();
                    assert!(
                        cells[start..start + 19]
                            .iter()
                            .all(|cell| cell.modifier.contains(Modifier::BOLD))
                    );
                    break;
                }
                tokio::select! {
                    result = &mut run => panic!("turn completed before intermediate frame: {result:?}"),
                    result = views.next_change() => result?,
                }
            }
                Ok::<(), Error>(())
            }).await.expect("canonical chunk must reach the styled frame")?;
            // The same request publication used by Ctrl+C, not an injected status.
            // Accepted chunks may be observed before their publisher receives
            // its acknowledgement. Keep driving the owner while publishing
            // control input, just as with_local_commands does in production.
            let (published, settled) =
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    tokio::join!(ui.cancel_active(), run)
                })
                .await
                .expect("cancel publication and owner must progress together");
            published?;
            assert!(matches!(
                settled,
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                ))
            ));
            let task = crate::runtime::task_id_for_admission(
                crate::task_volume::command_query::identity(match &request.key {
                    FileKey::Task {
                        file: TaskFile::Request(id),
                        ..
                    } => id,
                    _ => panic!("expected task request"),
                })
                .unwrap()
                .1,
            );
            assert!(matches!(
                local.runtime.task_host().outcome(task).await?,
                Some(crate::Outcome::Cancelled)
            ));
            let status = local
                .task_files()
                .snapshot(
                    &Query::File(FileKey::Task {
                        task: Component::new(task.to_string())?,
                        file: TaskFile::Status,
                    }),
                    1,
                )
                .await?;
            assert_eq!(
                serde_json::from_slice::<Value>(&status.files[0].bytes).unwrap()["state"],
                "cancelled"
            );
        } else {
            assert!(run.as_mut().now_or_never().is_none());
            let snapshot = local.task_files().snapshot(&Query::Tasks, 4096).await?;
            assert!(!snapshot.files.iter().any(|file| matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::Chunk { .. },
                    ..
                }
            )));
        }
    }
    assert_eq!(provider.requests().len(), 1);
    drop(views);
    drop(owner);
    drop(ui);
    drop(reader);
    local.disconnect().await
}

#[tokio::test]
async fn edited_prompt_is_an_immutable_replacement_branch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("replacement".into()),
    ]);
    let local =
        LocalHarness::open(volume_config(directory.path(), project, provider.clone())?).await?;
    let session = OperationId::new();
    let mut ui = client::local_ui(&local, &session.to_string(), &local.client_owner()).await?;
    let reader = local.client_reader()?;
    let original = ui.submit("original wording", reader.as_ref()).await?;
    let mut owner = CommandSession::open_local(
        &local,
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        4096,
    )
    .await?;
    let request = owner.next_request().await?;
    client::run_local_command(&mut owner, &local, &request).await?;
    ui.refresh_files().await?;
    client::apply_tui_input(
        &mut ui,
        reader.as_ref(),
        crate::tui::InputEffect::Submit {
            text: "corrected wording".into(),
            queued: false,
            edit: Some(original.to_string()),
        },
    )
    .await?;
    let replacement = owner.next_request().await?;
    let output = client::run_local_command(&mut owner, &local, &replacement)
        .await?
        .unwrap();
    assert_eq!(output["text"], "replacement");
    assert_eq!(output["replaces"], json!(original));
    assert_ne!(
        output["task"],
        json!(crate::runtime::task_id_for_admission(original))
    );
    let sent = serde_json::to_string(&provider.requests()[1]).map_err(super::super::storage)?;
    assert!(sent.contains("corrected wording"));
    assert!(!sent.contains("original wording"));
    let retained = local
        .task_files()
        .snapshot(&Query::File(request.key.clone()), 1)
        .await?;
    assert_eq!(retained.files.first(), Some(&request));
    drop(owner);
    drop(ui);
    drop(reader);
    local.close().await
}

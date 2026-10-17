//! Harness client behavior rendered through the same TestBackend used by TUI.
use super::*;
use acyclic_harness::{
    OperationId, TaskId,
    client::{CommandControl, CommandFileRoot as Root, CommandSession, run_command_client},
    task_volume::{FileKey, FileValue, Query, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::stream;
use ratatui::{Terminal, backend::TestBackend, widgets::Paragraph};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

async fn files() -> Arc<TaskVolume<MemoryStream>> {
    Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("driver").unwrap(),
        )
        .await
        .unwrap(),
    )
}
async fn session(volume: Arc<TaskVolume<MemoryStream>>) -> CommandSession<MemoryStream> {
    CommandSession::open(volume, uuid::Uuid::from_bytes([1; 16]), 65_536, 128)
        .await
        .unwrap()
}
fn draw(terminal: &mut Terminal<TestBackend>, output: &mut String, text: &str) -> Result<()> {
    output.push_str(text);
    terminal
        .draw(|frame| frame.render_widget(Paragraph::new(output.as_str()), frame.area()))
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}
async fn control_file(volume: &TaskVolume<MemoryStream>, task: TaskId, paused: bool) -> Result<()> {
    let key = FileKey::parse(".ui/config/task-control.json").map_err(volume_error)?;
    loop {
        let cut = volume
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(volume_error)?
            .generation;
        let bytes = serde_json::to_vec(
            &json!({"all":null,"tasks":{(task.to_string()):[cut.position,paused]}}),
        )
        .unwrap();
        match volume
            .append(cut, &FileValue::new(key.clone(), bytes))
            .await
        {
            Ok(_) => return Ok(()),
            Err(acyclic_harness::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(volume_error(error)),
        }
    }
}
async fn eof_case(fail: bool) -> Result<()> {
    let volume = files().await;
    let client = session(volume).await;
    let eof = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let mut lines = VecDeque::from([Ok("/shell effect".to_owned())]);
    let input_eof = eof.clone();
    let input = stream::poll_fn(move |_| {
        if let Some(line) = lines.pop_front() {
            std::task::Poll::Ready(Some(line))
        } else {
            input_eof.notify_one();
            std::task::Poll::Ready(None)
        }
    });
    let mut terminal = Terminal::new(TestBackend::new(120, 80)).unwrap();
    let mut output = String::new();
    let completion = completed.clone();
    let result = run_command_client(
        client,
        input,
        &DeniedReader,
        move |_, _, _, _| {
            let eof = eof.clone();
            let completion = completion.clone();
            async move {
                eof.notified().await;
                completion.store(true, Ordering::SeqCst);
                if fail {
                    Err(Error::Invalid("failed dispatch".into()))
                } else {
                    Ok(json!({"stdout":"EOF effect finished"}))
                }
            }
        },
        |text| draw(&mut terminal, &mut output, text),
        false,
    );
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), result)
        .await
        .expect("EOF driver parked");
    assert!(
        completed.load(Ordering::SeqCst),
        "EOF cancelled its submitted effect"
    );
    if fail {
        assert!(matches!(result, Err(Error::Invalid(message)) if message == "failed dispatch"));
    } else {
        let client = result?;
        assert!(output.contains("EOF effect finished"));
        assert_eq!(output.matches("EOF effect finished").count(), 1);
        assert_eq!(client.rendered_sequence()?, client.output_sequence());
        assert!(
            terminal
                .backend()
                .buffer()
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
                .contains("EOF effect finished")
        );
    }
    Ok(())
}
#[tokio::test]
async fn eof_drains_submitted_effects_through_test_backend() -> Result<()> {
    eof_case(false).await
}
#[tokio::test]
async fn headless_worker_failure_propagates_after_eof() -> Result<()> {
    eof_case(true).await
}

#[tokio::test]
async fn results_followed_during_submission_render_once_and_reopen_at_the_display_cursor()
-> Result<()> {
    let volume = files().await;
    let mut client = session(volume.clone()).await;
    client.submit("/shell effect", &DeniedReader).await?;
    let request = client.take_request().unwrap();
    let mut worker = session(volume.clone()).await;
    worker
        .consume(&request, |_, _, _| async {
            Ok(json!({"stdout":"saved result"}))
        })
        .await?;
    // This settings read follows the result before the renderer starts.
    client.refresh_files().await?;
    client.submit("/details", &DeniedReader).await?;
    assert!(client.output_sequence() > 0);
    assert_eq!(client.rendered_sequence()?, 0);
    let mut terminal = Terminal::new(TestBackend::new(120, 80)).unwrap();
    let mut output = String::new();
    let execute = |_, _, _, _| async { Err(Error::Invalid("unexpected dispatch".into())) };
    let client = run_command_client(
        client,
        stream::empty(),
        &DeniedReader,
        execute,
        |text| draw(&mut terminal, &mut output, text),
        false,
    )
    .await?;
    assert_eq!(output.matches("saved result").count(), 1);
    assert_eq!(client.rendered_sequence()?, client.output_sequence());
    drop(client);
    let reopened = session(volume).await;
    let mut replay = String::new();
    run_command_client(
        reopened,
        stream::empty(),
        &DeniedReader,
        execute,
        |text| {
            replay.push_str(text);
            Ok(())
        },
        false,
    )
    .await?;
    assert!(!replay.contains("saved result"));
    Ok(())
}

#[tokio::test]
async fn stop_shell_help_resume_and_quit_use_the_harness_input_loop() -> Result<()> {
    let volume = files().await;
    let mut client = session(volume.clone()).await;
    let task = TaskId::from_bytes([2; 16]);
    client
        .write_state(Root::Session, "layout.json", &json!({"selected_task":task}))
        .await?;
    let (sender, input) = futures::channel::mpsc::unbounded();
    sender.unbounded_send(Ok("/stop".into())).unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let execute = move |_: OperationId, _, resolution: CommandResolution, _| {
        let volume = volume.clone();
        let effects = count.clone();
        async move {
            match resolution {
                CommandResolution::Submit {
                    request: acyclic_harness::client::CommandRequest::Pause { .. },
                } => {
                    control_file(&volume, task, true).await?;
                    Ok(json!({"paused":true}))
                }
                CommandResolution::Submit {
                    request: acyclic_harness::client::CommandRequest::Resume { .. },
                } => {
                    control_file(&volume, task, false).await?;
                    Ok(json!({"paused":false}))
                }
                CommandResolution::Tool { .. } => {
                    CommandControl::wait(&volume, &[task], 65_536).await?;
                    effects.fetch_add(1, Ordering::SeqCst);
                    Ok(json!({"stdout":"resumed effect"}))
                }
                _ => Err(Error::Invalid("unexpected dispatch".into())),
            }
        }
    };
    let mut terminal = Terminal::new(TestBackend::new(120, 100)).unwrap();
    let mut output = String::new();
    let mut stage = 0;
    let running = run_command_client(
        client,
        input,
        &DeniedReader,
        execute,
        |text| {
            draw(&mut terminal, &mut output, text)?;
            if stage == 0 && text.contains("\"paused\":true") {
                stage = 1;
                sender.unbounded_send(Ok("/shell parked".into())).unwrap();
                sender.unbounded_send(Ok("/help".into())).unwrap();
            } else if stage == 1 && text.contains("/quit") {
                assert_eq!(effects.load(Ordering::SeqCst), 0);
                stage = 2;
                sender.unbounded_send(Ok("/resume".into())).unwrap();
            } else if stage == 2 && text.contains("resumed effect") {
                stage = 3;
                sender.unbounded_send(Ok("/quit".into())).unwrap();
            }
            Ok(())
        },
        false,
    );
    let client = tokio::time::timeout(std::time::Duration::from_secs(3), running)
        .await
        .expect("resume input was blocked by its parked effect")?;
    assert_eq!(stage, 3);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(client.exit_requested()?);
    Ok(())
}

#[tokio::test]
async fn admission_receipt_keeps_the_installed_client_identity() -> Result<()> {
    let volume = files().await;
    let mut client = session(volume).await;
    client.submit("plain prompt", &DeniedReader).await?;
    let request = client.take_request().unwrap();
    let mut operation = None;
    let task = TaskId::from_bytes([4; 16]);
    let result = client
        .consume(&request, |id, _, _| {
            operation = Some(id);
            async move { Ok(json!({"task":task,"selected_task":task,"admission_operation":id})) }
        })
        .await?
        .unwrap();
    let rendered = acyclic_harness::client::render_command_record(&json!({"ok":result}));
    assert!(rendered.contains(&format!(
        "queued task={task} admission-operation={}",
        operation.unwrap()
    )));
    Ok(())
}

#[tokio::test]
async fn explicit_quit_cancels_a_parked_effect() -> Result<()> {
    let volume = files().await;
    let mut client = session(volume.clone()).await;
    let task = TaskId::from_bytes([2; 16]);
    client
        .write_state(Root::Session, "layout.json", &json!({"selected_task":task}))
        .await?;
    control_file(&volume, task, true).await?;
    client.refresh_files().await?;
    let completed = Arc::new(AtomicBool::new(false));
    let completion = completed.clone();
    let mut terminal = Terminal::new(TestBackend::new(120, 80)).unwrap();
    let mut output = String::new();
    let result = run_command_client(
        client,
        stream::iter([Ok("/shell parked".to_owned()), Ok("/quit".to_owned())]),
        &DeniedReader,
        move |_, _, _, _| {
            let volume = volume.clone();
            let completion = completion.clone();
            async move {
                CommandControl::wait(&volume, &[task], 65_536).await?;
                completion.store(true, Ordering::SeqCst);
                Ok(Value::Null)
            }
        },
        |text| draw(&mut terminal, &mut output, text),
        false,
    );
    let client = tokio::time::timeout(std::time::Duration::from_secs(3), result)
        .await
        .expect("quit waited on its paused effect")?;
    assert!(client.exit_requested()?);
    assert!(!completed.load(Ordering::SeqCst));
    Ok(())
}

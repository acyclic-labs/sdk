//! One completed answer has one transcript copy, through the production
//! LocalHarness command owner, TaskFileClient follow and ClientRenderer frame
//! sequence of `view::run` (follow, update, configure, layout, screen). The
//! answer comes from a scripted provider and, separately, over loopback HTTP
//! through the binary's own owner composition.
use super::*;
use acyclic_harness::{
    Error, LocalHarness, TaskId,
    client::{self, CommandFileRoot, CommandSession, OutputOwner, TaskFileClient},
    context::ModelContextCapacity,
    model::Model,
    task_volume::Query,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use futures::FutureExt as _;
use std::sync::Arc;

const SECTIONS: usize = 12;
const PROMPT: &str = "Write a long markdown answer";
/// File changes the driver may consume before the turn must have settled.
const MAXIMUM_CHANGES: usize = 512;
/// Find progress steps before the match count must be final.
const MAXIMUM_SEARCH_STEPS: usize = 64;
/// Hang guard only: a stalled owner fails instead of running until the job
/// ceiling. Settlement itself is decided by files, never by time.
const HANG_GUARD: std::time::Duration = std::time::Duration::from_secs(60);

/// One composed frame at the given size, as `view::run` paints it.
fn frame<P: acyclic_stream::StreamProvider + ?Sized>(
    renderer: &mut ClientRenderer,
    commands: &CommandSession<P>,
    width: u16,
    height: u16,
) -> Result<String, Error> {
    let input = PromptInput::default();
    renderer.layout(commands, &input, width, height)?;
    let screen = renderer.screen_for(commands)?;
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| {
            screen.render(frame, &input, &mut Viewport::default(), 0, 0);
        })
        .unwrap();
    Ok(screen_text(&terminal))
}

/// Every visible answer row, including bodies, and a body-text search count.
#[derive(Debug, PartialEq)]
struct Transcript {
    rows: Vec<String>,
    find: String,
}

fn rows(renderer: &ClientRenderer) -> Vec<String> {
    renderer
        .screen()
        .transcript
        .iter()
        .map(ToString::to_string)
        .filter(|row| !row.trim().is_empty())
        .collect()
}

fn expected_rows(answer: &str) -> Vec<String> {
    answer
        .lines()
        .filter(|row| !row.trim().is_empty())
        .map(str::to_owned)
        .collect()
}
fn expected_transcript(prompt: &str, answer: &str) -> Vec<String> {
    std::iter::once(format!("You: {prompt}"))
        .chain(expected_rows(answer))
        .collect()
}

fn assert_painted(
    renderer: &mut ClientRenderer,
    commands: &CommandSession<impl acyclic_stream::StreamProvider + ?Sized>,
    expected: &[String],
) -> Result<(), Error> {
    let painted = frame(renderer, commands, 100, 200)?;
    assert_eq!(
        rows(renderer),
        expected,
        "a completed answer must be painted exactly once"
    );
    for row in expected {
        assert_eq!(
            painted.lines().filter(|line| line.trim() == row).count(),
            expected.iter().filter(|expected| *expected == row).count(),
            "every owned transcript body row reaches the actual screen once: {row}"
        );
    }
    Ok(())
}

/// A twelve-section Markdown answer.
fn sections() -> String {
    (0..SECTIONS)
        .map(|section| format!("# Section {section}\n\nBody {section}.\n\n"))
        .collect()
}

/// The owner over a scripted provider that answers `answer`.
async fn scripted(workspace: &std::path::Path, answer: String) -> Arc<LocalHarness> {
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text(answer)]).with_context_capacity(
            ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            },
        ),
    );
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )
    .unwrap();
    Arc::new(
        LocalHarness::open(local_harness_config(workspace, (model, provider)).unwrap())
            .await
            .unwrap(),
    )
}

/// A loopback Responses endpoint streaming `answer` one section per delta.
struct Loopback {
    address: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

impl Loopback {
    async fn start(answer: &str) -> Self {
        use axum::{Json, Router, routing::post};
        let mut events = String::new();
        for delta in answer.split_inclusive("\n\n") {
            let event = serde_json::json!({"type": "response.output_text.delta", "delta": delta});
            events.push_str(&format!("data: {event}\n\n"));
        }
        let completed = serde_json::json!({"type": "response.completed", "response": {
            "status": "completed", "usage": {"input_tokens": 64, "output_tokens": 64}
        }});
        events.push_str(&format!("data: {completed}\n\n"));
        let app = Router::new()
            .route(
                "/count",
                post(|| async { Json(serde_json::json!({"input_tokens": 64})) }),
            )
            .route(
                "/responses",
                post(move || {
                    let events = events.clone();
                    async move { ([("content-type", "text/event-stream")], events) }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            stop,
            server,
        }
    }

    async fn stop(self) {
        let _ = self.stop.send(());
        self.server.await.unwrap();
    }
}

/// The owner exactly as the shipping binary opens it from argv, over the
/// loopback endpoint, with explicit empty credentials so no native login,
/// keychain or environment credential is consulted.
async fn shipping(directory: &std::path::Path, address: std::net::SocketAddr) -> Arc<LocalHarness> {
    use clap::Parser as _;
    let workspace = directory.join("workspace");
    let settings = directory.join("settings");
    std::fs::create_dir(&settings).unwrap();
    let catalog = directory.join("catalog.json");
    let provider = serde_json::json!({"graphcoder-qual": {
        "adapter": "responses", "authentication": "optional",
        "base_url": format!("http://{address}"),
        "models": {"tiny": {
            "capacity": {"context_tokens": 32768, "output_tokens": 4096},
            "features": {"tools": true, "images": false}
        }},
        "max_event_bytes": 4096,
        "token_counter": {
            "path": "count", "adapter": "responses",
            "response_field": "input_tokens", "framing_tokens": 0
        }
    }});
    std::fs::write(&catalog, serde_json::to_vec(&provider).unwrap()).unwrap();
    let mut argv: Vec<std::ffi::OsString> = vec!["graphcoder".into()];
    for (flag, value) in [
        ("--workspace", workspace.as_os_str()),
        ("--catalog", catalog.as_os_str()),
        ("--settings-root", settings.as_os_str()),
        ("--model", "graphcoder-qual/tiny".as_ref()),
    ] {
        argv.push(flag.into());
        argv.push(value.into());
    }
    let args = crate::args::Args::try_parse_from(argv).unwrap();
    let credentials = acyclic_harness_providers::Credentials::explicit(Default::default());
    crate::open(&args, credentials).await.unwrap()
}

/// Whether the renderer's own client follows the canonical result file of a
/// completed command in its command session: the turn is over whatever the
/// screen shows.
fn answered(client: &TaskFileClient) -> bool {
    client
        .views()
        .selections()
        .any(|(_, view)| match view.query() {
            Query::CommandSession { session, .. } => view
                .outputs(&OutputOwner::Session(session.clone()), 0)
                .next()
                .is_some(),
            _ => false,
        })
}

/// Which provider answers the turn.
#[derive(Clone, Copy)]
enum Provider {
    Scripted,
    Http,
}

async fn settled_transcript(provider: Provider, answer: String) -> Transcript {
    let expected = expected_transcript(PROMPT, &answer);
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let (local, loopback) = match provider {
        Provider::Scripted => (scripted(&workspace, answer).await, None),
        Provider::Http => {
            let loopback = Loopback::start(&answer).await;
            let local = shipping(directory.path(), loopback.address).await;
            (local, Some(loopback))
        }
    };
    let operation = client::local_identity(&local, None, None).await.unwrap();
    let presentation = {
        let local = local.clone();
        async move {
            let authority = local.client_owner();
            let root = TaskId::parse(&authority.id)?;
            let mut client = TaskFileClient::observe_task(
                local.task_files().as_ref(),
                &authority,
                root,
                1024,
                4 * 1024 * 1024,
            )
            .await?;
            let mut commands = client::local_ui(&local, &operation.to_string(), &authority).await?;
            client.follow_commands(&commands).await?;
            let reader = local.client_reader()?;
            commands.submit(PROMPT, reader.as_ref()).await?;
            let mut renderer = ClientRenderer::default();
            let mut submitted = false;
            for changes in 0.. {
                assert!(
                    changes < MAXIMUM_CHANGES,
                    "the turn did not settle within {MAXIMUM_CHANGES} file changes"
                );
                // The production loop: follow the active task, else the root.
                client
                    .follow_task(
                        local.task_files().as_ref(),
                        commands.active_task().unwrap_or(root),
                    )
                    .await?;
                renderer.update_files(&client, &authority);
                renderer.configure(client.views(), &authority, &commands)?;
                // Paint every observed live/final cut, not only settlement.
                frame(&mut renderer, &commands, 100, 200)?;
                assert!(
                    expected.starts_with(&rows(&renderer)),
                    "a completed answer must be painted exactly once"
                );
                submitted |= commands.active_task().is_some();
                // Settled: the request was claimed, its canonical result file
                // is followed and the root is followed again. The screen is
                // observed only afterwards, so a suppressed answer still
                // reaches the assertions.
                if submitted
                    && commands.active_task().is_none()
                    && !renderer.is_busy()
                    && answered(&client)
                {
                    break;
                }
                futures::select! {
                    change = client.next_change().fuse() => change?,
                    change = commands.observe_change().fuse() => change?,
                }
            }
            assert_painted(&mut renderer, &commands, &expected)?;
            let rows = rows(&renderer);
            // Reopen the production snapshot/follow path and rebuild every
            // disposable render cache from the same durable transcript files.
            let mut reopened = TaskFileClient::observe_task(
                local.task_files().as_ref(),
                &authority,
                root,
                1024,
                4 * 1024 * 1024,
            )
            .await?;
            reopened.follow_commands(&commands).await?;
            let mut replay = ClientRenderer::default();
            replay.update_files(&reopened, &authority);
            replay.configure(reopened.views(), &authority, &commands)?;
            assert_painted(&mut replay, &commands, &expected)?;
            let mut layout = commands
                .state(CommandFileRoot::Session, "layout.json")?
                .unwrap_or_default();
            layout["open_view"] = "find".into();
            layout["view_input"] = "find".into();
            layout["search_query"] = "Body 7.".into();
            commands
                .write_state(CommandFileRoot::Session, "layout.json", &layout)
                .await?;
            let mut find = String::new();
            for _ in 0..MAXIMUM_SEARCH_STEPS {
                let painted = frame(&mut renderer, &commands, 120, 40)?;
                let status = painted
                    .lines()
                    .find_map(|line| line.trim().strip_prefix("Find: "))
                    .unwrap_or_default();
                find = status
                    .split(" matches")
                    .next()
                    .unwrap_or_default()
                    .to_owned();
                if !status.contains('…') {
                    break;
                }
                renderer.search_progress().await;
            }
            Ok::<_, Error>(Transcript { rows, find })
        }
    };
    let transcript = tokio::time::timeout(
        HANG_GUARD,
        client::with_local_commands(local.clone(), operation, presentation),
    )
    .await
    .expect("hang guard: the owner stalled before the turn settled")
    .unwrap();
    client::disconnect_local(local).await.unwrap();
    if let Some(loopback) = loopback {
        loopback.stop().await;
    }
    transcript
}

/// The checked property: every section of a completed answer is painted
/// exactly once, in order, and Find sees exactly one match.
fn assert_answer_once(transcript: &Transcript) {
    assert_eq!(
        transcript,
        &Transcript {
            rows: expected_transcript(PROMPT, &sections()),
            find: "1/1".into(),
        },
        "a completed answer must be painted exactly once"
    );
}

#[tokio::test]
async fn a_completed_answer_is_painted_once() {
    assert_answer_once(&settled_transcript(Provider::Scripted, sections()).await);
}

/// The live symptom's own path: the shipping owner composition and the
/// Responses adapter streaming the answer over loopback HTTP.
#[tokio::test]
async fn a_completed_http_answer_is_painted_once() {
    assert_answer_once(&settled_transcript(Provider::Http, sections()).await);
}

/// A turn whose answer never reaches the transcript still settles on its
/// result file and fails the check, rather than waiting for another change.
#[tokio::test]
#[should_panic(expected = "a completed answer must be painted exactly once")]
async fn a_missing_answer_is_detected_negative_control() {
    let answer = "A reply with no section headings.".to_owned();
    assert_answer_once(&settled_transcript(Provider::Scripted, answer).await);
}

/// The two-copy transcript the live pass observed: sections 0-11 twice and
/// Find reporting 2/2.
#[test]
#[should_panic(expected = "a completed answer must be painted exactly once")]
fn a_duplicated_answer_is_detected_negative_control() {
    let once = expected_transcript(PROMPT, &sections());
    assert_answer_once(&Transcript {
        rows: [once.clone(), once].concat(),
        find: "1/2".into(),
    });
}

#[path = "answer_ownership.rs"]
mod ownership;

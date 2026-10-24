//! A real GraphCoder TUI on a pseudo-terminal continues one conversation.
#![cfg(target_os = "linux")]
use alacritty_terminal::{
    event::EventListener,
    grid::Dimensions,
    term::{Config, Term},
    vte::ansi::Processor,
};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{Read as _, Write as _},
    process::{Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

#[expect(
    clippy::disallowed_macros,
    reason = "Cargo-provided executable metadata for this integration crate"
)]
const EXECUTABLE: &str = env!("CARGO_BIN_EXE_graphcoder");
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const ANSWERS: [&str; 3] = ["ALPHA_REPLY", "BRAVO_REPLY", "CHARLIE_REPLY"];

fn answer(text: &str) -> String {
    format!(
        "data: {{\"type\":\"response.output_text.delta\",\"delta\":\"{text}\"}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\",\"usage\":{{\"input_tokens\":64,\"output_tokens\":2}}}}}}\n\n"
    )
}

struct Screen {
    bytes: Vec<u8>,
    output: mpsc::Receiver<Vec<u8>>,
    terminal: Term<Events>,
    parser: Processor,
    input: File,
    answered: usize,
}

#[derive(Clone, Copy)]
struct Events;
impl EventListener for Events {}
struct Grid;
impl Dimensions for Grid {
    fn total_lines(&self) -> usize {
        40
    }
    fn screen_lines(&self) -> usize {
        40
    }
    fn columns(&self) -> usize {
        120
    }
}

/// Kills and reaps the TUI unless it exited on its own, so a failed step never
/// leaves it holding the terminal.
struct Running(std::process::Child);

impl Running {
    /// Waits for the requested exit; the deadline only bounds a quit regression.
    fn exit(mut self, screen: &mut Screen) -> TestResult<std::process::ExitStatus> {
        // PTY EOF is delivered by the blocking reader, including every final
        // output byte. Waiting for it bounds quit without polling the process.
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            match screen
                .output
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(bytes) => screen.advance(&bytes),
                Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(self.0.wait()?),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err("GraphCoder did not exit on Ctrl+D".into());
                }
            }
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn settled(text: &str, answer: &str) -> bool {
    text.starts_with("○ GraphCoder") && text.contains(answer)
}

impl Screen {
    fn advance(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
        self.parser.advance(&mut self.terminal, bytes);
    }

    fn visible(&self) -> String {
        self.terminal
            .grid()
            .display_iter()
            .map(|cell| cell.c)
            .collect()
    }

    /// Waits for `condition`; the deadline only bounds a hang, nothing is timed.
    fn until(&mut self, what: &str, mut condition: impl FnMut(&str) -> bool) -> TestResult {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let text = String::from_utf8_lossy(&self.bytes);
            // Answer each primary device-attributes probe so startup never waits.
            let probes = text.matches("\x1b[c").count();
            for _ in self.answered..probes {
                self.input.write_all(b"\x1b[?62;1c")?;
            }
            self.answered = probes;
            assert!(
                !text.contains("cannot accept children"),
                "a later prompt was refused:\n{text}"
            );
            if condition(&self.visible()) {
                return Ok(());
            }
            match self
                .output
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(bytes) => self.advance(&bytes),
                Err(error) => {
                    return Err(format!("no {what} ({error}):\n{}", self.visible()).into());
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_prompts_in_the_tui_continue_one_conversation() -> TestResult {
    conversation(false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blocked_task_exits_the_tui_nonzero() -> TestResult {
    conversation(true).await
}

async fn conversation(blocked: bool) -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let app = responses(&bodies, blocked);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let directory = tempfile::tempdir()?;
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    let home = directory.path().join("home");
    for path in [&workspace, &settings, &home] {
        std::fs::create_dir(path)?;
    }
    let catalog = directory.path().join("catalog.json");
    std::fs::write(
        &catalog,
        serde_json::to_vec(&json!({"graphcoder-qual":{
            "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
            "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
            "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
        }}))?,
    )?;
    let (child, screen, reader) = launch(&workspace, &catalog, &settings, &home)?;
    let driven = drive(child, screen, bodies.clone(), blocked).await?;
    reader.join().map_err(|_| "PTY reader panicked")?;
    assert_eq!(
        driven.code(),
        Some(if blocked { 3 } else { 0 }),
        "{driven:?}"
    );
    check_requests(&bodies, blocked)?;
    stop.send(())
        .map_err(|_| "server stopped before test completed")?;
    server.await??;
    Ok(())
}

fn responses(bodies: &Arc<Mutex<Vec<Value>>>, blocked: bool) -> Router {
    let recorded = bodies.clone();
    Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let bodies = recorded.clone();
                async move {
                    let mut bodies = bodies
                        .lock()
                        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
                    bodies.push(body);
                    if blocked && bodies.len() == 1 {
                        let event = json!({"type":"response.output_item.done","output_index":0,
                            "item":{"type":"function_call","call_id":"block_goal","name":"write",
                            "arguments":json!({"path":".volumes/scratch/GOAL.md",
                                "text":"---\nstatus: blocked\nreason: waiting\n---\nShip",
                                "media_type":"text/markdown","display_name":"goal"}).to_string()}});
                        let completed =
                            json!({"type":"response.completed","response":{"status":"completed"}});
                        return Ok::<_, axum::http::StatusCode>((
                            [("content-type", "text/event-stream")],
                            format!("data: {event}\n\ndata: {completed}\n\n"),
                        ));
                    }
                    let text = ANSWERS.get(bodies.len() - 1).copied().unwrap_or("EXTRA");
                    Ok(([("content-type", "text/event-stream")], answer(text)))
                }
            }),
        )
}

fn launch(
    workspace: &std::path::Path,
    catalog: &std::path::Path,
    settings: &std::path::Path,
    home: &std::path::Path,
) -> TestResult<(Running, Screen, std::thread::JoinHandle<()>)> {
    let size = rustix_openpty::rustix::termios::Winsize {
        ws_row: 40,
        ws_col: 120,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = rustix_openpty::openpty(None, Some(&size))?;
    let controller = File::from(pty.controller);
    let user = File::from(pty.user);
    // `setsid --ctty` makes the pseudo-terminal the child's /dev/tty.
    let child = Running(
        Command::new("setsid")
            .arg("--ctty")
            .arg(EXECUTABLE)
            .arg("--workspace")
            .arg(workspace)
            .arg("--catalog")
            .arg(catalog)
            .arg("--settings-root")
            .arg(settings)
            .args(["--model", "graphcoder-qual/tiny"])
            .current_dir(workspace)
            .env("TERM", "xterm-256color")
            .env("HOME", home)
            .env("CODEX_HOME", home.join("codex-home"))
            .env_remove("OPENAI_API_KEY")
            .env("XDG_DATA_HOME", home.join("data"))
            .stdin(Stdio::from(user.try_clone()?))
            .stdout(Stdio::from(user.try_clone()?))
            .stderr(Stdio::from(user))
            .spawn()?,
    );
    let (sink, output_events) = mpsc::channel();
    let mut output = controller.try_clone()?;
    let reader = std::thread::spawn(move || {
        let mut buffer = [0; 8192];
        // The read fails with EIO once the child closes the terminal.
        while let Ok(count @ 1..) = output.read(&mut buffer) {
            if sink.send(buffer[..count].to_vec()).is_err() {
                break;
            }
        }
    });
    let screen = Screen {
        bytes: Vec::new(),
        output: output_events,
        terminal: Term::new(Config::default(), &Grid, Events),
        parser: Processor::new(),
        input: controller,
        answered: 0,
    };
    Ok((child, screen, reader))
}

async fn drive(
    child: Running,
    mut screen: Screen,
    requested: Arc<Mutex<Vec<Value>>>,
    blocked: bool,
) -> TestResult<std::process::ExitStatus> {
    tokio::task::spawn_blocking(move || -> TestResult<_> {
        let child = child;
        screen.until("composer", |text| {
            text.starts_with("○ GraphCoder") && text.contains('└')
        })?;
        let prompts: &[&str] = if blocked {
            &["Use goal mode"]
        } else {
            &["first prompt", "first prompt", "third prompt"]
        };
        for (index, (prompt, expected)) in prompts.iter().zip(ANSWERS).enumerate() {
            screen.input.write_all(format!("{prompt}\r").as_bytes())?;

            // A fast turn can settle between frames without ever painting busy.
            // Require the actual answer and idle header in the current terminal,
            // rather than a transient glyph somewhere in the output history.
            if blocked {
                screen.until("blocked goal", |text| {
                    settled(text, "goal blocked: GOAL.md is blocked.")
                })?;
            } else {
                screen.until(expected, |text| {
                    requested
                        .lock()
                        .is_ok_and(|bodies| bodies.len() == index + 1)
                        && settled(text, expected)
                })?;
            }
        }
        screen.input.write_all(b"\x04")?;
        let status = child.exit(&mut screen);
        drop(screen);
        status
    })
    .await?
}

fn check_requests(bodies: &Arc<Mutex<Vec<Value>>>, blocked: bool) -> TestResult {
    let bodies = bodies.lock().map_err(|error| error.to_string())?;
    if blocked {
        assert_eq!(
            bodies.len(),
            2,
            "the tool result completes its turn, then the blocked goal stops continuation"
        );
        return Ok(());
    }
    assert_eq!(bodies.len(), 3);
    // Each later request carries every earlier prompt and answer.
    for (index, (body, expected)) in bodies.iter().zip(ANSWERS).enumerate() {
        let body = body.to_string();
        for earlier in ANSWERS.iter().take(index) {
            assert!(body.contains(earlier), "request {index} lacks {earlier}");
        }
        assert!(
            !body.contains(expected),
            "request {index} saw its own answer"
        );
    }
    assert!(
        bodies
            .get(2)
            .ok_or("missing third request")?
            .to_string()
            .contains("first prompt")
    );
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    #[test]
    fn settlement_uses_current_cells_across_any_byte_boundary(split in 0usize..256) {
        let bytes = "\x1b[H○ GraphCoder\x1b[3;1HALPHA_REPLY".as_bytes();
        let split = split % (bytes.len() + 1);
        let mut terminal = Term::new(Config::default(), &Grid, Events);
        let mut parser: Processor = Processor::new();
        parser.advance(&mut terminal, &bytes[..split]);
        parser.advance(&mut terminal, &bytes[split..]);
        let visible = |terminal: &Term<Events>| terminal.grid().display_iter().map(|cell| cell.c).collect::<String>();
        proptest::prop_assert!(settled(&visible(&terminal), ANSWERS[0]));
        // Negative control: the former wait requires a busy frame even when
        // the final rendered answer is already idle. It can never accept this.
        proptest::prop_assert!(!String::from_utf8_lossy(bytes).contains('●'));
        parser.advance(&mut terminal, "\x1b[1;1H●".as_bytes());
        proptest::prop_assert!(!settled(&visible(&terminal), ANSWERS[0]));
        parser.advance(&mut terminal, "\x1b[1;1H○\x1b[3;1H\x1b[2K".as_bytes());
        // An answer in the raw history does not prove it remains on screen.
        proptest::prop_assert!(!settled(&visible(&terminal), ANSWERS[0]));
    }
}

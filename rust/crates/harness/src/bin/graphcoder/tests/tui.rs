//! A real GraphCoder TUI on a pseudo-terminal continues one conversation.
#![cfg(target_os = "linux")]
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{Read as _, Write as _},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const ANSWERS: [&str; 3] = ["ALPHA_REPLY", "BRAVO_REPLY", "CHARLIE_REPLY"];

fn answer(text: &str) -> String {
    format!(
        "data: {{\"type\":\"response.output_text.delta\",\"delta\":\"{text}\"}}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\",\"usage\":{{\"input_tokens\":64,\"output_tokens\":2}}}}}}\n\n"
    )
}

struct Screen {
    bytes: Arc<Mutex<Vec<u8>>>,
    input: File,
    answered: usize,
}

fn find(bytes: &[u8], needle: &str) -> Option<usize> {
    bytes
        .windows(needle.len())
        .position(|window| window == needle.as_bytes())
}

impl Screen {
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().unwrap().clone()
    }

    /// Waits for `condition`; the deadline only bounds a hang, nothing is timed.
    fn until(&mut self, what: &str, mut condition: impl FnMut(&[u8]) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let bytes = self.bytes();
            let text = String::from_utf8_lossy(&bytes);
            // Answer each primary device-attributes probe so startup never waits.
            let probes = text.matches("\x1b[c").count();
            for _ in self.answered..probes {
                self.input.write_all(b"\x1b[?62;1c").unwrap();
            }
            self.answered = probes;
            assert!(
                !text.contains("cannot accept children"),
                "a later prompt was refused:\n{text}"
            );
            if condition(&bytes) {
                return;
            }
            assert!(Instant::now() < deadline, "no {what}:\n{text}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_prompts_in_the_tui_continue_one_conversation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let recorded = bodies.clone();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let bodies = recorded.clone();
                async move {
                    let mut bodies = bodies.lock().unwrap();
                    bodies.push(body);
                    let text = ANSWERS.get(bodies.len() - 1).copied().unwrap_or("EXTRA");
                    ([("content-type", "text/event-stream")], answer(text))
                }
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    let home = directory.path().join("home");
    for path in [&workspace, &settings, &home] {
        std::fs::create_dir(path).unwrap();
    }
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let size = rustix_openpty::rustix::termios::Winsize {
        ws_row: 40,
        ws_col: 120,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = rustix_openpty::openpty(None, Some(&size)).unwrap();
    let controller = File::from(pty.controller);
    let user = File::from(pty.user);
    // `setsid --ctty` makes the pseudo-terminal the child's /dev/tty.
    let mut child = Command::new("setsid")
        .arg("--ctty")
        .arg(env!("CARGO_BIN_EXE_graphcoder"))
        .arg("--workspace")
        .arg(&workspace)
        .arg("--catalog")
        .arg(&catalog)
        .arg("--settings-root")
        .arg(&settings)
        .args(["--model", "graphcoder-qual/tiny"])
        .current_dir(&workspace)
        .env("TERM", "xterm-256color")
        .env("HOME", &home)
        .env("XDG_DATA_HOME", home.join("data"))
        .stdin(Stdio::from(user.try_clone().unwrap()))
        .stdout(Stdio::from(user.try_clone().unwrap()))
        .stderr(Stdio::from(user))
        .spawn()
        .unwrap();
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let mut output = controller.try_clone().unwrap();
    let sink = bytes.clone();
    let reader = std::thread::spawn(move || {
        let mut buffer = [0; 8192];
        // The read fails with EIO once the child closes the terminal.
        while let Ok(count @ 1..) = output.read(&mut buffer) {
            sink.lock().unwrap().extend_from_slice(&buffer[..count]);
        }
    });
    let requested = bodies.clone();
    let mut screen = Screen {
        bytes,
        input: controller,
        answered: 0,
    };
    let driven = tokio::task::spawn_blocking(move || {
        screen.until("composer", |bytes| {
            find(bytes, "GraphCoder").is_some() && find(bytes, "└").is_some()
        });
        for (index, prompt) in ["first prompt", "second prompt", "third prompt"]
            .into_iter()
            .enumerate()
        {
            let start = screen.bytes().len();
            screen
                .input
                .write_all(format!("{prompt}\r").as_bytes())
                .unwrap();
            // Rendering is incremental, so the answer text need not appear
            // whole. The status glyph turns busy, then idle once it is in.
            screen.until(ANSWERS[index], |bytes| {
                let bytes = &bytes[start..];
                requested.lock().unwrap().len() == index + 1
                    && find(bytes, "●").is_some_and(|busy| find(&bytes[busy..], "○").is_some())
            });
        }
        screen.input.write_all(b"\x04").unwrap();
        screen
    })
    .await
    .unwrap();
    let status = child.wait().unwrap();
    drop(driven);
    reader.join().unwrap();
    assert!(status.success(), "{status:?}");
    let bodies = bodies.lock().unwrap();
    assert_eq!(bodies.len(), 3);
    // Each later request carries every earlier prompt and answer.
    for (index, body) in bodies.iter().enumerate() {
        let body = body.to_string();
        for earlier in 0..index {
            assert!(
                body.contains(ANSWERS[earlier]),
                "request {index} lacks {}",
                ANSWERS[earlier]
            );
        }
        assert!(
            !body.contains(ANSWERS[index]),
            "request {index} saw its own answer"
        );
    }
    assert!(bodies[2].to_string().contains("first prompt"));
    stop.send(()).unwrap();
    server.await.unwrap();
}

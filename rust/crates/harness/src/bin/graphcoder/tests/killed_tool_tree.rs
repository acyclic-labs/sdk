//! A client killed during a tool leaves no part of that tool's process tree
//! able to act after the harness records the call as interrupted.
//!
//! The oracle observes the tree from outside it. Every process in the tree
//! holds `alive`; it reaches end-of-file only once all of them have exited. The
//! tree's effect waits on `gate`, which can be released only while a member of
//! the tree is still blocked reading it. Unix only: Windows trees are Jobs,
//! covered by native-runtime's custody tests.
use super::*;
use std::{sync::Mutex, time::Instant};

mod oracle {
    use std::{
        fs::{File, OpenOptions},
        io::{Read as _, Write as _},
        os::unix::fs::OpenOptionsExt as _,
        path::{Path, PathBuf},
    };

    pub(super) struct Tree {
        alive: File,
        gate: PathBuf,
        pub(super) started: PathBuf,
        pub(super) effect: PathBuf,
        alive_path: PathBuf,
    }

    fn fifo(path: &Path) {
        let status = std::process::Command::new("mkfifo")
            .arg(path)
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// A failed assertion must not leave the tree blocked forever: releasing
    /// the gate lets any survivor finish and end its descendant.
    impl Drop for Tree {
        fn drop(&mut self) {
            if let Ok(mut gate) = OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&self.gate)
            {
                let _ = gate.write_all(
                    b"go
",
                );
            }
        }
    }

    impl Tree {
        pub(super) fn new(directory: &Path) -> Self {
            let alive_path = directory.join("alive");
            let gate = directory.join("gate");
            fifo(&alive_path);
            fifo(&gate);
            // Opened before the tree exists, so the tree's own open never blocks.
            let alive = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&alive_path)
                .unwrap();
            Self {
                alive,
                gate,
                started: directory.join("started"),
                effect: directory.join("effect.txt"),
                alive_path,
            }
        }

        /// The tool: a shell and a background descendant both hold `alive`;
        /// the shell writes the effect only once `gate` is released, then
        /// ends its descendant, so a released tree exits on its own.
        pub(super) fn script(&self) -> String {
            format!(
                "exec 3>'{}'; sleep 3600 & printf started > '{}'; read -r _ < '{}'; printf SLEPT > '{}'; kill $!",
                self.alive_path.display(),
                self.started.display(),
                self.gate.display(),
                self.effect.display()
            )
        }

        /// Whether any process of the started tree still exists. Only
        /// meaningful once `started` exists, since the writer opened first.
        pub(super) fn alive(&self) -> bool {
            match (&self.alive).read(&mut [0; 16]) {
                Ok(0) => false,
                Ok(_) => panic!("the tree wrote to its liveness pipe"),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => true,
                Err(error) => panic!("liveness read failed: {error}"),
            }
        }

        /// Releases the effect. False when no process remains to receive it.
        pub(super) fn release(&self) -> bool {
            match OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&self.gate)
            {
                Ok(mut gate) => {
                    gate.write_all(b"go\n").unwrap();
                    true
                }
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => false,
                Err(error) => panic!("gate open failed: {error}"),
            }
        }

        /// Blocks until the last process of the tree exits: a blocking reader
        /// reaches end-of-file only once every writer has closed.
        pub(super) fn wait_exit(&self) {
            if self.alive() {
                let mut reader = File::open(&self.alive_path).unwrap();
                assert_eq!(reader.read(&mut [0; 16]).unwrap(), 0);
            }
        }

        /// A tree outside any custody: its parent exits at once.
        pub(super) fn spawn_uncontained(&self) {
            let status = std::process::Command::new("sh")
                .args([
                    "-c",
                    "bash --noprofile --norc -c \"$1\" </dev/null >/dev/null 2>&1 & exit 0",
                    "sh",
                ])
                .arg(self.script())
                .status()
                .unwrap();
            assert!(status.success());
        }
    }
}

use oracle::Tree;

fn bash_call(command: &str) -> String {
    let call = json!({"type":"response.output_item.done","output_index":0,"item":{
        "type":"function_call","call_id":"orphan","name":"bash",
        "arguments":json!({"command":command}).to_string()
    }});
    format!(
        "data: {call}\n\ndata: {}\n\n",
        json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":64,"output_tokens":2}}})
    )
}

fn wait_for(path: &Path, child: &mut std::process::Child) {
    while !path.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "client exited before its tool started"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Negative control: the oracle sees a tree that outlives its parent and
/// lets its effect land, so the assertions below can fail.
#[test]
fn oracle_observes_an_uncontained_tree_landing_its_effect() {
    let directory = tempfile::tempdir().unwrap();
    let tree = Tree::new(directory.path());
    tree.spawn_uncontained();
    while !tree.started.exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(tree.alive(), "an uncontained tree outlives its parent");
    assert!(
        tree.release(),
        "the uncontained tree still waits on its gate"
    );
    tree.wait_exit();
    assert_eq!(std::fs::read_to_string(&tree.effect).unwrap(), "SLEPT");
}

#[tokio::test]
async fn killed_client_tool_tree_is_dead_before_the_next_turn() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    let tree = Arc::new(Tree::new(directory.path()));
    let script = tree.script();
    // Liveness of the killed tree when each later model request arrived.
    let observed = Arc::new(Mutex::new(Vec::new()));
    let generated = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post({
                let (tree, observed, generated) = (
                    Arc::clone(&tree),
                    Arc::clone(&observed),
                    Arc::clone(&generated),
                );
                move || {
                    let first = generated.fetch_add(1, Ordering::SeqCst) == 0;
                    if !first {
                        observed
                            .lock()
                            .unwrap()
                            .push((tree.alive(), Instant::now()));
                    }
                    let body = if first {
                        bash_call(&script)
                    } else {
                        ANSWER.into()
                    };
                    async move { ([("content-type", "text/event-stream")], body) }
                }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let catalog = catalog(directory.path(), address);

    // Kill only the client process, as a crash would; its tool is running.
    let mut first = graphcoder(
        &workspace,
        &catalog,
        &settings,
        OperationId::new(),
        Some("start"),
    );
    let started = tree.started.clone();
    tokio::task::spawn_blocking(move || {
        let mut child = first.spawn().unwrap();
        wait_for(&started, &mut child);
        child.kill().unwrap();
        child.wait().unwrap();
    })
    .await
    .unwrap();

    let reopened = Instant::now();
    let output = run(
        graphcoder(
            &workspace,
            &catalog,
            &settings,
            OperationId::new(),
            Some("next"),
        ),
        None,
    )
    .await
    .unwrap();
    let finished = Instant::now();
    assert!(
        output.status.success(),
        "exit {:?}\n{}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let observed = observed.lock().unwrap().clone();
    assert_eq!(observed.len(), 1, "the next turn made one model request");
    let (alive_at_next_turn, requested) = observed[0];
    eprintln!(
        "next turn: model request {:?} after reopen, settled {:?} after reopen",
        requested - reopened,
        finished - reopened
    );
    // Ordering, not wall time: the whole killed tree was gone before the next
    // turn reached its model, so after its interruption was recorded.
    assert!(
        !alive_at_next_turn,
        "the killed tool tree was still running when the next turn started"
    );
    // (a) Nothing remains that could land the effect.
    assert!(
        !tree.release(),
        "a killed tool process still waits on its gate"
    );
    assert!(!tree.effect.exists(), "the killed tool's effect landed");
    server.abort();
}

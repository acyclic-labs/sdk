//! The Escape ladder through the production prompt, file input and prompt
//! files, in `view::run` order.
use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession, PromptFiles, file_tui_input},
    task_volume::{FileKey, Query, TaskFile, TaskVolume},
    tui::{
        PromptInput,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}

struct Ui {
    volume: Arc<TaskVolume<MemoryStream>>,
    files: CommandSession<MemoryStream>,
    input: PromptInput,
    prompt: PromptFiles,
}

impl Ui {
    async fn open() -> Self {
        let volume = Arc::new(
            TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("escape").unwrap(),
            )
            .await
            .unwrap(),
        );
        let mut files = CommandSession::open(
            volume.clone(),
            OperationId::from_bytes([5; 16])
                .to_string()
                .parse()
                .unwrap(),
            65_536,
            64,
        )
        .await
        .unwrap();
        files
            .write_state(
                CommandFileRoot::Config,
                "settings.json",
                &serde_json::json!({"renderer":"ratatui"}),
            )
            .await
            .unwrap();
        files
            .write_state(
                CommandFileRoot::Session,
                "layout.json",
                &serde_json::json!({"selected_task": acyclic_harness::TaskId::from_bytes([4; 16])}),
            )
            .await
            .unwrap();
        let mut ui = Self {
            volume,
            files,
            input: PromptInput::default(),
            prompt: PromptFiles::default(),
        };
        ui.prompt.refresh(&ui.files, &mut ui.input).unwrap();
        ui
    }

    async fn key(&mut self, code: KeyCode) {
        for effect in self
            .input
            .key(KeyEvent::new(code, KeyModifiers::NONE), 1, 78)
        {
            file_tui_input(&mut self.files, &NoFiles, effect, 10, 0)
                .await
                .unwrap();
        }
        self.prompt
            .save(&mut self.files, &mut self.input)
            .await
            .unwrap();
        self.prompt.refresh(&self.files, &mut self.input).unwrap();
    }

    async fn type_text(&mut self, text: &str) {
        for character in text.chars() {
            self.key(KeyCode::Char(character)).await;
        }
    }

    fn notifications(&self) -> serde_json::Value {
        self.files
            .state(CommandFileRoot::Session, "notifications.json")
            .unwrap()
            .unwrap_or(serde_json::json!([]))
    }

    fn open_view(&self) -> serde_json::Value {
        self.files
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap()["open_view"]
            .clone()
    }
}

#[tokio::test]
async fn escape_closes_views_then_is_quiet_and_never_leaves_a_stale_prompt() {
    let mut ui = Ui::open().await;
    ui.type_text("/help").await;
    ui.key(KeyCode::Enter).await;
    if ui.open_view().is_null() {
        // The first Enter may only accept the completion.
        ui.key(KeyCode::Enter).await;
    }
    assert!(!ui.open_view().is_null(), "help opened");
    assert_eq!(
        ui.input.editor.text(),
        "",
        "a submitted command clears the prompt"
    );
    ui.key(KeyCode::Esc).await;
    assert!(ui.open_view().is_null(), "Escape closes the view");
    // Idle Escape with nothing open is quiet: no unsupported-action feedback.
    ui.key(KeyCode::Esc).await;
    assert_eq!(ui.notifications(), serde_json::json!([]));
    ui.type_text("Section").await;
    assert_eq!(ui.input.editor.text(), "Section");

    // Feedback from a failed command is dismissed by Escape, and the next
    // input starts from an empty prompt.
    ui.input.editor.set_text("", 0);
    ui.type_text("/definitely-unknown").await;
    ui.key(KeyCode::Enter).await;
    ui.key(KeyCode::Enter).await;
    assert_ne!(
        ui.notifications(),
        serde_json::json!([]),
        "unknown command feedback"
    );
    ui.key(KeyCode::Esc).await;
    assert_eq!(ui.notifications(), serde_json::json!([]));
    ui.type_text("x").await;
    assert_eq!(ui.input.editor.text(), "x");
}

/// Idle Escape publishes no feedback; before, it reported an unsupported action.
#[tokio::test]
async fn idle_escape_is_quiet() {
    idle_escape::<false>().await;
}

/// Feedback published on the session's notification channel, as the old
/// unsupported-action reply was, must fail the quiet check.
#[tokio::test]
#[should_panic(expected = "idle Escape left feedback")]
async fn idle_feedback_is_detected_negative_control() {
    idle_escape::<true>().await;
}

async fn idle_escape<const FEEDBACK: bool>() {
    let mut ui = Ui::open().await;
    if FEEDBACK {
        ui.type_text("/definitely-unknown").await;
        ui.key(KeyCode::Enter).await;
        ui.key(KeyCode::Enter).await;
    } else {
        ui.key(KeyCode::Esc).await;
    }
    assert_eq!(
        ui.notifications(),
        serde_json::json!([]),
        "idle Escape left feedback"
    );
}

#[tokio::test]
async fn escape_while_a_turn_runs_publishes_its_cancellation() {
    cancellation::<true>().await;
}

/// A key that leaves the running turn alone must fail the cancellation check.
#[tokio::test]
#[should_panic(expected = "Escape publishes the same cancellation request as Ctrl+C")]
async fn an_ignored_escape_is_detected_negative_control() {
    cancellation::<false>().await;
}

async fn cancellation<const ESCAPE: bool>() {
    let mut ui = Ui::open().await;
    ui.type_text("hello").await;
    ui.key(KeyCode::Enter).await;
    let task = ui.files.active_task().expect("submitted prompt is active");
    if ESCAPE {
        ui.key(KeyCode::Esc).await;
    }
    assert_eq!(ui.notifications(), serde_json::json!([]));
    let cancel = FileKey::Task {
        task: acyclic_harness::task_volume::Component::new(task.to_string()).unwrap(),
        file: TaskFile::Request(
            acyclic_harness::task_volume::Component::new(format!("cancel-{task}")).unwrap(),
        ),
    };
    assert_eq!(
        ui.volume
            .snapshot(&Query::File(cancel), 1)
            .await
            .unwrap()
            .files
            .len(),
        1,
        "Escape publishes the same cancellation request as Ctrl+C"
    );
}

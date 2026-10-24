use super::ClientRenderer;
use crate::{
    TaskId,
    client::{self, CommandFileRoot, CommandSession, TaskFileClient},
    task_volume::{Channel, Component, FileKey, FileValue, Query, TaskFile, TaskVolume},
    tui::{
        Editor, InputEffect, PromptInput, RowPosition, ScreenAreas, TranscriptAnchor,
        TranscriptPart, Viewport,
        key_event::{InputEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind},
        ratatui::{Terminal, backend::TestBackend, layout::Rect},
    },
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::StreamExt;
use std::sync::Arc;

struct NoFiles;
impl crate::conversation::ContentResidencyVerifier for NoFiles {}
const SESSION: uuid::Uuid = uuid::Uuid::from_bytes([27; 16]);

struct Fixture {
    volume: Arc<TaskVolume<MemoryStream>>,
    client: TaskFileClient,
    owner: wire::Authority,
    files: CommandSession<MemoryStream>,
    renderer: ClientRenderer,
    input: PromptInput,
    viewport: Viewport,
    terminal: Terminal<TestBackend>,
    areas: ScreenAreas,
}

impl Fixture {
    async fn new() -> Self {
        let volume = Arc::new(
            TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("scrollbar").unwrap(),
            )
            .await
            .unwrap(),
        );
        let task = TaskId::from_bytes([28; 16]);
        let mut chunk = FileValue::new(
            FileKey::Task {
                task: Component::new(task.to_string()).unwrap(),
                file: TaskFile::Chunk {
                    turn: 0,
                    sequence: 0,
                },
            },
            format!(
                "```text\n{}```\n",
                (0..80)
                    .map(|row| format!("message {row}\n"))
                    .collect::<String>()
            )
            .into_bytes(),
        );
        chunk.channel = Some(Channel::Text);
        volume
            .append(volume.head().await.unwrap(), &chunk)
            .await
            .unwrap();
        let status = FileValue::new(
            FileKey::parse(&format!(".tasks/{task}/status.json")).unwrap(),
            br#"{"state":"completed"}"#.to_vec(),
        );
        volume
            .append(volume.head().await.unwrap(), &status)
            .await
            .unwrap();
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: task.to_string(),
        };
        let client = TaskFileClient::observe(volume.as_ref(), &owner, 128, 65536)
            .await
            .unwrap();
        let mut files = CommandSession::open(volume.clone(), SESSION, 65536, 128)
            .await
            .unwrap();
        files
            .write_state(
                CommandFileRoot::Session,
                "layout.json",
                &serde_json::json!({"selected_task":task.to_string()}),
            )
            .await
            .unwrap();
        let mut fixture = Self {
            volume,
            client,
            owner,
            files,
            renderer: ClientRenderer::default(),
            input: PromptInput::new(Editor::new("unsent draft")),
            viewport: Viewport::default(),
            terminal: Terminal::new(TestBackend::new(60, 20)).unwrap(),
            areas: ScreenAreas::default(),
        };
        fixture.paint();
        fixture
    }

    fn paint(&mut self) {
        self.renderer.update_files(&self.client, &self.owner);
        self.renderer
            .configure(self.client.views(), &self.owner, &self.files)
            .unwrap();
        let size = self.terminal.size().unwrap();
        self.renderer
            .layout(&self.files, &self.input, size.width, size.height)
            .unwrap();
        let screen = self.renderer.screen_for(&self.files).unwrap();
        self.terminal
            .draw(|frame| {
                self.areas = screen.render(frame, &self.input, &mut self.viewport, 0, 2);
            })
            .unwrap();
    }

    fn route<const BOUND: bool>(&mut self, event: InputEvent) -> Vec<InputEffect> {
        if BOUND {
            self.areas
                .event(&mut self.input, event, &mut self.viewport, 1, &mut |byte| {
                    self.renderer.anchor_at(byte)
                })
                .collect()
        } else {
            self.input
                .event(
                    event,
                    crate::tui::PromptBox::input_area(self.areas.prompt),
                    &mut self.viewport,
                    1,
                )
                .collect()
        }
    }

    async fn apply(&mut self, effects: Vec<InputEffect>) {
        for effect in effects {
            client::file_tui_input(
                &mut self.files,
                &NoFiles,
                effect,
                usize::from(self.areas.transcript.height),
                0,
                |from, rows| self.renderer.move_view(from, rows),
            )
            .await
            .unwrap();
        }
        assert!(self.files.take_request().is_none());
        self.paint();
    }
}

fn mouse(kind: MouseEventKind, column: u16, row: u16) -> InputEvent {
    InputEvent::Mouse(MouseEvent {
        kind,
        column,
        row,
        modifiers: KeyModifiers::NONE,
    })
}

async fn click_drag_reopen<const BOUND: bool>() {
    let mut fixture = Fixture::new().await;
    assert!(
        fixture
            .terminal
            .backend()
            .buffer()
            .content
            .iter()
            .any(|cell| cell.symbol() == "█")
    );
    let first = Some(TranscriptAnchor {
        part: TranscriptPart::Answer,
        position: RowPosition::default(),
    });
    for (kind, bottom, expected) in [
        (MouseEventKind::Down(MouseButton::Left), false, first),
        (MouseEventKind::Drag(MouseButton::Left), true, None),
        (MouseEventKind::Drag(MouseButton::Left), false, first),
    ] {
        let track = fixture.areas.scrollbar;
        let row = if bottom { track.bottom() - 1 } else { track.y };
        let mut followed = fixture
            .volume
            .observe(
                Query::UiDirectory(Component::new(format!("session-{SESSION}")).unwrap()),
                None,
                128,
            )
            .await
            .unwrap();
        let mut at = followed.snapshot.take().unwrap().cursor.position;
        let effects = fixture.route::<BOUND>(mouse(kind, track.x, row));
        assert_eq!(
            effects.len(),
            1,
            "scrollbar hit must produce one file-write intent"
        );
        fixture.apply(effects).await;
        let cut = fixture.volume.head().await.unwrap().position;
        let mut writes = 0;
        while at < cut {
            let change = followed.changes.next().await.unwrap().unwrap();
            at = change.cursor.position;
            writes += change.file.iter().chain(&change.files).count();
            assert!(change.removed.is_empty());
        }
        assert_eq!(writes, 1, "one followed layout publication per seek");
        assert_eq!(client::tui_anchor(&fixture.files).unwrap(), expected);
        assert_eq!(fixture.input.editor.text(), "unsent draft");
        let reopened = CommandSession::open(fixture.volume.clone(), SESSION, 65536, 128)
            .await
            .unwrap();
        assert_eq!(client::tui_anchor(&reopened).unwrap(), expected);
        let fresh_client =
            TaskFileClient::observe(fixture.volume.as_ref(), &fixture.owner, 128, 65536)
                .await
                .unwrap();
        let mut fresh = ClientRenderer::default();
        fresh.update_files(&fresh_client, &fixture.owner);
        fresh
            .configure(fresh_client.views(), &fixture.owner, &reopened)
            .unwrap();
        fresh.layout(&reopened, &fixture.input, 60, 20).unwrap();
        let screen = fresh.screen_for(&reopened).unwrap();
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        terminal
            .draw(|frame| {
                screen.render(frame, &fixture.input, &mut Viewport::default(), 0, 2);
            })
            .unwrap();
        assert_eq!(
            terminal.backend().buffer(),
            fixture.terminal.backend().buffer(),
            "reopen paints the followed file position"
        );
    }
}

async fn popup<const EXCLUDE: bool>() {
    let mut fixture = Fixture::new().await;
    let track = fixture.areas.scrollbar;
    let effects = fixture.route::<true>(mouse(
        MouseEventKind::Down(MouseButton::Left),
        track.x,
        track.y,
    ));
    fixture.apply(effects).await;
    fixture
        .input
        .completion
        .set_file("commands/scroll-test.md", Some(b"Scroll test"))
        .unwrap();
    fixture.input.editor.set_text("/", 3);
    fixture.input.refresh_completions();
    fixture.paint();
    let covered = fixture
        .areas
        .completions
        .intersection(fixture.areas.scrollbar);
    assert!(
        covered.width != 0 && covered.height != 0,
        "paint a real popup over the track"
    );
    if !EXCLUDE {
        // Restore the missing popup guard, retaining actual painted geometry
        // and the production event router with its captured scrollbar drag.
        fixture.areas.completions = Rect::default();
    }
    let effects = fixture.route::<true>(mouse(
        MouseEventKind::Drag(MouseButton::Left),
        covered.x,
        covered.y,
    ));
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, InputEffect::ScrollTo { .. })),
        "popup must own the covered pointer"
    );
}

async fn capture_ends_with_release_focus_or_hidden_track() {
    for cause in [0, 1, 2, 3] {
        let mut fixture = Fixture::new().await;
        let track = fixture.areas.scrollbar;
        let effects = fixture.route::<true>(mouse(
            MouseEventKind::Down(MouseButton::Left),
            track.x,
            track.y,
        ));
        fixture.apply(effects).await;
        match cause {
            0 => {
                let effects = fixture.route::<true>(mouse(
                    MouseEventKind::Up(MouseButton::Left),
                    track.x,
                    track.y,
                ));
                fixture.apply(effects).await;
            }
            1 => {
                let effects = fixture.route::<true>(InputEvent::Focus(false));
                fixture.apply(effects).await;
            }
            2 => {
                fixture.terminal.backend_mut().resize(60, 0);
                fixture.paint();
            }
            _ => {
                fixture
                    .files
                    .write_state(
                        CommandFileRoot::Session,
                        "layout.json",
                        &serde_json::json!({"open_view":"feedback","view_input":"feedback"}),
                    )
                    .await
                    .unwrap();
                fixture.paint();
            }
        }
        let before = fixture.volume.head().await.unwrap();
        let effects = fixture.route::<true>(mouse(
            MouseEventKind::Drag(MouseButton::Left),
            track.x,
            track.bottom() - 1,
        ));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, InputEffect::ScrollTo { .. })),
            "inactive track must release capture after cause {cause}"
        );
        fixture.apply(effects).await;
        assert_eq!(fixture.volume.head().await.unwrap(), before);
    }
}

fn run(future: impl std::future::Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future);
}

#[test]
fn transferring_capture_cannot_leave_a_hidden_prompt_drag() {
    run(async {
        let mut fixture = Fixture::new().await;
        let prompt = crate::tui::PromptBox::input_area(fixture.areas.prompt);
        let effects = fixture.route::<true>(mouse(
            MouseEventKind::Down(MouseButton::Left),
            prompt.x + 1,
            prompt.y,
        ));
        fixture.apply(effects).await;
        let before = (
            fixture.input.editor.cursor(),
            fixture.input.editor.selection(),
        );
        let track = fixture.areas.scrollbar;
        for kind in [
            MouseEventKind::Down(MouseButton::Left),
            MouseEventKind::Up(MouseButton::Left),
        ] {
            let effects = fixture.route::<true>(mouse(kind, track.x, track.y));
            fixture.apply(effects).await;
        }
        let effects = fixture.route::<true>(mouse(
            MouseEventKind::Drag(MouseButton::Left),
            prompt.x + 8,
            prompt.y,
        ));
        fixture.apply(effects).await;
        assert_eq!(
            (
                fixture.input.editor.cursor(),
                fixture.input.editor.selection()
            ),
            before,
            "one drag owner cannot retain an underlying prompt capture"
        );
    });
}
#[test]
fn click_drag_and_reopen_follow_the_layout_file() {
    run(click_drag_reopen::<true>());
}
#[test]
#[should_panic(expected = "scrollbar hit must produce one file-write intent")]
fn negative_control_missing_mouse_binding_is_detected() {
    run(click_drag_reopen::<false>());
}
#[test]
fn a_completion_popup_releases_the_underlying_scrollbar_capture() {
    run(popup::<true>());
}
#[test]
#[should_panic(expected = "popup must own the covered pointer")]
fn negative_control_omitting_popup_capture_is_detected() {
    run(popup::<false>());
}
#[test]
fn inactive_scrollbars_release_capture_without_writing() {
    run(capture_ends_with_release_focus_or_hidden_track());
}

async fn route_sequence<const SKIP_FOCUS: bool>(actions: &[u8]) {
    let mut fixture = Fixture::new().await;
    let first = Some(TranscriptAnchor {
        part: TranscriptPart::Answer,
        position: RowPosition::default(),
    });
    let mut captured = false;
    let mut expected = None;
    for &action in actions {
        let track = fixture.areas.scrollbar;
        let mut seek = false;
        let event = match action {
            0 => {
                captured = true;
                seek = true;
                expected = first;
                mouse(MouseEventKind::Down(MouseButton::Left), track.x, track.y)
            }
            1 => {
                seek = captured;
                if captured {
                    expected = None;
                }
                mouse(
                    MouseEventKind::Drag(MouseButton::Left),
                    track.x,
                    track.bottom() - 1,
                )
            }
            2 => {
                captured = false;
                mouse(MouseEventKind::Up(MouseButton::Left), track.x, track.y)
            }
            3 => {
                captured = false;
                InputEvent::Focus(false)
            }
            4 => {
                captured = false;
                let area = crate::tui::PromptBox::input_area(fixture.areas.prompt);
                mouse(MouseEventKind::Down(MouseButton::Left), area.x + 1, area.y)
            }
            5 => {
                captured = false;
                fixture.terminal.backend_mut().resize(60, 0);
                fixture.paint();
                mouse(MouseEventKind::Drag(MouseButton::Left), track.x, track.y)
            }
            _ => mouse(MouseEventKind::Down(MouseButton::Right), track.x, track.y),
        };
        let effects = if SKIP_FOCUS && action == 3 {
            // Keep all actual routing and file publication, but drop the focus
            // event that releases capture, reproducing a stale pointer owner.
            Vec::new()
        } else {
            fixture.route::<true>(event)
        };
        assert_eq!(
            effects
                .iter()
                .filter(|effect| matches!(effect, InputEffect::ScrollTo { .. }))
                .count(),
            usize::from(seek),
            "pointer routing must equal the independent capture model"
        );
        fixture.apply(effects).await;
        assert_eq!(client::tui_anchor(&fixture.files).unwrap(), expected);
        assert_eq!(fixture.input.editor.text(), "unsent draft");
        if action == 5 {
            fixture.terminal.backend_mut().resize(60, 20);
            fixture.paint();
        }
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: arbitrary pointer/focus/resize sequences publish precisely the
    // source positions selected by an independent capture reducer, retain the
    // draft, and release capture on focus loss, release, or a zero-sized track.
    #[test]
    fn routed_pointer_sequences_match_the_file_model(actions in proptest::collection::vec(0u8..7, 0..20)) {
        run(route_sequence::<false>(&actions));
    }
}

#[test]
#[should_panic(expected = "pointer routing must equal the independent capture model")]
fn negative_control_losing_focus_release_fails_the_same_model() {
    run(route_sequence::<true>(&[0, 3, 1]));
}

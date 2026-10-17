use super::super::{
    PromptInput, Thinking, Viewport,
    ratatui::{Terminal, backend::TestBackend, buffer::Buffer},
};
use super::*;
use crate::{
    OperationId,
    client::{CommandFileRoot, CommandSession},
    task_volume::TaskVolume,
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

struct NoFiles;
impl crate::conversation::ContentResidencyVerifier for NoFiles {}

fn paint<const BOUND: bool>(
    renderer: &mut ClientRenderer,
    files: &CommandSession<MemoryStream>,
    now: u64,
) -> Buffer {
    let mut input = PromptInput::default();
    input.busy = BOUND && renderer.is_busy();
    let screen = renderer.screen_for(files).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    let scroll = screen.tail_scroll(
        super::super::ratatui::layout::Rect::new(0, 0, 80, 20),
        &input,
    );
    terminal
        .draw(|frame| {
            screen.render(frame, &input, &mut Viewport::default(), scroll, now);
        })
        .unwrap();
    terminal.backend().buffer().clone()
}

async fn followed_busy<const BOUND: bool>() {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("thinking").unwrap(),
        )
        .await
        .unwrap(),
    );
    let id = OperationId::from_bytes([7; 16])
        .to_string()
        .parse()
        .unwrap();
    let mut files = CommandSession::open(volume.clone(), id, 65_536, 64)
        .await
        .unwrap();
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "owner".into(),
    };
    let views = TaskFileViews::default();
    let mut renderer = ClientRenderer::default();
    renderer.update(&views, &owner);
    renderer.configure(&views, &owner, &files).unwrap();
    let text = |buffer: &Buffer| {
        buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
    };
    assert!(!text(&paint::<BOUND>(&mut renderer, &files, 0)).contains("Thinking…"));
    files.submit("hello", &NoFiles).await.unwrap();
    renderer.configure(&views, &owner, &files).unwrap();
    let first = paint::<BOUND>(&mut renderer, &files, 0);
    let later = paint::<BOUND>(&mut renderer, &files, Thinking::FRAME_MS);
    assert!(
        text(&first).contains("Thinking…"),
        "followed busy state must render thinking"
    );
    let start = first
        .content
        .windows(9)
        .position(|cells| cells.iter().map(|cell| cell.symbol()).collect::<String>() == "Thinking…")
        .unwrap();
    assert_ne!(first.content[start].style(), later.content[start].style());
    assert_ne!(
        first.content[start + 1].style(),
        later.content[start + 1].style()
    );
    let reopened = CommandSession::open(volume, id, 65_536, 64).await.unwrap();
    renderer.configure(&views, &owner, &reopened).unwrap();
    assert_eq!(
        paint::<BOUND>(&mut renderer, &files, 0),
        paint::<BOUND>(&mut renderer, &reopened, 0)
    );
    let request = files.take_request().unwrap();
    files
        .consume(&request, |_, _, _| async {
            Ok(serde_json::json!({"text":"done"}))
        })
        .await
        .unwrap();
    renderer.configure(&views, &owner, &files).unwrap();
    assert!(!text(&paint::<BOUND>(&mut renderer, &files, 0)).contains("Thinking…"));
    assert!(
        files
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .is_none()
    );
}

#[test]
fn file_backed_busy_state_animates_replays_and_settles() {
    futures::executor::block_on(followed_busy::<true>());
}

#[test]
#[should_panic(expected = "followed busy state must render thinking")]
fn missing_busy_binding_is_detected_negative_control() {
    futures::executor::block_on(followed_busy::<false>());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn shimmer_text_and_phase_are_periodic(now in proptest::prelude::any::<u64>()) {
        let theme = super::super::config::ThemeStyles::default();
        let row = Thinking::new(&theme,now).line();
        proptest::prop_assert_eq!(row.spans.iter().map(|span|span.content.as_ref()).collect::<String>(),"Thinking…");
        let lit: Vec<_> = row.spans.iter().enumerate().filter(|(_,span)|span.style.add_modifier.contains(Modifier::BOLD)).map(|(i,_)|i).collect();
        proptest::prop_assert_eq!(lit,vec![((now/Thinking::FRAME_MS)%9) as usize]);
        let period = Thinking::FRAME_MS*9;
        if let Some(later) = now.checked_add(period) {
            proptest::prop_assert_eq!(row,Thinking::new(&theme,later).line());
        }
    }
}

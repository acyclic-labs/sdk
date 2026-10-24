use super::*;
use crate::{
    TaskId,
    client::{CommandFileRoot, CommandSession, TaskFileClient},
    task_volume::{Query, TaskVolume},
    tui::{
        ClientRenderer, PromptInput, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    },
    tui_files::CompletionFiles,
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

struct Fixture {
    client: TaskFileClient,
    commands: CommandSession<MemoryStream>,
    completions: CompletionFiles,
    prompt: PromptInput,
    renderer: ClientRenderer,
    documents: crate::client::DocumentFiles,
    owner: wire::Authority,
    terminal: Terminal<TestBackend>,
    expected: serde_json::Value,
}

async fn fixture(publications: usize) -> Fixture {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("frame-publication-baseline").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([73; 16]);
    let commands = CommandSession::open(volume.clone(), session, 65536, 128)
        .await
        .unwrap();
    let task = TaskId::from_bytes([74; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let mut client = TaskFileClient::observe_task(volume.as_ref(), &owner, task, 128, 65536)
        .await
        .unwrap();
    client.follow_commands(&commands).await.unwrap();
    let mut prompt = PromptInput::default();
    let completions = CompletionFiles::observe(volume.as_ref(), &mut prompt, None, 128)
        .await
        .unwrap();
    let documents = crate::client::DocumentFiles::observe(
        volume.as_ref(),
        client.views_mut(),
        &[(Query::Inputs, 128)],
        65536,
    )
    .await
    .unwrap();
    // A separate publisher ensures the UI must consume its followed updates.
    let mut publisher = CommandSession::open(volume, session, 65536, 128)
        .await
        .unwrap();
    let mut expected = serde_json::Value::Null;
    for index in 0..publications {
        expected = serde_json::json!([format!("notice-{index}")]);
        publisher
            .write_state(CommandFileRoot::Session, "notifications.json", &expected)
            .await
            .unwrap();
    }
    assert_ne!(
        commands
            .state(CommandFileRoot::Session, "notifications.json")
            .unwrap(),
        Some(expected.clone())
    );
    Fixture {
        client,
        commands,
        completions,
        prompt,
        renderer: ClientRenderer::default(),
        documents,
        owner,
        terminal: Terminal::new(TestBackend::new(80, 20)).unwrap(),
        expected,
    }
}

async fn consume<const UNPACED: bool>(fixture: &mut Fixture) -> usize {
    let mut input = futures::stream::pending();
    let mut frames = 0;
    if !UNPACED {
        fixture.renderer.frame_presented();
    }
    while fixture
        .commands
        .state(CommandFileRoot::Session, "notifications.json")
        .unwrap()
        != Some(fixture.expected.clone())
    {
        assert!(
            next_file_tui_event(
                &mut fixture.client,
                &mut input,
                &mut fixture.completions,
                &mut fixture.prompt,
                &mut fixture.commands,
                false,
                &mut fixture.renderer,
                &mut fixture.documents,
            )
            .await
            .unwrap()
            .is_none()
        );
        fixture
            .renderer
            .update_files(&fixture.client, &fixture.owner);
        fixture
            .renderer
            .configure(fixture.client.views(), &fixture.owner, &fixture.commands)
            .unwrap();
        fixture
            .renderer
            .layout(&fixture.commands, &fixture.prompt, 80, 20)
            .unwrap();
        let screen = fixture.renderer.screen_for(&fixture.commands).unwrap();
        fixture
            .terminal
            .draw(|frame| {
                screen.render(frame, &fixture.prompt, &mut Viewport::default(), 0, 0);
            })
            .unwrap();
        frames += 1;
        if !UNPACED {
            fixture.renderer.frame_presented();
        }
    }
    assert!(frames > 0);
    frames
}

#[test]
#[ignore = "manual publication-to-frame measurement"]
fn publication_redraw_measurement() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for publications in [32, 64, 128] {
        let mut fixture = runtime.block_on(fixture(publications));
        let start = std::time::Instant::now();
        let mut frames = 0;
        let allocations = acyclic_test_support::measure(|| {
            frames = runtime.block_on(consume::<false>(&mut fixture));
        });
        eprintln!(
            "CLIENT_FRAME_AFTER publications={publications} frames={frames} elapsed_us={} allocations={} bytes={}",
            start.elapsed().as_micros(),
            allocations.count_total,
            allocations.bytes_total
        );
    }
}

async fn burst<const UNPACED: bool>() {
    for publications in [32, 64, 128] {
        let mut fixture = fixture(publications).await;
        assert_eq!(
            consume::<UNPACED>(&mut fixture).await,
            1,
            "publication burst presents once"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn publications_drain_before_one_frame_at_n_and_2n() {
    burst::<false>().await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "publication burst presents once")]
async fn negative_control_unpaced_publications_present_every_change() {
    burst::<true>().await;
}

#[tokio::test(start_paused = true)]
async fn successful_presentations_are_separated_by_at_least_one_sixtieth_second() {
    use futures::FutureExt as _;
    use std::time::Duration;
    let mut renderer = ClientRenderer::default();
    for _ in 0..120 {
        renderer.frame_presented();
        let start = tokio::time::Instant::now();
        let wait = renderer.wait_frame();
        tokio::pin!(wait);
        assert!(wait.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_nanos(16_666_666)).await;
        assert!(wait.as_mut().now_or_never().is_none(), "no early frame");
        tokio::time::advance(Duration::from_nanos(1)).await;
        wait.await;
        assert!(start.elapsed() >= Duration::from_nanos(16_666_667));
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_wait_retains_dirty_publications_without_idle_wakeups() {
    use futures::FutureExt as _;
    use std::time::Duration;
    let mut fixture = fixture(32).await;
    fixture.renderer.frame_presented();
    let mut input = futures::stream::pending();
    assert!(
        tokio::task::unconstrained(next_file_tui_event(
            &mut fixture.client,
            &mut input,
            &mut fixture.completions,
            &mut fixture.prompt,
            &mut fixture.commands,
            false,
            &mut fixture.renderer,
            &mut fixture.documents,
        ))
        .now_or_never()
        .is_none()
    );
    assert_eq!(
        fixture
            .commands
            .state(CommandFileRoot::Session, "notifications.json")
            .unwrap(),
        Some(fixture.expected)
    );
    assert!(
        fixture.renderer.frame_deadline().is_some(),
        "dirty state survives cancellation"
    );
    tokio::time::advance(Duration::from_millis(17)).await;
    assert!(
        next_file_tui_event(
            &mut fixture.client,
            &mut input,
            &mut fixture.completions,
            &mut fixture.prompt,
            &mut fixture.commands,
            false,
            &mut fixture.renderer,
            &mut fixture.documents,
        )
        .await
        .unwrap()
        .is_none()
    );
    fixture.renderer.frame_presented();
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(
        next_file_tui_event(
            &mut fixture.client,
            &mut input,
            &mut fixture.completions,
            &mut fixture.prompt,
            &mut fixture.commands,
            false,
            &mut fixture.renderer,
            &mut fixture.documents,
        )
        .now_or_never()
        .is_none(),
        "an idle view has no periodic frame wakeup"
    );
}

#[tokio::test(start_paused = true)]
async fn ready_input_precedes_a_dirty_frame_without_waiting_for_its_deadline() {
    use crate::tui::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
    let mut fixture = fixture(32).await;
    fixture.renderer.frame_presented();
    fixture.renderer.request_frame();
    let event = InputEvent::Key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
    let mut input = futures::stream::iter([Ok(InputBatch::from([event.clone()]))]);
    let before = tokio::time::Instant::now();
    let received = next_file_tui_event(
        &mut fixture.client,
        &mut input,
        &mut fixture.completions,
        &mut fixture.prompt,
        &mut fixture.commands,
        false,
        &mut fixture.renderer,
        &mut fixture.documents,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(received, InputBatch::from([event]));
    assert_eq!(before.elapsed(), std::time::Duration::ZERO);
    assert!(fixture.renderer.frame_deadline().is_some());
}

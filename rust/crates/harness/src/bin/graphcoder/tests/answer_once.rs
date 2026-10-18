//! One completed answer has one transcript copy, through the production
//! LocalHarness command owner, TaskFileClient follow and ClientRenderer frame
//! sequence of `view::run` (follow, update, configure, layout, screen).
use super::*;
use acyclic_harness::{
    Error, LocalHarness, TaskId,
    client::{self, CommandFileRoot, CommandSession},
    context::ModelContextCapacity,
    model::Model,
    test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
};
use futures::FutureExt as _;
use std::sync::Arc;

const SECTIONS: usize = 12;

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

/// What the transcript holds once the turn has settled: every painted section
/// heading on a frame tall enough for the whole answer, and Find's match count
/// for one heading.
#[derive(Debug, PartialEq)]
struct Transcript {
    sections: Vec<usize>,
    find: String,
}

async fn settled_transcript() -> Transcript {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let answer: String = (0..SECTIONS)
        .map(|section| format!("# Section {section}\n\nBody {section}.\n\n"))
        .collect();
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text(answer)]).with_context_capacity(
            ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
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
    let local = Arc::new(
        LocalHarness::open(local_harness_config(&workspace, (model, provider)).unwrap())
            .await
            .unwrap(),
    );
    let operation = client::local_identity(&local, None, None).await.unwrap();
    let presentation = {
        let local = local.clone();
        async move {
            let authority = local.client_owner();
            let root = TaskId::parse(&authority.id)?;
            let mut client = client::TaskFileClient::observe_task(
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
            commands
                .submit("Write a long markdown answer", reader.as_ref())
                .await?;
            let mut renderer = ClientRenderer::default();
            let mut submitted = false;
            loop {
                // The production loop: follow the active task, else the root.
                client
                    .follow_task(
                        local.task_files().as_ref(),
                        commands.active_task().unwrap_or(root),
                    )
                    .await?;
                renderer.update_files(&client, &authority);
                renderer.configure(client.views(), &authority, &commands)?;
                submitted |= commands.active_task().is_some();
                // Settled: the request was claimed and answered, the root is
                // followed again and the answer reached the transcript.
                if submitted
                    && commands.active_task().is_none()
                    && !renderer.is_busy()
                    && frame(&mut renderer, &commands, 100, 200)?.contains("# Section")
                {
                    break;
                }
                futures::select! {
                    change = client.next_change().fuse() => change?,
                    change = commands.observe_change().fuse() => change?,
                }
            }
            let sections = frame(&mut renderer, &commands, 100, 200)?
                .lines()
                .filter_map(|line| line.trim().strip_prefix("# Section "))
                .map(|number| number.trim().parse().unwrap())
                .collect();
            let mut layout = commands
                .state(CommandFileRoot::Session, "layout.json")?
                .unwrap_or_default();
            layout["open_view"] = "find".into();
            layout["view_input"] = "find".into();
            layout["search_query"] = "Section 7".into();
            commands
                .write_state(CommandFileRoot::Session, "layout.json", &layout)
                .await?;
            let find = loop {
                let painted = frame(&mut renderer, &commands, 120, 40)?;
                let status = painted
                    .lines()
                    .find_map(|line| line.trim().strip_prefix("Find: "))
                    .unwrap()
                    .to_owned();
                if !status.contains('…') {
                    break status.split(" matches").next().unwrap().to_owned();
                }
                renderer.search_progress().await;
            };
            Ok::<_, Error>(Transcript { sections, find })
        }
    };
    let transcript = client::with_local_commands(local.clone(), operation, presentation)
        .await
        .unwrap();
    client::disconnect_local(local).await.unwrap();
    transcript
}

/// The checked property: every section of a completed answer is painted
/// exactly once, in order, and Find sees exactly one match.
fn assert_answer_once(transcript: &Transcript) {
    assert_eq!(
        transcript,
        &Transcript {
            sections: (0..SECTIONS).collect(),
            find: "1/1".into(),
        },
        "a completed answer must be painted exactly once"
    );
}

#[tokio::test]
async fn a_completed_answer_is_painted_once() {
    assert_answer_once(&settled_transcript().await);
}

/// The two-copy transcript the live pass observed: sections 0-11 twice and
/// Find reporting 2/2.
#[test]
#[should_panic(expected = "a completed answer must be painted exactly once")]
fn a_duplicated_answer_is_detected_negative_control() {
    let once: Vec<_> = (0..SECTIONS).collect();
    assert_answer_once(&Transcript {
        sections: [once.clone(), once].concat(),
        find: "1/2".into(),
    });
}

//! Immutable failed command outcomes remain visible and recoverable.
use acyclic_harness::{
    Error, OperationId, TaskId,
    client::{CommandFileRoot, CommandSession},
    task_volume::TaskVolume,
    tui::{ClientRenderer, PromptInput, Viewport},
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use std::sync::Arc;

struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}

fn rows(renderer: &mut ClientRenderer, files: &CommandSession<MemoryStream>) -> Vec<String> {
    renderer
        .screen_for(files)
        .unwrap()
        .notifications
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect()
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(12))]
    // Property: failed outcomes display sanitized feedback, successful outcomes
    // clear it, and replay/clear-history retain the same visible state.
    #[test]
    fn settled_failures_render_and_success_clears_feedback(
        sequence in prop::collection::vec(any::<bool>(), 1..6),
        diagnostic in "[a-z]{1,16}",
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("execution-feedback").unwrap()).await.unwrap());
            let id = OperationId::new().to_string().parse().unwrap();
            let mut files = CommandSession::open(volume.clone(), id, 65_536, 128).await.unwrap();
            let mut renderer = ClientRenderer::default();
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "owner".into() };
            renderer.update(&super::file_views::empty(), &owner);
            for failed in [true, false].into_iter().chain(sequence) {
                files.submit("hello", &NoFiles).await.unwrap();
                let request = files.take_request().unwrap();
                let task = TaskId::new();
                let message = format!("diagnostic {diagnostic}\u{1b}[31m");
                let error = Error::TaskFailed { task, message: message.clone() };
                let recorded_error = Error::TaskFailed { task, message };
                let expected = error.to_string().escape_debug().to_string();
                let result = files.consume(&request, |_, _, _| async {
                    if failed { Err(error) } else { Ok(serde_json::json!({"text":"recovered"})) }
                }).await;
                prop_assert_eq!(result.is_err(), failed);
                prop_assert_eq!(files.failure_is_saved(&request, &recorded_error).await.unwrap(), failed);
                prop_assert!(!files.failure_is_saved(&request, &Error::Storage("later publication failure".into())).await.unwrap());
                prop_assert!(!files.pending_execution().unwrap());
                let expected: Vec<String> = if failed { vec![expected] } else { vec![] };
                prop_assert_eq!(rows(&mut renderer, &files), expected.clone());
                prop_assert!(expected.iter().all(|text| !text.chars().any(char::is_control)));
                let replay = CommandSession::open(volume.clone(), id, 65_536, 128).await.unwrap();
                prop_assert_eq!(rows(&mut ClientRenderer::default(), &replay), expected);
                let mut terminal = super::Terminal::new(super::TestBackend::new(100, 20)).unwrap();
                let screen = renderer.screen_for(&files).unwrap();
                terminal.draw(|frame| { screen.render(frame, &PromptInput::default(), &mut Viewport::default(), 0, 0); }).unwrap();
                prop_assert_eq!(super::screen_text(&terminal).contains("diagnostic"), failed);
            }
            files.write_state(CommandFileRoot::Session, "layout.json", &serde_json::json!({"history_after":files.output_sequence()})).await.unwrap();
            prop_assert!(rows(&mut renderer, &files).is_empty());
            Ok(())
        })?;
    }
}

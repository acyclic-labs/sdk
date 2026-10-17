use super::*;
#[cfg(feature = "tui")]
use acyclic_harness::wire;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Activity is a fold of immutable intent/outcome, unaffected by delivery,
    // reopen, success/failure or another task's inbox.
    #[test]
    fn pending_activity_survives_delivery_and_clears_only_at_outcome(text in "[a-z]{1,64}", failed in any::<bool>()) {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            publish(&volume, ".tasks/foreign/requests/other.json", serde_json::to_vec(&json!({"kind":"prompt","text":"foreign","source":null,"mentions":[]})).unwrap()).await.unwrap();
            let mut commands = CommandSession::open(volume.clone(), session_id(), 8192, 128).await.unwrap();
            assert!(!commands.pending_execution().unwrap());
            commands.publish_resolution(acyclic_harness::client::CommandResolution::Prompt { text, source: None, mentions: Vec::new() }).await.unwrap();
            assert!(commands.pending_execution().unwrap());
            let request = loop {
                let request = commands.next_request().await.unwrap();
                if matches!(request.key, FileKey::Session { .. }) { break request; }
            };
            assert!(commands.pending_execution().unwrap());
            let restored = CommandSession::open(volume.clone(), session_id(), 8192, 128).await.unwrap();
            assert!(restored.pending_execution().unwrap());
            #[cfg(feature = "tui")] {
                use acyclic_harness::tui::{ClientRenderer, PromptInput, Viewport, ratatui::{Terminal, backend::TestBackend}};
                let authority = wire::Authority { kind: wire::AggregateKind::Task as i32, id: TaskId::from_bytes([3;16]).to_string() };
                let views = super::file_views::empty();
                let mut renderer = ClientRenderer::default();
                renderer.update(&views, &authority);
                renderer.configure(&views, &authority, &commands).unwrap();
                assert!(renderer.is_busy());
                let screen = renderer.screen_for(&commands).unwrap();
                let mut terminal = Terminal::new(TestBackend::new(40, 12)).unwrap();
                terminal.draw(|frame| { screen.render(frame, &PromptInput::default(), &mut Viewport::default(), 0, 0); }).unwrap();
                assert!(terminal.backend().buffer().content.iter().any(|cell| cell.symbol() == "●"));
            }
            let result = commands.consume(&request, |_,_,_| async move {
                if failed { Err(Error::Storage("failed".into())) } else { Ok(json!({"text":"done"})) }
            }).await;
            assert_eq!(result.is_err(), failed);
            assert!(!commands.pending_execution().unwrap());
            let restored = CommandSession::open(volume, session_id(), 8192, 128).await.unwrap();
            assert!(!restored.pending_execution().unwrap());
        });
    }
}

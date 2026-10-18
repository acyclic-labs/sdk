//! Ordinary checkout paths remain usable through the stock task command host.
use super::*;

#[tokio::test]
async fn ordinary_read_edit_and_native_overwrite_share_one_file_identity() -> Result<()> {
    let _diagnostics = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_test_writer()
            .finish(),
    );
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::write(
        directory.path().join("example.txt"),
        "before λ\nsecond line\n",
    )
    .map_err(super::super::storage)?;
    let call = |index: u32, name: &str, arguments: Value| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                format!("path-{index}"),
                name.into(),
                arguments,
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ])
    };
    let provider = scripted([
        call(0, "read", json!({"path":"example.txt"})),
        call(
            1,
            "edit",
            json!({"path":"example.txt","old_string":"before λ","new_string":"after λ"}),
        ),
        call(2, "read", json!({"path":"example.txt"})),
        call(3, "bash", json!({"command":"printf native > example.txt"})),
        call(4, "read", json!({"path":"example.txt"})),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        checkout_config(directory.path(), (model()?, provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    super::checkout_reopen::run(&harness).await?;
    let requests = provider.requests();
    assert_eq!(requests.len(), 6);
    for (index, expected) in [(0, "before λ"), (2, "after λ"), (4, "native")] {
        let id = format!("path-{index}");
        let result = requests
            .last()
            .unwrap()
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                crate::model::ModelContentPart::ToolResult {
                    call_id, content, ..
                } if call_id == &id => Some(content),
                _ => None,
            })
            .expect("complete exchange");
        let encoded = serde_json::to_string(result).map_err(super::super::storage)?;
        assert!(encoded.contains(expected), "{encoded}");
    }
    assert_eq!(
        std::fs::read(directory.path().join("example.txt")).map_err(super::super::storage)?,
        b"native"
    );
    Ok(())
}

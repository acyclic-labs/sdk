#![no_main]
#[cfg(test)]
use acyclic_harness::{AgentId, conversation, resources};

use acyclic_harness::{
    Error, OperationId, Result,
    client::{
        CommandCatalog, CommandFileChange, CommandFileSnapshot, CommandPaletteState,
        CommandResolution,
    },
    conversation::ContentResidencyVerifier,
};
use libfuzzer_sys::fuzz_target;

#[path = "../../harness/src/local/command_machine.rs"]
mod command_machine;
#[path = "../command_oracle.rs"]
mod command_oracle;
#[path = "../../harness/src/client/commands/syntax.rs"]
mod syntax;
use acyclic_harness::workflow::ResumableMachine as _;
use acyclic_harness::{Outcome, filesystem, runtime, tool, workflow};
struct NoReads;
impl ContentResidencyVerifier for NoReads {}

fuzz_target!(|input: &[u8]| {
    if input.len() <= 1024 {
        let _ = futures::executor::block_on(async {
            use acyclic_harness::task_volume::{FileKey, FileValue, Query, TaskVolume};
            use std::sync::Arc;
            let volume = Arc::new(
                TaskVolume::open(
                    Arc::new(acyclic_stream::MemoryStream::default()),
                    acyclic_stream::StreamPath::new("fuzz-sources").unwrap(),
                )
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            );
            let mut client = acyclic_harness::client::CommandSession::open(
                volume.clone(),
                serde_json::from_str("\"00000000-0000-0000-0000-000000000001\"").unwrap(),
                32_768,
                64,
            )
            .await?;
            if let Ok(layout) = serde_json::from_slice::<serde_json::Value>(input) {
                if layout.is_object() {
                    client
                        .write_state(
                            acyclic_harness::client::CommandFileRoot::Session,
                            "layout.json",
                            &layout,
                        )
                        .await?;
                    let _ = client.submit("yes", &NoReads).await;
                    let _ = client.submit("no", &NoReads).await;
                }
            }
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(input) {
                if let Some(path) = value["command_key"].as_str() {
                    if let Ok(key) = FileKey::parse(path) {
                        let cut = volume
                            .snapshot(&Query::File(key.clone()), 1)
                            .await
                            .map_err(|error| Error::Storage(error.to_string()))?
                            .generation;
                        let bytes = serde_json::to_vec(&CommandResolution::Prompt {
                            text: "fuzz request".into(),
                            source: None,
                            mentions: vec![],
                        })
                        .unwrap();
                        let _ = volume.append(cut, &FileValue::new(key, bytes)).await;
                        client.refresh_files().await?;
                        if let Some(request) = client.take_request() {
                            let _ = client
                                .consume(&request, |_, _, _| async { Ok(serde_json::Value::Null) })
                                .await;
                        }
                    }
                }
            }
            let (path, bytes) = if input.first() == Some(&1) {
                ("skills/fuzz/SKILL.md", &input[1..])
            } else {
                (".codex/prompts/fuzzed.md", input)
            };
            let cut = volume
                .snapshot(&Query::Inputs, 64)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            volume
                .append(
                    cut.generation,
                    &FileValue::new(
                        FileKey::parse(path).expect("canonical source path"),
                        bytes.to_vec(),
                    ),
                )
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            let _source = acyclic_harness::client::CommandSources::observe(
                &mut client,
                acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
                vec![],
                vec![],
                Arc::new(NoReads),
                Default::default(),
                32_768,
                64,
            )
            .await?;
            let encoded =
                serde_json::to_vec(&client.catalog().commands().collect::<Vec<_>>()).unwrap();
            let restored = CommandCatalog::from_bytes(&encoded, 32_768)?;
            assert_eq!(
                client.catalog().commands().collect::<Vec<_>>(),
                restored.commands().collect::<Vec<_>>()
            );
            Ok::<_, Error>(())
        });
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(input) {
        let machine = std::sync::Arc::new(command_machine::DirectTool::new());
        let definition = machine.definition().unwrap();
        assert_eq!(definition.identity().name, machine.identity().name);
        assert_eq!(definition.identity().version, machine.identity().version);
        if let Ok(state) = machine.initialize(&value) {
            let encoded = serde_json::to_vec(&state).unwrap();
            let restored = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(state, restored);
            let _ = machine.transition(&restored, &value);
        }
        let _ = machine.transition(&value, &value);
    }
    if let Ok(control) = acyclic_harness::client::CommandControl::from_bytes(input, 8192) {
        let bytes = serde_json::to_vec(&control).unwrap();
        assert_eq!(
            acyclic_harness::client::CommandControl::from_bytes(&bytes, bytes.len() as u64)
                .unwrap(),
            control
        );
    }
    if let Ok(query) = serde_json::from_slice::<acyclic_harness::task_volume::Query>(input) {
        let encoded = serde_json::to_vec(&query).unwrap();
        assert_eq!(
            serde_json::from_slice::<acyclic_harness::task_volume::Query>(&encoded).unwrap(),
            query
        );
    }
    if let Ok(catalog) = CommandCatalog::from_bytes(input, 8192) {
        let specs: Vec<_> = catalog.commands().collect();
        let encoded = serde_json::to_vec(&specs).unwrap();
        let round_trip = CommandCatalog::from_bytes(&encoded, encoded.len() as u64).unwrap();
        assert_eq!(round_trip.commands().collect::<Vec<_>>(), specs);
    }
    let catalog = CommandCatalog::builtins().unwrap();
    if let Ok(state) = serde_json::from_slice::<CommandPaletteState>(input) {
        let palette = catalog.palette(&state);
        for state in [palette.next(), palette.previous()] {
            assert!(state.selected < palette.entries().len().max(1) as u64);
            let encoded = serde_json::to_vec(&state).unwrap();
            assert_eq!(
                serde_json::from_slice::<CommandPaletteState>(&encoded).unwrap(),
                state
            );
        }
    }
    let snapshot: CommandFileSnapshot = serde_json::from_slice(input).unwrap_or_else(|_| serde_json::from_value(serde_json::json!({
            "config":{"settings.json":{"diff_mode":"auto","diff_modes":["auto","unified","split"]}},
            "task":{"settings.json":{"agent_mode":"code","agent_modes":["code","plan"]}},
            "session":{"layout.json":{"selected_task":"00000000-0000-0000-0000-000000000005"},"attention.json":{"tasks":["00000000-0000-0000-0000-000000000003"]}}
        })).unwrap());
    if let Ok(resolution) = serde_json::from_slice::<CommandResolution>(input) {
        if let Ok(changes) = resolution.file_changes(&snapshot, OperationId::new()) {
            assert!(changes.len() <= 1);
            let encoded = serde_json::to_vec(&changes).unwrap();
            assert_eq!(
                serde_json::from_slice::<Vec<CommandFileChange>>(&encoded).unwrap(),
                changes
            );
        }
    }
    if let Ok(request) = serde_json::from_slice::<acyclic_harness::client::CommandRequest>(input) {
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(
            serde_json::from_slice::<acyclic_harness::client::CommandRequest>(&encoded).unwrap(),
            request
        );
    }
    let Ok(input) = std::str::from_utf8(input) else {
        return;
    };
    if let Ok(query) = acyclic_harness::task_volume::Query::folder(input) {
        assert_eq!(
            serde_json::from_slice::<acyclic_harness::task_volume::Query>(
                &serde_json::to_vec(&query).unwrap()
            )
            .unwrap(),
            query
        );
    }
    if let Ok(key) = acyclic_harness::task_volume::FileKey::parse(input) {
        assert_eq!(
            acyclic_harness::task_volume::FileKey::parse(&key.path()).unwrap(),
            key
        );
        let encoded = serde_json::to_vec(&key).unwrap();
        assert_eq!(
            serde_json::from_slice::<acyclic_harness::task_volume::FileKey>(&encoded).unwrap(),
            key
        );
    }
    let maximum = input.len() as u64;
    let result = syntax::split(input, maximum);
    assert!(command_oracle::accepts(input, maximum, &result));
    let bounded = maximum.saturating_sub(1);
    assert!(command_oracle::accepts(
        input,
        bounded,
        &syntax::split(input, bounded)
    ));
    if input.is_empty() {
        assert!(result.is_err());
    } else if let Some((name, arguments)) = result.expect("positive exact input bound") {
        let normalized = format!("/{name} {arguments}");
        assert_eq!(
            syntax::split(&normalized, normalized.len() as u64).unwrap(),
            Some((name, arguments))
        );
    } else {
        assert!(!input.trim_start().starts_with('/'));
    }
    assert!(syntax::split(input, 0).is_err());
    if !input.is_empty() {
        assert!(syntax::split(input, maximum - 1).is_err());
    }
    let index =
        input.as_bytes().first().copied().unwrap_or_default() as usize % catalog.commands().count();
    let name = &catalog.commands().nth(index).unwrap().name;
    let command = format!("/{name} {input}");
    if let Ok(resolution) = futures::executor::block_on(catalog.resolve(&command, &NoReads, 8192)) {
        assert!(!matches!(&resolution, CommandResolution::Prompt { .. }));
        if let Ok(changes) = resolution.file_changes(&snapshot, OperationId::new()) {
            assert!(changes.len() <= 1);
        }
    }
});

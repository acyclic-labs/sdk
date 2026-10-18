#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{
    Error, OperationId, Result,
    client::{
        CommandCatalog, CommandFileChange, CommandFileSnapshot, CommandPaletteState,
        CommandResolution,
    },
    conversation::ContentResidencyVerifier,
};

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

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
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
                fuzz_support::must_decode(serde_json::from_str(
                    "\"00000000-0000-0000-0000-000000000001\"",
                )),
                32_768,
                64,
            )
            .await?;
            if let Some(layout) = fuzz_support::FuzzInput::parse(
                fuzz_input,
                serde_json::from_slice::<serde_json::Value>,
            ) && layout.is_object()
            {
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
            if let Some(value) = fuzz_support::json_input!(fuzz_input, serde_json::Value)
                && let Some(path) =
                    value.optional_field(|value| value["command_key"].as_str().map(str::as_bytes))
            {
                let fuzz_input = path;
                if let Some(key) = fuzz_support::FuzzInput::parse_text(fuzz_input, FileKey::parse) {
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
                        fuzz_support::must_decode(FileKey::parse(path)),
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
            let expected = client.catalog().commands().cloned().collect::<Vec<_>>();
            let encoded = serde_json::to_vec(&expected).unwrap();
            let _ = fuzz_support::round_trip(
                &expected,
                CommandCatalog::from_bytes(&encoded, 32_768)
                    .map(|catalog| catalog.commands().cloned().collect::<Vec<_>>()),
            );
            Ok::<_, Error>(())
        });
    }
    if let Some(value) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<serde_json::Value>)
    {
        let machine = std::sync::Arc::new(command_machine::DirectTool::new());
        let definition = machine.definition().unwrap();
        assert_eq!(definition.identity().name, machine.identity().name);
        assert_eq!(definition.identity().version, machine.identity().version);
        if let Ok(state) =
            acyclic_harness::workflow::ResumableMachine::initialize(machine.as_ref(), &value)
        {
            let encoded = serde_json::to_vec(&state).unwrap();
            let restored = fuzz_support::round_trip(&state, serde_json::from_slice(&encoded));

            let _ = machine.transition(&restored, &value);
        }
        let _ = machine.transition(&value, &value);
    }
    if let Some(control) =
        fuzz_support::FuzzInput::parse_bounded(fuzz_input, 8192, |input, maximum| {
            acyclic_harness::client::CommandControl::from_bytes(input, maximum)
        })
    {
        let bytes = serde_json::to_vec(&control).unwrap();
        let _ = fuzz_support::round_trip(
            &control,
            acyclic_harness::client::CommandControl::from_bytes(&bytes, bytes.len() as u64),
        );
    }
    if let Some(query) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<acyclic_harness::task_volume::Query>,
    ) {
        let encoded = serde_json::to_vec(&query).unwrap();
        let _ = fuzz_support::round_trip(
            &(query),
            serde_json::from_slice::<acyclic_harness::task_volume::Query>(&encoded),
        );
    }
    if let Some(catalog) =
        fuzz_support::FuzzInput::parse_bounded(fuzz_input, 8192, |input, maximum| {
            CommandCatalog::from_bytes(input, maximum)
        })
    {
        let specs: Vec<_> = catalog.commands().cloned().collect();
        let encoded = serde_json::to_vec(&specs).unwrap();
        let _ = fuzz_support::round_trip(
            &specs,
            CommandCatalog::from_bytes(&encoded, encoded.len() as u64)
                .map(|catalog| catalog.commands().cloned().collect::<Vec<_>>()),
        );
    }
    let catalog = CommandCatalog::builtins().unwrap();
    if let Some(state) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<CommandPaletteState>)
    {
        let palette = catalog.palette(&state);
        for state in [palette.next(), palette.previous()] {
            assert!(state.selected < palette.entries().len().max(1) as u64);
            let encoded = serde_json::to_vec(&state).unwrap();
            let _ = fuzz_support::round_trip(
                &(state),
                serde_json::from_slice::<CommandPaletteState>(&encoded),
            );
        }
    }
    let snapshot: CommandFileSnapshot = fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<CommandFileSnapshot>).unwrap_or_else(|| fuzz_support::must_decode(serde_json::from_value(serde_json::json!({
            "config":{"settings.json":{"diff_mode":"auto","diff_modes":["auto","unified","split"]}},
            "task":{"settings.json":{"agent_mode":"code","agent_modes":["code","plan"]}},
            "session":{"layout.json":{"selected_task":"00000000-0000-0000-0000-000000000005"},"attention.json":{"tasks":["00000000-0000-0000-0000-000000000003"]}}
        }))));
    if let Some(resolution) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<CommandResolution>)
        && let Ok(changes) = acyclic_harness::client::CommandResolution::file_changes(
            &resolution,
            &snapshot,
            OperationId::new(),
        )
    {
        assert!(changes.len() <= 1);
        let encoded = serde_json::to_vec(&changes).unwrap();
        let _ = fuzz_support::round_trip(
            &(changes),
            serde_json::from_slice::<Vec<CommandFileChange>>(&encoded),
        );
    }
    if let Some(request) = fuzz_support::FuzzInput::parse(
        fuzz_input,
        serde_json::from_slice::<acyclic_harness::client::CommandRequest>,
    ) {
        let encoded = serde_json::to_vec(&request).unwrap();
        let _ = fuzz_support::round_trip(
            &(request),
            serde_json::from_slice::<acyclic_harness::client::CommandRequest>(&encoded),
        );
    }
    let Some(input) = fuzz_input.text() else {
        return;
    };
    if let Some(query) =
        fuzz_support::FuzzInput::parse_text(fuzz_input, acyclic_harness::task_volume::Query::folder)
    {
        let _ = fuzz_support::round_trip(
            &(query),
            serde_json::from_slice::<acyclic_harness::task_volume::Query>(
                &serde_json::to_vec(&query).unwrap(),
            ),
        );
    }
    if let Some(key) = fuzz_support::FuzzInput::parse_text(
        fuzz_input,
        acyclic_harness::task_volume::FileKey::parse,
    ) {
        let _ = fuzz_support::round_trip(
            &(key),
            acyclic_harness::task_volume::FileKey::parse(&key.path()),
        );
        let encoded = serde_json::to_vec(&key).unwrap();
        let _ = fuzz_support::round_trip(
            &(key),
            serde_json::from_slice::<acyclic_harness::task_volume::FileKey>(&encoded),
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
    if let Ok(resolution) = futures::executor::block_on(
        acyclic_harness::client::CommandCatalog::resolve(&catalog, &command, &NoReads, 8192),
    ) {
        assert!(!matches!(&resolution, CommandResolution::Prompt { .. }));
        if let Ok(changes) = acyclic_harness::client::CommandResolution::file_changes(
            &resolution,
            &snapshot,
            OperationId::new(),
        ) {
            assert!(changes.len() <= 1);
        }
    }
});

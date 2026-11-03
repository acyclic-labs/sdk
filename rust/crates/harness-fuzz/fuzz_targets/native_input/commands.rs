//! The command catalog and published request files use these same decoders.
use super::actions::admission;
use acyclic_harness::{
    OperationId, TaskId,
    client::commands::{
        BuiltinCommand, CommandAction, CommandArguments, CommandCatalog, CommandQuery,
        CommandRequest, CommandSpec, CommandTaskScope, CommandTool, DEFAULT_COMMANDS_JSON,
    },
};

fn action(value: &CommandAction) {
    admission(value, "command action", &["confirmation"]);
    if let CommandAction::Request { request, .. } = value {
        admission(request, "command request", &[]);
    }
}

fn catalog(bytes: &[u8]) {
    if let Ok(catalog) = CommandCatalog::from_bytes(bytes, bytes.len() as u64) {
        let specs: Vec<_> = catalog.commands().cloned().collect();
        let encoded = serde_json::to_vec(&specs).unwrap();
        let replay = CommandCatalog::from_bytes(&encoded, encoded.len() as u64).unwrap();
        assert_eq!(
            replay.commands().collect::<Vec<_>>(),
            specs.iter().collect::<Vec<_>>(),
            "admitted catalog must preserve its normalized descriptors"
        );
        for spec in &specs {
            for name in std::iter::once(&spec.name).chain(&spec.aliases) {
                assert_eq!(
                    catalog.lookup(name),
                    Some(spec),
                    "catalog name lost its owner"
                );
            }
        }
    }
}

fn catalog_admission() {
    let original: Vec<CommandSpec> = serde_json::from_slice(DEFAULT_COMMANDS_JSON).unwrap();
    let shipped = CommandCatalog::builtins().expect("installed catalog must be admitted");
    assert_eq!(shipped.commands().count(), original.len());
    catalog(DEFAULT_COMMANDS_JSON);
    catalog(b"[]");
    let capture = std::env::var_os("ACYCLIC_FUZZ_CORPUS").map(std::path::PathBuf::from);
    if let Some(directory) = &capture {
        std::fs::write(directory.join("catalog-installed"), DEFAULT_COMMANDS_JSON).unwrap();
        std::fs::write(directory.join("catalog-empty"), b"[]").unwrap();
    }
    for maximum in [0, DEFAULT_COMMANDS_JSON.len() as u64 - 1] {
        assert!(
            CommandCatalog::from_bytes(DEFAULT_COMMANDS_JSON, maximum).is_err(),
            "catalog byte bound must be enforced"
        );
    }
    for (index, spec) in original.into_iter().enumerate() {
        assert_eq!(
            shipped.lookup(&spec.name),
            Some(&spec),
            "installed catalog changed its descriptor"
        );
        let mut uppercase = spec.clone();
        uppercase.name.make_ascii_uppercase();
        for alias in &mut uppercase.aliases {
            alias.make_ascii_uppercase();
        }
        let encoded = serde_json::to_vec(&[uppercase]).unwrap();
        let normalized = CommandCatalog::from_bytes(&encoded, encoded.len() as u64).unwrap();
        assert_eq!(
            normalized.lookup(&spec.name),
            Some(&spec),
            "catalog normalization changed its descriptor"
        );
        catalog(&encoded);
        if let Some(directory) = &capture {
            std::fs::write(
                directory.join(format!("catalog-{index}-uppercase")),
                encoded,
            )
            .unwrap();
        }
        let refuse = |label: &str, specs: &[CommandSpec]| {
            let encoded = serde_json::to_vec(specs).unwrap();
            assert!(
                CommandCatalog::from_bytes(&encoded, encoded.len() as u64).is_err(),
                "catalog {label} must be refused"
            );
            if let Some(directory) = &capture {
                std::fs::write(directory.join(format!("catalog-{index}-{label}")), encoded)
                    .unwrap();
            }
        };
        for arguments in [
            CommandArguments::None,
            CommandArguments::Word,
            CommandArguments::Text,
            CommandArguments::RequiredText,
        ] {
            if arguments != spec.arguments {
                let mut changed = spec.clone();
                changed.arguments = arguments;
                refuse(&format!("argument-shape-{arguments:?}"), &[changed]);
            }
        }
        let mut changed = spec.clone();
        if let CommandAction::Request { confirmation, .. } = &mut changed.action {
            *confirmation = !*confirmation;
            refuse("confirmation-policy", &[changed]);
        }
        if index != 0 {
            continue;
        }
        let mut changed = spec.clone();
        changed.name.make_ascii_uppercase();
        refuse("duplicate-name", &[spec.clone(), changed]);
        for (case, name) in ["", "a::b", "../escape", "a b", "a\0b"]
            .into_iter()
            .enumerate()
        {
            let mut changed = spec.clone();
            changed.name = name.into();
            refuse(&format!("invalid-name-{case}"), &[changed]);
        }
        for (case, description) in ["", " \t\n", "description\0tail"].into_iter().enumerate() {
            let mut changed = spec.clone();
            changed.description = description.into();
            refuse(&format!("invalid-description-{case}"), &[changed]);
        }
        let mut changed = spec.clone();
        changed.aliases.push(changed.name.to_ascii_uppercase());
        refuse("duplicate-alias", &[changed]);
        let mut changed = spec.clone();
        changed.aliases.push("../escape".into());
        refuse("invalid-alias", &[changed]);
        let mut unknown = serde_json::to_value(&spec).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("unrecognized_catalog_field".into(), serde_json::Value::Null);
        let encoded = serde_json::to_vec(&[unknown]).unwrap();
        assert!(
            CommandCatalog::from_bytes(&encoded, encoded.len() as u64).is_err(),
            "catalog unknown descriptor fields must be refused"
        );
        if let Some(directory) = &capture {
            std::fs::write(directory.join("catalog-unknown-field"), encoded).unwrap();
        }
    }
}

pub(super) fn check(bytes: &[u8]) {
    catalog(bytes);
    if let Ok(value) = serde_json::from_slice::<CommandAction>(bytes) {
        action(&value);
    }
    if let Ok(value) = serde_json::from_slice::<CommandRequest>(bytes) {
        admission(&value, "command request", &[]);
    }
    let text = String::from_utf8_lossy(bytes).into_owned();
    admission(
        &CommandRequest::Replace {
            message: OperationId::from_bytes([19; 16]),
            text,
        },
        "command request",
        &[],
    );

    static WRITERS: std::sync::Once = std::sync::Once::new();
    WRITERS.call_once(|| {
        catalog_admission();
        let mut requests = vec![
            CommandRequest::Quit {},
            CommandRequest::New {},
            CommandRequest::Delete {},
            CommandRequest::TogglePause {},
            CommandRequest::CycleMode {},
            CommandRequest::ClearAll {},
            CommandRequest::Export {},
            CommandRequest::Rewind {},
            CommandRequest::Cancel {
                task: TaskId::from_bytes([23; 16]),
            },
            CommandRequest::Replace {
                message: OperationId::from_bytes([19; 16]),
                text: "caller replacement".into(),
            },
            CommandRequest::Theme {
                name: "caller".into(),
            },
            CommandRequest::Compact {
                instructions: "caller".into(),
            },
            CommandRequest::Model {
                model: "caller".into(),
            },
        ];
        for scope in [CommandTaskScope::Subtree, CommandTaskScope::All] {
            requests.push(CommandRequest::Pause { scope });
            requests.push(CommandRequest::Resume { scope });
        }
        let mut actions = vec![CommandAction::Theme {}];
        for request in requests {
            for confirmation in [false, true] {
                actions.push(CommandAction::Request {
                    request: request.clone(),
                    confirmation,
                });
            }
        }
        for query in [
            CommandQuery::Shortcuts,
            CommandQuery::Reasoning,
            CommandQuery::Details,
            CommandQuery::Tree,
            CommandQuery::Terminal,
            CommandQuery::Find,
            CommandQuery::Attention,
            CommandQuery::Theme,
            CommandQuery::Feedback,
            CommandQuery::Model,
            CommandQuery::Diff,
            CommandQuery::History,
            CommandQuery::Plan,
            CommandQuery::Goal,
            CommandQuery::Mcp,
        ] {
            actions.push(CommandAction::Select { query });
        }
        for tool in [
            CommandTool::Shell,
            CommandTool::Spawn,
            CommandTool::WebSearch,
            CommandTool::UrlFetch,
        ] {
            actions.push(CommandAction::Tool { tool });
        }
        for command in [
            BuiltinCommand::Compact,
            BuiltinCommand::Model,
            BuiltinCommand::Diff,
            BuiltinCommand::Plan,
            BuiltinCommand::Goal,
            BuiltinCommand::Mcp,
        ] {
            actions.push(CommandAction::Builtin { command });
        }
        let capture = std::env::var_os("ACYCLIC_FUZZ_CORPUS").map(std::path::PathBuf::from);
        for (index, original) in actions.into_iter().enumerate() {
            action(&original);
            if let CommandAction::Request {
                confirmation: false,
                ..
            } = &original
            {
                let mut omitted = serde_json::to_value(&original).unwrap();
                omitted.as_object_mut().unwrap().remove("confirmation");
                let decoded: CommandAction = serde_json::from_value(omitted).unwrap();
                assert!(
                    matches!(
                        decoded,
                        CommandAction::Request {
                            confirmation: false,
                            ..
                        }
                    ),
                    "omitted confirmation must remain false"
                );
            }
            if let Some(directory) = &capture {
                std::fs::write(
                    directory.join(format!("command-{index}")),
                    serde_json::to_vec(&original).unwrap(),
                )
                .unwrap();
            }
        }
    });
}

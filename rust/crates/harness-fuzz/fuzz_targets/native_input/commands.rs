//! The command catalog and published request files use these same decoders.
use super::actions::admission;
use acyclic_harness::{
    OperationId, TaskId,
    client::commands::{
        BuiltinCommand, CommandAction, CommandQuery, CommandRequest, CommandTaskScope, CommandTool,
    },
};

fn action(value: &CommandAction) {
    admission(value, "command action", &["confirmation"]);
    if let CommandAction::Request { request, .. } = value {
        admission(request, "command request", &[]);
    }
}

pub(super) fn check(bytes: &[u8]) {
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

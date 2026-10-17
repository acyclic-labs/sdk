//! Only advertise selections this renderer can display.
use crate::{
    Result,
    client::{
        BuiltinCommand, CommandAction, CommandFileRoot, CommandQuery, CommandSession, CommandSpec,
        DEFAULT_COMMANDS_JSON,
    },
};
use acyclic_stream::StreamProvider;

fn query_supported(query: CommandQuery) -> bool {
    matches!(
        query,
        CommandQuery::Shortcuts
            | CommandQuery::Reasoning
            | CommandQuery::Details
            | CommandQuery::Diff
            | CommandQuery::Goal
            | CommandQuery::Plan
            | CommandQuery::Feedback
    )
}

fn supported(spec: &CommandSpec) -> bool {
    match spec.action {
        CommandAction::Select { query } => query_supported(query),
        CommandAction::Theme
        | CommandAction::Builtin {
            command: BuiltinCommand::Model | BuiltinCommand::Mcp,
        } => false,
        _ => true,
    }
}

pub(crate) async fn configure<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
) -> Result<()> {
    let mut commands: Vec<_> = files
        .catalog()
        .commands()
        .filter(|spec| supported(spec))
        .cloned()
        .collect();
    // Upgrade only the exact catalog shipped by #626. A user's edited catalog
    // remains authoritative, including an intentionally absent feedback command.
    let mut defaults: Vec<CommandSpec> = serde_json::from_slice(DEFAULT_COMMANDS_JSON)
        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
    defaults.sort_by(|left, right| left.name.cmp(&right.name));
    let legacy: Vec<_> = defaults
        .iter()
        .filter(|spec| {
            supported(spec)
                && !matches!(
                    spec.action,
                    CommandAction::Select {
                        query: CommandQuery::Feedback
                    }
                )
        })
        .cloned()
        .collect();
    let upgrade = commands == legacy;
    if upgrade {
        commands = defaults.into_iter().filter(supported).collect();
    }
    if upgrade || commands.len() != files.catalog().commands().count() {
        let bytes = serde_json::to_vec(&commands)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        files.install_catalog(bytes).await?;
    }
    if let Some(mut layout) = files.state(CommandFileRoot::Session, "layout.json")? {
        let unsupported = layout
            .get("open_view")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|view| {
                !matches!(view, "shortcuts" | "diff" | "goal" | "plan" | "feedback")
            });
        // Only a visible feedback form can intercept chat. Stale form pointers
        // from another frontend must never consume the next ordinary prompt.
        let form = layout
            .get("view_input")
            .is_some_and(|value| !value.is_null())
            && !(layout["open_view"] == "feedback" && layout["view_input"] == "feedback");
        let closed_feedback =
            layout["open_view"] == "feedback" && layout["view_input"] != "feedback";
        if unsupported || form || closed_feedback {
            if let Some(object) = layout.as_object_mut() {
                for key in ["open_view", "view_input", "view_request", "view_back"] {
                    object.remove(key);
                }
            }
            files
                .write_state(CommandFileRoot::Session, "layout.json", &layout)
                .await?;
        }
    }
    Ok(())
}

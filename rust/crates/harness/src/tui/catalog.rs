//! Only advertise selections this renderer can display.
use crate::{
    Result,
    client::{BuiltinCommand, CommandAction, CommandFileRoot, CommandQuery, CommandSession},
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
    )
}

pub(crate) async fn configure<P: StreamProvider + ?Sized>(
    files: &mut CommandSession<P>,
) -> Result<()> {
    let commands: Vec<_> = files
        .catalog()
        .commands()
        .filter(|spec| match spec.action {
            CommandAction::Select { query } => query_supported(query),
            CommandAction::Theme
            | CommandAction::Builtin {
                command: BuiltinCommand::Model | BuiltinCommand::Mcp,
            } => false,
            _ => true,
        })
        .collect();
    if commands.len() != files.catalog().commands().count() {
        let bytes = serde_json::to_vec(&commands)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        files.install_catalog(bytes).await?;
    }
    if let Some(mut layout) = files.state(CommandFileRoot::Session, "layout.json")? {
        let unsupported = layout
            .get("open_view")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|view| !matches!(view, "shortcuts" | "diff" | "goal" | "plan"));
        // This renderer has no interactive view forms. A stale form pointer
        // must not intercept chat, even after another client closed the view.
        let form = layout
            .get("view_input")
            .is_some_and(|value| !value.is_null());
        if unsupported || form {
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

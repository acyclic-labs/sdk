//! The terminal's command projection, shared by help, completion and admission.
use super::*;
use acyclic_harness::client::commands::{BuiltinCommand, CommandAction, CommandSpec};

fn restriction(action: &CommandAction, arguments: Option<&str>) -> Option<&'static str> {
    use CommandQuery as Q;
    use CommandRequest as R;
    match action {
        CommandAction::Tool { .. } => {
            Some("Direct tools unavailable here; ask the agent to use its tools.")
        }
        CommandAction::Request { request, .. } => match request {
            R::Quit {}
            | R::New {}
            | R::Pause { .. }
            | R::Resume { .. }
            | R::ClearAll {}
            | R::Theme { .. }
            | R::Compact { .. }
            | R::Rewind {}
            | R::Merge {}
            | R::Peers { .. } => None,
            R::Cancel { .. }
            | R::Replace { .. }
            | R::Delete {}
            | R::TogglePause {}
            | R::CycleMode {}
            | R::Export {}
            | R::Model { .. } => Some("Unavailable in this terminal view."),
        },
        CommandAction::Select { query } => match query {
            Q::Shortcuts
            | Q::Reasoning
            | Q::Details
            | Q::Feedback
            | Q::Diff
            | Q::Cost
            | Q::History
            | Q::Sessions => None,
            Q::Tree
            | Q::Terminal
            | Q::Find
            | Q::Attention
            | Q::Theme
            | Q::Model
            | Q::Plan
            | Q::Goal
            | Q::Mcp => Some("Slash selection unavailable in this terminal view."),
        },
        CommandAction::Builtin { command } => match command {
            BuiltinCommand::Compact | BuiltinCommand::Diff => None,
            BuiltinCommand::Model => Some("Select the model with --model when starting a session."),
            BuiltinCommand::Plan | BuiltinCommand::Goal | BuiltinCommand::Mcp => {
                Some("Slash selection unavailable in this terminal view.")
            }
        },
        CommandAction::Theme {} if arguments.is_none_or(str::is_empty) => {
            Some("Theme picker unavailable; use /theme auto, dark or light.")
        }
        CommandAction::Theme {} | CommandAction::Peers {} => None,
    }
}

pub(super) fn commands(catalog: &CommandCatalog) -> impl Iterator<Item = CommandSpec> + '_ {
    catalog.commands().cloned().map(|mut spec| {
        if let Some(reason) = restriction(&spec.action, None) {
            spec.description = format!("{reason} {}", spec.description);
        }
        spec
    })
}

pub(super) fn selected(catalog: &CommandCatalog, input: &str) -> Option<&'static str> {
    let (name, arguments) = CommandCatalog::selected(input, u64::MAX).ok()??;
    restriction(&catalog.lookup(&name)?.action, Some(arguments))
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::client::commands::{CommandArguments, CommandTool};

    #[test]
    fn restrictions_follow_actions_and_aliases_not_command_names() -> Result<(), Failure> {
        let mut tool = CommandCatalog::builtins()?
            .lookup("shell")
            .ok_or("shell")?
            .clone();
        tool.name = "inspect".into();
        tool.aliases = vec!["status".into()];
        let mut help = tool.clone();
        help.name = "shell".into();
        help.aliases.clear();
        help.arguments = CommandArguments::None;
        help.action = CommandAction::Select {
            query: CommandQuery::Shortcuts,
        };
        let catalog = CommandCatalog::from_specs(vec![tool, help])?;
        for input in ["/inspect", "/status git status", "/STATUS git status"] {
            assert!(selected(&catalog, input).is_some(), "{input}");
        }
        assert!(selected(&catalog, "/shell").is_none());
        assert!(selected(&catalog, "ask the agent to inspect").is_none());
        let projected = CommandCatalog::from_specs(commands(&catalog).collect())?;
        assert!(
            projected
                .lookup("status")
                .ok_or("alias")?
                .description
                .contains("Direct tools unavailable")
        );
        assert!(matches!(
            projected.lookup("status").ok_or("alias")?.action,
            CommandAction::Tool {
                tool: CommandTool::Shell
            }
        ));
        let input = PromptInput::default();
        let help = View::help(&catalog, &input);
        assert!(help.iter().any(|line| {
            line.to_string().contains("/inspect /status")
                && line.to_string().contains("Direct tools unavailable")
        }));
        Ok(())
    }
}

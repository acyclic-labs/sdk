//! One command catalog drives completion, help and slash resolution.
//!
//! Built-in commands are data (`commands.json`). Prompt commands are
//! Markdown files a project declares under `.claude/commands` or
//! `.codex/prompts`, and skills are `SKILL.md` files under the skill roots;
//! both are resolved by name when a prompt selects them, through
//! [`CommandCatalog::expand`], so imported Markdown never becomes code.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
mod name_index;
mod parse;
mod syntax;
pub use parse::has_frontmatter;
use parse::{expand, validate_name};

pub use crate::command_request::{
    CommandQuery, CommandRequest, CommandResolution, CommandTaskScope, CommandTool, ThemeMode,
};

/// The built-in catalog, as installed.
pub const DEFAULT_COMMANDS_JSON: &[u8] = include_bytes!("commands.json");

/// Where project prompt commands live, in precedence order: a name both
/// define is a conflict.
pub const PROMPT_DIRECTORIES: [&str; 2] = [".claude/commands", ".codex/prompts"];

/// Small argument vocabulary shared by help and validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandArguments {
    /// No arguments accepted.
    None,
    /// An optional single identifier.
    Word,
    /// Optional free text, including newlines.
    Text,
    /// Nonempty free text, for a direct tool invocation.
    RequiredText,
}

/// Built-in command implementation selected by catalog data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinCommand {
    /// Request manual compaction.
    Compact,
    /// Inspect or change the selected model.
    Model,
    /// Query captured workspace changes.
    Diff,
    /// Query Markdown files in the private plans/ directory.
    Plan,
    /// Query the private GOAL.md.
    Goal,
    /// Query MCP server/tool configuration.
    Mcp,
}

/// Finite implementations; imported Markdown never becomes executable code.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandAction {
    /// A fixed client operation; destructive operations require confirmation.
    Request {
        /// Operation routed to its client owner, without a model turn.
        request: CommandRequest,
        /// Ask for confirmation before admitting this operation.
        #[serde(default)]
        confirmation: bool,
    },
    /// A renderer selection or toggle.
    Select {
        /// Selection handled by the renderer.
        query: CommandQuery,
    },
    /// Open an authored file under the selected agent's ui/ root.
    View {},
    /// Direct tool dispatch, never prompt expansion.
    Tool {
        /// Owner-selected tool implementation.
        tool: CommandTool,
    },
    /// Resolve a configured name, otherwise open the theme picker.
    Theme {},
    /// User-only membership change; never a settings or request-file write.
    Peers {},
    /// A typed built-in request or view selection.
    Builtin {
        /// Implementation identifier.
        command: BuiltinCommand,
    },
}

/// Renderer-independent command descriptor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandSpec {
    /// Slash-free name; subdirectories use `:` namespaces.
    pub name: String,
    /// Slash-free alternative names, reserved alongside the canonical name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Help and completion text.
    pub description: String,
    /// Accepted argument shape.
    pub arguments: CommandArguments,
    /// Resolution implementation.
    pub action: CommandAction,
}

/// Validated immutable catalog, sorted by name for help and completion.
#[derive(Clone, Debug)]
pub struct CommandCatalog {
    commands: BTreeMap<String, CommandSpec>,
    names: name_index::NameIndex,
}

impl CommandCatalog {
    /// The built-in commands.
    pub fn builtins() -> Result<Self> {
        let bytes = DEFAULT_COMMANDS_JSON;
        Self::from_bytes(bytes, bytes.len() as u64)
    }

    /// Decode a bounded catalog file; duplicate names or aliases fail.
    pub fn from_bytes(bytes: &[u8], maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 || bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid("catalog file exceeds byte bound".into()));
        }
        let specs: Vec<CommandSpec> = serde_json::from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("invalid command catalog: {error}")))?;
        Self::from_specs(specs)
    }

    /// Validate already decoded descriptors from a versioned file projection.
    /// The source reader owns byte admission; validation is shared with builtins.
    pub fn from_specs(specs: Vec<CommandSpec>) -> Result<Self> {
        let mut names = name_index::NameIndex::default();
        let mut commands = BTreeMap::new();
        for mut spec in specs {
            spec.name.make_ascii_lowercase();
            for alias in &mut spec.aliases {
                alias.make_ascii_lowercase();
            }
            for name in std::iter::once(&spec.name).chain(&spec.aliases) {
                validate_name(name)?;
                if names.insert(name, &spec.name).is_some() {
                    return Err(Error::Conflict("duplicate command or alias".into()));
                }
            }
            if spec.description.trim().is_empty() || spec.description.contains('\0') {
                return Err(Error::Invalid("invalid command description".into()));
            }
            let expected = match &spec.action {
                CommandAction::Builtin {
                    command: BuiltinCommand::Compact,
                }
                | CommandAction::Theme {} => CommandArguments::Text,
                CommandAction::Builtin {
                    command: BuiltinCommand::Model,
                }
                | CommandAction::Peers {} => CommandArguments::Word,
                CommandAction::Builtin { .. }
                | CommandAction::Request { .. }
                | CommandAction::Select { .. } => CommandArguments::None,
                CommandAction::Tool { .. } | CommandAction::View {} => {
                    CommandArguments::RequiredText
                }
            };
            if spec.arguments != expected {
                return Err(Error::Invalid(
                    "command argument shape disagrees with action".into(),
                ));
            }
            if let CommandAction::Request {
                request,
                confirmation,
            } = &spec.action
                && *confirmation
                    != matches!(
                        request,
                        CommandRequest::Delete {} | CommandRequest::ClearAll {}
                    )
            {
                return Err(Error::Invalid(
                    "confirmation is required only for delete/clearall".into(),
                ));
            }
            commands.insert(spec.name.clone(), spec);
        }
        Ok(Self { commands, names })
    }

    /// Same descriptors used for help, completion and resolution.
    pub fn commands(&self) -> impl Iterator<Item = &CommandSpec> {
        self.commands.values()
    }

    /// Prefix matches names or aliases, with direct tools first, then lexical order.
    pub fn complete<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a CommandSpec> {
        let prefix = prefix
            .strip_prefix('/')
            .unwrap_or(prefix)
            .to_ascii_lowercase();
        let matches = move |spec: &&CommandSpec| {
            spec.name.starts_with(&prefix)
                || spec.aliases.iter().any(|alias| alias.starts_with(&prefix))
        };
        self.commands
            .values()
            .filter(|spec| matches!(spec.action, CommandAction::Tool { .. }))
            .filter(matches.clone())
            .chain(
                self.commands
                    .values()
                    .filter(|spec| !matches!(spec.action, CommandAction::Tool { .. }))
                    .filter(matches),
            )
    }

    /// The built-in `input` selects, by name or alias.
    pub fn lookup(&self, name: &str) -> Option<&CommandSpec> {
        self.names
            .get(name)
            .and_then(|canonical| self.commands.get(canonical))
    }

    /// Resolves `input` against the built-ins: `None` when it is not a
    /// slash command or names no built-in, so it is a prompt whose
    /// command, if any, the harness resolves from the project's files.
    pub fn resolve(&self, input: &str, maximum_bytes: u64) -> Result<Option<CommandResolution>> {
        let Some((name, arguments)) = syntax::split(input, maximum_bytes)? else {
            return Ok(None);
        };
        let Some(spec) = self.lookup(name) else {
            return Ok(None);
        };
        match spec.arguments {
            CommandArguments::None if !arguments.is_empty() => {
                return Err(Error::Invalid(format!("/{name} accepts no arguments")));
            }
            CommandArguments::Word
                if arguments
                    .chars()
                    .any(|ch| ch.is_whitespace() || ch.is_control()) =>
            {
                return Err(Error::Invalid(format!("/{name} accepts one identifier")));
            }
            CommandArguments::RequiredText if arguments.is_empty() || arguments.contains('\0') => {
                return Err(Error::Invalid(format!("/{name} requires nonempty text")));
            }
            _ => {}
        }
        Ok(Some(match &spec.action {
            CommandAction::View {} => CommandResolution::View {
                path: crate::client::ui::UiPath::new(arguments)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .as_str()
                    .to_owned(),
            },
            CommandAction::Request {
                request,
                confirmation: true,
            } => CommandResolution::Confirm {
                request: request.clone(),
            },
            CommandAction::Request { request, .. } => CommandResolution::Submit {
                request: request.clone(),
            },
            CommandAction::Select { query } => CommandResolution::Select { query: *query },
            CommandAction::Tool { tool } => CommandResolution::Tool {
                tool: *tool,
                input: tool.input(arguments),
            },
            CommandAction::Theme {} => match arguments {
                "" => CommandResolution::Select {
                    query: CommandQuery::Theme,
                },
                mode => CommandResolution::Submit {
                    request: CommandRequest::Theme {
                        mode: match mode {
                            "auto" => ThemeMode::Auto,
                            "dark" => ThemeMode::Dark,
                            "light" => ThemeMode::Light,
                            _ => {
                                return Err(Error::Invalid(
                                    "/theme expects auto, dark or light".into(),
                                ));
                            }
                        },
                    },
                },
            },
            CommandAction::Peers {} => CommandResolution::Submit {
                request: CommandRequest::Peers {
                    enabled: match arguments {
                        "on" => true,
                        "off" => false,
                        _ => return Err(Error::Invalid("/peers expects on or off".into())),
                    },
                },
            },
            CommandAction::Builtin { command } => resolve_builtin(*command, arguments),
        }))
    }

    /// The prompt command or skill name `input` selects and its arguments,
    /// when its name is well formed. Client actions are resolved by the active
    /// catalogue before prompt admission; the server reserves no builtin names.
    pub fn selected(
        input: &str,
        maximum_bytes: impl Into<Option<u64>>,
    ) -> Result<Option<(String, &str)>> {
        let Some((name, arguments)) = syntax::split(input, maximum_bytes)? else {
            return Ok(None);
        };
        // A leading path such as `/usr/bin` names no command: it is prose.
        let name = name.to_ascii_lowercase();
        Ok(validate_name(&name).is_ok().then_some((name, arguments)))
    }

    /// A prompt command or skill body with its frontmatter removed and its
    /// `$ARGUMENTS` replaced by `arguments`.
    pub fn expand(
        body: &[u8],
        arguments: &str,
        maximum_bytes: impl Into<Option<u64>>,
    ) -> Result<String> {
        let body = if has_frontmatter(body) {
            let (_, _, start) = crate::context::frontmatter_end(body)?;
            &body[start..]
        } else {
            body
        };
        let text = std::str::from_utf8(body)
            .map_err(|_| Error::Invalid("command body is not UTF-8".into()))?;
        expand(text, arguments, maximum_bytes.into())
    }
}

/// A prompt command's relative file paths for `name`: one per prompt
/// directory, its `:` namespaces as subdirectories.
pub fn prompt_paths(name: &str) -> impl Iterator<Item = String> + '_ {
    PROMPT_DIRECTORIES
        .into_iter()
        .map(move |directory| format!("{directory}/{}.md", name.replace(':', "/")))
}

fn resolve_builtin(command: BuiltinCommand, arguments: &str) -> CommandResolution {
    use CommandQuery as Q;
    use CommandRequest as R;
    use CommandResolution::{Select, Submit};
    match command {
        BuiltinCommand::Compact => Submit {
            request: R::Compact {
                instructions: arguments.into(),
            },
        },
        BuiltinCommand::Model if !arguments.is_empty() => Submit {
            request: R::Model {
                model: arguments.into(),
            },
        },
        BuiltinCommand::Model => Select { query: Q::Model },
        BuiltinCommand::Diff => Select { query: Q::Diff },
        BuiltinCommand::Plan => Select { query: Q::Plan },
        BuiltinCommand::Goal => Select { query: Q::Goal },
        BuiltinCommand::Mcp => Select { query: Q::Mcp },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_are_user_controls_never_prompt_or_file_requests() -> Result<()> {
        let catalog = CommandCatalog::builtins()?;
        for (input, enabled) in [("/peers off", false), ("/peers on", true)] {
            let resolution = catalog
                .resolve(input, 64)?
                .ok_or_else(|| Error::Invalid("missing peer command".into()))?;
            assert_eq!(
                resolution,
                CommandResolution::Submit {
                    request: CommandRequest::Peers { enabled }
                }
            );
            assert!(resolution.file_inputs().is_empty());
            assert!(
                resolution
                    .file_changes(&Default::default(), crate::OperationId::new())
                    .is_err()
            );
            assert!(resolution.into_model_input().is_err());
        }
        for input in ["/peers", "/peers yes", "/peers off on"] {
            assert!(catalog.resolve(input, 64).is_err());
        }
        Ok(())
    }

    #[test]
    fn builtins_resolve_and_prompts_pass_through() -> Result<()> {
        let catalog = CommandCatalog::builtins()?;
        assert!(matches!(
            catalog.resolve("/diff", 64)?,
            Some(CommandResolution::Select {
                query: CommandQuery::Diff
            })
        ));
        let mut colliding: Vec<_> = catalog.commands.values().cloned().collect();
        colliding
            .iter_mut()
            .find(|command| command.name == "diff")
            .ok_or_else(|| Error::Invalid("diff command is missing".into()))?
            .aliases
            .push("view".into());
        assert!(matches!(
            CommandCatalog::from_specs(colliding),
            Err(Error::Conflict(_))
        ));
        let view = catalog.resolve("/view compare.md", 64)?;
        assert_eq!(
            view,
            Some(CommandResolution::View {
                path: "compare.md".into()
            })
        );
        assert!(
            view.ok_or_else(|| Error::Invalid("view command is missing".into()))?
                .into_model_input()
                .is_err()
        );
        for input in [
            "/view",
            "/view ../compare.md",
            "/view /compare.md",
            "/view volume:///home/memory.md",
        ] {
            assert!(catalog.resolve(input, 128).is_err(), "{input}");
        }
        assert!(matches!(
            catalog.resolve("/help ", 64)?,
            Some(CommandResolution::Select {
                query: CommandQuery::Shortcuts
            })
        ));
        assert!(matches!(
            catalog.resolve("/clearall", 64)?,
            Some(CommandResolution::Confirm {
                request: CommandRequest::ClearAll {}
            })
        ));
        assert_eq!(catalog.resolve("hello", 64)?, None);
        assert_eq!(catalog.resolve("/marker extra", 64)?, None);
        assert_eq!(
            CommandCatalog::selected("/Marker extra", 64)?,
            Some(("marker".into(), "extra"))
        );
        assert_eq!(
            CommandCatalog::selected("/help", 64)?,
            Some(("help".into(), ""))
        );
        assert_eq!(CommandCatalog::selected("/usr/bin/env x", 64)?, None);
        assert!(catalog.complete("/com").any(|spec| spec.name == "compact"));
        Ok(())
    }

    #[test]
    fn bodies_expand_arguments_after_frontmatter() -> Result<()> {
        assert_eq!(
            CommandCatalog::expand(b"MARKER $ARGUMENTS\n", "extra", 64)?,
            "MARKER extra\n"
        );
        assert_eq!(
            CommandCatalog::expand(b"---\nname: x\n---\nBODY\n", "more", 64)?,
            "BODY\n\nmore"
        );
        assert_eq!(
            prompt_paths("ns:leaf").collect::<Vec<_>>(),
            [".claude/commands/ns/leaf.md", ".codex/prompts/ns/leaf.md"]
        );
        Ok(())
    }
}

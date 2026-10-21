//! One catalog drives completion, help and composer resolution.

use super::resolve_file_mentions;
use crate::{
    Error, Result,
    context::{
        ContextDiscoveryLimits, ContextRoot, DiscoveryWork, PinnedContextPath, child,
        discover_skills, frontmatter_end, parse_frontmatter,
    },
    conversation::{ContentResidencyVerifier, PrivateDirectoryEntryKind},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
mod parse;
use parse::{expand, has_frontmatter, validate_name};
mod presentation;
pub use presentation::*;
mod control;
#[cfg(not(target_arch = "wasm32"))]
mod driver;
mod scheduling;
#[cfg(not(target_arch = "wasm32"))]
pub use driver::run_command_client;
pub use scheduling::{command_loop_finished, schedule_command_effect};
mod diff;
mod files;
pub use diff::render_command_diff;
mod view;
pub use view::render_command_record;
mod view_files;
pub use view_files::CommandViewFiles;
#[cfg(all(test, acyclic_loom))]
mod model;
pub use control::CommandControl;
mod name_index;
mod session;
mod sources;
pub use sources::CommandSources;
mod syntax;
pub use files::*;
pub use session::CommandSession;
#[cfg(all(
    feature = "filesystem-local",
    feature = "tui",
    not(target_arch = "wasm32")
))]
pub(crate) use session::ComposerResolver;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod local;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub(crate) use local::run_local_command_files;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local::{
    consume_local_command, run_local_command, run_local_commands, with_local_commands,
};

/// Skills share the context loader's validation, metadata and lazy pinned body.
pub use crate::command_request::{
    CommandQuery, CommandRequest, CommandResolution, CommandTaskScope, CommandTool,
};
pub use crate::context::SkillMetadata as SkillSpec;

/// Bootstrap bytes for commands.json in the config volume. The client installs
/// this file once, then loads its live-query revisions through `from_file`.
pub const DEFAULT_COMMANDS_JSON: &[u8] = include_bytes!("commands.json");

/// A declared home or checkout boundary; no host paths or implicit grants.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandRoot {
    /// User roots precede project roots; project definitions override user definitions.
    pub scope: CommandScope,
    /// Reads stay within this owner-authorized boundary.
    pub root: ContextRoot,
}

/// Origin of a command definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandScope {
    /// User home command directories.
    User,
    /// Project checkout command directories.
    Project,
}

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
    /// Native SDK qualification operation, selected from the installed catalog.
    Sdk {
        /// Existing SDK operation name, independent of its slash spelling.
        command: String,
    },
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
    /// Direct tool dispatch, never prompt expansion.
    Tool {
        /// Owner-selected tool implementation.
        tool: CommandTool,
    },
    /// Resolve a configured name, otherwise open the theme picker.
    Theme,
    /// A typed built-in request or view selection.
    Builtin {
        /// Implementation identifier.
        command: BuiltinCommand,
    },
    /// Lazy user/project prompt template.
    Prompt {
        /// Exact source revision.
        source: PinnedContextPath,
        /// Determines override precedence.
        scope: CommandScope,
    },
    /// Template captured from a followed command/skill definition file.
    Template {
        /// Relative input-file identity for provenance and replacement.
        file: crate::task_volume::FileKey,
        /// Exact file digest at the catalog's publication cut.
        digest: [u8; 32],
        /// Pinned template text; subsequent file edits publish a new catalog.
        body: String,
    },
    /// Selected skill using the ordinary context loader.
    Skill {
        /// Validated metadata and exact source revision.
        skill: SkillSpec,
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
#[derive(Clone, Debug, Serialize)]
pub struct CommandCatalog {
    commands: BTreeMap<String, CommandSpec>,
    #[serde(skip)]
    names: name_index::NameIndex,
    themes: Vec<String>,
}

#[derive(Default, Deserialize)]
struct PromptMetadata {
    description: Option<String>,
    #[serde(flatten)]
    _fields: BTreeMap<String, serde_json::Value>,
}

impl CommandCatalog {
    /// Bootstrap contents for the user's catalog file; live clients load that file.
    pub fn builtins() -> Result<Self> {
        let bytes = DEFAULT_COMMANDS_JSON;
        Self::from_bytes(bytes, bytes.len() as u64)
    }

    /// Discovers optional `.claude/commands` and `.codex/prompts` recursively.
    /// Builtins are reserved; project prompts override user prompts; other duplicates fail.
    /// Skills use explicit skill roots and the existing bounded context loader.
    pub async fn capture(
        reader: &dyn ContentResidencyVerifier,
        roots: &[CommandRoot],
        skill_roots: &[ContextRoot],
        limits: ContextDiscoveryLimits,
    ) -> Result<Self> {
        Self::builtins()?
            .with_sources(reader, roots, skill_roots, limits)
            .await
    }

    /// Fold declared prompt/skill source snapshots into a catalog loaded from
    /// the config volume. A new file-query cut produces a new immutable catalog.
    pub async fn with_sources(
        self,
        reader: &dyn ContentResidencyVerifier,
        roots: &[CommandRoot],
        skill_roots: &[ContextRoot],
        limits: ContextDiscoveryLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if roots.len().saturating_add(skill_roots.len()) > limits.directories as usize {
            return Err(Error::Invalid(
                "catalog declarations exceed directory bound".into(),
            ));
        }
        let mut catalog = self;
        let mut work = DiscoveryWork {
            entries: 0,
            directories: 0,
            limits,
        };
        for scope in [CommandScope::User, CommandScope::Project] {
            let mut names = BTreeSet::new();
            for declaration in roots.iter().filter(|root| root.scope == scope) {
                let root = &declaration.root;
                root.validate(&mut None)?;
                let (generation, entries) = work.list(reader, root, &root.directory, None).await?;
                for (directory, leaf) in [(".claude", "commands"), (".codex", "prompts")] {
                    let Some(entry) = entries.iter().find(|entry| entry.name == directory) else {
                        continue;
                    };
                    require_directory(entry.kind)?;
                    let path = child(&root.directory, directory);
                    let (_, children) = work.list(reader, root, &path, Some(&generation)).await?;
                    let Some(entry) = children.iter().find(|entry| entry.name == leaf) else {
                        continue;
                    };
                    require_directory(entry.kind)?;
                    let mut pending = vec![(child(&path, leaf), String::new())];
                    while let Some((path, namespace)) = pending.pop() {
                        let (_, entries) =
                            work.list(reader, root, &path, Some(&generation)).await?;
                        for entry in entries {
                            let source = PinnedContextPath {
                                root: root.clone(),
                                generation: generation.clone(),
                                path: child(&path, &entry.name),
                            };
                            if entry.kind == PrivateDirectoryEntryKind::Directory {
                                pending.push((source.path, format!("{namespace}{}:", entry.name)));
                                continue;
                            }
                            let Some(stem) = entry.name.strip_suffix(".md") else {
                                continue;
                            };
                            if entry.kind != PrivateDirectoryEntryKind::File {
                                return Err(Error::Invalid("command entry is not a file".into()));
                            }
                            let name = format!("{namespace}{stem}").to_ascii_lowercase();
                            validate_name(&name)?;
                            if !names.insert(name.clone()) {
                                return Err(Error::Conflict(format!("duplicate command: {name}")));
                            }
                            if let Some(existing) = catalog.lookup(&name) {
                                if existing.name != name {
                                    return Err(Error::Conflict(format!(
                                        "reserved command alias: {name}"
                                    )));
                                }
                                if let CommandAction::Prompt {
                                    scope: previous, ..
                                } = &existing.action
                                {
                                    if *previous > scope {
                                        continue;
                                    }
                                    if *previous == scope {
                                        return Err(Error::Conflict(format!(
                                            "duplicate command: {name}"
                                        )));
                                    }
                                } else {
                                    return Err(Error::Conflict(format!(
                                        "reserved command: {name}"
                                    )));
                                }
                            }
                            source.validate(&mut None)?;
                            let prefix = reader
                                .read_private_prefix(
                                    &root.volume,
                                    &root.directory,
                                    &source.path,
                                    &generation,
                                    u64::from(limits.header_bytes),
                                    None,
                                )?
                                .await?;
                            if prefix.len() > limits.header_bytes as usize {
                                return Err(Error::Storage("command prefix exceeded bound".into()));
                            }
                            if prefix.len() == limits.header_bytes as usize
                                && (b"---\n".starts_with(&prefix)
                                    || b"---\r\n".starts_with(&prefix))
                            {
                                return Err(Error::Invalid(
                                    "command header exceeds prefix bound".into(),
                                ));
                            }
                            let metadata: PromptMetadata = if has_frontmatter(&prefix) {
                                parse_frontmatter(&prefix)?
                            } else {
                                PromptMetadata::default()
                            };
                            let description = metadata.description.unwrap_or_else(|| name.clone());
                            if description.trim().is_empty() || description.contains('\0') {
                                return Err(Error::Invalid("invalid command description".into()));
                            }
                            let _ = catalog.names.insert(&name, &name);
                            catalog.commands.insert(
                                name.clone(),
                                CommandSpec {
                                    name,
                                    aliases: Vec::new(),
                                    description,
                                    arguments: CommandArguments::Text,
                                    action: CommandAction::Prompt { source, scope },
                                },
                            );
                        }
                    }
                }
            }
        }
        for skill in discover_skills(reader, skill_roots, "SKILL.md", &mut work).await? {
            let name = skill.name.to_ascii_lowercase();
            let spec = CommandSpec {
                name: name.clone(),
                aliases: Vec::new(),
                description: skill.description.clone(),
                arguments: CommandArguments::Text,
                action: CommandAction::Skill { skill },
            };
            if catalog.lookup(&name).is_some() {
                return Err(Error::Conflict(format!("skill/command collision: {name}")));
            }
            let _ = catalog.names.insert(&name, &name);
            catalog.commands.insert(name, spec);
        }
        Ok(catalog)
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

    /// Whether `input` selects a command no declared source file defines. Declared
    /// project and user sources can never change such a resolution, so capturing
    /// them would only let unrelated command files block it.
    #[cfg(feature = "native-execution")]
    pub(crate) fn selects_sourceless(&self, input: &str, maximum_bytes: u64) -> Result<bool> {
        Ok(
            syntax::split(input, maximum_bytes)?.is_some_and(|(name, _)| {
                self.lookup(name).is_some_and(|spec| {
                    !matches!(
                        spec.action,
                        CommandAction::Prompt { .. } | CommandAction::Skill { .. }
                    )
                })
            }),
        )
    }

    fn lookup(&self, name: &str) -> Option<&CommandSpec> {
        self.names
            .get(name)
            .and_then(|canonical| self.commands.get(canonical))
    }

    /// Install the owning renderer's theme data once; no duplicated preset table.
    pub fn with_themes(mut self, themes: impl IntoIterator<Item = String>) -> Result<Self> {
        self.set_themes(themes)?;
        Ok(self)
    }

    fn set_themes(&mut self, themes: impl IntoIterator<Item = String>) -> Result<()> {
        let mut names = BTreeSet::new();
        let mut validated = Vec::new();
        for theme in themes {
            if theme.trim() != theme
                || theme.is_empty()
                || theme.chars().any(char::is_control)
                || !names.insert(theme.to_lowercase())
            {
                return Err(Error::Invalid("invalid or duplicate theme name".into()));
            }
            validated.push(theme);
        }
        self.themes = validated;
        Ok(())
    }

    /// Resolves syntax, validates arguments and loads only the selected pinned body.
    pub async fn resolve(
        &self,
        input: &str,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
    ) -> Result<CommandResolution> {
        let Some((name, arguments)) = syntax::split(input, maximum_bytes)? else {
            return Ok(CommandResolution::Prompt {
                text: input.into(),
                source: None,
                mentions: Vec::new(),
            });
        };
        let spec = self
            .lookup(name)
            .ok_or_else(|| Error::NotFound(format!("Unknown command: /{name}")))?;
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
        match &spec.action {
            CommandAction::Sdk { command } => Ok(CommandResolution::Sdk {
                command: command.clone(),
                arguments: arguments.into(),
            }),
            CommandAction::Request {
                request,
                confirmation,
            } => Ok(if *confirmation {
                CommandResolution::Confirm {
                    request: request.clone(),
                }
            } else {
                CommandResolution::Submit {
                    request: request.clone(),
                }
            }),
            CommandAction::Select { query } => Ok(CommandResolution::Select { query: *query }),
            CommandAction::Tool { tool } => Ok(CommandResolution::Tool {
                tool: *tool,
                input: tool.input(arguments),
            }),
            CommandAction::Theme => {
                let normalized = arguments.to_lowercase();
                Ok(
                    match self
                        .themes
                        .iter()
                        .find(|theme| theme.to_lowercase() == normalized)
                    {
                        Some(name) => CommandResolution::Submit {
                            request: CommandRequest::Theme { name: name.clone() },
                        },
                        None => CommandResolution::Select {
                            query: CommandQuery::Theme,
                        },
                    },
                )
            }
            CommandAction::Builtin { command } => Ok(resolve_builtin(*command, arguments)),
            CommandAction::Template { body, .. } => Ok(CommandResolution::Prompt {
                text: expand(body, arguments, maximum_bytes)?,
                source: None,
                mentions: Vec::new(),
            }),
            CommandAction::Prompt { source, .. } => {
                let (_, bytes) = source.read(reader, maximum_bytes).await?;
                let body = if has_frontmatter(&bytes) {
                    let (_, _, body) = frontmatter_end(&bytes)?;
                    &bytes[body..]
                } else {
                    &bytes
                };
                let text = std::str::from_utf8(body)
                    .map_err(|_| Error::Invalid("command body is not UTF-8".into()))?;
                Ok(CommandResolution::Prompt {
                    text: expand(text, arguments, maximum_bytes)?,
                    source: Some(source.clone()),
                    mentions: Vec::new(),
                })
            }
            CommandAction::Skill { skill } => {
                let message = skill.invoke(reader, maximum_bytes).await?;
                let crate::model::ModelContent::Text(body) = message.content else {
                    return Err(Error::Invalid("skill body is not text".into()));
                };
                let (_, _, start) = frontmatter_end(body.as_bytes())?;
                let body = body
                    .get(start..)
                    .ok_or_else(|| Error::Invalid("invalid skill body offset".into()))?;
                Ok(CommandResolution::Prompt {
                    text: expand(body, arguments, maximum_bytes)?,
                    source: Some(skill.source.clone()),
                    mentions: Vec::new(),
                })
            }
        }
    }

    /// Resolves the complete composer input, including mentions in expanded prompts.
    /// The workspace revision is supplied independently of the command's home/project
    /// source, so user commands also mention files in the active workspace.
    pub async fn resolve_input(
        &self,
        input: &str,
        workspace: &ContextRoot,
        generation: &crate::resources::GenerationRef,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
    ) -> Result<CommandResolution> {
        let mut resolution = self.resolve(input, reader, maximum_bytes).await?;
        if let CommandResolution::Prompt { text, mentions, .. } = &mut resolution {
            *mentions =
                resolve_file_mentions(text, workspace, generation, reader, maximum_bytes).await?;
        }
        Ok(resolution)
    }
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

fn require_directory(kind: PrivateDirectoryEntryKind) -> Result<()> {
    if kind != PrivateDirectoryEntryKind::Directory {
        return Err(Error::Invalid(
            "command directory is not a directory".into(),
        ));
    }
    Ok(())
}

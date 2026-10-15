//! One catalog drives completion, help and composer resolution.

use super::{FileMention, resolve_file_mentions};
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

/// Skills share the context loader's validation, metadata and lazy pinned body.
pub use crate::context::SkillMetadata as SkillSpec;

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
}

/// Built-in command implementation selected by catalog data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinCommand {
    /// Request manual compaction.
    Compact,
    /// Inspect or change the selected model.
    Model,
    /// Inspect or change the approval mode.
    Approval,
    /// Query the task's plan.md.
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
    /// Help and completion text.
    pub description: String,
    /// Accepted argument shape.
    pub arguments: CommandArguments,
    /// Resolution implementation.
    pub action: CommandAction,
}

/// Typed client requests emitted by built-in commands.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandRequest {
    /// Compact with optional summary guidance.
    Compact {
        /// User-supplied summary guidance.
        instructions: String,
    },
    /// Change the model through the harness request owner.
    Model {
        /// Provider/model identifier.
        model: String,
    },
    /// Change human approval gating; this never grants capabilities.
    Approval {
        /// Owner-defined approval mode identifier.
        mode: String,
    },
}

/// View selected without submitting model text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandQuery {
    /// Current model and available models.
    Model,
    /// Current approval mode and available modes.
    Approval,
    /// Task plan.md.
    Plan,
    /// Private GOAL.md.
    Goal,
    /// MCP servers/tools.
    Mcp,
}

/// Complete composer classification; renderers do not parse slash syntax.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandResolution {
    /// Ordinary text or a resolved prompt template.
    Prompt {
        /// Text ready for mention resolution and submission.
        text: String,
        /// Pins the template/skill revision used to produce the text.
        source: Option<PinnedContextPath>,
        /// Pinned files resolved in the declared workspace, in mention order.
        mentions: Vec<FileMention>,
    },
    /// A typed harness operation.
    Submit {
        /// Operation to admit.
        request: CommandRequest,
    },
    /// A live view to render.
    Select {
        /// View identifier.
        query: CommandQuery,
    },
}

/// Validated immutable catalog, sorted by name for help and completion.
#[derive(Clone, Debug, Serialize)]
pub struct CommandCatalog {
    commands: BTreeMap<String, CommandSpec>,
}

#[derive(Default, Deserialize)]
struct PromptMetadata {
    description: Option<String>,
    #[serde(flatten)]
    _fields: BTreeMap<String, serde_json::Value>,
}

impl CommandCatalog {
    /// Parses the schema-owned built-in catalog.
    pub fn builtins() -> Result<Self> {
        let specs: Vec<CommandSpec> = serde_json::from_str(include_str!("commands.json"))
            .map_err(|error| Error::Invalid(format!("invalid command catalog: {error}")))?;
        let mut commands = BTreeMap::new();
        for spec in specs {
            validate_name(&spec.name)?;
            if commands.insert(spec.name.clone(), spec).is_some() {
                return Err(Error::Conflict("duplicate builtin command".into()));
            }
        }
        Ok(Self { commands })
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
        limits.validate()?;
        if roots.len().saturating_add(skill_roots.len()) > limits.directories as usize {
            return Err(Error::Invalid(
                "catalog declarations exceed directory bound".into(),
            ));
        }
        let mut catalog = Self::builtins()?;
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
                            let name = format!("{namespace}{stem}");
                            validate_name(&name)?;
                            if !names.insert(name.clone()) {
                                return Err(Error::Conflict(format!("duplicate command: {name}")));
                            }
                            if catalog.commands.get(&name).is_some_and(|spec| {
                                matches!(spec.action, CommandAction::Builtin { .. })
                            }) {
                                return Err(Error::Conflict(format!("reserved command: {name}")));
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
                            catalog.commands.insert(
                                name.clone(),
                                CommandSpec {
                                    name,
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
            let name = skill.name.clone();
            let spec = CommandSpec {
                name: name.clone(),
                description: skill.description.clone(),
                arguments: CommandArguments::Text,
                action: CommandAction::Skill { skill },
            };
            if catalog.commands.insert(name.clone(), spec).is_some() {
                return Err(Error::Conflict(format!("skill/command collision: {name}")));
            }
        }
        Ok(catalog)
    }

    /// Same descriptors used for help, completion and resolution.
    pub fn commands(&self) -> impl Iterator<Item = &CommandSpec> {
        self.commands.values()
    }

    /// Prefix completion in stable lexical order, accepting an optional slash.
    pub fn complete<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a CommandSpec> {
        let prefix = prefix.strip_prefix('/').unwrap_or(prefix);
        self.commands
            .values()
            .filter(move |spec| spec.name.starts_with(prefix))
    }

    /// Resolves syntax, validates arguments and loads only the selected pinned body.
    pub async fn resolve(
        &self,
        input: &str,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
    ) -> Result<CommandResolution> {
        if maximum_bytes == 0 || input.len() as u64 > maximum_bytes {
            return Err(Error::Invalid("command input exceeds byte bound".into()));
        }
        let Some(command) = input.trim_start().strip_prefix('/') else {
            return Ok(CommandResolution::Prompt {
                text: input.into(),
                source: None,
                mentions: Vec::new(),
            });
        };
        let end = command.find(char::is_whitespace).unwrap_or(command.len());
        let (name, arguments) = command.split_at(end);
        let arguments = arguments.trim();
        let spec = self
            .commands
            .get(name)
            .ok_or_else(|| Error::NotFound(format!("unknown command: /{name}")))?;
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
            _ => {}
        }
        match &spec.action {
            CommandAction::Builtin { command } => Ok(resolve_builtin(*command, arguments)),
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
        BuiltinCommand::Approval if !arguments.is_empty() => Submit {
            request: R::Approval {
                mode: arguments.into(),
            },
        },
        BuiltinCommand::Model => Select { query: Q::Model },
        BuiltinCommand::Approval => Select { query: Q::Approval },
        BuiltinCommand::Plan => Select { query: Q::Plan },
        BuiltinCommand::Goal => Select { query: Q::Goal },
        BuiltinCommand::Mcp => Select { query: Q::Mcp },
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.split(':').any(|part| {
        part.is_empty()
            || !part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    }) {
        return Err(Error::Invalid("invalid command name".into()));
    }
    Ok(())
}

fn require_directory(kind: PrivateDirectoryEntryKind) -> Result<()> {
    if kind != PrivateDirectoryEntryKind::Directory {
        return Err(Error::Invalid(
            "command directory is not a directory".into(),
        ));
    }
    Ok(())
}

fn has_frontmatter(bytes: &[u8]) -> bool {
    bytes.starts_with(b"---\n") || bytes.starts_with(b"---\r\n")
}

fn expand(body: &str, arguments: &str, maximum_bytes: u64) -> Result<String> {
    if body.contains('\0') {
        return Err(Error::Invalid("command body contains NUL".into()));
    }
    let mut text = String::new();
    for (index, part) in body.split("$ARGUMENTS").enumerate() {
        let argument = if index == 0 { "" } else { arguments };
        let length = text
            .len()
            .checked_add(argument.len())
            .and_then(|n| n.checked_add(part.len()))
            .ok_or_else(|| Error::Invalid("expanded command size overflow".into()))?;
        if length as u64 > maximum_bytes {
            return Err(Error::Invalid("expanded command exceeds byte bound".into()));
        }
        text.push_str(argument);
        text.push_str(part);
    }
    if !body.contains("$ARGUMENTS") && !arguments.is_empty() {
        if text.len() as u64 + 1 + arguments.len() as u64 > maximum_bytes {
            return Err(Error::Invalid("expanded command exceeds byte bound".into()));
        }
        text.push('\n');
        text.push_str(arguments);
    }
    Ok(text)
}

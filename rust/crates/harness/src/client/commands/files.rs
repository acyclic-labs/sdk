use super::*;

impl CommandCatalog {
    /// Decode a bounded catalog-file snapshot; duplicate names/aliases fail.
    pub fn from_bytes(bytes: &[u8], maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 || bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid("catalog file exceeds byte bound".into()));
        }
        let specs: Vec<CommandSpec> = serde_json::from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("invalid command catalog: {error}")))?;
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
                } => CommandArguments::Text,
                CommandAction::Builtin {
                    command: BuiltinCommand::Model,
                } => CommandArguments::Word,
                CommandAction::Builtin { .. }
                | CommandAction::Request { .. }
                | CommandAction::Select { .. } => CommandArguments::None,
                CommandAction::Tool { .. } => CommandArguments::RequiredText,
                CommandAction::Theme
                | CommandAction::Sdk { .. }
                | CommandAction::Prompt { .. }
                | CommandAction::Template { .. }
                | CommandAction::Skill { .. } => CommandArguments::Text,
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
            {
                if *confirmation
                    != matches!(request, CommandRequest::Delete | CommandRequest::ClearAll)
                {
                    return Err(Error::Invalid(
                        "confirmation is required only for delete/clearall".into(),
                    ));
                }
            }
            match &spec.action {
                CommandAction::Sdk { command } => validate_name(command)?,
                CommandAction::Template { file, body, .. } => {
                    if !matches!(file, crate::task_volume::FileKey::Input(_)) || body.contains('\0')
                    {
                        return Err(Error::Invalid("invalid file template".into()));
                    }
                }
                CommandAction::Prompt { source, .. } => source.validate(&mut None)?,
                CommandAction::Skill { skill } => skill.validate(&mut None)?,
                _ => {}
            }
            commands.insert(spec.name.clone(), spec);
        }
        Ok(Self {
            commands,
            names,
            themes: Vec::new(),
        })
    }

    /// Load the authoritative command-definition file at the live query's cut.
    /// Bootstrap/default installation and publication belong to the FS client.
    pub async fn from_file(
        source: &PinnedContextPath,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
    ) -> Result<Self> {
        let (_, bytes) = source.read(reader, maximum_bytes).await?;
        Self::from_bytes(&bytes, maximum_bytes)
    }
}

pub use crate::command_request::{CommandFileChange, CommandFileRoot, CommandFileSnapshot};

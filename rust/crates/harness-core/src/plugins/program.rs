//! Program files use the settings evaluator and the existing command runner.
use crate::{
    Error, Result,
    config::{self, ProgramManifest, Surface, Tree},
    hooks::HookRun,
    tool::ToolDefinition,
};

/// A manifest and entry bytes selected from one caller-pinned effective tree.
/// The revision includes imported manifest modules and both native entries.
/// Retain this value through admission and dispatch; do not reopen entry files.
#[derive(Debug)]
pub struct LoadedProgram {
    /// The same declaration consumed by the stock tool registry.
    pub definition: ToolDefinition,
    /// The same command consumed by the stock hook runner.
    pub run: HookRun,
    // Retained manifest and entry bytes retire before their original credit.
    custody: acyclic_fs::NativeCustody,
}

impl LoadedProgram {
    /// Binds an authored tool to the host's ordinary shell capability. Every
    /// invocation is canonicalized before permission and approval checks, so
    /// authorship cannot widen the execution grant. Arguments travel as JSON
    /// stdin, separately from executable argv and the pinned script source.
    ///
    /// # Errors
    /// Refuses a non-shell capability or an unsupported command form.
    pub fn bind(self, shell: std::sync::Arc<crate::tool::Tool>) -> Result<crate::tool::Tool> {
        let shell = std::sync::Arc::new(shell.executor.capability().unwrap_or(&shell).clone());
        if shell.definition.name != crate::tools::shell::PLATFORM.name {
            return Err(Error::Invalid(
                "programs require the canonical shell capability".into(),
            ));
        }
        let HookRun::Shell {
            command,
            shell: selected,
        } = self.run
        else {
            return Err(Error::Invalid("program has no pinned shell source".into()));
        };
        let (executable, arguments) = match selected {
            Some(selected) => {
                let arguments = acyclic_host_platform::shell::arguments(&selected)
                    .ok_or_else(|| Error::Unsupported("program shell is unavailable".into()))?;
                (selected, arguments)
            }
            None => {
                let executable =
                    acyclic_native_runtime::platform_shell().map_err(Error::process_io)?;
                let executable = executable.to_str().ok_or_else(|| {
                    Error::Unsupported("program shell path is not Unicode".into())
                })?;
                (
                    executable.to_owned(),
                    crate::tools::shell::PLATFORM.arguments,
                )
            }
        };
        use serde_json::{Map, Value};
        let mut argv = vec![Value::String(executable)];
        argv.extend(
            arguments
                .iter()
                .map(|argument| Value::String((*argument).to_owned())),
        );
        argv.push(Value::String(command));
        let stdin = Value::Object(Map::from_iter([(
            "$json".into(),
            Value::String("@arguments".into()),
        )]));
        let command = Value::Object(Map::from_iter([
            ("argv".into(), Value::Array(argv)),
            ("stdin".into(), stdin),
        ]));
        let template = Value::Object(Map::from_iter([("command".into(), command)]));
        crate::tools::native::alias(self.definition, shell, template, self.custody)
    }
}

/// Loads one named program, without scanning sibling manifests or entries.
/// Directory listings therefore need only names; descriptions and schemas are
/// materialized when the consumer selects a name. `tree` must retain a single
/// generation, just as it does for plugin and settings admission.
///
/// # Errors
/// Refuses an invalid manifest, a name mismatch, an escaping entry path, or a
/// missing native form. Operational reads retain their original error.
pub async fn load_program(tree: &dyn Tree, directory: &str) -> Result<LoadedProgram> {
    crate::conversation::validate_content_path(directory, &mut None)?;
    let (kind, name) = directory
        .split_once('/')
        .filter(|(kind, name)| {
            matches!(*kind, "tools" | "hooks")
                && crate::contract::is_valid_component_label(name)
                && !name.contains('/')
        })
        .ok_or_else(|| {
            Error::Invalid("program directory must be tools/<name> or hooks/<name>".into())
        })?;
    let path = format!("{kind}/{name}/program.ts");
    let source = tree
        .read(&path)
        .await?
        .ok_or_else(|| Error::NotFound(path.clone()))?;
    let evaluated = config::evaluate(
        &path,
        &source,
        tree,
        &serde_json::Value::Null,
        Surface::Data,
    )
    .await?;
    let mut custody = evaluated._custody;
    let manifest: ProgramManifest = serde_json::from_value(evaluated.value)
        .map_err(|error| Error::Invalid(format!("{path}: {error}")))?;
    if manifest.version != 1 || manifest.name != name {
        return Err(Error::Invalid(format!(
            "{path}: unsupported version or mismatched name"
        )));
    }
    let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.program.v1");
    identity.update(&evaluated.digest);
    let mut native = None;
    for (form, entry) in [("sh", manifest.entry.sh), ("ps1", manifest.entry.ps1)] {
        identity.update(form.as_bytes());
        let Some(entry) = entry else {
            identity.update(&[0]);
            continue;
        };
        crate::conversation::validate_content_path(&entry, &mut None)?;
        if !entry.ends_with(&format!(".{form}")) {
            return Err(Error::Invalid(format!(
                "{path}: {form} entry has the wrong extension"
            )));
        }
        let script_path = format!("{directory}/{entry}");
        let bytes = tree
            .read(&script_path)
            .await?
            .ok_or_else(|| Error::NotFound(script_path.clone()))?;
        let script = std::str::from_utf8(&bytes)
            .map_err(|_| Error::Invalid(format!("{script_path}: entry is not UTF-8")))?;
        identity.update(&[1]);
        identity.update(&(entry.len() as u64).to_be_bytes());
        identity.update(entry.as_bytes());
        identity.update(&(bytes.len() as u64).to_be_bytes());
        identity.update(&bytes);
        if (form == "ps1") == cfg!(windows) {
            if let Some(custody) = &mut custody {
                custody.grow(script.len() as u64)?;
                custody.work().spent(acyclic_fs::WorkCounters {
                    bytes_copied: script.len() as u64,
                    ..acyclic_fs::WorkCounters::UNCHARGED
                })?;
            }
            native = Some(script.to_owned());
        }
    }
    let command = native
        .ok_or_else(|| Error::Unsupported(format!("{path}: no native entry for this host")))?;
    let definition = ToolDefinition {
        name: manifest.name,
        revision: identity.finalize().to_hex().to_string(),
        description: manifest.description,
        input_schema: manifest.input_schema,
        output_schema: serde_json::json!({}),
        projection_schema: serde_json::json!({}),
    };
    definition.validate(&mut None)?;
    Ok(LoadedProgram {
        definition,
        run: HookRun::Shell {
            command,
            shell: if cfg!(windows) {
                None
            } else {
                Some("sh".into())
            },
        },
        custody,
    })
}

#[cfg(test)]
mod tests;

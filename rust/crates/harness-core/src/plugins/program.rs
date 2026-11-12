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
#[derive(Clone, Debug)]
pub struct LoadedProgram {
    /// The same declaration consumed by the stock tool registry.
    pub definition: ToolDefinition,
    /// The same command consumed by the stock hook runner.
    pub run: HookRun,
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
    })
}

#[cfg(test)]
mod tests;

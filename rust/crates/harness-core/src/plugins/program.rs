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
    definition: ToolDefinition,
    /// The same command consumed by the stock hook runner.
    run: HookRun,
    /// The admitted source selector for a hook; absent for a tool.
    on: Option<config::ProgramTrigger>,
    // Retained manifest and entry bytes retire before their original credit.
    custody: acyclic_fs::NativeCustody,
}

/// A selected hook and its canonical executable binding. Source selection
/// grants no authority; dispatch uses the binding's ordinary shell admission,
/// approvals, execution owner and receipts. No command bytes escape their
/// original allocation owner.
pub struct BoundHook {
    selector: HookSelector,
    binding: crate::tool::Tool,
}

struct HookSelector {
    value: config::ProgramTrigger,
    _custody: acyclic_fs::NativeCustody,
}

impl BoundHook {
    /// The immutable source selector from the selected program revision.
    pub fn on(&self) -> &config::ProgramTrigger {
        &self.selector.value
    }

    /// The existing tool binding, for ordinary canonical call admission.
    pub fn binding(&self) -> &crate::tool::Tool {
        &self.binding
    }
}

impl LoadedProgram {
    /// Borrows the selected declaration without separating its allocation owner.
    pub fn definition(&self) -> &ToolDefinition {
        &self.definition
    }

    /// Binds a hook through the same canonical shell adapter as authored tools.
    /// Retains the selected source and its original native credit independently
    /// of the executable's transferred ownership, including on failed binding.
    ///
    /// # Errors
    /// Refuses a tool manifest, an invalid shell binding, or an exhausted
    /// original preparation allowance.
    pub fn bind_hook(mut self, shell: std::sync::Arc<crate::tool::Tool>) -> Result<BoundHook> {
        let value = self
            .on
            .take()
            .ok_or_else(|| Error::Invalid("tool programs have no hook source".into()))?;
        let mut selector = HookSelector {
            value,
            _custody: None,
        };
        let bytes = match &selector.value {
            config::ProgramTrigger::Lifecycle { .. } => 0,
            config::ProgramTrigger::File { volume, prefix, .. } => {
                crate::core::owned_sum([volume.capacity() as u64, prefix.capacity() as u64])?
            }
            config::ProgramTrigger::Stream { path, .. } => path.capacity() as u64,
        };
        selector._custody = self
            .custody
            .as_mut()
            .map(|custody| custody.split_off(bytes))
            .transpose()?;
        let binding = self.bind(shell)?;
        Ok(BoundHook { selector, binding })
    }

    /// Binds an authored tool to the host's ordinary shell capability. Every
    /// invocation is canonicalized before permission and approval checks, so
    /// authorship cannot widen the execution grant. Arguments travel as JSON
    /// stdin, separately from executable argv and the pinned script source.
    ///
    /// # Errors
    /// Refuses a non-shell capability or an unsupported command form.
    pub fn bind(self, shell: std::sync::Arc<crate::tool::Tool>) -> Result<crate::tool::Tool> {
        if self.on.is_some() {
            return Err(Error::Invalid(
                "hook programs cannot be bound as authored tools".into(),
            ));
        }
        let mut custody = self.custody;
        let shell = match shell.executor.capability() {
            Some(capability) => std::sync::Arc::new(capability.clone()),
            None => shell,
        };
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
        let original_bytes = command.capacity() as u64;
        let rendered = acyclic_host_platform::shell::source_argument(
            selected
                .as_deref()
                .unwrap_or(crate::tools::shell::PLATFORM.language),
            &command,
            |bytes| {
                if let Some(custody) = &mut custody {
                    custody.grow(bytes as u64)?;
                    custody.work().spent(acyclic_fs::WorkCounters {
                        bytes_copied: bytes as u64,
                        ..acyclic_fs::WorkCounters::UNCHARGED
                    })?;
                }
                Ok::<_, Error>(())
            },
        )?
        .ok_or_else(|| Error::Unsupported("program source cannot be represented".into()))?;
        let command = match rendered {
            std::borrow::Cow::Borrowed(_) => command,
            std::borrow::Cow::Owned(rendered) => {
                drop(command);
                if let Some(custody) = &mut custody {
                    custody.release(original_bytes)?;
                }
                rendered
            }
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
        crate::tools::native::alias(self.definition, shell, template, custody)
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
    let lowering = tree
        .preparation_io()
        .and_then(crate::filesystem::PreparationIo::original_native_work)
        .map(crate::filesystem::PreparationIo::from_native_work)
        .transpose()?;
    let io = lowering.as_ref();
    crate::executor::with_history_io(tree.preparation_io(), |work| {
        crate::conversation::validate_content_path(directory, work)
    })?;
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
    let path = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(format_args!("{kind}/{name}/program.ts"), work)
    })?;
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
    let mut manifest = evaluated.value.record::<ProgramManifest>()?;
    let mut retained = crate::javascript_allocator::EngineResult {
        value: (),
        custody: None,
    };
    if manifest.value.version != 1 || manifest.value.name != name {
        return Err(Error::Invalid(format!(
            "{path}: unsupported version or mismatched name"
        )));
    }
    match (kind, &manifest.on) {
        ("tools", None) => {}
        ("hooks", Some(selector)) => selector.validate()?,
        _ => {
            return Err(Error::Invalid(format!(
                "{path}: tools omit on; hooks require on"
            )));
        }
    }
    let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.program.v1");
    identity.update(&evaluated.digest);
    let mut native = None;
    for (form, entry) in [
        ("sh", &manifest.value.entry.sh),
        ("ps1", &manifest.value.entry.ps1),
    ] {
        identity.update(form.as_bytes());
        let Some(entry) = entry else {
            identity.update(&[0]);
            continue;
        };
        crate::executor::with_history_io(tree.preparation_io(), |work| {
            crate::conversation::validate_content_path(entry, work)
        })?;
        if !entry.ends_with(if form == "sh" { ".sh" } else { ".ps1" }) {
            return Err(Error::Invalid(format!(
                "{path}: {form} entry has the wrong extension"
            )));
        }
        let script_path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{directory}/{entry}"), work)
        })?;
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
            native = Some(crate::executor::stock_string_copy(script, io)?);
        }
        crate::executor::stock_retire_text(script_path, io)?;
    }
    let command = native
        .ok_or_else(|| Error::Unsupported(format!("{path}: no native entry for this host")))?;
    let selected_bytes = crate::core::owned_sum([
        manifest.value.name.capacity() as u64,
        manifest.value.description.capacity() as u64,
        crate::executor::json::json_value_retained_owned_bytes(&manifest.value.input_schema)?,
    ])?;
    let revision = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(format_args!("{}", identity.finalize().to_hex()), work)
    })?;
    crate::javascript_allocator::transfer(&mut manifest.custody, selected_bytes, &mut retained)?;
    let definition = ToolDefinition {
        name: std::mem::take(&mut manifest.value.name),
        revision,
        description: std::mem::take(&mut manifest.value.description),
        input_schema: std::mem::take(&mut manifest.value.input_schema),
        output_schema: serde_json::json!({}),
        projection_schema: serde_json::json!({}),
    };
    crate::executor::stock_retire_text(path, io)?;
    crate::executor::with_history_io(tree.preparation_io(), |work| definition.validate(work))?;
    let selected_shell = if cfg!(windows) {
        None
    } else {
        Some(crate::executor::stock_string_copy("sh", io)?)
    };
    if let Some(lowering) = lowering {
        let bytes = lowering.owned_bytes()?;
        retained.absorb(Some(lowering.into_native_custody(bytes)?))?;
    }
    Ok(LoadedProgram {
        definition,
        on: manifest.on,
        run: HookRun::Shell {
            command,
            shell: selected_shell,
        },
        custody: retained.custody,
    })
}

#[cfg(test)]
mod tests;

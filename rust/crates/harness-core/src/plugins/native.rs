use super::after::{self, AfterHook};
use crate::executor::{stock_retire_text, stock_string_copy, stock_vec_push};
use crate::filesystem::UploadRecord as _;
use crate::{
    Error, Result,
    config::{self, Surface, Tree},
    hooks::{HookEvent, HookFailure, HookRun, HookSpec, Hooks},
    layers::Layer,
};
use serde_json::Value;

fn text(
    args: std::fmt::Arguments<'_>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<String> {
    crate::executor::with_history_io(io, |work| crate::executor::preparation_format(args, work))
}

fn settings_text(value: &Value, io: Option<&crate::filesystem::PreparationIo>) -> Result<String> {
    let bytes = match io {
        Some(io) => io.encode_vec(value, None, true)?,
        None => crate::executor::json::canonical_json_bytes_bounded(
            value,
            crate::executor::json::JsonBounds {
                maximum_bytes: None,
                maximum_items: None,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            None,
            &mut |_| Ok(()),
        )?,
    };
    String::from_utf8(bytes).map_err(|_| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "encoded plugin settings are not UTF-8",
    })
}

/// The one manifest: `volume:///home/plugins.ts`.
pub const MANIFEST: &str = "plugins.ts";

pub use crate::config::{
    AfterSource, Event, PluginEntry, PluginManifest, PluginSource, Plugins, RequestMatch, Start,
};

impl Event {
    /// The runner site judging this event, where this host has one.
    const fn site(self) -> Option<HookEvent> {
        match self {
            Self::Tool => Some(HookEvent::PreToolUse),
            Self::Model | Self::Spawn | Self::Fork | Self::Merge | Self::Commit => None,
        }
    }
}

/// The enabled plugin set, lowered to hook rules.
#[derive(Debug)]
pub struct Enabled(crate::javascript_allocator::EngineResult<EnabledData>);

impl std::ops::Deref for Enabled {
    type Target = EnabledData;
    fn deref(&self) -> &Self::Target {
        &self.0.value
    }
}

impl Default for Enabled {
    fn default() -> Self {
        Self(crate::javascript_allocator::EngineResult {
            value: EnabledData::default(),
            custody: None,
        })
    }
}

/// Immutable admitted plugin data; its owner retains the original allocation credit.
#[derive(Debug, Default)]
pub struct EnabledData {
    /// Enabled plugin names, sorted.
    pub names: Vec<String>,
    /// Before-hook rules, in authority order.
    pub rules: Hooks,
    /// Minimal model-visible descriptions of the admitted plugin set.
    pub descriptions: Vec<String>,
    /// Pure after-hooks bound to the original admitted module cut.
    pub after: Vec<AfterHook>,
    custody: Option<std::sync::Arc<acyclic_fs::NativeAllocation>>,
}

/// Loads the plugins `plugins.ts` enables over `layers` (lowest first), as
/// enabled by `enabler`. Plugin folders and their imports resolve in `tree`.
///
/// # Errors
/// [`Error::Invalid`] when `plugins.ts` or an enabled plugin is invalid: an
/// unknown or disabled requirement, a manifest naming another folder, a
/// hook file outside the hook grammar, or a hook this host cannot run.
pub async fn load(tree: &dyn Tree, layers: &[Layer<'_>], enabler: &str) -> Result<Enabled> {
    load_result(tree, layers, enabler).await?
}

/// Load without retaining input evidence. Operational tree failures remain
/// outer errors, even when their variant is [`Error::Invalid`]. The inner
/// result is the semantic decision made from the original inputs.
pub async fn load_result(
    tree: &dyn Tree,
    layers: &[Layer<'_>],
    enabler: &str,
) -> Result<Result<Enabled>> {
    let (value, _) =
        match config::compose_result(MANIFEST, layers, tree, Value::Object(Default::default()))
            .await?
        {
            Ok(composed) => composed,
            Err(error) => return Ok(Err(error)),
        };
    match load_enabled(tree, value, enabler, None).await {
        Ok(enabled) => Ok(Ok(enabled)),
        Err(LoadFailure::Semantic(error)) => Ok(Err(error)),
        Err(LoadFailure::Operational(error)) => Err(error),
    }
}

/// One plugin selection and the original inputs that admitted or refused it.
/// This evidence grants no caller, hook-execution or publication authority.
#[derive(Debug)]
pub struct Decision {
    identity: [u8; 32],
    result: Result<Enabled>,
}

impl Decision {
    /// Identity of the loader, enabler, layered selection, manifest dependency
    /// cuts and original hook observations, including absent or invalid files.
    #[must_use]
    pub const fn identity(&self) -> [u8; 32] {
        self.identity
    }

    /// Return the original semantic decision after retaining its input identity.
    pub fn into_result(self) -> Result<Enabled> {
        self.result
    }
}

/// Load through the same path as [`load`], retaining the original input cut
/// even on semantic refusal. Operational tree failures remain outer errors;
/// they can never become stable semantic decisions. No file is read again
/// to collect this evidence. The caller retains its original pinned tree.
pub async fn load_decision(
    tree: &dyn Tree,
    layers: &[Layer<'_>],
    enabler: &str,
) -> Result<Decision> {
    let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.plugins.v1");
    identity.update(&(enabler.len() as u64).to_be_bytes());
    identity.update(enabler.as_bytes());
    let composition =
        config::compose_with_evidence(MANIFEST, layers, tree, Value::Object(Default::default()))
            .await?;
    identity.update(&composition.identity());
    let result = match composition.into_result() {
        Ok((value, _)) => match load_enabled(tree, value, enabler, Some(&mut identity)).await {
            Ok(enabled) => Ok(enabled),
            Err(LoadFailure::Semantic(error)) => Err(error),
            Err(LoadFailure::Operational(error)) => return Err(error),
        },
        Err(error) => Err(error),
    };
    Ok(Decision {
        identity: identity.finalize().into(),
        result,
    })
}

// Classify by the original producer, never by an operational error's variant.
// A Tree is allowed to refuse a read with Error::Invalid too.
enum LoadFailure {
    Operational(Error),
    Semantic(Error),
}

impl LoadFailure {
    // Only failures produced by the closed record decoder are classified here;
    // source reads retain their original operational ownership.
    fn decoding(error: Error) -> Self {
        match error {
            error @ (Error::Invalid(_)
            | Error::NativeJsonAdmission { .. }
            | Error::NativeJson { .. }) => Self::Semantic(error),
            error => Self::Operational(error),
        }
    }
}
impl From<Error> for LoadFailure {
    fn from(error: Error) -> Self {
        Self::Semantic(error)
    }
}

fn observe(identity: Option<&mut blake3::Hasher>, path: &str, bytes: Option<&[u8]>) {
    let Some(identity) = identity else { return };
    identity.update(&[0]);
    identity.update(&(path.len() as u64).to_be_bytes());
    identity.update(path.as_bytes());
    identity.update(&[u8::from(bytes.is_some())]);
    if let Some(bytes) = bytes {
        identity.update(blake3::hash(bytes).as_bytes());
    }
}

async fn read_hook(
    tree: &dyn Tree,
    identity: Option<&mut blake3::Hasher>,
    hook: &str,
    io: Option<&crate::filesystem::PreparationIo>,
) -> std::result::Result<Option<String>, LoadFailure> {
    let bytes = tree.read(&hook).await.map_err(LoadFailure::Operational)?;
    observe(identity, &hook, bytes.as_deref());
    bytes
        .map(|bytes| {
            let script = std::str::from_utf8(&bytes).map_err(|_| {
                LoadFailure::Semantic(Error::Invalid(format!("{MANIFEST}: {hook} is not UTF-8")))
            })?;
            stock_string_copy(script, io).map_err(LoadFailure::Operational)
        })
        .transpose()
}

async fn load_enabled(
    tree: &dyn Tree,
    value: config::ConfigValue,
    enabler: &str,
    mut identity: Option<&mut blake3::Hasher>,
) -> std::result::Result<Enabled, LoadFailure> {
    let invalid = |reason: String| Error::Invalid(format!("{MANIFEST}: {reason}"));
    let lowering = value
        .work()
        .map(crate::filesystem::PreparationIo::from_native_work)
        .transpose()
        .map_err(LoadFailure::Operational)?;
    let io = lowering.as_ref();
    let selection = value.record::<Plugins>().map_err(LoadFailure::decoding)?;
    let entries = &selection.value.0;
    let mut loaded = crate::javascript_allocator::EngineResult {
        value: EnabledData::default(),
        custody: None,
    };
    loaded
        .absorb(
            value
                .work()
                .map(|work| {
                    work.claim(
                        std::mem::size_of::<EnabledData>() as u64,
                        1,
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )
                })
                .transpose()
                .map_err(|cause| LoadFailure::Operational(cause.into()))?,
        )
        .map_err(LoadFailure::Operational)?;
    for (name, entry) in entries.iter().filter(|(_, entry)| entry.enabled) {
        if name.is_empty() || name.contains(['/', '\\']) || name.starts_with('.') {
            return Err(invalid(format!("{name:?} is not a plugin folder name")).into());
        }
        if entry.source.is_some() {
            return Err(invalid(format!(
                "{name}: plugins from another folder are not supported on this host yet"
            ))
            .into());
        }
        let root = text(format_args!("plugins/{name}/"), io).map_err(LoadFailure::Operational)?;
        let path = text(format_args!("{root}plugin.ts"), io).map_err(LoadFailure::Operational)?;
        let source = tree.read(&path).await.map_err(LoadFailure::Operational)?;
        let Some(source) = source else {
            observe(identity.as_deref_mut(), &path, None);
            return Err(invalid(format!("{name} is enabled but {path} does not exist")).into());
        };
        let manifest = config::evaluate_decision(&path, &source, tree, &Value::Null, Surface::Data)
            .await
            .map_err(LoadFailure::Operational)?;
        if let Some(identity) = identity.as_deref_mut() {
            identity.update(&[1]);
            identity.update(&manifest.identity());
        }
        let manifest = manifest.into_result()?;
        let mut manifest = manifest
            .value
            .record::<PluginManifest>()
            .map_err(LoadFailure::decoding)?;
        if manifest.value.name != *name {
            return Err(invalid(format!("{path} names {:?}", manifest.value.name)).into());
        }
        if let Some(missing) = manifest
            .value
            .requires
            .iter()
            .find(|required| !entries.get(*required).is_some_and(|entry| entry.enabled))
        {
            return Err(invalid(format!("{name} requires {missing}, which is not enabled")).into());
        }
        let empty = Value::Object(Default::default());
        let settings = entry
            .config
            .as_ref()
            .or(manifest.value.defaults.as_ref())
            .unwrap_or(&empty);
        for (name, source) in &manifest.value.after {
            if name.is_empty() || name.contains(['/', '\\']) || name.starts_with('.') {
                return Err(invalid("after-hooks require one canonical hook name".into()).into());
            }
            if source.path != "live/agents/self/status.json" || source.from != Start::Now {
                return Err(invalid(format!("{root}hooks/after-{name}: after-hooks require a supported original publication source")).into());
            }
            let hook = text(format_args!("{root}hooks/after-{name}.ts"), io)
                .map_err(LoadFailure::Operational)?;
            let code = read_hook(tree, identity.as_deref_mut(), &hook, io)
                .await?
                .ok_or_else(|| invalid(format!("after-hooks module {hook} does not exist")))?;
            let prepared = config::prepare(&hook, code.as_bytes(), tree)
                .await
                .map_err(LoadFailure::Operational)?;
            stock_retire_text(code, io).map_err(LoadFailure::Operational)?;
            stock_retire_text(hook, io).map_err(LoadFailure::Operational)?;
            let evaluation =
                after::evaluate(&prepared, settings).map_err(LoadFailure::Operational)?;
            if let Some(identity) = identity.as_deref_mut() {
                identity.update(&evaluation.identity());
            }
            let hook_identity = evaluation.identity();
            let evaluated = evaluation.into_result().map_err(LoadFailure::Semantic)?;
            stock_vec_push(
                &mut loaded.value.after,
                AfterHook::new(evaluated, hook_identity).map_err(LoadFailure::decoding)?,
                io,
            )
            .map_err(LoadFailure::Operational)?;
        }
        // Reparent only the payload that leaves the decoded manifest. Its
        // remaining fields and admission retire with the original record.
        let description_bytes = manifest.value.description.capacity() as u64;
        crate::javascript_allocator::transfer(
            &mut manifest.custody,
            description_bytes,
            &mut loaded,
        )
        .map_err(LoadFailure::Operational)?;
        let description = std::mem::take(&mut manifest.value.description);
        stock_vec_push(&mut loaded.value.descriptions, description, io)
            .map_err(LoadFailure::Operational)?;
        let encoded_settings = once_cell::unsync::OnceCell::new();
        for event in Event::ALL {
            let path = |form: &str| {
                text(
                    format_args!("{root}hooks/before-{}.{form}", event.name()),
                    io,
                )
                .map_err(LoadFailure::Operational)
            };
            let paths = [path("ts")?, path("sh")?, path("ps1")?];
            let (ts, sh, ps1) = (
                read_hook(tree, identity.as_deref_mut(), &paths[0], io).await?,
                read_hook(tree, identity.as_deref_mut(), &paths[1], io).await?,
                read_hook(tree, identity.as_deref_mut(), &paths[2], io).await?,
            );
            if ts.is_some() {
                return Err(invalid(format!(
                    "{}: TypeScript hooks do not run on this host yet",
                    paths[0]
                ))
                .into());
            }
            for path in paths {
                stock_retire_text(path, io).map_err(LoadFailure::Operational)?;
            }
            if sh.is_none() && ps1.is_none() {
                continue;
            }
            let Some(site) = event.site() else {
                return Err(invalid(format!(
                    "{root}hooks/before-{}: this host does not judge {} requests yet",
                    event.name(),
                    event.name()
                ))
                .into());
            };
            let settings = encoded_settings
                .get_or_try_init(|| settings_text(settings, io))
                .map_err(LoadFailure::Operational)?;
            let matcher_bytes = manifest
                .value
                .matcher
                .get(&event)
                .map_or(Ok(0), |matcher| matcher.logical_owned_bytes())
                .map_err(LoadFailure::Operational)?;
            crate::javascript_allocator::transfer(
                &mut manifest.custody,
                matcher_bytes,
                &mut loaded,
            )
            .map_err(LoadFailure::Operational)?;
            let matcher = manifest.value.matcher.remove(&event);
            let hook = HookSpec {
                run: HookRun::Plugin {
                    instance: text(
                        format_args!("{enabler}:{MANIFEST}#{name}:hooks/before-{}", event.name()),
                        io,
                    )
                    .map_err(LoadFailure::Operational)?,
                    sh,
                    ps1,
                    config: stock_string_copy(&settings, io).map_err(LoadFailure::Operational)?,
                    matcher,
                },
                // A hook holds only its own request; it has no deadline.
                timeout_ms: None,
                failure: HookFailure::Block,
            };
            loaded
                .value
                .rules
                .add_rule_with_io(site, String::new(), std::iter::once(hook), io)
                .map_err(|error| match error {
                    Error::Invalid(reason) => LoadFailure::Semantic(invalid(format!(
                        "{root}hooks/before-{}: {reason}",
                        event.name()
                    ))),
                    error => LoadFailure::decoding(error),
                })?;
        }
        if let Some(settings) = encoded_settings.into_inner() {
            stock_retire_text(settings, io).map_err(LoadFailure::Operational)?;
        }
        stock_retire_text(path, io).map_err(LoadFailure::Operational)?;
        stock_retire_text(root, io).map_err(LoadFailure::Operational)?;
        let name = stock_string_copy(name, io).map_err(LoadFailure::Operational)?;
        stock_vec_push(&mut loaded.value.names, name, io).map_err(LoadFailure::Operational)?;
    }
    if let Some(lowering) = lowering {
        let retained = lowering.owned_bytes().map_err(LoadFailure::Operational)?;
        loaded
            .absorb(Some(
                lowering
                    .into_native_custody(retained)
                    .map_err(LoadFailure::Operational)?,
            ))
            .map_err(LoadFailure::Operational)?;
    }
    if let Some(mut custody) = loaded.custody.take() {
        let work = custody.work();
        let bytes = crate::core::owned_sum([
            acyclic_fs::NativeMaterializationWork::shared_allocation_bytes::<
                acyclic_fs::NativeAllocation,
            >()
            .map_err(|cause| LoadFailure::Operational(cause.into()))?,
            std::mem::size_of::<std::sync::Arc<acyclic_fs::NativeAllocation>>() as u64,
        ])?;
        let mut shared = work
            .claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(|cause| LoadFailure::Operational(cause.into()))?;
        custody
            .absorb(&mut shared)
            .map_err(|cause| LoadFailure::Operational(cause.into()))?;
        let custody = std::sync::Arc::new(custody);
        loaded
            .value
            .rules
            .retain_native(std::sync::Arc::clone(&custody));
        loaded.value.custody = Some(custody);
    }
    Ok(Enabled(loaded))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

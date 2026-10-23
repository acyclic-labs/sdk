use super::{TestResult, execution, matchers};
use acyclic_harness::{
    OperationId,
    client::{CommandCatalog, CommandFileRoot, CommandResolution, CommandRoot, CommandScope},
    context::{ContextDiscoveryLimits, ContextRoot, PinnedContextPath},
    filesystem::MemoryHarnessStorage,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Composer {
    #[serde(default)]
    seed: BTreeMap<String, String>,
    prompt: String,
    #[serde(default)]
    commands: Vec<Root>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default)]
    themes: Vec<String>,
    catalog: Option<String>,
    #[serde(default)]
    publish_files: bool,
    #[serde(default)]
    workspace: String,
    #[serde(default)]
    after_capture: BTreeMap<String, String>,
    #[serde(default = "maximum_bytes")]
    maximum_bytes: u64,
    #[serde(default)]
    discovery_limits: ContextDiscoveryLimits,
    complete: Option<String>,
    expect: Expectations,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    scope: CommandScope,
    #[serde(default)]
    directory: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    result: Option<Value>,
    error: Option<String>,
    completions: Option<Vec<String>>,
    mention_files: Option<Vec<String>>,
    #[serde(default)]
    files: BTreeMap<String, Option<String>>,
    #[serde(default)]
    file_values: BTreeMap<String, Value>,
}

fn maximum_bytes() -> u64 {
    32_768
}

impl Composer {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        if case.expect.result.is_some() == case.expect.error.is_some() {
            return Err("expect exactly one of result or error".into());
        }
        if case.expect.error.as_ref().is_some_and(String::is_empty) {
            return Err("error expectation must not be empty".into());
        }
        if case
            .expect
            .result
            .as_ref()
            .is_some_and(|value| value.as_object().is_none_or(|object| object.is_empty()))
        {
            return Err("composer result must be a nonempty JSON object".into());
        }
        if case.expect.completions.is_some() != case.complete.is_some() {
            return Err("completion expectations require a prefix".into());
        }
        if (case.expect.mention_files.is_some() || case.expect.completions.is_some())
            && case.expect.error.is_some()
        {
            return Err("mention expectations require a successful resolution".into());
        }
        Ok(case)
    }

    pub(super) async fn run(self) -> TestResult {
        let storage = execution::workspace(&BTreeMap::new()).await?;
        let observed = Box::pin(self.capture(&storage)).await?;
        self.check(&observed)
    }

    pub(super) fn paths(&self) -> Vec<String> {
        self.expect
            .files
            .keys()
            .chain(self.expect.file_values.keys())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(super) fn check(&self, observed: &Capture) -> TestResult {
        match (&observed.result, &self.expect.error) {
            (Err(error), Some(expected)) if error.contains(expected) => {}
            (Ok(actual), None) => {
                if !self
                    .expect
                    .result
                    .as_ref()
                    .is_some_and(|expected| matchers::subset(expected, actual))
                {
                    return Err(format!(
                        "composer result: expected {:?}, got {actual}",
                        self.expect.result
                    )
                    .into());
                }
                if self
                    .expect
                    .mention_files
                    .as_ref()
                    .is_some_and(|expected| observed.mention_files.as_ref() != Some(expected))
                {
                    return Err("mention files differ or resolution is not a prompt".into());
                }
            }
            (actual, expected) => {
                return Err(
                    format!("composer error: expected {expected:?}, got {actual:?}").into(),
                );
            }
        }
        if self
            .expect
            .completions
            .as_ref()
            .is_some_and(|expected| observed.completions.as_ref() != Some(expected))
        {
            return Err(format!(
                "completions: expected {:?}, got {:?}",
                self.expect.completions, observed.completions
            )
            .into());
        }
        for (path, expected) in &self.expect.files {
            if observed.files.get(path) != Some(&Ok(expected.clone())) {
                return Err(format!(
                    "file {path}: expected {expected:?}, got {:?}",
                    observed.files.get(path)
                )
                .into());
            }
        }
        for (path, expected) in &self.expect.file_values {
            let text = observed
                .files
                .get(path)
                .ok_or("missing file observation")?
                .as_ref()
                .map_err(|e| e.to_owned())?
                .as_ref()
                .ok_or("missing JSON file")?;
            let actual: Value = serde_json::from_str(text)?;
            if &actual != expected {
                return Err(format!("file {path}: expected {expected}, got {actual}").into());
            }
        }
        Ok(())
    }

    pub(super) async fn capture<P, A, O>(
        &self,
        storage: &MemoryHarnessStorage<P, A, O>,
    ) -> TestResult<Capture>
    where
        P: acyclic_stream::StreamProvider,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        execution::publish(storage, &self.seed).await?;
        let (result, completions) = Box::pin(self.resolve(storage)).await?;
        if self.publish_files
            && let Ok(resolution) = &result
        {
            publish_resolution(resolution, storage).await?;
        }
        let mention_files = if let Ok(CommandResolution::Prompt { mentions, .. }) = &result {
            let mut files = Vec::new();
            for mention in mentions {
                files.push(String::from_utf8(
                    storage.read(&mention.file).await?.to_vec(),
                )?);
            }
            Some(files)
        } else {
            None
        };
        let mut files = BTreeMap::new();
        for path in self.paths() {
            let content = match storage
                .read_delegated_path(storage.owner_scope(), "", &path, None)
                .await
            {
                Ok((_, bytes)) => String::from_utf8(bytes.to_vec())
                    .map(Some)
                    .map_err(|e| e.to_string()),
                Err(acyclic_harness::Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error.to_string()),
            };
            files.insert(path, content);
        }
        let result = match result {
            Ok(value) => Ok(serde_json::to_value(value)?),
            Err(error) => Err(error.to_string()),
        };
        Ok(Capture {
            result,
            completions,
            mention_files,
            files,
        })
    }

    async fn resolve<P, A, O>(
        &self,
        storage: &MemoryHarnessStorage<P, A, O>,
    ) -> TestResult<(
        acyclic_harness::Result<CommandResolution>,
        Option<Vec<String>>,
    )>
    where
        P: acyclic_stream::StreamProvider,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        let reader = storage.content_reader();
        let root = |directory| ContextRoot {
            volume: storage.volume().clone(),
            directory,
        };
        let commands: Vec<_> = self
            .commands
            .iter()
            .map(|declared| CommandRoot {
                scope: declared.scope,
                root: root(declared.directory.clone()),
            })
            .collect();
        let skills: Vec<_> = self.skills.iter().cloned().map(root).collect();
        let captured = if let Some(path) = &self.catalog {
            let (generation, _) = storage.list_private_directory("", None, None, 1).await?;
            let source = PinnedContextPath {
                root: root(String::new()),
                generation,
                path: path.clone(),
            };
            match CommandCatalog::from_file(&source, reader, self.maximum_bytes).await {
                Ok(catalog) => {
                    catalog
                        .with_sources(reader, &commands, &skills, self.discovery_limits)
                        .await
                }
                Err(error) => Err(error),
            }
        } else {
            CommandCatalog::capture(reader, &commands, &skills, self.discovery_limits).await
        };
        let catalog = match captured {
            Ok(catalog) => catalog.with_themes(self.themes.clone())?,
            Err(error) => return Ok((Err(error), None)),
        };
        let completions = self.complete.as_ref().map(|prefix| {
            catalog
                .complete(prefix)
                .map(|spec| spec.name.clone())
                .collect()
        });
        let (generation, _) = storage.list_private_directory("", None, None, 1).await?;
        execution::publish(storage, &self.after_capture).await?;
        let result = catalog
            .resolve_input(
                &self.prompt,
                &root(self.workspace.clone()),
                &generation,
                reader,
                self.maximum_bytes,
            )
            .await;
        Ok((result, completions))
    }
}

/// Raw composer outputs, without expected answers or a host-side verdict.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    /// Serialized command resolution or its diagnostic.
    pub result: Result<Value, String>,
    /// Completion names from the captured catalog.
    pub completions: Option<Vec<String>>,
    /// Loaded mention attachments for a prompt resolution.
    pub mention_files: Option<Vec<String>>,
    /// Requested path contents, absence, or read errors.
    pub files: BTreeMap<String, Result<Option<String>, String>>,
}

async fn publish_resolution<P, A, O>(
    resolution: &CommandResolution,
    storage: &MemoryHarnessStorage<P, A, O>,
) -> TestResult
where
    P: acyclic_stream::StreamProvider,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let mut snapshot = BTreeMap::new();
    for (binding, prefix) in [
        (CommandFileRoot::Config, "config"),
        (CommandFileRoot::Session, "session"),
        (CommandFileRoot::Task, "task"),
    ] {
        let mut files = BTreeMap::new();
        for name in [
            "settings.json",
            "layout.json",
            "theme.json",
            "attention.json",
        ] {
            match storage
                .read_delegated_path(storage.owner_scope(), "", &format!("{prefix}/{name}"), None)
                .await
            {
                Ok((_, bytes)) => {
                    files.insert(name.into(), serde_json::from_slice(&bytes)?);
                }
                Err(acyclic_harness::Error::NotFound(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        snapshot.insert(binding, files);
    }
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000001")?;
    for change in resolution.file_changes(&snapshot, operation)? {
        let prefix = match change.root {
            CommandFileRoot::Config => "config",
            CommandFileRoot::Session => "session",
            CommandFileRoot::Task => "task",
        };
        let path = format!("{prefix}/{}", change.path);
        if change.create_only
            && !matches!(
                storage
                    .read_delegated_path(storage.owner_scope(), "", &path, None)
                    .await,
                Err(acyclic_harness::Error::NotFound(_))
            )
        {
            return Err("request file must be a new immutable creation".into());
        }
        storage
            .stage(
                change.operation,
                &path,
                &serde_json::to_vec(&change.contents)?,
                "application/json",
                path.rsplit('/').next().unwrap_or_default(),
            )
            .await?;
    }
    Ok(())
}

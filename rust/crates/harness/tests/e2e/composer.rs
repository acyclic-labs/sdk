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
        let storage = execution::workspace(&self.seed).await?;
        let reader = storage.content_reader();
        let root = |directory| ContextRoot {
            volume: storage.volume().clone(),
            directory,
        };
        let commands: Vec<_> = self
            .commands
            .into_iter()
            .map(|declared| CommandRoot {
                scope: declared.scope,
                root: root(declared.directory),
            })
            .collect();
        let skills: Vec<_> = self.skills.into_iter().map(root).collect();
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
        let observed = match captured {
            Err(error) => Err(error),
            Ok(catalog) => {
                let catalog = catalog.with_themes(self.themes)?;
                if let Some(prefix) = &self.complete {
                    let names = || catalog.complete(prefix).map(|spec| spec.name.as_str());
                    if !self
                        .expect
                        .completions
                        .as_ref()
                        .is_some_and(|expected| expected.iter().map(String::as_str).eq(names()))
                    {
                        return Err(format!(
                            "completions: expected {:?}, got {:?}",
                            self.expect.completions,
                            names().collect::<Vec<_>>()
                        )
                        .into());
                    }
                }
                let (generation, _) = storage.list_private_directory("", None, None, 1).await?;
                execution::publish(&storage, &self.after_capture).await?;
                catalog
                    .resolve_input(
                        &self.prompt,
                        &root(self.workspace),
                        &generation,
                        reader,
                        self.maximum_bytes,
                    )
                    .await
            }
        };
        if self.publish_files {
            if let Ok(resolution) = &observed {
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
                            .read_delegated_path(
                                storage.owner_scope(),
                                "",
                                &format!("{prefix}/{name}"),
                                None,
                            )
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
                    if change.create_only {
                        match storage
                            .read_delegated_path(storage.owner_scope(), "", &path, None)
                            .await
                        {
                            Err(acyclic_harness::Error::NotFound(_)) => {}
                            _ => return Err("request file must be a new immutable creation".into()),
                        }
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
            }
        }
        check_resolution(&self.expect, observed, &storage).await?;
        for (path, expected) in &self.expect.file_values {
            let (_, bytes) = storage
                .read_delegated_path(storage.owner_scope(), "", path, None)
                .await?;
            let actual: Value = serde_json::from_slice(&bytes)?;
            if actual != *expected {
                return Err(format!("file {path}: expected {expected}, got {actual}").into());
            }
        }
        execution::check_files(&self.expect.files, &storage).await
    }
}

async fn check_resolution(
    expected: &Expectations,
    observed: acyclic_harness::Result<CommandResolution>,
    storage: &MemoryHarnessStorage,
) -> TestResult {
    match (observed, expected.error.as_ref()) {
        (Err(error), Some(expected)) if error.to_string().contains(expected) => {}
        (Ok(actual), None) => {
            if let Some(expected) = &expected.mention_files {
                let CommandResolution::Prompt { mentions, .. } = &actual else {
                    return Err("mention assertions require a prompt resolution".into());
                };
                if expected.len() != mentions.len() {
                    return Err(format!(
                        "mention files: expected {} files, got {}",
                        expected.len(),
                        mentions.len()
                    )
                    .into());
                }
                for (expected, mention) in expected.iter().zip(mentions) {
                    let bytes = storage.read(&mention.file).await?;
                    if expected.as_bytes() != bytes.as_ref() {
                        return Err(format!(
                            "mention file {}: expected {expected:?}, got {bytes:?}",
                            mention.file.path()
                        )
                        .into());
                    }
                }
            }
            let actual = serde_json::to_value(actual)?;
            if !expected
                .result
                .as_ref()
                .is_some_and(|expected| matchers::subset(expected, &actual))
            {
                return Err(format!(
                    "composer result: expected {:?}, got {actual}",
                    expected.result
                )
                .into());
            }
        }
        (actual, expected) => {
            return Err(format!("composer error: expected {expected:?}, got {actual:?}").into());
        }
    }
    Ok(())
}

//! `agent.ts` at session start: the seed's and the home's modules fold over
//! the model the session was started with; the result selects the model and
//! its sampling. Each home version's outcome is recorded as an
//! [`Admission`](crate::config::Admission) in the journal.
use super::*;
use crate::config::{self, Admission, AdmissionStatus, AgentConfig, Sampling};
use crate::conversation::{ContentAccess, ContentGrant};
use crate::layers::{Layer, is_off};

/// The settings in force for a session.
pub(super) struct Settings {
    pub(super) model: (Model, Arc<dyn ModelProvider>),
    pub(super) sampling: Option<Sampling>,
    pub(super) compaction: crate::context::CompactionPolicy,
}

/// Resolves a model selection the settings name.
pub(super) type Resolve<'a> =
    Option<&'a (dyn Fn(&str) -> Result<(Model, Arc<dyn ModelProvider>)> + Sync)>;

/// What a session's settings are read from and recorded in.
pub(super) struct Volumes<'a> {
    pub(super) files: &'a Arc<LocalFilesystem>,
    pub(super) verifier: &'a AuthorityVerifier,
    pub(super) signed: &'a Scope,
    pub(super) home: &'a VolumeRef,
    pub(super) journal: &'a VolumeRef,
    pub(super) maximum_bytes: u64,
}

/// Imports resolve in the home over the seed; `sys/` only in the seed.
struct Tree<'a> {
    volumes: &'a Volumes<'a>,
    read: ContentGrant,
    generation: crate::resources::GenerationRef,
}

impl Tree<'_> {
    async fn home(&self, path: &str) -> Result<Option<(String, acyclic_stream::OwnedBytes)>> {
        self.volumes
            .files
            .read_private_path_indexed_if_present(
                self.volumes.home,
                &self.read,
                path,
                Some(&self.generation),
                self.volumes.maximum_bytes,
                ContentAccess::default(),
            )
            .await
            .map(|file| file.map(|(file, bytes, _)| (file.version().to_owned(), bytes)))
    }
}

impl config::Tree for Tree<'_> {
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            let seed = || crate::seed::file(path).map(acyclic_stream::OwnedBytes::from_static);
            if !crate::layers::layered(path) {
                return Ok(seed());
            }
            Ok(match self.home(path).await? {
                Some((_, bytes)) if is_off(&bytes) => None,
                Some((_, bytes)) => Some(bytes),
                None => seed(),
            })
        })
    }
}

/// The settings `agent.ts` selects over `started`, recording the home
/// module's admission. An invalid or rejected decoded home module leaves the
/// seed's settings in force; source read failures remain terminal.
pub(super) async fn admit(
    volumes: &Volumes<'_>,
    agent: AgentId,
    started: (Model, Arc<dyn ModelProvider>),
    resolve: Resolve<'_>,
    compaction: (
        &crate::context::CompactionPolicy,
        &Option<crate::context::CompactionPolicy>,
    ),
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<Settings> {
    let tree = Tree::new(volumes).await?;
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(config::AGENT),
    };
    let home = tree.home(config::AGENT).await?;
    let Some((version, bytes)) = home else {
        return settle(&[seed], &tree, started, resolve, compaction)
            .await
            .map_err(Refused::seed);
    };
    let layers = [
        seed,
        Layer {
            name: "home",
            generation: &version,
            bytes: Some(&bytes),
        },
    ];
    let (settings, status, error) =
        match settle(&layers, &tree, started.clone(), resolve, compaction).await {
            Ok(settings) => (settings, AdmissionStatus::Applied, None),
            Err(Refused::Operation(error)) => return Err(error),
            Err(Refused::Settings(status, error)) => (
                settle(&layers[..1], &tree, started, resolve, compaction)
                    .await
                    .map_err(Refused::seed)?,
                status,
                Some(error),
            ),
        };
    record(
        volumes,
        agent,
        (config::AGENT, &version),
        (status, error),
        clock,
    )
    .await?;
    Ok(settings)
}

impl<'a> Tree<'a> {
    async fn new(volumes: &'a Volumes<'a>) -> Result<Self> {
        let read = ContentGrant::verify_directory_read(
            volumes.verifier,
            volumes.signed,
            volumes.home,
            "",
            &mut None,
        )?;
        let workspace = crate::filesystem::workspace_ref(
            volumes.home.provider().clone(),
            &volumes.home.storage_name(&mut None)?,
        )?;
        let generation = volumes.files.resolve(&workspace, None).await?.generation;
        Ok(Self {
            volumes,
            read,
            generation,
        })
    }
}

/// Records the outcome of the home's `path` at `version` in the journal,
/// once per version.
async fn record(
    volumes: &Volumes<'_>,
    agent: AgentId,
    (path, version): (&str, &str),
    (status, error): (AdmissionStatus, Option<String>),
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<()> {
    let admission = Admission {
        path: path.to_owned(),
        version: version.to_owned(),
        status,
        error,
        at: config::utc(clock.now_unix_millis()),
    };
    let bytes =
        serde_json::to_vec(&admission).map_err(|error| Error::Invalid(error.to_string()))?;
    let record = format!("live/agents/{agent}/admissions/{path}/{version}.json");
    volumes
        .files
        .put_content(
            volumes.journal,
            &ContentGrant::verify(
                volumes.verifier,
                volumes.signed,
                volumes.journal,
                VolumeOperation::Write,
                u64::MAX,
                &mut None,
            )?,
            &record,
            &bytes,
            "application/json",
            "admission",
            volumes.maximum_bytes,
            &crate::IdempotencyKey::new(format!(
                "admission:{agent}:{path}:{version}:{}",
                blake3::hash(&bytes)
            ))?,
            None,
            ContentAccess::default(),
        )
        .await?;
    Ok(())
}

/// The before-hook rules of the plugins `plugins.ts` enables, read as the
/// session starts, recording the home module's admission. An invalid decoded
/// home `plugins.ts` leaves the seed's set in force; read failures are terminal.
#[cfg(any(test, feature = "hooks"))]
pub(super) async fn plugins(
    volumes: &Volumes<'_>,
    agent: AgentId,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<crate::hooks::Hooks> {
    use crate::plugins::{MANIFEST, load};
    let tree = Tree::new(volumes).await?;
    let enabler = agent.to_string();
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(MANIFEST),
    };
    let seeded = |error: config::AdmissionError| match error {
        config::AdmissionError::Invalid(reason) => {
            Error::Invalid(format!("the seed's plugins are not admissible: {reason}"))
        }
        config::AdmissionError::Source(error) => error,
    };
    let Some((version, bytes)) = tree.home(MANIFEST).await? else {
        return Ok(load(&tree, &[seed], &enabler).await.map_err(seeded)?.rules);
    };
    let layers = [
        seed,
        Layer {
            name: "home",
            generation: &version,
            bytes: Some(&bytes),
        },
    ];
    let (rules, outcome) = match load(&tree, &layers, &enabler).await {
        Ok(enabled) => (enabled.rules, (AdmissionStatus::Applied, None)),
        Err(config::AdmissionError::Invalid(reason)) => (
            load(&tree, &layers[..1], &enabler)
                .await
                .map_err(seeded)?
                .rules,
            (AdmissionStatus::Invalid, Some(reason)),
        ),
        Err(config::AdmissionError::Source(error)) => return Err(error),
    };
    record(volumes, agent, (MANIFEST, &version), outcome, clock).await?;
    Ok(rules)
}

/// Only configuration refusals permit fallback. Operation failures retain their
/// original typed cause and cannot become an admission record.
#[derive(Debug)]
enum Refused {
    Settings(AdmissionStatus, String),
    Operation(Error),
}

impl Refused {
    /// The seed is the floor every home module falls back to; refusing it
    /// is a defect of this build. Read failures keep their original cause.
    fn seed(self) -> Error {
        match self {
            Self::Settings(_, reason) => {
                Error::Invalid(format!("the seed's settings are not admissible: {reason}"))
            }
            Self::Operation(error) => error,
        }
    }

    fn evaluated(error: config::AdmissionError) -> Self {
        match error {
            config::AdmissionError::Invalid(reason) => {
                Self::Settings(AdmissionStatus::Invalid, reason)
            }
            config::AdmissionError::Source(error) => Self::Operation(error),
        }
    }

    fn selection(error: Error) -> Self {
        match error {
            Error::Invalid(_) | Error::Unsupported(_) | Error::NotFound(_) => {
                Self::Settings(AdmissionStatus::Rejected, error.to_string())
            }
            error => Self::Operation(error),
        }
    }
}

impl From<Error> for Refused {
    fn from(error: Error) -> Self {
        Self::Operation(error)
    }
}

async fn settle(
    layers: &[Layer<'_>],
    tree: &dyn config::Tree,
    started: (Model, Arc<dyn ModelProvider>),
    resolve: Resolve<'_>,
    compaction: (
        &crate::context::CompactionPolicy,
        &Option<crate::context::CompactionPolicy>,
    ),
) -> std::result::Result<Settings, Refused> {
    let selection = format!("{}/{}", started.0.provider, started.0.name);
    let invalid = |error: String| Refused::Settings(AdmissionStatus::Invalid, error);
    let (value, _) = config::compose(
        config::AGENT,
        layers,
        tree,
        serde_json::json!({ "model": selection }),
    )
    .await
    .map_err(Refused::evaluated)?;
    let settings: AgentConfig = serde_json::from_value(value)
        .map_err(|error| invalid(format!("{}: {error}", config::AGENT)))?;
    let rejected = |reason: &str| {
        Refused::Settings(
            AdmissionStatus::Rejected,
            format!("{}: {reason}", config::AGENT),
        )
    };
    if settings.effort.is_some() {
        return Err(rejected(
            "effort is not selectable on this host; leave it null",
        ));
    }
    if settings
        .host
        .is_some_and(|host| host != config::Host::Builtin)
    {
        return Err(rejected(
            "another agent host is chosen when the session starts, not here",
        ));
    }
    if settings.replay.is_some() {
        return Err(rejected(
            "replay is chosen when the session starts, not here",
        ));
    }
    if let Some(sampling) = settings.sampling {
        if sampling.max_output_tokens == Some(0) {
            return Err(rejected("sampling.maxOutputTokens must be positive"));
        }
    }
    let unchanged_model = settings.model == selection;
    let model = if unchanged_model {
        started
    } else {
        let resolve = resolve.ok_or_else(|| {
            rejected("this session's model is bound by its host and cannot be changed here")
        })?;
        resolve(&settings.model).map_err(Refused::selection)?
    };
    if let Some(sampling) = settings.sampling {
        crate::model::ModelSampling::admit(
            Some(crate::model::ModelSampling {
                seed: None,
                temperature: sampling.temperature,
                top_p: sampling.top_p,
            }),
            model
                .1
                .model_profile(&model.0)?
                .map(|profile| profile.sampling()),
        )
        .map_err(Refused::selection)?;
    }
    let compaction = if unchanged_model {
        compaction.0.clone()
    } else {
        super::compaction::select(compaction.1.clone(), &model.0, model.1.as_ref())
            .map_err(Refused::selection)?
    };
    if let Some(output) = settings
        .sampling
        .and_then(|sampling| sampling.max_output_tokens)
    {
        let capacity = model.1.context_capacity(&model.0)?;
        match &compaction {
            crate::context::CompactionPolicy::Disabled => {
                capacity
                    .output_reservation(Some(output))
                    .map_err(Refused::selection)?;
            }
            crate::context::CompactionPolicy::Threshold(policy) => {
                policy
                    .validate(capacity, Some(output))
                    .map_err(Refused::selection)?;
            }
        }
    }
    Ok(Settings {
        compaction,
        model,
        sampling: settings.sampling,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::tests::settings::write;
    use crate::local::tests::{text, volume, volume_config};

    #[tokio::test]
    async fn imported_settings_keep_the_original_directory_read_refusal() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let harness = LocalHarness::open(
            volume_config(directory.path(), project, text())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?;
        write(
            &harness,
            "options.ts",
            "export const sampling = {temperature: 0.25};",
        )
        .await?;
        let volumes = Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            maximum_bytes: u64::MAX,
        };
        let layers = [Layer {
            name: "home",
            generation: "original",
            bytes: Some(b"import {sampling} from './options.ts'; export default base => ({...base, sampling});"),
        }];
        let disabled = crate::context::CompactionPolicy::Disabled;
        for prefix in ["", "allowed"] {
            let mut tree = Tree::new(&volumes).await?;
            tree.read = ContentGrant::verify_directory_read(
                &harness.verifier,
                &harness.signed,
                &harness.scratch,
                prefix,
                &mut None,
            )?;
            let result = settle(
                &layers,
                &tree,
                harness.agent()?.clone(),
                None,
                (&disabled, &None),
            )
            .await;
            match (prefix, result) {
                ("", Ok(settings)) => assert_eq!(
                    settings.sampling.and_then(|sampling| sampling.temperature),
                    Some(0.25),
                ),
                ("allowed", Err(Refused::Operation(error))) => {
                    assert_eq!(error.code(), crate::wire::ErrorCode::Unauthorized);
                    // A failed seed read also retains the operation's typed cause.
                    assert_eq!(
                        Refused::Operation(error).seed().code(),
                        crate::wire::ErrorCode::Unauthorized
                    );
                }
                (_, Err(error)) => panic!("{prefix}: {error:?}"),
                (_, Ok(_)) => panic!("{prefix}: imported settings escaped their read boundary"),
            }
        }
        write(&harness, config::AGENT, "export default base => base;").await?;
        let bounded = Volumes {
            maximum_bytes: 0,
            ..volumes
        };
        let refused = admit(
            &bounded,
            harness.root,
            harness.agent()?.clone(),
            None,
            (&disabled, &None),
            harness.clock.as_ref(),
        )
        .await;
        assert!(
            matches!(refused, Err(error) if error.code() == crate::wire::ErrorCode::Invalid),
            "the entry read bound became seed fallback"
        );

        // The entry fits, but its imported source exceeds the same bound.
        // Invalid module code may fall back; this original read refusal may not.
        for (path, source) in [
            ("options.ts", "/*".to_owned() + &"x".repeat(1024) + "*/ export const sampling = {};"),
            (config::AGENT, "import {sampling} from './options.ts'; export default base => ({...base, sampling});".to_owned()),
        ] {
            write(&harness, path, &source).await?;
        }
        let bounded = Volumes {
            maximum_bytes: 128,
            ..bounded
        };
        let tree = Tree::new(&bounded).await?;
        let original = tree
            .home("options.ts")
            .await
            .err()
            .expect("import fits bound");
        let (version, source) = tree.home(config::AGENT).await?.expect("entry is absent");
        let layers = [Layer {
            name: "home",
            generation: &version,
            bytes: Some(&source),
        }];
        assert!(
            matches!(
                settle(&layers, &tree, harness.agent()?.clone(), None, (&disabled, &None)).await,
                Err(Refused::Operation(error)) if error.to_string() == original.to_string()
            ),
            "an imported source refusal was classified as invalid settings"
        );
        let refused = admit(
            &bounded,
            harness.root,
            harness.agent()?.clone(),
            None,
            (&disabled, &None),
            harness.clock.as_ref(),
        )
        .await;
        assert!(
            matches!(refused, Err(error) if error.to_string() == original.to_string()),
            "the imported source read bound became seed fallback"
        );

        #[cfg(feature = "hooks")]
        {
            for (path, source) in [
                (
                    crate::plugins::MANIFEST,
                    "export default {guard: {enabled: true}};".to_owned(),
                ),
                (
                    "plugins/guard/plugin.ts",
                    "/*".to_owned()
                        + &"x".repeat(1024)
                        + "*/ export default {name: 'guard', description: ''};",
                ),
            ] {
                write(&harness, path, &source).await?;
            }
            let tree = Tree::new(&bounded).await?;
            let original = tree
                .home("plugins/guard/plugin.ts")
                .await
                .err()
                .expect("plugin fits bound");
            let (version, source) = tree
                .home(crate::plugins::MANIFEST)
                .await?
                .expect("plugin entry is absent");
            let layers = [Layer {
                name: "home",
                generation: &version,
                bytes: Some(&source),
            }];
            assert!(
                matches!(
                    crate::plugins::load(&tree, &layers, "root").await,
                    Err(config::AdmissionError::Source(error)) if error.to_string() == original.to_string()
                ),
                "a plugin source refusal was classified as invalid configuration"
            );
            let refused = plugins(&bounded, harness.root, harness.clock.as_ref()).await;
            assert!(
                matches!(refused, Err(error) if error.to_string() == original.to_string()),
                "the plugin source read bound became seed fallback"
            );
        }
        Ok(())
    }

    #[cfg(feature = "hooks")]
    #[tokio::test]
    async fn a_settings_tree_keeps_one_generation_across_concurrent_home_edits() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let harness = LocalHarness::open(
            volume_config(directory.path(), project, text())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?;
        for (path, source) in [
            (
                "plugins.ts",
                "import {enabled} from './switch.ts'; export default {guard: {enabled}};",
            ),
            ("switch.ts", "export const enabled = true;"),
            (
                "plugins/guard/plugin.ts",
                "export default {name: 'guard', description: 'Guard every tool'};",
            ),
            (
                "plugins/guard/hooks/before-tool.sh",
                "printf '%s' '{\"decision\":\"deny\"}'",
            ),
        ] {
            crate::local::tests::settings::write(&harness, path, source).await?;
        }
        let volumes = Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            maximum_bytes: u64::MAX,
        };
        let original = Tree::new(&volumes).await?;
        for (path, source) in [
            ("switch.ts", "export const enabled = false;"),
            (
                "plugins/guard/plugin.ts",
                "export default {name: 'changed', description: 'Changed'};",
            ),
            ("plugins/guard/hooks/before-tool.sh", "\0"),
        ] {
            crate::local::tests::settings::write(&harness, path, source).await?;
        }
        let enabler = harness.root.to_string();
        let (version, bytes) = original.home(crate::plugins::MANIFEST).await?.unwrap();
        let layers = [Layer {
            name: "home",
            generation: &version,
            bytes: Some(&bytes),
        }];
        let enabled = crate::plugins::load(&original, &layers, &enabler).await?;
        assert_eq!(enabled.names, ["guard"]);
        assert!(!enabled.rules.is_empty());
        let current = Tree::new(&volumes).await?;
        let disabled = crate::plugins::load(&current, &layers, &enabler).await?;
        assert!(disabled.names.is_empty());
        assert!(disabled.rules.is_empty());
        Ok(())
    }
}

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
    /// Original agent module links, retained for the selected revision.
    pub(super) links: Vec<config::Link>,
    /// Provider settings use this same immutable home, not a fresh resolution.
    pub(super) source: Arc<Source>,
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
    source: &'a Arc<Source>,
}

/// One settings admission's original read authority and immutable stack.
/// This is a source reference, not a committed model epoch.
pub(super) struct Source {
    read: ContentGrant,
    generation: crate::resources::GenerationRef,
    stack: [u8; 32],
    generation_label: String,
}

/// The provider module's result, retaining its original source association.
pub(super) struct ProviderSettings {
    pub(super) profile: config::ProviderProfile,
    pub(super) links: Vec<config::Link>,
    pub(super) stack: [u8; 32],
}

impl Source {
    async fn new(volumes: &Volumes<'_>) -> Result<Arc<Self>> {
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
        let generation_label = serde_json::to_string(&generation)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let stack = crate::layers::stack_digest([
            (crate::layers::SEED, crate::seed::digest().as_slice()),
            (crate::layers::HOME, generation_label.as_bytes()),
        ]);
        Ok(Arc::new(Self {
            read,
            generation,
            stack,
            generation_label,
        }))
    }

    /// Evaluate a provider entry using the same pinned tree as agent settings.
    /// The original model selection supplies its path and base; no family is
    /// inferred from a provider name, endpoint or credential.
    pub(super) async fn provider(
        self: &Arc<Self>,
        volumes: &Volumes<'_>,
        path: &str,
        base: serde_json::Value,
    ) -> Result<ProviderSettings> {
        if !path.starts_with("plugins/") || !config::composes(path) {
            return Err(Error::Invalid(format!(
                "provider settings admission failed: {path} is not a provider entry; recovery: select plugins/<provider>/provider.ts"
            )));
        }
        let tree = Tree {
            volumes,
            source: self,
        };
        let home = tree.home(path).await?;
        let seed_generation = blake3::Hash::from_bytes(*crate::seed::digest()).to_hex();
        let layers = [
            Layer {
                name: crate::layers::SEED,
                generation: seed_generation.as_str(),
                bytes: crate::seed::file(path),
            },
            Layer {
                name: crate::layers::HOME,
                generation: &self.generation_label,
                bytes: home.as_ref().map(|(_, bytes)| bytes.as_slice()),
            },
        ];
        let (profile, links) = config::provider::admit(path, &layers, &tree, base).await?;
        Ok(ProviderSettings {
            profile,
            links,
            stack: self.stack,
        })
    }
}

impl Tree<'_> {
    // Only the selected settings entry may fall back to the seed on an
    // invalid file. Enabled plugin files and imports retain their read errors.
    async fn entry(&self, path: &str) -> Result<Option<(String, Vec<u8>)>> {
        match self.home(path).await {
            Err(Error::ToolRejected {
                code: crate::wire::ErrorCode::Invalid,
                ..
            }) => Ok(None),
            result => result,
        }
    }

    async fn home(&self, path: &str) -> Result<Option<(String, Vec<u8>)>> {
        match self
            .volumes
            .files
            .read_private_path_indexed(
                self.volumes.home,
                &self.source.read,
                path,
                Some(&self.source.generation),
                self.volumes.maximum_bytes,
                ContentAccess::default(),
            )
            .await
        {
            Ok((file, bytes, _)) => Ok(Some((file.version().to_owned(), bytes.to_vec()))),
            Err(Error::NotFound(_))
            | Err(Error::ToolRejected {
                code: crate::wire::ErrorCode::NotFound,
                ..
            }) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl config::Tree for Tree<'_> {
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let seed = || crate::seed::file(path).map(<[u8]>::to_vec);
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
/// module's admission. An invalid or rejected home module leaves the seed's
/// settings in force; the seed's own module must be admissible.
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
    let source = Source::new(volumes).await?;
    let tree = Tree {
        volumes,
        source: &source,
    };
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(config::AGENT),
    };
    let home = tree.entry(config::AGENT).await?;
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
            Err(Refused(status, error)) => (
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
/// session starts, recording the home module's admission. An invalid home
/// `plugins.ts` leaves the seed's set in force.
#[cfg(any(test, feature = "hooks"))]
pub(super) async fn plugins(
    volumes: &Volumes<'_>,
    agent: AgentId,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<crate::hooks::Hooks> {
    use crate::plugins::{MANIFEST, load};
    let source = Source::new(volumes).await?;
    let tree = Tree {
        volumes,
        source: &source,
    };
    let enabler = agent.to_string();
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(MANIFEST),
    };
    let seeded = |error: Error| match error {
        Error::Invalid(reason) => {
            Error::Invalid(format!("the seed's plugins are not admissible: {reason}"))
        }
        error => error,
    };
    let Some((version, bytes)) = tree.entry(MANIFEST).await? else {
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
        Err(Error::Invalid(reason)) => (
            load(&tree, &layers[..1], &enabler)
                .await
                .map_err(seeded)?
                .rules,
            (AdmissionStatus::Invalid, Some(reason)),
        ),
        Err(error) => return Err(error),
    };
    record(volumes, agent, (MANIFEST, &version), outcome, clock).await?;
    Ok(rules)
}

/// Why a stack's settings are not in force.
struct Refused(AdmissionStatus, String);

impl Refused {
    /// The seed is the floor every home module falls back to; refusing it
    /// is a defect of this build, so the session does not open.
    fn seed(self) -> Error {
        Error::Invalid(format!(
            "the seed's settings are not admissible: {}",
            self.1
        ))
    }
}

impl From<Error> for Refused {
    fn from(error: Error) -> Self {
        Self(AdmissionStatus::Rejected, error.to_string())
    }
}

async fn settle(
    layers: &[Layer<'_>],
    tree: &Tree<'_>,
    started: (Model, Arc<dyn ModelProvider>),
    resolve: Resolve<'_>,
    compaction: (
        &crate::context::CompactionPolicy,
        &Option<crate::context::CompactionPolicy>,
    ),
) -> std::result::Result<Settings, Refused> {
    let selection = format!("{}/{}", started.0.provider, started.0.name);
    let invalid = |error: String| Refused(AdmissionStatus::Invalid, error);
    let (value, links) = config::compose(
        config::AGENT,
        layers,
        tree,
        serde_json::json!({ "model": selection }),
    )
    .await
    .map_err(|error| invalid(error.to_string()))?;
    let settings: AgentConfig = serde_json::from_value(value)
        .map_err(|error| invalid(format!("{}: {error}", config::AGENT)))?;
    let rejected = |reason: &str| {
        Refused(
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
        resolve(&settings.model)?
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
        )?;
    }
    let compaction = if unchanged_model {
        compaction.0.clone()
    } else {
        super::compaction::select(compaction.1.clone(), &model.0, model.1.as_ref())?
    };
    if let Some(output) = settings
        .sampling
        .and_then(|sampling| sampling.max_output_tokens)
    {
        let capacity = model.1.context_capacity(&model.0)?;
        match &compaction {
            crate::context::CompactionPolicy::Disabled => {
                capacity.output_reservation(Some(output))?;
            }
            crate::context::CompactionPolicy::Threshold(policy) => {
                policy.validate(capacity, Some(output))?;
            }
        }
    }
    Ok(Settings {
        compaction,
        model,
        sampling: settings.sampling,
        links,
        source: Arc::clone(tree.source),
    })
}

#[cfg(all(test, feature = "hooks"))]
mod tests {
    use super::*;
    use crate::local::tests::{settings::write, text, volume, volume_config};

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
            (
                "plugins/openai/provider.ts",
                "import {temperature} from './sampling.ts'; export default base => ({...base, sampling: {...base.sampling, temperature}});",
            ),
            (
                "plugins/openai/sampling.ts",
                "export const temperature = 0.25;",
            ),
        ] {
            write(&harness, path, source).await?;
        }
        let volumes = Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            maximum_bytes: u64::MAX,
        };
        let disabled = crate::context::CompactionPolicy::Disabled;
        let selected = admit(
            &volumes,
            harness.root,
            harness.agent()?.clone(),
            None,
            (&disabled, &None),
            harness.clock.as_ref(),
        )
        .await?;
        assert!(!selected.links.is_empty());
        let original_source = selected.source;
        let original = Tree {
            volumes: &volumes,
            source: &original_source,
        };
        let tools: Vec<serde_json::Value> =
            serde_json::from_slice(crate::seed::file("sys/native/openai/tools.json").unwrap())
                .unwrap();
        let tools: Vec<_> = tools.into_iter().map(|definition|
            serde_json::json!({"name": definition["name"], "definition": definition})
        ).collect();
        let base = serde_json::json!({
            "tools": tools,
            "cache": {"breakpoints": ["header", "segments"], "ttl": "5m"},
            "compaction": "native-when-available",
            "summaryHeader": "keep",
            "effort": {"low": {}, "medium": {}, "high": {}},
            "sampling": {"temperature": null, "topP": null, "maxOutputTokens": null},
        });
        for (path, source) in [
            ("switch.ts", "export const enabled = false;"),
            (
                "plugins/guard/plugin.ts",
                "export default {name: 'changed', description: 'Changed'};",
            ),
            ("plugins/guard/hooks/before-tool.sh", "\0"),
            (
                "plugins/openai/sampling.ts",
                "export const temperature = 0.75;",
            ),
            (
                "plugins/openai/provider.ts",
                "import {temperature} from './sampling.ts'; export default base => ({...base, tools: base.tools.map((tool, i) => i ? tool : {...tool, definition: {...tool.definition, description: 'User replacement'}}), sampling: {...base.sampling, temperature}});",
            ),
        ] {
            write(&harness, path, source).await?;
        }
        let enabler = harness.root.to_string();
        let (version, bytes) = original.entry(crate::plugins::MANIFEST).await?.unwrap();
        let layers = [Layer {
            name: "home",
            generation: &version,
            bytes: Some(&bytes),
        }];
        let enabled = crate::plugins::load(&original, &layers, &enabler).await?;
        assert_eq!(enabled.names, ["guard"]);
        assert!(!enabled.rules.is_empty());
        let current_source = Source::new(&volumes).await?;
        let current = Tree {
            volumes: &volumes,
            source: &current_source,
        };
        let disabled = crate::plugins::load(&current, &layers, &enabler).await?;
        assert!(disabled.names.is_empty());
        assert!(disabled.rules.is_empty());

        let path = "plugins/openai/provider.ts";
        let admitted = original_source
            .provider(&volumes, path, base.clone())
            .await?;
        let changed = current_source
            .provider(&volumes, path, base.clone())
            .await?;
        assert_eq!(admitted.profile.sampling.temperature, Some(0.25));
        assert_eq!(changed.profile.sampling.temperature, Some(0.75));
        assert_eq!(
            admitted.profile.tools,
            config::provider::decode(base.clone())?.tools
        );
        assert_ne!(changed.profile.tools[0], admitted.profile.tools[0]);
        assert_eq!(changed.profile.tools[1..], admitted.profile.tools[1..]);
        assert_eq!(admitted.links.len(), 2);
        assert_eq!(
            admitted.links[1].generation,
            original_source.generation_label
        );
        assert_ne!(
            admitted.links[1].module_digest,
            changed.links[1].module_digest
        );
        assert_ne!(admitted.stack, changed.stack);
        assert!(
            original_source
                .provider(&volumes, config::AGENT, base.clone())
                .await
                .is_err()
        );

        write(
            &harness,
            path,
            "export default base => ({...base, sampling: {...base.sampling, topP: 0}});",
        )
        .await?;
        let refused_source = Source::new(&volumes).await?;
        assert!(
            refused_source
                .provider(&volumes, path, base.clone())
                .await
                .is_err()
        );
        // Invalid replacement and later imports cannot corrupt an admitted
        // source or make it silently fall back to the current home's bytes.
        let retained = original_source.provider(&volumes, path, base).await?;
        assert_eq!(retained.profile, admitted.profile);
        assert_eq!(retained.links, admitted.links);
        assert_eq!(retained.stack, admitted.stack);
        Ok(())
    }
}

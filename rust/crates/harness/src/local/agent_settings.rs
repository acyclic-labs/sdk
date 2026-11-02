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
}

impl Tree<'_> {
    async fn home(&self, path: &str) -> Result<Option<(String, Vec<u8>)>> {
        match self
            .volumes
            .files
            .read_private_path_indexed(
                self.volumes.home,
                &self.read,
                path,
                None,
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
    let tree = Tree::new(volumes)?;
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(config::AGENT),
    };
    let home = match tree.home(config::AGENT).await {
        Ok(home) => home,
        // Selected-path refusal is invalid user settings. Storage, authority,
        // cancellation and metadata failures keep their original errors.
        Err(Error::ToolRejected {
            code: crate::wire::ErrorCode::Invalid,
            ..
        }) => None,
        Err(error) => return Err(error),
    };
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
    record(volumes, agent, (config::AGENT, &version), (status, error), clock).await?;
    Ok(settings)
}

impl<'a> Tree<'a> {
    fn new(volumes: &'a Volumes<'a>) -> Result<Self> {
        Ok(Self {
            volumes,
            read: ContentGrant::verify_directory_read(
                volumes.verifier,
                volumes.signed,
                volumes.home,
                "",
                &mut None,
            )?,
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
    let record = format!("live/agents/{agent}/admissions/{version}.json");
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
                "admission:{agent}:{version}:{}",
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
) -> Result<Vec<crate::hooks::HookRule>> {
    use crate::plugins::{MANIFEST, load};
    let tree = Tree::new(volumes)?;
    let enabler = agent.to_string();
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(MANIFEST),
    };
    let seeded = |error: Error| {
        Error::Invalid(format!("the seed's plugins are not admissible: {error}"))
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
        Err(error) => (
            load(&tree, &layers[..1], &enabler).await.map_err(seeded)?.rules,
            (AdmissionStatus::Invalid, Some(error.to_string())),
        ),
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
    let (value, _) = config::compose(
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
    })
}

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
            Err(Error::NotFound(_)) => Ok(None),
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
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<Settings> {
    let tree = Tree {
        volumes,
        read: ContentGrant::verify_directory_read(
            volumes.verifier,
            volumes.signed,
            volumes.home,
            "",
            &mut None,
        )?,
    };
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(config::AGENT),
    };
    let home = tree.home(config::AGENT).await?;
    let Some((version, bytes)) = home else {
        return settle(&[seed], &tree, started, resolve)
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
    let (settings, status, error) = match settle(&layers, &tree, started.clone(), resolve).await {
        Ok(settings) => (settings, AdmissionStatus::Applied, None),
        Err(Refused(status, error)) => (
            settle(&layers[..1], &tree, started, resolve)
                .await
                .map_err(Refused::seed)?,
            status,
            Some(error),
        ),
    };
    let admission = Admission {
        path: config::AGENT.to_owned(),
        version: version.clone(),
        status,
        error,
        at: config::utc(clock.now_unix_millis()),
    };
    let bytes =
        serde_json::to_vec(&admission).map_err(|error| Error::Invalid(error.to_string()))?;
    let path = format!("live/agents/{agent}/admissions/{version}.json");
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
            &path,
            &bytes,
            "application/json",
            "admission",
            volumes.maximum_bytes,
            &crate::IdempotencyKey::new(format!("admission:{agent}:{version}"))?,
            None,
            ContentAccess::default(),
        )
        .await?;
    Ok(settings)
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
        crate::model::ModelSampling {
            seed: None,
            temperature: sampling.temperature,
            top_p: sampling.top_p,
        }
        .validate()?;
        if sampling.max_output_tokens == Some(0) {
            return Err(rejected("sampling.maxOutputTokens must be positive"));
        }
    }
    let model = if settings.model == selection {
        started
    } else {
        let resolve = resolve.ok_or_else(|| {
            rejected("this session's model is bound by its host and cannot be changed here")
        })?;
        resolve(&settings.model)?
    };
    Ok(Settings {
        model,
        sampling: settings.sampling,
    })
}

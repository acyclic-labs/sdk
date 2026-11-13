//! `agent.ts` at session start: the six original sources fold over
//! the model the session was started with; the result selects the model and
//! its sampling. Each source cut's outcome is recorded as an
//! [`Admission`](crate::config::Admission) in the journal.
use super::*;
use crate::config::{self, AdmissionStatus, AgentConfig, Sampling};
use crate::conversation::{ContentAccess, ContentGrant};
use crate::layers::{Layer, is_off};
use crate::model::ResolvedModel;

/// The settings in force for a session.
pub(super) struct Settings {
    pub(super) model: Arc<ResolvedModel>,
    pub(super) sampling: Option<Sampling>,
}

/// Resolves a model selection the settings name.
pub(super) type Resolve<'a> = Option<&'a (dyn Fn(&str) -> Result<Arc<ResolvedModel>> + Sync)>;

/// What a session's settings are read from and recorded in.
pub(super) struct Volumes<'a> {
    pub(super) files: &'a Arc<LocalFilesystem>,
    pub(super) verifier: &'a AuthorityVerifier,
    pub(super) signed: &'a Scope,
    pub(super) home: &'a VolumeRef,
    pub(super) journal: &'a VolumeRef,
    pub(super) seed: (&'a VolumeRef, &'a crate::resources::GenerationRef),
    pub(super) configuration: &'a [Option<crate::layers::PinnedLayer>; 4],
    pub(super) maximum_bytes: Option<u64>,
}

/// One session admission's pinned home and issuer sources; `sys/` only in the seed.
/// Every settings entry point borrows this tree and its original read grant.
pub(super) struct Tree<'a> {
    volumes: &'a Volumes<'a>,
    read: ContentGrant,
    generation: crate::resources::GenerationRef,
    configuration: std::borrow::Cow<'a, [Option<crate::layers::PinnedLayer>; 4]>,
    io: Option<&'a crate::executor::PreparationIo>,
    _cut_hold: Option<crate::conversation::ContentHold<'a>>,
    _construction_custody: Option<crate::filesystem::PreparationCustody>,
}

mod step_cut;

type SourceFiles = [Option<(crate::conversation::FileRef, acyclic_stream::OwnedBytes)>; 5];

fn model_base(
    model: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<serde_json::Value> {
    #[derive(serde::Serialize)]
    struct Base<'a> {
        model: &'a str,
    }
    let base = Base { model };
    match io {
        Some(io) => io.value_json(&base, None),
        None => serde_json::to_value(base).map_err(|error| Error::Invalid(error.to_string())),
    }
}

impl Tree<'_> {
    pub(super) async fn model_selection(&self, base: &str) -> Result<String> {
        let sources = self.sources(config::AGENT).await?;
        let layers = self.layers(config::AGENT, &sources);
        let input = self
            .io
            .map(crate::executor::PreparationIo::scoped)
            .transpose()?;
        let value = config::compose_result(
            config::AGENT,
            &layers,
            self,
            model_base(base, input.as_ref())?,
        )
        .await??;
        Ok(value.decode::<AgentConfig>()?.into_unbounded()?.model)
    }

    async fn source(
        &self,
        layer: &crate::layers::PinnedLayer,
        path: &str,
    ) -> Result<Option<(crate::conversation::FileRef, acyclic_stream::OwnedBytes)>> {
        let read = crate::executor::with_history_io(self.io, |work| {
            ContentGrant::verify_directory_read(
                self.volumes.verifier,
                self.volumes.signed,
                &layer.volume,
                "",
                work,
            )
        })?;
        Ok(self
            .volumes
            .files
            .read_private_path_indexed(
                &layer.volume,
                &read,
                path,
                Some(&layer.generation),
                self.volumes.maximum_bytes,
                ContentAccess {
                    io: self.io,
                    ..ContentAccess::default()
                },
            )
            .await?
            .map(|(file, bytes, _)| (file, bytes)))
    }

    async fn sources(&self, path: &str) -> Result<SourceFiles> {
        let mut sources = std::array::from_fn(|_| None);
        for (index, layer) in self.configuration.iter().enumerate() {
            if let Some(layer) = layer {
                sources[index] = self.source(layer, path).await?;
            }
        }
        sources[4] = self.home(path).await?;
        Ok(sources)
    }

    fn layers<'a>(&self, path: &str, sources: &'a SourceFiles) -> [Layer<'a>; 6] {
        std::array::from_fn(|index| {
            if index == 0 {
                Layer {
                    name: crate::layers::SEED,
                    generation: "",
                    bytes: crate::seed::file(path),
                }
            } else {
                let source = sources[index - 1].as_ref();
                Layer {
                    name: crate::layers::ORDER[index],
                    generation: source.map_or("", |(file, _)| file.version()),
                    bytes: source.map(|(_, bytes)| bytes.as_ref()),
                }
            }
        })
    }

    pub(super) fn cut(&self) -> Result<blake3::Hash> {
        let issuers = self.configuration.as_ref().each_ref().map(|layer| {
            layer
                .as_ref()
                .map(|layer| (&layer.volume, &layer.generation))
        });
        crate::executor::with_history_io(self.io, |work| {
            crate::executor::json::canonical_json_digest(
                &(
                    self.volumes.seed,
                    issuers,
                    (self.volumes.home, &self.generation),
                ),
                None,
                work,
            )
            .map(blake3::Hash::from_bytes)
        })
    }
    async fn home(
        &self,
        path: &str,
    ) -> Result<Option<(crate::conversation::FileRef, acyclic_stream::OwnedBytes)>> {
        Ok(self
            .volumes
            .files
            .read_private_path_indexed(
                self.volumes.home,
                &self.read,
                path,
                Some(&self.generation),
                self.volumes.maximum_bytes,
                ContentAccess {
                    io: self.io,
                    ..ContentAccess::default()
                },
            )
            .await?
            .map(|(file, bytes, _)| (file, bytes)))
    }
}

impl config::Tree for Tree<'_> {
    fn preparation_io(&self) -> Option<&crate::executor::PreparationIo> {
        self.io
    }
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            let seed = || crate::seed::file(path).map(acyclic_stream::OwnedBytes::from_static);
            if !crate::layers::layered(path) {
                return Ok(seed());
            }
            if let Some((_, bytes)) = self.home(path).await? {
                return Ok((!is_off(&bytes)).then_some(bytes));
            }
            for layer in self.configuration.iter().rev().flatten() {
                if let Some((_, bytes)) = self.source(layer, path).await? {
                    return Ok((!is_off(&bytes)).then_some(bytes));
                }
            }
            Ok(seed())
        })
    }
}

/// The six-source settings fold selects over `started`, recording admission
/// under the original complete cut. Invalid or rejected content refuses the
/// session; operational source errors cannot select a lower layer.
pub(super) async fn admit(
    tree: &Tree<'_>,
    agent: AgentId,
    started: Option<Arc<ResolvedModel>>,
    resolve: Resolve<'_>,
    base_model: Option<&str>,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<Settings> {
    let sources = tree.sources(config::AGENT).await?;
    let layers = tree.layers(config::AGENT, &sources);
    let Some((file, _)) = sources.iter().rev().flatten().next() else {
        return settle(&layers, tree, started, resolve, base_model)
            .await
            .map_err(Refused::seed);
    };
    let cut = tree.cut()?.to_hex();
    admitted(
        tree.volumes,
        tree.io,
        agent,
        (config::AGENT, cut.as_str(), file.version()),
        settle(&layers, tree, started, resolve, base_model).await,
        clock,
    )
    .await
}

impl<'a> Tree<'a> {
    fn read_grant(
        volumes: &Volumes<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ContentGrant> {
        crate::executor::with_history_io(io, |work| {
            ContentGrant::verify_directory_read(
                volumes.verifier,
                volumes.signed,
                volumes.home,
                "",
                work,
            )
        })
    }
    pub(super) fn pinned(
        volumes: &'a Volumes<'a>,
        generation: crate::resources::GenerationRef,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let attempt = io.map(crate::executor::PreparationIo::scoped).transpose()?;
        let temporary = attempt.as_ref();
        let read = Self::read_grant(volumes, temporary)?;
        let custody = temporary
            .map(crate::executor::PreparationIo::take_all_owned_custody)
            .transpose()?;
        Ok(Self {
            volumes,
            read,
            generation,
            configuration: std::borrow::Cow::Borrowed(volumes.configuration),
            io,
            _cut_hold: None,
            _construction_custody: custody,
        })
    }
    pub(super) async fn new(
        volumes: &'a Volumes<'a>,
        entrypoint: &str,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let attempt = io.map(crate::executor::PreparationIo::scoped).transpose()?;
        let temporary = attempt.as_ref();
        let read = Self::read_grant(volumes, temporary)?;
        let generation = volumes
            .files
            .select_private_generation(
                volumes.home,
                &read,
                entrypoint,
                None,
                ContentAccess {
                    io: temporary,
                    ..ContentAccess::default()
                },
            )
            .await?;
        let custody = temporary
            .map(crate::executor::PreparationIo::take_all_owned_custody)
            .transpose()?;
        Ok(Self {
            volumes,
            read,
            generation,
            configuration: std::borrow::Cow::Borrowed(volumes.configuration),
            io,
            _cut_hold: None,
            _construction_custody: custody,
        })
    }
}

/// Records a settings decision before returning it. Operational failures keep
/// their original cause; invalid enforcement content cannot select defaults.
async fn admitted<T>(
    volumes: &Volumes<'_>,
    io: Option<&crate::executor::PreparationIo>,
    agent: AgentId,
    version: (&str, &str, &str),
    result: std::result::Result<T, Refused>,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<T> {
    let outcome = match &result {
        Ok(_) => (AdmissionStatus::Applied, None),
        Err(Refused::Settings(status, error)) => (*status, Some(error)),
        Err(Refused::Operation(_)) => return result.map_err(Refused::cause),
    };
    record(volumes, io, agent, version, outcome, clock).await?;
    result.map_err(Refused::cause)
}

/// Records the selected file version's outcome under the complete original
/// configuration cut, so issuer edits cannot overwrite another cut's outcome.
async fn record(
    volumes: &Volumes<'_>,
    io: Option<&crate::executor::PreparationIo>,
    agent: AgentId,
    (path, cut, version): (&str, &str, &str),
    (status, error): (AdmissionStatus, Option<&Error>),
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<()> {
    let attempt = io.map(crate::executor::PreparationIo::scoped).transpose()?;
    let io = attempt.as_ref();
    #[derive(serde::Serialize)]
    struct Outcome<'a, T> {
        path: &'a str,
        version: &'a str,
        status: AdmissionStatus,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<Diagnostic<'a>>,
        at: T,
    }
    struct Diagnostic<'a>(&'a Error);
    impl serde::Serialize for Diagnostic<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            serializer.collect_str(self.0)
        }
    }
    let admission = Outcome {
        path,
        version,
        status,
        error: error.map(Diagnostic),
        at: config::utc_value(clock.now_unix_millis()),
    };
    let bounded;
    let unbounded;
    let bytes: &[u8] = match io {
        Some(io) => {
            bounded = io.encode(&admission, volumes.maximum_bytes, false)?;
            &bounded
        }
        None => {
            unbounded = serde_json::to_vec(&admission)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            &unbounded
        }
    };
    let record = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(
            format_args!("live/agents/{agent}/admissions/{path}/{cut}/{version}.json"),
            work,
        )
    })?;
    crate::executor::with_history_io(io, |work| -> Result<()> {
        if let Some(work) = work.as_deref_mut() {
            work.hashed_bytes(bytes.len() as u64)?;
        }
        Ok(())
    })?;
    let digest = blake3::hash(bytes).to_hex();
    let key = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(
            format_args!("admission:{agent}:{path}:{cut}:{version}:{digest}"),
            work,
        )
    })?;
    let key = crate::IdempotencyKey::new(key)?;
    let grant = crate::executor::with_history_io(io, |work| {
        ContentGrant::verify(
            volumes.verifier,
            volumes.signed,
            volumes.journal,
            VolumeOperation::Write,
            work,
        )
    })?;
    volumes
        .files
        .put_content(
            volumes.journal,
            &grant,
            &record,
            bytes,
            "application/json",
            "admission",
            volumes.maximum_bytes,
            &key,
            None,
            ContentAccess {
                io,
                ..ContentAccess::default()
            },
        )
        .await?;
    Ok(())
}
/// The plugin set `plugins.ts` enables, read as the
/// session starts, recording the home module's admission. Invalid enforcement
/// manifests refuse the session rather than remove its guards.
#[cfg(any(test, not(target_arch = "wasm32")))]
pub(super) async fn plugins(
    question: bool,
    feedback: Option<bool>,
    tree: &Tree<'_>,
    agent: AgentId,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<crate::plugins::Enabled> {
    use crate::plugins::{MANIFEST, load_result};
    let enabler = agent.to_string();
    let feedback = feedback
        .map(|enabled| format!("feedback: {{ ...base.feedback, enabled: {enabled} }},"))
        .unwrap_or_default();
    let selection = format!(
        "export default base => ({{ ...base, question: {{ enabled: {question} }}, {feedback} }});"
    );
    let session = Layer {
        name: "session",
        generation: "",
        bytes: Some(selection.as_bytes()),
    };
    let sources = tree.sources(MANIFEST).await?;
    let original = tree.layers(MANIFEST, &sources);
    let layers: [Layer<'_>; 7] =
        std::array::from_fn(|index| if index == 6 { session } else { original[index] });
    let Some((file, _)) = sources.iter().rev().flatten().next() else {
        return Ok(load_result(tree, &layers, &enabler)
            .await?
            .map_err(|error| match error {
                Error::Invalid(reason) => {
                    Error::Invalid(format!("the seed's plugins are not admissible: {reason}"))
                }
                error => error,
            })?);
    };
    let cut = tree.cut()?.to_hex();
    admitted(
        tree.volumes,
        tree.io,
        agent,
        (MANIFEST, cut.as_str(), file.version()),
        load_result(tree, &layers, &enabler)
            .await?
            .map_err(Refused::evaluated),
        clock,
    )
    .await
}
/// Configuration refusals have a recorded status. Operational failures retain
/// their original typed cause and cannot become an admission record.
#[derive(Debug)]
enum Refused {
    Settings(AdmissionStatus, Error),
    Operation(Error),
}

impl Refused {
    /// The seed itself must be admissible. Read failures keep their cause.
    fn seed(self) -> Error {
        match self {
            Self::Settings(_, reason) => {
                Error::Invalid(format!("the seed's settings are not admissible: {reason}"))
            }
            Self::Operation(error) => error,
        }
    }

    fn cause(self) -> Error {
        match self {
            Self::Settings(_, error) | Self::Operation(error) => error,
        }
    }

    fn evaluated(error: Error) -> Self {
        Self::Settings(AdmissionStatus::Invalid, error)
    }

    fn decoded(error: Error) -> Self {
        if error.is_tool_failure() {
            Self::evaluated(error)
        } else {
            Self::Operation(error)
        }
    }

    fn selection(error: Error) -> Self {
        match error {
            error @ (Error::Invalid(_) | Error::Unsupported(_) | Error::NotFound(_)) => {
                Self::Settings(AdmissionStatus::Rejected, error)
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
    started: Option<Arc<ResolvedModel>>,
    resolve: Resolve<'_>,
    base_model: Option<&str>,
) -> std::result::Result<Settings, Refused> {
    let input = tree
        .preparation_io()
        .map(crate::executor::PreparationIo::scoped)
        .transpose()?;
    let selection = started
        .as_ref()
        .filter(|_| base_model.is_none())
        .map(|model| {
            crate::executor::with_history_io(input.as_ref(), |work| {
                crate::executor::preparation_format(
                    format_args!("{}/{}", model.0.provider, model.0.name),
                    work,
                )
            })
        })
        .transpose()?;
    let base = base_model.or(selection.as_deref()).ok_or_else(|| {
        Refused::Operation(Error::Invalid("startup model selection is absent".into()))
    })?;
    let value = config::compose_result(
        config::AGENT,
        layers,
        tree,
        model_base(base, input.as_ref())?,
    )
    .await?
    .map_err(Refused::evaluated)?;
    let settings = value.decode::<AgentConfig>().map_err(Refused::decoded)?;
    let rejected = |reason: &str| {
        Refused::Settings(
            AdmissionStatus::Rejected,
            Error::Invalid(format!("{}: {reason}", config::AGENT)),
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
    let model = match started.filter(|model| {
        settings
            .model
            .strip_prefix(model.0.provider.as_str())
            .and_then(|name| name.strip_prefix('/'))
            == Some(model.0.name.as_str())
    }) {
        Some(started) => started,
        None => {
            let resolve = resolve.ok_or_else(|| {
                rejected("this session's model is bound by its host and cannot be changed here")
            })?;
            resolve(&settings.model).map_err(Refused::selection)?
        }
    };
    if let Some(sampling) = settings.sampling {
        crate::model::ModelSampling::admit(
            sampling.model_sampling(),
            model
                .1
                .model_profile(&model.0)?
                .map(|profile| profile.sampling()),
        )
        .map_err(Refused::selection)?;
    }
    Ok(Settings {
        model,
        sampling: settings.sampling,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::tests::{text, volume, volume_config};

    #[tokio::test]
    async fn authored_programs_authorize_their_sources_and_share_the_callers_read_allowance()
    -> Result<()> {
        use crate::tool::ToolSource as _;
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let harness = Arc::new(
            LocalHarness::open(
                volume_config(directory.path(), project, text())?
                    .compaction(crate::context::CompactionPolicy::Disabled),
            )
            .await?,
        );
        let grant = ContentGrant::verify(
            &harness.verifier,
            &harness.signed,
            &harness.scratch,
            VolumeOperation::Write,
            &mut None,
        )?;
        let files = [
            ("program.ts", "import a from './first.ts'; import b from './second.ts'; export default {version: 1, name: 'budget', description: a+b, inputSchema: {type: 'object'}, entry: {sh: 'main.sh', ps1: 'main.ps1'}};".to_owned()),
            ("first.ts", format!("export default '{}';", "a".repeat(4096))),
            ("second.ts", format!("export default '{}';", "b".repeat(4096))),
            ("main.sh", "cat".to_owned()),
            ("main.ps1", "[Console]::Out.Write([Console]::In.ReadToEnd())".to_owned()),
        ];
        for (name, contents) in &files {
            let path = format!("tools/budget/{name}");
            harness
                .files
                .put_content(
                    &harness.scratch,
                    &grant,
                    &path,
                    contents.as_bytes(),
                    "text/plain",
                    name,
                    None,
                    &crate::IdempotencyKey::new(format!("program-{name}"))?,
                    None,
                    ContentAccess::default(),
                )
                .await?;
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        let workspace = harness
            .workspace
            .as_ref()
            .expect("the fixture owns a workspace");
        let home = workspace
            .filesystem
            .open_workspace(
                &harness.scratch.storage_name(&mut None)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| storage(failure.error))?
            .value;
        let mut policy: acyclic_fs::access::Policy =
            serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY).map_err(storage)?;
        policy.version = 2;
        let mut deny = policy.layers[0][0].clone();
        deny.id = "private-settings".into();
        deny.effect = acyclic_fs::access::Effect::Deny;
        deny.actions = [
            acyclic_fs::access::Action::Read,
            acyclic_fs::access::Action::ReadHistory,
            acyclic_fs::access::Action::List,
        ]
        .into_iter()
        .collect();
        deny.resource.path = crate::config::AGENT.into();
        policy.layers[0].push(deny);
        let mut writer = home
            .checkout(
                acyclic_fs::model::GenerationSelector::Head,
                acyclic_fs::model::CheckoutMode::tracking_transaction(),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| storage(failure.error))?
            .value;
        writer
            .write_access_policy(
                &serde_json::to_vec(&policy).map_err(storage)?,
                acyclic_fs::NamedAttributeWriteMode::Replace,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| storage(failure.error))?;
        let publication = writer
            .commit::<acyclic_fs::FsError, _, _>(
                acyclic_fs::OperationId::new(),
                None,
                None,
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
                |_| {
                    std::future::ready(Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await
            .map_err(|failure| storage(failure.error))?
            .value;
        assert!(matches!(
            publication,
            acyclic_fs::LivePublicationObservation::Committed { .. }
        ));
        let volumes = Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            seed: (&harness.seed.0, &harness.seed.1),
            configuration: harness.harness.issuer_configuration(),
            maximum_bytes: None,
        };
        let refused = Tree::new(&volumes, crate::config::AGENT, None)
            .await
            .err()
            .ok_or_else(|| Error::Invalid("private settings selection was authorized".into()))?;
        assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
        let source = super::super::programs::Programs {
            local: Arc::downgrade(&harness),
            home: harness.scratch.clone(),
            journal: harness.journal.clone(),
            verifier: harness.verifier.clone(),
            scope: harness.signed.copy(&mut None)?,
            shell: Arc::new(harness.tools.get("shell", None, None)?.unwrap().clone()),
            catalog: crate::tool::ToolRegistry::default(),
        };
        let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
        let control = crate::executor::PreparationIo::from_budget(budget, &cancellation)?;
        assert!(source.load("budget", Some(&control)).await?.is_some());
        let mut allowance = budget;
        allowance.output_bytes = control
            .receipt()
            .output_bytes
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("program reads produced no accounting".into()))?;
        assert!(
            files
                .iter()
                .all(|(_, contents)| (contents.len() as u64) < allowance.output_bytes)
        );
        let limited = crate::executor::PreparationIo::from_budget(allowance, &cancellation)?;
        assert!(
            source.load("budget", Some(&limited)).await.is_err(),
            "a source must not reopen an unmeasured ledger for its imports"
        );
        assert!(limited.receipt().output_bytes <= allowance.output_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn admission_records_use_original_work_before_publication() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let harness = LocalHarness::open(volume_config(directory.path(), project, text())?).await?;
        crate::local::tests::settings::write(
            &harness,
            config::AGENT,
            "export default base => base;",
        )
        .await?;
        let volumes = Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            seed: (&harness.seed.0, &harness.seed.1),
            configuration: harness.harness.issuer_configuration(),
            maximum_bytes: None,
        };
        let grant = ContentGrant::verify_directory_read(
            &harness.verifier,
            &harness.signed,
            &harness.journal,
            "",
            &mut None,
        )?;
        async fn observed(
            volumes: &Volumes<'_>,
            grant: &ContentGrant,
            agent: AgentId,
            cut: &str,
        ) -> Result<Option<crate::config::Admission>> {
            let path = format!("live/agents/{agent}/admissions/agent.ts/{cut}/original.json");
            volumes
                .files
                .read_private_path_indexed(
                    volumes.journal,
                    grant,
                    &path,
                    None,
                    None,
                    ContentAccess::default(),
                )
                .await?
                .map(|(_, bytes, _)| {
                    serde_json::from_slice(&bytes)
                        .map_err(|error| Error::Invalid(error.to_string()))
                })
                .transpose()
        }
        struct Clock;
        impl acyclic_stream::UnixMillisClock for Clock {
            fn now_unix_millis(&self) -> u64 {
                0
            }
        }
        let cancel = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: 16 * 1024 * 1024,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            },
            &cancel,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = crate::executor::PreparationIo::from_native_work(&work)?;
        let generation = Tree::new(&volumes, config::AGENT, None).await?.generation;
        for pinned in [false, true] {
            // A warm-up may retain a shared filesystem cache. A later tree's
            // grant and selected-generation metadata must retire independently.
            let warm = if pinned {
                Tree::pinned(&volumes, generation.clone(), Some(&io))?
            } else {
                Tree::new(&volumes, config::AGENT, Some(&io)).await?
            };
            drop(warm);
            let baseline = work.live_allocation_bytes();
            let tree = if pinned {
                Tree::pinned(&volumes, generation.clone(), Some(&io))?
            } else {
                Tree::new(&volumes, config::AGENT, Some(&io)).await?
            };
            assert!(
                work.live_allocation_bytes() > baseline,
                "a live settings tree must retain its original grant storage"
            );
            drop(tree);
            assert_eq!(
                work.live_allocation_bytes(),
                baseline,
                "dropping a settings tree must retire its original preparation metadata"
            );
        }
        let occupied = work.claim(
            work.remaining()?.peak_allocation_bytes,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let result = record(
            &volumes,
            Some(&io),
            harness.root,
            (config::AGENT, "unfunded", "original"),
            (AdmissionStatus::Applied, None),
            &Clock,
        )
        .await;
        assert!(matches!(
            result,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        ..
                    }
                ),
                ..
            })
        ));
        assert!(
            observed(&volumes, &grant, harness.root, "unfunded")
                .await?
                .is_none()
        );
        drop(occupied);
        let reason = Error::Invalid("original refusal: \"source\"\n".into());
        record(
            &volumes,
            Some(&io),
            harness.root,
            (config::AGENT, "admitted", "original"),
            (AdmissionStatus::Rejected, Some(&reason)),
            &Clock,
        )
        .await?;
        let outcome = observed(&volumes, &grant, harness.root, "admitted")
            .await?
            .ok_or_else(|| Error::NotFound("funded settings admission".into()))?;
        assert_eq!(outcome.status, AdmissionStatus::Rejected);
        assert_eq!(outcome.error.as_deref(), Some(reason.to_string().as_str()));
        assert_eq!(outcome.path, config::AGENT);
        assert_eq!(outcome.version, "original");
        assert_eq!(outcome.at, "1970-01-01T00:00:00.000Z");
        assert!(work.receipt().allocation_operations > 0);
        let retained = work.live_allocation_bytes();
        record(
            &volumes,
            Some(&io),
            harness.root,
            (config::AGENT, "admitted", "original"),
            (AdmissionStatus::Rejected, Some(&reason)),
            &Clock,
        )
        .await?;
        assert_eq!(
            work.live_allocation_bytes(),
            retained,
            "replaying the same record must retire temporary publication storage"
        );
        cancel.cancel();
        let result = record(
            &volumes,
            Some(&io),
            harness.root,
            (config::AGENT, "cancelled", "original"),
            (AdmissionStatus::Applied, None),
            &Clock,
        )
        .await;
        assert!(
            matches!(result, Err(ref error) if error.code() == crate::wire::ErrorCode::Cancelled)
        );
        assert!(
            observed(&volumes, &grant, harness.root, "cancelled")
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn imported_settings_keep_the_original_directory_read_refusal() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let project = volume(directory.path()).await?;
        let harness = LocalHarness::open(
            volume_config(directory.path(), project, text())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?;
        let write = ContentGrant::verify(
            &harness.verifier,
            &harness.signed,
            &harness.scratch,
            VolumeOperation::Write,
            &mut None,
        )?;
        harness
            .files
            .put_content(
                &harness.scratch,
                &write,
                "options.ts",
                b"export const sampling = {temperature: 0.25};",
                "text/typescript",
                "options.ts",
                None,
                &crate::IdempotencyKey::new("settings-import-control".to_owned())?,
                None,
                ContentAccess::default(),
            )
            .await?;
        let volumes = Volumes {
            files: &harness.files,
            verifier: &harness.verifier,
            signed: &harness.signed,
            home: &harness.scratch,
            journal: &harness.journal,
            seed: (&harness.seed.0, &harness.seed.1),
            configuration: harness.harness.issuer_configuration(),
            maximum_bytes: None,
        };
        let layers = [Layer {
            name: "home",
            generation: "original",
            bytes: Some(b"import {sampling} from './options.ts'; export default base => ({...base, sampling});"),
        }];
        for prefix in ["", "allowed"] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget {
                    peak_allocation_bytes: 16 * 1024 * 1024,
                    ..acyclic_fs::WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = crate::executor::PreparationIo::from_native_work(&work)?;
            let mut tree = Tree::new(&volumes, crate::config::AGENT, Some(&io)).await?;
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
                Some(Arc::new(ResolvedModel::new(harness.agent()?.clone()))),
                None,
                None,
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
                (_, Err(error)) => panic!(
                    "settings admission lost its original read producer: {prefix}: {error:?}"
                ),
                (_, Ok(_)) => panic!("{prefix}: imported settings escaped their read boundary"),
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
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
                "agent.ts",
                "import {enabled} from './switch.ts'; export default base => ({...base, sampling: {temperature: enabled ? 0.25 : 0.75}});",
            ),
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
            seed: (&harness.seed.0, &harness.seed.1),
            configuration: harness.harness.issuer_configuration(),
            maximum_bytes: None,
        };
        let storage = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let journal = storage.journal();
        let operation = crate::OperationId::new();
        let original = Tree::for_step(&volumes, journal.as_ref(), operation, 0, None).await?;
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
        let replayed = Tree::for_step(&volumes, journal.as_ref(), operation, 0, None).await?;
        assert_eq!(
            original.cut()?,
            replayed.cut()?,
            "replay substituted current home files"
        );
        let clock = harness.clock.as_ref();
        let enabled = plugins(false, None, &replayed, harness.root, clock).await?;
        assert!(
            !enabled.rules.is_empty(),
            "plugin admission refreshed its pinned home generation"
        );
        let admitted_model = Arc::new(ResolvedModel::new(harness.agent()?.clone()));
        let settings = admit(
            &replayed,
            harness.root,
            Some(Arc::clone(&admitted_model)),
            None,
            None,
            clock,
        )
        .await?;
        assert!(Arc::ptr_eq(&settings.model, &admitted_model));
        assert_eq!(
            settings.sampling.and_then(|sampling| sampling.temperature),
            Some(0.25),
            "settings admission refreshed its pinned home generation"
        );
        let current = Tree::for_step(&volumes, journal.as_ref(), operation, 1, None).await?;
        assert_ne!(
            original.cut()?,
            current.cut()?,
            "next step retained the preceding cut"
        );
        assert!(
            plugins(false, None, &current, harness.root, clock)
                .await?
                .rules
                .is_empty()
        );
        let settings = admit(
            &current,
            harness.root,
            Some(Arc::clone(&admitted_model)),
            None,
            None,
            clock,
        )
        .await?;
        assert!(Arc::ptr_eq(&settings.model, &admitted_model));
        assert_eq!(
            settings.sampling.and_then(|sampling| sampling.temperature),
            Some(0.75)
        );
        Ok(())
    }
}

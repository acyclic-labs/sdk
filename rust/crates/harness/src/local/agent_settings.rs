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
    pub(super) maximum_bytes: Option<u64>,
}

/// One session admission's pinned home over the seed; `sys/` only in the seed.
/// Every settings entry point borrows this tree and its original read grant.
pub(super) struct Tree<'a> {
    volumes: &'a Volumes<'a>,
    read: ContentGrant,
    generation: crate::resources::GenerationRef,
    io: Option<&'a crate::executor::PreparationIo>,
}

impl Tree<'_> {
    pub(super) async fn model_selection(&self, base: &str) -> Result<String> {
        let home = self.home(config::AGENT).await?;
        let layers = [
            Layer {
                name: "seed",
                generation: "",
                bytes: crate::seed::file(config::AGENT),
            },
            Layer {
                name: "home",
                generation: home.as_ref().map_or("", |(file, _)| file.version()),
                bytes: home.as_ref().map(|(_, bytes)| bytes.as_ref()),
            },
        ];
        let (value, _) = config::compose(
            config::AGENT,
            &layers,
            self,
            serde_json::json!({"model": base}),
        )
        .await?;
        let settings: AgentConfig = serde_json::from_value(value.into_unbounded()?)
            .map_err(|error| Error::Invalid(format!("{}: {error}", config::AGENT)))?;
        Ok(settings.model)
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
            Ok(match self.home(path).await? {
                Some((_, bytes)) if is_off(&bytes) => None,
                Some((_, bytes)) => Some(bytes),
                None => seed(),
            })
        })
    }
}

/// The settings `agent.ts` selects over `started`, recording the home
/// module's admission. Invalid or rejected enforcement settings refuse the
/// session; the seed applies only when the home module is absent.
pub(super) async fn admit(
    tree: &Tree<'_>,
    agent: AgentId,
    started: Option<(Model, Arc<dyn ModelProvider>)>,
    resolve: Resolve<'_>,
    base_model: Option<&str>,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<Settings> {
    let seed = Layer {
        name: "seed",
        generation: "",
        bytes: crate::seed::file(config::AGENT),
    };
    let home = tree.home(config::AGENT).await?;
    let Some((file, bytes)) = home else {
        return settle(&[seed], tree, started, resolve, base_model)
            .await
            .map_err(Refused::seed);
    };
    let version = file.version();
    let layers = [
        seed,
        Layer {
            name: "home",
            generation: version,
            bytes: Some(&bytes),
        },
    ];
    admitted(
        tree.volumes,
        agent,
        (config::AGENT, version),
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
    ) -> Result<Self> {
        let read = Self::read_grant(volumes, None)?;
        Ok(Self {
            volumes,
            read,
            generation,
            io: None,
        })
    }
    pub(super) async fn new(
        volumes: &'a Volumes<'a>,
        entrypoint: &str,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let read = Self::read_grant(volumes, io)?;
        let generation = volumes
            .files
            .select_private_generation(
                volumes.home,
                &read,
                entrypoint,
                None,
                ContentAccess {
                    io,
                    ..ContentAccess::default()
                },
            )
            .await?;
        Ok(Self {
            volumes,
            read,
            generation,
            io,
        })
    }
}

/// Records a settings decision before returning it. Operational failures keep
/// their original cause; invalid enforcement content cannot select defaults.
async fn admitted<T>(
    volumes: &Volumes<'_>,
    agent: AgentId,
    version: (&str, &str),
    result: std::result::Result<T, Refused>,
    clock: &dyn acyclic_stream::UnixMillisClock,
) -> Result<T> {
    let outcome = match &result {
        Ok(_) => (AdmissionStatus::Applied, None),
        Err(Refused::Settings(status, error)) => (*status, Some(error.to_string())),
        Err(Refused::Operation(_)) => return result.map_err(Refused::cause),
    };
    record(volumes, agent, version, outcome, clock).await?;
    result.map_err(Refused::cause)
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
    let Some((file, bytes)) = tree.home(MANIFEST).await? else {
        return Ok(load_result(tree, &[seed, session], &enabler)
            .await?
            .map_err(seeded)?);
    };
    let version = file.version();
    let layers = [
        seed,
        Layer {
            name: "home",
            generation: version,
            bytes: Some(&bytes),
        },
        session,
    ];
    admitted(
        tree.volumes,
        agent,
        (MANIFEST, version),
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
    started: Option<(Model, Arc<dyn ModelProvider>)>,
    resolve: Resolve<'_>,
    base_model: Option<&str>,
) -> std::result::Result<Settings, Refused> {
    let selection = started
        .as_ref()
        .map(|(model, _)| format!("{}/{}", model.provider, model.name));
    let base = base_model.or(selection.as_deref()).ok_or_else(|| {
        Refused::Operation(Error::Invalid("startup model selection is absent".into()))
    })?;
    let (value, _) = config::compose_result(
        config::AGENT,
        layers,
        tree,
        serde_json::json!({ "model": base }),
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
    let model = match started.filter(|_| selection.as_deref() == Some(settings.model.as_str())) {
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
            let result = settle(&layers, &tree, Some(harness.agent()?.clone()), None, None).await;
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
            maximum_bytes: None,
        };
        let original = Tree::new(&volumes, crate::config::AGENT, None).await?;
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
        let clock = harness.clock.as_ref();
        let enabled = plugins(false, None, &original, harness.root, clock).await?;
        assert!(
            !enabled.rules.is_empty(),
            "plugin admission refreshed its pinned home generation"
        );
        let settings = admit(
            &original,
            harness.root,
            Some(harness.agent()?.clone()),
            None,
            None,
            clock,
        )
        .await?;
        assert_eq!(
            settings.sampling.and_then(|sampling| sampling.temperature),
            Some(0.25),
            "settings admission refreshed its pinned home generation"
        );
        let current = Tree::new(&volumes, crate::config::AGENT, None).await?;
        assert!(
            plugins(false, None, &current, harness.root, clock)
                .await?
                .rules
                .is_empty()
        );
        let settings = admit(
            &current,
            harness.root,
            Some(harness.agent()?.clone()),
            None,
            None,
            clock,
        )
        .await?;
        assert_eq!(
            settings.sampling.and_then(|sampling| sampling.temperature),
            Some(0.75)
        );
        Ok(())
    }
}

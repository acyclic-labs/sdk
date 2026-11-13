//! Root directory membership is committed with root creation. Latest lookup
//! reads one directory head, independently of the number of sessions.
use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, HeadProjection, HeadsRequest, StreamProviderExt as _,
};

use crate::client_files::{SessionDirectory, SessionDirectoryEntry as Entry};

pub(super) fn directory_path(within: &ActorReference) -> Result<acyclic_stream::StreamPath, Error> {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.root-directory.v1");
    for part in [
        within.account().as_bytes(),
        within.namespace().as_str().as_bytes(),
        within.id().as_bytes(),
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    acyclic_stream::StreamPath::new(format!(
        "harness/session-directory/{}",
        hasher.finalize().to_hex()
    ))
    .map_err(Error::Stream)
}

pub(super) struct DirectoryFile {
    pub(super) path: acyclic_stream::StreamPath,
}

impl<P: acyclic_stream::StreamProvider + ?Sized> crate::client_files::FileView<P>
    for DirectoryFile
{
    fn identity(&self) -> &'static str {
        "session-directory/v1"
    }

    fn source(
        &self,
        _: &AgentPath,
        root: crate::client_files::FileRoot,
        path: &str,
    ) -> crate::Result<Option<acyclic_stream::StreamPath>> {
        Ok((matches!(root, crate::client_files::FileRoot::Private {})
            && path == "status/session-directory.json")
            .then(|| self.path.clone()))
    }

    fn read<'a>(
        &'a self,
        provider: &'a P,
        _: &'a AgentPath,
        paths: &'a [&'a str],
        source: &'a acyclic_stream::StreamPath,
        through: u64,
        at_least: acyclic_stream::RevisionCut,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Vec<crate::client_files::QueryFile>>>
    {
        Box::pin(async move {
            use futures::TryStreamExt as _;
            if source != &self.path || paths != ["status/session-directory.json"] {
                return Err(crate::Error::Invalid(
                    "session directory file source differs".into(),
                ));
            }
            if through == 0 {
                return Ok(Vec::new());
            }
            let mut records = provider
                .read(acyclic_stream::ReadRequest {
                    path: source.clone(),
                    from: 0,
                    limit: 1,
                    key: None,
                    at_least,
                })
                .await
                .map_err(crate::Error::from)?;
            let record = records
                .try_next()
                .await
                .map_err(crate::Error::from)?
                .ok_or_else(|| {
                    crate::Error::Invalid("session directory creation is absent".into())
                })?;
            if record.sequence != 0
                || !matches!(
                    serde_json::from_slice::<Entry>(&record.value),
                    Ok(Entry::Created { version: 1 })
                )
            {
                return Err(crate::Error::Invalid(
                    "invalid session directory creation".into(),
                ));
            }
            Ok(vec![crate::client_files::QueryFile {
                path: paths[0].to_owned(),
                input: crate::supervision::Position {
                    stream: source.clone(),
                    sequence: 0,
                },
                text: serde_json::to_string(&SessionDirectory {
                    version: 1,
                    source: source.clone(),
                })
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
                media_type: "application/json".into(),
            }])
        })
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
    /// Read-only discovery resource for participating roots in this account.
    pub fn peer_resource(&self) -> String {
        format!(
            "actors:/peer-directory/{}",
            acyclic_auth::encode_component(self.within.account().as_bytes())
        )
    }

    /// A directory exposes every root on this placement, so enrollment requires
    /// its original unbound whole-volume owner, before any storage observation.
    pub fn admit_sessions(&self) -> Result<(), Error> {
        if let TurnAdmission::Admitted { scope, .. } = &self.admission {
            self.issuer
                .verifier(&mut None)
                .map_err(turn)?
                .verify(scope, &mut None)
                .map_err(turn)?;
            let whole = acyclic_fs::access::Capability::whole_volume(
                None,
                &acyclic_fs::access::Action::ALL,
            )
            .to_scope_label()
            .map_err(|error| turn(crate::Error::Invalid(error.to_string())))?;
            if scope.agent().is_some()
                || !scope
                    .capabilities()
                    .iter()
                    .any(|grant| grant == whole.as_str())
            {
                return Err(turn(crate::Error::Unauthorized("session directory enrollment requires the placement owner's whole-volume admission".into())));
            }
        }
        Ok(())
    }

    pub(super) fn session_directory(&self) -> Result<acyclic_stream::StreamPath, Error> {
        directory_path(&self.within)
    }

    /// Scope-directory entries created atomically with their independent roots.
    pub fn session_roots(&self) -> Result<acyclic_stream::StreamPath, Error> {
        self.admit_sessions()?;
        self.peer_roots()
    }

    /// Root-directory location for host-authorized peer discovery. Unlike
    /// directory admission this conveys no authority to control other roots.
    /// The host must check peer-resource authority and live membership.
    pub fn peer_roots(&self) -> Result<acyclic_stream::StreamPath, Error> {
        acyclic_stream::StreamPath::new(format!("{}/roots", self.session_directory()?.as_str()))
            .map_err(Error::Stream)
    }

    fn session_names(&self) -> Result<crate::agent::NameIndex<P>, Error> {
        let directory = self.session_directory()?;
        let scope = directory
            .as_str()
            .rsplit('/')
            .next()
            .ok_or(Error::Record("session directory has no scope"))?;
        let path = acyclic_stream::StreamPath::new(format!("harness/names/{scope}"))
            .map_err(Error::Stream)?;
        Ok(crate::agent::NameIndex::new(
            self.agents.provider().clone(),
            path,
        ))
    }

    fn session_member(&self, agent: &AgentPath) -> Result<acyclic_stream::StreamPath, Error> {
        if agent.parent().is_some() {
            return Err(Error::Record(
                "session membership requires an independent root",
            ));
        }
        acyclic_stream::StreamPath::new(format!(
            "{}/{}",
            self.session_roots()?.as_str(),
            agent.id()
        ))
        .map_err(Error::Stream)
    }

    /// Resolve a stable alias through one name-index head. Discovery conveys
    /// no authority to bind, read or drive the returned actor.
    pub async fn resolve_session(&self, name: &str) -> Result<AgentPath, Error> {
        self.admit_sessions()?;
        self.session_names()?
            .resolve(name)
            .await?
            .ok_or(Error::NoSuchAgent)
    }

    /// Bind an additional stable alias, atomically conditioned on this root's
    /// directory membership. A name already bound to another root is refused.
    pub async fn name_session(
        &self,
        agent: &AgentPath,
        name: &str,
        id: &IntentId,
    ) -> Result<(), Error> {
        self.admit_sessions()?;
        let member = self.session_member(agent)?;
        let names = self.session_names()?;
        if let Some(original) = names.resolve(name).await? {
            return if original == *agent {
                Ok(())
            } else {
                Err(Error::Record(
                    "session name already belongs to another root",
                ))
            };
        }
        let directory = self.session_directory()?;
        let mut request = acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            effects: vec![acyclic_stream::CommitEffect::hard(
                vec![acyclic_stream::CommitCondition::Present { path: member }],
                Vec::new(),
            )],
            idempotency_key: crate::agent::intent_key(
                "name-session",
                &[directory.as_str(), agent.as_str(), name, id.as_str()],
            )?,
        };
        names.bind(name, agent, &mut request)?;
        match self
            .agents
            .provider()
            .commit(request)
            .await
            .map_err(Error::Stream)?
        {
            acyclic_stream::CommitOutcome::Committed(_) => Ok(()),
            acyclic_stream::CommitOutcome::Conflict(_) => {
                Err(Error::Record("session name or membership changed"))
            }
        }
    }

    /// Idempotent placement startup. This directory's version-one creation
    /// record is written once; no per-session setup or readiness probe occurs.
    pub async fn initialize_sessions(&self) -> Result<(), Error> {
        self.admit_sessions()?;
        let path = self.session_directory()?;
        let key = crate::agent::intent_key("root-directory", &[path.as_str()])?;
        let value = serde_json::to_vec(&Entry::Created { version: 1 })
            .map_err(|_| Error::Record("directory creation cannot encode"))?;
        let outcome = self
            .agents
            .provider()
            .append(AppendRequest {
                path,
                records: vec![value.into()],
                if_tail: None,
                idempotency_key: key,
            })
            .await
            .map_err(Error::Stream)?;
        match outcome {
            AppendOutcome::Committed(_) => Ok(()),
            AppendOutcome::TailConflict { .. } => Err(Error::Record(
                "unconditional directory creation was refused",
            )),
        }
    }

    pub(super) fn index_session(
        &self,
        agent: &AgentPath,
        request: &mut acyclic_stream::CommitRequest,
    ) -> Result<(), Error> {
        self.admit_sessions()?;
        let path = self.session_directory()?;
        let value = serde_json::to_vec(&Entry::Root {
            version: 1,
            agent: agent.clone(),
        })
        .map_err(|_| Error::Record("directory membership cannot encode"))?;
        let entry = self.session_member(agent)?;
        request.effects.push(acyclic_stream::CommitEffect::hard(
            vec![
                acyclic_stream::CommitCondition::Present { path: path.clone() },
                acyclic_stream::CommitCondition::Absent {
                    path: entry.clone(),
                },
            ],
            vec![
                acyclic_stream::CommitMutation::Append {
                    path,
                    records: vec![value.clone().into()],
                },
                acyclic_stream::CommitMutation::Append {
                    path: entry,
                    records: vec![value.into()],
                },
            ],
        ));
        Ok(())
    }

    /// Fork a session's volumes through the shared child-volume composer.
    /// The root, its directory membership and every volume appear together.
    pub async fn prepare_isolated_session(
        &self,
        intent: &IntentId,
        root: RootRequest,
        source: &AgentPath,
        questions: Option<bool>,
    ) -> Result<Prepared, Error> {
        self.prepare_session_fork(intent, root, source, None, questions)
            .await
    }

    /// A parentless session at an indexed coupled cut, born paused. Its
    /// original session remains available with every later turn and file.
    pub async fn prepare_session_history(
        &self,
        intent: &IntentId,
        root: RootRequest,
        source: &AgentPath,
        at: u64,
    ) -> Result<Prepared, Error> {
        self.prepare_session_fork(intent, root, source, Some(at), None)
            .await
    }

    async fn prepare_session_fork(
        &self,
        intent: &IntentId,
        mut root: RootRequest,
        source: &AgentPath,
        at: Option<u64>,
        questions: Option<bool>,
    ) -> Result<Prepared, Error> {
        self.admit_sessions()?;
        self.admit_path(source)?;
        if !root.fresh.is_empty() || !root.retained.is_empty() {
            return Err(Error::Record(
                "a root volume fork cannot also bind supplied volumes",
            ));
        }
        let source = self.agents.state(source).await?;
        self.admit_agent(&source.path, &source.volumes)?;
        let saved = match at {
            Some(at) => Some(
                super::super::history::read(self.agents.provider().as_ref(), &source.path, at)
                    .await
                    .map_err(turn)?,
            ),
            None => None,
        };
        let history = saved.as_ref().map(|saved| crate::agent::HistorySelection {
            conversation: saved.conversation,
            checkpoint: saved.checkpoint,
        });
        let child = AgentPath::root(&root.agent);
        root.fresh =
            super::super::spawn_volumes::fork_bindings(&child, &source.volumes, history.as_ref())?;
        self.admit_root(&root)?;
        let adopted = ForkAdopted {
            agent: root.agent,
            parent: None,
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: root.fresh,
            brief: root.brief,
            policy: root.policy,
        };
        let Spawn { child, mut request } = match (at, saved.as_ref()) {
            (Some(at), Some(saved)) => self
                .agents
                .prepare_root_history(intent, &source, adopted, at, saved, root.peer)?,
            _ => self.agents.prepare_root_with_peer(
                intent,
                adopted,
                questions.unwrap_or(self.turns.question_plugin()),
                root.peer,
            )?,
        };
        self.index_session(&child, &mut request)?;
        let commit = self
            .spawns
            .root_volumes(&source, &child, history.as_ref(), request)
            .await
            .map_err(|error| {
                turn(crate::Error::Invalid(format!(
                    "root volume preparation: {error}"
                )))
            })?;
        Ok(Prepared { child, commit })
    }

    /// The last committed root membership, or no sessions yet. The lookup
    /// neither walks directory pages nor reads any transcript or volume.
    pub async fn latest_session(&self) -> Result<Option<AgentPath>, Error> {
        self.admit_sessions()?;
        let request = HeadsRequest {
            paths: [self.session_directory()?].into_iter().collect(),
            retained: Default::default(),
            durable_through: None,
            projection: HeadProjection::History,
            children: None,
            limit: 1,
            at_least: Default::default(),
        };
        let view = self
            .agents
            .provider()
            .heads(request.clone())
            .await
            .map_err(Error::Stream)?;
        view.validate_for(&request).map_err(Error::Stream)?;
        let record = view
            .heads
            .into_iter()
            .next()
            .flatten()
            .and_then(|head| head.last)
            .ok_or(Error::Record("session directory has no creation record"))?;
        match serde_json::from_slice(&record.value)
            .map_err(|_| Error::Record("session directory record is malformed"))?
        {
            Entry::Created { version: 1 } => Ok(None),
            Entry::Root { version: 1, agent } if agent.parent().is_none() => Ok(Some(agent)),
            _ => Err(Error::Record(
                "session directory requires version one root entries",
            )),
        }
    }

    /// Record an explicit attach in the directory's original feed. Read-only
    /// selection and preview queries do not change the user's latest session.
    pub async fn select_session(&self, agent: &AgentPath, id: &IntentId) -> Result<(), Error> {
        self.admit_sessions()?;
        let directory = self.session_directory()?;
        let member = self.session_member(agent)?;
        let value = serde_json::to_vec(&Entry::Root {
            version: 1,
            agent: agent.clone(),
        })
        .map_err(|_| Error::Record("session selection cannot encode"))?;
        let request = acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            effects: vec![acyclic_stream::CommitEffect::hard(
                vec![acyclic_stream::CommitCondition::Present { path: member }],
                vec![acyclic_stream::CommitMutation::Append {
                    path: directory.clone(),
                    records: vec![value.into()],
                }],
            )],
            idempotency_key: crate::agent::intent_key(
                "select-session",
                &[directory.as_str(), id.as_str()],
            )?,
        };
        match self
            .agents
            .provider()
            .commit(request)
            .await
            .map_err(Error::Stream)?
        {
            acyclic_stream::CommitOutcome::Committed(_) => Ok(()),
            acyclic_stream::CommitOutcome::Conflict(_) => {
                Err(Error::Record("session is outside the admitted directory"))
            }
        }
    }
}

//! Root directory membership is committed with root creation. Latest lookup
//! reads one directory head, independently of the number of sessions.
use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, HeadProjection, HeadsRequest, StreamProviderExt as _,
};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Entry {
    Created { version: u32 },
    Root { version: u32, agent: AgentPath },
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
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

    fn session_directory(&self) -> Result<acyclic_stream::StreamPath, Error> {
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.root-directory.v1");
        for part in [
            self.within.account().as_bytes(),
            self.within.namespace().as_str().as_bytes(),
            self.within.id().as_bytes(),
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

    /// Scope-directory entries created atomically with their independent roots.
    pub fn session_roots(&self) -> Result<acyclic_stream::StreamPath, Error> {
        self.admit_sessions()?;
        acyclic_stream::StreamPath::new(format!("{}/roots", self.session_directory()?.as_str()))
            .map_err(Error::Stream)
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
        request
            .conditions
            .push(acyclic_stream::CommitCondition::Present { path: path.clone() });
        let value = serde_json::to_vec(&Entry::Root {
            version: 1,
            agent: agent.clone(),
        })
        .map_err(|_| Error::Record("directory membership cannot encode"))?;
        let entry = acyclic_stream::StreamPath::new(format!(
            "{}/{}",
            self.session_roots()?.as_str(),
            agent.id()
        ))
        .map_err(Error::Stream)?;
        request
            .conditions
            .push(acyclic_stream::CommitCondition::Absent {
                path: entry.clone(),
            });
        request.mutations.extend([
            acyclic_stream::CommitMutation::Append {
                path,
                records: vec![value.clone().into()],
            },
            acyclic_stream::CommitMutation::Append {
                path: entry,
                records: vec![value.into()],
            },
        ]);
        Ok(())
    }

    /// Fork a session's volumes through the shared child-volume composer.
    /// The root, its directory membership and every volume appear together.
    pub async fn prepare_isolated_session(
        &self,
        intent: &IntentId,
        root: RootRequest,
        source: &AgentPath,
    ) -> Result<Prepared, Error> {
        self.prepare_session_fork(intent, root, source, None).await
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
        self.prepare_session_fork(intent, root, source, Some(at))
            .await
    }

    async fn prepare_session_fork(
        &self,
        intent: &IntentId,
        mut root: RootRequest,
        source: &AgentPath,
        at: Option<u64>,
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
                .prepare_root_history(intent, &source, adopted, at, saved)?,
            _ => self.agents.prepare_root(intent, adopted)?,
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
        if agent.parent().is_some() {
            return Err(Error::Record(
                "session selection requires an independent root",
            ));
        }
        let directory = self.session_directory()?;
        let member = acyclic_stream::StreamPath::new(format!(
            "{}/{}",
            self.session_roots()?.as_str(),
            agent.id()
        ))
        .map_err(Error::Stream)?;
        let value = serde_json::to_vec(&Entry::Root {
            version: 1,
            agent: agent.clone(),
        })
        .map_err(|_| Error::Record("session selection cannot encode"))?;
        let request = acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![acyclic_stream::CommitCondition::Present { path: member }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: directory.clone(),
                records: vec![value.into()],
            }],
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

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
}

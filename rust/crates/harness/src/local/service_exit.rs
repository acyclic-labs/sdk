//! One durable terminal record per local owner incarnation. Publication precedes
//! session lifetime closure; clients select the original incarnation, never the
//! next process that happens to serve the same workspace.
use super::Directory;
use crate::{
    Error, Result,
    server::{Composition, Hosted, Verifier},
};
use acyclic_auth::refusal::{Canonical, Cause, Parts, Refusal};
use acyclic_native_runtime::{RenameMode, private_file};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    incarnation: crate::OperationId,
    state: State,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Running,
    Stopped,
    Failed { status: Parts },
}

pub(super) struct Journal {
    selection: Observation,
    attempted: AtomicBool,
}

#[derive(Clone)]
pub(in crate::local) struct Observation {
    path: PathBuf,
    incarnation: crate::OperationId,
}

impl Journal {
    pub(super) fn new(directory: &Directory) -> Result<Arc<Self>> {
        let incarnation = crate::OperationId::new();
        let journal = Arc::new(Self {
            selection: Observation::new(directory, incarnation),
            attempted: AtomicBool::new(false),
        });
        journal.publish(State::Running, RenameMode::NoReplace)?;
        Ok(journal)
    }

    pub(super) fn incarnation(&self) -> crate::OperationId {
        self.selection.incarnation
    }

    fn publish(&self, state: State, mode: RenameMode) -> Result<()> {
        let parent = self
            .selection
            .path
            .parent()
            .ok_or_else(|| Error::Invalid("owner record has no directory".into()))?;
        let mut staged = private_file::StagedFile::create(parent, "owner-exit-").map_err(failed)?;
        serde_json::to_writer(
            staged.as_file_mut(),
            &Record {
                version: 1,
                incarnation: self.selection.incarnation,
                state,
            },
        )
        .map_err(failed)?;
        // The original private-file primitive owns fsync and durable namespace publication.
        staged.publish(&self.selection.path, mode).map_err(failed)
    }

    pub(super) fn complete<T>(&self, result: Result<T>) -> Result<T> {
        if self.attempted.swap(true, Ordering::Relaxed) {
            return result;
        }
        let state = match &result {
            Ok(_) => State::Stopped,
            Err(error) => {
                let mut message = error.to_string();
                let mut source = std::error::Error::source(error);
                while let Some(cause) = source {
                    message.push_str("; caused by: ");
                    message.push_str(&cause.to_string());
                    source = cause.source();
                }
                State::Failed {
                    status: Parts {
                        code: 14,
                        message,
                        details: Default::default(),
                        metadata: {
                            let mut metadata = http::HeaderMap::new();
                            let value = http::HeaderValue::from_static(error.code().as_str_name());
                            metadata.insert("acyclic-owner-error-code", value);
                            metadata
                        },
                    },
                }
            }
        };
        Error::after_cleanup(result, self.publish(state, RenameMode::Replace))
    }
}

impl Observation {
    pub(super) fn new(directory: &Directory, incarnation: crate::OperationId) -> Self {
        Self {
            path: directory.0.join(format!("owner-exit-{incarnation}.json")),
            incarnation,
        }
    }

    pub(in crate::local) fn cause(&self) -> Result<Refusal> {
        let file = private_file::open(&self.path, false).map_err(failed)?;
        let record: Record = serde_json::from_reader(file).map_err(failed)?;
        if record.version != 1 || record.incarnation != self.incarnation {
            return Err(Error::Invalid(
                "owner exit record identity or version differs".into(),
            ));
        }
        let (status, reason) = match record.state {
            State::Failed { status } => (status, "failed"),
            State::Stopped => (
                Parts {
                    code: 14,
                    message: "local owner stopped".into(),
                    details: Default::default(),
                    metadata: Default::default(),
                },
                "stopped",
            ),
            State::Running => (
                Parts {
                    code: 14,
                    message: "no durable terminal record was reported for this owner incarnation"
                        .into(),
                    details: Default::default(),
                    metadata: Default::default(),
                },
                "unsettled",
            ),
        };
        Refusal::capture_owned(
            status,
            Canonical {
                code: 14,
                message: "stream unavailable",
                details: &[],
                owned: &[],
            },
            Some(Cause::new("service.owner", reason)),
        )
        .ok_or_else(|| Error::Invalid("owner record has no attribution".into()))
    }
}

/// Decorates the existing composition, not the server or its request protocol.
pub(super) struct Recorded<C> {
    pub(super) composition: C,
    pub(super) journal: Arc<Journal>,
}

impl<C: Composition<Error = Error>> Composition for Recorded<C> {
    type Stream = C::Stream;
    type Objects = C::Objects;
    type Owner = C::Owner;
    type Error = Error;

    fn open(
        self,
        verifier: &Arc<dyn Verifier>,
    ) -> acyclic_stream::BoxProviderFuture<
        'static,
        Result<Hosted<Self::Stream, Self::Objects, Self::Owner>>,
    > {
        let Self {
            composition,
            journal,
        } = self;
        let opening = composition.open(verifier);
        Box::pin(async move {
            let mut host = match opening.await {
                Ok(host) => host,
                Err(error) => return journal.complete(Err(error)),
            };
            let stopped = Arc::clone(&journal);
            host.shutdown = Some(Box::new(move || stopped.complete(Ok(()))));
            if let Some(completion) = host.completion.take() {
                host.completion = Some(Box::pin(async move { journal.complete(completion.await) }));
            }
            Ok(host)
        })
    }
}

fn failed(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::exchange(crate::ExchangeStep::Step("owner exit record"), source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        local::{LocalHarness, agent_conversation::Conversation},
        model::Model,
        test_support::{ScriptedModelProvider, local_harness_config},
    };

    #[tokio::test]
    async fn owner_completion_is_durable_before_session_closure_and_survives_restart() -> Result<()>
    {
        for fail_owner in [false, true] {
            assert_owner_completion(fail_owner).await?;
        }
        Ok(())
    }

    async fn assert_owner_completion(fail_owner: bool) -> Result<()> {
        let expected = if fail_owner {
            "open original journal"
        } else {
            "local owner stopped"
        };
        let workspace = tempfile::tempdir().map_err(failed)?;
        let state = tempfile::tempdir().map_err(failed)?;
        let provider = Arc::new(ScriptedModelProvider::new([]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            },
        ));
        let config = local_harness_config(
            workspace.path(),
            (
                Model::new("scripted".into(), "test".into(), "1".into())?,
                provider,
            ),
        )?
        .state_directory(state.path())
        .feedback_plugin(false)
        .without_default_tools();
        let directory = Directory::for_config(&config)?;
        let owner = directory.own()?;
        let original = owner.exit.selection.clone();
        let journal = Arc::clone(&owner.exit);
        let listener = owner.listener()?;
        let local = Arc::new(LocalHarness::open(config).await?);
        local.agents().await?;
        let hosted = local
            .agents
            .get()
            .ok_or_else(|| Error::Invalid("missing hosted owner".into()))?;
        let changes = hosted.server().bound().changes().map_err(failed)?;
        let (fail, failing) = tokio::sync::oneshot::channel();
        let absent = workspace.path().join("missing-original-journal");
        let hosted = Hosted::new(
            Arc::clone(hosted.server()),
            local.clock.clone(),
            local.clone(),
            Some(Arc::new(changes)),
        )
        .with_completion(async move {
            failing.await.map_err(failed)?;
            std::fs::File::open(absent).map_err(|source| {
                Error::exchange(crate::ExchangeStep::Step("open original journal"), source)
            })?;
            Ok(())
        });
        let config = crate::server::ServerConfig::new(Recorded {
            composition: hosted,
            journal,
        });
        let (stop, stopping) = tokio::sync::oneshot::channel();
        let (ready, readiness) = tokio::sync::oneshot::channel();
        let serving = tokio::spawn(async move {
            let result = crate::server::serve(
                listener,
                config,
                |endpoint| {
                    let credential = endpoint
                        .observation
                        .as_ref()
                        .map(|value| value.credential.clone());
                    let _ = ready.send(owner.publish(endpoint).map(|()| credential));
                },
                async move {
                    let _ = stopping.await;
                },
            )
            .await;
            drop(owner);
            result
        });
        let credential = readiness
            .await
            .map_err(failed)??
            .ok_or_else(|| Error::Invalid("missing credential".into()))?;
        let connection = directory.connect().await?;
        let conversation = Conversation::open(connection.clone());
        let mut events = conversation.observe(None).await?;
        if fail_owner {
            fail.send(())
                .map_err(|_| Error::Invalid("completion receiver ended".into()))?;
        } else {
            stop.send(())
                .map_err(|_| Error::Invalid("shutdown receiver ended".into()))?;
        }
        let error = loop {
            match events.next().await {
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert!(error.to_string().contains(expected), "{error}");
        let mut source = std::error::Error::source(&error);
        let mut retained = false;
        while let Some(cause) = source {
            if let Some(refusal) = cause.downcast_ref::<Refusal>() {
                retained |= refusal
                    .original()
                    .is_some_and(|parts| parts.message.contains(expected));
            }
            source = cause.source();
        }
        assert!(retained, "client lost the typed retained owner status");
        assert_eq!(serving.await.map_err(failed)?.is_err(), fail_owner);
        let bytes = std::fs::read(&original.path).map_err(failed)?;
        assert!(!String::from_utf8_lossy(&bytes).contains(&credential));
        let mut malformed: serde_json::Value = serde_json::from_slice(&bytes).map_err(failed)?;
        malformed["state"]["status"]["unknown"] = true.into();
        assert!(serde_json::from_value::<Record>(malformed).is_err());
        let selected = connection.select(connection.root().clone()).await;
        let error = selected
            .err()
            .ok_or_else(|| Error::Invalid("closed owner selected an agent".into()))?;
        assert!(error.to_string().contains(expected), "{error}");
        let next = directory.own()?;
        assert_ne!(next.exit.incarnation(), original.incarnation);
        let old = connection.stream_failure(acyclic_stream::StreamError::Unavailable);
        assert!(old.to_string().contains(expected));
        assert!(original.cause()?.original().is_some());
        Ok(())
    }
}

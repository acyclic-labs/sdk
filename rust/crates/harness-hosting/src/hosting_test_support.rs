//! Fixtures the hosting tests share: one admitted local root, one platform that
//! decides every command, and the session client that calls it.
//!
//! Compiled for the crate's own unit tests and, through the `test-support`
//! feature, for the integration test binaries, so a fixture has one definition.
use crate::{
    OperationId,
    agent::{AgentPath, AgentState},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{TurnAuthority, TurnExecutors},
    hosting::{DiffSource, Platform},
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_actors::wire;
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_harness_client::{
    AgentCommand, AgentControlFailure, AgentReply, PreparedCommand, decode_agent_reply,
};
use acyclic_stream::{
    BoxProviderFuture, StreamCategory, StreamError,
    session::{CallAuthorization, Client},
};
use bytes::Bytes;
use prost::Message as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Whether a refusal falls in `category`, however the client wraps its cause.
pub fn refused_as(error: &(dyn std::error::Error + 'static), category: &StreamCategory) -> bool {
    error
        .downcast_ref::<StreamError>()
        .is_some_and(|error| error.category() == *category)
}

/// Whether a refusal is the stream's access-denied category.
pub fn access_denied(error: &(dyn std::error::Error + 'static)) -> bool {
    refused_as(error, &StreamCategory::AccessDenied)
}

/// A turn executor set that refuses every turn.
pub struct Never;

impl TurnExecutors for Never {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, crate::Result<Arc<dyn Executor>>> {
        Box::pin(async { Err(crate::Error::Unsupported("no turns in this test".into())) })
    }

    fn templates(&self) -> bool {
        true
    }
}

/// Allows every command until denied; runs children unless childless.
#[derive(Default)]
pub struct Owner {
    /// Refuses every command once set.
    pub denied: AtomicBool,
    /// Revokes the next read, then refuses every command.
    pub revoke_read: AtomicBool,
    /// Revokes the next query, then refuses every command.
    pub revoke_query: AtomicBool,
    /// Serves no children.
    pub childless: AtomicBool,
    /// Serves the placement's roots as independent sessions.
    pub sessions: AtomicBool,
    /// Suspends the next diff source until released.
    pub diff: std::sync::Mutex<Option<QueryPause>>,
    /// The authority the diff source's reader is scoped by.
    pub diff_issuer: std::sync::OnceLock<AuthorityRoot>,
    /// Suspends the next admission until released.
    pub admission: std::sync::Mutex<Option<QueryPause>>,
}

/// A suspension point a test releases once it has observed the wait.
pub struct QueryPause {
    /// Signals that the wait was entered.
    pub entered: tokio::sync::oneshot::Sender<()>,
    /// Releases the wait.
    pub release: tokio::sync::oneshot::Receiver<()>,
}

impl Platform for Owner {
    fn sessions(&self) -> bool {
        self.sessions.load(Ordering::SeqCst)
    }

    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        command: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        let denied = self.denied.load(Ordering::SeqCst);
        if (matches!(command, AgentCommand::Read { .. })
            && self.revoke_read.swap(false, Ordering::SeqCst))
            || self.revoke_query.swap(false, Ordering::SeqCst)
        {
            self.denied.store(true, Ordering::SeqCst);
        }
        Box::pin(async move {
            Ok(if denied {
                PolicyDecision::Deny
            } else {
                PolicyDecision::Allow
            })
        })
    }

    fn runs_children(&self) -> bool {
        !self.childless.load(Ordering::SeqCst)
    }

    fn admitted(&self) -> BoxProviderFuture<'_, std::result::Result<(), StreamError>> {
        Box::pin(async move {
            let pause = self
                .admission
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .take();
            if let Some(pause) = pause {
                pause
                    .entered
                    .send(())
                    .map_err(|()| StreamError::Unavailable)?;
                pause.release.await.map_err(|_| StreamError::Unavailable)?;
            }
            Ok(())
        })
    }

    fn diff_source<'a>(
        &'a self,
        authorization: &'a CallAuthorization,
        agent: &'a AgentPath,
    ) -> BoxProviderFuture<'a, crate::Result<DiffSource>> {
        Box::pin(async move {
            authorization
                .principal()
                .map_err(|error| crate::Error::Storage(error.to_string()))?;
            let pause = self
                .diff
                .lock()
                .map_err(|_| crate::Error::Conflict("diff control poisoned".into()))?
                .take();
            if let Some(pause) = pause {
                pause
                    .entered
                    .send(())
                    .map_err(|()| crate::Error::Conflict("diff observer closed".into()))?;
                pause
                    .release
                    .await
                    .map_err(|_| crate::Error::Conflict("diff source owner closed".into()))?;
            }
            let provider = provider().map_err(|error| crate::Error::Storage(error.to_string()))?;
            let host =
                crate::filesystem::FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
            let owner = crate::AgentId::from_bytes([41; 16]);
            let volume = VolumeRef::new(
                provider.clone(),
                "diff-control".into(),
                VolumeClass::Project,
                VolumeOwner::Agent(owner),
                &mut None,
            )?;
            host.create_volume(&volume).await?;
            let issuer = self.diff_issuer.get_or_init(|| {
                AuthorityRoot::new(
                    "diff-control",
                    [41; 32],
                    Authority {
                        kind: AggregateKind::Conversation,
                        id: "diff-control".into(),
                    },
                )
            });
            let scope = issuer.bootstrap_scope(
                "reader",
                &crate::Capabilities::new([
                    volume.directory_read_capability("", &mut None)?,
                    volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                ]),
                Some(owner),
                &mut None,
            )?;
            let reader = crate::filesystem::FilesystemContentVerifier::new(
                host.clone(),
                &issuer.verifier(&mut None)?,
                &scope,
                64 * 1024,
                None,
            )?;
            let reference =
                crate::filesystem::workspace_ref(provider, &volume.storage_name(&mut None)?)?;
            let generation = host.resolve(&reference, None).await?.generation;
            Ok(DiffSource::new(
                reader,
                acyclic_harness_client::DiffSnapshot {
                    agent: agent.clone(),
                    volume,
                    base: generation.clone(),
                    current: generation,
                },
            ))
        })
    }
}

/// The provider every fixture volume is stored under.
pub fn provider() -> Result<ProviderRef> {
    Ok(ProviderRef::new(
        "agents".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?)
}

/// A credential that may invoke exactly `resource`.
pub fn credential(resource: &str) -> Bytes {
    credentials(&[resource])
}

/// A credential that may invoke exactly `resources`.
pub fn credentials(resources: &[&str]) -> Bytes {
    use acyclic_auth::contract::*;
    acyclic_auth::testing::root(|block| {
        let Some(key) = block
            .certificates
            .as_mut()
            .and_then(|certificates| certificates.keys.first_mut())
        else {
            return;
        };
        key.key.grants = GrantSet {
            grants: resources
                .iter()
                .map(|resource| Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: (*resource).into(),
                            prefix: false,
                        },
                        actions: vec!["actors/invoke".into()],
                    }),
                })
                .collect(),
        };
    })
    .into()
}

/// The canonical Actors invocation that carries `command` to `namespace`.
pub fn invocation(namespace: &str, command: &AgentCommand) -> Result<Bytes> {
    Ok(wire::InvokeActorRequest {
        deployment_envelope: Vec::new(),
        actor_id: "owner".into(),
        method: "POST".into(),
        url: "https://agents.invalid/command".into(),
        body: PreparedCommand::new(command)?.command().to_vec(),
        namespace: Some(namespace.into()),
        account_id: b"account".to_vec(),
        ..Default::default()
    }
    .encode_to_vec()
    .into())
}

/// Dispatches `command` over the session's control channel.
pub async fn call(
    client: &Client,
    namespace: &str,
    command: &AgentCommand,
) -> Result<std::result::Result<AgentReply, AgentControlFailure>> {
    let reply = client
        .actor_call(&invocation(namespace, command)?, 0, Some(1 << 20))
        .await
        .inspect_err(|error| eprintln!("session control {command:?}: {error}"))?;
    let response = wire::InvokeActorResponse::decode(reply)?;
    Ok(decode_agent_reply(&response.body, command)?)
}

/// Dispatches `command` over the session's diff projection.
pub async fn diff_call(
    client: &Client,
    namespace: &str,
    command: &AgentCommand,
) -> Result<std::result::Result<AgentReply, AgentControlFailure>> {
    let mut query = client
        .actor_socket(
            acyclic_harness_client::DiffProjection::Patch.open(&invocation(namespace, command)?)?,
            0,
        )
        .await?;
    while let Some(frame) = query.next().await? {
        match frame.split_first() {
            Some((1 | 4, _)) => {}
            Some((2 | 5, rest)) => return Ok(decode_agent_reply(rest, command)?),
            _ => return Err("unexpected diff source frame".into()),
        }
    }
    Ok(Ok(AgentReply::Diff { items: Vec::new() }))
}

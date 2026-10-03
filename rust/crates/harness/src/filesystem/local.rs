//! Durable local Harness composition using public Stream and Filesystem providers.
use super::{FilesystemHost, HarnessStorage};
use crate::{
    AgentId, ConversationId, Error, OperationId, Result,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer},
    executor::TurnOutput,
    model::{Model, ModelProvider},
    resources::ProviderRef,
};
use acyclic_fs::{LocalAuthorityBackend, LocalFs, LocalObjectBackend, LocalOptions};
use acyclic_stream::{AppendOutcome, LocalStream, LocalStreamLimits, StreamClient, StreamError};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

/// Persistent providers own durability; storage semantics are shared with memory.
pub type DurableHarnessStorage =
    HarnessStorage<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionDescriptor {
    version: u32,
    agent: AgentId,
    conversation: Authority,
    private_volume: VolumeRef,
    signing_key: [u8; 32],
    model: Model,
    limits: Limits,
}

impl SessionDescriptor {
    fn fresh(model: Model, limits: Limits) -> Result<Self> {
        let agent = AgentId::new();
        let descriptor = Self {
            version: 1,
            agent,
            conversation: Authority {
                kind: AggregateKind::Conversation,
                id: ConversationId::new().to_string(),
            },
            private_volume: VolumeRef::new(
                ProviderRef::new("local", "filesystem", "2")?,
                OperationId::new().to_string(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )?,
            signing_key: *blake3::hash(&OperationId::new().into_bytes()).as_bytes(),
            model,
            limits,
        };
        Ok(descriptor)
    }
}

fn validate_descriptor(
    descriptor: &SessionDescriptor,
    model: &Model,
    limits: Limits,
) -> Result<()> {
    if descriptor.version != 1
        || &descriptor.model != model
        || crate::contract::canonical_json_digest(&descriptor.limits)?
            != crate::contract::canonical_json_digest(&limits)?
    {
        return Err(Error::Conflict(
            "local session composition differs from its pinned descriptor".into(),
        ));
    }
    Ok(())
}

/// Ready-to-run, reopenable local agent with pinned composition.
pub struct PersistentLocalHarness {
    storage: DurableHarnessStorage,
    bundle: crate::Harness,
}
impl PersistentLocalHarness {
    /// Opens an exclusive session root, preserving stable identities and configuration.
    pub async fn open(
        root: impl AsRef<Path>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        limits: Limits,
    ) -> Result<Self> {
        limits.validate()?;
        let root = root.as_ref();
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(root.join("history"), LocalStreamLimits::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        ));
        let metadata = stream
            .stream("harness/session")
            .map_err(|error| Error::Storage(error.to_string()))?;
        let missing = match metadata.tail().await {
            Ok(1) => false,
            Ok(0) | Err(StreamError::NotFound) => true,
            Ok(_) => {
                return Err(Error::Storage(
                    "invalid local session descriptor tail".into(),
                ));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if missing {
            let descriptor = SessionDescriptor::fresh(model.clone(), limits)?;
            match metadata
                .append_at(crate::contract::canonical_json_bytes(&descriptor)?, 0)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
            {
                AppendOutcome::Committed(_) => {}
                AppendOutcome::TailConflict { .. } => {
                    return Err(Error::Conflict(
                        "local initialization lost ownership".into(),
                    ));
                }
            }
        }
        let mut records = metadata
            .read(0, 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let record = records
            .next()
            .await
            .ok_or_else(|| Error::Storage("local descriptor is missing".into()))?
            .map_err(|error| Error::Storage(error.to_string()))?;
        let descriptor: SessionDescriptor = serde_json::from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        validate_descriptor(&descriptor, &model, limits)?;
        let fs = LocalFs::local(LocalOptions::new(root.join("filesystem")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let host = Arc::new(FilesystemHost::new(
            fs,
            descriptor.private_volume.provider().clone(),
        )?);
        host.create_volume(&descriptor.private_volume).await?;
        let issuer = AuthorityIssuer::new(
            "local-harness",
            descriptor.signing_key,
            descriptor.conversation.clone(),
        );
        let storage = DurableHarnessStorage::from_providers(
            descriptor.agent,
            limits.file_bytes,
            host,
            stream,
            descriptor.private_volume,
            descriptor.conversation,
            issuer,
        )
        .await?;
        let tools = storage.default_tools(limits)?;
        let bundle = storage
            .builder()
            .model(model, provider)
            .tools(tools)
            .grant("model:generate")
            .grant("tool:call:acyclic.read_file")
            .grant("tool:call:acyclic.stage_file")
            .grant("tool:call:acyclic.list_files")
            .limits(limits)
            .build()?;
        Ok(Self { storage, bundle })
    }
    /// Runs or recovers an exact prompt with a caller-retained operation identity.
    pub async fn run(&self, operation: OperationId, prompt: &str) -> Result<TurnOutput> {
        let content = self
            .storage
            .stage(
                operation,
                &format!("turns/{operation}/user.txt"),
                prompt.as_bytes(),
                "text/plain",
                "prompt.txt",
            )
            .await?;
        let max_steps = u32::try_from(self.bundle.limits().model_steps)
            .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?;
        self.storage
            .run_conversation(&self.bundle, operation, content, vec![], max_steps)
            .await
    }
    /// Provider-bound storage for tools and recovery.
    #[must_use]
    pub fn storage(&self) -> &DurableHarnessStorage {
        &self.storage
    }
    /// Runtime shared with other local host compositions.
    #[must_use]
    pub fn bundle(&self) -> &crate::Harness {
        &self.bundle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelAttempt, ModelEvent, ModelRequest};
    use futures::{future::BoxFuture, stream::BoxStream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Mock(AtomicUsize);
    impl ModelProvider for Mock {
        fn generate<'a>(&'a self, _: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter([
                Ok(ModelEvent::Content {
                    delta: "persisted".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: serde_json::Value::Null,
                }),
            ]))
        }
        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }
    #[tokio::test]
    async fn reopen_recovers_completed_turn_without_dispatch() -> Result<()> {
        let root = std::env::temp_dir().join(format!("harness-reopen-{}", OperationId::new()));
        let provider = Arc::new(Mock(AtomicUsize::new(0)));
        let model = Model::new("mock", "durable", "1", serde_json::json!({}))?;
        let operation = OperationId::new();
        let file;
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                provider.clone(),
                Limits::default(),
            )
            .await?;
            file = session
                .storage()
                .stage(
                    OperationId::new(),
                    "example.txt",
                    b"original",
                    "text/plain",
                    "example.txt",
                )
                .await?;
            assert_eq!(
                session.run(operation, "exact prompt\r\n").await?.text,
                "persisted"
            );
        }
        {
            let session = PersistentLocalHarness::open(
                &root,
                model.clone(),
                provider.clone(),
                Limits::default(),
            )
            .await?;
            assert_eq!(session.storage().read(&file).await?, b"original");
            assert_eq!(
                session.run(operation, "exact prompt\r\n").await?.text,
                "persisted"
            );
            assert_eq!(provider.0.load(Ordering::SeqCst), 1);
            assert!(session.run(operation, "different").await.is_err());
        }
        let changed = Model::new("mock", "changed", "1", serde_json::json!({}))?;
        assert!(
            PersistentLocalHarness::open(&root, changed, provider, Limits::default())
                .await
                .is_err()
        );
        std::fs::remove_dir_all(root).map_err(|error| Error::Storage(error.to_string()))?;
        Ok(())
    }
}

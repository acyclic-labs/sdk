mod extensions;
#[path = "history.rs"]
mod history_tests;
mod interactions;
#[cfg(feature = "filesystem")]
#[path = "../measured_fork/mod.rs"]
mod measured_fork_tests;
#[cfg(feature = "filesystem")]
#[path = "../measured_open/mod.rs"]
mod measured_open_tests;
mod messages;
mod projections;

use super::*;

use crate::wire_codec::encode_event;

use crate::{
    Capabilities, OperationId,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    core::{Action, AggregateKind, AuthorityIssuer, ExtensionDependency, ExtensionForkPolicy},
    interaction::{
        Interaction, InteractionKind, InteractionOutcome, InteractionResolution, InteractionTicket,
    },
    resources::ProviderRef,
};

use acyclic_stream::MemoryStream;

use serde_json::json;

use std::sync::Arc;

fn interaction_file(path: &str, bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        content()?.volume().clone(),
        path.into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
        "interaction.json".into(),
        &mut None,
    )
}

fn authority() -> Authority {
    Authority {
        kind: AggregateKind::Conversation,
        id: "conversation-1".into(),
    }
}

fn command(identity: u8) -> Result<Command> {
    Ok(Command {
        operation_id: OperationId::from_bytes([identity; 16]),
        idempotency_key: IdempotencyKey(format!("append-conversation-1-{identity}")),
        expected_revision: 0,
        scope: issuer()
            .issue(
                "root",
                &Capabilities::new(["event:append"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::AppendCustom {
            schema: "example.message".into(),
            version: 1,
            content: content()?,
        },
    })
}

const EXTENSION_BYTES: &[u8] = br#"{"text":"hello"}"#;

fn content() -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
            "extension".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
            &mut None,
        )?,
        "extension/event.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(EXTENSION_BYTES, "application/json", &mut None)?,
        "event.json".into(),
        &mut None,
    )
}

struct TestContent;

struct TestPublisher {
    pub(super) volume: VolumeRef,
    pub(super) stages: std::sync::atomic::AtomicUsize,
    pub(super) files: MigrationContentStore,
}

#[derive(Clone, Default)]
pub(super) struct MigrationContentStore(
    pub(super) Arc<std::sync::Mutex<BTreeMap<String, Vec<u8>>>>,
);

impl MigrationContentStore {
    fn key(reference: &FileRef) -> Result<String> {
        Ok(format!(
            "{}:{}",
            serde_json::to_string(reference.volume())
                .map_err(|error| Error::Invalid(error.to_string()))?,
            reference.path()
        ))
    }

    fn insert(&self, reference: &FileRef, bytes: &[u8]) -> Result<()> {
        reference.descriptor().verify(bytes, &mut None)?;
        self.0
            .lock()
            .map_err(|_| Error::Storage("migration content lock poisoned".into()))?
            .insert(Self::key(reference)?, bytes.to_vec());
        Ok(())
    }
}

impl ContentResidencyVerifier for MigrationContentStore {
    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if io.is_some() {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "fixture has no finite content reader",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length() > maximum_bytes {
                return Err(Error::Invalid("content exceeds fixture read bound".into()));
            }
            let bytes = self
                .0
                .lock()
                .map_err(|_| Error::Storage("migration content lock poisoned".into()))?
                .get(&Self::key(reference)?)
                .cloned()
                .ok_or_else(|| Error::NotFound("migration content is absent".into()))?;
            if bytes.len() as u64 > maximum_bytes {
                return Err(Error::Invalid("content exceeds byte bound".into()));
            }
            reference.descriptor().verify(&bytes, &mut None)?;
            Ok(bytes.into())
        })
    }
}

impl ContentPublisher for TestPublisher {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage<'a>(
        &'a self,
        _operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
        self.supports_stage(io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if bytes != EXTENSION_BYTES || media_type != "application/json" {
                return Err(Error::Invalid("unexpected migration output".into()));
            }
            self.stages
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let reference = FileRef::new(
                self.volume.clone(),
                path.into(),
                "generation-1".into(),
                FileDescriptor::from_bytes(bytes, media_type, &mut None)?,
                display_name.into(),
                &mut None,
            )?;
            self.files.insert(&reference, bytes)?;
            Ok((reference, None))
        })
    }
}

impl ContentResidencyVerifier for TestContent {
    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if io.is_some() {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "fixture has no finite content reader",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if EXTENSION_BYTES.len() as u64 > maximum_bytes {
                return Err(Error::Invalid("content exceeds byte bound".into()));
            }
            reference.descriptor().verify(EXTENSION_BYTES, &mut None)?;
            Ok(acyclic_stream::OwnedBytes::from_static(EXTENSION_BYTES))
        })
    }
}

async fn with_content<P: StreamProvider>(client: &Arc<P>) -> Result<StreamAggregate<P>> {
    Ok(StreamAggregate::open(
        client,
        authority(),
        issuer().verifier(&mut None)?,
        schemas(),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(Arc::new(TestContent)))
}

fn issuer() -> AuthorityIssuer {
    AuthorityIssuer::new("test", [7; 32], authority())
}

fn schemas() -> SchemaRegistry {
    let mut schemas = SchemaRegistry::new();
    assert!(
        schemas
            .register(
                "example.message",
                1,
                json!({"type": "object"}),
                [9; 32],
                crate::core::ExtensionForkPolicy::Inherit,
                [],
                None
            )
            .is_ok()
    );
    schemas
}

use projections::projection_command;

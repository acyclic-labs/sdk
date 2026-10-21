use super::*;
use crate::conversation::{
    Attachment, ConversationMessage, FileDescriptor, FileRef, ReferencedAttachments, VolumeClass,
    VolumeOwner, VolumeRef,
};
use crate::{AgentId, OperationId, resources::ProviderRef};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};
use uuid::Uuid;

#[derive(Default)]
struct Resolver {
    bytes: Vec<u8>,
    calls: AtomicUsize,
}
impl ContentResidencyVerifier for Resolver {
    fn read<'a>(
        &'a self,
        _: &'a FileRef,
        _: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.bytes.clone().into())
        })
    }
}

fn file(agent: AgentId, path: &str, media_type: &str, bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?,
        path.into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, media_type, &mut None)?,
        "data".into(),
        &mut None,
    )
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn recorded_tool_metadata_projects_on_its_original_native_ledger() -> Result<()> {
    const RECORD: &[u8] = br#"{"invocation":{"operation_id":"00000000-0000-0000-0000-000000000001","call_id":"call","name":"echo","arguments":{"value":1}},"metadata":{"signature":"opaque","blocks":[{"thinking":"plan"}]}}"#;
    struct NativeRecord;
    impl ContentResidencyVerifier for NativeRecord {
        fn read<'a>(
            &'a self,
            _: &'a FileRef,
            _: u64,
            io: Option<&'a PreparationIo>,
        ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
        {
            assert!(io.and_then(PreparationIo::original_native_work).is_some());
            crate::conversation::BoundedContentFuture::new(io, || async {
                Ok(acyclic_stream::OwnedBytes::from_static(RECORD))
            })
        }
    }
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 4 * 1024 * 1024),
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let before = work.live_allocation_bytes();
    let reference = file(
        AgentId::new(),
        "invocation.json",
        "application/json",
        RECORD,
    )?;
    {
        let io = PreparationIo::from_native_work(&work)?;
        let call = artifacts::read_tool_call(&NativeRecord, &reference, 4096, Some(&io)).await?;
        assert_eq!(call.call_id, "call");
        assert_eq!(call.arguments, serde_json::json!({"value":1}));
        assert_eq!(
            call.metadata,
            serde_json::json!({"signature":"opaque","blocks":[{"thinking":"plan"}]})
        );
        let custody = io.take_native_custody(call.payload_bytes()?)?;
        assert!(custody.work().shares_allocation_context(&work));
        drop(call);
        drop(custody);
    }
    assert_eq!(work.live_allocation_bytes(), before);
    Ok(())
}

fn message(
    sequence: u64,
    kind: MessageKind,
    content: FileRef,
    reply_to: Option<Uuid>,
) -> ConversationMessage {
    ConversationMessage {
        id: Uuid::new_v4(),
        sequence,
        kind,
        content,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to,
        tool_call_id: None,
        extensions: BTreeMap::new(),
    }
}

fn append(
    conversation: &mut ConversationState,
    content: &FileRef,
    kind: MessageKind,
    reply: Option<Uuid>,
) -> Result<Uuid> {
    let message = message(
        conversation.logical_revision() + 1,
        kind,
        content.clone(),
        reply,
    );
    let id = message.id;
    conversation.append(message, &mut None)?;
    Ok(id)
}

#[test]
fn canonical_checkpoint_selects_exact_bounded_delta_from_ten_thousand_records() -> Result<()> {
    let agent = AgentId::new();
    let mut conversation = ConversationState::default();
    conversation.bind(agent)?;
    let content = file(agent, "message.txt", "text/plain", b"data")?;
    for _ in 0..5_000 {
        let user = append(&mut conversation, &content, MessageKind::User, None)?;
        append(
            &mut conversation,
            &content,
            MessageKind::Assistant,
            Some(user),
        )?;
    }
    let current = message(10_001, MessageKind::User, content, None);
    let limits = Limits {
        context_messages: 3,
        ..Limits::default()
    };
    assert!(select_turn_delta(&conversation, &current, true, None, limits).is_err());
    let payload = file(agent, "source.json", "application/json", b"data")?;
    let checkpoint = crate::context::CanonicalContextCheckpoint {
        operation_id: OperationId::new(),
        selection: ModelContextSelection {
            conversation_revision: 9_998,
            message_ids: vec![
                conversation.messages[9_996].id,
                conversation.messages[9_997].id,
            ],
            checkpoint: Some(file(agent, "previous.json", "application/json", b"data")?),
        },
        source: payload.clone(),
        retained: payload.clone(),
        compaction: payload,
    };
    let bytes = crate::contract::canonical_json_bytes(&checkpoint)?;
    let pinned = file(agent, "checkpoint.json", "application/json", &bytes)?;
    let selected = select_turn_delta(
        &conversation,
        &current,
        true,
        Some((&checkpoint, pinned.clone())),
        limits,
    )?;
    assert_eq!(
        selected.message_ids,
        vec![
            conversation.messages[9_998].id,
            conversation.messages[9_999].id,
            current.id
        ]
    );
    assert_eq!(selected.checkpoint, Some(pinned.clone()));
    assert_eq!(selected.conversation_revision, 10_001);
    assert!(
        select_turn_delta(
            &conversation,
            &current,
            true,
            Some((&checkpoint, pinned.clone())),
            Limits {
                context_messages: 2,
                ..limits
            }
        )
        .is_err()
    );
    let mut changed = checkpoint.clone();
    changed.operation_id = OperationId::new();
    assert!(
        select_turn_delta(
            &conversation,
            &current,
            true,
            Some((&changed, pinned.clone())),
            limits
        )
        .is_err()
    );
    conversation.append(current.clone(), &mut None)?;
    assert_eq!(
        select_turn_delta(
            &conversation,
            &current,
            false,
            Some((&checkpoint, pinned)),
            limits
        )?,
        selected
    );
    Ok(())
}

#[test]
fn dispatch_admission_checks_bounds_and_current_user() {
    let mut selected = SelectedModelContext {
        selection: ModelContextSelection {
            checkpoint: None,
            conversation_revision: 1,
            message_ids: vec![Uuid::new_v4()],
        },
        messages: vec![ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("hello".into()),
        }],
    };
    assert!(selected.validate_for_dispatch(Limits::default()).is_ok());
    assert!(
        selected
            .validate_for_dispatch(Limits {
                render_bytes: 4,
                ..Limits::default()
            })
            .is_err()
    );
    selected.messages[0].role = ModelRole::Assistant;
    assert!(selected.validate_for_dispatch(Limits::default()).is_err());
    selected.messages[0].role = ModelRole::User;
    selected.selection.message_ids.clear();
    assert!(selected.validate_for_dispatch(Limits::default()).is_err());
}

#[tokio::test]
async fn manifest_projection_checks_limit_before_resolution_and_exact_count_after() -> Result<()> {
    let agent = AgentId::new();
    let attachment = Attachment {
        file: file(agent, "image.png", "image/png", b"data")?,
        label: None,
    };
    let bytes = crate::conversation::encode_attachment_manifest(std::slice::from_ref(&attachment))?;
    let mut message = message(
        1,
        MessageKind::User,
        file(agent, "message.txt", "text/plain", b"data")?,
        None,
    );
    message.attachments = ReferencedAttachments::Manifest {
        manifest: file(
            agent,
            "manifest.json",
            "application/vnd.acyclic.harness.attachments+json",
            &bytes,
        )?,
        item_count: 2,
    };
    let mut conversation = ConversationState::default();
    conversation.bind(agent)?;
    conversation.append(message.clone(), &mut None)?;
    let selection = ModelContextSelection {
        checkpoint: None,
        conversation_revision: 1,
        message_ids: vec![message.id],
    };
    let resolver = Resolver {
        bytes,
        ..Resolver::default()
    };
    let limits = Limits {
        context_messages: 256,
        attachments: 1,
        render_bytes: 128 * 1024,
        ..Limits::default()
    };
    assert!(
        select_model_context(
            &conversation,
            1,
            selection.clone(),
            &resolver,
            limits,
            1,
            None
        )
        .await
        .is_err()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    let limits = Limits {
        attachments: 2,
        ..limits
    };
    assert!(
        select_model_context(
            &conversation,
            1,
            selection.clone(),
            &resolver,
            limits,
            2,
            None
        )
        .await
        .is_err()
    );
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    let bytes = crate::conversation::encode_attachment_manifest(&[attachment.clone(), attachment])?;
    message.attachments = ReferencedAttachments::Manifest {
        manifest: file(
            agent,
            "manifest.json",
            "application/vnd.acyclic.harness.attachments+json",
            &bytes,
        )?,
        item_count: 2,
    };
    let mut conversation = ConversationState::default();
    conversation.bind(agent)?;
    conversation.append(message, &mut None)?;
    let resolver = Resolver {
        bytes,
        ..Resolver::default()
    };
    let projected =
        select_model_context(&conversation, 1, selection, &resolver, limits, 2, None).await?;
    assert_eq!(projected.messages.len(), 1);
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn json_projection_checks_resolved_bytes_before_parsing() -> Result<()> {
    let reference = file(AgentId::new(), "tool.json", "application/json", b"{}")?;
    let resolver = Resolver {
        bytes: vec![0; 1_025],
        ..Resolver::default()
    };
    assert!(matches!(
        artifacts::read_value(&resolver, &reference, 1_024, None).await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_manifest_resolution_never_calls_the_ordinary_resolver() -> Result<()> {
    native_manifest_resolution(false).await
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_manifest_resolution_rejects_foreign_bytes() -> Result<()> {
    native_manifest_resolution(true).await
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
async fn native_manifest_resolution(foreign_bytes: bool) -> Result<()> {
    struct NativeResolver {
        ordinary: AtomicUsize,
        native: AtomicUsize,
        foreign_bytes: bool,
    }
    impl ContentResidencyVerifier for NativeResolver {
        fn supports_read(&self, _: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
            if let Some(io) = io {
                io.check_cancellation()?;
            }
            Ok(true)
        }
        fn read<'a>(
            &'a self,
            _: &'a FileRef,
            _: u64,
            io: Option<&'a PreparationIo>,
        ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>
        {
            crate::conversation::BoundedContentFuture::new(io, || async move {
                let work = io.and_then(|io| io.original_native_work());
                let counter = if work.is_some() {
                    &self.native
                } else {
                    &self.ordinary
                };
                counter.fetch_add(1, Ordering::SeqCst);
                if !self.foreign_bytes
                    && let Some(work) = work
                {
                    let (mut bytes, custody) = work.copy_bytes(b"[]")?;
                    return acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                        &mut bytes,
                        &mut Some(custody),
                        &work.runtime_meter(),
                    )
                    .map_err(|error| work.runtime_error(error).into());
                }
                Ok(acyclic_stream::OwnedBytes::from_static(b"[]"))
            })
        }
    }
    let agent = AgentId::new();
    let mut message = message(
        1,
        MessageKind::User,
        file(agent, "message.txt", "text/plain", b"data")?,
        None,
    );
    message.attachments = ReferencedAttachments::Manifest {
        manifest: file(
            agent,
            "manifest.json",
            "application/vnd.acyclic.harness.attachments+json",
            b"[]",
        )?,
        item_count: 1,
    };
    let resolver = NativeResolver {
        ordinary: AtomicUsize::new(0),
        native: AtomicUsize::new(0),
        foreign_bytes,
    };
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            peak_allocation_bytes: 16 * 1024 * 1024,
            allocation_operations: 1024,
            source_bytes_read: 1024 * 1024,
            bytes_copied: 1024 * 1024,
            bytes_hashed: 1024 * 1024,
            bytes_encoded: 1024 * 1024,
            items_examined: 1024,
            ..Default::default()
        },
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let expected = if foreign_bytes {
        crate::wire::ErrorCode::Unauthorized
    } else {
        Invalid
    };
    assert!(matches!(
        attachments::resolve(&message, &resolver, 1, Some(&io)).await,
        Err(Error::NativeBoundary { code, .. }) if code == expected
    ));
    assert_eq!(resolver.ordinary.load(Ordering::SeqCst), 0);
    assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
    work.cancellation().cancel();
    assert!(
        attachments::resolve(&message, &resolver, 1, Some(&io))
            .await
            .is_err()
    );
    assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
    Ok(())
}

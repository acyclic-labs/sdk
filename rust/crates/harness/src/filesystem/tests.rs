use super::*;

use crate::core::{AggregateKind, Authority, AuthorityIssuer};
use crate::{
    AgentId, Capabilities,
    conversation::{VolumeClass, VolumeOwner},
};

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_verifier_copies_authentic_scope_and_keeps_custody_until_drop() -> Result<()> {
    let provider = ProviderRef::new(
        "native-verifier-test".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &(provider), None)?;
    let issuer = AuthorityIssuer::new(
        "native-verifier-test",
        [4; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "parent".into(),
        },
    );
    let scope = issuer
        .issue(
            "parent-scope",
            &Capabilities::new([capability::FORK_PUBLISH, capability::PROJECT_MERGE]),
            Some(AgentId::from_bytes([8; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let verifier = FilesystemForkVerifier::new(
        Arc::clone(&host),
        4096,
        None,
        Some((
            &issuer.verifier(&mut None).expect("verifier copy"),
            &scope,
            &crate::filesystem::PreparationIo::from_native_work(&work)?,
        )),
    )?;
    let other = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    assert!(matches!(
        verifier
            .native_binding
            .as_ref()
            .unwrap()
            .require_native_work(&other),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    let forged_issuer = AuthorityIssuer::new(
        "native-verifier-test",
        [5; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "parent".into(),
        },
    );
    assert!(
        FilesystemProjectMergeVerifier::new(
            host,
            Some((
                &forged_issuer.verifier(&mut None).expect("verifier copy"),
                &scope,
                &PreparationIo::from_native_work(&work)?
            ))
        )
        .is_err()
    );
    drop(verifier);
    Ok(())
}

#[tokio::test]
async fn resolves_exact_memory_generation_and_rejects_foreign_provider() -> Result<()> {
    let filesystem = Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "adapter-test",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(map_error)?;
    let expected = workspace
        .head(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(map_error)?;
    let provider = ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let reference = workspace_ref(provider, "adapter-test")?;
    let observed = host.resolve(&reference).await?;
    assert_eq!(
        observed.generation.as_resource().key(),
        expected.id().digest().into_bytes()
    );

    let foreign = workspace_ref(
        ProviderRef::new("other".into(), "filesystem".into(), "1".into(), &mut None)?,
        "adapter-test",
    )?;
    assert!(matches!(
        host.resolve(&foreign).await,
        Err(Error::Invalid(_))
    ));

    let updated = host
        .apply(
            &reference,
            Some(&observed.generation),
            &[WorkspaceMutation::PutFile {
                path: "/hello.txt".into(),
                bytes: b"hello".to_vec().into(),
            }],
            &IdempotencyKey::new("write-hello")?,
        )
        .await?;
    assert_eq!(
        host.read(&reference, Some(&updated), "/hello.txt", 16)
            .await?,
        acyclic_stream::OwnedBytes::from_static(b"hello")
    );
    assert!(matches!(
        host.apply(
            &reference,
            Some(&observed.generation),
            &[WorkspaceMutation::PutFile {
                path: "/hello.txt".into(),
                bytes: b"changed".to_vec().into(),
            }],
            &IdempotencyKey::new("stale-write")?,
        )
        .await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn staged_content_is_version_pinned_and_requires_exact_grants() -> Result<()> {
    let filesystem = Fs::memory();
    let provider = ProviderRef::new("example".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let owner = AgentId::from_bytes([3; 16]);
    let volume = VolumeRef::new(
        provider,
        "agent-private-a".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "test",
        [4; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "test".into(),
        },
    );
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(owner),
            None,
            &mut None,
        )
        .expect("scope proof");
    let verifier = issuer.verifier(&mut None).expect("verifier copy");
    let read = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Read,
        u64::MAX,
        &mut None,
    )?;
    let write = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let first = host
        .put_content(
            &volume,
            &write,
            "messages/first.txt",
            b"one",
            "text/plain",
            "first.txt",
            16,
            &IdempotencyKey::new("stage-first")?,
            None,
            None,
        )
        .await?;
    let second = host
        .put_content(
            &volume,
            &write,
            "messages/first.txt",
            b"two",
            "text/plain",
            "first.txt",
            16,
            &IdempotencyKey::new("stage-second")?,
            None,
            None,
        )
        .await?;
    assert_eq!(
        host.put_content(
            &volume,
            &write,
            "messages/first.txt",
            b"one",
            "text/plain",
            "first.txt",
            16,
            &IdempotencyKey::new("stage-first")?,
            None,
            None
        )
        .await?,
        first
    );
    for (path, media_type, display_name) in [
        (
            "messages/first.txt",
            "application/octet-stream",
            "first.txt",
        ),
        ("messages/first.txt", "text/plain", "renamed.txt"),
        ("messages/other.txt", "text/plain", "first.txt"),
    ] {
        assert!(
            host.put_content(
                &volume,
                &write,
                path,
                b"one",
                media_type,
                display_name,
                16,
                &IdempotencyKey::new("stage-first")?,
                None,
                None
            )
            .await
            .is_err()
        );
    }
    assert_ne!(first.version(), second.version());
    assert_eq!(
        host.read_content(&first, &read, 16, None).await?,
        acyclic_stream::OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        host.read_content(&second, &read, 16, None).await?,
        acyclic_stream::OwnedBytes::from_static(b"two")
    );
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let native = FilesystemForkVerifier::new(
            Arc::clone(&host),
            16,
            None,
            Some((
                &verifier,
                &scope,
                &crate::filesystem::PreparationIo::from_native_work(&work)?,
            )),
        )?;
        let io = PreparationIo::from_native_work(&work)?;
        let binding = native.native_binding.as_ref().unwrap();
        binding.require_file(&first, &io)?;
        let bytes = host.read_pinned_io(&first, 16, Some(&io)).await?;
        assert_eq!(bytes.as_ref(), b"one");
        drop(bytes);
        binding.require_file(&second, &io)?;
        let bytes = host.read_pinned_io(&second, 16, Some(&io)).await?;
        assert_eq!(bytes.as_ref(), b"two");
        drop(bytes);
        let tampered = FileRef::new(
            first.volume().clone(),
            first.path().into(),
            first.version().into(),
            second.descriptor().clone(),
            first.display_name().into(),
            &mut None,
        )?;
        assert!(host.read_pinned_io(&tampered, 16, Some(&io)).await.is_err());
        let denied_scope = issuer
            .issue(
                "no-files",
                &Capabilities::default(),
                Some(owner),
                None,
                &mut None,
            )
            .expect("scope proof");
        let denied = FilesystemForkVerifier::new(
            Arc::clone(&host),
            16,
            None,
            Some((
                &verifier,
                &denied_scope,
                &crate::filesystem::PreparationIo::from_native_work(&work)?,
            )),
        )?;
        let before = work.receipt().backend_read_operations;
        assert!(matches!(
            denied
                .native_binding
                .as_ref()
                .unwrap()
                .require_file(&first, &io),
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));
        assert_eq!(work.receipt().backend_read_operations, before);
        let before = work.receipt();
        cancellation.cancel();
        assert!(binding.authenticate(&work).is_err());
        assert_eq!(
            work.receipt().allocation_operations,
            before.allocation_operations
        );
    }
    let delegated_scope = issuer.delegate_private_directory_read(
        &scope,
        AgentId::from_bytes([8; 16]),
        "reader",
        &volume,
        "messages",
    )?;
    let delegated_reader =
        FilesystemContentVerifier::new(host.clone(), &verifier, &delegated_scope, 16, None)?;
    let (listed_generation, page) = delegated_reader
        .list_private_directory(&volume, "messages", "messages", None, None, 10)
        .await?;
    assert_eq!(page.entries.len(), 1);
    let erased: &dyn ContentResidencyVerifier = delegated_reader.as_ref();
    let portable_page = erased
        .list_private_directory(
            &volume,
            "messages",
            "messages",
            Some(&listed_generation),
            None,
            10,
            None,
        )?
        .await?;
    assert_eq!(portable_page.generation, listed_generation);
    assert_eq!(
        portable_page.entries,
        vec![PrivateDirectoryEntry {
            name: "first.txt".into(),
            kind: PrivateDirectoryEntryKind::File,
        }]
    );
    let (portable_ref, portable_bytes, _portable_hold) = erased
        .read_private_path(
            &volume,
            "messages",
            "messages/first.txt",
            Some(&listed_generation),
            16,
            None,
        )?
        .await?;
    assert_eq!(portable_ref, second);
    assert_eq!(portable_bytes.as_ref(), b"two");
    let (pinned, delegated_bytes) = delegated_reader
        .read_private_path(
            &volume,
            "messages",
            "messages/first.txt",
            Some(&listed_generation),
            u64::MAX,
            None,
        )?
        .await
        .map(|(file, bytes, _)| (file, bytes))?;
    assert_eq!(pinned, second);
    assert_eq!(delegated_bytes.as_ref(), b"two");
    host.put_content(
        &volume,
        &write,
        "messages/later.txt",
        b"later",
        "text/plain",
        "later.txt",
        16,
        &IdempotencyKey::new("stage-later")?,
        None,
        None,
    )
    .await?;
    let (old_generation, old_page) = delegated_reader
        .list_private_directory(
            &volume,
            "messages",
            "messages",
            Some(&listed_generation),
            None,
            10,
        )
        .await?;
    assert_eq!(old_generation, listed_generation);
    assert_eq!(old_page.entries, page.entries);
    let (_, current_page) = delegated_reader
        .list_private_directory(&volume, "messages", "messages", None, None, 10)
        .await?;
    assert_eq!(current_page.entries.len(), 2);
    let (old_ref, old_bytes) = delegated_reader
        .read_private_path(
            &volume,
            "messages",
            "messages/first.txt",
            Some(&listed_generation),
            u64::MAX,
            None,
        )?
        .await
        .map(|(file, bytes, _)| (file, bytes))?;
    assert_eq!(old_ref, second);
    assert_eq!(old_bytes.as_ref(), b"two");
    assert!(
        delegated_reader
            .list_private_directory(&volume, "messages", "elsewhere", None, None, 10)
            .await
            .is_err()
    );
    let delegated = ContentGrant::verify_directory_read(
        &verifier,
        &delegated_scope,
        &volume,
        "messages",
        &mut None,
    )?;
    let (discovered, bytes) = host
        .read_private_path(&volume, &delegated, "messages/first.txt", None, 16, None)
        .await?;
    assert_eq!(bytes.as_ref(), b"two");
    assert_ne!(discovered.version(), second.version());
    assert_eq!(discovered.path(), second.path());
    assert_eq!(discovered.descriptor(), second.descriptor());
    assert!(
        host.put_content(
            &volume,
            &write,
            ".system/forged.json",
            b"bad",
            "application/json",
            "forged.json",
            16,
            &IdempotencyKey::new("internal-write")?,
            None,
            None
        )
        .await
        .is_err()
    );
    let empty = host
        .put_content(
            &volume,
            &write,
            "messages/empty.txt",
            b"",
            "text/plain",
            "empty.txt",
            16,
            &IdempotencyKey::new("stage-empty")?,
            None,
            None,
        )
        .await?;
    assert!(matches!(
        delegated_reader
            .read_private_path(
                &volume,
                "messages",
                "messages/empty.txt",
                Some(&listed_generation),
                u64::MAX,
                None
            )?
            .await
            .map(|(file, bytes, _)| (file, bytes)),
        Err(Error::NotFound(_))
    ));
    assert!(host.read_content(&empty, &read, 16, None).await?.is_empty());
    let wrong_digest = FileRef::new(
        volume.clone(),
        first.path().into(),
        first.version().into(),
        FileDescriptor::new([0; 32], 3, "text/plain".into())?,
        first.display_name().into(),
        &mut None,
    )?;
    assert!(
        host.read_content(&wrong_digest, &read, 16, None)
            .await
            .is_err()
    );
    assert!(
        host.put_content(
            &volume,
            &write,
            "../escape",
            b"bad",
            "text/plain",
            "escape",
            16,
            &IdempotencyKey::new("invalid-path")?,
            None,
            None
        )
        .await
        .is_err()
    );
    assert!(host.read_content(&first, &write, 16, None).await.is_err());
    let other = VolumeRef::new(
        volume.provider().clone(),
        "agent-private-b".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([5; 16])),
        &mut None,
    )?;
    assert!(
        ContentGrant::verify(
            &verifier,
            &scope,
            &other,
            VolumeOperation::Read,
            u64::MAX,
            &mut None
        )
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn conversation_stream_admission_requires_resident_files() -> Result<()> {
    use crate::conversation::{Attachment, ConversationMessage, MessageKind};
    use crate::core::{Action, Command, SchemaRegistry};
    use crate::store::StreamAggregate;
    use crate::{IdempotencyKey, OperationId};
    use acyclic_stream::MemoryStream;
    use std::{collections::BTreeMap, sync::Arc};

    let filesystem = Fs::memory();
    let provider = ProviderRef::new("example".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let agent = AgentId::from_bytes([7; 16]);
    let volume = VolumeRef::new(
        provider,
        "scratch".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "integration".into(),
    };
    let issuer = AuthorityIssuer::new("integration", [11; 32], authority.clone());
    let scope = issuer
        .issue(
            "agent",
            &Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Read,
        u64::MAX,
        &mut None,
    )?;
    let write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let file = host
        .put_content(
            &volume,
            &write,
            "messages/input.txt",
            b"hello",
            "text/plain",
            "input.txt",
            32,
            &IdempotencyKey::new("input-upload")?,
            None,
            None,
        )
        .await?;
    let manifest = host
        .put_attachment_manifest(
            &volume,
            &write,
            "attachments/list.json",
            &[Attachment {
                file: file.clone(),
                label: Some("primary copy".into()),
            }],
            4_096,
            &IdempotencyKey::new("attachment-list")?,
        )
        .await?;
    let verifier = FilesystemContentVerifier::new(
        Arc::new(host),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        4_096,
        None,
    )?;
    let stream = Arc::new(MemoryStream::default());
    let mut aggregate = StreamAggregate::open(
        &stream,
        authority,
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?
    .with_content_verifier(verifier);
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes([1; 16]),
                idempotency_key: IdempotencyKey::new("bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let message = ConversationMessage {
        id: uuid::Uuid::from_bytes([2; 16]),
        sequence: 1,
        kind: MessageKind::User,
        content: file.clone(),
        attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: BTreeMap::new(),
    };
    let mut missing = message.clone();
    missing.content = FileRef::new(
        volume,
        "messages/missing.txt".into(),
        file.version().into(),
        file.descriptor().clone(),
        "missing.txt".into(),
        &mut None,
    )?;
    let command = |id: u8, value: ConversationMessage| -> Result<Command> {
        Ok(Command {
            operation_id: OperationId::from_bytes([id; 16]),
            idempotency_key: IdempotencyKey::new(format!("append-{id}"))?,
            expected_revision: 1,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(value),
            },
        })
    };
    assert!(
        aggregate
            .execute(
                command(3, missing)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), 1);
    let mut missing_attachment = message.clone();
    missing_attachment.attachments = crate::conversation::ReferencedAttachments::Inline {
        items: vec![Attachment {
            file: FileRef::new(
                file.volume().clone(),
                "attachments/missing.bin".into(),
                file.version().into(),
                file.descriptor().clone(),
                "missing.bin".into(),
                &mut None,
            )?,
            label: None,
        }],
    };
    assert!(
        aggregate
            .execute(
                command(5, missing_attachment)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), 1);
    aggregate
        .execute(
            command(4, message.clone())?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert_eq!(
        aggregate
            .reducer()
            .conversation()
            .map(|state| state.messages.as_slice()),
        Some(&[message][..])
    );
    let manifest_message = ConversationMessage {
        id: uuid::Uuid::from_bytes([6; 16]),
        sequence: 2,
        kind: MessageKind::User,
        content: file.clone(),
        attachments: manifest.clone(),
        reply_to: None,
        tool_call_id: None,
        extensions: BTreeMap::new(),
    };
    let mut bad_count = manifest_message.clone();
    if let ReferencedAttachments::Manifest { item_count, .. } = &mut bad_count.attachments {
        *item_count += 1;
    }
    assert!(
        aggregate
            .execute(
                Command {
                    operation_id: OperationId::from_bytes([7; 16]),
                    idempotency_key: IdempotencyKey::new("manifest-bad-count")?,
                    expected_revision: 2,
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::AppendConversationMessage {
                        message: Box::new(bad_count)
                    },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await
            .is_err()
    );
    assert_eq!(aggregate.reducer().revision(), 2);
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes([8; 16]),
                idempotency_key: IdempotencyKey::new("manifest-valid")?,
                expected_revision: 2,
                scope,
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(manifest_message),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    assert_eq!(
        aggregate
            .reducer()
            .conversation()
            .map(|state| state.messages.len()),
        Some(2)
    );
    assert!(
        read.require(file.volume(), VolumeOperation::Read, None)
            .is_ok()
    );
    Ok(())
}

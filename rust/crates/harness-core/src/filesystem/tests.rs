use super::*;

use crate::core::{AggregateKind, Authority, AuthorityRoot};
use crate::{
    AgentId, Capabilities,
    conversation::{VolumeClass, VolumeOwner},
};

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
    let observed = host.resolve(&reference, None).await?;
    assert_eq!(
        observed.generation.as_resource().key(),
        expected.id().digest().into_bytes()
    );

    let foreign = workspace_ref(
        ProviderRef::new("other".into(), "filesystem".into(), "1".into(), &mut None)?,
        "adapter-test",
    )?;
    assert!(matches!(
        host.resolve(&foreign, None).await,
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
        host.read(&reference, Some(&updated), "/hello.txt", 16, None)
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
    let issuer = AuthorityRoot::new(
        "test",
        [4; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "test".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
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
            crate::conversation::ContentAccess::default(),
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
            crate::conversation::ContentAccess::default(),
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
            crate::conversation::ContentAccess::default()
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
                crate::conversation::ContentAccess::default()
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
        .list_private_directory(
            &volume,
            "messages",
            "messages",
            crate::filesystem::DirectoryOptions {
                generation: None,
                after: None,
                maximum_entries: 10,
                ceiling: None,
            },
        )
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
            crate::conversation::ContentAccess::default(),
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
            crate::conversation::ContentAccess::default(),
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
        crate::conversation::ContentAccess::default(),
    )
    .await?;
    let (old_generation, old_page) = delegated_reader
        .list_private_directory(
            &volume,
            "messages",
            "messages",
            crate::filesystem::DirectoryOptions {
                generation: Some(&listed_generation),
                after: None,
                maximum_entries: 10,
                ceiling: None,
            },
        )
        .await?;
    assert_eq!(old_generation, listed_generation);
    assert_eq!(old_page.entries, page.entries);
    let (_, current_page) = delegated_reader
        .list_private_directory(
            &volume,
            "messages",
            "messages",
            crate::filesystem::DirectoryOptions {
                generation: None,
                after: None,
                maximum_entries: 10,
                ceiling: None,
            },
        )
        .await?;
    assert_eq!(current_page.entries.len(), 2);
    let (old_ref, old_bytes) = delegated_reader
        .read_private_path(
            &volume,
            "messages",
            "messages/first.txt",
            Some(&listed_generation),
            u64::MAX,
            crate::conversation::ContentAccess::default(),
        )?
        .await
        .map(|(file, bytes, _)| (file, bytes))?;
    assert_eq!(old_ref, second);
    assert_eq!(old_bytes.as_ref(), b"two");
    assert!(
        delegated_reader
            .list_private_directory(
                &volume,
                "messages",
                "elsewhere",
                crate::filesystem::DirectoryOptions {
                    generation: None,
                    after: None,
                    maximum_entries: 10,
                    ceiling: None
                }
            )
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
        .read_private_path(
            &volume,
            &delegated,
            "messages/first.txt",
            None,
            16,
            crate::conversation::ContentAccess::default(),
        )
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
            crate::conversation::ContentAccess::default()
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
            crate::conversation::ContentAccess::default(),
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
                crate::conversation::ContentAccess::default()
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
            crate::conversation::ContentAccess::default()
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

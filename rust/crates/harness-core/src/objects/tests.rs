use super::*;
use crate::{
    AgentId, Capabilities,
    conversation::{ContentMountResolver, VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityRoot},
    resources::ProviderRef,
};
use acyclic_objects::MemoryObjects;

#[tokio::test]
async fn an_upload_retry_naming_other_content_is_refused() -> Result<()> {
    let objects = Arc::new(MemoryObjects::memory());
    let owner = AgentId::from_bytes([7; 16]);
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "objects".into(), "1".into(), &mut None)?,
        "harness-content".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let issuer = AuthorityRoot::new(
        "objects-test",
        [9; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "objects-test".into(),
        },
    );
    let owner_scope = issuer.bootstrap_scope(
        "owner",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner),
        &mut None,
    )?;
    let owner_store = ObjectContentStore::new(
        objects,
        volume.provider().clone(),
        volume.clone(),
        (
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope.clone(),
        ),
        owner_scope,
        4_096,
    )?;
    let stage = |operation: u8, path: &'static str, bytes: &'static [u8]| {
        owner_store.stage(
            OperationId::from_bytes([operation; 16]),
            path,
            bytes,
            "text/plain",
            "one.txt",
            crate::conversation::ContentAccess::default(),
        )
    };
    let file = stage(10, "notes/one.txt", b"owner bytes")?.await?.0;
    assert_eq!(stage(10, "notes/one.txt", b"owner bytes")?.await?.0, file);
    for (path, bytes) in [
        ("notes/one.txt", &b"other bytes"[..]),
        ("notes/two.txt", b"owner bytes"),
    ] {
        let conflicting = stage(10, path, bytes)?.await;
        assert!(matches!(conflicting, Err(Error::Conflict(_))));
    }
    assert_eq!(stage(42, "notes/one.txt", b"owner bytes")?.await?.0, file);
    Ok(())
}
#[tokio::test]
async fn lazy_mount_routes_registered_volumes_and_requires_exact_grant() -> Result<()> {
    let objects = Arc::new(MemoryObjects::memory());
    let name_a = "harness-owner-a";
    let name_b = "harness-owner-b";
    let owner_a = AgentId::from_bytes([31; 16]);
    let owner_b = AgentId::from_bytes([32; 16]);
    let attached = AgentId::from_bytes([33; 16]);
    let issuer = AuthorityRoot::new(
        "objects-mount-test",
        [34; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "objects-mount-test".into(),
        },
    );
    let provider = ProviderRef::new("local".into(), "objects".into(), "1".into(), &mut None)?;
    let volume_a = VolumeRef::new(
        provider.clone(),
        name_a.into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner_a),
        &mut None,
    )?;
    let volume_b = VolumeRef::new(
        provider.clone(),
        name_b.into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner_b),
        &mut None,
    )?;
    let owner_scope_a = issuer.bootstrap_scope(
        "owner-a",
        &Capabilities::new([
            volume_a.capability(VolumeOperation::Read, &mut None)?,
            volume_a.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner_a),
        &mut None,
    )?;
    let owner_scope_b = issuer.bootstrap_scope(
        "owner-b",
        &Capabilities::new([
            volume_b.capability(VolumeOperation::Read, &mut None)?,
            volume_b.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner_b),
        &mut None,
    )?;
    let owner_store_a = ObjectContentStore::new(
        objects.clone(),
        provider.clone(),
        volume_a.clone(),
        (
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_a.clone(),
        ),
        owner_scope_a.clone(),
        4_096,
    )?;
    let owner_store_b = ObjectContentStore::new(
        objects.clone(),
        provider.clone(),
        volume_b.clone(),
        (
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_b.clone(),
        ),
        owner_scope_b.clone(),
        4_096,
    )?;
    let file_a = owner_store_a
        .stage(
            OperationId::from_bytes([35; 16]),
            "a.txt",
            b"bucket a",
            "text/plain",
            "a.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .0;
    let file_b = owner_store_b
        .stage(
            OperationId::from_bytes([36; 16]),
            "b.txt",
            b"bucket b",
            "text/plain",
            "b.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .0;
    let attached_a_scope =
        issuer.delegate_private_file_read(&owner_scope_a, attached, "attached-a", &file_a)?;
    let attached_b_scope =
        issuer.delegate_private_file_read(&owner_scope_b, attached, "attached-b", &file_b)?;
    let attached_a = ObjectContentStore::new(
        objects.clone(),
        provider.clone(),
        volume_a.clone(),
        (
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_a.clone(),
        ),
        attached_a_scope,
        4_096,
    )?;
    let attached_b = ObjectContentStore::new(
        objects.clone(),
        provider.clone(),
        volume_b.clone(),
        (
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_b.clone(),
        ),
        attached_b_scope,
        4_096,
    )?;
    let resolver = ObjectContentMountResolver::new(vec![attached_a, attached_b])?;
    let (grant_a, reader_a) = resolver.mount(&file_a).await?;
    grant_a.require_file_read(&file_a, None)?;
    assert_eq!(
        reader_a
            .read(&file_a, file_a.descriptor().byte_length(), None, None)?
            .await?
            .as_slice(),
        b"bucket a"
    );
    let (grant_b, reader_b) = resolver.mount(&file_b).await?;
    grant_b.require_file_read(&file_b, None)?;
    assert_eq!(
        reader_b
            .read(&file_b, file_b.descriptor().byte_length(), None, None)?
            .await?
            .as_slice(),
        b"bucket b"
    );

    // A whole-volume grant on A never reads B's bytes through a reference
    // rebuilt on A: the shared account holds them, but A has no record of them.
    for path in ["b.txt", "a.txt"] {
        let forged = FileRef::new(
            volume_a.clone(),
            path.into(),
            file_b.version().into(),
            file_b.descriptor().clone(),
            file_b.display_name().into(),
            &mut None,
        )?;
        assert!(matches!(
            owner_store_a
                .read(&forged, forged.descriptor().byte_length(), None, None)?
                .await,
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            owner_store_a
                .read_range(
                    &forged,
                    acyclic_fs::ByteRange {
                        offset: 1,
                        length: 3
                    },
                    None,
                    None
                )?
                .await,
            Err(Error::Unauthorized(_))
        ));
    }

    let no_grant = issuer.bootstrap_scope(
        "no-grant",
        &Capabilities::new([] as [String; 0]),
        Some(attached),
        &mut None,
    )?;
    let denied_store = ObjectContentStore::new(
        objects,
        provider,
        volume_a,
        (
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_a,
        ),
        no_grant,
        4_096,
    )?;
    let denied = ObjectContentMountResolver::new(vec![denied_store])?;
    assert!(denied.mount(&file_a).await.is_err());
    Ok(())
}

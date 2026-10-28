use super::*;
use crate::{
    AgentId, Capabilities,
    conversation::{ContentMountResolver, VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityRoot},
    resources::ProviderRef,
};
use acyclic_objects::MemoryObjects;
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

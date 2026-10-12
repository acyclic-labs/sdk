use super::*;
use crate::{
    AgentId, Capabilities,
    conversation::{ContentMountResolver, Limits, VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
    resources::ProviderRef,
};
use acyclic_objects::v1::{MemoryObjects, MemoryOptions};

#[tokio::test]
async fn staged_objects_are_exact_and_attached_readers_never_gain_write() -> Result<()> {
    let objects = Arc::new(MemoryObjects::new(MemoryOptions::default()).map_err(storage)?);
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "harness-content".into(),
            mutation: None,
        })
        .await
        .map_err(storage)?
        .bucket
        .ok_or_else(|| Error::Storage("bucket identity is missing".into()))?;
    let owner = AgentId::from_bytes([7; 16]);
    let reader = AgentId::from_bytes([8; 16]);
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "objects".into(), "1".into(), &mut None)?,
        bucket.name.clone(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "objects-test",
        [9; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "objects-test".into(),
        },
    );
    let owner_scope = issuer.issue(
        "owner",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner),
        None,
        &mut None,
    )?;
    let provider = volume.provider().clone();
    let owner_store = ObjectContentStore::new(
        objects.clone(),
        bucket.clone(),
        provider.clone(),
        volume.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        owner_scope.clone(),
        owner_scope.clone(),
        4_096,
    )
    .await?;
    let file = owner_store
        .stage(
            OperationId::from_bytes([10; 16]),
            "notes/one.txt",
            b"owner bytes",
            "text/plain",
            "one.txt",
            None,
        )?
        .await?
        .0;
    assert_eq!(
        owner_store
            .stage(
                OperationId::from_bytes([10; 16]),
                "notes/one.txt",
                b"owner bytes",
                "text/plain",
                "one.txt",
                None
            )?
            .await?
            .0,
        file
    );
    assert_eq!(
        owner_store
            .read(&file, file.descriptor().byte_length(), None)?
            .await?
            .as_slice(),
        b"owner bytes"
    );
    let duplicate = owner_store
        .stage(
            OperationId::from_bytes([42; 16]),
            "notes/one.txt",
            b"owner bytes",
            "text/plain",
            "one.txt",
            None,
        )?
        .await?
        .0;
    assert_eq!(duplicate, file);
    assert!(
        owner_store
            .stage(
                OperationId::from_bytes([42; 16]),
                "notes/other.txt",
                b"owner bytes",
                "text/plain",
                "one.txt",
                None
            )?
            .await
            .is_err()
    );
    let artifact = owner_store.artifact_ref(&file)?;
    owner_store.verify_artifact(&artifact).await?;
    let missing_version = ArtifactRef::new(
        provider.clone(),
        artifact.as_resource().key(),
        Some("missing-version".into()),
    )?;
    assert!(owner_store.verify_artifact(&missing_version).await.is_err());
    let unpinned = ArtifactRef::new(provider.clone(), artifact.as_resource().key(), None)?;
    owner_store.verify_artifact(&unpinned).await?;
    let traversal = ArtifactRef::new(
        provider.clone(),
        format!("{}/../outside", owner_store.volume.storage_name(&mut None)?).into_bytes(),
        Some(file.version().into()),
    )?;
    assert!(matches!(
        owner_store.verify_artifact(&traversal).await,
        Err(Error::Invalid(_))
    ));
    let mut request: ForkRequest =
        serde_json::from_str(include_str!("../../fixtures/v2/fork-request.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    request.preparation.maximum_inherited_messages = 1;
    request.preparation.maximum_inherited_bytes = 1_024;
    request.preparation.maximum_inherited_references = 1;
    let artifact_selection = ForkSelection {
        required: false,
        revision: ResourceRevision::Artifact(artifact.clone()),
    };
    request.selections.push(artifact_selection.clone());
    request.validate(&mut None)?;
    let captured = CapturedResource {
        source: artifact_selection.revision.clone(),
        revision: artifact_selection.revision.clone(),
    };
    assert_eq!(
        owner_store.capture(&request, &artifact_selection).await?,
        Capture::Captured(captured.clone())
    );
    let mut seed: ForkSeed = serde_json::from_str(include_str!("../../fixtures/v2/fork-seed.json"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    seed.resources.push(captured);
    ForkSeedVerifier::verify(owner_store.as_ref(), &seed, &mut None)?.await?;
    assert!(owner_store.bindings().writer.is_some());
    let items = vec![Attachment {
        file: file.clone(),
        label: None,
    }];
    let manifest_bytes =
        serde_json::to_vec(&items).map_err(|error| Error::Invalid(error.to_string()))?;
    let manifest = owner_store
        .stage(
            OperationId::from_bytes([12; 16]),
            "lists/one.json",
            &manifest_bytes,
            "application/vnd.acyclic.harness.attachments+json",
            "one.json",
            None,
        )?
        .await?
        .0;
    owner_store
        .verify_manifest(&manifest, 1, &Limits::default(), None)?
        .await?;
    assert_eq!(
        crate::conversation::load_manifest(owner_store.as_ref(), &manifest, 1, None)
            .await?
            .0,
        items
    );
    assert!(
        crate::conversation::load_manifest(owner_store.as_ref(), &manifest, 2, None)
            .await
            .is_err()
    );
    let delegated = issuer.delegate_private_file_read(&owner_scope, reader, "attached", &file)?;
    let attached = ObjectContentStore::new(
        objects.clone(),
        bucket.clone(),
        provider.clone(),
        volume.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        owner_scope.clone(),
        delegated,
        4_096,
    )
    .await?;
    assert_eq!(
        attached
            .read(&file, file.descriptor().byte_length(), None)?
            .await?
            .as_slice(),
        b"owner bytes"
    );
    assert!(attached.bindings().writer.is_none());
    assert!(
        attached
            .stage(
                OperationId::from_bytes([11; 16]),
                "notes/denied.txt",
                b"denied",
                "text/plain",
                "denied.txt",
                None
            )?
            .await
            .is_err()
    );
    assert!(
        owner_store
            .stage(
                OperationId::from_bytes([10; 16]),
                "notes/one.txt",
                b"owner bytes",
                "text/plain",
                "changed.txt",
                None
            )?
            .await
            .is_err()
    );
    assert!(
        owner_store
            .stage(
                OperationId::from_bytes([10; 16]),
                "notes/one.txt",
                b"other bytes",
                "text/plain",
                "one.txt",
                None
            )?
            .await
            .is_err()
    );
    assert!(
        owner_store
            .stage(
                OperationId::from_bytes([10; 16]),
                "notes/two.txt",
                b"owner bytes",
                "text/plain",
                "one.txt",
                None
            )?
            .await
            .is_err()
    );
    let impostor = AuthorityIssuer::new(
        "impostor",
        [13; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "impostor".into(),
        },
    );
    let impostor_scope = impostor.issue(
        "forged",
        &Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]),
        Some(reader),
        None,
        &mut None,
    )?;
    assert!(
        ObjectContentStore::new(
            objects.clone(),
            bucket.clone(),
            provider.clone(),
            volume.clone(),
            impostor.verifier(&mut None).expect("verifier copy"),
            owner_scope.clone(),
            impostor_scope,
            4_096
        )
        .await
        .is_err()
    );
    assert!(
        ObjectContentStore::new(
            objects.clone(),
            bucket.clone(),
            ProviderRef::new("impostor".into(), "objects".into(), "1".into(), &mut None)?,
            volume.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope.clone(),
            owner_scope,
            4_096
        )
        .await
        .is_err()
    );
    let wrong = FileRef::new(
        volume,
        "notes/one.txt".into(),
        (file.version()).into(),
        FileDescriptor::from_bytes(b"other", "text/plain", &mut None)?,
        "one.txt".into(),
        &mut None,
    )?;
    assert!(
        owner_store
            .read(&wrong, wrong.descriptor().byte_length(), None)?
            .await
            .is_err()
    );
    let replacement = owner_store
        .stage(
            OperationId::from_bytes([19; 16]),
            "notes/one.txt",
            b"replacement bytes",
            "text/plain",
            "one.txt",
            None,
        )?
        .await?
        .0;
    assert_ne!(replacement.version(), file.version());
    assert_eq!(
        owner_store
            .read(&file, file.descriptor().byte_length(), None)?
            .await?
            .as_slice(),
        b"owner bytes"
    );
    assert_eq!(
        owner_store
            .read(&replacement, replacement.descriptor().byte_length(), None)?
            .await?
            .as_slice(),
        b"replacement bytes"
    );
    let replacement_artifact = owner_store.artifact_ref(&replacement)?;
    assert!(replacement_artifact.as_resource().version().is_none());
    owner_store.verify_artifact(&replacement_artifact).await?;
    owner_store
        .objects
        .put(
            wire::PutObjectHeader {
                bucket: Some(owner_store.bucket.clone()),
                object_key: String::from_utf8(replacement_artifact.as_resource().key().to_vec())
                    .map_err(|_| Error::Invalid("invalid test key".into()))?,
                metadata: Some(wire::ObjectMetadata {
                    content_type: "text/plain".into(),
                    user: [("harness-display-name".into(), "one.txt".into())].into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            bytes::Bytes::from_static(b"corrupt replacement"),
        )
        .await
        .map_err(storage)?;
    assert!(
        owner_store
            .verify_artifact(&replacement_artifact)
            .await
            .is_err()
    );
    assert!(
        owner_store
            .read(&replacement, replacement.descriptor().byte_length(), None)?
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn lazy_mount_routes_registered_buckets_and_requires_exact_grant() -> Result<()> {
    let objects = Arc::new(MemoryObjects::new(MemoryOptions::default()).map_err(storage)?);
    let bucket_a = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "harness-owner-a".into(),
            mutation: None,
        })
        .await
        .map_err(storage)?
        .bucket
        .ok_or_else(|| Error::Storage("bucket identity is missing".into()))?;
    let bucket_b = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "harness-owner-b".into(),
            mutation: None,
        })
        .await
        .map_err(storage)?
        .bucket
        .ok_or_else(|| Error::Storage("bucket identity is missing".into()))?;
    let owner_a = AgentId::from_bytes([31; 16]);
    let owner_b = AgentId::from_bytes([32; 16]);
    let attached = AgentId::from_bytes([33; 16]);
    let issuer = AuthorityIssuer::new(
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
        bucket_a.name.clone(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner_a),
        &mut None,
    )?;
    let volume_b = VolumeRef::new(
        provider.clone(),
        bucket_b.name.clone(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner_b),
        &mut None,
    )?;
    let owner_scope_a = issuer.issue(
        "owner-a",
        &Capabilities::new([
            volume_a.capability(VolumeOperation::Read, &mut None)?,
            volume_a.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner_a),
        None,
        &mut None,
    )?;
    let owner_scope_b = issuer.issue(
        "owner-b",
        &Capabilities::new([
            volume_b.capability(VolumeOperation::Read, &mut None)?,
            volume_b.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner_b),
        None,
        &mut None,
    )?;
    let owner_store_a = ObjectContentStore::new(
        objects.clone(),
        bucket_a.clone(),
        provider.clone(),
        volume_a.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        owner_scope_a.clone(),
        owner_scope_a.clone(),
        4_096,
    )
    .await?;
    let owner_store_b = ObjectContentStore::new(
        objects.clone(),
        bucket_b.clone(),
        provider.clone(),
        volume_b.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        owner_scope_b.clone(),
        owner_scope_b.clone(),
        4_096,
    )
    .await?;
    let file_a = owner_store_a
        .stage(
            OperationId::from_bytes([35; 16]),
            "a.txt",
            b"bucket a",
            "text/plain",
            "a.txt",
            None,
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
            None,
        )?
        .await?
        .0;
    let attached_a_scope =
        issuer.delegate_private_file_read(&owner_scope_a, attached, "attached-a", &file_a)?;
    let attached_b_scope =
        issuer.delegate_private_file_read(&owner_scope_b, attached, "attached-b", &file_b)?;
    let attached_a = ObjectContentStore::new(
        objects.clone(),
        bucket_a.clone(),
        provider.clone(),
        volume_a.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        owner_scope_a.clone(),
        attached_a_scope,
        4_096,
    )
    .await?;
    let attached_b = ObjectContentStore::new(
        objects.clone(),
        bucket_b.clone(),
        provider.clone(),
        volume_b.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        owner_scope_b.clone(),
        attached_b_scope,
        4_096,
    )
    .await?;
    let resolver = ObjectContentMountResolver::new(vec![attached_a, attached_b])?;
    let (grant_a, reader_a) = resolver.mount(&file_a).await?;
    grant_a.require_file_read(&file_a, None)?;
    assert_eq!(
        reader_a
            .read(&file_a, file_a.descriptor().byte_length(), None)?
            .await?
            .as_slice(),
        b"bucket a"
    );
    let (grant_b, reader_b) = resolver.mount(&file_b).await?;
    grant_b.require_file_read(&file_b, None)?;
    assert_eq!(
        reader_b
            .read(&file_b, file_b.descriptor().byte_length(), None)?
            .await?
            .as_slice(),
        b"bucket b"
    );

    let no_grant = issuer.issue(
        "no-grant",
        &Capabilities::new([] as [String; 0]),
        Some(attached),
        None,
        &mut None,
    )?;
    let denied_store = ObjectContentStore::new(
        objects,
        bucket_a,
        provider,
        volume_a,
        issuer.verifier(&mut None).expect("verifier copy"),
        owner_scope_a,
        no_grant,
        4_096,
    )
    .await?;
    let denied = ObjectContentMountResolver::new(vec![denied_store])?;
    assert!(denied.mount(&file_a).await.is_err());
    Ok(())
}

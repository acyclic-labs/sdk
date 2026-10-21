use super::*;
use crate::{
    AgentId, Capabilities,
    conversation::{VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
};
use acyclic_objects::v1::{MemoryObjects, MemoryOptions};
use proptest::prelude::*;

async fn binding() -> Result<(Arc<MemoryObjects>, Arc<ObjectContentStore>)> {
    let objects = Arc::new(MemoryObjects::new(MemoryOptions::default()).map_err(store::storage)?);
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "range-test".into(),
            mutation: None,
        })
        .await
        .map_err(store::storage)?
        .bucket
        .ok_or_else(|| Error::Storage("missing test bucket".into()))?;
    let agent = AgentId::new();
    let volume = VolumeRef::new(
        ProviderRef::new("ranges".into(), "objects".into(), "1".into(), &mut None)?,
        bucket.name.clone(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "ranges",
        [3; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "ranges".into(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    let store = ObjectContentStore::new(
        objects.clone(),
        bucket,
        volume.provider().clone(),
        volume,
        (issuer.verifier(&mut None)?, scope.clone()),
        scope,
        4096,
    )
    .await?;
    Ok((objects, store))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Every valid interval returns exactly that slice, including empty files
    // and EOF; malformed/overflowing intervals never return body bytes.
    #[test]
    fn object_intervals_match_the_published_representation(
        bytes in prop::collection::vec(any::<u8>(), 0..256),
        start in any::<u16>(),
        length in any::<u16>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (_, store) = binding().await.unwrap();
            let file = store.stage(OperationId::new(), "data", &bytes,
                "application/octet-stream", "data", None).unwrap().await.unwrap().0;
            let encoded = serde_json::to_vec(&file).unwrap();
            let decoded: FileRef = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(&decoded, &file);
            let offset = u64::from(start) % (bytes.len() as u64 + 1);
            let length = u64::from(length) % (bytes.len() as u64 - offset + 1);
            let selected = store.read_range(&file, acyclic_fs::ByteRange {offset,length}, None)
                .unwrap().await.unwrap();
            prop_assert_eq!(selected.as_ref(), &bytes[offset as usize..(offset + length) as usize]);
            let forged = decoded.with_version(format!("wrong-{}", file.version())).unwrap();
            prop_assert!(store.read_range(&forged,
                acyclic_fs::ByteRange {offset, length}, None).unwrap().await.is_err(),
                "a changed representation identity must refuse the interval");
            for range in [
                acyclic_fs::ByteRange {offset: bytes.len() as u64 + 1, length: 0},
                acyclic_fs::ByteRange {offset: u64::MAX, length: 1},
            ] {
                prop_assert!(store.read_range(&file, range, None).unwrap().await.is_err());
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn replacing_even_identical_bytes_invalidates_the_pinned_range() -> Result<()> {
    let (objects, store) = binding().await?;
    let file = store
        .stage(
            OperationId::new(),
            "data",
            b"original",
            "text/plain",
            "data",
            None,
        )?
        .await?
        .0;
    let artifact = store.artifact_ref(&file)?;
    let key = String::from_utf8(artifact.as_resource().key().to_vec())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    objects
        .put(
            wire::PutObjectHeader {
                bucket: Some(store.bucket.clone()),
                object_key: key,
                metadata: Some(wire::ObjectMetadata {
                    content_type: "text/plain".into(),
                    user: [("harness-display-name".into(), "data".into())].into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            acyclic_stream::OwnedBytes::from_static(b"original"),
        )
        .await
        .map_err(store::storage)?;
    for range in [
        acyclic_fs::ByteRange {
            offset: 1,
            length: 2,
        },
        acyclic_fs::ByteRange {
            offset: 8,
            length: 0,
        },
    ] {
        assert!(matches!(
            store.read_range(&file, range, None)?.await,
            Err(Error::Conflict(_))
        ));
    }
    Ok(())
}

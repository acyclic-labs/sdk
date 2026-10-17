use super::*;
use proptest::prelude::*;

proptest! {
    // Property: publication retries retain the exact immutable reference;
    // conflicting bytes under that identity cannot alter the project head.
    #[test]
    fn retry_identity_preserves_content(bytes in prop::collection::vec(any::<u8>(), 0..32)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let provider = ProviderRef::new("properties".into(), "filesystem".into(), "2".into(), &mut None)?;
            let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
            let volume = VolumeRef::new(provider, "project".into(), VolumeClass::Project,
                VolumeOwner::Project("test".into()), &mut None)?;
            host.create_volume(&volume).await?;
            let issuer = crate::core::AuthorityIssuer::new("properties", [7;32], Authority {
                kind: crate::core::AggregateKind::Conversation, id: "test".into(),
            });
            let scope = issuer.issue("write", &crate::Capabilities::new([
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]), None, None, &mut None)?;
            let grant = ContentGrant::verify(&issuer.verifier(&mut None)?, &scope, &volume,
                VolumeOperation::Write, 4096, &mut None)?;
            let key = IdempotencyKey::new("original")?;
            let publish = |bytes| host.put_content(&volume, &grant, "src/main.rs", bytes,
                "text/plain", "main.rs", 4096, &key, None, None);
            let file = publish(&bytes).await?;
            assert_eq!(publish(&bytes).await?, file);
            let workspace = workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
            let metadata = host.read(&workspace, None,
                &content_metadata_path_io("src/main.rs", true, None)?, 4096, None).await?;
            let recorded: FileRef = crate::contract::json_from_slice(&metadata)
                .map_err(|error| Error::Storage(error.to_string()))?;
            assert_eq!(super::super::file_publication(&recorded, None)?,
                crate::OperationId::from_bytes(filesystem_key(&key).into_bytes()));
            let head = host.resolve(&workspace, None).await?;
            let mut conflicting = bytes.clone();
            conflicting.push(0);
            assert!(matches!(publish(&conflicting).await, Err(Error::Conflict(_))));
            assert_eq!(host.resolve(&workspace, None).await?, head);
            assert_eq!(host.read_pinned_io(&file, 4096, None).await?.as_ref(), bytes.as_slice());
            Ok::<(), Error>(())
        })?;
    }
}

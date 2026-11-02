use super::{store::storage, *};
use crate::{
    AgentId, Capabilities,
    conversation::{ContentMountResolver, VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityRoot},
    resources::ProviderRef,
};
use acyclic_objects::MemoryObjects;

#[tokio::test]
async fn an_upload_retry_naming_other_content_is_refused()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    upload_contract(MemoryObjects::memory(), None).await?;
    #[cfg(not(target_arch = "wasm32"))]
    {
        use acyclic_objects::{LocalClose, LocalMedium, Store, binary::BinaryObjects};
        use std::sync::atomic::{AtomicBool, Ordering};

        acyclic_native_runtime::initialize_native_io_services()?;
        let root = tempfile::tempdir()?;
        let medium = LocalMedium::open(root.path(), Default::default(), None).await?;
        let store = Store::new(medium.clone());
        let account = Arc::new(store.account(b"original-owner")?);
        let other = BinaryObjects::new(LocalCell {
            account: Arc::new(store.account(b"non-owner")?),
            lost_retain_reply: Arc::new(AtomicBool::new(false)),
        });
        let lost_retain_reply = Arc::new(AtomicBool::new(false));
        let objects = BinaryObjects::new(LocalCell {
            account: account.clone(),
            lost_retain_reply: lost_retain_reply.clone(),
        });
        let (file, uncertain) = upload_contract(objects, Some(lost_retain_reply.clone())).await?;
        assert!(!lost_retain_reply.load(Ordering::SeqCst));
        let digest = Digest::from_hex(file.version())?;
        assert_eq!(
            other
                .get(digest, ByteRange::FULL, u64::MAX)
                .await
                .unwrap_err()
                .code,
            acyclic_objects::wire::ErrorCode::NotFound,
            "a non-owner account cannot read the retained content"
        );
        assert_eq!(
            other.delete(digest).await?.outcome,
            acyclic_objects::Delete::Absent
        );
        drop(other);
        drop(account);
        drop(store);
        assert_eq!(medium.close().await?, LocalClose::Released);

        let medium = LocalMedium::open(root.path(), Default::default(), None).await?;
        let store = Store::new(medium.clone());
        let objects = Arc::new(BinaryObjects::new(LocalCell {
            account: Arc::new(store.account(b"original-owner")?),
            lost_retain_reply: Arc::new(AtomicBool::new(false)),
        }));
        objects.collect().await?;
        objects.collect().await?;
        let binding = upload_binding(objects.clone())?;
        for (file, expected) in [
            (file, b"owner bytes".as_slice()),
            (
                uncertain.expect("lost-reply upload"),
                b"accepted before reply loss",
            ),
        ] {
            assert_eq!(
                binding
                    .read(&file, file.descriptor().byte_length(), None, None)?
                    .await?
                    .as_slice(),
                expected,
                "the original volume binding and payload survive collection and reopen"
            );
            if file.path() == "notes/uncertain.txt" {
                let retry = |body: &'static [u8]| {
                    binding.stage(
                        OperationId::from_bytes([77; 16]),
                        "notes/uncertain.txt",
                        body,
                        "text/plain",
                        "one.txt",
                        crate::conversation::ContentAccess::default(),
                    )
                };
                assert_eq!(retry(expected)?.await?.0, file);
                assert!(matches!(
                    retry(b"another body")?.await,
                    Err(Error::Conflict(_))
                ));
            }
        }
        drop(binding);
        drop(objects);
        drop(store);
        assert_eq!(medium.close().await?, LocalClose::Released);
    }
    Ok(())
}

// This cell serves actual canonical dispatch and returns its encoded response;
// only the post-retain delivery cut is controlled. No response is invented.
#[cfg(not(target_arch = "wasm32"))]
struct LocalCell {
    account: Arc<dyn Blobs>,
    lost_retain_reply: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_objects::binary::Transport for LocalCell {
    async fn invoke(
        &self,
        route: &str,
        input: bytes::Bytes,
    ) -> std::result::Result<acyclic_objects::binary::Reply, acyclic_objects::Error> {
        let (body, revision) =
            acyclic_objects::route::invoke_at(self.account.as_ref(), route, input).await?;
        if route == "blobs/retain"
            && self
                .lost_retain_reply
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(acyclic_objects::wire::ErrorCode::OutcomeUnknown.into());
        }
        Ok((body.into(), revision))
    }

    async fn availability(
        &self,
        digest: Digest,
    ) -> std::result::Result<acyclic_objects::Availability, acyclic_objects::Error> {
        self.account.availability(digest).await
    }
}

async fn upload_contract(
    objects: impl Blobs + 'static,
    lost_retain_reply: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> Result<(FileRef, Option<FileRef>)> {
    #[cfg(feature = "conformance")]
    let objects = acyclic_stream::testing::Counted::new(objects);
    #[cfg(feature = "conformance")]
    let ledger = objects.ledger();
    let objects = Arc::new(objects);
    let owner_store = upload_binding(objects.clone())?;
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
    #[cfg(feature = "conformance")]
    let before = ledger.requests();
    let file = stage(10, "notes/one.txt", b"owner bytes")?.await?.0;
    #[cfg(feature = "conformance")]
    {
        let calls = ledger.requests().since(before);
        assert_eq!(calls.data(), 2, "one sealed batch and the original retain");
        assert_eq!(calls.control(), 0);
    }
    assert_eq!(stage(10, "notes/one.txt", b"owner bytes")?.await?.0, file);
    for (path, bytes) in [
        ("notes/one.txt", &b"other bytes"[..]),
        ("notes/two.txt", b"owner bytes"),
    ] {
        #[cfg(feature = "conformance")]
        let before = ledger.requests();
        let conflicting = stage(10, path, bytes)?.await;
        assert!(matches!(conflicting, Err(Error::Conflict(_))));
        #[cfg(feature = "conformance")]
        {
            let calls = ledger.requests().since(before);
            assert_eq!(
                calls.data(),
                1,
                "a refused intent must not retain the batch"
            );
            assert_eq!(calls.control(), 0);
        }
    }
    // Partial conflicting batches may leave immutable orphans. The accepted
    // original intent must still retain its exact record and payload.
    objects.collect().await.map_err(storage)?;
    objects.collect().await.map_err(storage)?;
    assert_eq!(
        owner_store
            .read(&file, file.descriptor().byte_length(), None, None)?
            .await?
            .as_slice(),
        b"owner bytes"
    );
    assert_eq!(stage(42, "notes/one.txt", b"owner bytes")?.await?.0, file);
    let uncertain = if let Some(lost_retain_reply) = lost_retain_reply {
        let body = b"accepted before reply loss";
        let expected = FileRef::new(
            owner_store.volume().clone(),
            "notes/uncertain.txt".into(),
            Digest::of(body).to_hex(),
            FileDescriptor::from_bytes(body, "text/plain", &mut None)?,
            "one.txt".into(),
            &mut None,
        )?;
        lost_retain_reply.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            stage(77, "notes/uncertain.txt", body)?.await,
            Err(Error::Storage(_))
        ));
        // Do not retry here: a replay could repair lost retention and mask it.
        // The original binding must survive collection/reopen before any retry.
        Some(expected)
    } else {
        None
    };
    #[cfg(feature = "conformance")]
    {
        // Real serial publication is the cost negative control. The shared
        // ledger measures provider calls, not hidden physical requests.
        let before = ledger.requests();
        let mut references = Vec::new();
        for body in [
            b"serial content".as_slice(),
            b"serial record",
            b"serial intent",
        ] {
            let sealed =
                acyclic_objects::Sealed::new(acyclic_stream::OwnedBytes::copy_from_slice(body));
            let digest = Digest::sealed(&sealed);
            let result = objects
                .put_sealed(sealed, references)
                .await
                .map_err(storage)?;
            assert_eq!(result.verified().map(|value| value.digest), Some(digest));
            references = vec![digest];
        }
        objects
            .retain(
                &acyclic_objects::RootId::new(b"serial-negative".to_vec()).map_err(storage)?,
                references,
            )
            .await
            .map_err(storage)?;
        let calls = ledger.requests().since(before);
        assert_eq!(calls.data(), 4);
        assert_eq!(calls.control(), 0);
    }
    Ok((file, uncertain))
}

fn upload_binding(objects: Arc<dyn Blobs>) -> Result<Arc<ObjectContentStore>> {
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
    ObjectContentStore::new(
        objects,
        volume.provider().clone(),
        volume,
        (
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope.clone(),
        ),
        owner_scope,
        4_096,
    )
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

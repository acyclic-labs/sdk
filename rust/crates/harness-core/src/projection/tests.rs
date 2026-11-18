use super::*;
use crate::conversation::{
    BoundedContentFuture, ConversationMessage, Custody, FileDescriptor, FileRef,
    ReferencedAttachments, VolumeClass, VolumeOwner, VolumeRef,
};
use crate::{AgentId, Capabilities, resources::ProviderRef};
use std::sync::atomic::{AtomicUsize, Ordering};
#[path = "../../tests/projection/fixture.rs"]
mod fixture;
use fixture::{Resolver, file, message};

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn recorded_tool_metadata_projects_on_its_original_native_ledger() -> Result<()> {
    const RECORD: &[u8] = br#"{"invocation":{"operation_id":"00000000-0000-0000-0000-000000000001","call_id":"call","name":"echo","arguments":{"value":1}},"metadata":{"signature":"opaque","blocks":[{"thinking":"plan"}]}}"#;
    struct NativeRecord;
    impl ContentResidencyVerifier for NativeRecord {
        fn read<'a>(
            &'a self,
            _: &'a FileRef,
            _: u64,
            _ceiling: Option<&'a crate::Capabilities>,
            io: Option<&'a PreparationIo>,
        ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::Custody>>
        {
            assert!(io.and_then(PreparationIo::original_native_work).is_some());
            crate::conversation::BoundedContentFuture::new(io, || async {
                Ok(crate::conversation::Custody::Unmetered(
                    acyclic_stream::OwnedBytes::from_static(RECORD),
                ))
            })
        }
    }
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs_work::performance::work_budget(1_000_000, 4 * 1024 * 1024),
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
        let call =
            artifacts::read_tool_call(&NativeRecord, &reference, Some(4096), Some(&io)).await?;
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

#[tokio::test]
async fn json_projection_checks_resolved_bytes_before_parsing() -> Result<()> {
    let reference = file(AgentId::new(), "tool.json", "application/json", b"{}")?;
    let resolver = Resolver {
        bytes: vec![0; 1_025],
        ..Resolver::default()
    };
    assert!(matches!(
        artifacts::read_value(&resolver, &reference, Some(1_024), None).await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_manifest_resolution_never_calls_the_ordinary_resolver() -> Result<()> {
    native_manifest_resolution(false).await
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_manifest_resolution_rejects_foreign_bytes() -> Result<()> {
    native_manifest_resolution(true).await
}

#[cfg(not(target_arch = "wasm32"))]
async fn native_manifest_resolution(foreign_bytes: bool) -> Result<()> {
    struct NativeResolver {
        bytes: acyclic_stream::OwnedBytes,
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
            _ceiling: Option<&'a crate::Capabilities>,
            io: Option<&'a PreparationIo>,
        ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::Custody>>
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
                    let (mut bytes, custody) = work.copy_bytes(&self.bytes)?;
                    return acyclic_stream::OwnedBytes::from_vec_with_guard_in_place(
                        &mut bytes,
                        &mut Some(custody),
                        &work.runtime_meter(),
                    )
                    .map(crate::conversation::Custody::Unmetered)
                    .map_err(|error| work.runtime_error(error).into());
                }
                Ok(crate::conversation::Custody::Unmetered(self.bytes.clone()))
            })
        }
    }
    let bytes = crate::conversation::encode_attachment_manifest(&[])?;
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
            &bytes,
        )?,
        item_count: 1,
    };
    let resolver = NativeResolver {
        bytes: bytes.into(),
        ordinary: AtomicUsize::new(0),
        native: AtomicUsize::new(0),
        foreign_bytes,
    };
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs_work::performance::WorkBudget {
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
        attachments::resolve(&message, &resolver, std::num::NonZeroUsize::new(1), Some(&io)).await,
        Err(Error::NativeBoundary { code, .. }) if code == expected
    ));
    assert_eq!(resolver.ordinary.load(Ordering::SeqCst), 0);
    assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
    work.cancellation().cancel();
    assert!(
        attachments::resolve(
            &message,
            &resolver,
            std::num::NonZeroUsize::new(1),
            Some(&io)
        )
        .await
        .is_err()
    );
    assert_eq!(resolver.native.load(Ordering::SeqCst), 1);
    Ok(())
}

use super::*;
use crate::{
    AgentId, Capabilities,
    conversation::{VolumeClass, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::SchedulerPayloadStore,
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Receipts written under the original scheduler contract must be reused
    // byte-for-byte, without republishing or changing their location.
    #[test]
    fn saved_scheduler_payload_receipts_keep_their_original_path(
        key in "retry-[a-z]{1,16}", bytes in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async move {
            let provider = crate::resources::ProviderRef::new("saved".into(), "filesystem".into(), "2".into(), &mut None)?;
            let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
            let agent = AgentId::from_bytes([29;16]);
            let volume = VolumeRef::new(provider, "private".into(), VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent), &mut None)?;
            let issuer = AuthorityIssuer::new("saved", [28;32], Authority {kind:AggregateKind::Task,id:"saved".into()});
            let verifier = issuer.verifier(&mut None)?;
            let scope = issuer.issue("owner", &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]), Some(agent), None, &mut None)?;
            let grant = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write, 4096, &mut None)?;
            host.create_volume(&volume).await?;
            let operation = OperationId::from_bytes([27;16]);
            let digest = blake3::hash(key.as_bytes()).to_hex();
            // Freeze the pre-upgrade location and retry identity independently
            // of the publisher's current path selection.
            let path = format!("scheduler/results/{operation}/{digest}.json");
            let retry = IdempotencyKey::new(format!("scheduler-result:{operation}:{digest}"))?;
            let original = host.put_content(&volume, &grant, &path, &bytes,
                "application/json", "result.json", 4096, &retry, None, None).await?;
            let workspace = super::super::workspace_ref(volume.provider().clone(), &volume.storage_name(&mut None)?)?;
            let before = host.resolve(&workspace, None).await?.generation;
            let publisher = FilesystemContentPublisher::new(host.clone(), &volume, &verifier, &scope, 4096, None)?;
            let replayed = SchedulerPayloadStore::stage(publisher.as_ref(), operation, &key, &bytes, None)?.await?;
            assert_eq!(replayed, original);
            assert_eq!(host.resolve(&workspace, None).await?.generation, before);
            // A wake may retry the same operation/key after an old scheduler
            // upload. Its different purpose must not collide with that receipt.
            let input = SchedulerPayloadStore::stage_input(publisher.as_ref(), operation, &key, b"null", None)?.await?;
            assert_eq!(input.path(), format!("inputs/{operation}/{digest}.json"));
            let before = host.resolve(&workspace, None).await?.generation;
            assert_eq!(SchedulerPayloadStore::stage_input(publisher.as_ref(), operation, &key, b"null", None)?.await?, input);
            assert_eq!(SchedulerPayloadStore::stage(publisher.as_ref(), operation, &key, &bytes, None)?.await?, original);
            assert_eq!(host.resolve(&workspace, None).await?.generation, before);
            Ok::<_, crate::Error>(())
        }).unwrap();
    }
}

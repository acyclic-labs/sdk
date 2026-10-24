use super::*;
use crate::conversation::ContentAccess;
use crate::core::{AggregateKind, AuthorityIssuer};
use crate::{AgentId, Capabilities};

#[tokio::test]
async fn published_content_survives_collection_and_reopen_without_extra_pin_authorities()
-> Result<()> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new(
        "content-history".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([4; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "history",
        [5; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "history".into(),
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
    let verifier = issuer.verifier(&mut None)?;
    let read = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Read,
        1024,
        &mut None,
    )?;
    let write = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        1024,
        &mut None,
    )?;
    let mut files = Vec::new();
    let retained_authorities;
    {
        let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path()))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let host = FilesystemHost::new(fs.clone(), &provider, None)?;
        host.create_volume(&volume).await?;
        assert_eq!(
            content_authorities(&fs, &volume)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
                .len(),
            1
        );
        let publication_appends = raw_publication_appends(&fs).await?;
        retained_authorities = content_authorities(&fs, &volume)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        // Ordinary, bounded, and original-native publications share the same
        // retained-history contract, including idempotent observation.
        for mode in 0..3 {
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut allowance = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
            allowance.authority_records_appended = publication_appends;
            let native = acyclic_fs::NativeMaterializationWork::new(allowance, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = match mode {
                0 => None,
                1 => Some(PreparationIo::from_budget(allowance, &cancellation)?),
                _ => Some(PreparationIo::from_native_work(&native)?),
            };
            let bytes = format!("version-{mode}");
            let key = crate::IdempotencyKey::new(format!("content-{mode}"))?;
            let file = host
                .put_content(
                    &volume,
                    &write,
                    "message.txt",
                    bytes.as_bytes(),
                    "text/plain",
                    "message.txt",
                    1024,
                    &key,
                    None,
                    ContentAccess {
                        ceiling: None,
                        io: io.as_ref(),
                    },
                )
                .await
                .map_err(|error| Error::Storage(format!("publication mode {mode}: {error:?}")))?;
            if let Some(io) = io.as_ref() {
                assert_eq!(io.receipt().authority_records_appended, publication_appends);
            }
            assert_eq!(
                host.content_for_operation(
                    &volume,
                    &write,
                    &key,
                    "message.txt",
                    1024,
                    ContentAccess {
                        ceiling: None,
                        io: io.as_ref()
                    }
                )
                .await?,
                Some(file.clone())
            );
            assert_eq!(
                host.put_content(
                    &volume,
                    &write,
                    "message.txt",
                    bytes.as_bytes(),
                    "text/plain",
                    "message.txt",
                    1024,
                    &key,
                    None,
                    ContentAccess {
                        ceiling: None,
                        io: io.as_ref()
                    }
                )
                .await?,
                file
            );
            files.push((file, bytes));
        }
        assert_eq!(
            content_authorities(&fs, &volume)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
            retained_authorities
        );
        fs.collect_local_garbage(None, &acyclic_fs::CancellationToken::new())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
    }
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path()))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let host = FilesystemHost::new(fs.clone(), &provider, None)?;
    for (file, bytes) in files {
        let range = acyclic_fs::ByteRange {
            offset: 1,
            length: 4,
        };
        assert_eq!(
            host.read_content(&file, &read, 1024, None)
                .await
                .map_err(|error| Error::Storage(format!(
                    "reopened ordinary read {bytes}: {error:?}"
                )))?
                .as_ref(),
            bytes.as_bytes()
        );
        assert_eq!(
            host.read_pinned_range(&file, range, None).await?.as_ref(),
            &bytes.as_bytes()[1..5]
        );
        let mut allowance = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
        allowance.authority_records_appended = 0;
        let native = acyclic_fs::NativeMaterializationWork::new(
            allowance,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&native)?;
        assert_eq!(
            host.read_content(&file, &read, 1024, Some(&io))
                .await
                .map_err(|error| Error::Storage(format!(
                    "reopened native read {bytes}: {error:?}"
                )))?
                .as_ref(),
            bytes.as_bytes()
        );
        assert_eq!(native.receipt().authority_records_appended, 0);
        assert_eq!(
            host.read_pinned_range(&file, range, Some(&io))
                .await?
                .as_ref(),
            &bytes.as_bytes()[1..5]
        );
        assert_eq!(native.receipt().authority_records_appended, 0);
        let owned = PreparationIo::from_budget(allowance, &acyclic_fs::CancellationToken::new())?;
        assert_eq!(
            host.read_content(&file, &read, 1024, Some(&owned))
                .await?
                .as_ref(),
            bytes.as_bytes()
        );
        assert_eq!(
            host.read_pinned_range(&file, range, Some(&owned))
                .await?
                .as_ref(),
            &bytes.as_bytes()[1..5]
        );
        assert_eq!(owned.receipt().authority_records_appended, 0);
    }
    assert_eq!(
        content_authorities(&fs, &volume)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        retained_authorities
    );
    Ok(())
}

async fn raw_publication_appends(fs: &acyclic_fs::LocalFs) -> Result<u64> {
    // Measure the backend's atomic publication independently of content
    // retention. This includes main's operation locator, without allowing a
    // content path to create any additional pin authority.
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
    let workspace = fs
        .create_workspace("publication-oracle", budget, &cancellation)
        .await
        .map_err(|failure| Error::Storage(format!("{failure:?}")))?
        .value;
    let mut transaction = workspace
        .begin_transaction(acyclic_fs::IdempotencyKey::new(), budget, &cancellation)
        .await
        .map_err(|failure| Error::Storage(format!("{failure:?}")))?
        .value;
    transaction
        .write_text("/payload", "publication")
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let receipt = transaction
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            budget,
            &cancellation,
            |_| async {
                Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::default(),
                })
            },
        )
        .await
        .map_err(|failure| Error::Storage(format!("{failure:?}")))?;
    assert!(matches!(
        receipt.value,
        acyclic_fs::TransactionCommit::Committed(_)
    ));
    assert!(receipt.work.authority_records_appended > 0);
    Ok(receipt.work.authority_records_appended)
}

async fn content_authorities(
    fs: &acyclic_fs::LocalFs,
    volume: &VolumeRef,
) -> std::result::Result<
    std::collections::BTreeSet<acyclic_fs::AuthorityId>,
    acyclic_fs::AuthorityStoreError,
> {
    let id = fs
        .workspace_id(volume.storage_name(&mut None).expect("valid volume"))
        .expect("valid workspace identity");
    let (provider, _) = fs.authority_records(id.volume_id())?;
    acyclic_fs::StreamAuthorityStore::new(provider)
        .authorities(64)
        .await
        .map(|authorities| authorities.into_iter().collect())
}

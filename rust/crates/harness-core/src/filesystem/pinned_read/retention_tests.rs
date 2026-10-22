use super::super::*;
use crate::core::{AggregateKind, AuthorityIssuer};

type Host = FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Fixture {
    host: Arc<Host>,
    authority: acyclic_fs::MemoryAuthorityBackend,
    workspace: Workspace<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>,
    file: FileRef,
    grant: ContentGrant,
    pin: acyclic_fs::AuthorityId,
}

impl Fixture {
    async fn new(bytes: &[u8]) -> TestResult<Self> {
        Self::with_publication(bytes, false).await
    }

    async fn with_publication(bytes: &[u8], published: bool) -> TestResult<Self> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let filesystem = Fs::from_memory_providers(stream.clone(), Arc::new(objects), bucket);
        let authority = acyclic_fs::distributed::StreamAuthorityStore::new(stream);
        let provider = ProviderRef::new(
            "retention".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let host = FilesystemHost::new(filesystem, &provider, None)?;
        host.create_volume(&volume).await?;
        let reference = fork_preparer_io::content_workspace(&provider, &volume, None)?;
        let workspace = host.open(&reference, None).await?;
        let generation = if published {
            host.apply(
                &reference,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/payload".into(),
                    bytes: bytes::Bytes::copy_from_slice(bytes).into(),
                }],
                &IdempotencyKey::new("raw")?,
            )
            .await?
        } else {
            let cancel = acyclic_fs::CancellationToken::new();
            let mut checkout = workspace
                .checkout(
                    acyclic_fs::model::GenerationSelector::Head,
                    acyclic_fs::model::CheckoutMode::tracking_transaction(),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancel,
                )
                .await?
                .value;
            let path = acyclic_fs::kernel::NamespacePath::new(
                vec![acyclic_fs::kernel::LogicalName::new(
                    acyclic_fs::kernel::NameEncoding::Utf8,
                    b"payload".to_vec(),
                    checkout.volume_config().limits.maximum_component_bytes,
                )?],
                checkout.volume_config().limits,
            )?;
            checkout
                .create(
                    acyclic_fs::AuthoredMutation::CreateFile {
                        path,
                        bytes: bytes::Bytes::copy_from_slice(bytes).into(),
                        metadata: acyclic_fs::kernel::FileMetadata::default(),
                    },
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancel,
                )
                .await?;
            let generation = checkout
                .checkpoint(acyclic_fs::WorkBudget::UNBOUNDED, &cancel)
                .await?
                .value;
            host.generation_ref_id(generation, None)?
        };
        let version = hex::encode(generation.as_resource().key());
        let file = FileRef::new(
            volume.clone(),
            "payload".into(),
            version.clone(),
            FileDescriptor::from_bytes(bytes, "application/octet-stream", &mut None)?,
            "payload".into(),
            &mut None,
        )?;
        let workspace = host.open(&reference, None).await?;
        let pin = acyclic_fs::kernel::retention_authority_id(
            workspace.id().volume_id(),
            acyclic_fs::kernel::RetentionKind::Pin,
            &format!("harness-generation-{version}"),
        );
        let issuer = AuthorityIssuer::new(
            "retention",
            [2; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "owner".into(),
            },
        );
        let scope = issuer.issue(
            "owner",
            &crate::Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]),
            Some(agent),
            None,
            &mut None,
        )?;
        let grant = ContentGrant::verify_directory_read(
            &issuer.verifier(&mut None)?,
            &scope,
            &volume,
            "",
            &mut None,
        )?;
        Ok(Self {
            host,
            authority,
            workspace,
            file,
            grant,
            pin,
        })
    }

    async fn assert_unretained(&self) {
        let cancellation = acyclic_fs::CancellationToken::new();
        assert!(matches!(
            self.authority
                .head(
                    self.pin,
                    &mut acyclic_fs::AuthorityWork::new(
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &cancellation
                    )
                )
                .await,
            Err(acyclic_fs::AuthorityStoreError::Missing)
        ));
    }

    async fn assert_retained_exactly_once(&self) -> TestResult {
        let cancellation = acyclic_fs::CancellationToken::new();
        let (records, _) = self
            .authority
            .replay(
                self.pin,
                acyclic_fs::Sequence::GENESIS,
                acyclic_fs::ReplayLimit {
                    records: 2,
                    payload_bytes: 4096,
                },
                &mut acyclic_fs::AuthorityWork::new(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                ),
            )
            .await?;
        assert_eq!(records.len(), 1);
        let retained = acyclic_fs::kernel::decode_retention_created(&records[0].payload, 4096)?;
        assert_eq!(retained.volume_id, self.workspace.id().volume_id());
        assert_eq!(
            hex::encode(retained.generation_root.digest.into_bytes()),
            self.file.version()
        );
        assert_eq!(
            retained.label,
            format!("harness-generation-{}", self.file.version())
        );
        Ok(())
    }
}

#[test]
fn content_read_pins_an_initially_unretained_generation_without_reopening() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for bytes in [b"".as_slice(), b"retained payload".as_slice()] {
        let fixture = runtime.block_on(Fixture::new(bytes))?;
        runtime.block_on(fixture.assert_unretained());
        let observed = acyclic_test_support::differential::observe::<_, _, (&'static str, u64)>(
            || (),
            |_| {
                runtime.block_on(fixture.host.read_content(
                    &fixture.file,
                    &fixture.grant,
                    4096,
                    None,
                ))
            },
        );
        assert_eq!(observed.0?.as_ref(), bytes);
        let opens = observed
            .1
            .iter()
            .filter(|event| **event == ("open workspace", 1))
            .count();
        assert_eq!(
            opens, 1,
            "a retaining read reauthenticated its already selected generation"
        );
        runtime.block_on(fixture.assert_retained_exactly_once())?;
    }
    Ok(())
}

#[tokio::test]
async fn authenticated_receipts_retain_the_original_generation_after_head_advancement() -> TestResult
{
    let fixture = Fixture::new(b"original").await?;
    let first = fixture
        .host
        .read_pinned_receipt_io(&fixture.file, 4096, None)
        .await?;
    let second = fixture
        .host
        .read_pinned_receipt_io(&fixture.file, 4096, None)
        .await?;
    fixture.assert_unretained().await;
    fixture
        .workspace
        .write_text("/payload", "later generation")
        .await?;
    let (first, second) = futures::join!(first.retain(), second.retain());
    assert_eq!(first?.as_ref(), b"original");
    assert_eq!(second?.as_ref(), b"original");
    fixture.assert_retained_exactly_once().await?;
    Ok(())
}

#[tokio::test]
async fn a_failed_descriptor_check_does_not_retain_unmetered_content() -> TestResult {
    let fixture = Fixture::new(b"original").await?;
    let mut digest = *fixture.file.descriptor().sha256();
    digest[0] ^= 1;
    let invalid = FileRef::new(
        fixture.file.volume().clone(),
        fixture.file.path().into(),
        fixture.file.version().into(),
        FileDescriptor::new(digest, 8, "application/octet-stream".into())?,
        "payload".into(),
        &mut None,
    )?;
    assert!(
        fixture
            .host
            .read_content(&invalid, &fixture.grant, 4096, None)
            .await
            .is_err()
    );
    fixture.assert_unretained().await;
    Ok(())
}

#[tokio::test]
async fn published_reads_reuse_authenticated_history_without_a_pin() -> TestResult {
    for bytes in [b"".as_slice(), b"published original".as_slice()] {
        let fixture = Fixture::with_publication(bytes, true).await?;
        fixture.assert_unretained().await;
        let first = fixture
            .host
            .read_pinned_receipt_io(&fixture.file, 4096, None)
            .await?;
        fixture.workspace.write_text("/payload", "advanced").await?;
        assert_eq!(first.retain().await?.as_ref(), bytes);
        assert_eq!(
            fixture
                .host
                .read_content(&fixture.file, &fixture.grant, 4096, None)
                .await?
                .as_ref(),
            bytes
        );
        fixture.assert_unretained().await;
    }
    Ok(())
}

use super::*;
use crate::core::{AggregateKind, AuthorityRoot};

fn publication_grants(volume: &VolumeRef, agent: AgentId) -> Result<(ContentGrant, ContentGrant)> {
    let issuer = AuthorityRoot::new(
        "batch",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "batch".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "batch-owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Write, &mut None)?,
            volume.capability(VolumeOperation::Read, &mut None)?,
        ]),
        Some(agent),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let write = ContentGrant::verify(
        &verifier,
        &scope,
        volume,
        VolumeOperation::Write,
        64,
        &mut None,
    )?;
    let read = ContentGrant::verify(
        &verifier,
        &scope,
        volume,
        VolumeOperation::Read,
        64,
        &mut None,
    )?;
    Ok((write, read))
}

#[tokio::test]
async fn publication_adapters_preserve_pinned_content_and_reject_changed_operation() -> Result<()> {
    for adapter in 0..3 {
        let provider =
            ProviderRef::new("batch".into(), "filesystem".into(), "1".into(), &mut None)?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = crate::AgentId::from_bytes([37; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let (write, read) = publication_grants(&volume, agent)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = match adapter {
            0 => None,
            1 => Some(PreparationIo::from_budget(budget, &cancellation)?),
            _ => Some(PreparationIo::from_native_work(&native)?),
        };
        let access = crate::conversation::ContentAccess {
            ceiling: None,
            io: io.as_ref(),
        };
        let key = IdempotencyKey::new("batch-first")?;
        let original = host
            .put_content(
                &volume,
                &write,
                "nested/value",
                b"long original",
                "text/plain",
                "value",
                64,
                &key,
                None,
                access,
            )
            .await?;
        let replacement = host
            .put_content(
                &volume,
                &write,
                "nested/value",
                b"x",
                "text/plain",
                "value",
                64,
                &IdempotencyKey::new("batch-replacement")?,
                None,
                access,
            )
            .await?;
        assert_eq!(
            host.read_content(&original, &read, 64, io.as_ref())
                .await?
                .as_ref(),
            b"long original"
        );
        assert_eq!(
            host.read_content(&replacement, &read, 64, io.as_ref())
                .await?
                .as_ref(),
            b"x"
        );
        assert_eq!(
            host.content_for_operation(&volume, &write, &key, "nested/value", 64, access)
                .await?,
            Some(original)
        );
        let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
        let before = host.resolve(&workspace, None).await?;
        assert!(
            host.put_content(
                &volume,
                &write,
                "nested/value",
                b"forged",
                "text/plain",
                "value",
                64,
                &key,
                None,
                access,
            )
            .await
            .is_err()
        );
        assert_eq!(host.resolve(&workspace, None).await?, before);
    }
    Ok(())
}

type MemoryHost =
    FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;

#[tokio::test]
async fn measured_replacement_failures_retain_the_workspace_cause() -> Result<()> {
    for native_adapter in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
        let workspace = Fs::memory()
            .create_workspace("typed-staging-error", budget, &cancellation)
            .await
            .map_err(|failure| map_error(failure.error))?
            .value;
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = if native_adapter {
            PreparationIo::from_native_work(&native)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let mut transaction = io.sdk(
            workspace
                .begin_transaction(acyclic_fs::IdempotencyKey::new(), &io, &cancellation)
                .await,
        )?;
        let error = MemoryHost::stage_publication_batch(
            &mut transaction,
            &[WorkspaceMutation::PutFile {
                path: "/".into(),
                bytes: b"forged root".to_vec().into(),
            }],
            FilePublication::Replace(None),
            Some(&io),
            &cancellation,
        )
        .await
        .expect_err("a directory cannot become a regular file");
        assert!(
            matches!(error, Error::NativeWorkspace { source, .. }
                if matches!(*source, WorkspaceError::NotRegularFile)),
            "measured publication formatted away its workspace cause"
        );
    }
    Ok(())
}

struct InternalPublicationCase<'a> {
    host: &'a MemoryHost,
    volume: &'a VolumeRef,
    write: &'a ContentGrant,
    read: &'a ContentGrant,
    io: Option<&'a PreparationIo>,
}

impl InternalPublicationCase<'_> {
    async fn put(
        &self,
        path: &str,
        bytes: &[u8],
        key: &IdempotencyKey,
        class: InternalContentClass,
    ) -> Result<FileRef> {
        self.host
            .put_internal_content(
                self.volume,
                self.write,
                path,
                bytes,
                "application/json",
                "payload",
                64,
                key,
                class,
                self.io,
            )
            .await
    }

    async fn single_record(&self, workspace: &WorkspaceRef, path: &str) -> Result<bool> {
        let metadata_path = content_metadata_path_io(path, true, None)?;
        let receipt_path = content_receipt_path_io(path, true, true, None)?;
        let obsolete = format!(
            "/.system/harness-uploads/{}.json",
            blake3::hash(path.as_bytes()).to_hex()
        );
        let single_record = receipt_path == metadata_path
            && matches!(
                self.host.read(workspace, None, &obsolete, 4096, None).await,
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound
            );
        let record = self
            .host
            .read(workspace, None, &metadata_path, 4096, None)
            .await?;
        let (staged, expected): (FileRef, Option<GenerationRef>) =
            serde_json::from_slice(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(staged.path(), path);
        assert!(expected.is_none());
        assert_eq!(
            crate::contract::canonical_json_bytes(&(staged, expected))?,
            record.as_ref()
        );
        Ok(single_record)
    }

    async fn verify(&self, workspace: &WorkspaceRef, class: InternalContentClass) -> Result<bool> {
        let path = format!("{}payload.json", class.prefix());
        let key = IdempotencyKey::new(format!("first-{path}"))?;
        let original = self.put(&path, b"original", &key, class).await?;
        let single_record = self.single_record(workspace, &path).await?;
        self.put(
            &path,
            b"replacement",
            &IdempotencyKey::new(format!("second-{path}"))?,
            class,
        )
        .await?;
        let recovered = self
            .host
            .content_for_operation_record_io(
                PublicationLookup {
                    volume: self.volume,
                    key: &key,
                    expected_path: Some(&path),
                    maximum_bytes: 64,
                    internal_class: Some(class),
                    purpose: PublicationPurpose::Content,
                    scope: self.write.policy_scope(),
                    ceiling: None,
                },
                self.io,
            )
            .await?
            .ok_or_else(|| Error::Storage("original publication is absent".into()))?;
        assert_eq!(recovered.0, original);
        assert_eq!(
            recovered
                .1
                .ok_or_else(|| Error::Storage("recovered payload is absent".into()))?
                .as_ref(),
            b"original"
        );
        assert_eq!(
            self.host
                .read_content(&original, self.read, 64, self.io)
                .await?
                .as_ref(),
            b"original"
        );
        let before = self.host.resolve(workspace, None).await?;
        assert!(matches!(
            self.put(&path, b"forged", &key, class).await,
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict
        ));
        assert_eq!(self.host.resolve(workspace, None).await?, before);
        Ok(single_record)
    }
}

#[tokio::test]
async fn internal_publication_has_one_metadata_and_recovery_record_for_every_adapter() -> Result<()>
{
    let mut single_records = Vec::new();
    for class in [
        InternalContentClass::Interaction,
        InternalContentClass::Workflow,
        InternalContentClass::Execution,
        InternalContentClass::ModelPrefix,
        InternalContentClass::Conversation,
    ] {
        for adapter in 0..3 {
            let provider =
                ProviderRef::new("batch".into(), "filesystem".into(), "1".into(), &mut None)?;
            let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
            let agent = crate::AgentId::from_bytes([37; 16]);
            let volume = VolumeRef::new(
                provider.clone(),
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
                &mut None,
            )?;
            host.create_volume(&volume).await?;
            let (write, read) = publication_grants(&volume, agent)?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
            let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let io = match adapter {
                0 => None,
                1 => Some(PreparationIo::from_budget(budget, &cancellation)?),
                _ => Some(PreparationIo::from_native_work(&native)?),
            };
            let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
            let case = InternalPublicationCase {
                host: &host,
                volume: &volume,
                write: &write,
                read: &read,
                io: io.as_ref(),
            };
            single_records.push(case.verify(&workspace, class).await.map_err(|error| {
                Error::Storage(format!(
                    "adapter {adapter}, family {}: {error:?}",
                    class.prefix()
                ))
            })?);
        }
    }
    assert_eq!(
        single_records, [true; 15],
        "internal publications retained duplicate metadata and recovery indexes"
    );
    Ok(())
}

#[tokio::test]
async fn unmetered_immutable_batches_refuse_existing_and_duplicate_bindings() -> Result<()> {
    immutable_record_batch(0).await
}

#[tokio::test]
async fn measured_immutable_batches_refuse_existing_and_duplicate_bindings() -> Result<()> {
    immutable_record_batch(1).await
}

#[tokio::test]
async fn native_immutable_batches_refuse_existing_and_duplicate_bindings() -> Result<()> {
    immutable_record_batch(2).await
}

async fn immutable_record_batch(adapter: u8) -> Result<()> {
    let provider = ProviderRef::new(
        "immutable-batch".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let filesystem = Fs::memory();
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
    filesystem
        .create_workspace("records", budget, &cancellation)
        .await
        .map_err(|failure| map_error(failure.error))?;
    let host = FilesystemHost::new(filesystem, &provider, None)?;
    let workspace = workspace_ref(provider, "records")?;
    let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = match adapter {
        0 => None,
        1 => Some(PreparationIo::from_budget(budget, &cancellation)?),
        _ => Some(PreparationIo::from_native_work(&native)?),
    };
    let first = [
        WorkspaceMutation::CreateDirectory {
            path: "/records/nested".into(),
        },
        WorkspaceMutation::PutFile {
            path: "/records/nested/sealed".into(),
            bytes: b"original".to_vec().into(),
        },
    ];
    host.create_records(
        &workspace,
        &first,
        IdempotencyKey::new("create-original")?,
        io.as_ref(),
    )?
    .await?;
    let before = host.resolve(&workspace, None).await?;
    for duplicate in [false, true] {
        let forged = [
            WorkspaceMutation::CreateDirectory {
                path: "/uncommitted".into(),
            },
            WorkspaceMutation::PutFile {
                path: "/uncommitted/first".into(),
                bytes: b"first".to_vec().into(),
            },
            WorkspaceMutation::PutFile {
                path: if duplicate {
                    "/uncommitted/first"
                } else {
                    "/records/nested/sealed"
                }
                .into(),
                bytes: b"forged".to_vec().into(),
            },
        ];
        assert!(matches!(
            host.create_records(
                &workspace,
                &forged,
                IdempotencyKey::new("overwrite-sealed")?,
                io.as_ref()
            )?
            .await,
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict
        ));
        assert_eq!(host.resolve(&workspace, None).await?, before);
        assert_eq!(
            host.read(&workspace, None, "/records/nested/sealed", 64, None)
                .await?
                .as_ref(),
            b"original"
        );
        assert!(
            matches!(host.read(&workspace, None, "/uncommitted/first", 64, None).await,
        Err(error) if error.code() == crate::wire::ErrorCode::NotFound)
        );
    }
    Ok(())
}

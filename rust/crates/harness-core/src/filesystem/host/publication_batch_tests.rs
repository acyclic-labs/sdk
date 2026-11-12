use super::*;
use crate::core::{AggregateKind, AuthorityRoot};
use crate::{AgentId, conversation::VolumeOwner, core::Authority};

fn publication_grants(volume: &VolumeRef, agent: AgentId) -> Result<(ContentGrant, ContentGrant)> {
    use acyclic_fs::access::{Action, Capability};

    let issuer = AuthorityRoot::host_owned(
        "batch",
        [41; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "batch".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "batch-owner",
        &crate::Capabilities::new([Capability::whole_volume(
            None,
            &[
                Action::List,
                Action::Read,
                Action::ReadHistory,
                Action::Create,
                Action::Write,
                Action::Delete,
            ],
        )
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?]),
        Some(agent),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let write = ContentGrant::verify(&verifier, &scope, volume, VolumeOperation::Write, &mut None)?;
    let read = ContentGrant::verify(&verifier, &scope, volume, VolumeOperation::Read, &mut None)?;
    Ok((write, read))
}

#[tokio::test]
async fn content_routing_evidence_never_creates_or_overwrites_a_workspace_path() -> Result<()> {
    for stored in [false, true] {
        let provider =
            ProviderRef::new("routing".into(), "filesystem".into(), "1".into(), &mut None)?;
        let filesystem = Fs::memory();
        let store = acyclic_fs::StreamGitCompatStore::new(acyclic_fs::StreamAuthorityStore::new(
            filesystem.stream_provider(),
        ));
        let host =
            FilesystemHost::new(filesystem, &provider, None)?.with_project_families(store)?;
        let agent = AgentId::from_bytes([37; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "routing".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let reference = host.create_volume(&volume).await?.workspace;
        let root = host.open(&reference, None).await?;
        let (grant, _) = publication_grants(&volume, agent)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::for_unmeasured(&cancellation)?;
        let (_, fence) = host
            .authorize_content_write(&root, &grant, "file", false, None, Some(&io))?
            .await?;
        let operation = FilesystemKey::new();
        let before = io.sdk(root.head(&io, &cancellation).await)?.id();
        if stored {
            let digest = blake3::derive_key(
                "acyclic-harness-content-workspace-v1",
                &operation.into_bytes(),
            );
            let mut key = [0; 16];
            key.copy_from_slice(&digest[..16]);
            if key == [0; 16] {
                key[0] = 1;
            }
            let mut transaction = io.sdk(
                root.begin_transaction(FilesystemKey::from_bytes(key), &io, &cancellation)
                    .await,
            )?;
            io.sdk(
                transaction
                    .create_dir_all("/.system", &io, &cancellation)
                    .await,
            )?;
            let mut bytes = vec![1];
            bytes.extend_from_slice(&root.id().into_bytes());
            io.sdk(
                transaction
                    .write(
                        "/.system/harness-content-workspace",
                        bytes,
                        &io,
                        &cancellation,
                    )
                    .await,
            )?;
            FilesystemHost::commit_contextual_publication(
                transaction,
                &io,
                super::FilePublication::Authorized(fence, &grant, None),
                None,
            )?
            .await?;
        } else {
            host.bind_content_operation(&root, root.id(), operation, (fence, &grant), &io)?
                .await?;
            assert_eq!(
                io.sdk(root.head(&io, &cancellation).await)?.id(),
                before,
                "routing mutated the user's tree"
            );
            let original = io.sdk(root.head(&io, &cancellation).await)?;
            assert!(
                io.sdk(original.stat("/.system", &io, &cancellation).await)
                    .is_err()
            );
        }
        let binding_cut = io.sdk(root.head(&io, &cancellation).await)?.id();
        host.apply(
            &reference,
            None,
            &[
                WorkspaceMutation::CreateDirectory {
                    path: "/.system".into(),
                },
                WorkspaceMutation::PutFile {
                    path: "/.system/harness-content-workspace".into(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"authored file"),
                },
            ],
            &IdempotencyKey::new("authored")?,
        )
        .await?;
        let (selected, binding) = host
            .content_operation_workspace(&root, operation, false, &io)
            .await?
            .ok_or_else(|| io.invalid("routing disappeared"))?;
        assert_eq!(selected.id(), root.id());
        assert_eq!(
            binding.ok_or_else(|| io.invalid("binding absent"))?.id(),
            binding_cut
        );
        assert_eq!(
            host.read(
                &reference,
                None,
                "/.system/harness-content-workspace",
                64,
                None
            )
            .await?
            .as_ref(),
            b"authored file"
        );
    }
    Ok(())
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
        // Recover the same original creation when a prior initializer chose
        // another concurrency contract. Its config and content remain intact.
        let (name, operation) = host.volume_creation(&volume)?;
        let mut initialization = host.volume_initialization();
        initialization.config.concurrency = acyclic_fs::model::ConcurrencyMode::Optimistic;
        host.filesystem
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                &name,
                initialization,
                operation,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                None,
            )
            .await
            .map_err(|failure| map_error(failure.error))?;
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
        host.create_volume(&volume).await?;
        assert_eq!(
            host.open(&workspace_ref(provider.clone(), &name)?, None)
                .await?
                .volume_config(),
            initialization.config
        );
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

async fn replace_source(
    host: &MemoryHost,
    write: &ContentGrant,
    source: &FileRef,
    bytes: &[u8],
    key: &IdempotencyKey,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let expected = host.file_generation(source, io)?;
    host.put_content_impl(
        source.volume(),
        write,
        source.path(),
        bytes,
        source.descriptor().media_type(),
        source.display_name(),
        64,
        key,
        None,
        Some(&expected),
        None,
        io,
    )?
    .await
}

#[tokio::test]
async fn replacement_fences_the_original_file_instead_of_unrelated_publications() -> Result<()> {
    for adapter in 0..3 {
        let provider =
            ProviderRef::new("replace".into(), "filesystem".into(), "1".into(), &mut None)?;
        let mut host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = crate::AgentId::from_bytes([38; 16]);
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
        let budget = acyclic_fs::performance::work_budget(8_000_000, 8_000_000);
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
        let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
        let opened = host.open(&workspace, None).await?;
        let race = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let race_arm = race.clone();
        let race_workspace = opened.clone();
        Arc::get_mut(&mut host).unwrap().publication_pause = Some(Arc::new(move || {
            let armed = race_arm.swap(false, std::sync::atomic::Ordering::SeqCst);
            let workspace = race_workspace.clone();
            Box::pin(async move {
                if armed {
                    workspace
                        .write_text("/nested/value", "racer")
                        .await
                        .unwrap();
                }
            })
        }));
        let original = host
            .put_content(
                &volume,
                &write,
                "nested/value",
                b"before",
                "text/plain",
                "value",
                64,
                &IdempotencyKey::new("original")?,
                None,
                access,
            )
            .await?;
        // A real tool-result publication advances the same private volume.
        host.put_internal_content(
            &volume,
            &write,
            &format!("{}result", InternalContentClass::Execution.prefix()),
            b"settled",
            "application/json",
            "result",
            64,
            &IdempotencyKey::new("tool-result")?,
            InternalContentClass::Execution,
            io.as_ref(),
        )
        .await?;
        let key = IdempotencyKey::new("edit")?;
        let edited = replace_source(&host, &write, &original, b"after", &key, io.as_ref()).await?;
        assert_eq!(
            host.read_content(&edited, &read, 64, io.as_ref())
                .await?
                .as_ref(),
            b"after"
        );
        assert_eq!(
            host.read_content(&original, &read, 64, io.as_ref())
                .await?
                .as_ref(),
            b"before"
        );
        assert_eq!(
            replace_source(&host, &write, &original, b"after", &key, io.as_ref()).await?,
            edited
        );
        let before = host.resolve(&workspace, None).await?;
        assert!(matches!(
            replace_source(&host, &write, &original, b"lost update", &IdempotencyKey::new("stale")?, io.as_ref()).await,
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict
        ));
        assert!(matches!(
            replace_source(&host, &write, &original, b"changed retry", &key, io.as_ref()).await,
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict
        ));
        assert_eq!(host.resolve(&workspace, None).await?, before);
        // Equal body bytes in a different publication still invalidate the source.
        let equal = host
            .put_content(
                &volume,
                &write,
                "nested/value",
                b"after",
                "text/plain",
                "value",
                64,
                &IdempotencyKey::new("equal-body-new-publication")?,
                None,
                access,
            )
            .await?;
        assert!(matches!(
            replace_source(&host, &write, &edited, b"lost update", &IdempotencyKey::new("old-publication")?, io.as_ref()).await,
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict
        ));
        if io.is_some() {
            // Change the target after comparison and policy-fence registration.
            race.store(true, std::sync::atomic::Ordering::SeqCst);
            assert!(matches!(
                replace_source(&host, &write, &equal, b"overwrite racer", &IdempotencyKey::new("raced")?, io.as_ref()).await,
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict
            ));
        } else {
            opened
                .write_text("/nested/value", "racer")
                .await
                .map_err(map_error)?;
        }
        // Raw filesystem writes did not update the protected publication index.
        let before = host.resolve(&workspace, None).await?;
        assert!(matches!(
            replace_source(&host, &write, &equal, b"overwrite raw write", &IdempotencyKey::new("raw-stale")?, io.as_ref()).await,
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict
        ));
        assert_eq!(host.resolve(&workspace, None).await?, before);
        assert_eq!(
            host.read(&workspace, None, "/nested/value", 64, None)
                .await?
                .as_ref(),
            b"racer"
        );
    }
    Ok(())
}

// Two harness publications to one volume (a turn's journal and a prompt's
// staged input) each add a receipt beside the other's. Losing the race to
// the other must rebase the proven-disjoint candidate on every adapter.
#[tokio::test]
async fn publication_rebases_past_an_unrelated_concurrent_writer() -> Result<()> {
    for adapter in 0..3 {
        let provider = ProviderRef::new("race".into(), "filesystem".into(), "1".into(), &mut None)?;
        let mut host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = crate::AgentId::from_bytes([39; 16]);
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
        let budget = acyclic_fs::performance::work_budget(8_000_000, 8_000_000);
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
        let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
        let opened = host.open(&workspace, None).await?;
        let race = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let race_arm = race.clone();
        Arc::get_mut(&mut host).unwrap().publication_pause = Some(Arc::new(move || {
            let armed = race_arm.swap(false, std::sync::atomic::Ordering::SeqCst);
            let workspace = opened.clone();
            Box::pin(async move {
                if armed {
                    workspace
                        .write_text("/.system/harness-uploads/racer", "racer")
                        .await
                        .unwrap();
                }
            })
        }));
        let mut staged = Vec::new();
        for (key, armed) in [("first", false), ("second", true)] {
            race.store(armed, std::sync::atomic::Ordering::SeqCst);
            let file = host
                .put_content(
                    &volume,
                    &write,
                    &format!("inputs/{key}"),
                    b"input",
                    "text/plain",
                    "input",
                    64,
                    &IdempotencyKey::new(key)?,
                    None,
                    access,
                )
                .await?;
            staged.push(file);
        }
        assert!(!race.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            host.read_content(&staged[1], &read, 64, io.as_ref())
                .await?
                .as_ref(),
            b"input"
        );
        assert_eq!(
            host.read(&workspace, None, "/.system/harness-uploads/racer", 64, None)
                .await?
                .as_ref(),
            b"racer"
        );
    }
    Ok(())
}

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
            Some(&io),
            &cancellation,
        )
        .await
        .expect_err("a directory cannot become a regular file");
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
        assert!(
            matches!(error, Error::NativeWorkspace { source, .. }
                if matches!(*source, WorkspaceError::NotRegularFile)),
            "measured publication formatted away its workspace cause"
        );
    }
    Ok(())
}

struct PublicationCase<'a> {
    host: &'a MemoryHost,
    volume: &'a VolumeRef,
    write: &'a ContentGrant,
    read: &'a ContentGrant,
    io: Option<&'a PreparationIo>,
}

impl PublicationCase<'_> {
    async fn put(
        &self,
        path: &str,
        bytes: &[u8],
        key: &IdempotencyKey,
        class: Option<InternalContentClass>,
    ) -> Result<FileRef> {
        self.host
            .put_content_impl(
                self.volume,
                self.write,
                path,
                bytes,
                "application/json",
                "payload",
                64,
                key,
                class,
                None,
                None,
                self.io,
            )?
            .await
    }

    async fn original_evidence(
        &self,
        workspace: &WorkspaceRef,
        path: &str,
        key: &IdempotencyKey,
        class: Option<InternalContentClass>,
    ) -> Result<()> {
        let metadata_path = content_metadata_path_io(self.volume, path, true, None)?;
        let record = self
            .host
            .read(workspace, None, &metadata_path, 4096, None)
            .await?;
        let (version, (staged, expected)): (u8, (FileRef, Option<GenerationRef>)) =
            serde_json::from_slice(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(version, 1);
        assert_eq!(staged.path(), path);
        assert!(expected.is_none());
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::for_unmeasured(&cancellation)?;
        let opened = self.host.open(workspace, None).await?;
        let publication = io
            .sdk(
                opened
                    .operation_publication(filesystem_key(key), None, &io, &cancellation)
                    .await,
            )?
            .ok_or_else(|| io.invalid("publication evidence is absent"))?;
        assert_eq!(publication.evidence(), Some(record.as_ref()));
        let identity = if class.is_some() { path } else { key.as_str() };
        let former_receipt = format!(
            "/.system/harness-uploads/{}.json",
            blake3::hash(identity.as_bytes()).to_hex()
        );
        assert!(
            io.sdk(
                publication
                    .generation()
                    .stat(&former_receipt, &io, &cancellation)
                    .await
            )
            .is_err()
        );
        Ok(())
    }

    async fn put_without_evidence(
        &self,
        workspace: &WorkspaceRef,
        path: &str,
        key: &IdempotencyKey,
        file_only_index: bool,
        class: Option<InternalContentClass>,
    ) -> Result<FileRef> {
        let mut file = FileRef::new(
            self.volume.copy(&mut None)?,
            path.into(),
            bounded_io::hex_key(&filesystem_key(key).into_bytes(), None)?,
            FileDescriptor::from_bytes(b"original", "application/json", &mut None)?,
            "payload".into(),
            &mut None,
        )?;
        let namespace = self.volume.storage_name(&mut None)?;
        let mut hash = blake3::Hasher::new_derive_key("acyclic harness file index v1");
        hash.update(namespace.as_bytes());
        hash.update(&[0]);
        hash.update(path.as_bytes());
        let metadata = format!(
            "/.system/harness-file-metadata/v1/{}.json",
            hash.finalize().to_hex()
        );
        let receipt = crate::contract::canonical_json_bytes(&(&file, None::<GenerationRef>))?;
        let index = if file_only_index {
            serde_json::to_vec(&file).map_err(|error| Error::Invalid(error.to_string()))?
        } else {
            receipt.clone()
        };
        let identity = if class.is_some() { path } else { key.as_str() };
        let receipt_path = format!(
            "/.system/harness-uploads/{}.json",
            blake3::hash(identity.as_bytes()).to_hex()
        );
        let generation = self
            .host
            .apply(
                workspace,
                None,
                &[
                    WorkspaceMutation::CreateDirectory {
                        path: format!("/{}", path.rsplit_once('/').unwrap().0),
                    },
                    WorkspaceMutation::CreateDirectory {
                        path: "/.system/harness-file-metadata/v1".into(),
                    },
                    WorkspaceMutation::CreateDirectory {
                        path: "/.system/harness-uploads".into(),
                    },
                    WorkspaceMutation::PutFile {
                        path: format!("/{path}"),
                        bytes: acyclic_stream::OwnedBytes::from_static(b"original"),
                    },
                    WorkspaceMutation::PutFile {
                        path: metadata,
                        bytes: index.into(),
                    },
                    WorkspaceMutation::PutFile {
                        path: receipt_path,
                        bytes: receipt.into(),
                    },
                ],
                key,
            )
            .await?;
        file.pin_version(
            bounded_io::hex_key(generation.as_resource().key(), None)?,
            &mut None,
        )?;
        Ok(file)
    }

    async fn verify(
        &self,
        workspace: &WorkspaceRef,
        class: Option<InternalContentClass>,
        file_only_index: Option<bool>,
    ) -> Result<()> {
        let format = match file_only_index {
            None => "evidence",
            Some(false) => "tuple",
            Some(true) => "file",
        };
        let path = format!(
            "{}payload-{format}.json",
            class.map_or("uploads/", |class| class.prefix())
        );
        let key = IdempotencyKey::new(format!("first-{path}"))?;
        let original = if let Some(file_only) = file_only_index {
            self.put_without_evidence(workspace, &path, &key, file_only, class)
                .await?
        } else {
            let file = self.put(&path, b"original", &key, class).await?;
            self.original_evidence(workspace, &path, &key, class)
                .await?;
            file
        };
        if class.is_none() {
            let grant = ContentGrant::verify_directory_read(
                &self.read.copy_verifier(&mut None)?,
                self.read.policy_scope(),
                self.volume,
                "",
                &mut None,
            )?;
            let (indexed, bytes, _) = self
                .host
                .read_private_path_indexed(
                    self.volume,
                    &grant,
                    &path,
                    None,
                    64,
                    crate::conversation::ContentAccess {
                        ceiling: None,
                        io: self.io,
                    },
                )
                .await?
                .ok_or_else(|| Error::Storage("indexed upload is absent".into()))?;
            assert_eq!(indexed.descriptor(), original.descriptor());
            assert_eq!(indexed.display_name(), original.display_name());
            assert_eq!(bytes.as_ref(), b"original");
        }
        let before_retry = self.host.resolve(workspace, None).await?;
        assert_eq!(self.put(&path, b"original", &key, class).await?, original);
        assert_eq!(self.host.resolve(workspace, None).await?, before_retry);
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
                    internal_class: class,
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
        Ok(())
    }
}

#[tokio::test]
async fn publication_recovers_original_uploads_for_every_adapter() -> Result<()> {
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
        let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
        for class in [
            None,
            Some(InternalContentClass::Interaction),
            Some(InternalContentClass::Execution),
            Some(InternalContentClass::ToolResult),
            Some(InternalContentClass::Conversation),
        ] {
            for file_only_index in [None, Some(false), Some(true)] {
                let cancellation = acyclic_fs::CancellationToken::new();
                let budget = acyclic_fs::performance::work_budget(1_000_000, 1_000_000);
                let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let io = match adapter {
                    0 => None,
                    1 => Some(PreparationIo::from_budget(budget, &cancellation)?),
                    _ => Some(PreparationIo::from_native_work(&native)?),
                };
                let case = PublicationCase {
                    host: &host,
                    volume: &volume,
                    write: &write,
                    read: &read,
                    io: io.as_ref(),
                };
                case.verify(&workspace, class, file_only_index)
                    .await
                    .map_err(|error| {
                        Error::Storage(format!(
                            "adapter {adapter}, family {}, index {file_only_index:?}: {error:?}",
                            class.map_or("uploads/", |class| class.prefix())
                        ))
                    })?;
            }
        }
    }
    Ok(())
}

async fn conditional_without_evidence(
    host: &MemoryHost,
    volume: &VolumeRef,
    key: &IdempotencyKey,
) -> Result<crate::conversation::ContentChanges> {
    use crate::conversation::{ContentChangeResult, ContentChanges};
    let workspace = bounded_io::content_workspace(volume.provider(), volume, None)?;
    let operation = bounded_io::hex_key(&filesystem_key(key).into_bytes(), None)?;
    let namespace = volume.storage_name(&mut None)?;
    let mut files = Vec::new();
    let mut mutations = vec![
        WorkspaceMutation::CreateDirectory {
            path: "/nested".into(),
        },
        WorkspaceMutation::CreateDirectory {
            path: "/.system/harness-file-metadata/v1".into(),
        },
        WorkspaceMutation::CreateDirectory {
            path: "/.system/harness-uploads".into(),
        },
    ];
    for path in ["nested/first", "second"] {
        let file = FileRef::new(
            volume.copy(&mut None)?,
            path.into(),
            operation.clone(),
            FileDescriptor::from_bytes(b"first", "text/plain", &mut None)?,
            "file".into(),
            &mut None,
        )?;
        let mut hash = blake3::Hasher::new_derive_key("acyclic harness file index v1");
        hash.update(namespace.as_bytes());
        hash.update(&[0]);
        hash.update(path.as_bytes());
        mutations.push(WorkspaceMutation::PutFile {
            path: format!("/{path}"),
            bytes: acyclic_stream::OwnedBytes::from_static(b"first"),
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: format!(
                "/.system/harness-file-metadata/v1/{}.json",
                hash.finalize().to_hex()
            ),
            bytes: crate::contract::canonical_json_bytes(&file)?.into(),
        });
        files.push(file);
    }
    let records: Vec<_> = files
        .iter()
        .map(|file| serde_json::json!({"action":"create", "file":file}))
        .collect();
    mutations.push(WorkspaceMutation::PutFile {
        path: format!(
            "/.system/harness-uploads/{}.json",
            blake3::hash(key.as_str().as_bytes()).to_hex()
        ),
        bytes: crate::contract::canonical_json_bytes(&(2_u8, records))?.into(),
    });
    let generation = host.apply(&workspace, None, &mutations, key).await?;
    let version = bounded_io::hex_key(generation.as_resource().key(), None)?;
    let members = files
        .into_iter()
        .map(|mut file| {
            file.pin_version(version.clone(), &mut None)?;
            Ok(ContentChangeResult::Created(file))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ContentChanges {
        generation,
        members,
    })
}

#[tokio::test]
async fn conditional_changes_publish_once_with_action_and_parent_authority() -> Result<()> {
    use crate::conversation::{ContentAccess, ContentChange};
    use acyclic_fs::access::{Action, Capability};
    for (adapter, stored) in [
        (0, false),
        (0, true),
        (1, false),
        (1, true),
        (2, false),
        (2, true),
    ] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(8_000_000, 8_000_000);
        let native = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = match adapter {
            0 => None,
            1 => Some(PreparationIo::from_budget(budget, &cancellation)?),
            _ => Some(PreparationIo::from_native_work(&native)?),
        };
        let provider = ProviderRef::new(
            "conditional".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([39; 16]);
        let volume = VolumeRef::new(
            provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let (write, read) = publication_grants(&volume, agent)?;
        let actions = |actions: &[Action]| -> Result<crate::Capabilities> {
            Ok(crate::Capabilities::new([Capability::whole_volume(
                None, actions,
            )
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?]))
        };
        let create = actions(&[Action::Create])?;
        let create_access = ContentAccess {
            ceiling: Some(&create),
            io: io.as_ref(),
        };
        let add = |path| ContentChange::CreateAbsent {
            path,
            bytes: b"first",
            media_type: "text/plain",
            display_name: "file",
        };
        let changes = [add("nested/first"), add("second")];
        let key = IdempotencyKey::new("conditional-create")?;
        let original = if stored {
            conditional_without_evidence(&host, &volume, &key).await?
        } else {
            host.put_changes(&volume, &write, &changes, &key, 64, create_access, None)?
                .await?
        };
        let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
        let opened = host.open(&workspace, None).await?;
        let proof_io = PreparationIo::for_unmeasured(&cancellation)?;
        let publication = proof_io
            .sdk(
                opened
                    .operation_publication(filesystem_key(&key), None, &proof_io, &cancellation)
                    .await,
            )?
            .ok_or_else(|| proof_io.invalid("conditional publication evidence is absent"))?;
        if !stored {
            assert!(
                proof_io
                    .sdk(
                        publication
                            .generation()
                            .stat("/.system/harness-uploads", &proof_io, &cancellation)
                            .await
                    )
                    .is_err()
            );
        }
        let former_receipt = format!(
            "/.system/harness-uploads/{}.json",
            blake3::hash(key.as_str().as_bytes()).to_hex()
        );
        host.apply(
            &workspace,
            None,
            &[
                WorkspaceMutation::CreateDirectory {
                    path: "/.system/harness-uploads".into(),
                },
                WorkspaceMutation::PutFile {
                    path: former_receipt.clone(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"ordinary authored bytes"),
                },
            ],
            &IdempotencyKey::new("authored-former-receipt")?,
        )
        .await?;
        let receipt_bytes = if let Some(bytes) = publication.evidence() {
            bytes.len() as u64
        } else {
            host.read(
                &workspace,
                Some(&original.generation),
                &former_receipt,
                64_000,
                None,
            )
            .await?
            .len() as u64
        };
        let refused = host
            .observe_changes(&volume, &write, &key, receipt_bytes - 1, create_access)?
            .await
            .expect_err("oversized receipt must refuse in the canonical decoder");
        if stored {
            assert!(
                matches!(refused, Error::NativeWorkspace { source, .. } if matches!(*source, WorkspaceError::ReadLimitExceeded))
            );
        } else {
            assert!(matches!(
                refused,
                Error::NativeWorkspace { source, .. }
                    if matches!(
                        *source,
                        WorkspaceError::Filesystem(acyclic_fs::FsError::Decode(
                            acyclic_fs::kernel::CanonicalDecodeError::FieldTooLarge { .. }
                        ))
                    )
            ));
        }
        let observed = host
            .observe_changes(&volume, &write, &key, receipt_bytes, create_access)?
            .await?
            .ok_or_else(|| proof_io.invalid("conditional receipt absent"))?;
        assert_eq!(observed.generation, original.generation);
        assert_eq!(
            observed.files().collect::<Vec<_>>(),
            original.files().collect::<Vec<_>>()
        );
        let first = original.files().next().unwrap();
        assert_eq!(
            host.read_content(first, &read, 64, None).await?.as_ref(),
            b"first"
        );
        let replay = host
            .put_changes(&volume, &write, &changes, &key, 64, create_access, None)?
            .await?;
        assert_eq!(replay.generation, original.generation);
        assert_eq!(
            replay.files().collect::<Vec<_>>(),
            original.files().collect::<Vec<_>>()
        );
        assert_eq!(
            host.read(&workspace, None, &former_receipt, 64, None)
                .await?
                .as_ref(),
            b"ordinary authored bytes"
        );
        let workspace = bounded_io::content_workspace(&provider, &volume, None)?;
        let before = host.resolve(&workspace, None).await?;
        // The first member is valid; an existing later destination refuses the whole cut.
        let collision = host
            .put_changes(
                &volume,
                &write,
                &[add("unpublished"), add("second")],
                &IdempotencyKey::new("conditional-conflict")?,
                64,
                create_access,
                None,
            )?
            .await
            .expect_err("an existing destination must refuse the entire publication");
        assert_eq!(collision.code(), crate::wire::ErrorCode::Conflict);
        assert_eq!(host.resolve(&workspace, None).await?, before);
        let deny_parent = |action, path: &str| {
            if action == Action::Create && path == "forbidden" {
                Err(Error::Unauthorized("directory creation denied".into()))
            } else {
                Ok(())
            }
        };
        assert!(
            host.put_changes(
                &volume,
                &write,
                &[add("valid"), add("forbidden/file")],
                &IdempotencyKey::new("conditional-parent")?,
                64,
                create_access,
                Some(&deny_parent)
            )?
            .await
            .is_err()
        );
        assert_eq!(host.resolve(&workspace, None).await?, before);
        assert!(
            host.put_changes(
                &volume,
                &write,
                &[ContentChange::UpdateExpected {
                    source: first,
                    bytes: b"denied"
                }],
                &IdempotencyKey::new("conditional-write-denied")?,
                64,
                create_access,
                None
            )?
            .await
            .is_err()
        );
        assert_eq!(host.resolve(&workspace, None).await?, before);
        let volume_write =
            crate::Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]);
        for ceiling in [&create, &volume_write] {
            let denied = host
                .put_changes(
                    &volume,
                    &write,
                    &[ContentChange::DeleteExpected { source: first }],
                    &IdempotencyKey::new("conditional-delete-without-grant")?,
                    64,
                    ContentAccess {
                        ceiling: Some(ceiling),
                        io: io.as_ref(),
                    },
                    None,
                )?
                .await
                .expect_err("create or write authority cannot authorize deletion");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
            assert_eq!(host.resolve(&workspace, None).await?, before);
        }
        let structured_delete = actions(&[Action::Delete])?;
        let volume_delete =
            crate::Capabilities::new([volume.capability(VolumeOperation::Delete, &mut None)?]);
        for (index, delete) in [structured_delete, volume_delete].iter().enumerate() {
            let first = original.files().nth(index).unwrap();
            let before = host.resolve(&workspace, None).await?;
            let delete_access = ContentAccess {
                ceiling: Some(delete),
                io: io.as_ref(),
            };
            for changes in [
                &[add("delete-cannot-create")] as &[_],
                &[ContentChange::UpdateExpected {
                    source: first,
                    bytes: b"delete-cannot-write",
                }],
                &[
                    ContentChange::DeleteExpected { source: first },
                    add("delete-cannot-authorize-a-later-create"),
                ],
            ] {
                let denied = async {
                    host.put_changes(
                        &volume,
                        &write,
                        changes,
                        &IdempotencyKey::new("conditional-delete-denied")?,
                        64,
                        delete_access,
                        None,
                    )?
                    .await
                }
                .await
                .expect_err("a delete ceiling cannot authorize creation or replacement");
                assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
                assert_eq!(host.resolve(&workspace, None).await?, before);
            }
            let denied = host
                .observe_changes(&volume, &write, &key, 64_000, delete_access)?
                .await
                .expect_err("a delete ceiling cannot recover a creation receipt");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
            let delete_key = IdempotencyKey::new(if index == 0 {
                "conditional-structured-delete"
            } else {
                "conditional-volume-delete"
            })?;
            let removed = host
                .put_changes(
                    &volume,
                    &write,
                    &[ContentChange::DeleteExpected { source: first }],
                    &delete_key,
                    64,
                    delete_access,
                    None,
                )?
                .await
                .expect("a delete-only ceiling must publish deletions");
            assert_eq!(removed.members.len(), 1);
            assert_eq!(removed.files().count(), 0);
            let replay = host
                .put_changes(
                    &volume,
                    &write,
                    &[ContentChange::DeleteExpected { source: first }],
                    &delete_key,
                    64,
                    delete_access,
                    None,
                )?
                .await?;
            assert_eq!(replay, removed);
            let observed = host
                .observe_changes(&volume, &write, &delete_key, 64_000, delete_access)?
                .await?
                .expect("the delete ceiling can recover its deletion receipt");
            assert_eq!(observed.generation, removed.generation);
            assert_eq!(observed.members, removed.members);
        }
        // The original immutable source remains readable after deletion.
        assert_eq!(
            host.read_content(first, &read, 64, None).await?.as_ref(),
            b"first"
        );
    }
    Ok(())
}

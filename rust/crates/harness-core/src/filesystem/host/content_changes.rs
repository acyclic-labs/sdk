//! Conditional file changes share the existing authored transaction, successful
//! operation index, protected publication receipt and commit authorization.
use super::*;
use crate::conversation::{ContentAccess, ContentChange, ContentChangeResult, ContentChanges};
use acyclic_fs::access::Action;

// One action determines both the receipt shape and its recovery authorization.
// Version 2 has no optional-field combinations or synthetic deleted FileRef.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ChangeRecord<S, F, P, G> {
    Create { file: F },
    Update { source: S, file: F },
    DeleteExpected { source: S },
    DeleteAt { source: G },
    Delete { path: P },
}

impl<S, F, P, G> ChangeRecord<S, F, P, G> {
    fn file(&self) -> Option<&F> {
        match self {
            Self::Create { file } | Self::Update { file, .. } => Some(file),
            Self::DeleteExpected { .. } | Self::DeleteAt { .. } | Self::Delete { .. } => None,
        }
    }
}

type StoredChange = ChangeRecord<FileRef, FileRef, String, crate::conversation::PathSelection>;

#[derive(Deserialize, crate::HeapLayout)]
struct ChangeReceipt(u8, Vec<StoredChange>);

impl UploadRecord for ChangeReceipt {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.1.iter().try_fold(
            self.1.capacity() as u64 * std::mem::size_of::<StoredChange>() as u64,
            |bytes, record| {
                let owned = match record {
                    ChangeRecord::Create { file } => file.logical_owned_bytes()?,
                    ChangeRecord::Update { source, file } => crate::core::owned_sum([
                        source.logical_owned_bytes()?,
                        file.logical_owned_bytes()?,
                    ])?,
                    ChangeRecord::DeleteExpected { source } => source.logical_owned_bytes()?,
                    ChangeRecord::DeleteAt { source } => source.logical_owned_bytes()?,
                    ChangeRecord::Delete { path } => path.capacity() as u64,
                };
                crate::core::owned_sum([bytes, owned])
            },
        )
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) fn check_change_destinations<'a>(
        &'a self,
        volume: &'a VolumeRef,
        grant: &'a ContentGrant,
        changes: &'a [ContentChange<'a>],
        access: ContentAccess<'a, 'a>,
        inspect_destinations: bool,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        grant.require(volume, VolumeOperation::Write, access.io)?;
        crate::conversation::BoundedContentFuture::new(access.io, || async move {
            let ordinary;
            let io = if let Some(io) = access.io {
                io
            } else {
                ordinary = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &ordinary
            };
            let mut checked_actions = [false; 2];
            for change in changes {
                require_change_ceiling(
                    change.action(),
                    volume,
                    access.ceiling,
                    &mut checked_actions,
                    io,
                )?;
                crate::conversation::validate_content_path(
                    change.path(),
                    &mut Some(&mut io.history_io()),
                )?;
                InternalContentClass::require_path(None, change.path(), Some(io))?;
            }
            let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
            let root = self.open(&reference, Some(io)).await?;
            let opened = self.select_current_workspace(&root, io).await?;
            let payload = change_payload(grant.policy_scope(), volume, io)?;
            let root_view = if root.id() != opened.id() {
                Some(
                    access::access_view(
                        &root,
                        grant.policy_scope(),
                        None,
                        payload,
                        access.ceiling,
                        io,
                    )
                    .await?,
                )
            } else {
                None
            };
            let view = access::access_view(
                &opened,
                grant.policy_scope(),
                None,
                payload,
                access.ceiling,
                io,
            )
            .await?;
            // Authorize the complete batch before inspecting any destination.
            // Create-only callers need Create even when the target is present.
            for change in changes {
                access::require(&view.0, change.action(), change.path(), io)?;
                if let Some(root_view) = &root_view {
                    access::require(&root_view.0, change.action(), change.path(), io)?;
                }
            }
            if !inspect_destinations {
                return Ok(());
            }
            let selected = io.sdk(opened.generation(view.2, io, io.cancellation()).await)?;
            for change in changes {
                if matches!(
                    change,
                    ContentChange::CreateAbsent { .. } | ContentChange::UpdateExpected { .. }
                ) {
                    let absolute = crate::executor::preparation_format(
                        format_args!("/{}", change.path()),
                        &mut Some(&mut io.history_io()),
                    )?;
                    if io
                        .sdk_if_present(selected.stat(&absolute, io, io.cancellation()).await)?
                        .is_some_and(|stat| stat.kind == acyclic_fs::kernel::FileKind::Directory)
                    {
                        return Err(io.invalid("path is a directory"));
                    }
                }
            }
            Ok(())
        })
    }

    pub(in crate::filesystem) fn observe_changes<'a>(
        &'a self,
        volume: &'a VolumeRef,
        grant: &'a ContentGrant,
        key: &'a IdempotencyKey,
        maximum_receipt_bytes: Option<u64>,
        access: ContentAccess<'a, 'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<ContentChanges>>> {
        grant.require(volume, VolumeOperation::Write, access.io)?;
        crate::conversation::BoundedContentFuture::new(access.io, || async move {
            let ordinary;
            let io = if let Some(io) = access.io {
                io
            } else {
                ordinary = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &ordinary
            };
            let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
            let root = self.open(&reference, Some(io)).await?;
            let operation = bounded_io::operation_key(key, Some(io))?;
            let Some((opened, binding)) = self
                .content_operation_workspace(&root, operation, false, io)
                .await?
            else {
                return Ok(None);
            };
            let Some(publication) = io.sdk(
                opened
                    .operation_publication(operation, maximum_receipt_bytes, io, io.cancellation())
                    .await,
            )?
            else {
                return Ok(None);
            };
            let generation = publication.generation();
            let bytes = super::publication::publication_evidence(publication.evidence(), Some(io))?;
            let receipt: ChangeReceipt = io.decode_json(bytes)?;
            if receipt.0 != 1 {
                return Err(io.invalid("unsupported conditional publication receipt"));
            }
            let payload = change_payload(grant.policy_scope(), volume, io)?;
            let view = access::access_view(
                &opened,
                grant.policy_scope(),
                None,
                payload,
                access.ceiling,
                io,
            )
            .await?;
            let root_view = if root.id() != opened.id() {
                Some(
                    access::access_view(
                        &root,
                        grant.policy_scope(),
                        None,
                        payload,
                        access.ceiling,
                        io,
                    )
                    .await?,
                )
            } else {
                None
            };
            let mut checked_actions = [false; 2];
            for record in &receipt.1 {
                let (path, action, identity) = match record {
                    ChangeRecord::Create { file } => (file.path(), Action::Create, Some(file)),
                    ChangeRecord::Update { file, .. } => (file.path(), Action::Write, Some(file)),
                    ChangeRecord::DeleteExpected { source } => {
                        (source.path(), Action::Delete, Some(source))
                    }
                    ChangeRecord::DeleteAt { source } => {
                        source.validate(&mut Some(&mut io.history_io()))?;
                        if &source.volume != volume {
                            return Err(io.invalid("conditional receipt volume differs"));
                        }
                        (source.path.as_str(), Action::Delete, None)
                    }
                    ChangeRecord::Delete { path } => (path.as_str(), Action::Delete, None),
                };
                require_change_ceiling(action, volume, access.ceiling, &mut checked_actions, io)?;
                crate::conversation::validate_content_path(path, &mut Some(&mut io.history_io()))?;
                InternalContentClass::require_path(None, path, Some(io))?;
                if let Some(file) = identity {
                    file.validate(&mut Some(&mut io.history_io()))?;
                    if file.volume() != volume {
                        return Err(io.invalid("conditional receipt volume differs"));
                    }
                }
                access::require(&view.0, action, path, io)?;
                if let Some(root_view) = &root_view {
                    access::require(&root_view.0, action, path, io)?;
                }
            }
            if let Some(root_view) = &root_view {
                access::require_current(&root, &root_view.0, io).await?;
            }
            if let Some(binding) = binding {
                bounded_io::retain(&binding, Some(io)).await?;
            }
            access::require_current(&opened, &view.0, io).await?;
            grant.recheck(Some(io))?;
            bounded_io::retain(generation, Some(io)).await?;
            let count = receipt.1.len();
            self.finish_change_files(
                generation,
                receipt
                    .1
                    .into_iter()
                    .map(|record| change_result(record, io)),
                count,
                io,
            )
            .map(Some)
        })
    }
    pub(in crate::filesystem) fn put_changes<'a>(
        &'a self,
        volume: &'a VolumeRef,
        grant: &'a ContentGrant,
        changes: &'a [ContentChange<'a>],
        key: &'a IdempotencyKey,
        maximum_bytes: Option<u64>,
        access: ContentAccess<'a, 'a>,
        check: Option<&'a (dyn Fn(Action, &str) -> Result<()> + Send + Sync)>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ContentChanges>> {
        let ContentAccess { ceiling, io } = access;
        grant.require(volume, VolumeOperation::Write, io)?;
        if volume.provider() != &self.provider || changes.is_empty() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "conditional publication needs a nonempty bound-volume request",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let ordinary;
            let io = match io {
                Some(io) => io,
                None => {
                    ordinary =
                        PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                    &ordinary
                }
            };
            let operation = bounded_io::operation_key(key, Some(io))?;
            let version = bounded_io::hex_key(&operation.into_bytes(), Some(io))?;
            let mut paths = crate::model::vec_with_capacity(changes.len(), Some(io))?;
            paths.extend(changes.iter().map(ContentChange::path));
            let mut compared = 0_u64;
            paths.sort_unstable_by(|left, right| {
                compared = compared.saturating_add(1);
                left.cmp(right)
            });
            io.charge(WorkCounters {
                items_examined: compared,
                ..Default::default()
            })?;
            if paths.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(io.invalid("conditional publication repeats a destination path"));
            }
            drop(paths);
            let mut records = crate::model::vec_with_capacity(changes.len(), Some(io))?;
            // The ordered receipt contains source identity and resulting
            // descriptors, never another copy of the file contents.
            let mut checked_actions = [false; 2];
            for change in changes {
                crate::model::charge_items(Some(io), 1)?;
                require_change_ceiling(change.action(), volume, ceiling, &mut checked_actions, io)?;
                crate::conversation::validate_content_path(
                    change.path(),
                    &mut Some(&mut io.history_io()),
                )?;
                InternalContentClass::require_path(None, change.path(), Some(io))?;
                if let Some(source) = change.source() {
                    source.validate(&mut Some(&mut io.history_io()))?;
                    if source.volume() != volume {
                        return Err(io.invalid("conditional source belongs to another volume"));
                    }
                }
                if let Some(source) = change.path_source() {
                    source.validate(&mut Some(&mut io.history_io()))?;
                    if &source.volume != volume {
                        return Err(io.invalid("conditional source belongs to another volume"));
                    }
                }
                let output = match change {
                    ContentChange::CreateAbsent {
                        path,
                        bytes,
                        media_type,
                        display_name,
                    } => Some((*path, *bytes, *media_type, *display_name)),
                    ContentChange::UpdateExpected { source, bytes } => Some((
                        source.path(),
                        *bytes,
                        source.descriptor().media_type(),
                        source.display_name(),
                    )),
                    ContentChange::DeleteExpected { .. }
                    | ContentChange::DeleteAt { .. }
                    | ContentChange::Delete { .. } => None,
                };
                let file = if let Some((path, bytes, media, display)) = output {
                    if maximum_bytes.is_some_and(|maximum| bytes.len() as u64 > maximum) {
                        return Err(
                            io.invalid("conditional file exceeds admitted content capacity")
                        );
                    }
                    Some(FileRef::new(
                        volume.copy(&mut Some(&mut io.history_io()))?,
                        stock_string_copy(path, Some(io))?,
                        stock_string_copy(&version, Some(io))?,
                        FileDescriptor::from_bytes(bytes, media, &mut Some(&mut io.history_io()))?,
                        stock_string_copy(display, Some(io))?,
                        &mut Some(&mut io.history_io()),
                    )?)
                } else {
                    None
                };
                records.push(match change {
                    ContentChange::CreateAbsent { .. } => ChangeRecord::Create {
                        file: file.ok_or_else(|| io.invalid("create has no staged file"))?,
                    },
                    ContentChange::UpdateExpected { source, .. } => ChangeRecord::Update {
                        source: *source,
                        file: file.ok_or_else(|| io.invalid("update has no staged file"))?,
                    },
                    ContentChange::DeleteExpected { source } => {
                        ChangeRecord::DeleteExpected { source: *source }
                    }
                    ContentChange::DeleteAt { source } => {
                        ChangeRecord::DeleteAt { source: *source }
                    }
                    ContentChange::Delete { path } => ChangeRecord::Delete { path: *path },
                });
            }
            // Versioned as a fresh public contract; the key uses the original
            // workspace operation index and the receipt commits with all files.
            let receipt = io.encode(&(1_u8, &records), io.remaining()?.bytes_encoded, true)?;
            let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
            let root = self.open(&reference, Some(io)).await?;
            let binding = self
                .content_operation_workspace(&root, operation, false, io)
                .await?;
            let needs_binding = binding.is_none();
            let (opened, binding) = match binding {
                Some(binding) => binding,
                None => (self.select_current_workspace(&root, io).await?, None),
            };
            let payload = change_payload(grant.policy_scope(), volume, io)?;
            let root_view = if opened.id() != root.id() || needs_binding {
                Some(
                    access::access_view(&root, grant.policy_scope(), None, payload, ceiling, io)
                        .await?,
                )
            } else {
                None
            };
            if let Some(root_view) = &root_view {
                for change in changes {
                    access::require(&root_view.0, change.action(), change.path(), io)?;
                }
                if needs_binding {
                    let fence = root_view
                        .0
                        .publication_fence()
                        .ok_or_else(|| io.invalid("missing root publication policy fence"))?;
                    self.bind_content_operation(&root, opened.id(), operation, (fence, grant), io)?
                        .await?;
                }
            }
            if let Some(binding) = binding {
                bounded_io::retain(&binding, Some(io)).await?;
            }
            let prior = io.sdk(
                opened
                    .operation_publication(operation, None, io, io.cancellation())
                    .await,
            )?;
            let root_cut = if opened.id() != root.id() && prior.is_none() {
                Some(io.sdk(root.capture_policy_cut(io, io.cancellation()).await)?)
            } else {
                None
            };
            let view =
                access::access_view(&opened, grant.policy_scope(), None, payload, ceiling, io)
                    .await?;
            for change in changes {
                let action = change.action();
                if let Some(check) = check {
                    check(action, change.path())?;
                }
                access::require(&view.0, action, change.path(), io)?;
            }
            let generation = if let Some(prior) = prior {
                if super::publication::publication_evidence(prior.evidence(), Some(io))?
                    != receipt.as_ref()
                {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "conditional publication identity belongs to a different request",
                    ));
                }
                access::require_current(&opened, &view.0, io).await?;
                if let Some(root_view) = &root_view {
                    access::require_current(&root, &root_view.0, io).await?;
                }
                prior.into_generation()
            } else {
                let selected = io.sdk(opened.generation(view.2, io, io.cancellation()).await)?;
                let selected_ref = self.generation_ref_id(view.2, Some(io))?;
                // All conditions are resolved at this one cut. The original
                // transaction observes these bindings and publishes once.
                for change in changes {
                    let content_generation = change
                        .source()
                        .map(|source| self.file_generation(source, Some(io)))
                        .transpose()?;
                    let source_generation = content_generation
                        .as_ref()
                        .or_else(|| change.path_source().map(|source| &source.generation));
                    if let Some(source_generation) = source_generation {
                        let absolute = crate::executor::preparation_format(
                            format_args!("/{}", change.path()),
                            &mut Some(&mut io.history_io()),
                        )?;
                        if let Some(source) = change.source() {
                            let pinned = if source_generation == &selected_ref {
                                selected.clone()
                            } else {
                                self.generation(&opened, source_generation, Some(io))
                                    .await?
                            };
                            Self::authenticate_selected_file(
                                &opened, &pinned, source, &absolute, io,
                            )
                            .await?;
                        }
                        if source_generation != &selected_ref {
                            let metadata =
                                content_metadata_key_io(volume, change.path(), Some(io))?;
                            self.require_unchanged_content(
                                &opened,
                                source_generation,
                                &selected_ref,
                                (&absolute, &metadata),
                                Some(io),
                            )?
                            .await?;
                        }
                    }
                }
                let mut transaction = io.sdk(
                    opened
                        .begin_transaction_at(&selected, operation, io, io.cancellation())
                        .await,
                )?;
                let fence = view
                    .0
                    .publication_fence()
                    .ok_or_else(|| io.invalid("missing conditional publication policy fence"))?;
                io.sdk(
                    transaction
                        .observe_policy_fence(fence, io, io.cancellation())
                        .await,
                )?;
                for (change, record) in changes.iter().zip(&records) {
                    let absolute = crate::executor::preparation_format(
                        format_args!("/{}", change.path()),
                        &mut Some(&mut io.history_io()),
                    )?;
                    match change {
                        ContentChange::CreateAbsent { bytes, .. } => {
                            // Creating parent directories is itself conditional
                            // on the policy at the same cut, before any commit.
                            if change.path().contains('/') {
                                let (parent, _) = absolute
                                    .rsplit_once('/')
                                    .ok_or_else(|| io.invalid("missing parent"))?;
                                io.sdk(
                                    transaction
                                        .create_dir_all_checked(
                                            parent,
                                            io,
                                            io.cancellation(),
                                            |prefix| {
                                                let candidate = prefix.trim_start_matches('/');
                                                access::require(
                                                    &view.0,
                                                    Action::Create,
                                                    candidate,
                                                    io,
                                                )?;
                                                if let Some(root_view) = &root_view {
                                                    access::require(
                                                        &root_view.0,
                                                        Action::Create,
                                                        candidate,
                                                        io,
                                                    )?;
                                                }
                                                if let Some(check) = check {
                                                    check(Action::Create, candidate)?;
                                                }
                                                Ok::<_, Error>(())
                                            },
                                        )
                                        .await,
                                )?;
                            }
                            let (written, recorded) = transaction
                                .write_if(
                                    &absolute,
                                    bounded_io::clone_bytes(bytes, Some(io))?,
                                    acyclic_fs::FileCondition::Absent,
                                    io,
                                    io.cancellation(),
                                )
                                .await;
                            // This API's StaleIdentity is exactly a false file
                            // condition. Other SDK and accounting failures keep
                            // their original terminal cause; sdk checks the
                            // recorded native work before the condition result.
                            let written = io.sdk((
                                written.map_err(|error| match error {
                                    WorkspaceError::StaleIdentity => Error::ToolRejected {
                                        code: crate::wire::ErrorCode::Conflict,
                                        reason: "conditional file already exists",
                                    },
                                    error => Error::from(error),
                                }),
                                recorded,
                            ));
                            if let Err(error) = written {
                                // Diagnose the rejected condition at its original
                                // authorized cut. Successful creates need no extra read.
                                if matches!(
                                    error,
                                    Error::ToolRejected {
                                        code: crate::wire::ErrorCode::Conflict,
                                        reason: "conditional file already exists",
                                    }
                                ) && io
                                    .sdk_if_present(
                                        selected.stat(&absolute, io, io.cancellation()).await,
                                    )?
                                    .is_some_and(|stat| {
                                        stat.kind == acyclic_fs::kernel::FileKind::Directory
                                    })
                                {
                                    return Err(io.invalid("path is a directory"));
                                }
                                return Err(error);
                            }
                        }
                        ContentChange::UpdateExpected { bytes, .. } => {
                            io.sdk(
                                transaction
                                    .write(
                                        &absolute,
                                        bounded_io::clone_bytes(bytes, Some(io))?,
                                        io,
                                        io.cancellation(),
                                    )
                                    .await,
                            )?;
                        }
                        ContentChange::DeleteExpected { .. }
                        | ContentChange::DeleteAt { .. }
                        | ContentChange::Delete { .. } => {
                            io.sdk(transaction.remove(&absolute, io, io.cancellation()).await)?;
                        }
                    }
                    if let Some(file) = record.file() {
                        let metadata_key =
                            content_metadata_key_io(volume, change.path(), Some(io))?;
                        let bytes = io.encode(
                            &(1_u8, (file, Option::<&GenerationRef>::None)),
                            io.remaining()?.bytes_encoded,
                            true,
                        )?;
                        io.sdk(
                            transaction
                                .change_named_attribute(
                                    &absolute,
                                    &metadata_key,
                                    Some((
                                        bytes.as_ref(),
                                        acyclic_fs::NamedAttributeWriteMode::Upsert,
                                    )),
                                    io,
                                    io.cancellation(),
                                )
                                .await,
                        )?;
                    }
                    // Removing a file removes its annotation in the same mutation.
                }
                let root_fence = root_view
                    .as_ref()
                    .and_then(|view| view.0.publication_fence());
                let publication = Self::commit_contextual_publication(
                    transaction,
                    io,
                    workspaces::FilePublication::Authorized(fence, grant, root_fence),
                    Some(workspaces::publication_context(receipt, io)?),
                )?;
                match root_cut {
                    Some(cut) => cut.scope(publication).await?,
                    None => publication.await?,
                }
            };
            bounded_io::retain(&generation, Some(io)).await?;
            grant.recheck(Some(io))?;
            let count = records.len();
            self.finish_change_files(
                &generation,
                records.into_iter().map(|record| change_result(record, io)),
                count,
                io,
            )
        })
    }

    fn finish_change_files(
        &self,
        generation: &Generation<A, O>,
        originals: impl Iterator<Item = Result<ContentChangeResult>>,
        capacity: usize,
        io: &PreparationIo,
    ) -> Result<ContentChanges> {
        let generation_ref = self.generation_ref_id(generation.id(), Some(io))?;
        let version = bounded_io::hex_key(generation_ref.as_resource().key(), Some(io))?;
        let mut members = crate::model::vec_with_capacity(capacity, Some(io))?;
        for member in originals {
            let member = member?;
            let member = match member {
                ContentChangeResult::Created(file) => {
                    ContentChangeResult::Created(self.finish_change_file(file, &version, io)?)
                }
                ContentChangeResult::Updated(file) => {
                    ContentChangeResult::Updated(self.finish_change_file(file, &version, io)?)
                }
                deleted @ ContentChangeResult::Deleted(_) => deleted,
            };
            members.push(member);
        }
        Ok(ContentChanges {
            generation: generation_ref,
            members,
        })
    }

    fn finish_change_file(
        &self,
        mut file: FileRef,
        version: &str,
        io: &PreparationIo,
    ) -> Result<FileRef> {
        file.pin_version(
            stock_string_copy(version, Some(io))?,
            &mut Some(&mut io.history_io()),
        )?;
        self.seal_publication(file, Some(io))
    }
}

// The authenticated logical-volume admission precedes this conversion. A plain
// write grant remains create/replace only; removal needs its own signed label.
fn change_payload<'a>(
    scope: &'a Scope,
    volume: &VolumeRef,
    io: &PreparationIo,
) -> Result<&'a [u8]> {
    let grants = scope.capabilities();
    let label = crate::core::filesystem_scope_label(grants, &mut Some(&mut io.history_io()))?;
    if label.is_some() {
        return access::capability_payload(label, VolumeOperation::Write, io);
    }
    let delete = crate::core::contains_content_grant_with_work(
        grants,
        volume,
        VolumeOperation::Delete.capability_prefix(),
        &mut Some(&mut io.history_io()),
    )?;
    access::whole_volume_payload(if delete {
        acyclic_fs::access::ActionSet::DestinationEdit
    } else {
        acyclic_fs::access::ActionSet::DestinationWrite
    })
}

fn require_change_ceiling(
    action: Action,
    volume: &VolumeRef,
    ceiling: Option<&crate::Capabilities>,
    checked_actions: &mut [bool; 2],
    io: &PreparationIo,
) -> Result<()> {
    let checked = &mut checked_actions[usize::from(action == Action::Delete)];
    if *checked {
        return Ok(());
    }
    let operation = if action == Action::Delete {
        VolumeOperation::Delete
    } else {
        VolumeOperation::Write
    };
    if let Some(grants) = ceiling
        && crate::core::filesystem_scope_label(grants, &mut Some(&mut io.history_io()))?.is_none()
        && !crate::core::contains_content_grant_with_work(
            grants,
            volume,
            operation.capability_prefix(),
            &mut Some(&mut io.history_io()),
        )?
    {
        return Err(stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Unauthorized,
            "content caller lacks the requested filesystem action grant",
        ));
    }
    *checked = true;
    Ok(())
}

fn change_result<
    S: std::borrow::Borrow<FileRef>,
    P: AsRef<str>,
    G: std::borrow::Borrow<crate::conversation::PathSelection>,
>(
    record: ChangeRecord<S, FileRef, P, G>,
    io: &PreparationIo,
) -> Result<ContentChangeResult> {
    Ok(match record {
        ChangeRecord::Create { file } => ContentChangeResult::Created(file),
        ChangeRecord::Update { file, .. } => ContentChangeResult::Updated(file),
        ChangeRecord::DeleteExpected { source } => {
            ContentChangeResult::Deleted(stock_string_copy(source.borrow().path(), Some(io))?)
        }
        ChangeRecord::DeleteAt { source } => {
            ContentChangeResult::Deleted(stock_string_copy(&source.borrow().path, Some(io))?)
        }
        ChangeRecord::Delete { path } => {
            ContentChangeResult::Deleted(stock_string_copy(path.as_ref(), Some(io))?)
        }
    })
}

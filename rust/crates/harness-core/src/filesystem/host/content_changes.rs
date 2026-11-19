//! Conditional file changes share the existing authored transaction, successful
//! operation index, protected publication receipt and commit authorization.
use super::*;
use crate::conversation::{
    ContentAccess, ContentChange, ContentChangeResult, ContentChanges, FileIdentity, PathSelection,
};
use acyclic_fs::access::Action;
use futures::future::Either::{Left, Right};

// One action determines both the receipt shape and its recovery authorization.
// Version 2 has no optional-field combinations or synthetic deleted FileRef.
#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ChangeRecord<S, F, P, G> {
    Create { file: F },
    Update { file: F, source: S },
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

type StoredChange = ChangeRecord<FileRef, FileRef, String, PathSelection>;
type BorrowedChange<'a> = ChangeRecord<&'a FileRef, PendingFile<'a>, &'a str, &'a PathSelection>;

struct PendingFile<'a> {
    volume: &'a VolumeRef,
    path: &'a str,
    version: &'a str,
    descriptor: FileDescriptor,
    display_name: &'a str,
}

impl PendingFile<'_> {
    fn identity(&self) -> FileIdentity<'_> {
        FileIdentity {
            volume: self.volume,
            path: self.path,
            version: self.version,
            descriptor: &self.descriptor,
            display_name: self.display_name,
        }
    }

    fn pin(self, version: String, io: &PreparationIo) -> Result<FileRef> {
        FileRef::new(
            self.volume.copy(&mut Some(&mut io.history_io()))?,
            stock_string_copy(self.path, Some(io))?,
            version,
            self.descriptor,
            stock_string_copy(self.display_name, Some(io))?,
            &mut Some(&mut io.history_io()),
        )
    }
}

fn change_parent<'a>(change: &'a ContentChange<'_>) -> Option<&'a str> {
    change.path().rsplit_once('/').map(|(parent, _)| parent)
}

fn absolute_change_path(path: &str, io: &PreparationIo) -> Result<String> {
    crate::executor::preparation_format(format_args!("/{path}"), &mut Some(&mut io.history_io()))
}

fn change_annotation(
    namespace: &str,
    file: &PendingFile<'_>,
    io: &PreparationIo,
) -> Result<(AttributeName, acyclic_stream::OwnedBytes)> {
    let key = super::keys::content_metadata_key_in_namespace_io(namespace, file.path, Some(io))?;
    let bytes = io.encode(
        &(1_u8, (file.identity(), Option::<&GenerationRef>::None)),
        io.remaining()?.bytes_encoded,
        false,
    )?;
    Ok((key, bytes))
}

/// The receipt has a closed shape. Stream its existing canonical field order
/// and borrow its descriptors instead of constructing an owned JSON tree.
struct OrderedChanges<'a, 'source>(&'a [BorrowedChange<'source>]);

impl Serialize for OrderedChanges<'_, '_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|record| match record {
            ChangeRecord::Create { file } => ChangeRecord::Create {
                file: file.identity(),
            },
            ChangeRecord::Update { file, source } => ChangeRecord::Update {
                file: file.identity(),
                source: source.canonical(),
            },
            ChangeRecord::DeleteExpected { source } => ChangeRecord::DeleteExpected {
                source: source.canonical(),
            },
            ChangeRecord::DeleteAt { source } => ChangeRecord::DeleteAt {
                source: source.canonical(),
            },
            ChangeRecord::Delete { path } => ChangeRecord::Delete { path },
        }))
    }
}

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
                InternalContentClass::require_path(volume, None, change.path(), Some(io))?;
            }
            let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
            let root = self.open(&reference, Some(io)).await?;
            let opened = self.select_current_workspace(&root, io).await?;
            let payload = change_payload(grant.policy_scope(), volume, io)?;
            let now = self.access_now()?;
            let root_view = if root.id() != opened.id() {
                Some(
                    access::access_view(
                        &root,
                        grant.policy_scope(),
                        None,
                        payload,
                        access.ceiling,
                        io,
                        now,
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
                now,
            )
            .await?;
            // Authorize the complete batch before inspecting any destination.
            // Create-only callers need Create even when the target is present.
            for change in changes {
                self.require(&view.0, change.action(), change.path(), io)?;
                if let Some(root_view) = &root_view {
                    self.require(&root_view.0, change.action(), change.path(), io)?;
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
            let now = self.access_now()?;
            let view = access::access_view(
                &opened,
                grant.policy_scope(),
                None,
                payload,
                access.ceiling,
                io,
                now,
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
                        now,
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
                InternalContentClass::require_path(volume, None, path, Some(io))?;
                if let Some(file) = identity {
                    file.validate(&mut Some(&mut io.history_io()))?;
                    if file.volume() != volume {
                        return Err(io.invalid("conditional receipt volume differs"));
                    }
                }
                self.require(&view.0, action, path, io)?;
                if let Some(root_view) = &root_view {
                    self.require(&root_view.0, action, path, io)?;
                }
            }
            if let Some(root_view) = &root_view {
                access::require_current(&root, &root_view.0, io, self.clock()).await?;
            }
            if let Some(binding) = binding {
                bounded_io::retain(&binding, Some(io)).await?;
            }
            access::require_current(&opened, &view.0, io, self.clock()).await?;
            grant.recheck(Some(io))?;
            bounded_io::retain(generation, Some(io)).await?;
            self.finish_change_files(
                operation,
                generation,
                receipt.1.into_iter(),
                |mut file, version, io| {
                    file.pin_version(version, &mut Some(&mut io.history_io()))?;
                    Ok(file)
                },
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
            let mut paths = crate::model::vec_with_capacity(changes.len(), Some(io))?;
            let mut writes = false;
            for change in changes {
                paths.push(change.path());
                writes |= change.action() != Action::Delete;
            }
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
            let version = if writes {
                bounded_io::hex_key(&operation.into_bytes(), Some(io))?
            } else {
                String::new()
            };
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
                InternalContentClass::require_path(volume, None, change.path(), Some(io))?;
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
                    let file = PendingFile {
                        volume,
                        path,
                        version: &version,
                        descriptor: FileDescriptor::from_bytes(
                            bytes,
                            media,
                            &mut Some(&mut io.history_io()),
                        )?,
                        display_name: display,
                    };
                    file.identity().validate(&mut Some(&mut io.history_io()))?;
                    Some(file)
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
            let now = self.access_now()?;
            let root_view = if opened.id() != root.id() || needs_binding {
                Some(
                    access::access_view(
                        &root,
                        grant.policy_scope(),
                        None,
                        payload,
                        ceiling,
                        io,
                        now,
                    )
                    .await?,
                )
            } else {
                None
            };
            if let Some(root_view) = &root_view {
                for change in changes {
                    self.require(&root_view.0, change.action(), change.path(), io)?;
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
                    .operation_publication_seal(operation, io, io.cancellation())
                    .await,
            )?;
            let root_cut = if opened.id() != root.id() && prior.is_none() {
                Some(io.sdk(root.capture_policy_cut(io, io.cancellation()).await)?)
            } else {
                None
            };
            let view = access::access_view(
                &opened,
                grant.policy_scope(),
                None,
                payload,
                ceiling,
                io,
                now,
            )
            .await?;
            for change in changes {
                let action = change.action();
                if let Some(check) = check {
                    check(action, change.path())?;
                }
                self.require(&view.0, action, change.path(), io)?;
            }
            let generation = if let Some((generation, evidence)) = prior {
                let evidence = super::publication::publication_evidence(evidence, Some(io))?;
                if io.publication_evidence_seal(&(1_u8, OrderedChanges(&records)))? != evidence {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "conditional publication identity belongs to a different request",
                    ));
                }
                access::require_current(&opened, &view.0, io, self.clock()).await?;
                if let Some(root_view) = &root_view {
                    access::require_current(&root, &root_view.0, io, self.clock()).await?;
                }
                generation
            } else {
                let namespace = std::str::from_utf8(reference.as_resource().key())
                    .map_err(|_| io.invalid("workspace name is not UTF-8"))?;
                // The receipt shares the original operation index and commits with all files.
                let receipt = io.encode(
                    &(1_u8, OrderedChanges(&records)),
                    io.remaining()?.bytes_encoded,
                    false,
                )?;
                let selected = io.sdk(opened.generation(view.2, io, io.cancellation()).await)?;
                let selected_key = selected.id().to_bytes();
                // All conditions are resolved at this one cut. The original
                // transaction observes these bindings and publishes once.
                for change in changes {
                    let file = change.source();
                    let source = if let Some(file) = file {
                        Left(self.file_generation_id(file, Some(io))?)
                    } else if let Some(path) = change.path_source() {
                        Right(&path.generation)
                    } else {
                        continue;
                    };
                    let same_generation = match source {
                        Left(generation) => generation == selected.id(),
                        Right(reference) => {
                            let source = reference.as_resource();
                            source.provider() == &self.provider
                                && source.version().is_none()
                                && source.key() == selected_key.as_slice()
                        }
                    };
                    if same_generation && file.is_none() {
                        continue;
                    }
                    let absolute = absolute_change_path(change.path(), io)?;
                    let _path_hold = io.guard_owned(absolute.capacity() as u64)?;
                    let metadata = super::keys::content_metadata_key_in_namespace_io(
                        namespace,
                        change.path(),
                        Some(io),
                    )?;
                    let _key_hold = io.guard_owned(metadata.retained_bytes() as u64)?;
                    let annotation = (absolute.as_str(), &metadata);
                    let pinned = if same_generation {
                        selected.clone()
                    } else {
                        match source {
                            Left(generation) => {
                                self.select_generation(&opened, generation, io).await?
                            }
                            Right(reference) => {
                                self.generation(&opened, reference, Some(io)).await?
                            }
                        }
                    };
                    if let Some(source) = file {
                        Self::authenticate_selected_file(&opened, &pinned, source, annotation, io)
                            .await?;
                    }
                    if !same_generation {
                        Self::require_unchanged_generations(&pinned, &selected, annotation, io)
                            .await?;
                    }
                }
                let mut transaction = io.sdk(
                    opened
                        .begin_transaction_at(&selected, operation, io, io.cancellation())
                        .await,
                )?;
                #[cfg(test)]
                if let Some(pause) = &self.publication_pause {
                    pause().await;
                }
                let fence = view
                    .0
                    .publication_fence()
                    .ok_or_else(|| io.invalid("missing conditional publication policy fence"))?;
                io.sdk(
                    transaction
                        .observe_policy_fence(fence, self.access_now()?, io, io.cancellation())
                        .await,
                )?;
                let mut remaining = records.as_slice();
                for group in changes.chunk_by(|left, right| {
                    matches!(
                        (left, right),
                        (
                            ContentChange::CreateAbsent { .. },
                            ContentChange::CreateAbsent { .. },
                        )
                    ) && change_parent(left) == change_parent(right)
                }) {
                    let (group_records, rest) = remaining
                        .split_at_checked(group.len())
                        .ok_or_else(|| io.invalid("missing conditional publication records"))?;
                    remaining = rest;
                    let [change, ..] = group else {
                        return Err(io.invalid("empty conditional publication group"));
                    };
                    match change {
                        ContentChange::CreateAbsent { .. } => {
                            if let Some(parent) = change_parent(change) {
                                let parent = absolute_change_path(parent, io)?;
                                io.sdk(
                                    transaction
                                        .create_dir_all_checked(
                                            &parent,
                                            io,
                                            io.cancellation(),
                                            |prefix| {
                                                let candidate = prefix.trim_start_matches('/');
                                                self.require(
                                                    &view.0,
                                                    Action::Create,
                                                    candidate,
                                                    io,
                                                )?;
                                                if let Some(root_view) = &root_view {
                                                    self.require(
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
                            Self::stage_created_files(
                                &mut transaction,
                                &selected,
                                group,
                                group_records,
                                namespace,
                                io,
                            )
                            .await?;
                        }
                        ContentChange::UpdateExpected { bytes, .. } => {
                            let absolute = absolute_change_path(change.path(), io)?;
                            let file = group_records
                                .first()
                                .and_then(ChangeRecord::file)
                                .ok_or_else(|| io.invalid("conditional update has no metadata"))?;
                            let (key, metadata) = change_annotation(namespace, file, io)?;
                            io.sdk(
                                transaction
                                    .write_with_attribute(
                                        &absolute,
                                        bounded_io::clone_bytes(bytes, Some(io))?,
                                        (&key, metadata.as_ref()),
                                        io,
                                        io.cancellation(),
                                    )
                                    .await,
                            )?;
                        }
                        ContentChange::DeleteExpected { .. } | ContentChange::DeleteAt { .. } => {
                            let path = absolute_change_path(change.path(), io)?;
                            io.sdk(transaction.remove(&path, io, io.cancellation()).await)?;
                        }
                        ContentChange::Delete { .. } => {
                            let path = absolute_change_path(change.path(), io)?;
                            io.sdk_if_present(
                                transaction.remove(&path, io, io.cancellation()).await,
                            )?;
                        }
                    }
                }
                let root_fence = root_view
                    .as_ref()
                    .and_then(|view| view.0.publication_fence());
                let publication = Self::commit_contextual_publication(
                    transaction,
                    io,
                    workspaces::FilePublication::Authorized(fence, grant, root_fence),
                    Some(workspaces::publication_context(receipt, io)?),
                    self.clock_handle(),
                )?;
                match root_cut {
                    Some(cut) => cut.scope(publication).await?,
                    None => publication.await?,
                }
            };
            bounded_io::retain(&generation, Some(io)).await?;
            grant.recheck(Some(io))?;
            self.finish_change_files(
                operation,
                &generation,
                records.into_iter(),
                PendingFile::pin,
                io,
            )
        })
    }

    async fn stage_created_files(
        transaction: &mut acyclic_fs::Transaction<A, O>,
        selected: &Generation<A, O>,
        changes: &[ContentChange<'_>],
        records: &[BorrowedChange<'_>],
        namespace: &str,
        io: &PreparationIo,
    ) -> Result<()> {
        let scope = crate::filesystem::PreparationScope::new(Some(io))?;
        let work = scope
            .io()
            .ok_or_else(|| io.invalid("missing create work scope"))?;
        let mut inputs = crate::model::vec_with_capacity(changes.len(), Some(work))?;
        for (change, record) in changes.iter().zip(records) {
            let ContentChange::CreateAbsent { path, bytes, .. } = change else {
                return Err(work.invalid("file creation group contains another action"));
            };
            let file = record
                .file()
                .ok_or_else(|| work.invalid("conditional create has no metadata"))?;
            let (key, metadata) = change_annotation(namespace, file, work)?;
            inputs.push((
                absolute_change_path(path, work)?,
                bounded_io::clone_bytes(bytes, Some(work))?,
                key,
                metadata,
            ));
        }
        let (written, receipt) = transaction
            .create_annotated_files(
                inputs.iter().map(|(path, bytes, key, metadata)| {
                    (path.as_str(), bytes.clone(), key, metadata.as_ref())
                }),
                work,
                work.cancellation(),
            )
            .await;
        let written = work.sdk((
            written.map_err(|error| match error {
                WorkspaceError::StaleIdentity => Error::ToolRejected {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "conditional file already exists",
                },
                WorkspaceError::IsDirectory => work.invalid("path is a directory"),
                error => Error::from(error),
            }),
            receipt,
        ));
        if matches!(
            &written,
            Err(Error::ToolRejected {
                code: crate::wire::ErrorCode::Conflict,
                reason: "conditional file already exists",
            })
        ) {
            for (path, ..) in &inputs {
                if let Some(stat) =
                    work.sdk_if_present(selected.stat(path, work, work.cancellation()).await)?
                {
                    if stat.kind == acyclic_fs::kernel::FileKind::Directory {
                        return Err(work.invalid("path is a directory"));
                    }
                    break;
                }
            }
        }
        written?;
        drop(inputs);
        scope.finish(0)
    }

    fn finish_change_files<
        S: std::borrow::Borrow<FileRef>,
        F,
        P: AsRef<str>,
        G: std::borrow::Borrow<PathSelection>,
    >(
        &self,
        operation: acyclic_fs::IdempotencyKey,
        generation: &Generation<A, O>,
        originals: impl ExactSizeIterator<Item = ChangeRecord<S, F, P, G>>,
        pin: impl Fn(F, String, &PreparationIo) -> Result<FileRef>,
        io: &PreparationIo,
    ) -> Result<ContentChanges> {
        let generation_ref = self.generation_ref_id(generation.id(), Some(io))?;
        let mut version = None;
        let mut members = crate::model::vec_with_capacity(originals.len(), Some(io))?;
        for record in originals {
            members.push(change_result(record, io, |file| {
                let version = match &mut version {
                    Some(version) => version,
                    slot @ None => slot.insert(bounded_io::hex_key(
                        generation_ref.as_resource().key(),
                        Some(io),
                    )?),
                };
                let file = pin(file, stock_string_copy(version, Some(io))?, io)?;
                self.seal_publication(file, Some(io))
            })?);
        }
        Ok(ContentChanges {
            operation,
            generation: generation_ref,
            members,
        })
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
    F,
    P: AsRef<str>,
    G: std::borrow::Borrow<crate::conversation::PathSelection>,
>(
    record: ChangeRecord<S, F, P, G>,
    io: &PreparationIo,
    pin: impl FnOnce(F) -> Result<FileRef>,
) -> Result<ContentChangeResult> {
    Ok(match record {
        ChangeRecord::Create { file } => ContentChangeResult::Created(pin(file)?),
        ChangeRecord::Update { file, .. } => ContentChangeResult::Updated(pin(file)?),
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

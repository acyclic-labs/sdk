//! Conditional file changes share the existing authored transaction, successful
//! operation index, protected publication receipt and commit authorization.
use super::*;
use crate::conversation::{ContentAccess, ContentChange};
use acyclic_fs::access::Action;

#[derive(Serialize)]
struct ChangeRecord<'a> {
    source: Option<&'a FileRef>,
    file: Option<FileRef>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId,
        conversation::VolumeOwner,
        core::{AggregateKind, Authority, AuthorityRoot},
    };

    #[tokio::test]
    async fn conditional_batch_is_atomic_and_recovers_its_original_cut() -> Result<()> {
        let provider =
            ProviderRef::new("changes".into(), "filesystem".into(), "1".into(), &mut None)?;
        let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([37; 16]);
        let volume = VolumeRef::new(
            provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let root = AuthorityRoot::new(
            "changes",
            [41; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "changes".into(),
            },
        );
        let scope = root.bootstrap_scope(
            "changes-owner",
            &crate::Capabilities::new([
                volume.capability(VolumeOperation::Write, &mut None)?,
                volume.capability(VolumeOperation::Read, &mut None)?,
            ]),
            Some(agent),
            &mut None,
        )?;
        let verifier = root.verifier(&mut None)?;
        let write = ContentGrant::verify(
            &verifier,
            &scope,
            &volume,
            VolumeOperation::Write,
            4096,
            &mut None,
        )?;
        let read = ContentGrant::verify(
            &verifier,
            &scope,
            &volume,
            VolumeOperation::Read,
            4096,
            &mut None,
        )?;
        let access = ContentAccess::default();
        let initial = [
            ContentChange::CreateAbsent {
                path: "a",
                bytes: b"old",
                media_type: "text/plain",
                display_name: "a",
            },
            ContentChange::CreateAbsent {
                path: "b",
                bytes: b"remove",
                media_type: "text/plain",
                display_name: "b",
            },
        ];
        let original = host
            .put_changes(
                &volume,
                &write,
                &initial,
                &IdempotencyKey::new("initial-changes")?,
                4096,
                access,
            )?
            .await?;
        let changes = [
            ContentChange::UpdateExpected {
                source: &original[0],
                bytes: b"new",
            },
            ContentChange::DeleteExpected {
                source: &original[1],
            },
            ContentChange::CreateAbsent {
                path: "c",
                bytes: b"added",
                media_type: "text/plain",
                display_name: "c",
            },
        ];
        let key = IdempotencyKey::new("complete-changes")?;
        let published = host
            .put_changes(&volume, &write, &changes, &key, 4096, access)?
            .await?;
        assert_eq!(published.len(), 2);
        assert_eq!(published[0].version(), published[1].version());
        assert_eq!(
            host.read_private_path(&volume, &read, "a", None, 4096, access)
                .await?
                .1
                .as_ref(),
            b"new"
        );
        assert_eq!(
            host.read_private_path(&volume, &read, "c", None, 4096, access)
                .await?
                .1
                .as_ref(),
            b"added"
        );
        assert!(
            host.read_private_path(&volume, &read, "b", None, 4096, access)
                .await
                .is_err()
        );
        let stale = [
            ContentChange::CreateAbsent {
                path: "must-stay-absent",
                bytes: b"partial",
                media_type: "text/plain",
                display_name: "partial",
            },
            ContentChange::UpdateExpected {
                source: &original[0],
                bytes: b"stale",
            },
        ];
        assert!(
            host.put_changes(
                &volume,
                &write,
                &stale,
                &IdempotencyKey::new("stale-changes")?,
                4096,
                access
            )?
            .await
            .is_err()
        );
        assert!(
            host.read_private_path(&volume, &read, "must-stay-absent", None, 4096, access)
                .await
                .is_err()
        );
        host.put_content(
            &volume,
            &write,
            "c",
            b"later",
            "text/plain",
            "c",
            4096,
            &IdempotencyKey::new("later-content")?,
            None,
            access,
        )
        .await?;
        let replayed = host
            .put_changes(&volume, &write, &changes, &key, 4096, access)?
            .await?;
        assert_eq!(replayed, published);
        assert_eq!(
            host.observe_changes(&volume, &write, &key, 4096, access)?
                .await?,
            Some(published)
        );
        let changed = [ContentChange::CreateAbsent {
            path: "different",
            bytes: b"changed",
            media_type: "text/plain",
            display_name: "different",
        }];
        assert!(
            host.put_changes(&volume, &write, &changed, &key, 4096, access)?
                .await
                .is_err()
        );
        assert!(
            host.read_private_path(&volume, &read, "different", None, 4096, access)
                .await
                .is_err()
        );
        Ok(())
    }
}

#[derive(Deserialize, crate::HeapLayout)]
struct StoredChange {
    source: Option<FileRef>,
    file: Option<FileRef>,
}

#[derive(Deserialize, crate::HeapLayout)]
struct ChangeReceipt(u8, Vec<StoredChange>);

impl UploadRecord for ChangeReceipt {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.1.iter().try_fold(
            self.1.capacity() as u64 * std::mem::size_of::<StoredChange>() as u64,
            |bytes, record| {
                crate::core::owned_sum([
                    bytes,
                    record
                        .source
                        .as_ref()
                        .map_or(Ok(0), FileRef::logical_owned_bytes)?,
                    record
                        .file
                        .as_ref()
                        .map_or(Ok(0), FileRef::logical_owned_bytes)?,
                ])
            },
        )
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) fn observe_changes<'a>(
        &'a self,
        volume: &'a VolumeRef,
        grant: &'a ContentGrant,
        key: &'a IdempotencyKey,
        maximum_receipt_bytes: u64,
        access: ContentAccess<'a, 'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<Vec<FileRef>>>> {
        grant.require(volume, VolumeOperation::Write, access.io)?;
        crate::conversation::BoundedContentFuture::new(access.io, || async move {
            let ordinary;
            let io = if let Some(io) = access.io {
                io
            } else {
                ordinary = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &ordinary
            };
            if let Some(ceiling) = access.ceiling
                && !crate::core::volume_write_granted_with_work(
                    ceiling,
                    volume,
                    &mut Some(&mut io.history_io()),
                )?
            {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "content caller lacks a filesystem write grant",
                ));
            }
            let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
            let opened = self.open(&reference, Some(io)).await?;
            let operation = bounded_io::operation_key(key, Some(io))?;
            let Some(generation) = io.sdk(
                opened
                    .operation_generation(operation, io, io.cancellation())
                    .await,
            )?
            else {
                return Ok(None);
            };
            let path = content_receipt_path_io(key.as_str(), false, true, Some(io))?;
            let bytes =
                read_generation(&generation, &path, maximum_receipt_bytes, Some(io)).await?;
            let receipt: ChangeReceipt = io.decode_json(&bytes)?;
            if receipt.0 != 1 {
                return Err(io.invalid("unsupported conditional publication receipt"));
            }
            let label = crate::core::filesystem_scope_label(
                grant.policy_scope().capabilities(),
                &mut Some(&mut io.history_io()),
            )?;
            let view = access::access_view(
                &opened,
                grant.policy_scope(),
                None,
                access::capability_payload(label, VolumeOperation::Write, io)?,
                access.ceiling,
                io,
            )
            .await?;
            for record in &receipt.1 {
                let (file, action) = match (&record.source, &record.file) {
                    (None, Some(file)) => (file, Action::Create),
                    (Some(_), Some(file)) => (file, Action::Write),
                    (Some(file), None) => (file, Action::Delete),
                    (None, None) => return Err(io.invalid("empty conditional publication member")),
                };
                file.validate(&mut Some(&mut io.history_io()))?;
                if file.volume() != volume {
                    return Err(io.invalid("conditional receipt volume differs"));
                }
                access::require(&view.0, action, file.path(), io)?;
            }
            access::require_current(&opened, &view.0, io).await?;
            grant.recheck(Some(io))?;
            bounded_io::retain(&generation, Some(io)).await?;
            let count = receipt.1.len();
            self.finish_change_files(
                &generation,
                receipt.1.into_iter().filter_map(|record| record.file),
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
        maximum_bytes: u64,
        access: ContentAccess<'a, 'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<FileRef>>> {
        let ContentAccess { ceiling, io } = access;
        grant.require(volume, VolumeOperation::Write, io)?;
        if volume.provider() != &self.provider || changes.is_empty() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "conditional publication needs a nonempty bound-volume request",
            ));
        }
        if let Some(ceiling) = ceiling
            && !crate::executor::with_history_io(io, |work| {
                crate::core::volume_write_granted_with_work(ceiling, volume, work)
            })?
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content caller lacks a filesystem write grant",
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
            for change in changes {
                crate::model::charge_items(Some(io), 1)?;
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
                    ContentChange::DeleteExpected { .. } => None,
                };
                let file = if let Some((path, bytes, media, display)) = output {
                    if bytes.len() as u64 > maximum_bytes {
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
                records.push(ChangeRecord {
                    source: change.source(),
                    file,
                });
            }
            // Versioned as a fresh public contract; the key uses the original
            // workspace operation index and the receipt commits with all files.
            let receipt = io.encode(&(1_u8, &records), io.remaining()?.bytes_encoded, true)?;
            let receipt_path = content_receipt_path_io(key.as_str(), false, true, Some(io))?;
            let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
            let opened = self.open(&reference, Some(io)).await?;
            let prior = io.sdk(
                opened
                    .operation_generation(operation, io, io.cancellation())
                    .await,
            )?;
            let label = crate::core::filesystem_scope_label(
                grant.policy_scope().capabilities(),
                &mut Some(&mut io.history_io()),
            )?;
            let view = access::access_view(
                &opened,
                grant.policy_scope(),
                None,
                access::capability_payload(label, VolumeOperation::Write, io)?,
                ceiling,
                io,
            )
            .await?;
            for change in changes {
                let action = match change {
                    ContentChange::CreateAbsent { .. } => Action::Create,
                    ContentChange::UpdateExpected { .. } => Action::Write,
                    ContentChange::DeleteExpected { .. } => Action::Delete,
                };
                access::require(&view.0, action, change.path(), io)?;
            }
            let generation = if let Some(prior) = prior {
                let recorded =
                    read_generation(&prior, &receipt_path, receipt.len() as u64, Some(io)).await?;
                if recorded.as_ref() != receipt.as_ref() {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "conditional publication identity belongs to a different request",
                    ));
                }
                access::require_current(&opened, &view.0, io).await?;
                prior
            } else {
                let selected = io.sdk(opened.generation(view.2, io, io.cancellation()).await)?;
                let selected_ref = self.generation_ref_id(view.2, Some(io))?;
                // All conditions are resolved at this one cut. The original
                // transaction observes these bindings and publishes once.
                for change in changes {
                    if let Some(source) = change.source() {
                        let source_generation = self.file_generation(source, Some(io))?;
                        if source_generation != selected_ref {
                            let absolute = crate::executor::preparation_format(
                                format_args!("/{}", source.path()),
                                &mut Some(&mut io.history_io()),
                            )?;
                            let metadata = content_metadata_path_io(source.path(), true, Some(io))?;
                            self.require_unchanged_content(
                                &opened,
                                &source_generation,
                                &selected_ref,
                                [&metadata, &absolute],
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
                            if let Some((parent, _)) = change.path().rsplit_once('/') {
                                let mut candidate = parent;
                                loop {
                                    let prefix = &absolute[..candidate.len() + 1];
                                    match io.sdk(selected.stat(prefix, io, io.cancellation()).await)
                                    {
                                        Ok(_) => break,
                                        Err(error)
                                            if error.code() == crate::wire::ErrorCode::NotFound =>
                                        {
                                            access::require(&view.0, Action::Create, candidate, io)?
                                        }
                                        Err(error) => return Err(error),
                                    }
                                    if let Some((parent, _)) = candidate.rsplit_once('/') {
                                        candidate = parent;
                                    } else {
                                        break;
                                    }
                                }
                                let (parent, _) = absolute
                                    .rsplit_once('/')
                                    .ok_or_else(|| io.invalid("missing parent"))?;
                                io.sdk(
                                    transaction
                                        .create_dir_all(parent, io, io.cancellation())
                                        .await,
                                )?;
                            }
                            io.sdk(
                                transaction
                                    .write_if(
                                        &absolute,
                                        bounded_io::clone_bytes(bytes, Some(io))?,
                                        acyclic_fs::FileCondition::Absent,
                                        io,
                                        io.cancellation(),
                                    )
                                    .await,
                            )?;
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
                        ContentChange::DeleteExpected { .. } => {
                            io.sdk(transaction.remove(&absolute, io, io.cancellation()).await)?;
                        }
                    }
                    let metadata_path = content_metadata_path_io(change.path(), true, Some(io))?;
                    if let Some(file) = &record.file {
                        let bytes = io.encode(
                            &(file, Option::<&GenerationRef>::None),
                            io.remaining()?.bytes_encoded,
                            true,
                        )?;
                        io.sdk(
                            transaction
                                .create_dir_all(
                                    "/.system/harness-file-metadata",
                                    io,
                                    io.cancellation(),
                                )
                                .await,
                        )?;
                        io.sdk(
                            transaction
                                .write(&metadata_path, bytes, io, io.cancellation())
                                .await,
                        )?;
                    } else {
                        match io.sdk(
                            transaction
                                .remove(&metadata_path, io, io.cancellation())
                                .await,
                        ) {
                            Ok(_) => {}
                            Err(error) if error.code() == crate::wire::ErrorCode::NotFound => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
                io.sdk(
                    transaction
                        .create_dir_all("/.system/harness-uploads", io, io.cancellation())
                        .await,
                )?;
                io.sdk(
                    transaction
                        .write(&receipt_path, receipt, io, io.cancellation())
                        .await,
                )?;
                Self::commit_publication(
                    transaction,
                    io,
                    workspaces::FilePublication::Authorized(fence, grant),
                )
                .await?
            };
            bounded_io::retain(&generation, Some(io)).await?;
            grant.recheck(Some(io))?;
            let count = records.len();
            self.finish_change_files(
                &generation,
                records.into_iter().filter_map(|record| record.file),
                count,
                io,
            )
        })
    }

    fn finish_change_files(
        &self,
        generation: &Generation<A, O>,
        originals: impl Iterator<Item = FileRef>,
        capacity: usize,
        io: &PreparationIo,
    ) -> Result<Vec<FileRef>> {
        let generation_ref = self.generation_ref_id(generation.id(), Some(io))?;
        let version = bounded_io::hex_key(generation_ref.as_resource().key(), Some(io))?;
        let mut files = crate::model::vec_with_capacity(capacity, Some(io))?;
        for mut file in originals {
            file.pin_version(
                stock_string_copy(&version, Some(io))?,
                &mut Some(&mut io.history_io()),
            )?;
            files.push(self.seal_publication(file, Some(io))?);
        }
        Ok(files)
    }
}

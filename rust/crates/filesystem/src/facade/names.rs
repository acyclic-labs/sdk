use super::*;

/// Page size for case-folded sibling scans.
pub(super) const CASE_FOLD_COLLISION_SCAN_PAGE: u32 = 256;

pub(super) fn name_path_error(error: crate::kernel::NamespacePathError) -> FsError {
    match error {
        crate::kernel::NamespacePathError::Work(error) => FsError::Work(error),
        crate::kernel::NamespacePathError::Cancelled => FsError::Cancelled(CancellationError),
        error => FsError::Path(PathLookupError::NamespacePath(error)),
    }
}

pub(super) fn name_cancellation<'a>(accounting: &KernelAccounting<'a>) -> &'a CancellationToken {
    match accounting {
        KernelAccounting::Standalone { cancellation, .. }
        | KernelAccounting::BorrowedStandalone { cancellation, .. } => cancellation,
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { cancellation, .. } => cancellation,
    }
}

pub(super) fn name_fold_key(
    name: &LogicalName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), FsError> {
    let cancellation = name_cancellation(accounting);
    if name.encoding() == NameEncoding::Utf8 {
        let text = std::str::from_utf8(name.as_bytes()).map_err(|_| FsError::NonNormalizedName)?;
        return types::case_fold_unicode(text, accounting, cancellation);
    }
    let delta = WorkCounters {
        bytes_copied: name.as_bytes().len() as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(delta)?;
    let mut bytes = Owned::<Vec<u8>>::allocate(name.as_bytes().len(), accounting)?;
    bytes.value.extend_from_slice(name.as_bytes());
    accounting.record_completed(delta)?;
    Ok((bytes.value, bytes.allocation))
}

pub(super) fn name_fold_equal(
    left: &LogicalName,
    right: &LogicalName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, FsError> {
    let left = name_fold_key(left, accounting)?;
    let right = name_fold_key(right, accounting)?;
    let equal = left.0 == right.0;
    let left_bytes = left.0.capacity() as u64;
    retire_name_owned(left, left_bytes, accounting)?;
    let right_bytes = right.0.capacity() as u64;
    retire_name_owned(right, right_bytes, accounting)?;
    Ok(equal)
}

pub(super) fn folded_binding_match<'a>(
    target: &LogicalName,
    folded_target: &[u8],
    include_exact: bool,
    allowed: &[FileId],
    entries: impl Iterator<Item = &'a crate::kernel::TreeEntry>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<&'a LogicalName>, FsError> {
    let mut found = None;
    for entry in entries {
        let folded = name_fold_key(&entry.name, accounting)?;
        let collision = (include_exact || entry.name != *target)
            && !allowed.contains(&entry.file_id)
            && folded.0 == folded_target;
        let bytes = folded.0.capacity() as u64;
        retire_name_owned(folded, bytes, accounting)?;
        if collision {
            if found.is_some() {
                return Err(FsError::NameCollision);
            }
            found = Some(&entry.name);
        }
    }
    Ok(found)
}

pub(super) fn retire_name_owned<T>(
    owned: (T, KernelOwnedAllocation),
    bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let (data, mut guard) = owned;
    drop(data);
    accounting.release_owned(&mut guard, bytes)
}

pub(super) fn pending_binding_path<'a>(
    components: &[LogicalName],
    pending: &[(&'a NamespacePath, FileId)],
    case_sensitivity: CaseSensitivity,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<(&'a NamespacePath, FileId)>, FsError> {
    let Some((name, parent)) = components.split_last() else {
        return Ok(None);
    };
    for (path, id) in pending.iter().rev() {
        let Some((other_parent, other_name)) = path.split_last() else {
            continue;
        };
        if parent != other_parent {
            continue;
        }
        let same = match case_sensitivity {
            CaseSensitivity::Sensitive => name == other_name,
            CaseSensitivity::ProfileFolded => name_fold_equal(name, other_name, accounting)?,
        };
        if same {
            return Ok(Some((*path, *id)));
        }
    }
    Ok(None)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(super) async fn admit_mutation_names_with_accounting(
        &mut self,
        operations: &mut [Mutation],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded
            && self.volume.config.unicode != UnicodePolicy::RequireNfc
        {
            return Ok(());
        }
        let mut pending =
            Owned::<Vec<(&NamespacePath, FileId)>>::allocate(operations.len(), accounting)?;
        let mut removed =
            Owned::<Vec<(&NamespacePath, FileId)>>::allocate(operations.len(), accounting)?;
        for operation in operations {
            accounting.check()?;
            self.canonicalize_mutation(
                operation,
                &pending.value,
                operations_allocation,
                accounting,
                cancellation,
            )
            .await?;
            let binding = match operation {
                Mutation::Create { path, record } | Mutation::Restore { path, record } => {
                    Some((path, record.file_id))
                }
                Mutation::Link {
                    source,
                    destination,
                } => {
                    let id = self
                        .pending_or_existing_file_id(
                            source,
                            &pending.value,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    Some((destination, id))
                }
                Mutation::Rename {
                    source,
                    destination,
                    replace,
                } => {
                    let id = self
                        .pending_or_existing_file_id(
                            source,
                            &pending.value,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    let removed_id = pending_binding_path(
                        destination.components(),
                        &removed.value,
                        CaseSensitivity::ProfileFolded,
                        accounting,
                    )?
                    .map(|(_, id)| id)
                    .filter(|other| *other != id);
                    let ids = [id, removed_id.unwrap_or(id)];
                    let allowed = if removed_id.is_some() {
                        ids.as_slice()
                    } else {
                        std::slice::from_ref(&id)
                    };
                    self.admit_binding_name(
                        destination,
                        &pending.value,
                        allowed,
                        accounting,
                        cancellation,
                    )
                    .await?;
                    pending.value.retain(|(path, _)| {
                        *path != &*source && (!*replace || *path != &*destination)
                    });
                    removed.value.push((source, id));
                    pending.value.push((destination, id));
                    None
                }
                Mutation::Remove { path, .. } => {
                    if pending_binding_path(
                        path.components(),
                        &pending.value,
                        self.volume.config.case_sensitivity,
                        accounting,
                    )?
                    .is_none()
                    {
                        let id = self
                            .pending_or_existing_file_id(
                                path,
                                &pending.value,
                                accounting,
                                cancellation,
                            )
                            .await?;
                        removed.value.push((path, id));
                    }
                    pending.value.retain(|(other, _)| *other != &*path);
                    None
                }
                Mutation::SetMetadata { .. }
                | Mutation::Write { .. }
                | Mutation::ValidateRegular { .. }
                | Mutation::Resize { .. }
                | Mutation::ZeroRange { .. }
                | Mutation::Preallocate { .. }
                | Mutation::CloneRange { .. }
                | Mutation::File { .. }
                | Mutation::CloneFileRange { .. } => None,
            };
            if let Some((path, id)) = binding {
                let allowed = pending_binding_path(
                    path.components(),
                    &removed.value,
                    CaseSensitivity::ProfileFolded,
                    accounting,
                )?
                .map(|(_, id)| id);
                self.admit_binding_name(
                    path,
                    &pending.value,
                    allowed.as_slice(),
                    accounting,
                    cancellation,
                )
                .await?;
                pending.value.push((path, id));
            }
        }
        pending.retire(accounting)?;
        removed.retire(accounting)
    }

    async fn admit_binding_name(
        &mut self,
        path: &NamespacePath,
        pending: &[(&NamespacePath, FileId)],
        allowed: &[FileId],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        let Some((parent, name)) = path.split_last() else {
            return Ok(());
        };
        if self.volume.config.unicode == UnicodePolicy::RequireNfc
            && name.encoding() == NameEncoding::Utf8
            && !std::str::from_utf8(name.as_bytes()).is_ok_and(unicode_normalization::is_nfc)
        {
            return Err(FsError::NonNormalizedName);
        }
        if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded {
            if pending_binding_path(
                parent,
                pending,
                self.volume.config.case_sensitivity,
                accounting,
            )?
            .is_none()
                && self
                    .find_case_folded_sibling_with_accounting(
                        parent,
                        name,
                        accounting,
                        cancellation,
                        allowed,
                        false,
                    )
                    .await?
                    .is_some()
            {
                return Err(FsError::NameCollision);
            }
            for (other, id) in pending {
                if let Some((other_parent, other_name)) = other.split_last()
                    && parent == other_parent
                    && name != other_name
                    && !allowed.contains(id)
                    && name_fold_equal(name, other_name, accounting)?
                {
                    return Err(FsError::NameCollision);
                }
            }
        }
        Ok(())
    }

    async fn canonicalize_mutation(
        &mut self,
        operation: &mut Mutation,
        pending: &[(&NamespacePath, FileId)],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Ok(());
        }
        let (paths, destination) = match operation {
            Mutation::Remove { path, .. }
            | Mutation::SetMetadata { path, .. }
            | Mutation::Write { path, .. }
            | Mutation::ValidateRegular { path }
            | Mutation::Resize { path, .. }
            | Mutation::ZeroRange { path, .. }
            | Mutation::Preallocate { path, .. } => ([Some(path), None], None),
            Mutation::Rename {
                source,
                destination,
                ..
            }
            | Mutation::Link {
                source,
                destination,
            } => ([Some(source), None], Some(destination)),
            Mutation::CloneRange {
                source,
                destination,
                ..
            } => ([Some(source), Some(destination)], None),
            Mutation::Create { path, .. } => ([None, None], Some(path)),
            Mutation::Restore { .. } | Mutation::File { .. } | Mutation::CloneFileRange { .. } => {
                return Ok(());
            }
        };
        for path in paths.into_iter().flatten() {
            let replacement = self
                .canonical_existing_path(path, pending, accounting, cancellation)
                .await?;
            Self::replace_name_path(path, replacement, operations_allocation, accounting)?;
        }
        if let Some(path) = destination
            && let Some((parent, name)) = path.split_last()
            && !parent.is_empty()
            && let Some(resolved) = self
                .canonicalize_path_components(parent, pending, accounting, cancellation)
                .await?
        {
            let replacement = resolved.0.child_with_accounting(
                name,
                self.volume.config.limits,
                accounting,
                cancellation,
            )?;
            let bytes = resolved.0.retained_bytes();
            retire_name_owned(resolved, bytes, accounting)?;
            Self::replace_name_path(path, replacement, operations_allocation, accounting)?;
        }
        Ok(())
    }

    pub(super) fn replace_name_path(
        path: &mut NamespacePath,
        mut replacement: (NamespacePath, KernelOwnedAllocation),
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let old_bytes = path.retained_bytes();
        operations_allocation.absorb(&mut replacement.1)?;
        drop(std::mem::replace(path, replacement.0));
        accounting.release_owned(operations_allocation, old_bytes)?;
        accounting.check()
    }

    pub(super) async fn canonical_existing_path(
        &mut self,
        path: &NamespacePath,
        pending: &[(&NamespacePath, FileId)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespacePath, KernelOwnedAllocation), FsError> {
        if let Some((exact, _)) = pending_binding_path(
            path.components(),
            pending,
            self.volume.config.case_sensitivity,
            accounting,
        )? {
            let token = name_cancellation(accounting);
            return exact.clone_with_accounting(accounting, token);
        }
        let exact = self
            .lookup_no_follow_at_current_with_accounting(path, accounting, cancellation, false)
            .await?;
        if exact.0.record.is_some() {
            let token = name_cancellation(accounting);
            return path.clone_with_accounting(accounting, token);
        }
        if path.is_root() || self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Err(FsError::NotFound);
        }
        self.canonicalize_path_components(path.components(), pending, accounting, cancellation)
            .await?
            .ok_or(FsError::NotFound)
    }

    pub(super) async fn canonicalize_path_components(
        &mut self,
        requested: &[LogicalName],
        pending: &[(&NamespacePath, FileId)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(NamespacePath, KernelOwnedAllocation)>, FsError> {
        let allocation = accounting.claim_owned(0, 0)?;
        let mut canonical = (
            NamespacePath::new(Vec::new(), self.volume.config.limits).map_err(name_path_error)?,
            allocation,
        );
        for name in requested {
            accounting.check()?;
            let candidate = canonical.0.child_with_accounting(
                name,
                self.volume.config.limits,
                accounting,
                cancellation,
            )?;
            if let Some((exact, _)) = pending_binding_path(
                candidate.0.components(),
                pending,
                self.volume.config.case_sensitivity,
                accounting,
            )? {
                let token = name_cancellation(accounting);
                let next = exact.clone_with_accounting(accounting, token)?;
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(std::mem::replace(&mut canonical, next), bytes, accounting)?;
                let bytes = candidate.0.retained_bytes();
                retire_name_owned(candidate, bytes, accounting)?;
                continue;
            }
            let probe = self
                .lookup_no_follow_at_current_with_accounting(
                    &candidate.0,
                    accounting,
                    cancellation,
                    false,
                )
                .await?;
            if probe.0.record.is_some() {
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(
                    std::mem::replace(&mut canonical, candidate),
                    bytes,
                    accounting,
                )?;
                continue;
            }
            let Some(exact_name) = self
                .find_case_folded_sibling_with_accounting(
                    canonical.0.components(),
                    name,
                    accounting,
                    cancellation,
                    &[],
                    true,
                )
                .await?
            else {
                let bytes = candidate.0.retained_bytes();
                retire_name_owned(candidate, bytes, accounting)?;
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(canonical, bytes, accounting)?;
                return Ok(None);
            };
            let next = canonical.0.child_with_accounting(
                &exact_name.0,
                self.volume.config.limits,
                accounting,
                cancellation,
            )?;
            let bytes = canonical.0.retained_bytes();
            retire_name_owned(std::mem::replace(&mut canonical, next), bytes, accounting)?;
            let bytes = candidate.0.retained_bytes();
            retire_name_owned(candidate, bytes, accounting)?;
            let bytes = exact_name.0.retained_bytes() as u64;
            retire_name_owned(exact_name, bytes, accounting)?;
        }
        Ok(Some(canonical))
    }

    pub(super) async fn pending_or_existing_file_id(
        &mut self,
        path: &NamespacePath,
        pending: &[(&NamespacePath, FileId)],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        if let Some((_, id)) = pending_binding_path(
            path.components(),
            pending,
            self.volume.config.case_sensitivity,
            accounting,
        )? {
            return Ok(id);
        }
        let lookup = self
            .lookup_no_follow_with_accounting(path, accounting, cancellation, true)
            .await?;
        Ok(lookup.0.record.ok_or(FsError::NotFound)?.file_id)
    }

    pub(super) fn find_case_folded_sibling_with_accounting<'a, 'accounting>(
        &'a mut self,
        parent_components: &'a [LogicalName],
        target: &'a LogicalName,
        accounting: &'a mut KernelAccounting<'accounting>,
        cancellation: &'a CancellationToken,
        allowed_file_ids: &'a [FileId],
        unique: bool,
    ) -> impl std::future::Future<
        Output = Result<Option<(LogicalName, KernelOwnedAllocation)>, FsError>,
    > + ProviderTask
    + 'a {
        async move {
            let mut frame_custody = None;
            let (future, frame_bytes) = crate::heap_future::in_heap_admitted(
                (
                    &mut *self,
                    parent_components,
                    target,
                    allowed_file_ids,
                    &mut *accounting,
                ),
                |(_, _, _, _, context), bytes| {
                    frame_custody = Some(context.claim_owned(bytes, u64::from(bytes != 0))?);
                    Ok::<(), FsError>(())
                },
                |(this, parent_components, target, allowed_file_ids, accounting)| async move {
                    let allocation = accounting.claim_owned(0, 0)?;
                    let mut parent = (
                        NamespacePath::new(Vec::new(), this.volume.config.limits)
                            .map_err(name_path_error)?,
                        allocation,
                    );
                    for name in parent_components {
                        let next = parent.0.child_with_accounting(
                            name,
                            this.volume.config.limits,
                            accounting,
                            cancellation,
                        )?;
                        let bytes = parent.0.retained_bytes();
                        retire_name_owned(std::mem::replace(&mut parent, next), bytes, accounting)?;
                    }
                    let folded_target = name_fold_key(target, accounting)?;
                    let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
                    let mut found = None;
                    loop {
                        accounting.check()?;
                        this.synchronize_live_with_accounting(accounting, cancellation)
                            .await?;
                        let (lookup, _allocation) = this
                            .lookup_no_follow_at_current_with_accounting(
                                &parent.0,
                                accounting,
                                cancellation,
                                false,
                            )
                            .await?;
                        let page = this
                            .list_directory_record_with_accounting(
                                lookup.record.ok_or(FsError::NotFound)?,
                                after
                                    .as_ref()
                                    .map(|owned| &owned.0)
                                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                                CASE_FOLD_COLLISION_SCAN_PAGE,
                                accounting,
                                cancellation,
                            )
                            .await?;
                        if let Some(name) = folded_binding_match(
                            target,
                            &folded_target.0,
                            unique,
                            allowed_file_ids,
                            page.0.entries.iter(),
                            accounting,
                        )? {
                            if found.is_some() {
                                return Err(FsError::NameCollision);
                            }
                            found = Some(name.clone_with_accounting(accounting, cancellation)?);
                        }
                        let has_more = page.0.has_more;
                        if let Some(old) = after.take() {
                            let bytes = old.0.retained_bytes() as u64;
                            retire_name_owned(old, bytes, accounting)?;
                        }
                        after = match page
                            .0
                            .entries
                            .last()
                            .filter(|_| has_more && (unique || found.is_none()))
                        {
                            Some(entry) => {
                                Some(entry.name.clone_with_accounting(accounting, cancellation)?)
                            }
                            None => None,
                        };
                        let bytes = page.0.entries.iter().try_fold(
                            (page.0.entries.capacity() * size_of::<crate::kernel::TreeEntry>())
                                as u64,
                            |bytes, entry| {
                                bytes
                                    .checked_add(entry.name.retained_bytes() as u64)
                                    .ok_or(WorkError::Overflow)
                            },
                        )?;
                        retire_name_owned(page, bytes, accounting)?;
                        if (!unique && found.is_some()) || !has_more {
                            break;
                        }
                    }
                    let bytes = parent.0.retained_bytes();
                    retire_name_owned(parent, bytes, accounting)?;
                    let bytes = folded_target.0.capacity() as u64;
                    retire_name_owned(folded_target, bytes, accounting)?;
                    Ok(found)
                },
            )?;
            let mut future = future;
            let result = future.as_mut().await;
            drop(future);
            let mut frame_custody =
                frame_custody.ok_or(FsError::PendingMutationAllocationFailed)?;
            accounting.release_owned(&mut frame_custody, frame_bytes)?;
            result
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(crate) async fn directory_binding_with_accounting(
        &self,
        id: FileId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<String>, FsError> {
        let before = readonly_standalone_live(accounting);
        let result = async {
            let (search, allocation) =
                crate::workspace::namespace_records_to_file_root_with_accounting(
                    &self.volume,
                    &self.root,
                    self.generation_root,
                    &[id],
                    // Bindings can outnumber file records through hard links.
                    // The original work budget bounds this cold traversal.
                    u32::MAX,
                    accounting,
                    cancellation,
                )
                .await?;
            let bytes = crate::workspace::namespace_records_bytes(&search.records);
            let search = Owned::new(search, allocation, bytes);
            let result = (|| {
                let [(found, records)] = search.value.records.as_slice() else {
                    return Err(FsError::FileIdentityCollision);
                };
                let [(path, record)] = records.as_slice() else {
                    return Err(FsError::FileIdentityCollision);
                };
                if !search.value.complete || *found != id || record.kind != FileKind::Directory {
                    return Err(FsError::FileIdentityCollision);
                }
                let (path, allocation) =
                    path.unicode_text_with_accounting(accounting, cancellation)?;
                let bytes = path.capacity() as u64;
                Ok(Owned::new(path, allocation, bytes))
            })();
            search.retire(accounting)?;
            result
        }
        .await;
        let retained = result.as_ref().map_or(0, |path| path.bytes);
        readonly_cleanup_standalone(accounting, before, retained)?;
        result
    }
}

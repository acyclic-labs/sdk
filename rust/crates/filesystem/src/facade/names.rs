use super::*;

/// Directory-listing page size used to scan siblings for a case-folded
pub(super) const CASE_FOLD_COLLISION_SCAN_PAGE: u32 = 256;

#[cfg(not(target_arch = "wasm32"))]
pub(super) type NameOperationsAllocation = NativeAllocation;

#[cfg(target_arch = "wasm32")]
pub(super) type NameOperationsAllocation = ();

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
    name: &LogicalName, accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), FsError> {
    let cancellation = name_cancellation(accounting);
    if name.encoding() == NameEncoding::Utf8 {
        let text = std::str::from_utf8(name.as_bytes()).map_err(|_| FsError::NonNormalizedName)?;
        return types::lowercase_unicode_with_accounting(text, accounting, cancellation);
    }
    let delta = WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(delta)?;
    let requested = name.as_bytes().len() as u64;
    let mut allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(name.as_bytes().len()).map_err(|_| FsError::PendingMutationAllocationFailed)?;
    accounting.reconcile_owned(&mut allocation, requested, bytes.capacity() as u64)?;
    bytes.extend_from_slice(name.as_bytes());
    accounting.record_completed(delta)?;
    Ok((bytes, allocation))
}

pub(super) fn name_fold_equal(
    left: &LogicalName, right: &LogicalName, accounting: &mut KernelAccounting<'_>,
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

pub(super) fn retire_name_owned<T>(
    owned: (T, KernelOwnedAllocation), bytes: u64, accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> { let (data, mut guard) = owned; drop(data); accounting.release_owned(&mut guard, bytes) }

pub(super) fn pending_binding_path<'a>(
    components: &[LogicalName], pending: &'a [(NamespacePath, FileId)],
    case_sensitivity: CaseSensitivity,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<(&'a NamespacePath, FileId)>, FsError> {
    let Some((name, parent)) = components.split_last() else { return Ok(None); };
    for (path, id) in pending.iter().rev() {
        let Some((other_parent, other_name)) = path.split_last() else { continue; };
        if parent != other_parent { continue; }
        let same = match case_sensitivity {
            CaseSensitivity::Sensitive => name == other_name,
            CaseSensitivity::ProfileFolded => name_fold_equal(name, other_name, accounting)?,
        };
        if same { return Ok(Some((path, *id))); }
    }
    Ok(None)
}

pub(super) fn removed_binding_id(
    path: &NamespacePath,
    removed: &[(NamespacePath, FileId)], accounting: &mut KernelAccounting<'_>,
) -> Result<Option<FileId>, FsError> {
    Ok(pending_binding_path(path.components(), removed, CaseSensitivity::ProfileFolded, accounting)? .map(|(_, id)| id))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {


    pub(super) async fn admit_mutation_names_with_accounting(
        &mut self,
        operations: &mut [Mutation],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        accounting.check()?;
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded
            && self.volume.config.unicode != UnicodePolicy::RequireNfc
        {
            return Ok(());
        }
        let mut pending = Self::name_binding_vector(operations.len(), accounting)?;
        let mut removed = Self::name_binding_vector(operations.len(), accounting)?;
        for operation in operations {
            accounting.check()?;
            self.canonicalize_existing_mutation(
                operation, &pending.0, operations_allocation, accounting, cancellation,
            ).await?;
            self.canonicalize_new_mutation_parents(
                operation, &pending.0, operations_allocation, accounting, cancellation,
            ).await?;
            match operation {
                Mutation::Create { path, record } | Mutation::Restore { path, record } => {
                    let allowed = removed_binding_id(path, &removed.0, accounting)?;
                    let allowed_ids = allowed.as_ref().map_or(&[][..], std::slice::from_ref);
                    self.admit_binding_name(path, &pending.0, allowed_ids, accounting, cancellation).await?;
                    Self::push_name_binding(&mut pending, path, record.file_id, accounting)?;
                }
                Mutation::Link { source, destination } => {
                    let id = self.pending_or_existing_file_id(source, &pending.0, accounting, cancellation).await?;
                    let allowed = removed_binding_id(destination, &removed.0, accounting)?;
                    let allowed_ids = allowed.as_ref().map_or(&[][..], std::slice::from_ref);
                    self.admit_binding_name(destination, &pending.0, allowed_ids, accounting, cancellation).await?;
                    Self::push_name_binding(&mut pending, destination, id, accounting)?;
                }
                Mutation::Rename { source, destination, replace } => {
                    let id = self.pending_or_existing_file_id(source, &pending.0, accounting, cancellation).await?;
                    let removed_id = removed_binding_id(destination, &removed.0, accounting)?.filter(|other| *other != id);
                    let ids = [id, removed_id.unwrap_or(id)];
                    let allowed_ids = if removed_id.is_some() { &ids[..] } else { &ids[..1] };
                    self.admit_binding_name(destination, &pending.0, allowed_ids, accounting, cancellation).await?;
                    Self::retain_name_bindings(&mut pending, source, accounting)?;
                    if *replace {
                        Self::retain_name_bindings(&mut pending, destination, accounting)?;
                    }
                    Self::push_name_binding(&mut removed, source, id, accounting)?;
                    Self::push_name_binding(&mut pending, destination, id, accounting)?;
                }
                Mutation::Remove { path, .. } => {
                    if pending_binding_path(path.components(), &pending.0, self.volume.config.case_sensitivity, accounting)?.is_none() {
                        let id = self.pending_or_existing_file_id(path, &pending.0, accounting, cancellation).await?;
                        Self::push_name_binding(&mut removed, path, id, accounting)?;
                    }
                    Self::retain_name_bindings(&mut pending, path, accounting)?;
                }
                Mutation::SetMetadata { .. } | Mutation::Write { .. }
                | Mutation::ValidateRegular { .. } | Mutation::Resize { .. }
                | Mutation::ZeroRange { .. } | Mutation::Preallocate { .. }
                | Mutation::CloneRange { .. } | Mutation::File { .. }
                | Mutation::CloneFileRange { .. } => {}
            }
        }
        Ok(())
    }


    pub(super) fn name_binding_vector(
        length: usize,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(Vec<(NamespacePath, FileId)>, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        let requested = length.checked_mul(size_of::<(NamespacePath, FileId)>())
            .ok_or(WorkError::Overflow)? as u64;
        let mut allocation = accounting.claim_owned(requested, u64::from(length != 0))?;
        let mut values = Vec::new();
        values.try_reserve_exact(length).map_err(|_| FsError::PendingMutationAllocationFailed)?;
        let actual = values.capacity().checked_mul(size_of::<(NamespacePath, FileId)>())
            .ok_or(WorkError::Overflow)? as u64;
        accounting.reconcile_owned(&mut allocation, requested, actual)?;
        Ok((values, allocation))
    }


    pub(super) fn push_name_binding(
        bindings: &mut (Vec<(NamespacePath, FileId)>, crate::kernel::allocation::KernelOwnedAllocation),
        path: &NamespacePath,
        id: FileId,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let cancellation = name_cancellation(accounting);
        let mut copied = path.clone_with_accounting(accounting, cancellation)?;
        bindings.1.absorb(&mut copied.1)?;
        bindings.0.push((copied.0, id));
        Ok(())
    }


    pub(super) fn retain_name_bindings(
        bindings: &mut (Vec<(NamespacePath, FileId)>, crate::kernel::allocation::KernelOwnedAllocation),
        removed: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let bytes = bindings.0.iter().filter(|(path, _)| path == removed)
            .try_fold(0_u64, |total, (path, _)| total.checked_add(path.retained_bytes()).ok_or(WorkError::Overflow))?;
        bindings.0.retain(|(path, _)| path != removed);
        accounting.release_owned(&mut bindings.1, bytes)
    }


    pub(super) fn replace_name_path(
        path: &mut NamespacePath,
        mut replacement: (NamespacePath, crate::kernel::allocation::KernelOwnedAllocation),
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let old_bytes = path.retained_bytes();
        operations_allocation.absorb(&mut replacement.1)?;
        drop(std::mem::replace(path, replacement.0));
        accounting.release_owned(operations_allocation, old_bytes)?;
        accounting.check()
    }


    pub(super) async fn admit_binding_name(
        &mut self, path: &NamespacePath, pending: &[(NamespacePath, FileId)],
        allowed: &[FileId], accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let parent_pending = match path.split_last() {
            Some((parent, _)) => pending_binding_path(parent, pending,
                self.volume.config.case_sensitivity, accounting)?.is_some(),
            None => false,
        };
        self.admit_new_name_with_accounting(path, accounting, cancellation, allowed, parent_pending).await?;
        if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
            && let Some((parent, name)) = path.split_last()
        {
            for (other, id) in pending {
                if let Some((other_parent, other_name)) = other.split_last()
                    && parent == other_parent && name != other_name && !allowed.contains(id)
                    && name_fold_equal(name, other_name, accounting)?
                {
                    return Err(FsError::NameCollision);
                }
            }
        }
        Ok(())
    }


    pub(super) async fn canonicalize_new_mutation_parents(
        &mut self,
        operation: &mut Mutation,
        pending: &[(NamespacePath, FileId)],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded { return Ok(()); }
        let (Mutation::Create { path, .. } | Mutation::Link { destination: path, .. }
            | Mutation::Rename { destination: path, .. }) = operation else { return Ok(()); };
        let Some((parent, name)) = path.split_last() else { return Ok(()); };
        if parent.is_empty() { return Ok(()); }
        let Some(resolved) = self.canonicalize_path_components(parent, pending, accounting, cancellation).await?
            else { return Ok(()); };
        let replacement = resolved.0.child_with_accounting(
            name, self.volume.config.limits, accounting, cancellation,
        )?;
        let bytes = resolved.0.retained_bytes();
        retire_name_owned(resolved, bytes, accounting)?;
        Self::replace_name_path(path, replacement, operations_allocation, accounting)
    }


    pub(super) async fn canonicalize_existing_mutation(
        &mut self,
        operation: &mut Mutation,
        pending: &[(NamespacePath, FileId)],
        operations_allocation: &mut KernelOwnedAllocation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded { return Ok(()); }
        match operation {
            Mutation::Remove { path, .. } | Mutation::SetMetadata { path, .. }
            | Mutation::Write { path, .. } | Mutation::ValidateRegular { path }
            | Mutation::Resize { path, .. } | Mutation::ZeroRange { path, .. }
            | Mutation::Preallocate { path, .. } => {
                let replacement = self.canonical_existing_path(path, pending, accounting, cancellation).await?;
                Self::replace_name_path(path, replacement, operations_allocation, accounting)?;
            }
            Mutation::Rename { source, .. } | Mutation::Link { source, .. } => {
                let replacement = self.canonical_existing_path(source, pending, accounting, cancellation).await?;
                Self::replace_name_path(source, replacement, operations_allocation, accounting)?;
            }
            Mutation::CloneRange { source, destination, .. } => {
                let replacement = self.canonical_existing_path(source, pending, accounting, cancellation).await?;
                Self::replace_name_path(source, replacement, operations_allocation, accounting)?;
                let replacement = self.canonical_existing_path(destination, pending, accounting, cancellation).await?;
                Self::replace_name_path(destination, replacement, operations_allocation, accounting)?;
            }
            Mutation::Create { .. } | Mutation::Restore { .. } | Mutation::File { .. }
            | Mutation::CloneFileRange { .. } => {}
        }
        Ok(())
    }


    pub(super) async fn canonical_existing_path(
        &mut self,
        path: &NamespacePath,
        pending: &[(NamespacePath, FileId)],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(NamespacePath, crate::kernel::allocation::KernelOwnedAllocation), FsError> {
        if let Some((exact, _)) = pending_binding_path(path.components(), pending,
            self.volume.config.case_sensitivity, accounting)?
        {
            let token = name_cancellation(accounting);
            return exact.clone_with_accounting(accounting, token);
        }
        let exact = self.lookup_no_follow_at_current_with_accounting(path, accounting, cancellation, false).await?;
        if exact.0.record.is_some() {
            let token = name_cancellation(accounting);
            return path.clone_with_accounting(accounting, token);
        }
        if path.is_root() || self.volume.config.case_sensitivity != CaseSensitivity::ProfileFolded {
            return Err(FsError::NotFound);
        }
        self.canonicalize_path_components(path.components(), pending, accounting, cancellation).await?
            .ok_or(FsError::NotFound)
    }


    pub(super) async fn canonicalize_path_components(
        &mut self,
        requested: &[LogicalName],
        pending: &[(NamespacePath, FileId)],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(NamespacePath, crate::kernel::allocation::KernelOwnedAllocation)>, FsError> {
        let allocation = accounting.claim_owned(0, 0)?;
        let mut canonical = (
            NamespacePath::new(Vec::new(), self.volume.config.limits).map_err(name_path_error)?,
            allocation,
        );
        for name in requested {
            accounting.check()?;
            let candidate = canonical.0.child_with_accounting(
                name, self.volume.config.limits, accounting, cancellation,
            )?;
            if let Some((exact, _)) = pending_binding_path(candidate.0.components(), pending,
                self.volume.config.case_sensitivity, accounting)?
            {
                let token = name_cancellation(accounting);
                let next = exact.clone_with_accounting(accounting, token)?;
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(std::mem::replace(&mut canonical, next), bytes, accounting)?;
                let bytes = candidate.0.retained_bytes();
                retire_name_owned(candidate, bytes, accounting)?;
                continue;
            }
            let probe = self.lookup_no_follow_at_current_with_accounting(
                &candidate.0, accounting, cancellation, false,
            ).await?;
            if probe.0.record.is_some() {
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(std::mem::replace(&mut canonical, candidate), bytes, accounting)?;
                continue;
            }
            let Some(exact_name) = self.find_case_folded_sibling_with_accounting(
                canonical.0.components(), name, accounting, cancellation, &[],
            ).await? else {
                let bytes = candidate.0.retained_bytes();
                retire_name_owned(candidate, bytes, accounting)?;
                let bytes = canonical.0.retained_bytes();
                retire_name_owned(canonical, bytes, accounting)?;
                return Ok(None);
            };
            let next = canonical.0.child_with_accounting(
                &exact_name.0, self.volume.config.limits, accounting, cancellation,
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
        pending: &[(NamespacePath, FileId)],
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileId, FsError> {
        if let Some((_, id)) = pending_binding_path(path.components(), pending,
            self.volume.config.case_sensitivity, accounting)? {
            return Ok(id);
        }
        let lookup = self.lookup_no_follow_with_accounting(path, accounting, cancellation, true).await?;
        Ok(lookup.0.record.ok_or(FsError::NotFound)?.file_id)
    }


    pub(super) async fn admit_new_name_with_accounting(
        &mut self,
        path: &NamespacePath,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
        allowed_file_ids: &[FileId],
        parent_pending: bool,
    ) -> Result<(), FsError> {
        accounting.check()?;
        let Some((parent, name)) = path.split_last() else { return Ok(()); };
        if self.volume.config.unicode == UnicodePolicy::RequireNfc
            && name.encoding() == NameEncoding::Utf8
            && !std::str::from_utf8(name.as_bytes()).is_ok_and(unicode_normalization::is_nfc)
        {
            return Err(FsError::NonNormalizedName);
        }
        if self.volume.config.case_sensitivity == CaseSensitivity::ProfileFolded
            && !parent_pending
            && self.find_case_folded_sibling_with_accounting(
                parent, name, accounting, cancellation, allowed_file_ids,
            ).await?.is_some()
        {
            return Err(FsError::NameCollision);
        }
        Ok(())
    }


    pub(super) fn find_case_folded_sibling_with_accounting<'a, 'accounting>(
        &'a mut self,
        parent_components: &'a [LogicalName],
        target: &'a LogicalName,
        accounting: &'a mut crate::kernel::allocation::KernelAccounting<'accounting>,
        cancellation: &'a CancellationToken,
        allowed_file_ids: &'a [FileId],
    ) -> impl std::future::Future<Output = Result<Option<(LogicalName, crate::kernel::allocation::KernelOwnedAllocation)>, FsError>> + ProviderTask + 'a {
        async move {
            let mut frame_custody = None;
            let (future, frame_bytes) = crate::heap_future::in_heap_admitted(
                &mut *accounting,
                |context, bytes| {
                    frame_custody = Some(context.claim_owned(bytes, u64::from(bytes != 0))?);
                    Ok::<(), FsError>(())
                },
                |accounting| async move {
                    let allocation = accounting.claim_owned(0, 0)?;
                    let mut parent = (
                        NamespacePath::new(Vec::new(), self.volume.config.limits).map_err(name_path_error)?,
                        allocation,
                    );
                    for name in parent_components {
                        let next = parent.0.child_with_accounting(
                            name, self.volume.config.limits, accounting, cancellation,
                        )?;
                        let bytes = parent.0.retained_bytes();
                        retire_name_owned(std::mem::replace(&mut parent, next), bytes, accounting)?;
                    }
                    let folded_target = name_fold_key(target, accounting)?;
                    let mut after: Option<(LogicalName, crate::kernel::allocation::KernelOwnedAllocation)> = None;
                    let found = loop {
                        accounting.check()?;
                        let page = self.list_directory_with_accounting(
                            &parent.0, after.as_ref().map(|owned| &owned.0).map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                            CASE_FOLD_COLLISION_SCAN_PAGE, accounting, cancellation,
                        ).await?;
                        let mut found = None;
                        for entry in &page.0.entries {
                            let folded_entry = name_fold_key(&entry.name, accounting)?;
                            let collision = entry.name != *target && !allowed_file_ids.contains(&entry.file_id)
                                && folded_entry.0 == folded_target.0;
                            let bytes = folded_entry.0.capacity() as u64;
                            retire_name_owned(folded_entry, bytes, accounting)?;
                            if collision
                            {
                                found = Some(entry.name.clone_with_accounting(accounting, cancellation)?);
                                break;
                            }
                        }
                        let has_more = page.0.has_more;
                        if let Some(old) = after.take() {
                            let bytes = old.0.retained_bytes() as u64;
                            retire_name_owned(old, bytes, accounting)?;
                        }
                        after = match page.0.entries.last().filter(|_| has_more && found.is_none()) {
                            Some(entry) => Some(entry.name.clone_with_accounting(accounting, cancellation)?),
                            None => None,
                        };
                        let bytes = page.0.entries.iter().try_fold(
                            (page.0.entries.capacity() * size_of::<crate::kernel::TreeEntry>()) as u64,
                            |bytes, entry| bytes.checked_add(entry.name.retained_bytes() as u64).ok_or(WorkError::Overflow),
                        )?;
                        retire_name_owned(page, bytes, accounting)?;
                        if found.is_some() || !has_more { break found; }
                    };
                    let bytes = parent.0.retained_bytes();
                    retire_name_owned(parent, bytes, accounting)?;
                    let bytes = folded_target.0.capacity() as u64;
                    retire_name_owned(folded_target, bytes, accounting)?;
                    Ok(found)
                },
            )?;
            let mut future: acyclic_stream::BoxProviderFuture<'_, _> = future;
            let result = future.as_mut().await;
            drop(future);
            let mut frame_custody = frame_custody.ok_or(FsError::PendingMutationAllocationFailed)?;
            accounting.release_owned(&mut frame_custody, frame_bytes)?;
            result
        }
    }
}


use super::*;

impl<S: crate::AsyncObjectStore> ProofContext<'_, S> {
    async fn changes(
        &self,
        before: ObjectId,
        after: ObjectId,
    ) -> Result<(crate::GenerationDiff, ProofGuard), crate::FsError> {
        let maximum = u32::try_from(self.limits.maximum_files).unwrap_or(u32::MAX);
        let (diff, _allocation) = match &self.meter {
            #[cfg(not(target_arch = "wasm32"))]
            ProofMeter::Native {
                original,
                cancellation,
            } => {
                let mut accounting =
                    crate::kernel::allocation::KernelAccounting::native(original, cancellation);
                let (diff, allocation) = crate::facade::diff_generation_file_tables(
                    self.store,
                    before,
                    after,
                    maximum,
                    self.limits.decode,
                    &mut accounting,
                    cancellation,
                )
                .await?;
                let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
                    allocation
                else {
                    return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                };
                return Ok((diff, ProofGuard::Native(allocation)));
            }
            ProofMeter::Standalone { cancellation, .. } => {
                let remaining = self
                    .meter
                    .with_accounting(|accounting| accounting.remaining())?;
                let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
                    remaining,
                    cancellation,
                );
                let result = crate::facade::diff_generation_file_tables(
                    self.store,
                    before,
                    after,
                    maximum,
                    self.limits.decode,
                    &mut accounting,
                    cancellation,
                )
                .await;
                self.meter.with_accounting(|parent| {
                    let (allocations, work, budget, _) = parent
                        .standalone_parts()
                        .ok_or(crate::FsError::HandleAllocationFailed)?;
                    *work = work.with_backend(accounting.spent(), allocations.live_bytes())?;
                    work.verify(budget)?;
                    Ok(())
                })?;
                result?
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        drop(_allocation);
        let bytes = (diff.files.capacity() * size_of::<crate::FileRecordChange>())
            .checked_add(diff.bindings.capacity() * size_of::<crate::DirectoryBindingChange>())
            .and_then(|bytes| {
                diff.bindings.iter().try_fold(bytes, |bytes, binding| {
                    bytes.checked_add(binding.name.retained_bytes())
                })
            })
            .ok_or(WorkError::Overflow)? as u64;
        self.meter.owned(|accounting| {
            let allocation = accounting.claim_owned(bytes, 0)?;
            Ok((diff, allocation, bytes))
        })
    }

    pub(super) async fn prove_increment(
        &mut self,
        root: &GenerationRoot,
        previous: &CachedProof,
    ) -> Result<Option<(u64, u128, u64)>, crate::FsError> {
        let diff = self
            .changes(previous.root.file_table, root.file_table)
            .await?;
        if diff.0.truncated {
            return Ok(None);
        }
        if !self.validate_increment_namespace(&diff.0)? {
            return Ok(None);
        }
        let mut file_count = previous.file_count;
        let mut logical_bytes = previous.logical_file_bytes;
        for change in &diff.0.files {
            self.examine(1)?;
            let Some(after) = change.after else {
                return Ok(None);
            };
            logical_bytes = logical_bytes
                .checked_sub(change.before.map_or(0, file_bytes))
                .and_then(|bytes| bytes.checked_add(file_bytes(after)))
                .ok_or(ClosureError::LogicalBytesOverflow)?;
            if change.before.is_none() {
                file_count = file_count.checked_add(1).ok_or(WorkError::Overflow)?;
            }
        }
        if file_count > self.limits.maximum_files {
            return Ok(None);
        }
        self.collect_pages::<crate::kernel::file_table_mutation::FileTableFormat>(
            root.file_table,
            Some(previous.root.file_table),
        )
        .await?;
        for change in &diff.0.files {
            let Some(record) = change.after else {
                return Ok(None);
            };
            if let FilePayload::Directory { entries } = record.payload {
                self.prove_metadata(record.metadata).await?;
                let before = change.before.and_then(|record| match record.payload {
                    FilePayload::Directory { entries } => Some(entries),
                    _ => None,
                });
                self.collect_pages::<crate::kernel::tree_mutation::TreeFormat>(entries, before)
                    .await?;
            } else {
                self.prove_record(&record).await?;
            }
        }
        let bytes = previous
            .object_bytes
            .checked_add(self.object_bytes)
            .ok_or(WorkError::Overflow)?;
        // These are conservative bounds; recompute the exact closure when they reach a limit.
        let object_count = previous
            .object_count
            .checked_add(self.objects.values.len() as u64)
            .ok_or(WorkError::Overflow)?;
        if bytes > self.limits.maximum_object_bytes || object_count > self.limits.maximum_objects {
            return Ok(None);
        }
        self.object_bytes = bytes;
        Ok(Some((file_count, logical_bytes, object_count)))
    }

    fn validate_increment_namespace(
        &self,
        diff: &crate::GenerationDiff,
    ) -> Result<bool, crate::FsError> {
        // File changes arrive in FileId order from the authenticated file-table diff.
        let mut links = ProofVec::new(&self.meter);
        for change in &diff.files {
            self.examine(1)?;
            let Some(after) = change.after else {
                return Ok(false);
            };
            if after.link_count != 1
                || !matches!(
                    after.kind,
                    crate::kernel::FileKind::Directory | crate::kernel::FileKind::Regular
                )
                || change
                    .before
                    .is_some_and(|before| before.kind != after.kind || before.link_count != 1)
            {
                return Ok(false);
            }
            links.push(IncrementLink {
                parent: None,
                anchored: change.before.is_some(),
                visiting: false,
            })?;
        }
        for binding in &diff.bindings {
            self.examine(1)?;
            let Some(after) = &binding.after else {
                return Ok(false);
            };
            if binding.before.is_some() {
                return Ok(false);
            }
            let Some(child) = self.increment_file(&diff.files, after.file_id)? else {
                return Ok(false);
            };
            let Some(parent) = self.increment_file(&diff.files, binding.directory_id)? else {
                return Ok(false);
            };
            let change = diff
                .files
                .get(child)
                .ok_or(crate::ObjectStoreError::Corrupt)?;
            if change.before.is_some()
                || !change.after.is_some_and(|record| record.kind == after.kind)
                || !diff
                    .files
                    .get(parent)
                    .ok_or(crate::ObjectStoreError::Corrupt)?
                    .after
                    .is_some_and(|record| record.kind == crate::kernel::FileKind::Directory)
                || links
                    .values
                    .get_mut(child)
                    .ok_or(crate::ObjectStoreError::Corrupt)?
                    .parent
                    .replace(parent)
                    .is_some()
            {
                return Ok(false);
            }
        }
        for start in 0..links.values.len() {
            let mut next = start;
            loop {
                self.examine(1)?;
                let link = links
                    .values
                    .get_mut(next)
                    .ok_or(crate::ObjectStoreError::Corrupt)?;
                if link.anchored {
                    break;
                }
                if link.visiting {
                    return Ok(false);
                }
                link.visiting = true;
                let Some(parent) = link.parent else {
                    return Ok(false);
                };
                next = parent;
            }
            // Each new directory's ancestry is authenticated once, including shared prefixes.
            next = start;
            loop {
                self.examine(1)?;
                let link = links
                    .values
                    .get_mut(next)
                    .ok_or(crate::ObjectStoreError::Corrupt)?;
                if link.anchored {
                    break;
                }
                link.anchored = true;
                let Some(parent) = link.parent else {
                    return Ok(false);
                };
                next = parent;
            }
        }
        Ok(true)
    }

    fn increment_file(
        &self,
        files: &[crate::FileRecordChange],
        id: FileId,
    ) -> Result<Option<usize>, crate::FsError> {
        let (mut low, mut high) = (0, files.len());
        while low < high {
            self.examine(1)?;
            let middle = low + (high - low) / 2;
            match files
                .get(middle)
                .ok_or(crate::ObjectStoreError::Corrupt)?
                .file_id
                .cmp(&id)
            {
                std::cmp::Ordering::Equal => return Ok(Some(middle)),
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
            }
        }
        Ok(None)
    }

    async fn collect_pages<F: crate::kernel::persistent_btree::Format>(
        &mut self,
        root: ObjectId,
        before: Option<ObjectId>,
    ) -> Result<(), crate::FsError> {
        use crate::kernel::persistent_btree::Page;
        Self::require_kind(root, F::kind())?;
        let mut pending: ProofVec<'_, (PendingPage<F::Key>, Option<ObjectId>)> =
            ProofVec::new(&self.meter);
        pending.push(((root, None, None), before))?;
        let mut pages = ProofVec::new(&self.meter);
        while let Some(((id, lower, upper), before)) = pending.values.pop() {
            self.examine(1)?;
            if !set_insert(&mut pages, id)? {
                return Err(ClosureError::RoutingAlias.into());
            }
            let old_bytes = if let Some(before) = before.filter(|before| *before != id) {
                self.meter.charge(WorkCounters {
                    page_reads: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let bytes = self
                    .meter
                    .read(self.store, before, self.limits.decode.maximum_object_bytes)
                    .await?;
                self.object_bytes = self
                    .object_bytes
                    .checked_add(usize_to_u64(bytes.0.bytes.len()))
                    .ok_or(WorkError::Overflow)?;
                Some(bytes)
            } else {
                None
            };
            let bytes = self.read_object(id, true).await?;
            let page = self.meter.decode_steps(|observe| {
                F::decode(&bytes.0.bytes, self.limits.decode, &mut |step| {
                    observe(step)
                })
            })?;
            drop(bytes);
            let old = old_bytes
                .as_ref()
                .map(|bytes| {
                    self.meter.decode_steps(|observe| {
                        F::decode(&bytes.0.bytes, self.limits.decode, &mut |step| {
                            observe(step)
                        })
                    })
                })
                .transpose()?;
            drop(old_bytes);
            let old_children = old.as_ref().and_then(|page| match &page.0 {
                Page::Internal(children) => Some(children.as_slice()),
                Page::Leaf(_) => None,
            });
            let mut reused = before == Some(id);
            if !reused {
                for child in old_children.unwrap_or_default() {
                    self.examine(1)?;
                    if child.page == id {
                        reused = true;
                        break;
                    }
                }
            }
            let lower = lower.as_ref().map(|key| &key.0);
            let upper = upper.as_ref().map(|key| &key.0);
            match &page.0 {
                Page::Leaf(values) => {
                    validate_ordered_bounds(values.iter().map(F::key), lower, upper)?;
                }
                Page::Internal(children) => {
                    validate_ordered_bounds(
                        children.iter().map(|child| &child.first),
                        lower,
                        upper,
                    )?;
                    let old_children = old_children.unwrap_or_default();
                    let mut old_index = old_children.len();
                    for (index, child) in children.iter().enumerate().rev() {
                        // A proven subtree needs only its boundary checked against its new parent.
                        if reused && index + 1 != children.len() {
                            continue;
                        }
                        let upper_key = children.get(index + 1).map(|next| &next.first).or(upper);
                        // Equal immutable pages under equal bounds retain their parent proof.
                        let old = self.match_child(old_children, &mut old_index, child)?;
                        if old.is_some_and(|old| old.page == child.page && old.first == child.first)
                            && old_children
                                .get(old_index)
                                .is_some_and(|next| upper_key == Some(&next.first))
                        {
                            continue;
                        }
                        let lower = self
                            .meter
                            .clone_key::<F>(&child.first, self.limits.decode.maximum_name_bytes)?;
                        let upper = upper_key
                            .map(|key| {
                                self.meter
                                    .clone_key::<F>(key, self.limits.decode.maximum_name_bytes)
                            })
                            .transpose()?;
                        let prior = if reused || before == Some(child.page) {
                            Some(child.page)
                        } else if !old_children.is_empty() {
                            old.map(|old| old.page)
                        } else {
                            before
                        };
                        pending.push(((child.page, Some(lower), upper), prior))?;
                    }
                }
            }
        }
        Ok(())
    }

    fn match_child<'c, K: Ord>(
        &self,
        old: &'c [crate::kernel::Child<K>],
        cursor: &mut usize,
        child: &crate::kernel::Child<K>,
    ) -> Result<Option<&'c crate::kernel::Child<K>>, crate::FsError> {
        while let Some(index) = cursor.checked_sub(1) {
            self.examine(1)?;
            let candidate = old.get(index).ok_or(ObjectStoreError::Corrupt)?;
            if candidate.first <= child.first {
                return Ok(Some(candidate));
            }
            *cursor = index;
        }
        Ok(None)
    }

    pub(super) fn remember(
        &self,
        slot: Option<Arc<super::super::ProofSlot<CachedProof>>>,
        began: crate::collection::CollectionEpoch,
        proof: &GenerationProof,
        object_count: u64,
    ) -> Result<Option<PendingProof>, crate::FsError> {
        let Some(slot) = slot else { return Ok(None) };
        let root = self.meter.owned(|accounting| {
            let (root, allocation) =
                crate::collection::clone_generation_root_with_accounting(&proof.root, accounting)?;
            let bytes = (root.parents.capacity() * size_of::<GenerationId>()) as u64;
            Ok((root, allocation, bytes))
        })?;
        let mut root = root;
        let mut header = self.meter.owned(|accounting| {
            let bytes = (size_of::<CachedProof>() + 2 * size_of::<usize>()) as u64;
            Ok(((), accounting.claim_owned(bytes, 1)?, bytes))
        })?;
        root.1.absorb(&mut header.1)?;
        let cached = CachedProof {
            limits: self.limits,
            root: root.0,
            generation_id: proof.generation_id,
            file_count: proof.file_count,
            logical_file_bytes: proof.logical_file_bytes,
            object_count,
            _guard: root.1,
            object_bytes: self.object_bytes,
        };
        Ok(Some(PendingProof::new(slot, began, cached)))
    }
}

struct IncrementLink {
    parent: Option<usize>,
    anchored: bool,
    visiting: bool,
}

fn file_bytes(record: FileRecord) -> u128 {
    match record.payload {
        FilePayload::Regular { logical_bytes, .. } => u128::from(logical_bytes),
        FilePayload::InlineRegular(bytes) => bytes.as_bytes().len() as u128,
        _ => 0,
    }
}

#[cfg(test)]
mod tests;

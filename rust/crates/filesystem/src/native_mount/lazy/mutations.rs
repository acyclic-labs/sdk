use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) async fn mutation_file_id_with_carrier(
        &self,
        text: &str,
        lookup: &LazyLookup,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<FileId, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            let caller = carrier.cancellation();
            return self.lazy.stable_file_id_for_lookup(
                text, lookup, &mut crate::lazy_workspace::LazyReadonlyAccounting::Native { original: &original, cancellation: &caller },
            ).await.map_err(native_lazy_error);
        }
        let (budget, caller) = carrier.with_work(|work| {
            Ok::<_, MountSourceError>((
                work.remaining().map_err(mount_demand_error)?, work.cancellation().clone(),
            ))
        })?;
        let mut accounting = crate::lazy_workspace::LazyReadonlyAccounting::Portable {
            work: crate::WorkCounters::UNCHARGED, budget, cancellation: &caller,
        };
        let result = self.lazy.stable_file_id_for_lookup(text, lookup, &mut accounting).await;
        let result = accounting.finish_measured(result).map_err(LazyWorkspaceError::into_measured_failure);
        carrier.with_work(|work| mount_accept(work, result, lazy_error))
    }

pub(in crate::native_mount::lazy) fn reserve_detached_slot_with_carrier(
        &self, file_id: FileId, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<usize>, MountSourceError> {
        let Some(original) = carrier.original_native_work() else {
            return carrier.with_work(|work| self.reserve_detached_slot_with_work(file_id, work));
        };
        let mut entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let position = match carrier.with_work(|work| detached_position(&entries, file_id, work))? {
            Ok(_) => return Ok(None),
            Err(position) => position,
        };
        if entries.len() < entries.capacity() { return Ok(Some(position)); }
        use crate::native_mount::adapter::CaptureInputScratch;
        let count = entries.len().checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copied = entries.len().checked_mul(std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let copy = original.reserve_work(crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
        let mut items = original.reserve_work(crate::WorkCounters { items_examined: entries.len() as u64, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
        let mut scratch = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(&original)?;
        let values = scratch.vec::<(FileId, DetachedIdentity<A, O>)>(count)?;
        let bytes = values.capacity().checked_mul(std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let mut replacement = DetachedTable {
            entries: values, custody: None, bytes, native_allocation: Some(scratch.retain()),
        };
        let examined = entries.len() as u64;
        replacement.entries.append(&mut entries.entries);
        let old = std::mem::replace(&mut *entries, replacement);
        drop(old);
        let recorded_copy = { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork);
        let recorded_items = items.record_completed(crate::WorkCounters { items_examined: examined, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork);
        recorded_copy.and(recorded_items)?;
        Ok(Some(position))
    }

pub(in crate::native_mount::lazy) fn reserve_detached_slot_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Option<usize>, MountSourceError> {
        let mut entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let position = match detached_position(&entries, file_id, work)? {
            Ok(_) => return Ok(None),
            Err(position) => position,
        };
        if entries.len() < entries.capacity() { return Ok(Some(position)); }
        let mut scratch = DirectoryScratch::new(work);
        let custody = crate::native_mount::NativeMountAllocationCustody::new(scratch.work)
            .map_err(mount_demand_error)?;
        let count = entries.len().checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let copied = crate::WorkCounters {
            items_examined: entries.len() as u64,
            bytes_copied: (entries.len() * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        scratch.work.work.admit(&copied, &scratch.work.budget).map_err(MountSourceError::Work)?;
        let replacement = mount_vec(count, scratch.work)?;
        let new_bytes = (replacement.capacity() * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64;
        scratch.track(new_bytes)?;
        custody.retain_admitted(new_bytes, scratch.work).map_err(mount_demand_error)?;
        let mut replacement = DetachedTable { entries: replacement, custody: Some(custody), bytes: new_bytes, native_allocation: None };
        scratch.keep(new_bytes)?;
        replacement.entries.append(&mut entries.entries);
        let old = std::mem::replace(&mut *entries, replacement);
        drop(old);
        scratch.work.record(copied).map_err(mount_demand_error)?;
        Ok(Some(position))
    }

pub(in crate::native_mount::lazy) async fn prepare_removed_identity_with_carrier(
        &self,
        lookup: MountLookup,
        owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<PreparedMountedIdentity<A, O>, MountSourceError> {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let record = self.authored.record_by_id(lookup.node.file_id, owner, scratch.carrier).await?;
        let existing = scratch.carrier.with_work(|work| {
            let entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            Ok::<_, MountSourceError>(match detached_position(&entries, record.file_id, work)? {
                Ok(position) => Some(Arc::clone(&entries[position].1.file)),
                Err(_) => None,
            })
        })?;
        let file = if let Some(file) = existing { file } else {
            self.authored.detached_file_from_record(record, lookup.metadata, owner, scratch.carrier).await?
        };
        let position = self.reserve_detached_slot_with_carrier(record.file_id, scratch.carrier)?;
        let (paths, new_record, logical_bytes, final_work) = scratch.carrier.with_work(|work| {
            let entries = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
            let shift = position.map_or(0, |position| entries.len() - position);
            drop(entries);
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            let (paths, new_record) = if let Some(existing) = removals.records.get(&record.file_id) {
                (existing.paths, false)
            } else {
                let mut paths = 0_usize;
                for removed in removals.paths.values() {
                    mount_items(work, 1)?;
                    if removed.file_id == record.file_id {
                        paths = paths.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                    }
                }
                (paths, true)
            };
            // Logical inline data only; no BTree node layout or allocator event.
            let logical_bytes = if new_record {
                (std::mem::size_of::<FileId>() + std::mem::size_of::<CapturedRemovalData>()) as u64
            } else { 0 };
            work.adopt(logical_bytes).map_err(mount_demand_error)?;
            let copied = if new_record { logical_bytes } else {
                std::mem::size_of::<(FileRecord, FileMetadata)>() as u64
            };
            let final_work = crate::WorkCounters {
                items_examined: 1 + u64::from(position.is_some()),
                bytes_copied: copied + (shift * std::mem::size_of::<(FileId, DetachedIdentity<A, O>)>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            Ok::<_, MountSourceError>((paths, new_record, logical_bytes, final_work))
        })?;
        scratch.track(logical_bytes)?;
        let captured = if new_record {
            let custody = scratch.carrier.new_custody()?;
            scratch.carrier.with_work(|work| custody.retain_admitted(logical_bytes, work))
                .map_err(mount_demand_error)?;
            scratch.keep(logical_bytes)?;
            Some(CapturedRemovalRecord {
                value: (record, lookup.metadata), paths, custody: Some(custody), bytes: logical_bytes,
            })
        } else { None };
        crate::native_mount::adapter::record_view_change(&self.authored.shared_checkout().ledger, &ViewChange::Node(record.file_id), scratch.carrier)?;
        Ok(PreparedMountedIdentity {
            record, metadata: lookup.metadata, file, position, paths,
            captured, final_work,
        })
    }

pub(in crate::native_mount::lazy) fn prepare_removed_path_with_carrier(
        &self,
        path: &MountPath,
        key: MountOwnedBuffer<String>,
        file_id: FileId,
        identity: Option<&PreparedMountedIdentity<A, O>>,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<PreparedMountedRemoval, MountSourceError> {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let (prior, changed, old_paths, new_paths, logical_bytes, final_work) = scratch.carrier.with_work(|work| {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            let prior = removals.paths.get(key.value.as_str()).copied();
            let changed = prior.map(|prior| prior.file_id) != Some(file_id);
            let mut paths_for = |id| -> Result<Option<usize>, MountSourceError> {
                if let Some(identity) = identity.filter(|identity| identity.record.file_id == id) {
                    return Ok(Some(identity.paths));
                }
                mount_items(work, 1)?;
                Ok(removals.records.get(&id).map(|record| record.paths))
            };
            let old_paths = if changed {
                if let Some(prior) = prior {
                    paths_for(prior.file_id)?.map(|paths| paths.checked_sub(1)
                        .ok_or(MountSourceError::Stale)).transpose()?
                } else { None }
            } else { None };
            let new_paths = if changed {
                paths_for(file_id)?.map(|paths| paths.checked_add(1)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))).transpose()?
            } else { None };
            let logical_bytes = if prior.is_none() { std::mem::size_of::<MountedRemoval>() as u64 } else { 0 };
            work.adopt(logical_bytes).map_err(mount_demand_error)?;
            Ok::<_, MountSourceError>((prior, changed, old_paths, new_paths, logical_bytes, crate::WorkCounters {
                items_examined: 1 + u64::from(old_paths.is_some()) + u64::from(new_paths.is_some()),
                bytes_copied: std::mem::size_of::<MountedRemoval>() as u64,
                ..crate::WorkCounters::UNCHARGED
            }))
        })?;
        scratch.track(logical_bytes)?;
        let namespace = self.authored.namespace_path(path, scratch.carrier)?;
        if namespace.1.is_none() { scratch.track(namespace.0.retained_bytes())?; }
        crate::native_mount::adapter::record_view_change(&self.authored.shared_checkout().ledger, &ViewChange::Entry(&namespace.0, file_id), scratch.carrier)?;
        let bytes = key.legacy_owned_bytes().checked_add(logical_bytes)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let custody = scratch.carrier.new_custody()?;
        scratch.carrier.with_work(|work| custody.retain_admitted(bytes, work)).map_err(mount_demand_error)?;
        scratch.keep(logical_bytes)?;
        let key = MountedRemovalPath {
            text: key.value, custody: Some(custody), bytes, native_allocation: key.allocation,
        };
        Ok(PreparedMountedRemoval { key, file_id, prior, old_paths, new_paths, changed, final_work })
    }

pub(in crate::native_mount::lazy) fn prepare_rebound_with_carrier(
        &self,
        key: &str,
        identity: Option<&PreparedMountedIdentity<A, O>>,
        removal: &PreparedMountedRemoval,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<PreparedMountedRebound, MountSourceError> {
        carrier.with_work(|work| {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            mount_items(work, 1)?;
            let removed = removals.paths.get_key_value(key);
            let removed = removed.map(|(_, removed)| *removed);
            let paths = if let Some(removed) = removed {
                let before = if removal.changed && removal.file_id == removed.file_id {
                    removal.new_paths
                } else if removal.changed && removal.prior.is_some_and(|prior| prior.file_id == removed.file_id) {
                    removal.old_paths.filter(|paths| *paths != 0)
                } else if let Some(identity) = identity.filter(|identity| identity.record.file_id == removed.file_id) {
                    Some(identity.paths)
                } else {
                    mount_items(work, 1)?;
                    removals.records.get(&removed.file_id).map(|record| record.paths)
                };
                before.map(|paths| paths.checked_sub(1).ok_or(MountSourceError::Stale)).transpose()?
            } else { None };
            Ok(PreparedMountedRebound { removed, paths, final_work: crate::WorkCounters {
                items_examined: u64::from(removed.is_some()) + u64::from(paths.is_some()),
                ..crate::WorkCounters::UNCHARGED
            } })
        })
    }

pub(in crate::native_mount::lazy) fn commit_removed_with_carrier(
        &self,
        removal: PreparedMountedRemoval,
        identity: Option<PreparedMountedIdentity<A, O>>,
        rebound: Option<(&str, PreparedMountedRebound)>,
        scratch: &mut DirectoryCarrierScratch<'_, '_>,
    ) -> Result<(), MountSourceError> {
        let mut delta = removal.final_work;
        if let Some(identity) = &identity { delta = delta.checked_add(identity.final_work)?; }
        if let Some((_, rebound)) = &rebound { delta = delta.checked_add(rebound.final_work)?; }
        if delta.items_examined > scratch.reserved_items || delta.bytes_copied > scratch.reserved_copies {
            return Err(MountSourceError::Work(crate::WorkError::Overflow));
        }
        let mut completed = crate::WorkCounters::UNCHARGED;
        let result = (|| {
            if let Some(identity) = identity {
                let record_copies = if identity.captured.is_some() {
                    (std::mem::size_of::<FileId>() + std::mem::size_of::<CapturedRemovalData>()) as u64
                } else {
                    std::mem::size_of::<(FileRecord, FileMetadata)>() as u64
                };
                if let Some(position) = identity.position {
                    let mut detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
                    completed.items_examined += 1;
                    detached.insert(position, (
                        identity.record.file_id,
                        DetachedIdentity { file: identity.file, ready: true, published_epoch: 0, pending: None },
                    ));
                    completed.bytes_copied += identity.final_work.bytes_copied - record_copies;
                }
                let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
                completed.items_examined += 1;
                if let Some(captured) = identity.captured {
                    removals.records.insert(identity.record.file_id, captured);
                } else {
                    removals.records.get_mut(&identity.record.file_id).ok_or(MountSourceError::Stale)?
                        .value = (identity.record, identity.metadata);
                }
                completed.bytes_copied += record_copies;
            }
            let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            completed.items_examined += 1;
            removals.paths.insert(removal.key, MountedRemoval { file_id: removal.file_id, operation: None });
            completed.bytes_copied += std::mem::size_of::<MountedRemoval>() as u64;
            if removal.changed {
                if let (Some(prior), Some(paths)) = (removal.prior, removal.old_paths) {
                    completed.items_examined += 1;
                    if paths == 0 { removals.records.remove(&prior.file_id); } else {
                        removals.records.get_mut(&prior.file_id).ok_or(MountSourceError::Stale)?.paths = paths;
                    }
                }
                if let Some(paths) = removal.new_paths {
                    completed.items_examined += 1;
                    removals.records.get_mut(&removal.file_id).ok_or(MountSourceError::Stale)?.paths = paths;
                }
                removals.epoch = removals.epoch.wrapping_add(1);
            }
            if let Some((key, rebound)) = &rebound {
                if let Some(removed) = rebound.removed {
                    completed.items_examined += 1;
                    removals.paths.remove(*key);
                    if let Some(paths) = rebound.paths {
                        completed.items_examined += 1;
                        if paths == 0 { removals.records.remove(&removed.file_id); } else {
                            removals.records.get_mut(&removed.file_id).ok_or(MountSourceError::Stale)?.paths = paths;
                        }
                    }
                    removals.epoch = removals.epoch.wrapping_add(1);
                }
            }
            Ok(())
        })();
        let items = std::mem::take(&mut scratch.reserved_items);
        let copies = std::mem::take(&mut scratch.reserved_copies);
        let accounted = scratch.carrier.with_work(|work| {
            let copied = work.complete(crate::materializer::NativeResourceCredit::Copy, copies, completed.bytes_copied);
            let examined = work.complete(crate::materializer::NativeResourceCredit::Items, completed.items_examined, completed.items_examined);
            work.refund(crate::materializer::NativeResourceCredit::Items, items - completed.items_examined).expect("original reservation is balanced");
            copied.and(examined)
        });
        match (result, accounted) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(mount_demand_error(cleanup)),
            (Err(primary), Err(cleanup)) => Err(scratch.finalization_error(primary, cleanup)),
        }
    }

pub(in crate::native_mount::lazy) fn set_attributes_with_carrier(
        &self,
        path: &MountPath,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || async {
                let _mutation = self.source_mutation_lease_with_carrier(None, carrier).await?;
                let owner = SourceViewGate::callback_owner();
                let mut scratch = DirectoryCarrierScratch::new(carrier);
                let detached_present = scratch.carrier.with_work(|work| {
                    mount_items(work, 1)?;
                    Ok::<_, MountSourceError>(!self.detached.lock()
                        .map_err(|_| MountSourceError::Stale)?.is_empty())
                })?;
                if detached_present {
                    let text = mount_path_text_with_carrier(&self.root, path, scratch.carrier)?;
                    scratch.track(text.legacy_owned_bytes())?;
                    let resolution = self.resolve(path, &text, owner, scratch.carrier).await?;
                    if let Some(file) = self.content_detached_with_carrier(&resolution, &text, scratch.carrier).await? {
                        return file.set_attributes_async_with_carrier(metadata, logical_bytes, scratch.carrier).await;
                    }
                }
                self.promote_with_carrier(path, owner, scratch.carrier).await?;
                self.authored.set_attributes_async_with_carrier(path, metadata, logical_bytes, scratch.carrier).await
            })
        })
    }

pub(in crate::native_mount::lazy) fn record_rebound(&self, path: &str) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals.rebound(path) {
            self.cursors.clear();
        }
        Ok(())
    }
}

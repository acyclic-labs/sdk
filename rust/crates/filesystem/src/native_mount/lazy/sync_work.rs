use super::*;

pub(in crate::native_mount::lazy) struct MountSyncWork<'w> {
    pub(in crate::native_mount::lazy) meter: crate::native_mount::adapter::NativeDemandCarrier<'w>,
    pub(in crate::native_mount::lazy) view: crate::native_mount::view_ledger::ViewLedgerAdmission,
    pub(in crate::native_mount::lazy) view_slot: usize,
    pub(in crate::native_mount::lazy) scratch_custody: Option<crate::native_mount::NativeMountAllocationCustody>,
    pub(in crate::native_mount::lazy) reserved_items: u64,
    pub(in crate::native_mount::lazy) observation: MountPublicationObservation,
    pub(in crate::native_mount::lazy) publication_snapshot: Option<crate::NativePublicationSnapshot>,
    pub(in crate::native_mount::lazy) accounting_failure: Option<MountSourceError>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::native_mount::lazy) enum MountPublicationObservation { Ordinary, Qualified }

pub(in crate::native_mount::lazy) struct MountSyncBuffer<T: Default> {
    pub(in crate::native_mount::lazy) value: T,
    pub(in crate::native_mount::lazy) custody: Option<crate::native_mount::NativeMountAllocationCustody>,
    pub(in crate::native_mount::lazy) bytes: u64,
    pub(in crate::native_mount::lazy) native_allocation: Option<crate::NativeAllocation>,
}

impl<T: Default> MountSyncBuffer<T> {
    pub(in crate::native_mount::lazy) fn claim_additional(&mut self, bytes: u64, work: &mut MountSyncWork<'_>) -> Result<(), MountSourceError> {
        if bytes == 0 { return Ok(()); }
        let total = self.bytes.checked_add(bytes).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let custody = match &self.custody {
            Some(custody) => custody.clone(),
            None => {
                let custody = work.meter.with_work(crate::native_mount::NativeMountAllocationCustody::new)
                    .map_err(mount_demand_error)?;
                self.custody = Some(custody.clone());
                custody
            }
        };
        work.meter.with_work(|meter| custody.claim(bytes, meter)).map_err(mount_demand_error)?;
        self.bytes = total;
        Ok(())
    }

    pub(in crate::native_mount::lazy) fn absorb_buffer_guard<U: Default>(&mut self, other: &mut MountSyncBuffer<U>) -> Result<(), MountSourceError> {
        let total = self.bytes.checked_add(other.bytes).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        if let Some(other_allocation) = &mut other.native_allocation {
            if let Some(allocation) = &mut self.native_allocation {
                allocation.absorb(other_allocation).map_err(MountSourceError::NativeWork)?;
            } else {
                self.native_allocation = Some(other_allocation.split_off(other_allocation.bytes())
                    .map_err(MountSourceError::NativeWork)?);
            }
        }
        self.bytes = total;
        other.bytes = 0;
        Ok(())
    }

    pub(in crate::native_mount::lazy) fn take_native_parts(&mut self) -> Result<(T, crate::NativeAllocation), MountSourceError> {
        let allocation = self.native_allocation.take().ok_or(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::MissingCustody,
        ))?;
        self.bytes = 0;
        Ok((std::mem::take(&mut self.value), allocation))
    }
}

impl MountSyncBuffer<Vec<String>> {
    pub(in crate::native_mount::lazy) fn push_cloned_text(&mut self, text: &str, work: &mut MountSyncWork<'_>) -> Result<(), MountSourceError> {
        let copied = crate::WorkCounters { bytes_copied: text.len() as u64, ..crate::WorkCounters::UNCHARGED };
        if let Some(original) = work.meter.original_native_work() {
            use crate::native_mount::adapter::CaptureInputScratch;
            let mut scratch = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(&original)?;
            scratch.admit(copied)?;
            let mut bytes = scratch.vec::<u8>(text.len())?;
            bytes.extend_from_slice(text.as_bytes());
            scratch.record(copied)?;
            let mut owned = (String::from_utf8(bytes).expect("source String is UTF-8"), scratch.retain());
            let retained = owned.0.capacity() as u64;
            let total = self.bytes.checked_add(retained).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            if let Some(allocation) = &mut self.native_allocation {
                allocation.absorb(&mut owned.1).map_err(MountSourceError::NativeWork)?;
            } else {
                self.native_allocation = Some(owned.1.split_off(owned.1.bytes()).map_err(MountSourceError::NativeWork)?);
            }
            self.value.push(owned.0);
            self.bytes = total;
            Ok(())
        } else {
            work.admit(&copied)?;
            self.claim_additional(text.len() as u64, work)?;
            self.value.push(text.to_owned());
            work.meter.with_work(|meter| meter.record(copied)).map_err(mount_demand_error)
        }
    }
}

impl<T: Default> std::ops::Deref for MountSyncBuffer<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target { &self.value }
}

impl<T: Default> std::ops::DerefMut for MountSyncBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.value }
}

impl<T: Default> Drop for MountSyncBuffer<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.value));
        if let Some(custody) = &self.custody {
            custody.retire(self.bytes).expect("mounted sync buffer ownership balances");
        }
    }
}

pub(in crate::native_mount::lazy) fn retain_sync_buffer<T: Default>(
    value: T, bytes: u64, work: &mut MountSyncWork<'_>,
) -> Result<MountSyncBuffer<T>, MountSourceError> {
    if bytes == 0 { return Ok(MountSyncBuffer { value, custody: None, bytes: 0, native_allocation: None }); }
    match work.retain_scratch(bytes) {
        Ok(custody) => Ok(MountSyncBuffer { value, custody: Some(custody), bytes, native_allocation: None }),
        Err(error) => {
            drop(value);
            work.meter.with_work(|meter| meter.release(bytes)).map_err(mount_demand_error)?;
            Err(error)
        }
    }
}

impl<'w> MountSyncWork<'w> {
    pub(in crate::native_mount::lazy) fn new<A, O>(
        source: &CheckoutMountSource<A, O>, mut meter: crate::native_mount::adapter::NativeDemandCarrier<'w>,
    ) -> Result<Self, MountSourceError> {
        let (view_slot, view) = meter.with_work(|work| source.shared_checkout().ledger.take_for_work(work))?;
        Ok(Self { meter, view, view_slot, scratch_custody: None, reserved_items: 0,
            observation: MountPublicationObservation::Ordinary, publication_snapshot: None,
            accounting_failure: None })
    }

    pub(in crate::native_mount::lazy) fn retain_scratch(&mut self, bytes: u64) -> Result<crate::native_mount::NativeMountAllocationCustody, MountSourceError> {
        if self.scratch_custody.is_none() {
            let custody = self.meter.new_custody()?;
            self.scratch_custody = Some(custody);
        }
        let custody = self.scratch_custody.as_ref().expect("mounted scratch custody").clone();
        self.meter.with_work(|work| custody.retain_admitted(bytes, work))
            .map_err(mount_demand_error)?;
        Ok(custody)
    }

    pub(in crate::native_mount::lazy) fn owned_vec<T>(&mut self, count: usize) -> Result<MountSyncBuffer<Vec<T>>, MountSourceError> {
        if let Some(original) = self.meter.original_native_work() {
            use crate::native_mount::adapter::CaptureInputScratch;
            let mut scratch = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let value = scratch.vec::<T>(count)?;
            let bytes = value.capacity().checked_mul(std::mem::size_of::<T>())
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
            return Ok(MountSyncBuffer { value, custody: None, bytes, native_allocation: Some(scratch.retain()) });
        }
        let value = self.meter.with_work(|meter| mount_vec(count, meter))?;
        let bytes = value.capacity().checked_mul(std::mem::size_of::<T>())
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        retain_sync_buffer(value, bytes, self)
    }

    pub(in crate::native_mount::lazy) fn admit(&mut self, delta: &crate::WorkCounters) -> Result<(), MountSourceError> {
        if let Some(original) = self.meter.original_native_work() {
            original.admit(*delta).map_err(MountSourceError::NativeWork)
        } else {
            self.meter.with_work(|work| work.work.admit(delta, &work.budget)).map_err(MountSourceError::Work)
        }
    }

    pub(in crate::native_mount::lazy) fn namespace_buffer(
        &mut self, path: &str, config: crate::VolumeConfig,
    ) -> Result<MountSyncBuffer<Option<crate::kernel::NamespacePath>>, MountSourceError> {
        if let Some(original) = self.meter.original_native_work() {
            let cancellation = self.meter.cancellation();
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
            let owned = crate::kernel::NamespacePath::parse_in_profile_with_accounting(
                path, config.profile, config.limits, &mut accounting, &cancellation,
            ).map_err(MountSourceError::Filesystem)?;
            let allocation = match owned.1 {
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => allocation,
                crate::kernel::allocation::KernelOwnedAllocation::Standalone => {
                    return Err(MountSourceError::NativeWork(crate::NativeTreeMaterializationError::MissingCustody));
                }
            };
            let bytes = allocation.bytes();
            Ok(MountSyncBuffer { value: Some(owned.0), custody: None, bytes, native_allocation: Some(allocation) })
        } else {
            let namespace = self.meter.with_work(|meter| mount_namespace(path, config, meter))?;
            let bytes = mount_namespace_bytes(&namespace);
            retain_sync_buffer(Some(namespace), bytes, self)
        }
    }

    pub(in crate::native_mount::lazy) fn sdk_scope(&mut self) -> Result<(crate::WorkBudget, u64), MountSourceError> {
        self.meter.with_work(|work| Ok((
            work.remaining().map_err(mount_demand_error)?, work.live_allocation_bytes(),
        )))
    }

    pub(in crate::native_mount::lazy) fn reserve_items(&mut self, count: u64) -> Result<(), MountSourceError> {
        let reserved = self.reserved_items.checked_add(count)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.meter.with_work(|work| work.reserve(crate::materializer::NativeResourceCredit::Items, count)).map_err(mount_demand_error)?;
        self.reserved_items = reserved;
        Ok(())
    }
    pub(in crate::native_mount::lazy) fn restore_items(&mut self, count: u64) -> Result<(), MountSourceError> {
        let remaining = self.reserved_items.checked_sub(count)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.meter.with_work(|work| work.refund(crate::materializer::NativeResourceCredit::Items, count).expect("original reservation is balanced"));
        self.reserved_items = remaining;
        Ok(())
    }
    pub(in crate::native_mount::lazy) fn complete_items(&mut self, count: u64) -> Result<(), MountSourceError> {
        let remaining = self.reserved_items.checked_sub(count)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        self.meter.with_work(|work| work.complete(crate::materializer::NativeResourceCredit::Items, count, count)).map_err(mount_demand_error)?;
        self.reserved_items = remaining;
        Ok(())
    }

    pub(in crate::native_mount::lazy) async fn lock_checkout<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &mut self, source: &'a CheckoutMountSource<A, O>, cancellation: crate::CancellationToken,
    ) -> Result<crate::native_mount::adapter::SharedCheckoutGuard<'a, A, O>, MountSourceError> {
        source.shared_checkout().lock_with_carrier(
            &mut self.meter, &cancellation, Some(source.cancellation()),
        ).await
    }

    pub(in crate::native_mount::lazy) async fn sync_authored_guarded<A, O, F, Fut, E>(
        &mut self, source: &CheckoutMountSource<A, O>, permit: crate::PublicationPermit,
        force: bool, cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        source.sync_async_with_permit_carrier_guarded(
            permit, force, &mut self.meter, &mut self.view, cancellation, before_publication,
        ).await
    }

    pub(in crate::native_mount::lazy) async fn sync_authored_context_guarded<A, O, F, Fut, E>(
        &mut self, source: &CheckoutMountSource<A, O>, permit: crate::PublicationPermit,
        force: bool, cancellation: &crate::CancellationToken, before_publication: &mut F,
        context: crate::lazy_workspace::NativeSnapshotContext,
    ) -> Result<(), E>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        let result = source.sync_async_with_permit_carrier_guarded_context_with_ack(
            permit, force, &mut self.meter, &mut self.view, cancellation, before_publication,
            Some(context), &mut self.publication_snapshot,
        ).await;
        match result {
            Ok(snapshot) => self.publication_snapshot = Some(snapshot),
            Err((primary, accounting)) => {
                self.accounting_failure = accounting;
                return Err(primary);
            }
        }
        Ok(())
    }

    pub(in crate::native_mount::lazy) fn sync_authored_recovery_guarded<'a, A, O, F, Fut, E>(
        &'a mut self, source: &'a CheckoutMountSource<A, O>, permit: crate::PublicationPermit,
        cancellation: crate::CancellationToken, before_publication: &'a mut F,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>
    where A: AsyncAuthorityStore, O: AsyncObjectStore,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + Send + 'static,
    {
        Box::pin(async move {
            if self.observation == MountPublicationObservation::Qualified
                && let Some(context) = source.retained_publication_context_with_carrier(
                    &mut self.meter, &cancellation,
                ).await?
            {
                self.sync_authored_context_guarded(
                    source, permit, true, &cancellation, before_publication, context,
                ).await
            } else {
                self.sync_authored_guarded(source, permit, true, &cancellation, before_publication).await
            }
        })
    }
}

impl Drop for MountSyncWork<'_> {
    fn drop(&mut self) {
        let view = std::mem::take(&mut self.view);
        let reserved = std::mem::take(&mut self.reserved_items);
        self.meter.with_work(|work| {
            work.refund(crate::materializer::NativeResourceCredit::Items, reserved).expect("original reservation is balanced");
            work.restore_view_ledger_admission(self.view_slot, view);
        });
    }
}

pub(in crate::native_mount::lazy) fn mount_record_items(
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>, items: usize,
) -> Result<(), MountSourceError> {
    if let Some(original) = carrier.original_native_work() {
        original.charge(crate::WorkCounters { items_examined: items as u64, ..crate::WorkCounters::UNCHARGED })
            .map_err(MountSourceError::NativeWork)
    } else {
        carrier.with_work(|work| mount_items(work, items))
    }
}

pub(in crate::native_mount::lazy) fn mount_record_copy(
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>, bytes: u64, copy: impl FnOnce(),
) -> Result<(), MountSourceError> {
    if let Some(original) = carrier.original_native_work() {
        let reserved = original.reserve_work(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED }).map_err(MountSourceError::NativeWork)?;
        copy();
        { let mut copy = reserved; copy.record_completed(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED }) }.map_err(MountSourceError::NativeWork)
    } else {
        carrier.with_work(|work| work.charge(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED }))
            .map_err(mount_demand_error)?;
        copy();
        Ok(())
    }
}

pub(in crate::native_mount::lazy) fn mount_record_union<'a, I, J, K>(
    captured: I,
    candidate: J,
    dirty: K,
    mut output: Option<&mut Vec<(FileRecord, FileMetadata)>>,
    work: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
) -> Result<usize, MountSourceError>
where
    I: ExactSizeIterator<Item = (&'a FileRecord, &'a FileMetadata)>,
    J: ExactSizeIterator<Item = (&'a FileRecord, &'a FileMetadata)>,
    K: ExactSizeIterator<Item = (&'a FileRecord, &'a FileMetadata)>,
{
    let (mut captured, mut candidate, mut dirty) =
        (captured.peekable(), candidate.peekable(), dirty.peekable());
    let mut count = 0_usize;
    loop {
        let present = usize::from(captured.len() != 0)
            + usize::from(candidate.len() != 0) + usize::from(dirty.len() != 0);
        if present == 0 { return Ok(count); }
        mount_record_items(work, present)?;
        let first = captured.peek().map(|(record, _)| record.file_id);
        let second = candidate.peek().map(|(record, _)| record.file_id);
        let third = dirty.peek().map(|(record, _)| record.file_id);
        let id = [first, second, third].into_iter().flatten().min().ok_or(MountSourceError::Stale)?;
        let mut selected = None;
        if first == Some(id) { selected = captured.next(); }
        if second == Some(id) { selected = candidate.next(); }
        if third == Some(id) { selected = dirty.next(); }
        if let Some(output) = &mut output {
            let (record, metadata) = selected.ok_or(MountSourceError::Stale)?;
            mount_record_copy(work, std::mem::size_of::<(FileRecord, FileMetadata)>() as u64,
                || output.push((*record, *metadata)))?;
        }
        count = count.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    }
}

pub(in crate::native_mount::lazy) fn mount_upsert_record(
    records: &mut Vec<(FileRecord, FileMetadata)>,
    record: (FileRecord, FileMetadata),
    work: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
) -> Result<(), MountSourceError> {
    let (mut low, mut high) = (0, records.len());
    while low < high {
        mount_record_items(work, 1)?;
        let middle = low + (high - low) / 2;
        let existing = records.get(middle).ok_or(MountSourceError::Stale)?;
        match existing.0.file_id.cmp(&record.0.file_id) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                *records.get_mut(middle).ok_or(MountSourceError::Stale)? = record;
                return Ok(());
            }
        }
    }
    mount_record_copy(work, ((records.len() - low) * std::mem::size_of::<(FileRecord, FileMetadata)>()) as u64,
        || records.insert(low, record))?;
    Ok(())
}

use super::*;

#[cfg(any(target_os = "linux", windows))]
    #[derive(Default)]
    pub(in crate::demand) struct NativeSourceWatches {
        pub(in crate::demand) entries: Vec<Option<std::sync::Weak<NativeSourceWatchBacking>>>,
        pub(in crate::demand) backing: Option<Arc<SourceWatchAllocationCustody>>,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

#[cfg(any(target_os = "linux", windows))]
    impl Drop for NativeSourceWatches {
        fn drop(&mut self) {
            drop(std::mem::take(&mut self.entries));
            if let Some(backing) = &self.backing { backing.retired(); }
        }
    }

#[cfg(any(target_os = "linux", windows))]
    pub(in crate::demand) struct NativeSourceRegistration {
        pub(in crate::demand) inner: Arc<NativeDemandInner>,
        pub(in crate::demand) index: Option<usize>,
        pub(in crate::demand) backing: Option<Arc<SourceWatchAllocationCustody>>,
    }

#[cfg(any(target_os = "linux", windows))]
    impl NativeSourceRegistration {
    pub(in crate::demand) fn prepare<W: SourceDemandReadWork>(
                inner: &Arc<NativeDemandInner>, work: &mut W,
            ) -> Result<Self, DemandError> {
                let mut watches = inner.watches.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                work.admit(WorkCounters { items_examined: watches.entries.len() as u64, ..WorkCounters::UNCHARGED })?;
                let mut visited = 0;
                let available = watches.entries.iter().position(|slot| {
                    visited += 1;
                    slot.as_ref().is_some_and(|watch| watch.strong_count() == 0)
                });
                work.record(WorkCounters { items_examined: visited, ..WorkCounters::UNCHARGED })?;
                if let Some(index) = available {
                    watches.entries[index] = None;
                    return Ok(Self { inner: Arc::clone(inner), index: Some(index), backing: watches.backing.clone() });
                }
                let index = watches.entries.len();
                if index < watches.entries.capacity() {
                    watches.entries.push(None);
                    return Ok(Self { inner: Arc::clone(inner), index: Some(index), backing: watches.backing.clone() });
                }
                let count = watches.entries.capacity().checked_add(1).ok_or(WorkError::Overflow)?;
                let copied = WorkCounters {
                    bytes_copied: (index as u64).checked_mul(std::mem::size_of::<Option<std::sync::Weak<NativeSourceWatchBacking>>>() as u64).ok_or(WorkError::Overflow)?,
                    ..WorkCounters::UNCHARGED
                };
                work.admit(copied)?;
                let ownership = SourcePayloadOwnership::new(work)?;
                let control_ownership = SourcePayloadOwnership::new(work)?;
                let control = std::mem::size_of::<SourceWatchAllocationCustody>() as u64;
                work.allocate(control)?;
                let backing = Arc::new(SourceWatchAllocationCustody::new(control_ownership));
                backing.ownership.retain(control, work)?;
                let (mut entries, bytes) = source_vec(count, work)?;
                ownership.retain(bytes, work)?;
                backing.publish(bytes);
                entries.extend(watches.entries.iter().cloned());
                entries.push(None);
                let retired = std::mem::replace(&mut *watches, NativeSourceWatches { entries, backing: Some(Arc::clone(&backing)), ownership });
                drop(watches);
                drop(retired);
                let registration = Self { inner: Arc::clone(inner), index: Some(index), backing: Some(backing) };
                work.record(copied)?;
                Ok(registration)
            }

    pub(in crate::demand) fn publish(mut self, watch: &Arc<NativeSourceWatchBacking>) -> Option<Arc<SourceWatchAllocationCustody>> {
                let retired = {
                    let mut watches = self.inner.watches.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                    std::mem::replace(&mut watches.entries[self.index.take().expect("unpublished source registration")], Some(Arc::downgrade(watch)))
                };
                drop(retired);
                self.backing.take()
            }
}

#[cfg(any(target_os = "linux", windows))]
    impl Drop for NativeSourceRegistration {
        fn drop(&mut self) {
            if let Some(index) = self.index {
                self.inner.watches.write().unwrap_or_else(std::sync::PoisonError::into_inner)
                    .entries[index] = Some(std::sync::Weak::new());
            }
        }
    }

impl NativeDemandSource {
    #[allow(unsafe_code, reason = "initializes preadmitted watch owners before infallible registration")]
            pub(in crate::demand) fn subscribe<W: SourceDemandReadWork>(
                &self, sink: Arc<dyn SourceChangeSink>, work: &mut W,
            ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
                work.check()?;
                if !self.inner.inline { return Ok(None); }
                #[cfg(test)]
                if !self.watchable { return Ok(None); }
                #[cfg(any(target_os = "linux", windows))]
                let registration = NativeSourceRegistration::prepare(&self.inner, work)?;
                let changes_ownership = SourcePayloadOwnership::new(work)?;
                let bytes = std::mem::size_of::<NativeChanges>() as u64;
                work.allocate(bytes)?;
                let changes = Arc::new(NativeChanges {
                    inner: Arc::downgrade(&self.inner), sink, ownership: changes_ownership,
                });
                changes.ownership.retain(bytes, work)?;
                let watch_ownership = SourcePayloadOwnership::new(work)?;
                let watch_bytes = std::mem::size_of::<NativeSourceWatchBacking>() as u64;
                work.allocate(watch_bytes)?;
                let mut watch = Arc::<NativeSourceWatchBacking>::new_uninit();
                watch_ownership.retain(watch_bytes, work)?;
                let ownership = SourcePayloadOwnership::new(work)?;
                let lease_bytes = std::mem::size_of::<NativeWatch>() as u64;
                work.allocate(lease_bytes)?;
                let mut lease = Box::<NativeWatch>::new_uninit();
                ownership.retain(lease_bytes, work)?;
                work.with_original_leaf(|work| work.reserve(crate::materializer::NativeResourceCredit::ReturnedItems, 1))?;
                let producer = match crate::source_watch::NativeSourceWatch::start_with_output_with_work(&self.inner.root, changes, work) {
                    Ok(producer) => producer,
                    Err(error) => {
                        let _cleanup = work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::ReturnedItems, 1));
                        return Err(error);
                    }
                };
                Arc::get_mut(&mut watch).expect("private uninitialized watch owner")
                    .write(NativeSourceWatchBacking { watch: producer, ownership: watch_ownership });
                // SAFETY: the unique Arc was initialized above; all further
                let watch = unsafe { watch.assume_init() };
                #[cfg(any(target_os = "linux", windows))]
                let backing = registration.publish(&watch);
                #[cfg(not(any(target_os = "linux", windows)))]
                let backing = None;
                lease.write(NativeWatch { watch, backing, ownership });
                // SAFETY: the preallocated result owner was initialized above.
                Ok(Some(unsafe { lease.assume_init() } as Box<dyn SourceWatch>))
            }
}

pub(in crate::demand) struct NativeSourceWatchBacking {
        pub(in crate::demand) watch: crate::source_watch::NativeSourceWatch,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

pub(in crate::demand) struct NativeWatch {
        pub(in crate::demand) watch: Arc<NativeSourceWatchBacking>,
        pub(in crate::demand) backing: Option<Arc<SourceWatchAllocationCustody>>,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

impl SourceWatch for NativeWatch {
        fn logical_owned_bytes(&self) -> u64 {
            std::mem::size_of::<Self>() as u64
                + std::mem::size_of::<NativeSourceWatchBacking>() as u64
                + self.watch.watch.logical_owned_bytes()
        }

        fn allocation_custody(&self) -> Option<Arc<SourceWatchAllocationCustody>> {
            self.backing.as_ref().map(Arc::clone)
        }

        fn fence(&self, accounting: &mut crate::kernel::KernelAccounting<'_>) -> Result<(), DemandError> {
            if let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = accounting {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                return self.watch.watch.fence_with_work(
                    Duration::from_secs(10), &mut NativeSourceWork::from_native_materialization_work(work),
                ).map(|_| ());
            }
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = {
                let (_, _, _, cancellation) = accounting.standalone_parts().ok_or(DemandError::NativeWorkUnavailable)?;
                let mut work = DemandWork::new(budget, cancellation);
                let result = self.watch.watch.fence_with_work(Duration::from_secs(10), &mut work).map(|_| ());
                work.finish(result)
            };
            accept_source_result(answer, accounting)
        }

        fn stop(&self, accounting: &mut crate::kernel::KernelAccounting<'_>) -> Result<(), DemandError> {
            if let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = accounting {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                return self.watch.watch.stop_with_work(&mut NativeSourceWork::from_native_materialization_work(work));
            }
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = {
                let (_, _, _, cancellation) = accounting.standalone_parts().ok_or(DemandError::NativeWorkUnavailable)?;
                let mut work = DemandWork::new(budget, cancellation);
                let result = self.watch.watch.stop_with_work(&mut work);
                work.finish(result)
            };
            accept_source_result(answer, accounting)
        }

        fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
            self.watch.watch.stop_with_cleanup_wait(wait)
        }

        fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
            self.watch.watch.stop_accepted_cleanup()
        }

        fn is_stopped(&self) -> bool { self.watch.watch.is_stopped() }

        fn is_exact(&self) -> bool {
            self.watch.watch.is_exact()
        }
    }

struct NativeWatch {
    watch: Arc<crate::source_watch::NativeSourceWatch>,
    _ownership: SourcePayloadOwnership,
}
impl SourceWatch for NativeWatch {
    fn fence(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let mut context = callback_work(accounting)?;
        let result = self
            .watch
            .fence_with_work(std::time::Duration::from_secs(10), &mut context)
            .map(|_| ());
        finish_context(result, context, accounting)
    }
    fn stop(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let mut context = callback_work(accounting)?;
        let result = self.watch.stop_with_work(&mut context);
        finish_context(result, context, accounting)
    }
    fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        self.watch.stop_with_cleanup_wait(wait)
    }
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        self.watch.stop_accepted_cleanup()
    }
    fn is_stopped(&self) -> bool {
        self.watch.is_stopped()
    }
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64 + self.watch.logical_owned_bytes()
    }
    fn is_exact(&self) -> bool {
        self.watch.is_exact()
    }
}

struct NativeChanges {
    inner: std::sync::Weak<NativeDemandInner>,
    sink: Arc<dyn SourceChangeSink>,
    excluded: Arc<ExcludedPaths>,
}
impl NativeDemandSource {
    fn subscribe(
        &self,
        sink: Arc<dyn SourceChangeSink>,
        context: &mut CallbackWork,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        context.check()?;
        if !self.inner.inline {
            return Ok(None);
        }
        #[cfg(test)]
        if !self.watchable {
            return Ok(None);
        }
        let ownership = SourcePayloadOwnership::new(context)?;
        let bytes = std::mem::size_of::<NativeWatch>() as u64
            + crate::source_watch::arc_control_bytes::<crate::source_watch::NativeSourceWatch>();
        let claim = context.claim(bytes)?;
        ownership.retain(claim, context)?;
        let watch = Arc::new(crate::source_watch::NativeSourceWatch::start_with_work(
            &self.inner.root,
            Arc::new(NativeChanges {
                inner: Arc::downgrade(&self.inner),
                excluded: self.excluded.clone(),
                sink,
            }),
            context,
        )?);
        #[cfg(any(target_os = "linux", windows))]
        {
            let mut watches = self
                .inner
                .watches
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            watches.retain(|watch| watch.strong_count() != 0);
            if watches.len() == watches.capacity() {
                let before = watches.capacity() as u64
                    * std::mem::size_of::<std::sync::Weak<crate::source_watch::NativeSourceWatch>>()
                        as u64;
                let mut allocation = context.claim(std::mem::size_of::<
                    std::sync::Weak<crate::source_watch::NativeSourceWatch>,
                >() as u64)?;
                let reserve = watches
                    .try_reserve_exact(1)
                    .map_err(DemandError::Allocation);
                let actual = watches.capacity() as u64
                    * std::mem::size_of::<std::sync::Weak<crate::source_watch::NativeSourceWatch>>()
                        as u64
                    - before;
                context.reconcile_allocation(&mut allocation, actual)?;
                self.inner.ownership.retain(allocation, context)?;
                reserve?;
            }
            watches.push(Arc::downgrade(&watch));
        }
        Ok(Some(Box::new(NativeWatch {
            watch,
            _ownership: ownership,
        })))
    }
}

impl NativeDemandInner {
    fn name<W: SourceDemandReadWork>(
        &self,
        relative: &Path,
        work: &mut W,
    ) -> Result<(NamespacePath, bool, u64, Option<crate::NativeAllocation>), DemandError> {
        let (mut names, mut claimed) = crate::native_host::buffer::<crate::kernel::LogicalName, _>(
            relative.components().count(),
            work,
        )?;
        let mut native: Option<crate::NativeAllocation> = None;
        let result = (|| {
            for component in relative.components() {
                work.check()?;
                let std::path::Component::Normal(name) = component else {
                    break;
                };
                let ((encoding, bytes), mut guard) =
                    match crate::native_name::host_name_bytes_with_source_work(
                        name,
                        self.profile,
                        self.limits.maximum_component_bytes,
                        work,
                    ) {
                        Ok(name) => name,
                        Err(DemandError::InvalidRequest) => break,
                        Err(error) => return Err(error),
                    };
                let ordinary = if guard.is_none() {
                    bytes.capacity() as u64
                } else {
                    0
                };
                let name = match crate::kernel::LogicalName::new(
                    encoding,
                    bytes,
                    self.limits.maximum_component_bytes,
                ) {
                    Ok(name) => name,
                    Err(_) => {
                        drop(guard);
                        work.release(ordinary)?;
                        break;
                    }
                };
                if NamespacePath::validate_component_lengths(
                    names
                        .iter()
                        .map(|name| name.as_bytes().len())
                        .chain([name.as_bytes().len()]),
                    self.limits,
                )
                .is_err()
                {
                    drop(name);
                    drop(guard);
                    work.release(ordinary)?;
                    break;
                }
                claimed += ordinary;
                names.push(name);
                if let Some(mut guard) = guard.take() {
                    if let Some(native) = &mut native {
                        native
                            .absorb(&mut guard)
                            .map_err(crate::native_name::original_name_error)?;
                    } else {
                        native = Some(guard);
                    }
                }
            }
            let exact = names.len() == relative.components().count();
            Ok((
                NamespacePath::new(std::mem::take(&mut names), self.limits)?,
                exact,
            ))
        })();
        match result {
            Ok((path, exact)) => Ok((path, exact, claimed, native)),
            Err(error) => {
                drop(names);
                drop(native);
                work.release(claimed)?;
                Err(error)
            }
        }
    }
}
impl NativeChanges {
    fn deliver<W: SourceDemandReadWork>(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut W,
    ) -> Result<(), DemandError> {
        use crate::source_watch::HostChange;
        work.check()?;
        let Some(inner) = self.inner.upgrade() else {
            return Ok(());
        };
        let maximum = changes
            .len()
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)?;
        let (mut reported, mut claimed) =
            crate::native_host::buffer::<SourceChange, _>(maximum, work)?;
        let mut native: Option<crate::NativeAllocation> = None;
        let result = (|| {
            for change in changes {
                work.check()?;
                match change {
                    HostChange::Everything => reported.push(SourceChange::Everything),
                    #[cfg(any(target_os = "macos", windows))]
                    HostChange::Unconfirmed => reported.push(SourceChange::Unconfirmed),
                    #[cfg(windows)]
                    HostChange::File(index) => reported.push(SourceChange::Node(
                        windows_file_identity(inner.root.identity().device, *index),
                    )),
                    HostChange::Rebound(relative) | HostChange::Altered(relative) => {
                        let (name, exact, allocation, mut guard) = inner.name(relative, work)?;
                        claimed += allocation;
                        if let Some(mut incoming) = guard.take() {
                            if let Some(native) = &mut native {
                                native
                                    .absorb(&mut incoming)
                                    .map_err(crate::native_name::original_name_error)?;
                            } else {
                                native = Some(incoming);
                            }
                        }
                        let metadata = { #[cfg(unix)] { inner.root.stat(relative, work) } #[cfg(windows)] { crate::native_host::with_wide_path(relative, work, |path, work| inner.root.stat(path, work)) } };
                        let node = match metadata {
                            Ok(metadata) => Some(file_identity(&metadata)),
                            Err(DemandError::Io(error))
                                if matches!(
                                    error.kind(),
                                    std::io::ErrorKind::NotFound
                                        | std::io::ErrorKind::NotADirectory
                                ) =>
                            {
                                None
                            }
                            Err(error) => {
                                drop(name);
                                return Err(error);
                            }
                        };
                        let altered = matches!(change, HostChange::Altered(_))
                            && exact
                            && (node.is_some() || name.is_root());
                        reported.push(if altered {
                            SourceChange::Entry(name)
                        } else if name.is_root() {
                            SourceChange::Everything
                        } else {
                            SourceChange::Name(name)
                        });
                        reported.extend(node.map(SourceChange::Node));
                    }
                }
            }
            for batch in reported
                .chunk_by(|a, b| self.excluded.visible(a) == self.excluded.visible(b))
                .filter(|batch| self.excluded.visible(&batch[0]))
            {
                account_source(work, |accounting, _| {
                    self.sink.source_changed(batch, accounting)
                })?;
            }
            Ok(())
        })();
        drop(reported);
        drop(native);
        work.release(claimed)?;
        result
    }
    #[cfg(windows)]
    fn deliver_wide<W: SourceDemandReadWork>(
        &self,
        changes: &[crate::source_watch::HostChangeWide<'_>],
        work: &mut W,
    ) -> Result<(), DemandError> {
        use crate::source_watch::{HostChange, HostChangeWide};
        let (mut owned, mut claimed) =
            crate::native_host::buffer::<HostChange, _>(changes.len(), work)?;
        let result = (|| {
            for change in changes {
                owned.push(match change {
                    HostChangeWide::Everything => HostChange::Everything,
                    HostChangeWide::Unconfirmed => HostChange::Unconfirmed,
                    HostChangeWide::File(index) => HostChange::File(*index),
                    HostChangeWide::Path { action, path } => {
                        let mut allocation = work.claim(path.len() as u64 * 2)?;
                        let name = std::ffi::OsString::from_wide(path);
                        work.reconcile_allocation(&mut allocation, name.capacity() as u64)?;
                        claimed += allocation;
                        if matches!(action, 1 | 2 | 4 | 5) {
                            HostChange::Rebound(name.into())
                        } else {
                            HostChange::Altered(name.into())
                        }
                    }
                });
            }
            self.deliver(&owned, work)
        })();
        drop(owned);
        work.release(claimed)?;
        result
    }
}
impl crate::source_watch::HostChangeSink for NativeChanges {
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64 + self.sink.logical_owned_bytes()
    }
    fn host_changed(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        self.deliver(changes, work)
    }
    fn host_changed_with_native_work(
        &self,
        changes: &[crate::source_watch::HostChange],
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        self.deliver(changes, work)
    }
    #[cfg(windows)]
    fn host_changed_wide(
        &self,
        changes: &[crate::source_watch::HostChangeWide<'_>],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        self.deliver_wide(changes, work)
    }
    #[cfg(windows)]
    fn host_changed_wide_with_native_work(
        &self,
        changes: &[crate::source_watch::HostChangeWide<'_>],
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        self.deliver_wide(changes, work)
    }
}

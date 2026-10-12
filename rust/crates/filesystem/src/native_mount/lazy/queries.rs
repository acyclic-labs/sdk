use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) fn reported(&self, file_id: FileId) -> bool {
        !self
            .linked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values
            .binary_search(&file_id).is_ok()
    }

pub(in crate::native_mount::lazy) fn reported_with_meter(&self, file_id: FileId, work: &crate::NativeMaterializationWork) -> Result<bool, MountSourceError> {
        let mut work = work.native_read_meter().lock();
        work.check().map_err(mount_demand_error)?;
        self.authored.cancellation().check().map_err(|_| MountSourceError::Stale)?;
        let linked = self.linked.lock().map_err(|_| MountSourceError::Stale)?;
        let mut low = 0;
        let mut high = linked.values.len();
        while low < high {
            mount_items(&mut work, 1)?;
            let middle = low + (high - low) / 2;
            match linked.values[middle].cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(false),
            }
        }
        Ok(true)
    }

pub(in crate::native_mount::lazy) fn source_observed(&self) -> bool {
        self.source_watch
            .as_ref()
            .is_some_and(|watch| watch.is_exact())
    }

pub(in crate::native_mount::lazy) fn ledger_stamp(&self) -> Option<ViewStamp> {
        self.source_view
            .is_stable()
            .then(|| self.authored.view_stamp())
            .flatten()
    }

pub(in crate::native_mount::lazy) fn detached_by_id(
        &self,
        file_id: FileId,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        let published = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication
            .filter(|custody| custody.published).map(|custody| custody.operation);
        let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(detached_entry(&detached, file_id)
            .filter(|entry| entry.ready || (published.is_some()
                && entry.pending.as_ref().is_some_and(|pending| pending.operation == published)))
            .map(|entry| Arc::clone(&entry.file)))
    }

pub(in crate::native_mount::lazy) fn detached_by_id_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError> {
        work.check().map_err(mount_demand_error)?;
        let published = self.removals.lock().map_err(|_| MountSourceError::Stale)?.publication
            .filter(|custody| custody.published).map(|custody| custody.operation);
        let detached = self.detached.lock().map_err(|_| MountSourceError::Stale)?;
        let mut low = 0;
        let mut high = detached.len();
        while low < high {
            mount_items(work, 1)?;
            let middle = low + (high - low) / 2;
            match detached[middle].0.cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    let entry = &detached[middle].1;
                    return Ok((entry.ready || (published.is_some()
                        && entry.pending.as_ref().is_some_and(|pending| pending.operation == published)))
                        .then(|| Arc::clone(&entry.file)));
                }
            }
        }
        Ok(None)
    }

pub(in crate::native_mount::lazy) async fn detached_identity(
        &self,
        named: MountLookup,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<MountLookup>, MountSourceError> {
        let Some(file) = carrier.with_work(|work| self.detached_by_id_with_work(named.node.file_id, work))? else {
            return Ok(None);
        };
        let caller = carrier.cancellation();
        let (record, metadata, _) = file.snapshot(carrier, &caller).await?;
        let mut current = mount_lookup(LazyLookup::Shadow {
            record, metadata: WorkspaceMetadata::from_engine(metadata), source_link_count: named.node.link_count,
        }, named.node.file_id);
        current.node.link_count = named.node.link_count;
        Ok(Some(current))
    }

pub(in crate::native_mount::lazy) fn remember_linked_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), MountSourceError> {
        let mut linked = self.linked.lock().map_err(|_| MountSourceError::Stale)?;
        let mut low = 0;
        let mut high = linked.values.len();
        while low < high {
            mount_items(work, 1)?;
            let middle = low + (high - low) / 2;
            match linked.values[middle].cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(()),
            }
        }
        let width = std::mem::size_of::<FileId>();
        let shifted = linked.values.len() - low;
        if linked.values.len() < linked.values.capacity() {
            let copied = crate::WorkCounters {
                bytes_copied: ((shifted + 1) * width) as u64, ..crate::WorkCounters::default()
            };
            work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
            linked.values.insert(low, file_id);
            return work.record(copied).map_err(mount_demand_error);
        }
        let capacity = linked.values.len().checked_add(1)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
        let bytes = capacity.checked_mul(width)
            .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
        let copied = crate::WorkCounters {
            bytes_copied: linked.values.len().checked_add(shifted).and_then(|value| value.checked_add(1))
                .and_then(|value| value.checked_mul(width))
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64,
            ..crate::WorkCounters::default()
        };
        work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
        let custody = crate::native_mount::NativeMountAllocationCustody::new(work).map_err(mount_demand_error)?;
        custody.claim(bytes, work).map_err(mount_demand_error)?;
        let mut values = Vec::new();
        if let Err(error) = values.try_reserve_exact(capacity) {
            drop(values);
            custody.retire(bytes).map_err(MountSourceError::Work)?;
            return Err(MountSourceError::Allocation(error));
        }
        values.extend_from_slice(&linked.values);
        values.insert(low, file_id);
        if let Err(error) = work.record(copied) {
            drop(values);
            custody.retire(bytes).map_err(MountSourceError::Work)?;
            return Err(mount_demand_error(error));
        }
        let old = std::mem::replace(&mut linked.values, values);
        let old_bytes = (old.capacity() * width) as u64;
        let old_custody = linked.custody.replace(custody);
        drop(old);
        if let Some(custody) = old_custody {
            custody.retire(old_bytes).map_err(MountSourceError::Work)?;
        }
        Ok(())
    }

pub(in crate::native_mount::lazy) fn has_open_source_handle_with_work(
        &self,
        file_id: FileId,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        mount_items(work, 1)?;
        let open = self.open_sources.lock().map_err(|_| MountSourceError::Stale)?;
        let Some(handles) = open.get(&file_id) else { return Ok(false); };
        for handle in handles {
            mount_items(work, 1)?;
            if handle.strong_count() != 0 { return Ok(true); }
        }
        // Pruning expired handles is optional cache maintenance, not removal.
        Ok(false)
    }

pub(in crate::native_mount::lazy) fn detached_for_path(
        &self,
        path: &MountPath,
    ) -> Result<Option<Arc<CheckoutDetachedFile<A, O>>>, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if self
            .detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .is_empty()
        {
            return Ok(None);
        }
        if let Some(authored) = self.authored.lookup(path)? {
            return self.detached_by_id(authored.node.file_id);
        }
        let text = self.path(path)?;
        if self.is_removed(&text)? {
            return Err(MountSourceError::NotFound);
        }
        let file_id = self.wait(|| async {
            let lookup = self.lazy.lookup(&text).await.map_err(lazy_error)?;
            let mut accounting = crate::lazy_workspace::LazyReadonlyAccounting::Portable {
                work: crate::WorkCounters::UNCHARGED, budget: crate::WorkBudget::UNBOUNDED,
                cancellation: &crate::CancellationToken::new(),
            };
            let result = self.lazy.stable_file_id_for_lookup(&text, &lookup, &mut accounting).await;
            accounting.finish_measured(result).map(|receipt| receipt.value).map_err(lazy_error)
        })?;
        self.detached_by_id(file_id)
    }

pub(in crate::native_mount::lazy) fn path(&self, path: &MountPath) -> Result<String, MountSourceError> {
        let mut work = crate::demand::DemandWork::new(crate::WorkBudget::UNBOUNDED, self.authored.cancellation());
        mount_path_text(&self.root, path, &mut work)
    }

pub(in crate::native_mount::lazy) fn removed_by(paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, path: &str) -> bool {
        let mut current = path;
        loop {
            if paths.contains_key(current) {
                return true;
            }
            if current == "/" {
                return false;
            }
            current =
                current.rsplit_once('/').map_or(
                    "/",
                    |(parent, _)| if parent.is_empty() { "/" } else { parent },
                );
        }
    }

pub(in crate::native_mount::lazy) fn is_removed(&self, path: &str) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(Self::removed_by(&removals.paths, path))
    }

pub(in crate::native_mount::lazy) fn removed_by_with_work(
        paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, path: &str,
        work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        Self::removed_by_with_items(paths, path, || mount_items(work, 1))
    }

pub(in crate::native_mount::lazy) fn removed_by_with_items(
        paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, path: &str,
        mut visit: impl FnMut() -> Result<(), MountSourceError>,
    ) -> Result<bool, MountSourceError> {
        let mut current = path;
        loop {
            visit()?;
            if paths.contains_key(current) { return Ok(true); }
            if current == "/" { return Ok(false); }
            current = current.rsplit_once('/').map_or("/", |(parent, _)|
                if parent.is_empty() { "/" } else { parent });
        }
    }

pub(in crate::native_mount::lazy) fn is_removed_with_work(
        &self, path: &str, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Self::removed_by_with_work(&removals.paths, path, work)
    }

pub(in crate::native_mount::lazy) fn removed_identity_with_work(
        &self, path: &str, file_id: FileId, work: &mut crate::demand::DemandWork,
    ) -> Result<bool, MountSourceError> {
        mount_items(work, 1)?;
        self.removed_identity(path, file_id)
    }

pub(in crate::native_mount::lazy) fn removed_identity(
        &self, path: &str, file_id: FileId,
    ) -> Result<bool, MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        Ok(removals.paths.get(path).is_some_and(|removed| removed.file_id == file_id))
    }

pub(in crate::native_mount::lazy) fn removed_identity_with_carrier(
        &self, path: &str, file_id: FileId,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<bool, MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            original.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }).map_err(MountSourceError::NativeWork)?;
            self.removed_identity(path, file_id)
        } else {
            carrier.with_work(|work| self.removed_identity_with_work(path, file_id, work))
        }
    }

pub(in crate::native_mount::lazy) fn removal_snapshot_with_work(
        &self, directory: &str, work: &mut crate::demand::DemandWork,
    ) -> Result<(Vec<String>, bool, u64), MountSourceError> {
        let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        let opaque = Self::removed_by_with_work(&removals.paths, directory, work)?;
        let mut scratch = crate::native_mount::adapter::CaptureScratchAllocation::new(work);
        let children = Self::removal_children_with_scratch(&removals.paths, directory, &mut scratch)?;
        scratch.retain();
        Ok((children, opaque, removals.epoch))
    }

pub(in crate::native_mount::lazy) fn removal_snapshot_with_carrier(
        &self, directory: &str, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(MountOwnedBuffer<Vec<String>>, bool, u64), MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            let removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
            let opaque = Self::removed_by_with_items(&removals.paths, directory, || original.charge(
                crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED },
            ).map_err(MountSourceError::NativeWork))?;
            let mut scratch = crate::native_mount::adapter::NativeCaptureScratchAllocation::new(&original)?;
            let children = Self::removal_children_with_scratch(&removals.paths, directory, &mut scratch)?;
            Ok((MountOwnedBuffer { value: children, legacy_bytes: 0,
                allocation: Some(scratch.retain()) }, opaque, removals.epoch))
        } else {
            let (children, opaque, epoch) = carrier.with_work(|work|
                self.removal_snapshot_with_work(directory, work))?;
            let bytes = directory_removal_bytes(&children)?;
            Ok((MountOwnedBuffer { value: children, legacy_bytes: bytes, allocation: None }, opaque, epoch))
        }
    }

pub(in crate::native_mount::lazy) fn removal_children_with_scratch<C: crate::native_mount::adapter::CaptureInputScratch>(
        paths: &BTreeMap<MountedRemovalPath, MountedRemoval>, directory: &str, scratch: &mut C,
    ) -> Result<Vec<String>, MountSourceError> {
        let prefix: std::borrow::Cow<'_, str> = if directory == "/" {
            std::borrow::Cow::Borrowed("/")
        } else {
            let length = directory.len().checked_add(1)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            let copied = crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED };
            scratch.admit(copied)?;
            let mut bytes = scratch.vec::<u8>(length)?;
            bytes.extend_from_slice(directory.as_bytes());
            bytes.push(b'/');
            scratch.record(copied)?;
            std::borrow::Cow::Owned(String::from_utf8(bytes).expect("Unicode removal prefix"))
        };
        let range = || paths.range::<str, _>((
            std::ops::Bound::Included(prefix.as_ref()), std::ops::Bound::Unbounded,
        ));
        let mut count = 0_usize;
        for (path, _) in range() {
            scratch.items(1)?;
            if !path.starts_with(prefix.as_ref()) { break; }
            if !path[prefix.len()..].contains('/') {
                count = count.checked_add(1).ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            }
        }
        let mut children = scratch.vec::<String>(count)?;
        for (path, _) in range() {
            scratch.items(1)?;
            if !path.starts_with(prefix.as_ref()) { break; }
            if path[prefix.len()..].contains('/') { continue; }
            let copied = crate::WorkCounters { bytes_copied: path.len() as u64, ..crate::WorkCounters::UNCHARGED };
            scratch.admit(copied)?;
            let mut bytes = scratch.vec::<u8>(path.len())?;
            bytes.extend_from_slice(path.as_bytes());
            children.push(String::from_utf8(bytes).expect("Unicode removal path"));
            scratch.record(copied)?;
        }
        let prefix_bytes = match &prefix { std::borrow::Cow::Borrowed(_) => 0,
            std::borrow::Cow::Owned(prefix) => prefix.capacity() as u64 };
        drop(prefix);
        scratch.release(prefix_bytes)?;
        Ok(children)
    }

pub(in crate::native_mount::lazy) async fn lookup_pinned_in_view(
        &self,
        path: &MountPath,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Option<(MountLookup, Option<MountContentPin>)>, MountSourceError> {
        let owner = SourceViewGate::callback_owner();
        let _lease = self.source_read_lease_with_carrier(owner, None, carrier).await?;
        let text = mount_path_text_with_carrier(&self.root, path, carrier)?;
        let result = async {
            let resolution = if carrier.original_native_work().is_none()
                && carrier.with_work(|work| work.budget == crate::WorkBudget::UNBOUNDED)
            {
                self.resolve_cached(path, &text, owner, carrier).await?
            } else { self.resolve(path, &text, owner, carrier).await? };
            match resolution {
                Resolution::Authored(lookup) =>
                    Ok(Some((self.project_authored(lookup, carrier).await?, None))),
                Resolution::Absent => Ok(None),
                Resolution::Unauthored(lookup, source, allocation) =>
                    self.project_unauthored(&text, lookup, source, allocation, carrier).await.map(Some),
            }
        }.await;
        let retained = text.legacy_owned_bytes();
        drop(text);
        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
        result
    }

pub(in crate::native_mount::lazy) async fn resolve(
        &self,
        path: &MountPath,
        text: &str,
        owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Resolution, MountSourceError> {
        carrier.with_work(|work| work.check()).map_err(mount_demand_error)?;
        if let Some(lookup) = self.authored.lookup_async(path, owner, carrier).await? {
            if self.removed_identity_with_carrier(text, lookup.node.file_id, carrier)? {
                return Ok(Resolution::Absent);
            }
            return Ok(Resolution::Authored(lookup));
        }
        if carrier.with_work(|work| self.is_removed_with_work(text, work))? {
            return Ok(Resolution::Absent);
        }
        if carrier.with_work(|work| work.budget == crate::WorkBudget::UNBOUNDED)
            && path.parent().as_ref().is_some_and(|parent| self.source_lacks(parent))
        {
            return Ok(Resolution::Absent);
        }
        let caller = carrier.cancellation();
        if let Some(original) = carrier.original_native_work() {
            match self.lazy.inspect_unauthored(text, InspectBasis::default(),
                &LazyTreapWork::native_with_work(&original, &caller).map_err(lazy_error)?).await {
                Ok(LazyReadonlyNode::Native(owned)) => {
                    let ((lookup, source), allocation) = owned.into_parts();
                    Ok(Resolution::Unauthored(lookup, source, Some(allocation)))
                }
                Ok(_) => unreachable!("native inspection retains native funding"),
                Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::NotFound) =>
                    Ok(Resolution::Absent),
                Err(error) => Err(lazy_error(error)),
            }
        } else {
            let budget = carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
            let accounting = LazyTreapWork::new(budget, &caller);
            let result = self.lazy.inspect_unauthored(text, InspectBasis::default(), &accounting).await;
            let result = accounting.measured(result, |error, work| error.with_prior_work(work).into_measured_failure());
            match carrier.with_work(|work| mount_accept(work, result, lazy_error)) {
                Ok(LazyReadonlyNode::Portable((lookup, source))) => Ok(Resolution::Unauthored(lookup, source, None)),
                Ok(_) => unreachable!("ordinary inspection uses ordinary funding"),
                Err(MountSourceError::NotFound) => Ok(Resolution::Absent),
                Err(error) => Err(error),
            }
        }
    }

pub(in crate::native_mount::lazy) async fn resolve_cached(
        &self,
        path: &MountPath,
        text: &str,
        owner: crate::native_mount::view_gate::CallbackOwner,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<Resolution, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        in_heap(move || async move {
            if let Some(deferral) = self.resolutions.get(path) {
                let file_id = deferral.node.map(|node| self.lazy.source_file_id(&node.file_identity,
                    &mut crate::kernel::allocation::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))).transpose().map_err(lazy_error)?;
                if deferral.source == self.lazy.source_reference()
                    && self.unchanged_since(path, file_id, deferral.stamp)
                {
                    if self.source_observed() {
                        return Ok(deferral.resolution());
                    }
                    match self.lazy.source_lookup(deferral.source, text).await {
                        Ok(node) if node == deferral.node => return Ok(deferral.resolution()),
                        // The source changed, or its view was invalidated.
                        Ok(_) => {}
                        Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::StaleSource) => {}
                        Err(error) => return Err(lazy_error(error)),
                    }
                }
            }
            let stamp = self.ledger_stamp();
            let source = self.lazy.source_reference();
            let resolution = self.resolve(path, text, owner, carrier).await?;
            if let Some(stamp) = stamp {
                match &resolution {
                    Resolution::Unauthored(LazyLookup::Source(node), Some(source), _) => {
                        self.resolutions.remember(path, Deferral {
                            stamp, source: *source, node: Some(*node),
                        });
                    }
                    Resolution::Absent if !self.is_removed(text)? => {
                        self.resolutions.remember(path, Deferral { stamp, source, node: None });
                        if let Some(parent) = path.parent() {
                            self.observe_source_absence(&parent, stamp, source).await?;
                        }
                    }
                    _ => {}
                }
            }
            Ok(resolution)
        })
        .await
    }

pub(in crate::native_mount::lazy) fn source_lacks(&self, directory: &MountPath) -> bool {
        self.source_observed()
            && self
                .source_absent
                .get(directory)
                .is_some_and(|(stamp, source)| {
                    source == self.lazy.source_reference()
                        && self.source_view.is_stable()
                        && self.authored.binding_unchanged_since(directory, stamp)
                })
    }

pub(in crate::native_mount::lazy) async fn observe_source_absence(
        &self,
        directory: &MountPath,
        stamp: ViewStamp,
        source: SourceReference,
    ) -> Result<(), MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if directory.components().is_empty()
            || !self.source_observed()
            || self.source_absent.get(directory).is_some()
        {
            return Ok(());
        }
        let text = self.path(directory)?;
        match self.lazy.source_lookup(source, &text).await {
            Ok(None) => self.source_absent.remember(directory, stamp, source),
            Ok(Some(_)) => {}
            Err(error) if matches!(error.semantic_error(), LazyWorkspaceError::StaleSource) => {}
            Err(error) => return Err(lazy_error(error)),
        }
        Ok(())
    }

pub(in crate::native_mount::lazy) async fn project_authored(
        &self,
        lookup: MountLookup,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountLookup, MountSourceError> {
        Ok(self.detached_identity(lookup, carrier).await?.unwrap_or(lookup))
    }

pub(in crate::native_mount::lazy) async fn project_unauthored(
        &self,
        path: &str,
        lookup: LazyLookup,
        source: Option<SourceReference>,
        allocation: Option<crate::NativeAllocation>,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(MountLookup, Option<MountContentPin>), MountSourceError> {
        let lookup = MountOwnedBuffer { legacy_bytes: lookup.logical_owned_bytes(), value: lookup, allocation };
        let retained = lookup.legacy_owned_bytes();
        if let Err(error) = carrier.with_work(|work| work.record_adoption(retained, &None)) {
            drop(lookup);
            carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
            return Err(mount_demand_error(error));
        }
        let result = async {
        let file_id = self.mutation_file_id_with_carrier(path, &lookup.value, carrier).await?;
        if let LazyLookup::Source(node) = &lookup.value
            && linked(node)
        {
            carrier.with_work(|work| self.remember_linked_with_work(file_id, work))?;
        }
        let pin_source = match (&lookup.value, source) {
            (LazyLookup::Source(node), Some(source)) if node.kind == SourceNodeKind::RegularFile =>
                Some((source, *node)),
            _ => None,
        };
        let projected = mount_lookup_ref(&lookup.value, file_id);
        if let Some(current) = self.detached_identity(projected, carrier).await? {
            return Ok((current, None));
        }
        let pin = carrier.with_work(|work| pin_source
            .map(|(source, node)| source_content_pin_with_work(source, &node, work)).transpose())?;
        Ok((projected, pin))
        }.await;
        drop(lookup);
        carrier.with_work(|work| work.release(retained)).map_err(mount_demand_error)?;
        result
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Records that another handle on the workspace changed each of
    pub(crate) fn changed_outside(&self, paths: &[String]) -> Result<(), MountSourceError> {
        let mut effect = ViewEffect::default();
        for path in paths {
            effect
                .subtrees
                .push(self.lazy.namespace_path(path).map_err(lazy_error)?);
        }
        if !effect.is_empty() {
            self.cursors.clear();
            self.authored
                .record_projection_change(&ViewChange::Effect(&effect));
        }
        Ok(())
    }

pub(in crate::native_mount::lazy) fn is_source_projection(&self, path: &MountPath) -> Result<bool, MountSourceError> {
        let text = self.path(path)?;
        let owner = SourceViewGate::callback_owner();
        self.wait(|| async move {
            if self.authored.lookup_async(path, owner, &mut crate::native_mount::adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(crate::performance::BOUNDARY_BUDGET, self.authored.cancellation()))).await?.is_some() {
                return Ok(false);
            }
            if self.is_removed(&text)? {
                return Err(MountSourceError::NotFound);
            }
            match self.lazy.inspect_unauthored(&text, InspectBasis::default(),
                &LazyTreapWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())).await {
                Ok(LazyReadonlyNode::Portable((LazyLookup::Source(_), _))) => Ok(true),
                Ok(_) => Err(MountSourceError::Unsupported(
                    "named attributes of an unpromoted shadow are not projected".to_owned(),
                )),
                Err(error) => Err(lazy_error(error)),
            }
        })
    }
}

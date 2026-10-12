use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(in crate::native_mount::lazy) fn directory_page(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        with_native_callback_carrier(&self.runtime, carrier, |carrier, prepared| {
            self.runtime.wait_prepared(prepared, || {
                self.directory_page_async(path, cursor, maximum_entries, carrier)
            })
        })
    }

pub(in crate::native_mount::lazy) async fn directory_page_async(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let mut scratch = DirectoryCarrierScratch::new(carrier);
        let text = mount_path_text_with_carrier(&self.root, path, scratch.carrier)?;
        scratch.track(text.legacy_owned_bytes())?;
        let owner = SourceViewGate::callback_owner();
        let lease = self.source_read_lease_with_carrier(owner, None, scratch.carrier).await?;
        let (removed_children, opaque, removal_epoch) =
            scratch.carrier.with_work(|work| self.removal_snapshot_with_work(&text, work))?;
        scratch.track(directory_removal_bytes(&removed_children)?)?;
        if opaque && self.authored.lookup_async(path, owner, scratch.carrier).await?.is_none() {
            return Err(MountSourceError::NotFound);
        }
        let generation = (lease.generation, removal_epoch);
        let held = self.cursors.take(generation, cursor, scratch.carrier)?;
        let (stamp, mut cursor) = match held {
            Some((stamp, cursor)) => (stamp, Some(cursor)),
            None => (ViewStamp::current(), None),
        };
        if let Some(cursor) = &mut cursor { cursor.mark_input_borrowed(); }
        if !self.authored.unchanged_since_with_carrier(path, None, stamp, scratch.carrier)? {
            return Err(MountSourceError::Stale);
        }
        let caller = scratch.carrier.cancellation();
        let original = scratch.carrier.original_native_work();
        let (mut page, state) = {
            let observation = self.authored.shared_checkout().observe(
                owner, scratch.carrier, &caller, Some(self.authored.cancellation()),
            ).await?;
            observation.ensure_publication_resolved()?;
            if let Some(original) = &original {
                let state = self.lazy.state(&LazyTreapWork::native_with_work(original, &caller).map_err(lazy_error)?).await
                    .map_err(lazy_error)?;
                let mut checkout = observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .expect("unbounded observer preserves a validated checkout").value;
                let page = self.lazy.list_directory_in_state_with_native_work(
                    state.as_state(), &text, cursor, maximum_entries, original, &caller,
                    Some(&mut checkout), Some((&removed_children, opaque)), None,
                ).await.map_err(lazy_error)?;
                let LazyReadonlyNode::Native(state) = state else { unreachable!("native state retains native funding") };
                let (state, allocation) = state.into_parts();
                (MountOwnedBuffer { value: page.page, legacy_bytes: 0, allocation: Some(page.allocation) },
                    MountOwnedBuffer { value: state, legacy_bytes: 0, allocation: Some(allocation) })
            } else {
                let mut checkout = observation.observer(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                    .expect("unbounded observer preserves a validated checkout").value;
                let budget = scratch.carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?;
                let result = self.lazy.list_directory_in_checkout(
                    &mut checkout, &text, cursor, maximum_entries, Some((&removed_children, opaque)), budget, &caller,
                ).await;
                let (page, state) = scratch.carrier.with_work(|work| mount_accept(work, result, lazy_error))?;
                let page_bytes = crate::lazy_workspace::retained_directory_page_bytes(
                    &page.entries, page.entries.iter().map(|entry| entry.name.retained_bytes()),
                ).map_err(lazy_error)?.checked_add(page.next.as_ref()
                    .map(LazyDirectoryCursor::retained_bytes).transpose().map_err(lazy_error)?.unwrap_or(0))
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                let state_bytes = std::mem::size_of_val(&state) as u64;
                let bytes = page_bytes.checked_add(state_bytes)
                    .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
                scratch.track(bytes)?;
                scratch.carrier.with_work(|work| work.record_adoption(bytes, &None))
                    .map_err(mount_demand_error)?;
                (MountOwnedBuffer { value: page, legacy_bytes: page_bytes, allocation: None },
                    MountOwnedBuffer { value: state, legacy_bytes: state_bytes, allocation: None })
            }
        };
        let mut entries = scratch.vec::<MountDirectoryEntry>(page.value.entries.len())?;
        let output_storage = if scratch.native_backing.is_some() { 0 } else {
            (entries.capacity() * std::mem::size_of::<MountDirectoryEntry>()) as u64
        };
        let mut output_names = 0_u64;
        for entry in &page.value.entries {
            scratch.carrier.with_work(|work| mount_items(work, 1))?;
            let name = mount_name_with_carrier(&entry.name, scratch.carrier)?;
            let (name, mut allocation, name_bytes) = name.into_parts();
            if let Some(allocation) = &mut allocation { scratch.absorb_native(allocation)?; }
            scratch.track(name_bytes)?;
            let (mounted_child, mounted_bytes) = scratch.carrier.with_work(|work| mount_components_with_work(path.components(), Some(&name), work))?;
            scratch.track(mounted_bytes)?;
            let child = mount_path_text_with_carrier(&self.root, &mounted_child, scratch.carrier)?;
            scratch.track(child.legacy_owned_bytes())?;
            let (lookup, pin) = if let Some(node) = entry.source {
                let (lookup, source, allocation) = inspect(&self.lazy, &child,
                    InspectBasis { state: Some(&state.value), listed: Some(node), unobserved: entry.unobserved, ..InspectBasis::default() },
                    scratch.carrier,
                ).await?;
                if original.is_none() && self.source_view.is_stable()
                    && let (LazyLookup::Source(node), Some(source)) = (&lookup, source)
                {
                    remember(&self.resolutions, &mounted_child, Deferral { stamp, source, node: Some(*node) });
                }
                self.project_unauthored(&child, lookup, source, allocation, scratch.carrier).await?
            } else if let Some(authored) = self.authored.lookup_async(
                &mounted_child, owner, scratch.carrier,
            ).await? {
                let removed = self.removed_identity(&child, authored.node.file_id, scratch.carrier)?;
                if removed {
                    let name_capacity = name.capacity() as u64;
                    let child_bytes = child.legacy_owned_bytes();
                    drop(name); drop(child); drop(mounted_child);
                    if scratch.native_backing.is_some() { scratch.release_backing(name_capacity)?; }
                    scratch.release(name_bytes + child_bytes + mounted_bytes)?;
                    continue;
                }
                (self.project_authored(authored, scratch.carrier).await?, None)
            } else {
                let (lookup, source, allocation) = inspect(&self.lazy, &child,
                    InspectBasis { state: Some(&state.value), ..InspectBasis::default() }, scratch.carrier,
                ).await?;
                self.project_unauthored(&child, lookup, source, allocation, scratch.carrier).await?
            };
            let child_bytes = child.legacy_owned_bytes();
            drop(child); drop(mounted_child);
            scratch.release(child_bytes + mounted_bytes)?;
            output_names = output_names.checked_add(name_bytes)
                .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
            entries.push(MountDirectoryEntry { name, node: lookup.node, metadata: lookup.metadata, pin });
        }
        let unchanged = self.authored.unchanged_since_with_carrier(path, None, stamp, scratch.carrier)?;
        let latest = scratch.carrier.with_work(|work| self.removal_snapshot_with_work(&text, work))?;
        let latest_bytes = directory_removal_bytes(&latest.0)?;
        scratch.track(latest_bytes)?;
        let same_epoch = latest.2 == generation.1;
        drop(latest);
        scratch.release(latest_bytes)?;
        if !unchanged || !same_epoch { return Err(MountSourceError::Stale); }
        let mut cursor_bytes = 0;
        let next_cursor = if let Some(cursor) = page.value.next.take() {
            let token = self.cursors.remember(generation, (stamp, cursor), scratch.carrier)?;
            let (token, mut allocation, bytes) = token.into_parts();
            if let Some(allocation) = &mut allocation { scratch.absorb_native(allocation)?; }
            cursor_bytes = bytes;
            scratch.track(bytes)?;
            Some(token)
        } else { None };
        let lower_bytes = page.legacy_owned_bytes() + state.legacy_owned_bytes();
        drop(page); drop(state);
        scratch.release(lower_bytes)?;
        scratch.keep(output_storage + output_names + cursor_bytes)?;
        Ok(MountDirectoryPage::new(entries, next_cursor, scratch.native_backing.take()))
    }

pub(in crate::native_mount::lazy) fn list_directory(
        &self,
        path: &MountPath,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
        carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<MountDirectoryPage, MountSourceError>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let attempts = if cursor.is_some() { 1 } else { MAXIMUM_INTERRUPTED_LISTINGS };
        let mut page = Err(MountSourceError::Stale);
        for _ in 0..attempts {
            carrier.with_work(|work| mount_items(work, 1))?;
            page = self.directory_page(path, cursor, maximum_entries, carrier);
            if !matches!(page, Err(MountSourceError::Stale)) { break; }
        }
        let mut page = page?;
        while page.entries().is_empty() && page.next_cursor().is_some() {
            let (entries, next, mut ownership) = page.into_parts();
            let next = next.expect("the empty page has a continuation");
            let token_bytes = next.capacity() as u64;
            let native = ownership.has_native_custody();
            let token_ownership = match ownership.split_owned_payload(token_bytes) {
                Ok(token) => token,
                Err(error) => {
                    drop(entries);
                    drop(next);
                    drop(ownership);
                    return Err(MountSourceError::NativeWork(error));
                }
            };
            let storage = (entries.capacity() * std::mem::size_of::<MountDirectoryEntry>()) as u64;
            drop(entries);
            drop(ownership);
            if !native { carrier.with_work(|work| work.release(storage)).map_err(mount_demand_error)?; }
            let result = (|| {
                carrier.with_work(|work| mount_items(work, 1))?;
                self.directory_page(path, Some(&next), maximum_entries, carrier)
            })();
            drop(next);
            drop(token_ownership);
            if !native { carrier.with_work(|work| work.release(token_bytes)).map_err(mount_demand_error)?; }
            page = result?;
        }
        Ok(page)
    }
}

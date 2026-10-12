use super::*;

pub(super) fn resolve_path(
    source: &dyn MountFilesystem,
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<(Option<MountLookup>, Option<ViewStamp>), NativeMountError> {
    let binding_epoch = source.binding_epoch_with_meter(work)?;
    let _lease = source.acquire_binding_lease_with_meter(binding_epoch, work)?;
    let stamp = source.view_stamp_with_meter(work)?;
    let lookup = source.lookup_with_meter(path, work)?;
    if !source.view_is_stable_with_meter(work)? || source.binding_epoch_with_meter(work)? != binding_epoch {
        return Err(libc::ESTALE.into());
    }
    Ok((lookup, stamp))
}

pub(super) fn resolve_node(
    source: &dyn MountFilesystem,
    facts: Result<Resolved, NodeFacts>,
    work: &crate::NativeMaterializationWork,
) -> Result<Resolved, NativeMountError> {
    match facts {
        Ok(current) => Ok(current),
        Err(NodeFacts::Handle(open_file)) => {
            let stamp = source.view_stamp_with_meter(work)?;
            Ok(Resolved {
                lookup: open_file.file(work)?.lookup_with_meter(work)?,
                stamp,
                through: None,
            })
        }
        Err(NodeFacts::Names { file_id, mut paths }) => {
            for path in paths.drain(..) {
                let (lookup, stamp) = resolve_path(source, &path, work)?;
                if let Some(lookup) = lookup
                    && file_id.is_none_or(|file_id| lookup.node.file_id == file_id)
                {
                    return Ok(Resolved {
                        lookup,
                        stamp,
                        through: Some(path),
                    });
                }
            }
            Err(libc::ENOENT.into())
        }
    }
}

impl FuseProjection {
    pub(super) fn child(&self, state: &ProjectionState, parent: u64, name: &OsStr, work: &crate::NativeMaterializationWork) -> Result<ChildName, NativeMountError> {
        Ok(ChildName::new(
            self.core.source.as_ref(),
            state.child_path(parent, name, work)?,
            work,
        )?)
    }

    pub(super) fn confirm_names<'a>(
        &self,
        mut confirmed: HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>,
        resolved: impl IntoIterator<Item = (&'a MountPath, MountNode)>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<HashMap<(crate::FileId, FuseOwned<MountPath>), Option<ViewStamp>>, NativeMountError> {
        let source = self.core.source.as_ref();
        for (path, node) in resolved {
            fuse_item(work)?;
            if node.link_count <= 1 { continue; }
            let mut names = self.core.state()?.unvouched_names(source, node.file_id, path, work)?;
            for name in names.drain(..) {
                match resolve_path(source, &name, work) {
                    Ok((Some(found), stamp)) if found.node.file_id == node.file_id => {
                        confirm_name(&mut confirmed, node.file_id, name, stamp, work)?;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        self.core.record_handler_failure(work, error);
                        confirm_name(&mut confirmed, node.file_id, name, None, work)?;
                    }
                }
            }
        }
        Ok(confirmed)
    }

    pub(super) fn lookup_entry(&self, parent: u64, name: &OsStr, work: &crate::NativeMaterializationWork) -> Result<Entry, NativeMountError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let child = {
            let mut state = self.core.state()?;
            let child = self.child(&state, parent, name, work)?;
            let ProjectionState { by_inode, inode_by_path, .. } = &mut *state;
            if let Some((inode, lookup, stamp)) = cached_projected_lookup(by_inode, inode_by_path, child.key(),
                |file, stamp| source.unchanged_since_with_meter(child.key(), Some(file), stamp, work).map_err(NativeMountError::from), work)? {
                let attr = state.attr(inode, &lookup)?;
                let ttl = state.admit_entry(source, inode, &child, Some(stamp), work)?;
                return Ok(Entry { attr, ttl });
            }
            if let Some(empty) = state.empty_since(parent)
                && child.is_exact()
                && source.unchanged_since_with_meter(child.key(), None, empty, work)?
            {
                return Ok(state.negative_entry(source, parent, &child, Some(empty), work)?);
            }
            child
        };
        let (found, stamp) = resolve_path(source, &child.spelled, work)?;
        let confirmed = match found {
            Some(lookup) => {
                self.confirm_names(HashMap::new(), [(child.key(), lookup.node)], work)?
            }
            None => HashMap::new(),
        };
        let mut state = self.core.state()?;
        let Some(lookup) = found else {
            state.remove_path_cache(child.key(), work)?;
            return Ok(state.negative_entry(source, parent, &child, stamp, work)?);
        };
        let inode = state.intern(
            source,
            fuse_clone_path(child.key(), work)?,
            &lookup,
            stamp,
            true,
            &confirmed,
            work,
        )?;
        let attr = state
            .attr(inode, &lookup)
            .map_err(|error| state.release_lookup_reference(inode, 1, work).err().unwrap_or_else(|| error.into()))?;
        let ttl = state.admit_entry(source, inode, &child, stamp, work)?;
        Ok(Entry { attr, ttl })
    }

    pub(super) fn node_attributes(
        &self,
        inode: u64,
        handle: Option<u64>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(FileAttr, Duration), NativeMountError> {
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let facts = {
            let mut state = self.core.state()?;
            match state.node_facts(source, inode, handle, work)? {
                Ok(current) => {
                    let attr = state.attr(inode, &current.lookup)?;
                    let ttl = state.admit_attributes(source, inode, current.stamp, work)?;
                    return Ok((attr, ttl));
                }
                facts => facts,
            }
        };
        let resolved = resolve_node(source, facts, work)?;
        Ok(self.core.state()?.apply(source, inode, &resolved, work)?)
    }

    pub(super) fn create_node(
        &self,
        parent: u64,
        name: &OsStr,
        create: impl FnOnce(&MountPath, &crate::NativeMaterializationWork) -> Result<MountLookup, NativeMountError>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Entry, NativeMountError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let child = {
            let state = self.core.state()?;
            state.admit_write()?;
            self.child(&state, parent, name, work)?
        };
        let stamp = source.view_stamp_with_meter(work)?;
        let lookup = self.source_write(work, || create(&child.spelled, work))?;
        let empty = if lookup.node.kind == MountNodeKind::Directory {
            let after = source.view_stamp_with_meter(work)?;
            match stamp {
                Some(before) if source.listing_unchanged_since_with_meter(child.key(), before, work)? => after,
                _ => None,
            }
        } else { None };
        // A node just created has no recorded name to confirm.
        let confirmed =
            self.confirm_names(HashMap::new(), [(child.key(), lookup.node)], work)?;
        let mut state = self.core.state()?;
        let inode = state.intern(
            source,
            fuse_clone_path(child.key(), work)?,
            &lookup,
            stamp,
            true,
            &confirmed,
            work,
        )?;
        if let Some(entry) = state.by_inode.get_mut(&inode) {
            entry.empty_since = empty;
        }
        let attr = state
            .attr(inode, &lookup)
            .map_err(|error| state.release_lookup_reference(inode, 1, work).err().unwrap_or_else(|| error.into()))?;
        let ttl = state.admit_entry(source, inode, &child, stamp, work)?;
        Ok(Entry { attr, ttl })
    }

    pub(super) fn hold_unopened(&self, lookup: Option<&MountLookup>, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        if !self.skips_open {
            return Ok(());
        }
        let inode = self.core.state()?.unopened_referenced_inode(lookup);
        // The caller holds the names lock exclusively.
        inode.map_or(Ok(()), |inode| {
            self.reopen_implicit(inode, None, Access::Read, work)
                .map(|_| ())
        })
    }

    pub(super) fn remove_name(&self, parent: u64, name: &OsStr, work: &crate::NativeMaterializationWork) -> Result<(), NativeMountError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names_exclusive()?;
        let child = {
            let state = self.core.state()?;
            state.admit_write()?;
            self.child(&state, parent, name, work)?
        };
        let path = &child.spelled;
        let current = source.lookup_with_meter(path, work)?;
        self.hold_unopened(current.as_ref(), work)?;
        let detaching = self.core.state()?.detaching_inode(current.as_ref());
        let detached = detaching.map(|inode| {
            let detached = source.detach_file_with_meter(path, work)?;
            Ok::<_, NativeMountError>((inode, FuseOpenFile::Source(fuse_open_file(detached, work)?)))
        }).transpose()?;
        let removed = self.source_write(work, || source
            .remove(path, current.map(|lookup| lookup.node.file_id), work)
            .map_err(NativeMountError::from));
        let mut state = self.core.state()?;
        if let Some((inode, detached)) = detached {
            state.retain_detached_handles(inode, &detached, work)?;
        }
        removed?;
        state.remove_path_cache(child.key(), work)?;
        Ok(())
    }

    pub(super) fn rename_name(
        &self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP.into());
        }
        let replace = flags & RENAME_NOREPLACE == 0;
        let _names = self.core.names_exclusive()?;
        let (from, to) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                self.child(&state, parent, name, work)?,
                self.child(&state, new_parent, new_name, work)?,
            )
        };
        let replaced = if replace {
            source.lookup_with_meter(&to.spelled, work)?
        } else {
            None
        };
        self.hold_unopened(replaced.as_ref(), work)?;
        let detaching = self.core.state()?.detaching_inode(replaced.as_ref());
        let detached = detaching.map(|inode| {
            let detached = source.detach_file_with_meter(&to.spelled, work)?;
            Ok::<_, NativeMountError>((inode, FuseOpenFile::Source(fuse_open_file(detached, work)?)))
        }).transpose()?;
        let renamed = self.source_write(work, || source
            .rename(&from.spelled, &to.spelled, replace, work)
            .map_err(NativeMountError::from));
        let mut state = self.core.state()?;
        if let Some((inode, detached)) = detached {
            state.retain_detached_handles(inode, &detached, work)?;
        }
        renamed?;
        if from.key() != to.key() {
            state.invalidate_prefix(to.key(), work)?;
            state.rename_prefix(from.key(), to.key(), work)?;
        }
        Ok(())
    }

    pub(super) fn link_name(&self, inode: u64, new_parent: u64, new_name: &OsStr, work: &crate::NativeMaterializationWork) -> Result<Entry, NativeMountError> {
        self.check_source_write(work)?;
        let source = self.core.source.as_ref();
        let _names = self.core.names()?;
        let (from, to, facts) = {
            let state = self.core.state()?;
            state.admit_write()?;
            (
                fuse_clone_path(state.path(inode)?, work)?,
                self.child(&state, new_parent, new_name, work)?,
                state.node_facts(source, inode, None, work)?,
            )
        };
        let current = resolve_node(source, facts, work)?;
        let mut projected = current.lookup;
        projected.node.link_count = projected
            .node
            .link_count
            .checked_add(1)
            .ok_or(libc::EMLINK)?;
        self.source_write(work, || source.hard_link(&from, &to.spelled, work).map_err(NativeMountError::from))?;
        let mut state = self.core.state()?;
        let attr = state.attr(inode, &projected)?;
        let binding = {
            let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
            if entry.lookup.node.file_id != projected.node.file_id {
                return Err(libc::ESTALE.into());
            }
            if entry.binding(to.key(), work)?.is_none() {
                fuse_reserve(&mut entry.bindings, 1, work)?;
                Some(Binding { path: fuse_clone_path(to.key(), work)?, verified: None, kernel: None })
            } else { None }
        };
        let ProjectionState {
            by_inode,
            inode_by_path,
            inode_by_file,
            ..
        } = &mut *state;
        bind_path(by_inode, inode_by_path, inode_by_file, to.key(), inode, work)?;
        let entry = state.by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
        if let Some(binding) = binding { entry.bindings.push(binding); }
        // Derived locally from the facts before the link, not read back.
        entry.lookup = projected;
        entry.facts = None;
        entry.lookup_references = entry.lookup_references.saturating_add(1);
        let ttl = state.admit_entry(source, inode, &to, current.stamp, work)?;
        Ok(Entry { attr, ttl })
    }

}

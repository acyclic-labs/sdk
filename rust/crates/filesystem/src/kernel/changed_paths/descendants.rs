//! Only unchanged direct edges below a moved directory need subtree expansion.
use super::super::allocation::fixed_index::FixedIndex;
use super::*;

struct Directory {
    file_id: FileId,
    entries: Values<TreeFormat>,
}

pub(super) struct Descendants {
    frames: Owned<Vec<Directory>>,
    active: FixedIndex<()>,
    entry: Option<Owned<TreeEntry>>,
    parent: Option<FileId>,
    records: Option<[FileRecord; 2]>,
    path: Option<PathState>,
    side: usize,
}

impl Descendants {
    pub(super) fn new(
        record: FileRecord,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, FsError> {
        let mut result = Self {
            frames: Owned::allocate(0, accounting)?,
            active: FixedIndex::default(),
            entry: None,
            parent: None,
            records: None,
            path: None,
            side: 0,
        };
        if let Err(error) = result.push(record, config, accounting) {
            result.retire(accounting)?;
            return Err(error);
        }
        Ok(result)
    }

    fn push(
        &mut self,
        record: FileRecord,
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        let FilePayload::Directory { entries } = record.payload else {
            return Err(FsError::InvalidDiff);
        };
        let frames = &mut self.frames;
        let mut prepared = None;
        // The shared index publishes membership only after this original
        // frame admission succeeds. Existing keys never run the constructor.
        self.active.get_or_try_insert_with(
            record.file_id.into_bytes(),
            accounting,
            |accounting| {
                frames
                    .ensure_for_push(usize::from(config.limits.maximum_path_depth), accounting)?;
                prepared = Some(Values::new(
                    entries,
                    DecodeLimits::for_volume(config),
                    accounting,
                )?);
                Ok(())
            },
        )?;
        let entries = prepared.ok_or(FsError::InvalidDiff)?;
        frames.value.push(Directory {
            file_id: record.file_id,
            entries,
        });
        Ok(())
    }

    fn pop(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let frame = self.frames.value.pop().ok_or(FsError::InvalidDiff)?;
        // The exact directory cursor retires before its active membership.
        // Neither cleanup step admits new work or consults cancellation.
        let retired = frame.entries.retire(accounting);
        if !self.active.retire_if(frame.file_id.into_bytes(), |_| true) {
            return Err(FsError::InvalidDiff);
        }
        retired
    }

    pub(super) async fn next<S: AsyncObjectStore>(
        &mut self,
        store: &S,
        roots: [&GenerationRoot; 2],
        config: VolumeConfig,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<Candidate>, FsError> {
        loop {
            accounting.check()?;
            cancellation.check()?;
            if let Some(records) = self.records {
                if self.side < 2 {
                    let side = self.side;
                    let record = records[side];
                    if self.path.is_none() {
                        let parent = self.parent.ok_or(FsError::InvalidDiff)?;
                        let edge = if side == 0 {
                            let name = &self.entry.as_ref().ok_or(FsError::InvalidDiff)?.value.name;
                            let (name, allocation) =
                                name.clone_with_accounting(accounting, cancellation)?;
                            let bytes = name.logical_owned_bytes();
                            Owned::new(Binding { parent, name }, allocation, bytes)
                        } else {
                            let entry = self.entry.take().ok_or(FsError::InvalidDiff)?;
                            Owned::new(
                                Binding {
                                    parent,
                                    name: entry.value.name,
                                },
                                entry.allocation,
                                entry.bytes,
                            )
                        };
                        self.path = Some(PathState::new(record.file_id, edge, accounting)?);
                    }
                    self.path
                        .as_mut()
                        .ok_or(FsError::InvalidDiff)?
                        .advance(
                            store,
                            roots[side].root_file_id,
                            roots[side].file_table,
                            config,
                            accounting,
                            cancellation,
                        )
                        .await?;
                    let path = self
                        .path
                        .take()
                        .ok_or(FsError::InvalidDiff)?
                        .finish(config, accounting)?;
                    self.side += 1;
                    return Ok(Some(Candidate { path, record, side }));
                }
                self.records = None;
                self.parent = None;
                self.side = 0;
                continue;
            }
            let Some(frame) = self.frames.value.last_mut() else {
                return Ok(None);
            };
            self.parent = Some(frame.file_id);
            self.entry = frame.entries.next(store, accounting, cancellation).await?;
            let Some(entry) = &self.entry else {
                self.pop(accounting)?;
                continue;
            };
            let file_id = entry.value.file_id;
            let before = record(store, roots[0], file_id, config, accounting, cancellation)
                .await?
                .ok_or(FsError::InvalidDiff)?;
            if before.kind != entry.value.kind {
                return Err(FsError::InvalidDiff);
            }
            let after = record(store, roots[1], file_id, config, accounting, cancellation).await?;
            // A nested move, removal, or replacement owns its own frontier and
            // subtree; an ancestor must not expand it again.
            if let Some(after) = after
                && before.kind == FileKind::Directory
                && after.kind == FileKind::Directory
                && before.bindings == after.bindings
            {
                self.push(before, config, accounting)?;
            }
            if let Some(after) = after
                && !record_changed(&before, &after)
                && (before.bindings == after.bindings
                    || unchanged_edge(
                        store,
                        after,
                        self.parent.ok_or(FsError::InvalidDiff)?,
                        &self.entry.as_ref().ok_or(FsError::InvalidDiff)?.value.name,
                        config,
                        accounting,
                        cancellation,
                    )
                    .await?)
            {
                self.records = Some([before, after]);
            } else {
                // Content changes and changed direct edges are handled by the
                // file-table frontier. Retained edges still follow this move.
                self.entry
                    .take()
                    .ok_or(FsError::InvalidDiff)?
                    .retire(accounting)?;
            }
        }
    }

    pub(super) fn retire(mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        let mut result = self
            .entry
            .take()
            .map_or(Ok(()), |entry| entry.retire(accounting));
        let path = self
            .path
            .take()
            .map_or(Ok(()), |path| path.retire(accounting));
        result = result.and(path);
        while !self.frames.value.is_empty() {
            let retired = self.pop(accounting);
            result = result.and(retired);
        }
        drop(self.active);
        let frames = self.frames.retire(accounting);
        result.and(frames)
    }
}

/// Test the original direct edge in the authenticated set, without cloning its
/// name or expanding any unrelated alias into a path.
async fn unchanged_edge<S: AsyncObjectStore>(
    store: &S,
    record: FileRecord,
    parent: FileId,
    name: &super::super::LogicalName,
    config: VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    let present = super::super::persistent_point::lookup_projected_with_accounting::<
        S,
        BindingFormat,
        _,
        _,
        _,
    >(
        store,
        record.bindings.ok_or(FsError::InvalidDiff)?,
        |edge| edge.parent.cmp(&parent).then_with(|| edge.name.cmp(name)),
        |_, accounting| Ok(Owned::new((), accounting.claim_owned(0, 0)?, 0)),
        DecodeLimits::for_volume(config),
        accounting,
        cancellation,
        None,
    )
    .await?;
    let found = present.is_some();
    if let Some(present) = present {
        present.retire(accounting)?;
    }
    Ok(found)
}

#[cfg(test)]
mod tests;

//! Only unchanged direct edges below a moved directory need subtree expansion.
use super::*;

struct Directory {
    file_id: FileId,
    entries: Values<TreeFormat>,
}

pub(super) struct Descendants {
    frames: Owned<Vec<Directory>>,
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
        for frame in &self.frames.value {
            accounting.charge_items(1)?;
            if frame.file_id == record.file_id {
                return Err(FsError::InvalidDiff);
            }
        }
        self.frames.ensure_for_push(usize::MAX, accounting)?;
        let entries = Values::new(entries, DecodeLimits::for_volume(config), accounting)?;
        self.frames.value.push(Directory {
            file_id: record.file_id,
            entries,
        });
        Ok(())
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
                self.frames
                    .value
                    .pop()
                    .ok_or(FsError::InvalidDiff)?
                    .entries
                    .retire(accounting)?;
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
        while let Some(frame) = self.frames.value.pop() {
            let retired = frame.entries.retire(accounting);
            result = result.and(retired);
        }
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

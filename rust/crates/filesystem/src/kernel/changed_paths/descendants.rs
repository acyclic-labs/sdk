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
                    .ensure_for_push(usize::MAX, accounting)?;
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

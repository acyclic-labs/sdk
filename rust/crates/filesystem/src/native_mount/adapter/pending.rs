/// as one group: when a durability request settles it, when its bytes
/// outgrow [`PENDING_CREATE_BYTES`], when [`MAXIMUM_GROUPED_CHANGES`]
/// creations wait, or before anything reads or changes what it creates,
/// whichever is first. Its name is recorded as changed when it is created,
/// so nothing remembers it absent, and a lookup of that name reports it
/// from here; no other name's answer depends on it.
pub(super) struct PendingCreate {
    path: NamespacePath,
    file_id: FileId,
    origin: ViewOrigin,
    maximum_bytes: u64,
    state: StdMutex<PendingState>,
}

enum PendingState {
    Pending {
        metadata: Box<FileMetadata>,
        bytes: Vec<u8>,
    },
    Applying,
    Applied,
    Failed(MountSourceError),
}

impl PendingCreate {
    fn state(&self) -> std::sync::MutexGuard<'_, PendingState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The change that applies the creation, once.
    fn begin_applying(&self) -> Option<GroupedChange> {
        let mut state = self.state();
        match std::mem::replace(&mut *state, PendingState::Applying) {
            PendingState::Pending { metadata, bytes } => Some(GroupedChange::CreateFile {
                path: self.path.clone(),
                metadata,
                file_id: self.file_id,
                bytes: Bytes::from(bytes),
            }),
            other => {
                *state = other;
                None
            }
        }
    }

    fn finish_applying(&self, applied: Result<GroupedOutcome, MountSourceError>) {
        *self.state() = match applied {
            Ok(_) => PendingState::Applied,
            Err(error) => PendingState::Failed(error),
        };
    }

    /// The file as it is created, while its creation waits.
    fn lookup(&self) -> Option<MountLookup> {
        match &*self.state() {
            PendingState::Pending { metadata, bytes } => Some(MountLookup {
                node: MountNode {
                    file_id: self.file_id,
                    kind: MountNodeKind::Regular,
                    logical_bytes: bytes.len() as u64,
                    link_count: 1,
                    device: None,
                },
                metadata: **metadata,
            }),
            _ => None,
        }
    }

    /// What the creation left for a handle to report once applied.
    fn applied(&self) -> Result<(), MountSourceError> {
        match &*self.state() {
            PendingState::Failed(error) => Err(error.clone()),
            _ => Ok(()),
        }
    }
}

/// Most queued changes one hold of the checkout applies as one mutation.
const MAXIMUM_GROUPED_CHANGES: usize = 64;

/// Changes waiting for group commit, and whether a drain is running.
#[derive(Default)]
struct PendingCreatedFile<A, O> {
    pending: Arc<PendingCreate>,
    attached: CheckoutAttachedFile<A, O>,
}

impl<A, O> PendingCreatedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Applies the creation, if it is still pending, and reports how it went.
    fn apply(&self) -> Result<(), MountSourceError> {
        let pending = matches!(&*self.pending.state(), PendingState::Pending { .. });
        if pending {
            self.attached.runtime.wait(|| async {
                self.attached.checkout.settle_pending().await;
                Ok(())
            })?;
        }
        self.pending.applied()
    }

    /// Runs `held` on the bytes and facts the handle holds while its
    /// creation is pending and they stay within their bound; otherwise
    /// applies the creation and runs `attached` on the checkout's file.
    fn held_or_attached<T>(
        &self,
        held: impl FnOnce(&mut Box<FileMetadata>, &mut Vec<u8>, u64) -> Option<T>,
        attached: impl FnOnce(&CheckoutAttachedFile<A, O>) -> Result<T, MountSourceError>,
    ) -> Result<T, MountSourceError> {
        {
            let mut state = self.pending.state();
            if let PendingState::Pending { metadata, bytes } = &mut *state
                && let Some(value) = held(metadata, bytes, self.pending.maximum_bytes)
            {
                return Ok(value);
            }
        }
        self.apply()?;
        attached(&self.attached)
    }

    /// Records that the held file changed, as an applied change would.
    fn record_held_change(&self) {
        let _origin = self.pending.origin.enter();
        self.attached
            .checkout
            .ledger
            .record(&ViewChange::Node(self.pending.file_id));
        self.attached
            .checkout
            .revision
            .fetch_add(1, Ordering::AcqRel);
    }

    fn held_lookup(&self, metadata: FileMetadata, logical_bytes: u64) -> MountLookup {
        MountLookup {
            node: MountNode {
                file_id: self.pending.file_id,
                kind: MountNodeKind::Regular,
                logical_bytes,
                link_count: 1,
                device: None,
            },
            metadata,
        }
    }
}

/// Bytes `[offset, offset + length)` of `bytes`, clipped to its end.
fn held_range(bytes: &[u8], offset: u64, length: u64) -> Bytes {
    let start = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    let end = usize::try_from(offset.saturating_add(length))
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    Bytes::copy_from_slice(bytes.get(start..end).unwrap_or_default())
}

impl<A, O> MountOpenFile for PendingCreatedFile<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn lookup(&self) -> Result<MountLookup, MountSourceError> {
        self.held_or_attached(
            |metadata, bytes, _| Some(self.held_lookup(**metadata, bytes.len() as u64)),
            MountOpenFile::lookup,
        )
    }

    fn read_range(&self, offset: u64, length: u32) -> Result<Bytes, MountSourceError> {
        self.held_or_attached(
            |_, bytes, _| {
                let end = offset.checked_add(u64::from(length))?;
                (end <= bytes.len() as u64).then(|| held_range(bytes, offset, u64::from(length)))
            },
            |file| file.read_range(offset, length),
        )
    }

    fn read_up_to(&self, offset: u64, maximum_bytes: u32) -> Result<Bytes, MountSourceError> {
        self.held_or_attached(
            |_, bytes, _| Some(held_range(bytes, offset, u64::from(maximum_bytes))),
            |file| file.read_up_to(offset, maximum_bytes),
        )
    }

    fn seek(&self, offset: u64, target: MountSeekTarget) -> Result<Option<u64>, MountSourceError> {
        self.apply()?;
        self.attached.seek(offset, target)
    }

    fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
        let time = content_change_time()?;
        let written = bytes.clone();
        let held = self.held_or_attached(
            |metadata, held, maximum| {
                let end = offset.checked_add(written.len() as u64)?;
                if end > maximum {
                    return None;
                }
                let start = usize::try_from(offset).ok()?;
                let end = usize::try_from(end).ok()?;
                if held.len() < end {
                    held.resize(end, 0);
                }
                held.get_mut(start..end)?.copy_from_slice(&written);
                metadata.stamp_content_change(time);
                Some(true)
            },
            |file| file.write_range(offset, bytes).map(|()| false),
        )?;
        if held {
            self.record_held_change();
        }
        Ok(())
    }

    fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
        let time = content_change_time()?;
        let held = self.held_or_attached(
            |metadata, held, maximum| {
                if logical_bytes > maximum {
                    return None;
                }
                held.resize(usize::try_from(logical_bytes).ok()?, 0);
                metadata.stamp_content_change(time);
                Some(true)
            },
            |file| file.resize(logical_bytes).map(|()| false),
        )?;
        if held {
            self.record_held_change();
        }
        Ok(())
    }

    fn allocate_range(
        &self,
        offset: u64,
        length: u64,
        operation: MountRangeAllocation,
    ) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.allocate_range(offset, length, operation)
    }

    fn set_attributes(
        &self,
        metadata: FileMetadata,
        logical_bytes: Option<u64>,
    ) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.set_attributes(metadata, logical_bytes)
    }

    fn read_attribute(&self, name: &[u8]) -> Result<Option<Bytes>, MountSourceError> {
        self.apply()?;
        self.attached.read_attribute(name)
    }

    fn list_attributes(
        &self,
        cursor: Option<&[u8]>,
        maximum_entries: u32,
    ) -> Result<MountAttributePage, MountSourceError> {
        self.apply()?;
        self.attached.list_attributes(cursor, maximum_entries)
    }

    fn write_attribute(
        &self,
        name: &[u8],
        value: Bytes,
        mode: MountAttributeWriteMode,
    ) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.write_attribute(name, value, mode)
    }

    fn remove_attribute(&self, name: &[u8]) -> Result<(), MountSourceError> {
        self.apply()?;
        self.attached.remove_attribute(name)
    }

    fn settle(&self) -> Result<(), MountSourceError> {
        self.apply()
    }
}


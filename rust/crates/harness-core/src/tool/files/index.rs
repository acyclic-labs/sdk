//! Immutable ordinal-to-byte pages, stored by the original volume publisher.
use super::*;
use crate::{OperationId, runtime::PrivateContent};

// A page size splits work; it places no bound on a source or its ordinal count.
const ENTRIES: u64 = 256;
const HEADER: usize = 16;
const BYTES: usize = HEADER + ENTRIES as usize * 8;

#[derive(Clone, Copy)]
pub(super) enum Projection {
    Lines,
    Cells,
}

impl Projection {
    fn directory(self) -> &'static str {
        match self {
            Self::Lines => "lines",
            Self::Cells => "cells",
        }
    }
    fn domain(self) -> &'static [u8] {
        match self {
            Self::Lines => b"file-projection/line-index/v1\0",
            Self::Cells => b"file-projection/cell-index/v1\0",
        }
    }
}

struct Page {
    bytes: [u8; BYTES],
    count: usize,
}

impl Page {
    fn new(first: u64) -> Self {
        let mut page = Self {
            bytes: [0; BYTES],
            count: 0,
        };
        page.bytes[..8].copy_from_slice(&first.to_le_bytes());
        page
    }

    fn first(&self) -> u64 {
        word(&self.bytes[..8])
    }

    fn full(&self) -> bool {
        self.count == ENTRIES as usize
    }

    fn reset(&mut self, first: u64) {
        self.bytes[..8].copy_from_slice(&first.to_le_bytes());
        self.bytes[8] = 0;
        self.count = 0;
    }

    fn terminal(&self) -> bool {
        self.bytes[8] != 0
    }

    fn append(&mut self, ordinal: u64, position: u64, terminal: bool) -> bool {
        if ordinal < self.first() || self.terminal() {
            return false;
        }
        debug_assert_eq!(ordinal, self.first() + self.count as u64);
        debug_assert!(!self.full());
        let start = HEADER + self.count * 8;
        self.bytes[start..start + 8].copy_from_slice(&position.to_le_bytes());
        self.count += 1;
        self.bytes[8] = u8::from(terminal);
        self.full()
    }

    fn encoded(&self) -> &[u8] {
        &self.bytes[..HEADER + self.count * 8]
    }
}

fn word(bytes: &[u8]) -> u64 {
    let mut word = [0; 8];
    word.copy_from_slice(bytes);
    u64::from_le_bytes(word)
}

fn cursor(
    bytes: &[u8],
    first: u64,
    ordinal: u64,
    source_length: u64,
    work: crate::NativeWork<'_>,
) -> Result<(u64, u64)> {
    let invalid = || executor::rejection(work, "ordinal index differs from its source");
    if bytes.len() < HEADER + 8
        || bytes.len() > BYTES
        || (bytes.len() - HEADER) % 8 != 0
        || word(&bytes[..8]) != first
        || bytes[8] > 1
        || bytes[9..HEADER].iter().any(|byte| *byte != 0)
    {
        return Err(invalid());
    }
    let count = (bytes.len() - HEADER) / 8;
    let terminal = bytes[8] != 0;
    if !terminal && count != ENTRIES as usize {
        return Err(invalid());
    }
    let mut previous = None;
    for bytes in bytes[HEADER..].chunks_exact(8) {
        let position = word(bytes);
        if position > source_length || previous.is_some_and(|previous| previous >= position) {
            return Err(invalid());
        }
        previous = Some(position);
    }
    let relative = ordinal.checked_sub(first).ok_or_else(invalid)?;
    if terminal && relative >= count as u64 {
        return Err(executor::rejection(work, "ordinal offset exceeds source"));
    }
    if terminal && previous != Some(source_length) {
        return Err(invalid());
    }
    let selected = relative.min(count as u64 - 1);
    let start = HEADER + selected as usize * 8;
    Ok((first + selected, word(&bytes[start..start + 8])))
}

/// No registry or idle state: source identity and page number address the
/// original immutable receipt directly. Objects retain the page bytes.
pub(super) struct OrdinalIndex<'a> {
    task: &'a crate::runtime::TaskContext,
    destination: PrivateContent<'a>,
    identity: blake3::Hasher,
    source_length: u64,
    projection: Projection,
    origin: u64,
    logical_end: u64,
    page: Page,
    _hold: crate::NativeCustody,
}

impl<'a> OrdinalIndex<'a> {
    fn work(&self) -> crate::NativeWork<'_> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        return self._hold.as_ref().map(|hold| hold.work());
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        None
    }

    fn path(&self, operation: OperationId) -> Result<(String, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(hold) = &self._hold {
            let (path, hold) = hold.work().display(&format_args!(
                "projections/v1/{}/{operation}.bin",
                self.projection.directory()
            ))?;
            return Ok((path, Some(hold)));
        }
        Ok((
            format!(
                "projections/v1/{}/{operation}.bin",
                self.projection.directory()
            ),
            None,
        ))
    }

    pub fn destination(
        context: &'a ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<PrivateContent<'a>>> {
        if context
            .task()
            .scope()
            .limits()
            .file_bytes
            .is_some_and(|maximum| maximum < BYTES as u64)
        {
            return Ok(None);
        }
        context.task().projection_destination(io)
    }

    pub fn new(
        context: &'a ToolContext,
        destination: PrivateContent<'a>,
        version: &str,
        source_length: u64,
        projection: Projection,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        let domain = projection.domain();
        let hold = edit::allocation(work, std::mem::size_of::<Self>())?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                bytes_hashed: domain.len() as u64 + version.len() as u64,
                ..Default::default()
            })?;
        }
        let mut identity = blake3::Hasher::new();
        identity.update(domain);
        identity.update(version.as_bytes());
        let mut page = Page::new(0);
        page.append(0, 0, source_length == 0);
        Ok(Self {
            task: context.task(),
            destination,
            identity,
            source_length,
            projection,
            origin: 0,
            logical_end: source_length,
            page,
            _hold: hold,
        })
    }

    fn operation(&self, first: u64) -> Result<OperationId> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(hold) = &self._hold {
            hold.work().charge(acyclic_fs::WorkCounters {
                bytes_hashed: 8,
                ..Default::default()
            })?;
        }
        let mut identity = self.identity.clone();
        identity.update(&first.to_le_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&identity.finalize().as_bytes()[..16]);
        Ok(OperationId::from_bytes(bytes))
    }

    async fn get(
        &self,
        first: u64,
        ordinal: u64,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(u64, u64)>> {
        let Some(bytes) = self
            .observe(self.operation(first)?, BYTES as u64, io)
            .await?
        else {
            return Ok(None);
        };
        let selected = cursor(&bytes, first, ordinal, self.logical_end, work)?;
        if selected.1 < self.origin {
            return Err(executor::rejection(
                work,
                "ordinal index precedes its source array",
            ));
        }
        Ok(Some(selected))
    }

    async fn observe(
        &self,
        operation: OperationId,
        maximum: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let _file_hold;
        let (file, retained) = match self
            .task
            .observe_private_file_once(self.destination, operation, io)
            .await
        {
            Ok(found) => found,
            Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized => return Ok(None),
            Err(error) => return Err(error),
        };
        _file_hold = super::window::custody(io, retained)?;
        let Some(file) = file else {
            return Ok(None);
        };
        let _path_hold;
        let (path, hold) = self.path(operation)?;
        _path_hold = hold;
        if file.path() != path
            || file.descriptor().media_type() != "application/octet-stream"
            || file.display_name() != "ordinal-index.bin"
        {
            return Err(executor::rejection(
                self.work(),
                "projection receipt belongs to another publication",
            ));
        }
        if file.descriptor().byte_length() > maximum {
            return Err(executor::rejection(
                self.work(),
                "ordinal index page exceeds its framing",
            ));
        }
        let bytes = match self.task.read_file(&file, io).await {
            Ok(bytes) => bytes,
            Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized => return Ok(None),
            Err(error) => return Err(error),
        };
        edit::charge(&self._hold, 0, bytes.len() as u64)?;
        Ok(Some(bytes))
    }

    fn metadata_operation(&self) -> Result<OperationId> {
        edit::charge(&self._hold, 0, 1)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(hold) = &self._hold {
            hold.work().charge(acyclic_fs::WorkCounters {
                bytes_hashed: 4,
                ..Default::default()
            })?;
        }
        let mut identity = self.identity.clone();
        identity.update(b"root");
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&identity.finalize().as_bytes()[..16]);
        Ok(OperationId::from_bytes(bytes))
    }

    pub async fn metadata(
        &self,
        maximum: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        self.observe(self.metadata_operation()?, maximum, io).await
    }

    pub async fn publish_metadata(
        &self,
        bytes: &[u8],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.stage(self.metadata_operation()?, bytes, io).await
    }

    pub fn origin(&mut self, position: u64) -> Result<()> {
        if self.page.first() != 0 || self.page.count != 1 || position > self.source_length {
            return Err(executor::rejection(self.work(), "invalid ordinal origin"));
        }
        self.page.bytes[HEADER..HEADER + 8].copy_from_slice(&position.to_le_bytes());
        self.origin = position;
        Ok(())
    }

    pub fn end(&mut self, position: u64) -> Result<()> {
        if position < self.origin || position > self.source_length {
            return Err(executor::rejection(self.work(), "invalid ordinal end"));
        }
        self.logical_end = position;
        Ok(())
    }

    /// At most two direct lookups. A missing page never walks a directory or
    /// searches an index history; cold work resumes through the range reader.
    pub async fn seek(
        &mut self,
        ordinal: u64,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(u64, u64)> {
        if ordinal == 0 {
            return Ok((0, self.origin));
        }
        let first = ordinal / ENTRIES * ENTRIES;
        let found = match self.get(first, ordinal, work, io).await? {
            Some(cursor) => Some(cursor),
            None if first != 0 => self.get(first - ENTRIES, ordinal, work, io).await?,
            None => None,
        };
        if let Some(cursor) = found {
            // Already committed entries need no rewrite. Begin the next page
            // only when the source scan reaches its first unseen boundary.
            let next = (cursor.0 / ENTRIES * ENTRIES)
                .checked_add(ENTRIES)
                .ok_or_else(|| executor::rejection(work, "ordinal index position overflow"))?;
            self.page.reset(next);
            return Ok(cursor);
        }
        Ok((0, self.origin))
    }

    pub fn record(&mut self, ordinal: u64, position: u64) -> Result<bool> {
        if ordinal < self.page.first() || self.page.terminal() {
            return Ok(false);
        }
        if self.page.full()
            || self.page.first().checked_add(self.page.count as u64) != Some(ordinal)
            || position > self.source_length
        {
            return Err(executor::rejection(
                self.work(),
                "invalid ordinal index boundary",
            ));
        }
        edit::charge(&self._hold, 8, 1)?;
        Ok(self.page.append(
            ordinal,
            position,
            matches!(self.projection, Projection::Lines) && position == self.source_length,
        ))
    }

    pub fn finish(&mut self, ordinal: u64, position: u64) -> Result<()> {
        self.end(position)?;
        if ordinal == 0 && self.page.first() == 0 && self.page.count == 1 {
            self.page.bytes[HEADER..HEADER + 8].copy_from_slice(&position.to_le_bytes());
        } else {
            self.record(ordinal, position)?;
        }
        self.page.bytes[8] = 1;
        Ok(())
    }

    pub async fn publish(&mut self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        if self.page.count == 0 || !(self.page.full() || self.page.terminal()) {
            return Ok(());
        }
        let operation = self.operation(self.page.first())?;
        self.stage(operation, self.page.encoded(), io).await?;
        let next =
            self.page.first().checked_add(ENTRIES).ok_or_else(|| {
                executor::rejection(self.work(), "ordinal index position overflow")
            })?;
        self.page.reset(next);
        Ok(())
    }

    async fn stage(
        &self,
        operation: OperationId,
        bytes: &[u8],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let _path_hold;
        let (path, hold) = self.path(operation)?;
        _path_hold = hold;
        let _file_hold;
        let (file, retained) = match self
            .task
            .stage_private_file_once(
                self.destination,
                operation,
                &path,
                bytes,
                ("application/octet-stream", "ordinal-index.bin"),
                io,
            )
            .await
        {
            Ok(published) => published,
            Err(error) if error.code() == crate::wire::ErrorCode::Unauthorized => return Ok(()),
            Err(error) => return Err(error),
        };
        _file_hold = super::window::custody(io, retained)?;
        drop(file);
        Ok(())
    }
}

#[cfg(test)]
mod checks {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn immutable_pages_seek_exact_boundaries_and_reject_another_page(
            lengths in prop::collection::vec(1u32..100_000, 1..256),
            block in 0u64..100_000, selected in any::<usize>(),
        ) {
            let first = block * ENTRIES;
            let mut page = Page::new(first);
            let mut positions = vec![0];
            for length in lengths {
                positions.push(positions.last().unwrap() + u64::from(length));
            }
            let source_length = *positions.last().unwrap();
            for (index, position) in positions.iter().copied().enumerate() {
                page.append(first + index as u64, position, position == source_length);
            }
            let selected = selected % positions.len();
            prop_assert_eq!(cursor(page.encoded(), first, first + selected as u64, source_length, None).unwrap(),
                (first + selected as u64, positions[selected]));
            prop_assert!(cursor(page.encoded(), first + ENTRIES, first + selected as u64, source_length, None).is_err());
            prop_assert!(cursor(page.encoded(), first, first + positions.len() as u64, source_length, None).is_err());
            prop_assert!(cursor(page.encoded(), first, first, source_length - 1, None).is_err());
            page.bytes[HEADER + 8..HEADER + 16].copy_from_slice(&0u64.to_le_bytes());
            prop_assert!(cursor(page.encoded(), first, first, source_length, None).is_err());
        }
    }
}

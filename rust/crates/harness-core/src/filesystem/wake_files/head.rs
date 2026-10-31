//! A bounded selection from the original authored candidate, never a new head.
use super::*;
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Transaction, WorkspaceError};

/// The two ordered collections contributing to the actor's one deadline.
#[derive(Clone, Copy)]
pub enum WakeIndex {
    /// The next schedule occurrence.
    Schedules,
    /// The next monitor expiration. Unbounded monitors need no alarm entry.
    MonitorDeadlines,
}

impl WakeIndex {
    const fn directory(self) -> &'static str {
        match self {
            Self::Schedules => "schedules",
            Self::MonitorDeadlines => "monitors/bounded",
        }
    }
}

impl WakeFile {
    /// Select and admit only the first indexed document from this transaction.
    /// Staged moves/removals are visible, and the original transaction keeps
    /// the directory/read dependencies for its eventual atomic publication.
    /// An absent collection is empty; an invalid first entry is a typed error,
    /// never skipped in a scan. `maximum_bytes` is the caller's explicit bound.
    /// The transaction must already carry the original volume authority.
    pub fn first<'a, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
        transaction: &'a mut Transaction<A, O>,
        owner: &'a AgentPath,
        index: WakeIndex,
        maximum_bytes: u64,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<Self>>> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            let directory = index.directory();
            let (result, recorded) = transaction
                .list_directory(directory, None, 1, io, io.cancellation())
                .await;
            let selected = io.sdk((
                match result {
                    Ok(page) => Ok(Some(page)),
                    Err(WorkspaceError::NotFound) => Ok(None),
                    Err(error) => Err(error),
                },
                recorded,
            ))?;
            let Some(page) = selected else {
                return Ok(None);
            };
            let Some(entry) = page.0.entries.first() else {
                return Ok(None);
            };
            if entry.kind != acyclic_fs::kernel::FileKind::Regular {
                return Err(io.invalid("wake index entry is not a regular document"));
            }
            // The v1 timed name is exactly 20 digits, '-', a BLAKE3 hex digest
            // and '.json'. This is its schema width, not a collection limit.
            const NAME_BYTES: usize = 20 + 1 + 64 + 5;
            let encoded_width = match entry.name.encoding() {
                acyclic_fs::kernel::NameEncoding::WindowsUtf16Le => 2 * NAME_BYTES,
                _ => NAME_BYTES,
            };
            if entry.name.as_bytes().len() != encoded_width {
                return Err(io.invalid("wake index entry has an invalid timed name"));
            }
            let mut path = [0; "monitors/bounded/".len() + NAME_BYTES];
            let length = directory.len() + 1 + NAME_BYTES;
            io.charge(acyclic_fs::WorkCounters {
                bytes_copied: length as u64,
                items_examined: NAME_BYTES as u64,
                ..Default::default()
            })?;
            path[..directory.len()].copy_from_slice(directory.as_bytes());
            path[directory.len()] = b'/';
            let mut output = path[directory.len() + 1..length].iter_mut();
            for value in entry
                .name
                .unicode_chars()
                .map_err(|_| io.invalid("wake index entry is not Unicode"))?
            {
                let value = value.map_err(|_| io.invalid("wake index entry is not Unicode"))?;
                if !value.is_ascii() {
                    return Err(io.invalid("wake index entry is not an ASCII timed name"));
                }
                let Some(byte) = output.next() else {
                    return Err(io.invalid("wake index entry has an invalid timed name"));
                };
                *byte = value as u8;
            }
            if output.next().is_some() {
                return Err(io.invalid("wake index entry has an invalid timed name"));
            }
            let path = std::str::from_utf8(&path[..length])
                .map_err(|_| io.invalid("wake index entry is not Unicode"))?;
            drop(page);
            let bytes = io.sdk(
                transaction
                    .read(path, maximum_bytes, io, io.cancellation())
                    .await,
            )?;
            Self::decode(path, owner, &bytes, io).map(Some)
        })
    }
}

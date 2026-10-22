//! One durable metadata bundle per discovery pass. Per-record hard links
//! keep source identities stable across catalogs without additional file syncs.
use super::*;
use std::{collections::HashMap, io::Write as _};

#[derive(Default)]
pub(super) struct RetainedBatch {
    records: HashMap<String, RetainedSource>,
}

impl RetainedBatch {
    pub(super) fn retain(
        &mut self,
        journal: &VolumeRef,
        record: RetainedSource,
    ) -> Result<PinnedContextPath> {
        let digest = blake3::hash(&serde_json::to_vec(&record).map_err(storage)?);
        self.records.insert(digest.to_string(), record);
        Ok(PinnedContextPath {
            root: ContextRoot {
                volume: journal.clone(),
                directory: "inputs/skills".into(),
            },
            generation: crate::resources::GenerationRef::new(
                journal.provider(),
                digest.as_bytes(),
                None,
                None,
            )?
            .0,
            path: format!("inputs/skills/{digest}/SKILL.md"),
        })
    }

    pub(super) async fn publish(self, directory: PathBuf) -> Result<()> {
        self.publish_with(directory, |file| file.sync_all().map_err(storage))
            .await
    }

    pub(super) async fn publish_with(
        self,
        directory: PathBuf,
        sync: impl FnMut(&std::fs::File) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }
        acyclic_native_runtime::run_blocking_io(move || self.write(&directory, sync), None)
            .map_err(Error::process_io)?
            .await
            .map_err(Error::process_io)?
    }

    fn write(
        self,
        directory: &Path,
        mut sync: impl FnMut(&std::fs::File) -> Result<()>,
    ) -> Result<()> {
        let temporary = tempfile::NamedTempFile::new_in(directory).map_err(storage)?;
        let mut file = std::io::BufWriter::new(temporary.as_file());
        let digests = self.records.keys().cloned().collect::<Vec<_>>();
        let record = if self.records.len() == 1 {
            self.records
                .into_values()
                .next()
                .ok_or_else(|| Error::Invalid("retained skill batch is empty".into()))?
        } else {
            RetainedSource::Batch(self.records)
        };
        serde_json::to_writer(&mut file, &record).map_err(storage)?;
        file.flush().map_err(storage)?;
        // All records share the same immutable inode. Only this one sync is
        // needed, even when a discovery pass retains hundreds of skills.
        sync(file.get_ref())?;
        for digest in &digests {
            let destination = directory.join(format!("{digest}.json"));
            match std::fs::hard_link(temporary.path(), destination) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(storage(error)),
            }
        }
        Ok(())
    }
}

pub(super) fn read_record(bytes: &[u8], digest: &str) -> Result<RetainedSource> {
    let record = match serde_json::from_slice(bytes).map_err(storage)? {
        RetainedSource::Batch(mut records) => records
            .remove(digest)
            .ok_or_else(|| Error::Conflict("retained skill source is absent".into()))?,
        record => record,
    };
    if blake3::hash(&serde_json::to_vec(&record).map_err(storage)?)
        .to_hex()
        .as_str()
        != digest
    {
        return Err(Error::Conflict("retained skill source changed".into()));
    }
    Ok(record)
}

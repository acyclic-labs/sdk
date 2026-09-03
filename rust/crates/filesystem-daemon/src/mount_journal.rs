//! Durable process-mount intents and restart recovery.

use acyclic_fs::MountId;
use acyclic_fs::recover_native_mount_destination;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const SCHEMA: &str = "acyclic-fsd-mount-intent-v1";
const MAXIMUM_INTENTS: usize = 4_096;
const MAXIMUM_INTENT_BYTES: u64 = 64 * 1024;

#[derive(Debug, Error)]
pub(crate) enum MountJournalError {
    #[error("mount journal I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("mount journal record is malformed")]
    Malformed,
    #[error("mount journal exceeds its bounded intent count")]
    TooManyIntents,
    #[error("native mount restart recovery failed: {0}")]
    Recovery(String),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MountIntent {
    schema: String,
    mount_id: String,
    destination: String,
}

/// One daemon-local durable mount-intent directory.
pub(crate) struct MountJournal {
    root: PathBuf,
}

impl MountJournal {
    pub(crate) fn open(state_root: &Path) -> Result<Self, MountJournalError> {
        let root = state_root.join("fsd-mount-intents");
        std::fs::create_dir_all(&root)?;
        let metadata = std::fs::symlink_metadata(&root)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(MountJournalError::Malformed);
        }
        Ok(Self { root })
    }

    pub(crate) fn recover(&self) -> Result<u32, MountJournalError> {
        let mut records = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            if records.len() >= MAXIMUM_INTENTS {
                return Err(MountJournalError::TooManyIntents);
            }
            let entry = entry?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(MountJournalError::Malformed);
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| MountJournalError::Malformed)?;
            let extension = Path::new(&name).extension();
            if name.starts_with('.') && extension == Some(std::ffi::OsStr::new("tmp")) {
                std::fs::remove_file(path)?;
                continue;
            }
            if extension != Some(std::ffi::OsStr::new("json"))
                || metadata.len() > MAXIMUM_INTENT_BYTES
            {
                return Err(MountJournalError::Malformed);
            }
            records.push((name, path));
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));
        let mut recovered = 0_u32;
        for (name, path) in records {
            let intent = read_intent(&path)?;
            let expected_name = format!("{}.json", intent.mount_id);
            if name != expected_name
                || intent.schema != SCHEMA
                || intent.mount_id.len() != 32
                || !intent
                    .mount_id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(MountJournalError::Malformed);
            }
            let destination = PathBuf::from(&intent.destination);
            if !destination.is_absolute()
                || destination.file_name().is_none()
                || destination.components().any(|component| {
                    matches!(
                        component,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                })
            {
                return Err(MountJournalError::Malformed);
            }
            recover_native_mount_destination(&destination)
                .map_err(|error| MountJournalError::Recovery(error.to_string()))?;
            std::fs::create_dir(&destination)?;
            std::fs::remove_file(path)?;
            recovered = recovered.saturating_add(1);
        }
        sync_directory(&self.root)?;
        Ok(recovered)
    }

    pub(crate) fn admit(
        &self,
        mount_id: MountId,
        destination: &Path,
    ) -> Result<(), MountJournalError> {
        let destination = destination
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or(MountJournalError::Malformed)?;
        let mount_id = hex::encode(mount_id.into_bytes());
        let final_path = self.root.join(format!("{mount_id}.json"));
        let temporary = self.root.join(format!(".{mount_id}.tmp"));
        let encoded = serde_json::to_vec(&MountIntent {
            schema: SCHEMA.to_owned(),
            mount_id,
            destination: destination.to_owned(),
        })
        .map_err(|_| MountJournalError::Malformed)?;
        if encoded.len() as u64 > MAXIMUM_INTENT_BYTES {
            return Err(MountJournalError::Malformed);
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &final_path)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&final_path)?
            .sync_all()?;
        sync_directory(&self.root)?;
        Ok(())
    }

    pub(crate) fn complete(&self, mount_id: MountId) -> Result<(), MountJournalError> {
        let path = self
            .root
            .join(format!("{}.json", hex::encode(mount_id.into_bytes())));
        match std::fs::remove_file(path) {
            Ok(()) => {
                sync_directory(&self.root)?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn read_intent(path: &Path) -> Result<MountIntent, MountJournalError> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() > MAXIMUM_INTENT_BYTES {
        return Err(MountJournalError::Malformed);
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| MountJournalError::Malformed)?;
    let mut encoded = Vec::with_capacity(capacity);
    file.take(MAXIMUM_INTENT_BYTES.saturating_add(1))
        .read_to_end(&mut encoded)?;
    if encoded.len() as u64 > MAXIMUM_INTENT_BYTES {
        return Err(MountJournalError::Malformed);
    }
    serde_json::from_slice(&encoded).map_err(|_| MountJournalError::Malformed)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
    OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ_WRITE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?
        .sync_all()
}

#[cfg(not(any(unix, windows)))]
#[allow(clippy::unnecessary_wraps)]
fn sync_directory(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Result<Self, std::io::Error> {
            let root = std::env::temp_dir().join(format!(
                "acyclic-fsd-mount-journal-{}-{}",
                std::process::id(),
                NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir(&root)?;
            Ok(Self(root))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn admission_is_durable_and_completion_is_idempotent() -> Result<(), Box<dyn std::error::Error>>
    {
        let state = TestRoot::new()?;
        let destination = state.path().join("mount-destination");
        std::fs::create_dir(&destination)?;
        let journal = MountJournal::open(state.path())?;
        let mount_id = MountId::from_bytes([7; 16]);
        journal.admit(mount_id, &destination)?;
        let intent = journal.root.join(format!("{}.json", hex::encode([7; 16])));
        assert!(intent.is_file());
        journal.complete(mount_id)?;
        journal.complete(mount_id)?;
        assert!(!intent.exists());
        assert!(destination.is_dir());
        Ok(())
    }

    #[test]
    fn restart_recovery_detaches_intents_but_preserves_consumer_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let state = TestRoot::new()?;
        let destination = state.path().join("mount-destination");
        std::fs::create_dir(&destination)?;
        let journal = MountJournal::open(state.path())?;
        journal.admit(MountId::from_bytes([8; 16]), &destination)?;
        assert_eq!(journal.recover()?, 1);
        assert!(destination.is_dir());
        assert_eq!(journal.recover()?, 0);
        Ok(())
    }

    #[test]
    fn recovery_removes_torn_temporaries_and_rejects_forged_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let state = TestRoot::new()?;
        let journal = MountJournal::open(state.path())?;
        std::fs::write(journal.root.join(".partial.tmp"), b"partial")?;
        assert_eq!(journal.recover()?, 0);
        assert!(!journal.root.join(".partial.tmp").exists());

        std::fs::write(
            journal.root.join("not-a-mount-id.json"),
            serde_json::to_vec(&MountIntent {
                schema: SCHEMA.to_owned(),
                mount_id: "not-a-mount-id".to_owned(),
                destination: state.path().join("destination").display().to_string(),
            })?,
        )?;
        assert!(matches!(
            journal.recover(),
            Err(MountJournalError::Malformed)
        ));
        assert!(journal.root.join("not-a-mount-id.json").is_file());
        Ok(())
    }
}

//! The version of each path a task's model last read or wrote, so a
//! path-addressed edit publishes only against content the model has seen.
use crate::{OperationId, conversation::FileRef};
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

/// Pins kept per harness; the oldest is forgotten first, and an edit of a
/// forgotten path resolves against the current file as if it was never read.
pub(crate) const MAXIMUM_READ_PINS: usize = 4096;

type Key = (OperationId, String, String);

pub(crate) struct ReadPins {
    capacity: usize,
    pins: Mutex<(u64, HashMap<Key, (u64, FileRef)>)>,
}

impl Default for ReadPins {
    fn default() -> Self {
        Self::new(MAXIMUM_READ_PINS)
    }
}

impl ReadPins {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pins: Mutex::new((0, HashMap::new())),
        }
    }

    fn key(task: OperationId, file: &FileRef) -> Key {
        (task, file.volume().id().to_owned(), file.path().to_owned())
    }

    /// Records `file` as the version `task` last saw at its path.
    pub(crate) fn record(&self, task: OperationId, file: &FileRef) {
        let mut guard = self.pins.lock().unwrap_or_else(PoisonError::into_inner);
        let (next, pins) = &mut *guard;
        let key = Self::key(task, file);
        if !pins.contains_key(&key) && pins.len() >= self.capacity {
            let oldest = pins
                .iter()
                .min_by_key(|(_, (sequence, _))| *sequence)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                pins.remove(&oldest);
            }
        }
        *next += 1;
        pins.insert(key, (*next, file.clone()));
    }

    /// The version `task` last saw at `file`'s path, if it is still pinned.
    pub(crate) fn get(&self, task: OperationId, file: &FileRef) -> Option<FileRef> {
        let guard = self.pins.lock().unwrap_or_else(PoisonError::into_inner);
        guard
            .1
            .get(&Self::key(task, file))
            .map(|(_, file)| file.clone())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
    use proptest::prelude::*;

    fn file(path: &str, bytes: &[u8]) -> FileRef {
        let provider = crate::resources::ProviderRef::new(
            "pins".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )
        .unwrap();
        let volume = VolumeRef::new(
            provider,
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("pins".into()),
            &mut None,
        )
        .unwrap();
        let descriptor = FileDescriptor::from_bytes(bytes, "text/plain", &mut None).unwrap();
        FileRef::new(
            volume,
            path.into(),
            "g".into(),
            descriptor,
            path.replace('/', "-"),
            &mut None,
        )
        .unwrap()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Every lookup returns the latest record for its exact task and path,
        // the table never exceeds its capacity, and it forgets the oldest pin.
        #[test]
        fn pins_keep_the_latest_version_per_task_and_path(
            writes in proptest::collection::vec((0_u8..2, 0_u8..4, any::<u8>()), 1..24),
        ) {
            let pins = ReadPins::new(3);
            let mut model: Vec<(OperationId, FileRef)> = Vec::new();
            for (task, path, content) in writes {
                let task = OperationId::from_bytes([task; 16]);
                let written = file(&format!("dir/{path}.txt"), &[content]);
                pins.record(task, &written);
                model.retain(|(t, f)| (*t, f.path()) != (task, written.path()));
                model.push((task, written));
                if model.len() > 3 {
                    model.remove(0);
                }
                prop_assert!(pins.len() <= 3);
            }
            for task in 0_u8..2 {
                let task = OperationId::from_bytes([task; 16]);
                for path in 0_u8..4 {
                    let probe = file(&format!("dir/{path}.txt"), b"probe");
                    let expected = model
                        .iter()
                        .find(|(t, f)| *t == task && f.path() == probe.path())
                        .map(|(_, f)| f.clone());
                    prop_assert_eq!(pins.get(task, &probe), expected);
                    // Negative control: another task never sees this pin.
                    prop_assert!(pins.get(OperationId::from_bytes([9; 16]), &probe).is_none());
                }
            }
        }
    }
}

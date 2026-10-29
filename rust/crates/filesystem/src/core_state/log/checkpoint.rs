use super::*;

mod durability;
use durability::{CheckpointDurability, CheckpointDurable};

// Checkpoint leaves and their names remain attached to the held family and
// parent until both have crossed the barrier which permits WAL retirement.
struct CheckpointFamily {
    key: RecordKey,
    parent: crate::native_host::HostRoot,
    held: crate::native_host::HostRoot,
}

fn ordinary_checkpoint<T>(
    operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, crate::DemandError>,
) -> Result<T, LocalCoreStateStoreError> {
    operation(&mut crate::native_host::ordinary_work()).map_err(|error| match error {
        crate::DemandError::Io(error) => LocalCoreStateStoreError::from(error),
        error => LocalCoreStateStoreError::LazyWork(error.into()),
    })
}

impl CheckpointFamily {
    fn open(
        key: RecordKey,
        paths: &RecordPaths<'_>,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, LocalCoreStateStoreError> {
        fn directories<W: crate::native_host::NativeHostWork>(
            paths: &RecordPaths<'_>,
            work: &mut W,
        ) -> Result<(crate::native_host::HostRoot, crate::native_host::HostRoot), W::Error>
        {
            let directory = paths.directory();
            let parent = crate::native_host::HostRoot::open(
                directory.parent().ok_or_else(W::invalid_request)?,
                work,
            )?;
            let leaf = Path::new(directory.file_name().ok_or_else(W::invalid_request)?);
            #[cfg(unix)]
            let directory =
                crate::native_host::directory_path(&parent.directory, leaf, true, work)?;
            #[cfg(windows)]
            let directory = crate::native_host::with_wide_path(leaf, work, |leaf, work| {
                crate::native_host::directory_path(&parent.directory, leaf, true, work)
            })?;
            let identity = crate::native_host::directory_identity(&directory, work)?;
            Ok((
                parent,
                crate::native_host::HostRoot::from_parts(directory, identity),
            ))
        }
        let (parent, held) = match work {
            Some(work) => directories(paths, &mut &*work)?,
            None => ordinary_checkpoint(|work| directories(paths, work))?,
        };
        Ok(Self { key, parent, held })
    }

    fn file(
        &self,
        paths: &RecordPaths<'_>,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<File, LocalCoreStateStoreError> {
        let leaf = Path::new(
            paths
                .current
                .file_name()
                .ok_or(LocalCoreStateStoreError::Integrity)?,
        );
        let options = crate::native_host::NativeOpenOptions::ReadWriteCreate;
        let file = match work {
            Some(work) => {
                crate::native_host::open_leaf(&self.held.directory, leaf, &options, &mut &*work)?
            }
            None => ordinary_checkpoint(|work| {
                crate::native_host::open_leaf(&self.held.directory, leaf, &options, work)
            })?,
        };
        if let Some(work) = work {
            work.charge(crate::WorkCounters {
                backend_write_operations: 1,
                ..Default::default()
            })?;
        }
        acyclic_native_runtime::truncate_file(&file, 0)?;
        Ok(file)
    }

    fn validate(
        &self,
        paths: &RecordPaths<'_>,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        fn named<W: crate::native_host::NativeHostWork>(
            paths: &RecordPaths<'_>,
            work: &mut W,
        ) -> Result<(crate::native_host::HostRoot, crate::native_host::HostRoot), W::Error>
        {
            let directory = paths.directory();
            let parent = crate::native_host::HostRoot::open(
                directory.parent().ok_or_else(W::invalid_request)?,
                work,
            )?;
            let family = parent.open_directory(
                Path::new(directory.file_name().ok_or_else(W::invalid_request)?),
                work,
            )?;
            Ok((parent, family))
        }
        let (parent, family) = match work {
            Some(work) => named(paths, &mut &*work)?,
            None => ordinary_checkpoint(|work| named(paths, work))?,
        };
        if parent.identity() != self.parent.identity() || family.identity() != self.held.identity()
        {
            return Err(LocalCoreStateStoreError::Integrity);
        }
        Ok(())
    }

    fn sync(
        &self,
        root: &Namespace,
        durability: &CheckpointDurability<'_>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let paths = self.key.paths(root)?;
        let work = paths
            .allocation
            .as_ref()
            .map(|allocation| allocation.work());
        self.validate(&paths, work)?;
        durability.directory(&self.held.directory, work)?;
        Ok(())
    }
}

impl CoreLog {
    /// Installs only after-images whose storage was admitted before append.
    pub(in crate::core_state) fn apply(
        &mut self,
        change: PreparedChange,
        installation: Option<&crate::NativeWorkReservation>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let mut residents = self
            .residents
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let mut examined = 0;
        let result = (|| {
            for record in change.into_records() {
                let key = match &record {
                    PreparedResident::Ordinary(record) => record.key,
                    PreparedResident::Native(record) => record.key(),
                };
                if residents.install(record, Some(self.epoch), &mut examined)? {
                    if self.dirty.len() == self.dirty.capacity() {
                        return Err(LocalCoreStateStoreError::Integrity);
                    }
                    self.dirty.push(key);
                }
            }
            Ok(())
        })();
        if let Some(installation) = installation {
            installation.record_completed(crate::WorkCounters {
                items_examined: examined,
                ..crate::WorkCounters::UNCHARGED
            })?;
        }
        result
    }

    pub(in crate::core_state) fn checkpoint(
        &mut self,
        root: &Namespace,
    ) -> Result<(), LocalCoreStateStoreError> {
        let work = root
            ._allocation
            .as_ref()
            .map(|allocation| allocation.work());
        if let Some(work) = work {
            self.adopt_native(work)?;
        }
        self.flush_physical(root)?;
        let lease = self.lease(work)?;
        let durability = CheckpointDurability::new(lease.file(), work)?;
        // The durable WAL owns recovery until every record file and every
        // changed family directory has crossed its barrier. A directory flush
        // covers all names in that family, including earlier record writes.
        let mut families: [Option<CheckpointFamily>; 6] = std::array::from_fn(|_| None);
        for key in &self.dirty {
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let paths = key.paths(root)?;
            let family = match key {
                RecordKey::Context(_) => 0,
                RecordKey::Children(_) => 1,
                RecordKey::ChildCount(_) => 2,
                RecordKey::ChildIndex => 3,
                RecordKey::Lineage(_) => 4,
                RecordKey::LazyWorkspace(_) => 5,
            };
            if families[family].is_none() {
                families[family] = Some(CheckpointFamily::open(*key, &paths, work)?);
            }
            let held = families[family]
                .as_ref()
                .ok_or(LocalCoreStateStoreError::Integrity)?;
            let file = held.file(&paths, work)?;
            let mut writer = MaterializationWriter {
                file,
                work,
                encoded: false,
            };
            let residents = self.residents();
            let value = if let Some(index) = residents.position(*key, work)? {
                residents
                    .get(index)
                    .ok_or(LocalCoreStateStoreError::Integrity)?
                    .value
                    .as_ref()
            } else {
                None
            };
            RecordValue::write_checkpoint(value, &mut writer, work)?;
            crash_point("record written", self.flushed);
            durability.file(&writer.file, work)?;
            crash_point("record flushed", self.flushed);
        }
        for family in families.iter().flatten() {
            family.sync(root, &durability)?;
        }
        // Every family is a child of this namespace. Their new directory
        // entries share one held parent and one namespace barrier.
        if let Some(first) = families.iter().flatten().next() {
            if families
                .iter()
                .flatten()
                .any(|family| family.parent.identity() != first.parent.identity())
            {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            durability.directory(&first.parent.directory, work)?;
        }
        self.reset_checkpoint_wal(durability.finish(work)?, work)
    }

    // A successful device drain is the only constructor of this permission.
    // The token is consumed before any operation can retire the WAL prefix.
    fn reset_checkpoint_wal(
        &mut self,
        _durable: CheckpointDurable,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let mut reset = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })
            })
            .transpose()?;
        // Every early return after reset starts retains the broken fence.
        self.broken = true;
        // Appends name their offsets, so emptying the log leaves no cursor
        // to rewind.
        let truncated = self
            .lease(work)
            .and_then(|mut lease| lease.discard_contents());
        let truncate_recorded = reset
            .as_mut()
            .map(|reset| {
                reset.record_completed(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })
            })
            .transpose();
        truncated?;
        self.length = 0;
        self.flushed = 0;
        crash_point("log truncated", self.flushed);
        self.epoch = crate::identity::fresh_time_uuid();
        self.sequence = 0;
        self.dirty.clear();
        truncate_recorded?;
        self.broken = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_checkpoint_family_rejects_a_replaced_named_parent() {
        acyclic_native_runtime::initialize_native_io_services().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().unwrap();
        let key = RecordKey::Context(WorkspaceContextId::from_bytes([0x72; 16]));
        let paths = key.paths(&namespace).unwrap();
        let family = CheckpointFamily::open(key, &paths, None).unwrap();
        let anchor = File::create(directory.path().join("drain-anchor")).unwrap();
        let durability = CheckpointDurability::new(&anchor, None).unwrap();
        std::fs::rename(paths.directory(), directory.path().join("detached-family")).unwrap();
        std::fs::create_dir(paths.directory()).unwrap();
        assert!(
            matches!(
                family.sync(&namespace, &durability),
                Err(LocalCoreStateStoreError::Integrity)
            ),
            "ordinary checkpoint accepted a replaced named family"
        );
    }

    #[test]
    fn checkpoint_family_rejects_a_replaced_named_parent() {
        acyclic_native_runtime::initialize_native_io_services().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let cancellation = crate::CancellationToken::new();
        let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).unwrap();
        for value in budget.as_object_mut().unwrap().values_mut() {
            *value = serde_json::Value::from(100_000_000_u64);
        }
        let budget: crate::WorkBudget = serde_json::from_value(budget).unwrap();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let store = LocalCoreStateStore::new(directory.path()).materialization(work.clone());
        let namespace = store.namespace().unwrap();
        let key = RecordKey::Context(WorkspaceContextId::from_bytes([0x71; 16]));
        let paths = key.paths(&namespace).unwrap();
        let family = CheckpointFamily::open(key, &paths, Some(&work)).unwrap();
        let anchor = File::create(directory.path().join("drain-anchor")).unwrap();
        let durability = CheckpointDurability::new(&anchor, Some(&work)).unwrap();
        std::fs::rename(paths.directory(), directory.path().join("detached-family")).unwrap();
        std::fs::create_dir(paths.directory()).unwrap();
        assert!(
            matches!(
                family.sync(&namespace, &durability),
                Err(LocalCoreStateStoreError::Integrity)
            ),
            "checkpoint accepted a replaced named family"
        );
        work.receipt().verify(budget).unwrap();
    }
}

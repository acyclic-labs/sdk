use super::*;

// A checkpoint has a closed set of six record families. Hold each native
// parent once while writing its leaves; the WAL remains authoritative until
// every file and family has crossed its barrier.
struct CheckpointFamily {
    key: RecordKey,
    native: Option<crate::native_host::HostRoot>,
}

impl CheckpointFamily {
    fn open(
        key: RecordKey,
        paths: &RecordPaths<'_>,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, LocalCoreStateStoreError> {
        let native = if let Some(work) = work {
            crate::native_host::create_directories(paths.directory(), &mut &*work)?;
            Some(crate::native_host::HostRoot::open(
                paths.directory(),
                &mut &*work,
            )?)
        } else {
            std::fs::create_dir_all(paths.directory())?;
            None
        };
        Ok(Self { key, native })
    }

    fn sync(&self, root: &Namespace) -> Result<(), LocalCoreStateStoreError> {
        let paths = self.key.paths(root)?;
        if let Some(held) = &self.native {
            let work = paths
                .allocation
                .as_ref()
                .ok_or(LocalCoreStateStoreError::Integrity)?
                .work();
            // A held descriptor pins an inode, not its name. Reopen and compare
            // the named family before discarding the WAL; flushing a detached
            // or replaced directory must never make the checkpoint succeed.
            let named = crate::native_host::HostRoot::open(paths.directory(), &mut &*work)?;
            if named.identity() != held.identity() {
                return Err(LocalCoreStateStoreError::Integrity);
            }
            crate::native_host::sync_directory_held(&named.directory, &mut &*work)?;
        } else {
            paths.sync()?;
        }
        Ok(())
    }
}

impl CoreLog {
    /// Makes a committed change resident; its files are written back later.
    pub(in crate::core_state) fn apply(
        &mut self,
        change: Change,
        installation: Option<&crate::NativeWorkReservation>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let mut residents = self
            .residents
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let mut examined = 0;
        for record in change.0 {
            let key = record.key;
            let index = residents.0.iter().position(|resident| {
                examined += 1;
                resident.key == key
            });
            let dirty = self.dirty.iter().any(|candidate| {
                examined += 1;
                *candidate == key
            });
            if let Some(index) = index {
                *residents
                    .0
                    .get_mut(index)
                    .ok_or(LocalCoreStateStoreError::Integrity)? = record;
            } else {
                residents.0.push(record);
            }
            if !dirty {
                self.dirty.push(key);
            }
        }
        if let Some(installation) = installation {
            installation.record_completed(crate::WorkCounters {
                items_examined: examined,
                ..Default::default()
            })?;
        }
        Ok(())
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
        self.flush(root)?;
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
            let file = if let Some(work) = work {
                let parent = held
                    .native
                    .as_ref()
                    .ok_or(LocalCoreStateStoreError::Integrity)?;
                let leaf = paths
                    .current
                    .file_name()
                    .ok_or(LocalCoreStateStoreError::Integrity)?;
                let file = crate::native_host::open_leaf(
                    &parent.directory,
                    Path::new(leaf),
                    &crate::native_host::NativeOpenOptions::ReadWriteCreate,
                    &mut &*work,
                )?;
                work.charge(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })?;
                acyclic_native_runtime::truncate_file(&file, 0)?;
                file
            } else {
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&paths.current)?
            };
            let mut writer = MaterializationWriter {
                file,
                work,
                encoded: false,
            };
            let residents = self.residents();
            let value = if let Some(index) = residents.position(*key, work)? {
                residents
                    .0
                    .get(index)
                    .ok_or(LocalCoreStateStoreError::Integrity)?
                    .value
                    .as_ref()
            } else {
                None
            };
            RecordValue::write_checkpoint(value, &mut writer, work)?;
            crash_point("record written", self.flushed);
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    durability_operations: 1,
                    ..Default::default()
                })?;
            }
            acyclic_native_runtime::flush_file_all(&writer.file)?;
            crash_point("record flushed", self.flushed);
        }
        for family in families.into_iter().flatten() {
            family.sync(root)?;
        }
        let mut reset = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
                    backend_read_operations: 1,
                    ..Default::default()
                })
            })
            .transpose()?;
        let truncated = acyclic_native_runtime::truncate_file(&self.file, 0);
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
        let sought = self.file.seek(SeekFrom::Start(0));
        let seek_recorded = reset
            .as_mut()
            .map(|reset| {
                reset.record_completed(crate::WorkCounters {
                    backend_read_operations: 1,
                    ..Default::default()
                })
            })
            .transpose();
        if let Err(error) = sought {
            self.broken = true;
            return Err(error.into());
        }
        self.length = 0;
        self.flushed = 0;
        crash_point("log truncated", self.flushed);
        self.epoch = crate::identity::fresh_time_uuid();
        self.sequence = 0;
        self.dirty.clear();
        truncate_recorded?;
        seek_recorded?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        std::fs::rename(paths.directory(), directory.path().join("detached-family")).unwrap();
        std::fs::create_dir(paths.directory()).unwrap();
        assert!(
            matches!(
                family.sync(&namespace),
                Err(LocalCoreStateStoreError::Integrity)
            ),
            "checkpoint accepted a replaced named family"
        );
        work.receipt().verify(budget).unwrap();
    }
}

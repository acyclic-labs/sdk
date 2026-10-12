use super::*;

impl CoreLog {
    /// Makes a committed change resident; its files are written back later.
    pub(in crate::core_state) fn apply(&mut self, change: Change) {
        let mut residents = self
            .residents
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for record in change.0 {
            let key = record.key;
            residents.insert(key, record.value);
            if !self.dirty.contains(&key) {
                self.dirty.push(key);
            }
        }
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
        for key in &self.dirty {
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let paths = key.paths(root)?;
            let file = match work {
                Some(work) => {
                    crate::native_host::create_directories(&paths.directory, &mut &*work)?;
                    let file = materialization_open_options(
                        &paths.current,
                        &crate::native_host::NativeOpenOptions::ReadWriteCreate,
                        work,
                    )?;
                    work.charge(crate::WorkCounters {
                        backend_write_operations: 1,
                        ..Default::default()
                    })?;
                    file.set_len(0)?;
                    file
                }
                None => {
                    std::fs::create_dir_all(&paths.directory)?;
                    OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&paths.current)?
                }
            };
            let mut writer = MaterializationWriter {
                file,
                work,
                encoded: true,
            };
            let residents = self.residents();
            let mut value = None;
            for record in &residents.0 {
                if let Some(work) = work {
                    work.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
                if record.key == *key {
                    value = record.value.as_ref();
                    break;
                }
            }
            RecordValue::write_bare(value, &mut writer).map_err(measured_json_error)?;
            crash_point("record written", self.flushed);
            if let Some(work) = work {
                work.charge(crate::WorkCounters {
                    durability_operations: 1,
                    ..Default::default()
                })?;
            }
            writer.file.sync_all()?;
            crash_point("record flushed", self.flushed);
            paths.sync()?;
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
        let truncated = self.file.set_len(0);
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
        self.epoch = Uuid::now_v7();
        self.sequence = 0;
        self.dirty.clear();
        truncate_recorded?;
        seek_recorded?;
        Ok(())
    }
}

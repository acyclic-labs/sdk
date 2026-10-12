use super::*;

impl CoreLog {
    /// Commits the after-images once their WAL frame is durable.
    pub(in crate::core_state) fn commit(
        &mut self,
        root: &Namespace,
        change: Change,
    ) -> Result<(), LocalCoreStateStoreError> {
        let work = root
            ._allocation
            .as_ref()
            .map(|allocation| allocation.work());
        if let Some(work) = work {
            self.adopt_native(work)?;
        }
        let next_sequence = self.sequence.checked_add(1).ok_or(
            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        let installation = if let Some(work) = work {
            let mut residents = self.residents();
            let count = change.0.len();
            let resident_count = residents.0.len();
            let needed = resident_count.checked_add(count).ok_or(
                crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
            )?;
            let Residents(values, slots) = &mut *residents;
            reserve_slots(values, needed, work, slots)?;
            drop(residents);
            let needed = self.dirty.len().checked_add(count).ok_or(
                crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
            )?;
            reserve_slots(&mut self.dirty, needed, work, &mut self.slots)?;
            let scans = resident_count
                .checked_add(self.dirty.len())
                .and_then(|length| {
                    count
                        .checked_mul(2)
                        .and_then(|growth| length.checked_add(growth))
                })
                .and_then(|length| length.checked_mul(count))
                .ok_or(crate::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ))?;
            Some(work.reserve_work(crate::WorkCounters {
                items_examined: scans as u64,
                ..Default::default()
            })?)
        } else {
            None
        };
        let mut buffer = LogBuffer {
            bytes: Vec::new(),
            allocation: work
                .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
                .transpose()?,
            failure: None,
        };
        if let Some(work) = work {
            reserve(
                &mut buffer.bytes,
                65,
                work,
                buffer.allocation.as_mut().expect("frame buffer"),
            )?;
            work.charge(crate::WorkCounters {
                bytes_copied: 65,
                ..Default::default()
            })?;
        }
        buffer.bytes.extend_from_slice(&[b' '; 65]);
        let encoded = serde_json::to_writer(
            &mut buffer,
            &LogFrame {
                epoch: self.epoch,
                sequence: self.sequence,
                change: &change,
            },
        );
        if let Some(error) = buffer.failure.take() {
            return Err(error);
        }
        encoded?;
        if let Some(work) = work {
            work.charge(crate::WorkCounters {
                bytes_hashed: (buffer.bytes.len() - 65) as u64,
                ..Default::default()
            })?;
        }
        let checksum = blake3::hash(&buffer.bytes[65..]).to_hex();
        if let Some(work) = work {
            let needed = buffer.bytes.len() + 1;
            reserve(
                &mut buffer.bytes,
                needed,
                work,
                buffer.allocation.as_mut().expect("frame buffer"),
            )?;
            work.charge(crate::WorkCounters {
                bytes_copied: 65,
                ..Default::default()
            })?;
        }
        buffer.bytes[..64].copy_from_slice(checksum.as_bytes());
        buffer.bytes.push(b'\n');
        let deferred = root.durability == Durability::Deferred;
        let new_length = self.length.checked_add(buffer.bytes.len() as u64).ok_or(
            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        // Repair permission survives append failure, quota refusal and cancellation.
        let mut repair = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
                    backend_read_operations: 1,
                    durability_operations: 1,
                    ..Default::default()
                })
            })
            .transpose()?;
        let mut flush = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    durability_operations: u64::from(!deferred),
                    ..Default::default()
                })
            })
            .transpose()?;
        crash_point("before frame", self.flushed);
        let append = (|| {
            let mut position = 0;
            while position < buffer.bytes.len() {
                let mut credit = work
                    .map(|work| {
                        work.reserve_work(crate::WorkCounters {
                            backend_write_operations: 1,
                            object_bytes_written: (buffer.bytes.len() - position) as u64,
                            ..Default::default()
                        })
                    })
                    .transpose()?;
                let result = self.file.write(&buffer.bytes[position..]);
                let count = result.as_ref().copied().unwrap_or(0);
                let recorded = credit
                    .as_mut()
                    .map(|credit| {
                        credit.record_completed(crate::WorkCounters {
                            backend_write_operations: 1,
                            object_bytes_written: count as u64,
                            ..Default::default()
                        })
                    })
                    .transpose();
                let count = result?;
                recorded?;
                if count == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
                }
                position += count;
            }
            crash_point("frame written", self.flushed);
            if !deferred {
                let synced = self.file.sync_data();
                let recorded = flush
                    .as_mut()
                    .map(|flush| {
                        flush.record_completed(crate::WorkCounters {
                            durability_operations: 1,
                            ..Default::default()
                        })
                    })
                    .transpose();
                synced?;
                drop(recorded);
            }
            Ok::<_, LocalCoreStateStoreError>(())
        })();
        if let Err(error) = append {
            let truncated = self.file.set_len(self.length);
            drop(repair.as_mut().map(|repair| {
                repair.record_completed(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })
            }));
            let repaired = truncated
                .and_then(|()| {
                    let sought = self.file.seek(SeekFrom::Start(self.length));
                    drop(repair.as_mut().map(|repair| {
                        repair.record_completed(crate::WorkCounters {
                            backend_read_operations: 1,
                            ..Default::default()
                        })
                    }));
                    sought
                })
                .and_then(|_| {
                    let synced = self.file.sync_data();
                    drop(repair.as_mut().map(|repair| {
                        repair.record_completed(crate::WorkCounters {
                            durability_operations: 1,
                            ..Default::default()
                        })
                    }));
                    synced
                });
            self.broken = repaired.is_err();
            return Err(error);
        }
        self.length = new_length;
        if !deferred {
            self.flushed = self.length;
            crash_point("frame flushed", self.flushed);
        }
        self.sequence = next_sequence;
        self.apply(change, installation.as_ref()).map_err(|error| {
            self.broken = true;
            error
        })?;
        Ok(())
    }

    /// Makes every appended frame durable.
    pub(in crate::core_state) fn flush(
        &mut self,
        root: &Namespace,
    ) -> Result<(), LocalCoreStateStoreError> {
        if self.flushed < self.length {
            if let Some(allocation) = &root._allocation {
                allocation.work().charge(crate::WorkCounters {
                    durability_operations: 1,
                    ..Default::default()
                })?;
            }
            self.file.sync_data()?;
            self.flushed = self.length;
            crash_point("frames flushed", self.flushed);
        }
        Ok(())
    }
}

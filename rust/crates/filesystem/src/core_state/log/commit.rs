use super::*;

impl CoreLog {
    /// Commits the after-images once their WAL frame is durable.
    pub(in crate::core_state) fn commit(
        &mut self,
        root: &Namespace,
        mut change: Change,
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
        let mut install_allocation = work
            .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
            .transpose()?;
        let mut installs: Vec<(usize, bool)> = Vec::new();
        if let Some(work) = work {
            reserve(
                &mut installs,
                change.0.len(),
                work,
                install_allocation.as_mut().expect("install buffer"),
            )?;
            {
                let mut residents = self.residents();
                let needed = residents.0.len().checked_add(change.0.len()).ok_or(
                    crate::materializer::NativeTreeMaterializationError::Work(
                        crate::WorkError::Overflow,
                    ),
                )?;
                let Residents(values, slots) = &mut *residents;
                reserve_slots(values, needed, work, slots)?;
                let mut next = residents.0.len();
                for (ordinal, record) in change.0.iter().enumerate() {
                    let mut resident = None;
                    for (index, preceding) in residents.0.iter().enumerate() {
                        work.charge(crate::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        })?;
                        if preceding.key == record.key {
                            resident = Some(index);
                            break;
                        }
                    }
                    let mut prior = None;
                    for (index, preceding) in change.0[..ordinal].iter().enumerate() {
                        work.charge(crate::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        })?;
                        if preceding.key == record.key {
                            prior = Some(installs[index].0);
                            break;
                        }
                    }
                    let resident = resident.or(prior).unwrap_or_else(|| {
                        let index = next;
                        next += 1;
                        index
                    });
                    let mut dirty = prior.is_none();
                    for key in &self.dirty {
                        work.charge(crate::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        })?;
                        if *key == record.key {
                            dirty = false;
                            break;
                        }
                    }
                    installs.push((resident, dirty));
                }
            }
            let needed = self
                .dirty
                .len()
                .checked_add(installs.iter().filter(|(_, dirty)| *dirty).count())
                .ok_or(crate::materializer::NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ))?;
            reserve_slots(&mut self.dirty, needed, work, &mut self.slots)?;
        }
        let mut buffer = LogBuffer {
            bytes: Vec::new(),
            allocation: work
                .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
                .transpose()?,
            work,
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
        if work.is_some() {
            for (record, (resident, dirty)) in change.0.drain(..).zip(installs) {
                let key = record.key;
                let mut residents = self.residents();
                if resident < residents.0.len() {
                    residents.0[resident] = record;
                } else {
                    residents.0.push(record);
                }
                drop(residents);
                if dirty {
                    self.dirty.push(key);
                }
            }
        } else {
            self.apply(change);
        }
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

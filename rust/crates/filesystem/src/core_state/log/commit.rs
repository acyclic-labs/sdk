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
        let change = PreparedChange::new(change, work)?;
        let installation = self.admit_installation(change.change(), work)?;
        let buffer = Self::encode_frame(self.epoch, self.sequence, change.change(), work)?;
        let deferred = root.durability == Durability::Deferred
            || matches!(root.admission, Admission::Owned(_));
        let new_length = self.length.checked_add(buffer.bytes.len() as u64).ok_or(
            crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        // Repair permission survives append failure, quota refusal and cancellation.
        let mut repair = work
            .map(|work| {
                work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
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
        root.reserve_barrier()?;
        crash_point("before frame", self.flushed);
        let lease = self.lease(work)?;
        let append = self.append_frame(&lease, &buffer.bytes, work, &mut flush, deferred);
        if let Err(error) = append {
            let length = self.length;
            let truncated = lease.write(|file| acyclic_native_runtime::truncate_file(file, length));
            drop(repair.as_mut().map(|repair| {
                repair.record_completed(crate::WorkCounters {
                    backend_write_operations: 1,
                    ..Default::default()
                })
            }));
            let repaired = truncated.and_then(|()| {
                let synced = lease.flush(|file| file.flush_data());
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
        if let Admission::Owned(owner) = &root.admission {
            owner.pending.store(true, Ordering::Release);
        }
        self.apply(change, installation.as_ref()).inspect_err(|_| {
            self.broken = true;
        })?;
        Ok(())
    }

    pub(super) fn admit_installation(
        &mut self,
        change: &Change,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Option<crate::NativeWorkReservation>, LocalCoreStateStoreError> {
        let count = change.0.len();
        let dirty = self.residents().admit_change(change, self.epoch, work)?;
        let needed = self.dirty.len().checked_add(dirty).ok_or(
            crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
        )?;
        reserve_retained_slots(&mut self.dirty, needed, work, &mut self.slots)?;
        work.map(|work| {
            work.reserve_work(Residents::installation_work(count)?)
                .map_err(LocalCoreStateStoreError::from)
        })
        .transpose()
    }

    pub(in crate::core_state) fn encode_frame(
        epoch: Uuid,
        sequence: u64,
        change: &Change,
        work: Option<&crate::NativeMaterializationWork>,
    ) -> Result<LogBuffer, LocalCoreStateStoreError> {
        let mut buffer = LogBuffer {
            bytes: Vec::new(),
            allocation: work
                .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
                .transpose()?,
        };
        buffer.push_frame(
            FRAME_FORMAT,
            &LogFrame {
                epoch,
                sequence,
                change,
            },
        )?;
        Ok(buffer)
    }

    fn append_frame(
        &self,
        lease: &acyclic_native_runtime::FileLease,
        buffer: &[u8],
        work: Option<&crate::NativeMaterializationWork>,
        flush: &mut Option<crate::NativeWorkReservation>,
        deferred: bool,
    ) -> Result<(), LocalCoreStateStoreError> {
        write_append_batch(self.length, buffer, work, |offset, bytes| {
            lease.write(|file| acyclic_native_runtime::WriteAt::new(file, offset).write(bytes))
        })?;
        crash_point("frame written", self.flushed);
        if !deferred {
            let synced = lease.flush(|file| file.flush_data());
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
    }

    /// Makes every appended frame durable.
    pub(in crate::core_state) fn flush(
        &mut self,
        root: &Namespace,
    ) -> Result<(), LocalCoreStateStoreError> {
        // Owned callers finish through the owner's shared barrier after they
        // release the append mutex. Checkpoints call flush_physical directly.
        if matches!(root.admission, Admission::Owned(_)) {
            return Ok(());
        }
        self.flush_physical(root)
    }

    pub(in crate::core_state) fn flush_physical(
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
            if let Err(error) = self
                .lease(
                    root._allocation
                        .as_ref()
                        .map(|allocation| allocation.work()),
                )
                .and_then(|lease| lease.flush(|file| file.flush_data()))
            {
                // Checkpoints cross this barrier directly, before the ordinary
                // acknowledgement path. An uncertain flush must fence every
                // later reader, including readers of resident after-images.
                self.broken = true;
                if let Admission::Owned(owner) = &root.admission {
                    owner.failed.store(true, Ordering::Release);
                }
                return Err(error.into());
            }
            self.flushed = self.length;
            crash_point("frames flushed", self.flushed);
        }
        Ok(())
    }
}

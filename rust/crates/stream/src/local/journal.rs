impl Journal {
    fn open(
        root: &Path,
        limits: LocalStreamLimits,
        recovered: &mpsc::Sender<Recovered>,
        span: &tracing::Span,
    ) -> Result<Self, LocalStreamError> {
        let claim = lock_root(root, limits)?;
        let epoch = match read_snapshot(root, limits)? {
            Some(snapshot) => {
                blocking_send(
                    recovered,
                    Recovered::State {
                        encoded: snapshot.state,
                    },
                )
                .map_err(|_| LocalStreamError::Executor)?;
                snapshot.epoch
            }
            None => 0,
        };
        let path = root.join(JOURNAL_FILE);
        // Idle, the journal holds no descriptor: the process reopens it
        // when next used.
        let file = acyclic_native_runtime::CachedFile::new(
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)?,
            &path,
            true,
            true,
        )?;
        let mut journal = Self {
            _ownership_anchor: None,
            claim,
            file,
            epoch,
            operations: 0,
            bytes: 0,
            limits,
            appended: 0,
            required: 0,
            synced: 0,
            syncing: false,
            root: root.to_path_buf(),
            torn_tail: None,
            #[cfg(feature = "test-support")]
            flushed: 0,
        };
        let header_bytes =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        // Recovery reads by position, so it holds one descriptor throughout.
        let lease = journal.file.lease()?;
        let length = lease.file().metadata()?.len();
        let mut header = vec![0_u8; HEADER_BYTES];
        let found = if length < header_bytes {
            None
        } else {
            lease
                .file()
                .read_exact(&mut header)
                .map_err(|_| LocalStreamError::Corrupt)?;
            Some(decode_header(&header, limits))
        };
        match found {
            // Every frame follows a whole header, so a journal holding less
            // than a valid header never acknowledged one: its creation or
            // restart was torn, and it starts again.
            None => {
                lease.file().seek(SeekFrom::Start(0))?;
                let mut partial = Vec::new();
                lease.file().read_to_end(&mut partial)?;
                if !encode_header(limits, epoch)?.starts_with(&partial) {
                    return Err(LocalStreamError::Corrupt);
                }
                if !validate_recovery(recovered)? {
                    return Ok(journal);
                }
                journal.restart(epoch, None)?;
                sync_directory(&path, limits.durability)?;
                return Ok(journal);
            }
            Some(Some(found)) if found == epoch => {}
            // The snapshot for the next epoch landed before the journal
            // restarted, so it already holds every frame here.
            Some(Some(found)) if found.checked_add(1) == Some(epoch) => {
                if !validate_recovery(recovered)? {
                    return Ok(journal);
                }
                journal.restart(epoch, None)?;
                return Ok(journal);
            }
            _ => return Err(LocalStreamError::Corrupt),
        }
        journal.replay(lease.file(), length, recovered, span)?;
        if !validate_recovery(recovered)? {
            return Ok(journal);
        }
        journal.repair(&lease)?;
        Ok(journal)
    }

    /// Cuts a torn final frame from a replayed journal, and makes every
    /// replayed frame durable: a crashed process may have written frames it
    /// never flushed, and they are made durable before anything observes
    /// them.
    fn repair(&self, lease: &acyclic_native_runtime::FileLease) -> Result<(), LocalStreamError> {
        let durability = self.limits.durability;
        if let Some(frame_start) = self.torn_tail {
            match lease.write(|file| {
                acyclic_native_runtime::recover_log_tail(
                    file,
                    frame_start,
                    maximum_frame_bytes(),
                    native_durability(durability),
                    frame_is_valid,
                )
            })? {
                acyclic_native_runtime::LogTail::Torn => {}
                acyclic_native_runtime::LogTail::Corrupt => return Err(LocalStreamError::Corrupt),
            }
        }
        lease.flush(|file| sync_file_data(file, durability))?;
        Ok(())
    }

    /// Hands every whole frame of a journal `length` long to `recovered`,
    fn replay(
        &mut self,
        file: &File,
        length: u64,
        recovered: &mpsc::Sender<Recovered>,
        span: &tracing::Span,
    ) -> Result<(), LocalStreamError> {
        let limits = self.limits;
        let mut operations = 0_u64;
        let mut valid_length =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        let total_length = length;
        while valid_length < total_length {
            let frame_start = valid_length;
            let Some(frame) = read_frame(file, total_length - frame_start)? else {
                // Repair is deferred until semantic replay has accepted every prefix frame.
                self.torn_tail = Some(frame_start);
                break;
            };
            // An intact frame was written whole, so one that does not decode
            // may be a committed command: fail closed.
            let (committed_at_millis, command) = frame
                .command
                .split_first_chunk::<8>()
                .ok_or(LocalStreamError::Corrupt)?;
            let command = decode_command(command).map_err(|_| LocalStreamError::Corrupt)?;
            operations = operations.checked_add(1).ok_or(LocalStreamError::Corrupt)?;
            if operations > limits.journal_operations {
                return Err(LocalStreamError::Corrupt);
            }
            blocking_send(
                recovered,
                Recovered::Command {
                    committed_at_millis: u64::from_le_bytes(*committed_at_millis),
                    command,
                },
            )
            .map_err(|_| LocalStreamError::Executor)?;
            valid_length = valid_length
                .checked_add(frame_bytes(&frame.length))
                .ok_or(LocalStreamError::Corrupt)?;
        }
        if valid_length > limits.journal_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        span.record("frames", operations);
        span.record("bytes", valid_length);
        self.operations = operations;
        self.bytes = valid_length;
        Ok(())
    }

    /// Empties the journal to follow the snapshot of `epoch`.
    fn restart(
        &mut self,
        epoch: u64,
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        let header = encode_header(self.limits, epoch)?;
        let lease = self.file.lease()?;
        lease.write(|file| {
            acyclic_native_runtime::truncate_file(file, 0)?;
            acyclic_native_runtime::write_file_all(
                &mut acyclic_native_runtime::WriteAt::new(file, 0),
                &header,
            )
        })?;
        let durability = self.limits.durability;
        match work {
            Some(work) => lease.flush(|file| work.sync(|| sync_file(file, durability)))?,
            None => lease.flush(|file| sync_file(file, durability))?,
        }
        self.epoch = epoch;
        self.operations = 0;
        self.bytes = u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        // The header was synchronized after the snapshot holding every frame.
        self.synced = self.appended;
        #[cfg(feature = "test-support")]
        {
            self.flushed = self.bytes;
        }
        Ok(())
    }

    fn compaction_due(&self) -> bool {
        self.operations > 0
            && (self.operations >= self.limits.journal_operations / 2
                || self.bytes >= self.limits.journal_bytes / 2)
    }

    /// Makes `state`, which holds every frame so far, the snapshot the
    fn compact(
        &mut self,
        state: &[u8],
        work: Option<&Arc<crate::memory::Work>>,
    ) -> Result<(), LocalStreamError> {
        let epoch = self.epoch.checked_add(1).ok_or(LocalStreamError::Corrupt)?;
        let temporary = self.root.join(SNAPSHOT_TEMPORARY);
        let snapshot = self.root.join(SNAPSHOT_FILE);
        {
            let mut file = File::create(&temporary)?;
            acyclic_native_runtime::write_file_all(
                &mut file,
                &encode_snapshot(self.limits, epoch, state)?,
            )?;
            match work {
                Some(work) => work.sync(|| sync_file(&file, self.limits.durability))?,
                None => sync_file(&file, self.limits.durability)?,
            }
        }
        acyclic_native_runtime::rename_file(&temporary, &snapshot)?;
        match work {
            Some(work) => work.sync(|| {
                acyclic_native_runtime::sync_parent(
                    &snapshot,
                    native_durability(self.limits.durability),
                )
            })?,
            None => sync_directory(&snapshot, self.limits.durability)?,
        }
        self.restart(epoch, work)
    }

    fn admit(&self, frame: &PreparedFrame) -> Result<(), LocalStreamError> {
        if self.operations >= self.limits.journal_operations
            || self
                .bytes
                .checked_add(frame.encoded.len() as u64)
                .is_none_or(|value| value > self.limits.journal_bytes)
        {
            return Err(LocalStreamError::InvalidLimits);
        }
        Ok(())
    }

    /// Writes `frame` after every earlier one, without waiting for the
    fn append(
        &mut self,
        frame: &mut PreparedFrame,
        required: bool,
    ) -> Result<(), LocalStreamError> {
        let offset = self.bytes;
        self.file.lease()?.write(|file| {
            Ok(frame.write(&mut acyclic_native_runtime::WriteAt::new(file, offset)))
        })??;
        tracing::trace!(
            bytes = frame.encoded.len() as u64,
            required,
            "acyclic.stream.journal_append"
        );
        self.appended += 1;
        if required {
            self.required = self.appended;
        }
        self.operations += 1;
        self.bytes += frame.encoded.len() as u64;
        Ok(())
    }

    /// Whether frames were written since the journal last flushed.
    #[cfg(test)]
    fn unflushed(&self) -> bool {
        self.appended > self.synced
    }
}

impl Journal {
    fn open(
        root: &Path,
        limits: LocalStreamLimits,
        recovered: &mpsc::Sender<Recovered>,
        span: &tracing::Span,
    ) -> Result<Self, LocalStreamError> {
        let lock = lock_root(root, limits)?;
        let epoch = match read_snapshot(root, limits)? {
            Some(snapshot) => {
                recovered
                    .blocking_send(Recovered::State {
                        encoded: snapshot.state,
                    })
                    .map_err(|_| LocalStreamError::Executor)?;
                snapshot.epoch
            }
            None => 0,
        };
        let path = root.join(JOURNAL_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut journal = Self {
            _ownership_anchor: None,
            lock,
            sync: Arc::new(file.try_clone()?),
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
        };
        let header_bytes =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        let length = journal.file.metadata()?.len();
        let mut header = vec![0_u8; HEADER_BYTES];
        let found = if length < header_bytes {
            None
        } else {
            journal
                .file
                .read_exact(&mut header)
                .map_err(|_| LocalStreamError::Corrupt)?;
            Some(decode_header(&header, limits))
        };
        match found {
            // Every frame follows a whole header, so a journal holding less
            // than a valid header never acknowledged one: its creation or
            // restart was torn, and it starts again.
            None => {
                journal.file.seek(SeekFrom::Start(0))?;
                let mut partial = Vec::new();
                journal.file.read_to_end(&mut partial)?;
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
        journal.replay(length, recovered, span)?;
        if !validate_recovery(recovered)? {
            return Ok(journal);
        }
        if let Some(frame_start) = journal.torn_tail {
            match acyclic_native_runtime::recover_log_tail(
                &mut journal.file,
                frame_start,
                maximum_frame_bytes(),
                native_durability(limits.durability),
                frame_is_valid,
            )? {
                acyclic_native_runtime::LogTail::Torn => {}
                acyclic_native_runtime::LogTail::Corrupt => return Err(LocalStreamError::Corrupt),
            }
        }
        // Frames a crashed process wrote but never flushed are replayed, so
        // they are made durable before anything can observe them.
        sync_file_data(&journal.file, limits.durability)?;
        Ok(journal)
    }

    /// Hands every whole frame of a journal `length` long to `recovered`,
    fn replay(
        &mut self,
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
            let Some(frame) = read_frame(&mut self.file, total_length - frame_start)? else {
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
            recovered
                .blocking_send(Recovered::Command {
                    committed_at_millis: u64::from_le_bytes(*committed_at_millis),
                    command,
                })
                .map_err(|_| LocalStreamError::Executor)?;
            valid_length = valid_length
                .checked_add(frame_bytes(&frame.length))
                .ok_or(LocalStreamError::Corrupt)?;
        }
        if valid_length > limits.journal_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        self.file.seek(SeekFrom::End(0))?;
        obs::record(span, "frames", operations);
        obs::record(span, "bytes", valid_length);
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
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&encode_header(self.limits, epoch)?)?;
        match work {
            Some(work) => work.sync(|| sync_file(&self.file, self.limits.durability))?,
            None => sync_file(&self.file, self.limits.durability)?,
        }
        self.epoch = epoch;
        self.operations = 0;
        self.bytes = u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        // The header was synchronized after the snapshot holding every frame.
        self.synced = self.appended;
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
            file.write_all(&encode_snapshot(self.limits, epoch, state)?)?;
            match work {
                Some(work) => work.sync(|| sync_file(&file, self.limits.durability))?,
                None => sync_file(&file, self.limits.durability)?,
            }
        }
        std::fs::rename(&temporary, &snapshot)?;
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
        frame.write(&mut self.file)?;
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

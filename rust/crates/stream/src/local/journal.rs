use super::*;

pub(super) struct Journal {
    /// Held locked for as long as the provider is open.
    pub(super) lock: File,
    pub(super) file: File,
    /// Which snapshot the journal follows: the one naming this epoch.
    pub(super) epoch: u64,
    pub(super) operations: u64,
    pub(super) bytes: u64,
    pub(super) limits: LocalStreamLimits,
    /// How many frames were ever written, across every epoch: the sequence of the latest one.
    pub(super) appended: u64,
    /// The latest frame written by a mutation that waits for the device.
    pub(super) required: u64,
    /// Every frame through this sequence is durable.
    pub(super) synced: u64,
    /// Whether a flush is under way.
    pub(super) syncing: bool,
    pub(super) root: PathBuf,
    pub(super) torn_tail: Option<u64>,
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.lock.unlock();
    }
}

/// The blocking worker waits without releasing ownership; replay never authorizes truncation or restart after a rejected semantic prefix.
pub(super) fn validate_recovery(recovered: &mpsc::Sender<Recovered>) -> Result<bool, LocalStreamError> {
    let (complete, validation) = tokio::sync::oneshot::channel();
    recovered
        .blocking_send(Recovered::Validate(complete))
        .map_err(|_| LocalStreamError::Executor)?;
    validation
        .blocking_recv()
        .map_err(|_| LocalStreamError::Executor)
}

impl Journal {
    pub(super) fn open(
        root: &Path,
        limits: LocalStreamLimits,
        recovered: &mpsc::Sender<Recovered>,
        span: &tracing::Span,
    ) -> Result<(Self, File), LocalStreamError> {
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
        let sync = file.try_clone()?;
        let mut journal = Self {
            lock,
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
        let mut header = [0_u8; HEADER_BYTES];
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
            // An incomplete header cannot have acknowledged a frame.
            None => {
                journal.file.seek(SeekFrom::Start(0))?;
                let mut partial = Vec::new();
                journal.file.read_to_end(&mut partial)?;
                if !encode_header(limits, epoch)?.starts_with(&partial) {
                    return Err(LocalStreamError::Corrupt);
                }
                if !validate_recovery(recovered)? {
                    return Ok((journal, sync));
                }
                journal.restart(epoch)?;
                sync_directory(&path, limits.durability)?;
                return Ok((journal, sync));
            }
            Some(Some(found)) if found == epoch => {}
            Some(Some(found)) if found.checked_add(1) == Some(epoch) => {
                if !validate_recovery(recovered)? {
                    return Ok((journal, sync));
                }
                journal.restart(epoch)?;
                return Ok((journal, sync));
            }
            _ => return Err(LocalStreamError::Corrupt),
        }
        journal.replay(length, recovered, span)?;
        if !validate_recovery(recovered)? {
            return Ok((journal, sync));
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
        sync_file_data(&journal.file, limits.durability)?;
        Ok((journal, sync))
    }

    /// Hands every whole frame of a journal `length` long to `recovered`, cutting a torn one off the end.
    pub(super) fn replay(
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
            let (committed_at_millis, command) = frame
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
                .checked_add(4 + frame.len() as u64 + FRAME_CHECKSUM_BYTES as u64)
                .ok_or(LocalStreamError::Corrupt)?;
        }
        if valid_length > limits.journal_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        self.file.seek(SeekFrom::End(0))?;
        span.record("frames", operations);
        span.record("bytes", valid_length);
        self.operations = operations;
        self.bytes = valid_length;
        Ok(())
    }

    /// Empties the journal to follow the snapshot of `epoch`.
    pub(super) fn restart(&mut self, epoch: u64) -> Result<(), LocalStreamError> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&encode_header(self.limits, epoch)?)?;
        sync_file(&self.file, self.limits.durability)?;
        self.epoch = epoch;
        self.operations = 0;
        self.bytes = u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        // The header was synchronized after the snapshot holding every frame.
        self.synced = self.appended;
        Ok(())
    }

    pub(super) fn compaction_due(&self) -> bool {
        self.operations > 0
            && (self.operations >= self.limits.journal_operations / 2
                || self.bytes >= self.limits.journal_bytes / 2)
    }

    /// Makes `state`, which holds every frame so far, the snapshot the journal follows, then empties the journal.
    pub(super) fn compact(
        &mut self,
        state: &[u8],
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> (Result<(), LocalStreamError>, std::io::Result<()>, bool) {
        let mut observed = Ok(());
        let mut started = false;
        let physical = (|| {
            let epoch = self.epoch.checked_add(1).ok_or(LocalStreamError::Corrupt)?;
            let temporary = joined_path(&self.root, SNAPSHOT_TEMPORARY, meter)?;
            let snapshot = joined_path(&self.root, SNAPSHOT_FILE, meter)?;
            let encoded = encode_snapshot(self.limits, epoch, state, meter)?;
            let header = encode_header(self.limits, epoch)?;
            // Admit fixed boundaries before create; admit data retries separately.
            let mut controls = meter.map(|meter| NativeOperationReservation::new(
                Arc::clone(meter), NativeOperationKind::Write, 3,
            )).transpose()?;
            let mut seeks = meter.map(|meter| NativeOperationReservation::new(
                Arc::clone(meter), NativeOperationKind::Read, 1,
            )).transpose()?;
            let mut durability = meter.map(|meter| NativeOperationReservation::new(
                Arc::clone(meter), NativeOperationKind::Durability, 3,
            )).transpose()?;
            let mut snapshot_write = meter.map(|meter| AdmittedFrameWrite::new(meter, encoded.value.len()))
                .transpose()?;
            let mut header_write = meter.map(|meter| AdmittedFrameWrite::new(meter, header.len()))
                .transpose()?;
            let mut file = SnapshotFile::create(
                &temporary.value, meter, &mut controls, &mut observed, &mut started,
            )?;
            let handle = file.file.as_mut().expect("successful create retains its held file");
            let (written, recorded) = match &mut snapshot_write {
                Some(admission) => admission.write_all(handle, &encoded.value),
                None => (handle.write_all(&encoded.value).map_err(LocalStreamError::Io), Ok(())),
            };
            keep_observation(&mut observed, recorded);
            written?;
            let (synced, recorded) = attempt_control(&mut durability, || sync_file(handle, self.limits.durability));
            keep_observation(&mut observed, recorded);
            synced?;
            keep_observation(&mut observed, file.close());
            let (renamed, recorded) = attempt_control(&mut controls, || {
                std::fs::rename(&temporary.value, &snapshot.value)
            });
            keep_observation(&mut observed, recorded);
            renamed?;
            let (synced, recorded) = attempt_control(&mut durability, || {
                sync_directory(&snapshot.value, self.limits.durability)
            });
            keep_observation(&mut observed, recorded);
            synced?;
            let (truncated, recorded) = attempt_control(&mut controls, || self.file.set_len(0));
            keep_observation(&mut observed, recorded);
            truncated?;
            let (seeked, recorded) = attempt_control(&mut seeks, || self.file.seek(SeekFrom::Start(0)));
            keep_observation(&mut observed, recorded);
            seeked?;
            let (written, recorded) = match &mut header_write {
                Some(admission) => admission.write_all(&mut self.file, &header),
                None => (self.file.write_all(&header).map_err(LocalStreamError::Io), Ok(())),
            };
            keep_observation(&mut observed, recorded);
            written?;
            let (synced, recorded) = attempt_control(&mut durability, || sync_file(&self.file, self.limits.durability));
            keep_observation(&mut observed, recorded);
            synced?;
            self.epoch = epoch;
            self.operations = 0;
            self.bytes = HEADER_BYTES as u64;
            self.synced = self.appended;
            Ok(())
        })();
        (physical, observed, started)
    }

    pub(super) fn admit_bytes(&self, bytes: u64) -> Result<(), LocalStreamError> {
        if self.operations >= self.limits.journal_operations
            || self
                .bytes
                .checked_add(bytes)
                .is_none_or(|value| value > self.limits.journal_bytes)
        {
            return Err(LocalStreamError::InvalidLimits);
        }
        Ok(())
    }

    /// Uses the same cursor and counters.
    pub(super) fn append(
        &mut self,
        frame: &[u8],
        required: bool,
        admission: Option<&mut AdmittedFrameWrite>,
    ) -> (Result<(), LocalStreamError>, std::io::Result<()>) {
        let (written, observed) = match admission {
            Some(admission) => admission.write_all(&mut self.file, frame),
            None => (self.file.write_all(frame).map_err(LocalStreamError::Io), Ok(())),
        };
        if let Err(error) = written {
            return (Err(error.into()), observed);
        }
        tracing::trace!(
            bytes = frame.len() as u64,
            required,
            "acyclic.stream.journal_append"
        );
        self.appended += 1;
        if required {
            self.required = self.appended;
        }
        self.operations += 1;
        self.bytes += frame.len() as u64;
        (Ok(()), observed)
    }

    /// Whether frames were written since the journal last flushed.
    #[cfg(test)]
    pub(super) fn unflushed(&self) -> bool {
        self.appended > self.synced
    }
}

/// Creates `root` if it is missing and takes its exclusive lock.
pub(super) fn lock_root(root: &Path, limits: LocalStreamLimits) -> Result<File, LocalStreamError> {
    std::fs::create_dir_all(root)?;
    sync_directory(root, limits.durability)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(LOCK_FILE))?;
    lock.try_lock_exclusive().map_err(|error| {
        if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
            LocalStreamError::AlreadyOpen
        } else {
            LocalStreamError::Io(error)
        }
    })?;
    Ok(lock)
}

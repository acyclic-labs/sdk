struct PreparedFrame {
    encoded: Vec<u8>,
    work: Option<Arc<crate::memory::Work>>,
    operation: Option<
        acyclic_native_runtime::NativeOperationReservation<
            Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        >,
    >,
    transfer: Option<
        acyclic_native_runtime::NativeByteReservation<
            Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        >,
    >,
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}

impl PreparedFrame {
    fn reserve(
        capacity: usize,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        let mut allocation = work
            .as_ref()
            .map(|work| acyclic_native_runtime::ReadAllocation::new(&work.meter, capacity))
            .transpose()?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(capacity)
            .map_err(|error| match &work {
                Some(work) => LocalStreamError::Io(work.meter.allocation_failed(error)),
                None => LocalStreamError::InvalidLimits,
            })?;
        if let Some(allocation) = &mut allocation {
            allocation.reconcile_capacity(encoded.capacity())?;
        }
        let operation = work
            .as_ref()
            .map(|work| {
                acyclic_native_runtime::NativeOperationReservation::new(
                    Arc::clone(&work.meter),
                    acyclic_native_runtime::NativeOperationKind::Write,
                    1,
                )
            })
            .transpose()?;
        let transfer = work
            .as_ref()
            .map(|work| {
                acyclic_native_runtime::NativeByteReservation::new(
                    Arc::clone(&work.meter),
                    acyclic_native_runtime::NativeOperationKind::Write,
                    capacity,
                )
            })
            .transpose()?;
        if let Some(work) = &work {
            work.meter.admit_codec(
                capacity as u64,
                capacity as u64,
                (capacity + b"acyclic-stream-local-frame-v1\0".len()) as u64,
            )?;
        }
        Ok(Self {
            encoded,
            work,
            operation,
            transfer,
            _allocation: allocation,
        })
    }

    fn encode(
        command: &Command,
        time: u64,
        work: Option<Arc<crate::memory::Work>>,
    ) -> Result<Self, LocalStreamError> {
        if command.encoded_len() > MAX_COMMAND_BYTES {
            return Err(LocalStreamError::InvalidLimits);
        }
        let mut frame = Self::reserve(
            4 + FRAME_TIME_BYTES + command.encoded_len() + FRAME_CHECKSUM_BYTES,
            work,
        )?;
        frame.fill(command, time)?;
        if let Some(work) = &frame.work {
            work.meter.check()?;
        }
        Ok(frame)
    }

    fn fill(&mut self, command: &Command, time: u64) -> Result<(), LocalStreamError> {
        let size = command.encoded_len();
        if size > MAX_COMMAND_BYTES
            || 4 + FRAME_TIME_BYTES + size + FRAME_CHECKSUM_BYTES > self.encoded.capacity()
        {
            return Err(LocalStreamError::InvalidLimits);
        }
        let length = u32::try_from(FRAME_TIME_BYTES + size)
            .map_err(|_| LocalStreamError::InvalidLimits)?
            .to_le_bytes();
        self.encoded.clear();
        self.encoded.extend_from_slice(&length);
        self.encoded.extend_from_slice(&time.to_le_bytes());
        command.encode(&mut self.encoded);
        let checksum = frame_checksum(&length, &self.encoded[4..]);
        self.encoded.extend_from_slice(&checksum);
        if let Some(work) = &self.work {
            let hashed = b"acyclic-stream-local-frame-v1\0".len() + 4 + FRAME_TIME_BYTES + size;
            let _ = work.record(work.meter.record_codec(
                size as u64,
                (4 + FRAME_TIME_BYTES + size) as u64,
                hashed as u64,
            ));
        }
        Ok(())
    }

    fn write(&mut self, writer: &mut impl Write) -> Result<(), LocalStreamError> {
        let Some(work) = &self.work else {
            writer.write_all(&self.encoded)?;
            return Ok(());
        };
        let (result, observed) = self
            .operation
            .as_mut()
            .expect("funded journal write")
            .attempt(|| writer.write(&self.encoded));
        let _ = work.record(observed);
        let actual = result.as_ref().copied().unwrap_or(0);
        let _ = work.record(
            self.transfer
                .as_mut()
                .expect("funded journal bytes")
                .record(actual),
        );
        match result {
            Ok(length) if length == self.encoded.len() => Ok(()),
            Ok(_) => Err(LocalStreamError::IncompleteFrame),
            Err(error) => {
                let _ = work.record::<()>(Err(error));
                Err(LocalStreamError::IncompleteFrame)
            }
        }
    }
}

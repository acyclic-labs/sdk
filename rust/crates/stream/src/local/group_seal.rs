// A local-only tag outside the canonical Command variants (1, 2, 5).
const GROUP_SEAL_TAG: [u8; 2] = [0x32, 32];
const GROUP_SEAL_BYTES: usize = 4 + FRAME_TIME_BYTES + 2 + 4 * 8 + FRAME_CHECKSUM_BYTES;

struct GroupSeal {
    epoch: u64,
    start: u64,
    end: u64,
    operations: u64,
}

impl GroupSeal {
    fn encode(&self) -> [u8; GROUP_SEAL_BYTES] {
        let mut bytes = [0; GROUP_SEAL_BYTES];
        let length = 42_u32.to_le_bytes();
        bytes[..4].copy_from_slice(&length);
        bytes[12..14].copy_from_slice(&GROUP_SEAL_TAG);
        for (slot, value) in bytes[14..46].as_chunks_mut::<8>().0.iter_mut().zip([
            self.epoch,
            self.start,
            self.end,
            self.operations,
        ]) {
            slot.copy_from_slice(&value.to_le_bytes());
        }
        let checksum = frame_checksum(&length, &bytes[4..46]);
        bytes[46..].copy_from_slice(&checksum);
        bytes
    }

    fn decode(body: &[u8]) -> Option<Self> {
        if body.len() != 42
            || body.get(..8) != Some([0; 8].as_slice())
            || body.get(8..10) != Some(GROUP_SEAL_TAG.as_slice())
        {
            return None;
        }
        let number = |offset| {
            Some(u64::from_le_bytes(
                body.get(offset..offset + 8)?.try_into().ok()?,
            ))
        };
        Some(Self {
            epoch: number(10)?,
            start: number(18)?,
            end: number(26)?,
            operations: number(34)?,
        })
    }
}

/// Remaining original-request funding. Only the most recent raw append needs
/// to fund a group seal; replacing a slot refunds the previous unused credit.
struct SealCredit {
    work: Arc<crate::memory::Work>,
    operation: acyclic_native_runtime::NativeOperationReservation<
        Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    >,
    transfer: acyclic_native_runtime::NativeByteReservation<
        Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    >,
}

impl SealCredit {
    fn take(frame: &mut PreparedFrame) -> Option<Self> {
        Some(Self {
            work: frame.work.take()?,
            operation: frame.operation.take()?,
            transfer: frame.transfer.take()?,
        })
    }

    fn write(
        &mut self,
        writer: &mut impl Write,
        bytes: &[u8; GROUP_SEAL_BYTES],
    ) -> Result<(), LocalStreamError> {
        let _ = self.work.record(self.work.meter.record_codec(
            34,
            46,
            (46 + b"acyclic-stream-local-frame-v1\0".len()) as u64,
        ));
        let (result, observed) = self.operation.attempt(|| writer.write(bytes));
        let _ = self.work.record(observed);
        let _ = self
            .work
            .record(self.transfer.record(result.as_ref().copied().unwrap_or(0)));
        match result {
            Ok(GROUP_SEAL_BYTES) => Ok(()),
            Ok(_) => Err(LocalStreamError::IncompleteFrame),
            Err(error) => {
                let _ = self.work.record::<()>(Err(error));
                Err(LocalStreamError::IncompleteFrame)
            }
        }
    }
}

impl Journal {
    /// Caller holds the device gate until the seal's flush succeeds or poisons.
    fn seal(&mut self) -> Result<(), LocalStreamError> {
        if self.bytes == self.group_start {
            return Ok(());
        }
        // The model's negative control must detect bad ordering in release too.
        #[cfg(acyclic_loom)]
        assert!(
            self.sealed_sequence <= self.synced,
            "seal preceded predecessor flush"
        );
        #[cfg(not(acyclic_loom))]
        debug_assert!(
            self.sealed_sequence <= self.synced,
            "seal preceded predecessor flush"
        );
        let end = self
            .bytes
            .checked_add(GROUP_SEAL_BYTES as u64)
            .ok_or(LocalStreamError::InvalidLimits)?;
        if end > self.limits.journal_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        let bytes = GroupSeal {
            epoch: self.epoch,
            start: self.group_start,
            end: self.bytes,
            operations: self.operations,
        }
        .encode();
        match self.pending_seal.take() {
            Some(mut credit) => credit.write(&mut &*self.file, &bytes)?,
            None => acyclic_native_runtime::write_file_all(&mut &*self.file, &bytes)?,
        }
        self.bytes = end;
        self.group_start = end;
        self.sealed_sequence = self.appended;
        Ok(())
    }
}

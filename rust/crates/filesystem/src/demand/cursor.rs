use super::*;

/// Cursor bound to one directory and source epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    pub(super) source: SourceReference,
    pub(super) directory: NamespacePath,
    pub(super) directory_version: SourceVersion,
    pub(super) token: u64,
}

impl SourceCursor {
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.directory.retained_bytes()
    }
    pub(crate) fn cursor_encoded_len(&self) -> Result<usize, DemandError> {
        self.directory
            .cursor_encoded_len()
            .map_err(Self::cursor_path_error)?
            .checked_add(16 + 8 + 32 + 8)
            .ok_or(DemandError::InvalidRequest)
    }

    pub(crate) fn encode_cursor_into(
        &self,
        encoder: &mut crate::kernel::Encoder,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        accounting.check().map_err(Self::cursor_accounting_error)?;
        if encoder.remaining_capacity() < self.cursor_encoded_len()? {
            return Err(DemandError::InvalidRequest);
        }
        write_field(encoder, &self.source.identity, 16, accounting, cancellation)?;
        write_field(
            encoder,
            &self.source.epoch.to_le_bytes(),
            0,
            accounting,
            cancellation,
        )?;
        self.directory
            .encode_cursor_into_with_accounting(encoder, accounting, cancellation)
            .map_err(Self::cursor_accounting_error)?;
        write_field(
            encoder,
            &self.directory_version.0,
            32,
            accounting,
            cancellation,
        )?;
        write_field(
            encoder,
            &self.token.to_le_bytes(),
            0,
            accounting,
            cancellation,
        )
    }

    pub(crate) fn decode_cursor(
        decoder: &mut crate::kernel::Decoder<'_>,
        expected_source: SourceReference,
        expected_directory: &NamespacePath,
        profile: crate::FilesystemProfile,
        limits: crate::VolumeLimits,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        let mut allocation = accounting
            .claim_owned(0, 0)
            .map_err(Self::cursor_accounting_error)?;
        let source = SourceReference {
            identity: read_field(decoder, 16, accounting)?,
            epoch: u64::from_le_bytes(read_field(decoder, 0, accounting)?),
        };
        if source != expected_source {
            return Err(DemandError::StaleCursor);
        }
        let (directory, mut path_allocation) = NamespacePath::decode_cursor_with_accounting(
            decoder,
            profile,
            limits,
            accounting,
            cancellation,
        )
        .map_err(Self::cursor_accounting_error)?;
        let bytes = directory.retained_bytes();
        let result = (|| {
            let item = WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            };
            accounting
                .charge(item)
                .map_err(Self::cursor_accounting_error)?;
            if directory.depth() != expected_directory.depth() {
                return Err(DemandError::StaleCursor);
            }
            for (actual, expected) in directory
                .components()
                .iter()
                .zip(expected_directory.components())
            {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                accounting
                    .charge(item)
                    .map_err(Self::cursor_accounting_error)?;
                if actual.encoding() != expected.encoding()
                    || actual.as_bytes().len() != expected.as_bytes().len()
                {
                    return Err(DemandError::StaleCursor);
                }
                for (actual, expected) in actual.as_bytes().iter().zip(expected.as_bytes()) {
                    cancellation.check().map_err(|_| DemandError::Cancelled)?;
                    accounting
                        .charge(item)
                        .map_err(Self::cursor_accounting_error)?;
                    if actual != expected {
                        return Err(DemandError::StaleCursor);
                    }
                }
            }
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            let directory_version = SourceVersion(read_field(decoder, 32, accounting)?);
            let token = u64::from_le_bytes(read_field(decoder, 0, accounting)?);
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            accounting
                .record_completed(WorkCounters::UNCHARGED)
                .map_err(Self::cursor_accounting_error)?;
            Ok(Self {
                source,
                directory,
                directory_version,
                token,
            })
        })();
        match result {
            Ok(cursor) => {
                allocation
                    .absorb(&mut path_allocation)
                    .map_err(Self::cursor_accounting_error)?;
                Ok((cursor, allocation))
            }
            Err(error) => {
                accounting
                    .release_owned(&mut path_allocation, bytes)
                    .map_err(Self::cursor_accounting_error)?;
                Err(error)
            }
        }
    }

    pub(in crate::demand) fn cursor_path_error(error: NamespacePathError) -> DemandError {
        match error {
            NamespacePathError::Work(error) => DemandError::Work(error),
            NamespacePathError::Cancelled => DemandError::Cancelled,
            _ => DemandError::InvalidRequest,
        }
    }

    pub(in crate::demand) fn cursor_accounting_error(error: crate::FsError) -> DemandError {
        match error {
            crate::FsError::Work(error) => DemandError::Work(error),
            crate::FsError::Cancelled(_) => DemandError::Cancelled,
            crate::FsError::EncodedAllocationFailed(error) => DemandError::Allocation(error),
            #[cfg(not(target_arch = "wasm32"))]
            crate::FsError::NativeKernel(error) => match error {
                crate::NativeTreeMaterializationError::Work(error) => DemandError::Work(error),
                crate::NativeTreeMaterializationError::Cancelled => DemandError::Cancelled,
                crate::NativeTreeMaterializationError::OriginalWorkPoisoned => {
                    DemandError::OriginalWorkPoisoned
                }
                crate::NativeTreeMaterializationError::AllocationFailed(error) => {
                    DemandError::Allocation(error)
                }
                _ => DemandError::InvalidRequest,
            },
            _ => DemandError::InvalidRequest,
        }
    }
}

fn write_field(
    encoder: &mut crate::kernel::Encoder,
    bytes: &[u8],
    copied: u64,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), DemandError> {
    cancellation.check().map_err(|_| DemandError::Cancelled)?;
    let delta = WorkCounters {
        bytes_encoded: bytes.len() as u64,
        bytes_copied: copied,
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    accounting
        .admit(delta)
        .map_err(SourceCursor::cursor_accounting_error)?;
    encoder.fixed(bytes);
    accounting
        .record_completed(delta)
        .map_err(SourceCursor::cursor_accounting_error)?;
    cancellation.check().map_err(|_| DemandError::Cancelled)
}

fn read_field<const N: usize>(
    decoder: &mut crate::kernel::Decoder<'_>,
    copied: u64,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<[u8; N], DemandError> {
    let delta = WorkCounters {
        bytes_copied: copied,
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    };
    decoder
        .fixed::<N, crate::FsError>(&mut |step| match step {
            crate::kernel::ReadAllocationStep::BeforeCopy(_) => accounting.admit(delta),
            crate::kernel::ReadAllocationStep::Copied(_) => accounting.record_completed(delta),
            _ => unreachable!("fixed decoder only reports copy boundaries"),
        })
        .map_err(SourceCursor::cursor_accounting_error)
}

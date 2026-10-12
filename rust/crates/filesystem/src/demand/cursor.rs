use super::*;

/// Cursor bound to one directory and one source epoch.
pub struct SourceCursor {
    pub(in crate::demand) source: SourceReference,
    pub(in crate::demand) directory: NamespacePath,
    pub(in crate::demand) directory_version: SourceVersion,
    pub(in crate::demand) token: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::demand) native_ownership: Option<SourcePayloadOwnership>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::demand) borrowed: bool,
}

impl Clone for SourceCursor {
    fn clone(&self) -> Self {
        Self {
            source: self.source, directory: self.directory.clone(),
            directory_version: self.directory_version, token: self.token,
            #[cfg(not(target_arch = "wasm32"))]
            native_ownership: None,
                #[cfg(not(target_arch = "wasm32"))]
                borrowed: false,
        }
    }
}

impl std::fmt::Debug for SourceCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SourceCursor").field("source", &self.source)
            .field("directory", &self.directory).field("directory_version", &self.directory_version)
            .field("token", &self.token).finish()
    }
}

impl PartialEq for SourceCursor {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source && self.directory == other.directory
            && self.directory_version == other.directory_version && self.token == other.token
    }
}

impl Eq for SourceCursor {}

impl SourceCursor {
    #[cfg(not(target_arch = "wasm32"))]
        pub(crate) fn mark_input_borrowed(&mut self) {
            if self.native_ownership.is_none() {
                self.borrowed = true;
            }
        }

    #[cfg(not(target_arch = "wasm32"))]
        pub(crate) fn unclaimed_retained_bytes(&self) -> u64 {
            if self.borrowed || self.native_ownership.is_some() { 0 } else { self.retained_bytes() }
        }

    #[cfg(not(target_arch = "wasm32"))]
        pub(crate) fn attach_native_allocation(
            &mut self, allocation: &mut crate::NativeAllocation,
        ) -> Result<(), crate::NativeTreeMaterializationError> {
            if self.native_ownership.is_some() || self.borrowed {
                return Err(crate::NativeTreeMaterializationError::Work(crate::WorkError::Overflow));
            }
            let owned = allocation.split_off(self.retained_bytes())?;
            self.native_ownership = Some(SourcePayloadOwnership::from_native_allocation(owned));
            Ok(())
        }

    #[must_use]
        pub(crate) fn retained_bytes(&self) -> u64 {
            self.directory.retained_bytes()
        }

    pub(crate) fn cursor_encoded_len(&self) -> Result<usize, DemandError> {
            self.directory.cursor_encoded_len().map_err(Self::cursor_path_error)?
                .checked_add(16 + 8 + 32 + 8).ok_or(DemandError::InvalidRequest)
        }

    pub(crate) fn encode_cursor_into(
            &self,
            encoder: &mut crate::kernel::Encoder,
            accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
            cancellation: &CancellationToken,
        ) -> Result<(), DemandError> {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            accounting.check().map_err(Self::cursor_accounting_error)?;
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            if encoder.remaining_capacity() < self.cursor_encoded_len()? {
                return Err(DemandError::InvalidRequest);
            }
            let identity = WorkCounters { bytes_encoded: 16, bytes_copied: 16, items_examined: 1,
                ..WorkCounters::UNCHARGED };
            accounting.admit(identity).map_err(Self::cursor_accounting_error)?;
            encoder.fixed(&self.source.identity);
            accounting.record_completed(identity).map_err(Self::cursor_accounting_error)?;
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            let scalar = WorkCounters { bytes_encoded: 8, items_examined: 1,
                ..WorkCounters::UNCHARGED };
            accounting.admit(scalar).map_err(Self::cursor_accounting_error)?;
            encoder.u64(self.source.epoch);
            accounting.record_completed(scalar).map_err(Self::cursor_accounting_error)?;
            self.directory.encode_cursor_into_with_accounting(encoder, accounting, cancellation)
                .map_err(Self::cursor_accounting_error)?;
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            let version = WorkCounters { bytes_encoded: 32, bytes_copied: 32, items_examined: 1,
                ..WorkCounters::UNCHARGED };
            accounting.admit(version).map_err(Self::cursor_accounting_error)?;
            encoder.fixed(&self.directory_version.0);
            accounting.record_completed(version).map_err(Self::cursor_accounting_error)?;
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            accounting.admit(scalar).map_err(Self::cursor_accounting_error)?;
            encoder.u64(self.token);
            accounting.record_completed(scalar).map_err(Self::cursor_accounting_error)?;
            cancellation.check().map_err(|_| DemandError::Cancelled)
        }

    pub(crate) fn decode_cursor(
            decoder: &mut crate::kernel::Decoder<'_>,
            expected_source: SourceReference,
            expected_directory: &NamespacePath,
            profile: crate::FilesystemProfile,
            limits: crate::VolumeLimits,
            accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
            cancellation: &CancellationToken,
        ) -> Result<(Self, crate::kernel::allocation::KernelOwnedAllocation), DemandError> {
            cancellation.check().map_err(|_| DemandError::Cancelled)?;
            let mut allocation = accounting.claim_owned(0, 0).map_err(Self::cursor_accounting_error)?;
            let identity_bytes = decoder.take_exact(16).map_err(|_| DemandError::InvalidRequest)?;
            let identity_work = WorkCounters { bytes_copied: 16, items_examined: 1,
                ..WorkCounters::UNCHARGED };
            accounting.admit(identity_work).map_err(Self::cursor_accounting_error)?;
            let identity = <[u8; 16]>::try_from(identity_bytes).map_err(|_| DemandError::InvalidRequest)?;
            accounting.record_completed(identity_work).map_err(Self::cursor_accounting_error)?;
            let item = WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED };
            accounting.charge(item).map_err(Self::cursor_accounting_error)?;
            let epoch = decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(())).map_err(|_| DemandError::InvalidRequest)?;
            let source = SourceReference { identity, epoch };
            if source != expected_source {
                return Err(DemandError::StaleCursor);
            }
            let (directory, mut path_allocation) = NamespacePath::decode_cursor_with_accounting(
                decoder, profile, limits, accounting, cancellation,
            ).map_err(Self::cursor_accounting_error)?;
            let owned_bytes = directory.retained_bytes();
            let result = (|| {
                accounting.charge(item).map_err(Self::cursor_accounting_error)?;
                if directory.depth() != expected_directory.depth() {
                    return Err(DemandError::StaleCursor);
                }
                for (actual, expected) in directory.components().iter().zip(expected_directory.components()) {
                    cancellation.check().map_err(|_| DemandError::Cancelled)?;
                    accounting.charge(item).map_err(Self::cursor_accounting_error)?;
                    if actual.encoding() != expected.encoding() || actual.as_bytes().len() != expected.as_bytes().len() {
                        return Err(DemandError::StaleCursor);
                    }
                    for (actual, expected) in actual.as_bytes().iter().zip(expected.as_bytes()) {
                        cancellation.check().map_err(|_| DemandError::Cancelled)?;
                        accounting.charge(item).map_err(Self::cursor_accounting_error)?;
                        if actual != expected {
                            return Err(DemandError::StaleCursor);
                        }
                    }
                }
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                let version_bytes = decoder.take_exact(32).map_err(|_| DemandError::InvalidRequest)?;
                let version_work = WorkCounters { bytes_copied: 32, items_examined: 1,
                    ..WorkCounters::UNCHARGED };
                accounting.admit(version_work).map_err(Self::cursor_accounting_error)?;
                let directory_version = SourceVersion(<[u8; 32]>::try_from(version_bytes)
                    .map_err(|_| DemandError::InvalidRequest)?);
                accounting.record_completed(version_work).map_err(Self::cursor_accounting_error)?;
                accounting.charge(item).map_err(Self::cursor_accounting_error)?;
                let token = decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(())).map_err(|_| DemandError::InvalidRequest)?;
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                accounting.record_completed(WorkCounters::UNCHARGED).map_err(Self::cursor_accounting_error)?;
                Ok(Self {
                    source, directory, directory_version, token,
                    #[cfg(not(target_arch = "wasm32"))]
                    native_ownership: None,
                    #[cfg(not(target_arch = "wasm32"))]
                    borrowed: false,
                })
            })();
            match result {
                Ok(cursor) => {
                    allocation.absorb(&mut path_allocation).map_err(Self::cursor_accounting_error)?;
                    Ok((cursor, allocation))
                }
                Err(error) => {
                    accounting.release_owned(&mut path_allocation, owned_bytes).map_err(Self::cursor_accounting_error)?;
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
                    crate::NativeTreeMaterializationError::OriginalWorkPoisoned => DemandError::OriginalWorkPoisoned,
                    crate::NativeTreeMaterializationError::AllocationFailed(error) => DemandError::Allocation(error),
                    _ => DemandError::InvalidRequest,
                },
                _ => DemandError::InvalidRequest,
            }
        }
}

#[cfg(test)]
mod source_cursor_codec_tests {
    use super::*;
    use crate::kernel::allocation::{AllocationLedger, KernelAccounting};
    use crate::kernel::{Decoder, Encoder, LogicalName, NameEncoding};

    #[test]
    fn cursors_preserve_opaque_tokens_and_reject_foreign_bindings()
    -> Result<(), Box<dyn std::error::Error>> {
        const DOMAIN: &[u8] = b"source-cursor-control";
        let limits = crate::VolumeLimits::default();
        let cancellation = CancellationToken::new();
        for token in [0, u64::MAX] {
            let cursor = SourceCursor {
                source: SourceReference { identity: [4; 16], epoch: 7 },
                directory: NamespacePath::new(vec![LogicalName::new(
                    NameEncoding::WindowsUtf16Le, vec![0, 0xd8], limits.maximum_component_bytes,
                )?], limits)?,
                directory_version: SourceVersion([9; 32]), token,
                #[cfg(not(target_arch = "wasm32"))]
                native_ownership: None,
                #[cfg(not(target_arch = "wasm32"))]
                borrowed: false,
            };
            let mut ledger = AllocationLedger::default();
            let mut work = WorkCounters::default();
            let mut accounting = KernelAccounting::borrowed_standalone(
                &mut ledger, &mut work, WorkBudget::UNBOUNDED, &cancellation,
            );
            let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, DOMAIN.len() + 2 + cursor.cursor_encoded_len()?, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
            cursor.encode_cursor_into(&mut encoder, &mut accounting, &cancellation)?;
            let bytes = encoder.finish();
            for epoch in [cursor.source.epoch, cursor.source.epoch + 1] {
                let mut decoder = Decoder::new(&bytes, DOMAIN, 1, u64::MAX, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
                let result = SourceCursor::decode_cursor(
                    &mut decoder, SourceReference { epoch, ..cursor.source }, &cursor.directory,
                    crate::FilesystemProfile::Windows, limits, &mut accounting, &cancellation,
                );
                if epoch == cursor.source.epoch {
                    let (decoded, mut allocation) = result?;
                    decoder.finish()?;
                    assert_eq!(decoded, cursor);
                    let retained = decoded.retained_bytes();
                    drop(decoded);
                    accounting.release_owned(&mut allocation, retained)?;
                } else {
                    assert!(matches!(result, Err(DemandError::StaleCursor)));
                }
            }
        }
        Ok(())
    }
}

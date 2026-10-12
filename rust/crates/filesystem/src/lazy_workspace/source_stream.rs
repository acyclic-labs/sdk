use super::*;

pub(super) struct DemandBlobSource<'a, D> {
    pub(super) provider: &'a D,
    pub(super) source: SourceReference,
    pub(super) path: &'a NamespacePath,
    pub(super) version: SourceVersion,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) work: Option<WorkCounters>,
    pub(super) failure: Option<DemandError>,
}

impl<D: DemandSource> crate::kernel::AsyncBlobSource for DemandBlobSource<'_, D> {
    async fn read(
        &mut self,
        destination: &mut [u8],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> std::io::Result<usize> {
        let wanted = self
            .length
            .saturating_sub(self.offset)
            .min(destination.len() as u64);
        if wanted == 0 {
            return Ok(0);
        }
        let copying = WorkCounters {
            bytes_copied: wanted,
            ..WorkCounters::UNCHARGED
        };
        let allowance = copying.remaining(budget).map_err(std::io::Error::other)?;
        let result = self
            .provider
            .read_range(
                self.source,
                self.path,
                self.version,
                self.offset,
                wanted,
                allowance,
                cancellation,
            )
            .await;
        let chunk = match result {
            Ok(receipt) => {
                self.work = Some(receipt.work);
                receipt.value
            }
            Err(failure) => {
                self.work = Some(*failure.work);
                self.failure = Some(failure.error);
                return Err(std::io::Error::other("source read failed"));
            }
        };
        if chunk.bytes.is_empty() || chunk.bytes.len() as u64 > wanted {
            self.failure = Some(DemandError::StaleVersion);
            return Err(std::io::Error::other("incomplete source read"));
        }
        let len = chunk.bytes.len();
        destination[..len].copy_from_slice(&chunk.bytes);
        self.offset += len as u64;
        self.work = Some(
            self.work
                .unwrap_or_default()
                .checked_add(WorkCounters {
                    bytes_copied: len as u64,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(std::io::Error::other)?,
        );
        Ok(len)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &mut self,
        destination: &mut [u8],
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<usize, crate::FsError> {
        original.admit(WorkCounters::UNCHARGED)?;
        cancellation.check()?;
        let wanted = self
            .length
            .saturating_sub(self.offset)
            .min(destination.len() as u64);
        if wanted == 0 {
            return Ok(0);
        }
        let failure = crate::kernel::PreparedBlobDemandFailure::new(original)?;
        let mut copying = original.reserve_work(WorkCounters {
            bytes_copied: wanted,
            ..WorkCounters::UNCHARGED
        })?;
        let chunk = match self
            .provider
            .read_range(
                self.source,
                self.path,
                self.version,
                self.offset,
                wanted,
                original,
                cancellation,
            )
            .await
        {
            Ok(chunk) => chunk,
            Err(error) => {
                return Err(crate::kernel::BlobBuildError::Demand(failure.failed(error)).into());
            }
        };
        if chunk.bytes.is_empty() || chunk.bytes.len() as u64 > wanted {
            return Err(crate::kernel::BlobBuildError::Demand(
                failure.failed(DemandError::StaleVersion),
            )
            .into());
        }
        let len = chunk.bytes.len();
        destination[..len].copy_from_slice(&chunk.bytes);
        self.offset += len as u64;
        copying.record_completed(WorkCounters {
            bytes_copied: len as u64,
            ..WorkCounters::UNCHARGED
        })?;
        Ok(len)
    }

    fn take_work(&mut self) -> Option<WorkCounters> {
        self.work.take()
    }
}

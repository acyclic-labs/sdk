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

impl<D: DemandSource> DemandBlobSource<'_, D> {
    fn wanted(&self, destination: &[u8]) -> u64 {
        self.length
            .saturating_sub(self.offset)
            .min(destination.len() as u64)
    }
    fn copy(&mut self, bytes: &[u8], destination: &mut [u8]) -> Result<usize, DemandError> {
        if bytes.is_empty() || bytes.len() as u64 > self.wanted(destination) {
            return Err(DemandError::StaleVersion);
        }
        destination[..bytes.len()].copy_from_slice(bytes);
        self.offset += bytes.len() as u64;
        Ok(bytes.len())
    }
}

impl<D: DemandSource> crate::kernel::AsyncBlobSource for DemandBlobSource<'_, D> {
    async fn read(
        &mut self,
        destination: &mut [u8],
        budget: WorkBudget,
        cancel: &CancellationToken,
    ) -> std::io::Result<usize> {
        let wanted = self.wanted(destination);
        if wanted == 0 {
            return Ok(0);
        }
        let allowance = WorkCounters {
            bytes_copied: wanted,
            ..WorkCounters::UNCHARGED
        }
        .remaining(budget)
        .map_err(std::io::Error::other)?;
        let result = self
            .provider
            .read_range(
                self.source,
                self.path,
                self.version,
                self.offset,
                wanted,
                allowance,
                cancel,
            )
            .await;
        let chunk = match result {
            Ok(receipt) => {
                self.work = Some(receipt.work);
                receipt.value
            }
            Err(failed) => {
                self.work = Some(*failed.work);
                self.failure = Some(failed.error);
                return Err(std::io::Error::other("version-pinned source read failed"));
            }
        };
        let len = self.copy(&chunk.bytes, destination).map_err(|error| {
            self.failure = Some(error);
            std::io::Error::other("incomplete source read")
        })?;
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
        work: &crate::NativeMaterializationWork,
        cancel: &CancellationToken,
    ) -> Result<usize, crate::FsError> {
        work.admit(WorkCounters::UNCHARGED)?;
        cancel.check()?;
        let wanted = self.wanted(destination);
        if wanted == 0 {
            return Ok(0);
        }
        let failure = crate::kernel::PreparedBlobDemandFailure::new(work)?;
        let mut copying = work.reserve_work(WorkCounters {
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
                work,
                cancel,
            )
            .await
        {
            Ok(chunk) => chunk,
            Err(error) => {
                return Err(crate::kernel::BlobBuildError::Demand(failure.failed(error)).into());
            }
        };
        let len = self
            .copy(&chunk.bytes, destination)
            .map_err(|error| crate::kernel::BlobBuildError::Demand(failure.failed(error)))?;
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

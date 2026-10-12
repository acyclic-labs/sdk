use super::*;

/// Owner-scoped scheduler result staging into one agent-private Filesystem volume.
pub struct FilesystemSchedulerPayloadStore<A, O> {
    pub(in crate::filesystem) host: Arc<FilesystemHost<A, O>>,
    pub(in crate::filesystem) volume: VolumeRef,
    pub(in crate::filesystem) grant: ContentGrant,
    pub(in crate::filesystem) maximum_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemSchedulerPayloadStore<A, O> {
    /// Binds private-volume write authority with optional metadata accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>, volume: &VolumeRef,
        verifier: &AuthorityVerifier, scope: &Scope, maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider || maximum_bytes == 0
        {
            return Err(stock_boundary(io, crate::wire::ErrorCode::Invalid,
                "scheduler payload store requires a private volume and positive limit"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let grant = ContentGrant::verify(verifier, scope, volume, VolumeOperation::Write, u64::MAX, &mut work)?;
        let volume = volume.copy(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => {
                io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                Some(io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| stock_boundary(Some(io), crate::wire::ErrorCode::Invalid, "scheduler payload custody underflows"))?)?)
            }
            None => None,
        };
        Ok(Self { host, volume, grant, maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))] _native_metadata })
    }

    /// The intrinsic guard covers Self and deep metadata, not opaque Arc nodes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self._native_metadata.as_ref().is_some_and(|custody| work.shares_ledger(custody.work())) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native payload sharing requires original metadata custody" });
        }
        let io = PreparationIo::from_native_work(work)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }
}

impl<A, O> SchedulerPayloadStore for FilesystemSchedulerPayloadStore<A, O>
where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    fn stage<'a>(
        &'a self, operation_id: crate::OperationId, idempotency_key: &'a str, bytes: &'a [u8],
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
        use std::fmt::Write as _;
        let hashed = acyclic_fs::WorkCounters {
            bytes_hashed: idempotency_key.len() as u64, items_examined: 1,
            ..Default::default()
        };
        if let Some(io) = io { io.preflight(hashed)?; }
        let key = blake3::hash(idempotency_key.as_bytes()).to_hex();
        if let Some(io) = io { io.charge(hashed)?; }
        let path_bytes = "scheduler/results/".len() + 36 + 1 + key.len() + ".json".len();
        let retry_bytes = "scheduler-result:".len() + 36 + 1 + key.len();
        let scratch_hold;
        if let Some(io) = io {
            io.preflight(acyclic_fs::WorkCounters {
                bytes_copied: (path_bytes + retry_bytes) as u64, ..Default::default()
            })?;
            io.retain_owned((path_bytes + retry_bytes) as u64)?;
            scratch_hold = Some(io.guard_owned((path_bytes + retry_bytes) as u64)?);
            io.allocation_attempt(path_bytes as u64)?;
        } else { scratch_hold = None; }
        let mut path = String::with_capacity(path_bytes);
        write!(&mut path, "scheduler/results/{operation_id}/{key}.json")
            .map_err(|_| Error::Invalid("scheduler result path formatting failed".into()))?;
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters { bytes_copied: path.len() as u64, ..Default::default() })?;
        }
        if let Some(io) = io { io.allocation_attempt(retry_bytes as u64)?; }
        let mut retry = String::with_capacity(retry_bytes);
        write!(&mut retry, "scheduler-result:{operation_id}:{key}")
            .map_err(|_| Error::Invalid("scheduler retry formatting failed".into()))?;
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                bytes_copied: retry.len() as u64, ..Default::default()
            })?;
        }
        let retry = IdempotencyKey::new(retry)?;
        let file = self.host.put_content_impl(&self.volume, &self.grant, &path, bytes,
            "application/json", "result.json", self.maximum_bytes, &retry,
            None, None, io, None, None).await?;
        drop(retry);
        drop(path);
        drop(scratch_hold);
        Ok(file)
        })
    }
}

use crate::executor::{stock_boundary};

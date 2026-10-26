use super::*;
use crate::filesystem::PreparationIo;

impl<P: StreamProvider + ?Sized> TaskFiles<P> {
    /// Opens a private file index on the original finite native ledger.
    /// The shared binding owns its replay allocation custody.
    pub async fn open_with_native_work(
        provider: Arc<P>,
        volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<Arc<Self>> {
        if !work.remaining()?.has_finite_allowances() {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task-file replay requires finite native work",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        if volume.class() != VolumeClass::AgentPrivate
            || maximum_bytes == 0
            || maximum_bytes == u64::MAX
            || !owns_volume(volume, scope.agent())
        {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task files require their private owner and a finite bound",
            });
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            ContentGrant::authorize_volume(
                verifier,
                scope,
                volume,
                operation,
                maximum_bytes,
                &mut Some(&mut &io),
            )?;
        }
        Self::open_authorized_native(provider, volume, maximum_bytes, work).await
    }

    pub(crate) async fn open_authorized_native(
        provider: Arc<P>,
        volume: &VolumeRef,
        maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<Arc<Self>> {
        let io = PreparationIo::from_native_work(work)?;
        let name = volume.storage_name(&mut Some(&mut &io))?;
        let length = "harness/task-volumes/".len() + name.len();
        io.retain_owned(length as u64)?;
        io.allocation_attempt(length as u64)?;
        let path = StreamPath::new(format!("harness/task-volumes/{name}")).map_err(|_| {
            crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "invalid task-volume stream path",
            }
        })?;
        let inner = TaskVolume::open_native(provider, path, &io).await?;
        let volume = volume.copy(&mut Some(&mut &io))?;
        // Arc's two counters and the complete inline binding are admitted
        // before the allocation, alongside the index's retained storage.
        let shared = (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64;
        io.retain_owned(shared)?;
        io.allocation_attempt(shared)?;
        // Reconnect readers share the index and its custody. Admit the
        // custody's shared owner before allocating it on the same ledger.
        let shared_custody = (std::mem::size_of::<acyclic_fs::NativeAllocation>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        io.retain_owned(shared_custody)?;
        io.allocation_attempt(shared_custody)?;
        let retained = io.owned_bytes()?;
        let custody = Arc::new(io.into_native_custody(retained)?);
        Ok(Arc::new(Self {
            inner,
            volume,
            maximum_bytes,
            _native_custody: Some(custody),
        }))
    }
}

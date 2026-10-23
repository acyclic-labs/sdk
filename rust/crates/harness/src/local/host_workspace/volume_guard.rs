use super::*;
use acyclic_fs::native_mount::{
    NativeMountCallbackFuture, NativeMountCallbackGuard, NativeMountError,
};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};

struct Guard {
    owner: Arc<dyn OwnedTaskMailbox>,
    root: Arc<HeldRoot>,
    writable: bool,
    custody: NativeAllocation,
}

pub(super) fn options(
    owner: Arc<dyn OwnedTaskMailbox>,
    root: Arc<HeldRoot>,
    read_only: bool,
) -> Result<acyclic_fs::MountOptions> {
    let cancellation = acyclic_fs::CancellationToken::new();
    // Retain the mount constructor's existing allowance, rather than funding
    // a new context for each callback or publication.
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let custody = work.claim(
        std::mem::size_of::<Guard>() as u64,
        1,
        WorkCounters::UNCHARGED,
    )?;
    let guard = Arc::new(Guard {
        owner,
        root,
        writable: !read_only,
        custody,
    });
    let options = if read_only {
        acyclic_fs::MountOptions::read_only()
    } else {
        acyclic_fs::MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual)
    };
    Ok(options.native_context(&work, Some(guard), Some(&work)))
}

impl Guard {
    fn authorize<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
        write: bool,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        NativeMountCallbackFuture::new_guarded::<Error, _, _>(work, move || async move {
            if (write && !self.writable) || !self.custody.work().shares_allocation_context(work) {
                return Err(Error::Unauthorized("native volume owner differs".into()));
            }
            self.owner.verify_owner(None)?.await?;
            verify_root(&self.root)
        })
    }
}

impl NativeMountCallbackGuard for Guard {
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        self.authorize(work, false)
    }

    fn check_write<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        self.authorize(work, true)
    }
}

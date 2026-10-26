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
    options: acyclic_fs::MountOptions,
    owner: Arc<dyn OwnedTaskMailbox>,
    root: Arc<HeldRoot>,
    read_only: bool,
) -> Result<acyclic_fs::MountOptions> {
    // Policy refresh and lifetime custody share the original admission ledger.
    let work = options.require_native_work()?.clone();
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
    // Add lifetime custody to the authenticated options supplied by the
    // workspace authority. Reconstructing options would discard its access
    // view and policy refresh owner before shared-view admission.
    let options = if read_only {
        options.restrict_to_read_only()
    } else {
        options
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

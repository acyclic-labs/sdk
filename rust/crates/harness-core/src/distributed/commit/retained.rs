//! Transfers the original commit construction into the shared completion.
use super::*;
use crate::filesystem::PreparationCustody;
use acyclic_stream::{CommitRequest, NativePending, NativeProviderFuture, NativeStreamResult};

type Pending = NativePending<PreparationCustody>;

/// Field order keeps the exact request alive until its storage can retire.
pub(super) struct RequestCustody {
    _request: CommitRequest,
    _storage: (PreparationCustody, acyclic_native_runtime::ReadAllocation),
}

pub(super) fn prepare<P: crate::store::JournalProvider + ?Sized>(
    mut request: CommitRequest,
    quota: &mut CodecQuota<'_, '_>,
    provider: Arc<P>,
    original: &acyclic_fs::NativeMaterializationWork,
    deadline: Option<u64>,
) -> Result<(Pending, NativeProviderFuture<'static, ()>)> {
    request.deadline_unix_millis = request
        .deadline_unix_millis
        .into_iter()
        .chain(deadline)
        .min();
    let storage = quota.take_owned_custody()?.ok_or_else(|| {
        guarded_boundary(
            quota.work(),
            wire::ErrorCode::Invalid,
            "retained commit requires its original construction custody",
        )
    })?;
    NativePending::prepare_native(provider, request, storage, original.runtime_meter()).map_err(
        |error| Error::NativeWork {
            cause: original.runtime_error(error),
            custody: None,
        },
    )
}

pub(super) fn take(
    pending: Pending,
    original: &acyclic_fs::NativeMaterializationWork,
) -> Result<(
    NativeStreamResult<acyclic_stream::CommitOutcome>,
    RequestCustody,
)> {
    // The original owner has just driven its sole producer to completion;
    // no observer alias is created on this ordinary commit path.
    let (request, result, storage) = pending.try_take().map_err(|_| Error::NativeBoundary {
        code: wire::ErrorCode::Storage,
        reason: "retained commit producer or observer is still active",
    })?;
    let result = result.ok_or_else(|| Error::NativeWork {
        cause: original.runtime_error(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        custody: None,
    });
    let held = RequestCustody {
        _request: request,
        _storage: storage,
    };
    result.map(|result| (result, held))
}

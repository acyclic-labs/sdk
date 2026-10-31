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

fn prepare<P: crate::store::JournalProvider + ?Sized>(
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

fn take(
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

/// The original prepared request, binding, funding and completion. Its producer
/// is handed to the original execution owner, independently of any observer.
pub(crate) struct NativeCommit<P: crate::store::JournalProvider + ?Sized> {
    pending: Pending,
    settlement: Settlement,
    provider: Arc<P>,
    original: acyclic_fs::NativeMaterializationWork,
}

impl<P: crate::store::JournalProvider + ?Sized> NativeCommit<P> {
    /// Seals the sole admitted request before returning its funded producer.
    pub(crate) fn prepare(
        mut request: CommitRequest,
        quota: &mut CodecQuota<'_, '_>,
        provider: Arc<P>,
        deadline: Option<u64>,
    ) -> Result<(Self, NativeProviderFuture<'static, ()>)> {
        let original = quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .cloned()
            .ok_or_else(|| {
                guarded_boundary(
                    quota.work(),
                    wire::ErrorCode::Invalid,
                    "retained commit requires its original native observer",
                )
            })?;
        let Admission {
            binding,
            acknowledgment,
            ..
        } = admit(&mut request, quota)?;
        let (pending, producer) =
            prepare(request, quota, Arc::clone(&provider), &original, deadline)?;
        let settlement = Settlement {
            key: pending.request().idempotency_key.clone(),
            binding,
            acknowledgment,
            vectors: 0,
            input: 0,
        };
        Ok((
            Self {
                pending,
                settlement,
                provider,
                original,
            },
            producer,
        ))
    }

    pub(crate) fn original(&self) -> &acyclic_fs::NativeMaterializationWork {
        &self.original
    }

    /// Resumes through the same conditional-result decoder and retry binding.
    /// The caller retains this object while its one producer is being driven.
    /// Result adoption requires that original allocation context, never a new
    /// observer's authority or an unfunded clone of a native receipt/error.
    pub(crate) async fn finish(
        self,
        operation: Option<OperationId>,
        phase: Option<&mut CommitPhase>,
        quota: &mut CodecQuota<'_, '_>,
    ) -> Result<(acyclic_stream::CommitOutcome, u64)> {
        if !quota
            .work()
            .as_deref()
            .and_then(|io| io.original_native_work())
            .is_some_and(|work| work.shares_allocation_context(&self.original))
        {
            return Err(guarded_boundary(
                quota.work(),
                wire::ErrorCode::Invalid,
                "retained commit completion requires its original observer",
            ));
        }
        // Producer loss is terminal too; take moves that exact absence rather
        // than waiting on a successor or inventing a successful receipt.
        let _ = self.pending.result().await;
        let (result, request) = take(self.pending, &self.original)?;
        let committed = match native_call_result(&self.original, result)? {
            Ok(output) => {
                let (outcome, retained) = crate::store::history::adopt_native_sdk_output(
                    &self.original,
                    output,
                    quota.work(),
                )?;
                quota.adopt(retained)?;
                Ok(outcome)
            }
            Err(error) => Err(error),
        };
        let result = self
            .settlement
            .finish(committed, quota, &self.provider, operation, phase)
            .await;
        drop(request);
        result
    }
}

#[cfg(test)]
mod tests;

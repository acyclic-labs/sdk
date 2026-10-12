//! SDK boundaries on the original native ledger, without another budget/meter.

use super::HistoryIo;

use crate::{Error, Result};

use acyclic_fs::{
    NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError,
    NativeWorkReservation, WorkCounters,
};

/// Moves an actual SDK output claim into this observer without another claim or a cancellation check.
pub(crate) fn adopt_native_sdk_output<T>(
    original: &NativeMaterializationWork,
    output: acyclic_stream::NativeStreamOutput<T>,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(T, u64)> {
    let (value, incoming) = output.into_parts();
    let Some(observer) = work.as_deref_mut().filter(|observer| {
        observer
            .original_native_work()
            .is_some_and(|work| work.shares_allocation_context(original))
    }) else {
        drop(value);
        drop(incoming);
        return Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::InvalidLayout,
            custody: None,
        });
    };
    let incoming = match NativeAllocation::from_runtime_allocation(original, incoming) {
        Ok(incoming) => incoming,
        Err((cause, incoming)) => {
            drop(value);
            drop(incoming);
            return Err(Error::NativeWork {
                cause,
                custody: None,
            });
        }
    };
    let retained = incoming.bytes();
    match observer.absorb_native_custody(incoming) {
        Ok(()) => Ok((value, retained)),
        Err(error) => {
            drop(value);
            Err(error)
        }
    }
}

/// Preserves the native SDK's owned semantic failure and original allocation guard without formatting it or losing its cause during policy inspection.
pub(crate) fn native_sdk_result<T>(
    original: &NativeMaterializationWork,
    result: acyclic_stream::NativeStreamResult<T>,
) -> Result<acyclic_stream::NativeStreamOutput<T>> {
    match result {
        Err(error) => Err(Error::NativeWork {
            cause: original.runtime_error(error),
            custody: None,
        }),
        Ok(Ok(output)) => Ok(output),
        Ok(Err(failure)) => {
            let source = failure;
            let code = if source == acyclic_stream::StreamError::IdempotencyMismatch {
                crate::wire::ErrorCode::Conflict
            } else {
                crate::wire::ErrorCode::Storage
            };
            Err(Error::NativeStream {
                code,
                source,
                custody: None,
            })
        }
    }
}

mod completion;
pub(crate) use completion::AcceptedCompletionIo;
#[cfg(test)]
mod tests;

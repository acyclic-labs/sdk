//! Original-ledger admission for provider wire work.
//!
//! Every provider path that materialises a request or a response claims its
//! native allocation here before it builds the graph, and retains only the
//! capacity that survives. The helpers are shared by counting, dispatch,
//! credential I/O, the Codex login and the native model stream, so they live
//! below all of them.
use acyclic_harness_core::{Error, Result, model::PreparedModelRequest, wire::ErrorCode};
use {
    acyclic_fs::{NativeAllocation, NativeMaterializationWork},
    acyclic_fs_work::performance::WorkCounters,
};

#[cfg(test)]
#[path = "allocation_accounting_tests.rs"]
mod accounting_tests;

/// A native-boundary rejection with the invalid wire code.
pub fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: ErrorCode::Invalid,
        reason,
    }
}

fn error_has_no_excerpt(error: &Error) -> bool {
    matches!(error, Error::ModelUnavailable { excerpt, .. } if excerpt.is_empty())
}

/// Funds RESULT's diagnostic text from ALLOCATION, or returns the error unchanged.
pub fn diagnostic<T>(result: Result<T>, allocation: &mut NativeAllocation) -> Result<T> {
    result.map_err(|error| diagnostic_error(error, allocation))
}

/// Funds ERROR's diagnostic text from ALLOCATION, keeping its class.
pub fn diagnostic_error(error: Error, allocation: &mut NativeAllocation) -> Error {
    let (code, message, semantic) = match error {
        Error::Invalid(message) => (ErrorCode::Invalid, message, false),
        Error::Unauthorized(message) => (ErrorCode::Unauthorized, message, false),
        Error::Storage(message) => (ErrorCode::Storage, message, false),
        Error::ModelDiagnostic {
            code,
            message,
            custody: None,
        } => (code, message, true),
        // A native transport failure carries no excerpt to fund: it keeps its class.
        error @ Error::ModelUnavailable { .. } if error_has_no_excerpt(&error) => return error,
        error @ Error::ModelUnavailable { .. } => (error.code(), error.to_string(), true),
        error => return error,
    };
    match allocation.split_off(message.capacity() as u64) {
        Ok(custody) if semantic => Error::ModelDiagnostic {
            code,
            message,
            custody: Some(custody),
        },
        Ok(custody) => Error::NativeDiagnostic {
            code,
            message,
            custody,
        },
        Err(cause) => {
            drop(message);
            cause.into()
        }
    }
}

/// Apply the shared remote-response classification, then retain its original ledger.
pub fn model_diagnostic<T>(result: Result<T>, allocation: &mut NativeAllocation) -> Result<T> {
    diagnostic(result.map_err(crate::diagnostic::rejection), allocation)
}

// Count structural tokens without constructing a second JSON graph. Quoted
// punctuation and escaped quotes are payload, not allocation-bearing nodes.
/// The structural byte and node counts of BYTES, without building a second JSON graph.
pub fn shape(bytes: &[u8]) -> (u64, u64) {
    let (bytes, nodes) = acyclic_harness_core::executor::json::encoded_json_shape(bytes);
    (bytes, nodes as u64)
}

// Admission includes the temporary decoded graph, copies retained by the
// protocol state, geometric buffer growth and SDK event slots. It precedes the
// producer; only surviving capacity is transferred to the executor afterwards.
/// Claims the ledger capacity a request of BYTES and NODES needs, plus PRIOR retained bytes.
pub fn reserve(
    work: &NativeMaterializationWork,
    bytes: u64,
    nodes: u64,
    prior: u64,
) -> Result<NativeAllocation> {
    let capacity = bytes
        .checked_mul(8)
        .and_then(|n| {
            nodes
                .checked_mul(1024)
                .and_then(|slots| n.checked_add(slots))
        })
        .and_then(|n| prior.checked_mul(2).and_then(|prior| n.checked_add(prior)))
        .and_then(|n| n.checked_add(8192))
        .ok_or_else(|| invalid("provider allocation bound overflow"))?;
    let operations = nodes
        .checked_mul(16)
        .and_then(|n| n.checked_add(128))
        .ok_or_else(|| invalid("provider allocation operation bound overflow"))?;
    Ok(work.claim(
        capacity,
        operations,
        WorkCounters {
            bytes_copied: bytes.saturating_mul(8),
            items_examined: nodes,
            ..Default::default()
        },
    )?)
}

/// Count and generation admit the same immutable request projection before
/// resolving its files or constructing the protocol graph.
pub fn reserve_request(
    maximum_file_bytes: Option<u64>,
    request: &PreparedModelRequest,
    (key, account): (Option<&str>, Option<&str>),
    work: &NativeMaterializationWork,
) -> Result<NativeAllocation> {
    request.projected_file_bytes(maximum_file_bytes)?;
    let bytes = request.input_bytes()?;
    let (_, nodes) = request.projection_shape();
    reserve(
        work,
        bytes,
        nodes,
        key.map_or(0, |key| key.len() as u64) + account.map_or(0, |account| account.len() as u64),
    )
}

/// Retains BYTES of ALLOCATION, refusing a request that outgrew its admission.
pub fn retain(allocation: &mut NativeAllocation, bytes: u64) -> Result<()> {
    if bytes > allocation.bytes() {
        return Err(invalid("provider exceeded admitted allocation"));
    }
    allocation.resize(bytes)?;
    Ok(())
}

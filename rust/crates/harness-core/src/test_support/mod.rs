//! Deterministic model scripts for harness consumers and scenario tests.

/// Classifies pinned workspace bytes with the production media boundary.
pub fn workspace_media_type(bytes: &[u8]) -> &'static str {
    crate::filesystem::host::reads::media_type::classify(bytes)
}

/// Native agent-diff operand selected by the production filesystem command parser.
pub fn agent_diff(arguments: &[String]) -> Option<&str> {
    crate::tools::fs_command::agent_diff(arguments)
}

#[cfg(not(target_arch = "wasm32"))]
pub use crate::hooks::identity_test_support::assert_canonical_tool_consumers;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod accounted;
#[cfg(all(
    any(test, all(feature = "test-support", not(target_arch = "wasm32"))),
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod crash_cuts;
mod policy;
mod scripted;
pub use policy::FixedToolPolicy;

pub use scripted::{
    ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn, read_request_files,
};

/// Stages an admitted request through the production versioned producer.
pub async fn stage_model_request(
    journal: &dyn crate::executor::ExecutionJournal,
    operation: crate::OperationId,
    key: &str,
    prepared: &crate::model::PreparedModelRequest,
) -> crate::Result<crate::conversation::FileRef> {
    crate::executor::transcript::stage_request(journal, operation, key, prepared, None, None).await
}

/// Stages an unaccounted context through the production versioned producer.
pub async fn stage_context_projection(
    journal: &dyn crate::executor::ExecutionJournal,
    operation: crate::OperationId,
    key: &str,
    context: crate::context::Context,
    prepared: &crate::model::PreparedModelRequest,
) -> crate::Result<crate::conversation::FileRef> {
    crate::executor::transcript::stage_projection(
        journal,
        operation,
        key,
        &crate::executor::ResponseProjection {
            context,
            steering: None,
            canonical: None,
            checkpoint: None,
            working: None,
            accounting: None,
        },
        prepared,
        None,
        None,
    )
    .await
}

/// Observe the shipping error envelope and outcome uncertainty together.
/// Comparing only error codes would hide lost diagnostics or operation identity.
pub fn error_observation(error: &crate::Error) -> (crate::wire::Error, bool) {
    (crate::encode_error(error), error.uncertain())
}

/// Reads child-fixture controls while refusing ambient user directory discovery.
/// Available wherever this module is (unit tests on every target, or `test-support`).
pub fn environment(name: &str) -> Option<std::ffi::OsString> {
    crate::native_environment::variable_with_user_locations(name, false)
}

/// Holds automatic advancement of a paused Tokio clock during native I/O.
///
/// The test can still advance protocol deadlines explicitly. Dropping the
/// returned sender releases the blocking job, including on errors or unwinds.
#[cfg(not(target_arch = "wasm32"))]
pub fn hold_io_clock() -> tokio::sync::oneshot::Sender<()> {
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    // Tokio tracks its blocking pool as outstanding work. Native sockets and
    // the shared I/O service alone do not prevent virtual-time auto-advancement.
    drop(tokio::task::spawn_blocking(move || {
        let _ = wait.blocking_recv();
    }));
    release
}

/// A unique Streams idempotency key for a fixture write that has no retry
/// identity of its own. Stream helpers require a key; fixtures never replay.
#[cfg(test)]
pub(crate) fn fresh_key() -> Result<acyclic_stream::IdempotencyKey, acyclic_stream::StreamError> {
    acyclic_stream::IdempotencyKey::new(uuid::Uuid::new_v4().as_bytes().to_vec())
}

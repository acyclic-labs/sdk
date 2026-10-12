//! Customer-hostable Stream-backed coordinator and pull-worker admission.

use crate::contract::capability;
use crate::contract::next_revision;
use crate::store::{history::HistoryIo, operations};
use crate::wire_codec::CodecQuota;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    runtime,
    scheduler::{
        AssemblyKind, EntrypointRef, LeaseFence, OperationSpec, OperationState,
        OrchestrationDecision, Reservation, ResourceSnapshot, Scheduler, SchedulerEvent,
        assembly_invocation_digest, reduction_invocation_digest,
    },
    wire,
};
use acyclic_stream::OwnedBytes;
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream, StreamError,
    StreamProvider,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

mod authority;
mod commit;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod completion;
mod coordinator;
mod journal;
mod proof;
mod results;
mod scheduling;
mod serializer;
mod session;
#[cfg(test)]
mod tests;

#[cfg(feature = "filesystem")]
pub(crate) use authority::JournalWrite;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use commit::native_call_result;
pub(crate) use commit::{
    CommitPhase, SdkCallError, add_condition, add_record, append, commit, commit_request,
    sdk_error, sdk_source,
};
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use completion::prepare_materialization_commit;
pub use coordinator::DistributedCoordinator;
pub use journal::read_coordinator_event_page;
pub(crate) use proof::observe_declaration;
#[cfg(feature = "filesystem")]
pub(crate) use proof::original_reservation_proof;
pub use results::{ReducerRegistry, SchedulerPayloadStore};
pub use scheduling::{WorkLease, WorkPull, Worker};
pub(crate) use serializer::{digest_key, execution_path};

use authority::authorize_operation_scope;
use commit::{append_if_execution_idle, coordinator_retry_conflict};
use journal::{coordinator_boundary, coordinator_stream, next_record, read_intent, read_records};
use serializer::{
    decode, decode_json, encode, formatted_text, inspect_envelope, stream_key, stream_path,
    validate_idempotency_key,
};

const COORDINATOR_PATH: &str = "harness/v3/coordinator/events";
const COORDINATOR_WIRE_VERSION: &str = "3";
const COORDINATOR_WIRE_CONTRACT: &[u8] = b"acyclic.harness.coordinator.scheduler-event-envelope.v3";
const READ_PAGE_SIZE: u32 = 1_024;

//! Customer-hostable Stream-backed coordinator and pull-worker admission.

use crate::contract::capability;
use crate::store::{history::HistoryIo, operations};
use crate::wire_codec::CodecQuota;
use crate::contract::next_revision;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{Authority, AuthorityVerifier, Scope},
    runtime,
    scheduler::{
        AssemblyKind, EntrypointRef, LeaseFence, OperationSpec, OperationState,
        OrchestrationDecision, Reservation, ResourceRequest, Scheduler, SchedulerEvent,
        assembly_invocation_digest, reduction_invocation_digest,
    },
    wire,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamError, StreamProvider,
};
use acyclic_stream::OwnedBytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};


mod authority;
mod commit;
mod coordinator;
mod journal;
mod proof;
mod results;
mod scheduling;
mod serializer;
mod session;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod completion;
#[cfg(test)]
mod tests;

pub use coordinator::DistributedCoordinator;
pub use journal::read_coordinator_event_page;
pub use results::{ReducerRegistry, SchedulerPayloadStore};
pub use scheduling::{Worker, WorkLease, WorkPull};
pub(crate) use commit::{SdkCallError, sdk_source, sdk_error, append, commit, CommitPhase,
    commit_request, add_condition, add_record};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use commit::native_call_result;
#[cfg(feature = "filesystem")]
pub(crate) use authority::JournalWrite;
pub(crate) use proof::observe_declaration;
#[cfg(feature = "filesystem")]
pub(crate) use proof::original_reservation_proof;
pub(crate) use serializer::{execution_path, digest_key};
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use completion::prepare_materialization_commit;

use authority::authorize_operation_scope;
use commit::{coordinator_retry_conflict, append_if_execution_idle};
use journal::{read_records, coordinator_stream, read_intent, coordinator_boundary};
use serializer::{validate_idempotency_key, formatted_text, encode, decode_json, inspect_envelope,
    decode, stream_key, stream_path};

const COORDINATOR_PATH: &str = "harness/v3/coordinator/events";
const COORDINATOR_WIRE_VERSION: &str = "3";
const COORDINATOR_WIRE_CONTRACT: &[u8] = b"acyclic.harness.coordinator.scheduler-event-envelope.v3";
const READ_PAGE_SIZE: u32 = 1_024;

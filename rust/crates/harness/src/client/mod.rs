//! Bounded hypotheses over immutable authoritative references.

#![doc = include_str!("../../docs/client/README.md")]

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

mod identity;
pub use identity::{BranchId, Dependency, DependencyRequirement};
mod limits;
pub use limits::{Error, Limits};
mod outcome;
pub use outcome::{Correspondence, OperationOutcome, PredictionOutcome};
mod domain;
pub use domain::{Domain, Fact, Observation};
mod hypothesis;
pub use hypothesis::{Begin, Hypothesis, Provenance, View};
mod kernel;
pub use kernel::Client;
mod session;
pub use session::{
    ClientSession, ClientStateStore, MemoryClientState, SessionCheckpoint, SessionRecord,
};
mod transport;
pub use transport::{HarnessClient, ServiceConnector, connect_or_spawn};
mod content;
mod document_files;
mod documents;
pub use document_files::document_files;
pub use documents::DocumentViews;
mod views;
pub use views::{ClientViewHandle, ClientViewLimits, ClientViews, StatusView, SwarmChange};
mod task_compat;
pub(crate) use task_compat::views::authority_key;
pub use task_compat::{
    ClientViews as TaskFileViews, FileQueryView as TaskFileQueryView,
    GoalFileView as TaskGoalFileView, GoalProgress as TaskGoalProgress,
    GoalQueryView as TaskGoalQueryView, ModelFileView as TaskModelFileView,
    ModelQueryView as TaskModelQueryView, TaskView, ToolCallView, TranscriptView, ViewUpdate,
};
#[cfg(feature = "tui")]
pub use task_compat::{GoalPanel as TaskGoalPanel, PlanPanel as TaskPlanPanel};
#[cfg(not(target_arch = "wasm32"))]
mod journal;
#[cfg(not(target_arch = "wasm32"))]
mod journal_lock;
pub mod jsonl;
#[cfg(not(target_arch = "wasm32"))]
pub use journal::FileClientState;
mod headless;
pub use headless::{HeadlessClient, HeadlessOutcome, run_headless, run_jsonl};
mod commands;
pub use commands::*;
mod mentions;
pub use mentions::*;
mod diff;
pub use diff::*;
mod notifications;
pub use notifications::*;
mod task_list;
pub use task_list::{TaskItem, TaskList, TaskState, parse_task_list};
mod task_files;
pub use task_files::TaskFileClient;
mod command_output;
mod file_query;
mod task_projection;
pub use file_query::{FileQueryView, GoalFileView};
mod model_identity;
mod model_view;
pub use model_view::{ModelFileView, ModelQueryView};
mod goal_selection;
mod goal_view;
pub use goal_view::{GoalProgress, GoalQueryView};
#[cfg(feature = "tui")]
mod goal_panel;
#[cfg(feature = "tui")]
pub use goal_panel::GoalPanel;
#[cfg(feature = "tui")]
mod plan_panel;
#[cfg(feature = "tui")]
pub use plan_panel::PlanPanel;
mod interests;
pub use interests::{ClientInterests, InterestChanges, InterestTier};
mod interest_files;
pub use interest_files::InterestDocument;
mod swarm;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use swarm::SwarmClient;
pub use swarm::{SwarmClientLimits, SwarmUpdate};
mod file_views;
pub use file_views::QueryViewLimits;
mod query;
pub use query::ClientQuery;
mod file_submit;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) use file_submit::validate_request_key;
pub use file_submit::{FileClient, FileClientDurability, FileClientLimits};
mod file_error;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
mod retry;
#[cfg(any(test, all(feature = "wasm", target_arch = "wasm32")))]
pub(crate) mod waiting_calls;
pub use file_error::FileClientError;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) use retry::submission_retry;
mod offline;
pub use offline::validate_offline_request;
mod query_subscription;
mod session_views;
pub(crate) use file_views::FileViews;
pub(crate) use query_subscription::{PublishError, QuerySubscription, query_error};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod query_verification {
    pub use super::file_views::FileViews;
}
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use crate::wasm::swarm::WasmClientViews;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::WasmHypothesisClient;

type Record<D> = Fact<<D as Domain>::Key, <D as Domain>::Basis, <D as Domain>::Value>;
/// Persisted data specialized to a domain; never authoritative evidence.
pub type ClientHypothesis<D> = Hypothesis<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Assumption,
    <D as Domain>::Value,
>;
type Request<D> = Begin<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Assumption,
    <D as Domain>::Value,
>;

/// Evidence result specialized to a typed domain adapter.
pub type DomainObservation<D> = Observation<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Value,
>;

/// Narrow observation notification and accountable work receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Changes {
    /// The explicitly supplied demanded key acquired a new authoritative basis.
    pub authoritative: bool,
    /// Only changed hypothesis identities; no history/state serialization.
    pub hypotheses: Vec<BranchId>,
    /// Adapter steps and kernel branch/edge visits in this transition.
    pub work: usize,
}

type Resolution = (BranchId, OperationOutcome, PredictionOutcome);
#[cfg(any(all(feature = "wasm", target_arch = "wasm32"), acyclic_harness_loom))]
pub(crate) mod mailbox;
#[cfg(acyclic_harness_loom)]
pub use mailbox::verify_mailbox;

#[cfg(all(acyclic_harness_loom, not(target_arch = "wasm32")))]
mod journal_model;
#[cfg(all(acyclic_harness_loom, not(target_arch = "wasm32")))]
pub use journal_model::verify_journal;

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod local;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local::{connect_local, disconnect_local, local_documents, local_prompt};
#[cfg(all(
    feature = "filesystem-local",
    feature = "tui",
    not(target_arch = "wasm32")
))]
pub use local::{local_completions, local_ui};
#[cfg(all(feature = "terminal", not(target_arch = "wasm32")))]
mod native_input;
#[cfg(all(feature = "terminal", not(target_arch = "wasm32")))]
pub use native_input::stdin_jsonl;
#[cfg(all(feature = "terminal", feature = "tui", not(target_arch = "wasm32")))]
pub use native_input::{next_tui_event, terminal_events};

#[cfg(feature = "tui")]
mod input_files;
#[cfg(feature = "tui")]
pub use input_files::{
    apply_tui_input, file_tui_input, handle_tui_input, restore_tui_prompt, save_tui_draft,
};
#[cfg(feature = "tui")]
mod prompt_files;
#[cfg(feature = "tui")]
pub use prompt_files::PromptFiles;

#[cfg(feature = "tui")]
mod scroll_files;
#[cfg(feature = "tui")]
pub use scroll_files::{scroll_tui, tui_scroll};

#[cfg(all(
    feature = "terminal",
    feature = "tui",
    feature = "filesystem-local",
    not(target_arch = "wasm32")
))]
pub use native_input::next_file_tui_event;

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod local_headless;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local_headless::{LocalFileClient, parse_local_request, prepare_local_prompt};

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod local_identity;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local_identity::local_identity;

//! Integration behaviour tests.

#[cfg(feature = "filesystem")]
#[path = "support/stream/mod.rs"]
mod stream_fixture;

mod client_public_consumer;
mod client_transitions;
mod durable_extension_scope;
mod effects;
mod execution_journal;
mod fork_preparer;
mod history_selection;
mod local_conversation_fork;
mod model_tool_call;
mod native_media_boundary;
mod portable_file_replay;
mod portable_text_tools;
mod portable_text_variants;
mod project_workspaces;
mod provider_failure_exit;
mod recursive_fork;
mod refused_tool_conversation;
mod retained_content;
mod saved_tool_metadata;
mod scheduler_results;
mod volume_composition;
mod workflow_journal;

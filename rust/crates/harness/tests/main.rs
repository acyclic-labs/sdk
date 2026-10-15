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
mod mcp_durable;
mod native_media_boundary;
mod portable_file_replay;
mod portable_text_tools;
mod portable_text_variants;
mod project_workspaces;
mod recursive_fork;
mod retained_content;
mod scheduler_results;
mod task_workflow;
mod volume_composition;
mod workflow_journal;

//! Integration behaviour tests.
acyclic_test_support::counting_allocator!();

#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "support/progress_guard.rs"]
mod progress_guard;
#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "support/progress_registry.rs"]
mod progress_registry;
#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "e2e/stall.rs"]
mod stall;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "support/stream/mod.rs"]
mod stream_fixture;

#[cfg(feature = "test-support")]
mod client_public_consumer;
#[cfg(feature = "test-support")]
mod client_transitions;
#[cfg(feature = "test-support")]
mod durable_extension_scope;
#[cfg(feature = "test-support")]
mod effects;
#[cfg(feature = "test-support")]
mod execution_journal;
#[cfg(feature = "test-support")]
mod fork_preparer;
#[cfg(feature = "test-support")]
mod history_selection;
#[cfg(feature = "test-support")]
mod local_conversation_fork;
#[cfg(feature = "test-support")]
mod model_tool_call;
#[cfg(feature = "test-support")]
mod native_media_boundary;
#[cfg(feature = "test-support")]
mod portable_file_replay;
#[cfg(feature = "test-support")]
mod portable_text_tools;
#[cfg(feature = "test-support")]
mod portable_text_variants;
#[cfg(feature = "test-support")]
mod project_workspaces;
#[cfg(feature = "test-support")]
mod recursive_fork;
#[cfg(feature = "test-support")]
mod refused_tool_conversation;
#[cfg(feature = "test-support")]
mod retained_content;
#[cfg(feature = "test-support")]
mod saved_tool_metadata;
#[cfg(feature = "test-support")]
mod scheduler_results;
#[cfg(feature = "test-support")]
mod volume_composition;
#[cfg(feature = "test-support")]
mod workflow_journal;

// Included production source modules use these crate-root imports.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_harness::{Error, Result};

#[cfg(feature = "test-support")]
#[path = "cases/client_mention_attachments.rs"]
mod client_mention_attachments;

#[cfg(feature = "tui")]
#[path = "cases/tui_config.rs"]
mod tui_config;

#[cfg(all(feature = "tui", feature = "test-support"))]
#[path = "cases/tui_completion.rs"]
mod tui_completion;

#[cfg(feature = "test-support")]
#[path = "cases/decoder_properties.rs"]
mod decoder_properties;

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    feature = "test-support"
))]
#[path = "cases/local_task_files.rs"]
mod local_task_files;

#[cfg(feature = "filesystem")]
#[path = "cases/default_tools.rs"]
mod default_tools;

#[path = "cases/task_view.rs"]
mod task_view;

#[cfg(feature = "test-support")]
#[path = "cases/task_volume.rs"]
mod task_volume;

#[cfg(feature = "filesystem")]
#[path = "cases/file_query.rs"]
mod file_query;

#[path = "cases/client_model_views.rs"]
mod client_model_views;

#[cfg(feature = "wasm")]
#[path = "cases/client_views_parity.rs"]
mod client_views_parity;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/goal_files.rs"]
mod goal_files;

#[cfg(all(feature = "filesystem-local", feature = "test-support"))]
#[path = "cases/local_recursive_fork.rs"]
mod local_recursive_fork;

#[cfg(all(feature = "filesystem-local", feature = "test-support"))]
#[path = "cases/context_continuation.rs"]
mod context_continuation;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/context_discovery.rs"]
mod context_discovery;

#[path = "cases/filesystem_access.rs"]
mod filesystem_access;

#[path = "cases/client_history.rs"]
mod client_history;

#[path = "cases/jsonl_wire.rs"]
mod jsonl_wire;

#[cfg(feature = "mcp-oauth")]
#[path = "cases/mcp_oauth_provider.rs"]
mod mcp_oauth_provider;

#[cfg(all(
    feature = "filesystem-local",
    feature = "mcp-http",
    feature = "test-support"
))]
#[path = "cases/mcp_durable.rs"]
mod mcp_durable;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/client_commands.rs"]
mod client_commands;

#[path = "cases/client_notifications.rs"]
mod client_notifications;

#[path = "cases/plan_markdown.rs"]
mod plan_markdown;

#[path = "cases/client_interests.rs"]
mod client_interests;

#[cfg(feature = "test-support")]
#[path = "cases/client_query.rs"]
mod client_query;

#[path = "cases/client_swarm_views.rs"]
mod client_swarm_views;

#[cfg(all(feature = "filesystem-local", feature = "test-support"))]
#[path = "cases/task_workflow.rs"]
mod task_workflow;

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    feature = "test-support",
    feature = "grpc"
))]
include!("e2e/root.rs");

#[cfg(all(
    feature = "filesystem",
    feature = "search-http",
    feature = "test-support"
))]
#[path = "cases/search_http_codec.rs"]
mod search_http_codec;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/search_request.rs"]
mod search_request;

#[cfg(all(feature = "objects", feature = "test-support", feature = "grpc"))]
#[path = "local_cloud_objects/mod.rs"]
mod local_cloud_objects;

#[cfg(feature = "filesystem")]
#[path = "cases/client_diff.rs"]
mod client_diff;

#[path = "cases/client_file_submit.rs"]
mod client_file_submit;

#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "local_cloud_actors/mod.rs"]
mod local_cloud_actors;

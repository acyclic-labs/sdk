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

#[cfg(feature = "test-support")]
mod client_transitions;
#[cfg(feature = "test-support")]
mod history_selection;
#[cfg(feature = "test-support")]
mod model_tool_call;
#[cfg(feature = "test-support")]
mod portable_text_tools;
#[cfg(feature = "test-support")]
mod retained_content;
#[cfg(feature = "test-support")]
mod volume_composition;

// Included production source modules use these crate-root imports.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use acyclic_harness::{Error, Result};

#[cfg(feature = "test-support")]
#[path = "cases/client_mention_attachments.rs"]
mod client_mention_attachments;

#[cfg(feature = "test-support")]
#[path = "cases/decoder_properties.rs"]
mod decoder_properties;

#[cfg(feature = "filesystem")]
#[path = "cases/default_tools.rs"]
mod default_tools;

#[path = "cases/task_view.rs"]
mod task_view;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/goal_files.rs"]
mod goal_files;

#[cfg(all(feature = "filesystem-local", feature = "test-support"))]
#[path = "cases/context_continuation.rs"]
mod context_continuation;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/context_discovery.rs"]
mod context_discovery;

#[path = "cases/client_history.rs"]
mod client_history;

#[cfg(feature = "mcp-oauth")]
#[path = "cases/mcp_oauth_provider.rs"]
mod mcp_oauth_provider;

#[path = "cases/plan_markdown.rs"]
mod plan_markdown;

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

#[cfg(all(feature = "objects", feature = "test-support", feature = "grpc"))]
#[path = "local_cloud_objects/mod.rs"]
mod local_cloud_objects;

#[cfg(feature = "filesystem")]
#[path = "cases/client_diff.rs"]
mod client_diff;

#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "local_cloud_actors/mod.rs"]
mod local_cloud_actors;

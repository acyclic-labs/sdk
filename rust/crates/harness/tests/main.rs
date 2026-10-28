//! Integration behaviour tests.
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "support/progress_guard.rs"]
#[cfg(not(loom))]
mod progress_guard;
#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "support/progress_registry.rs"]
#[cfg(not(loom))]
mod progress_registry;
#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "e2e/stall.rs"]
#[cfg(not(loom))]
mod stall;

#[cfg(feature = "test-support")]
mod history_selection;
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
mod model_tool_call;
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
mod portable_text_tools;
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
mod retained_content;
#[cfg(feature = "test-support")]
#[cfg(not(loom))]
mod volume_composition;

#[cfg(feature = "test-support")]
#[path = "cases/client_mention_attachments.rs"]
#[cfg(not(loom))]
mod client_mention_attachments;

#[cfg(feature = "test-support")]
#[path = "cases/decoder_properties.rs"]
#[cfg(not(loom))]
mod decoder_properties;

#[cfg(feature = "filesystem")]
#[path = "cases/default_tools.rs"]
#[cfg(not(loom))]
mod default_tools;

#[path = "cases/task_view.rs"]
#[cfg(not(loom))]
mod task_view;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/goal_files.rs"]
#[cfg(not(loom))]
mod goal_files;

#[cfg(all(feature = "filesystem-local", feature = "test-support"))]
#[path = "cases/context_continuation.rs"]
#[cfg(not(loom))]
mod context_continuation;

#[cfg(all(feature = "filesystem", feature = "test-support"))]
#[path = "cases/context_discovery.rs"]
#[cfg(not(loom))]
mod context_discovery;

#[cfg(feature = "mcp-oauth")]
#[path = "cases/mcp_oauth_provider.rs"]
#[cfg(not(loom))]
mod mcp_oauth_provider;

#[path = "cases/plan_markdown.rs"]
#[cfg(not(loom))]
mod plan_markdown;

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    feature = "test-support",
    feature = "grpc"
))]
#[cfg(not(loom))]
include!("e2e/root.rs");

#[cfg(all(
    feature = "filesystem",
    feature = "search-http",
    feature = "test-support"
))]
#[path = "cases/search_http_codec.rs"]
#[cfg(not(loom))]
mod search_http_codec;

#[cfg(all(feature = "objects", feature = "test-support", feature = "grpc"))]
#[path = "local_cloud_objects/mod.rs"]
#[cfg(not(loom))]
mod local_cloud_objects;

#[cfg(feature = "filesystem")]
#[path = "cases/client_diff.rs"]
#[cfg(not(loom))]
mod client_diff;

#[cfg(all(feature = "test-support", feature = "grpc"))]
#[path = "local_cloud_actors/mod.rs"]
#[cfg(not(loom))]
mod local_cloud_actors;

// Preserve the ordinary allocator/runtime path independently of Loom schedules.
#[cfg(all(
    not(loom),
    not(target_arch = "wasm32"),
    feature = "filesystem-local",
    feature = "native-execution",
    feature = "test-support"
))]
#[path = "dst/main.rs"]
mod dst;

#[cfg(all(not(loom), feature = "filesystem", feature = "test-support"))]
mod filesystem_access;

#[cfg(all(
    not(loom),
    target_os = "linux",
    feature = "filesystem-local",
    feature = "native-execution",
    feature = "test-support"
))]
mod virtiofs;

#[cfg(all(not(loom), feature = "filesystem-local", feature = "native-execution"))]
mod host_hooks;

#[cfg(all(
    not(loom),
    not(target_arch = "wasm32"),
    feature = "filesystem-local",
    feature = "native-execution",
    feature = "test-support"
))]
mod skills_context;

#[cfg(all(loom, feature = "model-checking"))]
mod e2e_progress_model;
#[cfg(all(loom, feature = "model-checking"))]
mod client_mailbox_model;

//! Harness contract suites: pinned-file diffs, context discovery and continuation,
//! default tools, goal files, markdown task lists, search HTTP codecs, task views,
//! wire decoding and the MCP OAuth client.
#![cfg(not(loom))]

acyclic_test_support::counting_allocator!();

#[path = "cases/client_mention_attachments.rs"]
mod client_mention_attachments;

#[path = "cases/decoder_properties.rs"]
mod decoder_properties;

#[path = "cases/default_tools.rs"]
mod default_tools;

#[path = "cases/task_view.rs"]
mod task_view;

#[path = "cases/goal_files.rs"]
mod goal_files;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/context_continuation.rs"]
mod context_continuation;

#[path = "cases/context_discovery.rs"]
mod context_discovery;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/mcp_oauth_provider.rs"]
mod mcp_oauth_provider;

#[cfg(not(target_arch = "wasm32"))]
#[path = "cases/search_http_codec.rs"]
mod search_http_codec;

#[path = "cases/client_diff.rs"]
mod client_diff;

#[path = "cases/plan_markdown.rs"]
mod plan_markdown;

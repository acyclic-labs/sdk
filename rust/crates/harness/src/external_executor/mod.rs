//! External host hooks normalized into immutable task requests.

mod answer;
mod append;
#[cfg(feature = "filesystem")]
pub(crate) mod cancellation;
mod catalog;
#[cfg(feature = "filesystem")]
mod child_binding;
mod child_session;
#[cfg(all(feature = "filesystem", feature = "test-support"))]
pub(crate) use child_binding::fuzz as fuzz_child_binding;
mod client;
#[cfg(feature = "filesystem")]
mod command_catalog;
#[cfg(feature = "filesystem")]
mod commands;
#[cfg(feature = "filesystem-local")]
pub(crate) mod completion;
mod failure;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
mod handoff;
#[cfg(feature = "filesystem")]
mod intake;
#[cfg(feature = "filesystem")]
pub(crate) mod launch;
#[cfg(feature = "filesystem")]
pub mod native_context;
#[cfg(feature = "filesystem")]
pub(crate) use intake::LIFECYCLE;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod local_owner;
#[cfg(feature = "filesystem-local")]
pub(crate) mod local_paths;
mod manifest;
#[cfg(all(feature = "filesystem", feature = "hooks", not(target_arch = "wasm32")))]
mod mcp;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
mod native;
mod normalize;
#[cfg(feature = "filesystem")]
mod prepared;
#[cfg(feature = "filesystem-local")]
pub(crate) mod progress;
#[cfg(feature = "filesystem")]
mod rejection;
#[cfg(all(feature = "filesystem", feature = "test-support"))]
pub(crate) use prepared::fuzz_command;
#[cfg(feature = "filesystem")]
pub(crate) mod publication;
mod request;
mod result;
mod session;
#[cfg(feature = "filesystem-local")]
pub(crate) mod settlement;
#[cfg(feature = "filesystem-local")]
pub(crate) mod task_bindings;
#[cfg(feature = "filesystem")]
mod tool_input;
pub(crate) mod volume;

pub use answer::hook_answer;
pub(crate) use append::publish as publish_request;
pub use catalog::{ExternalHost, HookAction, HookSpec, hooks};
pub use child_session::ChildSession;
#[cfg(feature = "filesystem")]
pub(crate) use commands::ChildAdmission;
#[cfg(feature = "filesystem")]
pub use commands::RequestCommands;
pub use failure::hook_failure;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub(crate) use handoff::bound_key as native_result_key;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub use handoff::native_tool_request;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub(crate) use handoff::read_result as read_native_result;
#[cfg(feature = "filesystem")]
pub use intake::RequestConsumer;
#[cfg(all(
    feature = "test-support",
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use local_owner::fuzz_input as fuzz_local_owner_input;
#[cfg(all(
    feature = "test-support",
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use local_owner::fuzz_request as fuzz_request_record;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use local_owner::{run_local_owner, stop_local_owner};
pub use manifest::{claude_hooks, claude_manifest, owns_claude_hook};
#[cfg(all(feature = "filesystem", feature = "hooks", not(target_arch = "wasm32")))]
pub use mcp::{native_tool_catalog, serve_native_tools};
#[cfg(all(
    feature = "test-support",
    feature = "hooks",
    not(target_arch = "wasm32")
))]
pub(crate) use native::fuzz_codex_parent;
#[cfg(all(
    feature = "test-support",
    feature = "hooks",
    not(target_arch = "wasm32")
))]
pub(crate) use native::fuzz_file_input;
#[cfg(all(
    feature = "hooks",
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use native::local_hook_volumes;
#[cfg(all(
    feature = "test-support",
    feature = "hooks",
    not(target_arch = "wasm32")
))]
pub(crate) use native::prepare_hook as fuzz_prepare_hook;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub use native::{HookBinding, handle_hook, native_input_files, run_hook, run_native_files};
pub use normalize::request;
pub use request::{ExternalAction, ExternalRequest};
pub use result::tool_result;
pub use session::CommandSessionMachine;

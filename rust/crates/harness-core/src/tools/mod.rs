//! Standard coding tools and shared host executors.
pub(crate) mod children;
pub use children::{ArchiveInput, SendInput, SpawnInput, WaitInput, WaitMode};
mod agent_files;
mod agent_selection;
mod bash;
pub use bash::BashInput;
mod defaults;
pub use defaults::defaults;
pub(crate) mod catalog;
pub use catalog::{ToolBinding, ToolSpec, catalog};
mod command;
mod fs_command;
mod workspace_executor;
pub use command::parse_command;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub use fs_command::{FsCommand, parse_fs_command};
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub mod git;

#[doc(hidden)]
pub use defaults::bind_defaults;

/// The one conversion of a rejected fs/bash operand into a model-visible
/// result. Input the model must correct never becomes an executor error.
#[doc(hidden)]
pub fn rejected_input(error: crate::Error) -> crate::tool::ToolResult {
    crate::tool::ToolResult {
        value: serde_json::json!({"error": error.to_string()}),
    }
}

#[cfg(all(test, acyclic_harness_loom))]
pub(crate) use children::cancellable as cancellable_child_wait;

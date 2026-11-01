//! Standard coding tools and shared host executors.
mod agents;
pub(crate) use agents::awaited;
pub use agents::{
    AGENT_TOOLS, ArchiveAgentInput, REVISION as AGENT_TOOLS_REVISION, SendAgentInput,
    SpawnAgentInput, WaitAgentInput, WaitMode, bind_agent_tools,
    definitions as agent_tool_definitions, spawned_child,
};
pub mod shell;
pub use shell::ShellInput;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod code_mode;
mod defaults;
pub use defaults::{defaults, names as default_names};
pub(crate) mod catalog;
pub use catalog::{ToolBinding, ToolSpec, catalog};
mod bounds;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use bounds::prefix as json_string_prefix;
mod command;
mod fs_command;
mod workspace_executor;
pub use command::parse_command;
#[cfg(feature = "filesystem")]
#[doc(hidden)]
pub use fs_command::{FsCommand, parse_fs_command};
#[cfg(feature = "filesystem")]
pub mod git;

#[doc(hidden)]
pub use defaults::bind_defaults;

/// The one conversion of a rejected fs/shell operand into a model-visible
/// result. Input the model must correct never becomes an executor error.
#[doc(hidden)]
pub fn rejected_input(error: crate::Error) -> crate::tool::ToolResult {
    crate::tool::ToolResult {
        value: serde_json::json!({"error": error.to_string()}),
    }
}

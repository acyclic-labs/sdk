//! Standard coding tools and shared host executors.
mod agents;
pub use agents::{
    AGENT_TOOLS, ArchiveAgentInput, REVISION as AGENT_TOOLS_REVISION, SendAgentInput,
    SpawnAgentInput, bind_agent_tools,
};
pub mod shell;
pub use shell::ShellInput;
/// Largest chunk published by the shared tool-output writer.
#[doc(hidden)]
pub const OUTPUT_CHUNK_BYTES: usize = 64 * 1024;
#[cfg(all(
    feature = "filesystem",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod output_writer;
#[cfg(all(
    feature = "filesystem",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub use output_writer::ToolOutputWriter;
mod defaults;
pub use defaults::{defaults, names as default_names};
pub(crate) mod catalog;
pub use catalog::{ToolBinding, ToolSpec, catalog};
mod bounds;
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

/// The one conversion of a rejected fs/shell operand into a model-visible
/// result. Input the model must correct never becomes an executor error.
#[doc(hidden)]
pub fn rejected_input(error: crate::Error) -> crate::tool::ToolResult {
    crate::tool::ToolResult {
        value: serde_json::json!({"error": error.to_string()}),
    }
}

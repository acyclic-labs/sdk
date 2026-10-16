//! Standard coding tools and shared host executors.
mod bash;
pub use bash::BashInput;
mod defaults;
pub use defaults::defaults;
pub(crate) mod catalog;
pub use catalog::{ToolBinding, ToolSpec, catalog};
mod workspace_executor;
mod command;
pub use command::parse_command;
pub(crate) use command::{FsCommand, parse_fs_command};
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub mod git;

pub(crate) use defaults::bind_defaults;

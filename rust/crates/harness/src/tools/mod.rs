//! Standard coding tools and shared host executors.
mod bash;
pub use bash::BashInput;
mod defaults;
pub use defaults::defaults;
pub(crate) mod catalog;
pub use catalog::{ToolBinding, ToolSpec, catalog};
mod command;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(test, all(feature = "filesystem-local", feature = "native-execution"))
))]
mod fs_command;
mod workspace_executor;
pub use command::parse_command;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use fs_command::{FsCommand, parse_fs_command};
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub mod git;

pub(crate) use defaults::bind_defaults;

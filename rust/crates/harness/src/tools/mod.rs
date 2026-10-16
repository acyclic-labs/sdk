//! Standard coding tools and shared host executors.
mod defaults;
pub use defaults::defaults;
mod command;
pub use command::parse_command;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub mod git;

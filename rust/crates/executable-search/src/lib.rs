//! Native executable discovery and image classification.

mod command;
#[cfg(windows)]
mod executable;
mod shell;

pub use command::ExecutableSearch;
pub use shell::{host_command, platform_shell};

//! Native executable discovery and image classification.

mod command;
#[cfg(windows)]
mod executable;

pub use command::ExecutableSearch;

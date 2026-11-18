//! Native process resource and descriptor sampling.

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
pub mod resources;

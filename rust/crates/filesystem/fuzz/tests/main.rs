//! Filesystem fuzz oracles share one executable.

#[cfg(loom)]
mod model;

#[cfg(not(loom))]
mod window_state;

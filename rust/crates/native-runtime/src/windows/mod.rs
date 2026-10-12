//! Windows file I/O and detached service startup.
#![allow(unsafe_code, reason = "native operations retain kernel-visible handles and buffers")]

mod driver;
mod io;
mod service;

pub(super) use driver::{submit_read, submit_write};
pub(super) use io::{
    control_in_place, file_identity, read_at, read_at_known_overlapped, read_batch_in_place,
    set_len_once, sync_once, with_overlapped, write_all_at, write_at_known_overlapped, write_batch_in_place,
};
pub(super) use service::{spawn_service_process, take_standard_output};

#[cfg(test)]
mod tests;

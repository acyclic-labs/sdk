//! Windows filesystem adapter contracts.
#![deny(unsafe_code)]

#[cfg(windows)]
pub mod journal;
pub mod windows_usn_codec;

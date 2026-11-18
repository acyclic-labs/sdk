//! Canonical identity and object reference formats used by filesystem engines and adapters.
#![deny(unsafe_code)]

pub mod authority;
pub mod identity;
pub mod metadata;
pub mod object_reference;
pub mod s3_limits;

#[cfg(test)]
mod storage;

//! Canonical identity and object reference formats used by filesystem engines and adapters.
#![deny(unsafe_code)]

pub mod identity;
pub mod metadata;
pub mod object_reference;

#[cfg(test)]
mod storage;

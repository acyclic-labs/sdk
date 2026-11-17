#![deny(unsafe_code)]
#![no_std]
//! Pure control decisions shared by harness actors and their production proofs.

pub mod agent;
pub mod epochs;

/// Original production control source for correspondence with solver inputs.
#[cfg(feature = "test-support")]
pub const AGENT_SOURCE: &str = include_str!("agent.rs");

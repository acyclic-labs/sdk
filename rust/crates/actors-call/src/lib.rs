#![doc = include_str!("../README.md")]

/// The one invocation protocol: a caller-owned call journal.
pub mod call;
/// Per-agent spend balances staged in the original actor step transaction.
pub mod escrow;

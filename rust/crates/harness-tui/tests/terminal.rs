//! Terminal frame and negotiation suites share one executable.
#![cfg(all(test, not(loom)))]

acyclic_test_support::counting_allocator!();

#[path = "support/terminal_owner.rs"]
mod terminal_owner_support;
pub(crate) use terminal_owner_support::terminal_owner;

#[path = "terminal_negotiation.rs"]
mod terminal_negotiation;

#[path = "terminal_frames.rs"]
mod terminal_frames;

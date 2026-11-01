//! Guest-state models selected by the explicit Actors Loom runner.
#![cfg(loom)]
use acyclic_actors::guest::GuestError;
#[path = "../src/guest/state.rs"]
mod state;

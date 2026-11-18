//! Guest-state models against the compiled Actors contract.
#![cfg(all(loom, not(target_arch = "wasm32")))]

use acyclic_actors_contract::guest::{GuestError, GuestState};

#[path = "../src/guest/state/tests.rs"]
mod tests;

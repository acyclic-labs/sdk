//! Compile only this target with --cfg loom; dependencies retain normal cfgs.
#![cfg(loom)]
use acyclic_actors::guest::GuestError;
#[path = "../src/guest/state.rs"]
mod state;

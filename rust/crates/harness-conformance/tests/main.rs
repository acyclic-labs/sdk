//! Retained scenario, task and budget oracles.
#![cfg(test)]

mod case_tasks;
mod execution_budget;
#[cfg(not(target_arch = "wasm32"))]
mod machine_workspace;
mod scenarios;
mod target_lifecycle;

//! Retained scenario, task and budget oracles.
mod case_tasks;
mod execution_budget;
#[cfg(not(target_arch = "wasm32"))]
mod git_executor;
#[cfg(not(target_arch = "wasm32"))]
mod machine_workspace;
mod scenarios;
mod target_lifecycle;

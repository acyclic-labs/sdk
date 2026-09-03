//! Machine execution contract and deterministic simulator.

use acyclic_contracts::Result;
use async_trait::async_trait;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::Mutex;

/// A portable execution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRequest {
    /// Logical program name.
    pub program: String,
    /// Program arguments.
    pub args: Vec<String>,
}

/// Captured execution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionResult {
    /// Process-style exit code.
    pub code: i32,
    /// Captured standard output.
    pub stdout: Vec<u8>,
    /// Captured standard error.
    pub stderr: Vec<u8>,
}

/// Provider interface for execution placement.
#[async_trait]
pub trait MachinesProvider: Send + Sync {
    /// Executes an admitted request.
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResult>;
}

/// Deterministic simulator returning queued results.
#[derive(Clone, Default)]
pub struct SimulatedMachines {
    results: Arc<Mutex<VecDeque<ExecutionResult>>>,
}

impl SimulatedMachines {
    /// Creates a simulator with predetermined results.
    #[must_use]
    pub fn new(results: impl IntoIterator<Item = ExecutionResult>) -> Self {
        Self {
            results: Arc::new(Mutex::new(results.into_iter().collect())),
        }
    }
}

#[async_trait]
impl MachinesProvider for SimulatedMachines {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionResult> {
        if let Some(result) = self.results.lock().await.pop_front() {
            return Ok(result);
        }
        Ok(ExecutionResult {
            code: 0,
            stdout: format!("{} {}", request.program, request.args.join(" ")).into_bytes(),
            stderr: Vec::new(),
        })
    }
}

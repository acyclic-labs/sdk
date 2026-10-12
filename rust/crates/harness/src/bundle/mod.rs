//! Code-defined named bundles; bundles are constructors, not a configuration language.

use crate::{
    Capabilities, Error, Result,
    agent_loop::{AgentInput, AgentLoop, AgentRunOutput},
    context::ContextPipeline,
    conversation::Limits,
    core::{ExtensionAdmission, Reducer, SchemaRegistry},
    durable_tool::ResumableToolRegistry,
    executor::{ExecutionJournal, Executor, StockExecutor, TurnInput, TurnOutput},
    extension::ExtensionRuntime,
    model::{Model, ModelProvider},
    runtime::{
        AgentHarness, Bindings, DurableTaskHost, ExecutionProvider, InteractionResolver,
        InteractionRouter, RuntimeScope, TaskDefinition, TaskRegistry, TaskSpawner,
        TaskStateProvider, ToolPolicy,
    },
    tool::{Tool, ToolRegistry},
};
use std::sync::Arc;

mod builder;
pub use builder::HarnessBuilder;
mod bundle;
pub use bundle::HarnessBundle;
#[cfg(test)]
mod tests;

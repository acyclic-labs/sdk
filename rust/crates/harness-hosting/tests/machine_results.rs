//! Real local Machine execution observed through the ordinary served tree.
use crate::{
    OperationId,
    agent::{AgentState, IntentId, WorkspaceEntry},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{PRIVATE_VOLUME, TurnAuthority, TurnExecutors},
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_fs::LogicalObjectStore;
use acyclic_harness_client::{AgentCommand, AgentReply};
use acyclic_harness_core::*;
use acyclic_harness_hosting::hosting_test_support::*;
use acyclic_harness_hosting::*;
use acyclic_objects::Blobs;
use acyclic_stream::{BoxProviderFuture, MemoryStream, session::Client, testing::Counted};
use std::sync::Arc;

mod support;

#[path = "machine_results/cases.rs"]
mod cases;

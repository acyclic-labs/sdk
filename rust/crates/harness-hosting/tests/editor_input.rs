//! Editor input: attachments, projections and residency through the served tree.
use crate::{
    OperationId,
    agent::{AgentState, Delivered, InboxRecord, IntentId, Sender, WorkspaceEntry},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    executor::Executor,
    filesystem::{PRIVATE_VOLUME, TurnAuthority, TurnExecutors},
    model::ModelContent,
};
use acyclic_harness_client::{AgentCommand, AgentReply};
use acyclic_harness_core::agent::AgentRecord;
use acyclic_harness_core::*;
use acyclic_harness_hosting::hosting_test_support::*;
use acyclic_harness_hosting::*;
use acyclic_stream::{BoxProviderFuture, session::Client};
use prost::Message as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

mod support;

#[path = "editor_input/cases.rs"]
mod cases;

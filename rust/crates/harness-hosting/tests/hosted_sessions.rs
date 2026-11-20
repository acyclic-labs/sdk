//! Hosted session behaviour: merges, discovery pages and peer delivery.
use crate::{
    agent::{AgentPath, AgentState, Delivered, IntentId, StatusRecord, WorkspaceEntry},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::PRIVATE_VOLUME,
    model::ModelContent,
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply};
use acyclic_harness_core::agent::AgentRecord;
use acyclic_harness_core::*;
use acyclic_harness_hosting::hosting_test_support::*;
use acyclic_harness_hosting::*;
use acyclic_stream::{StreamProviderExt as _, session::Client};
use std::sync::Arc;
use std::sync::atomic::Ordering;

mod support;

#[path = "hosted_sessions/merge.rs"]
mod merge;

#[path = "hosted_sessions/session_pages.rs"]
mod session_pages;

#[path = "hosted_sessions/peer_delivery.rs"]
mod peer_delivery;

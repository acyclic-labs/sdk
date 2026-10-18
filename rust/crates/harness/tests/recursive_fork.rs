//! End-to-end recursive fork qualification across Stream and Filesystem.
#![cfg(feature = "filesystem")]
#![allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::indexing_slicing,
    reason = "one end-to-end scenario walks every fork level and indexes its own fixed fixtures"
)]

use acyclic_fs::{ConflictSide, Fs, JoinOutcome};
use acyclic_harness::conversation::{
    Attachment, ContentGrant, ContentResidencyVerifier, ConversationMessage, FileDescriptor,
    FileRef, Limits, MessageKind, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner,
    VolumeRef,
};
use acyclic_harness::core::{
    Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry, Scope,
};
use acyclic_harness::filesystem::workspace_ref;
use acyclic_harness::filesystem::{
    FilesystemContentVerifier, FilesystemForkVerifier, FilesystemHost,
    FilesystemProjectMergeVerifier, FilesystemProjectWorkspaces, ParentProjectController,
    WorkspaceMutation,
};
use acyclic_harness::fork::{
    Capture, CapturedResource, CompositeForkVerifier, ForkPreparation, ForkReport, ForkRequest,
    ForkSeed, ForkSelection, ResourceRevision, SharedGrant, StreamHistoryForkVerifier,
};
use acyclic_harness::merge::{ProjectJoinOutcome, ProjectMergeVerifier, ProjectWorkspaceProvider};
use acyclic_harness::model::{FileProjectionPolicy, ModelContent, ModelContentPart};
use acyclic_harness::projection::{ModelContextSelection, select_model_context};
use acyclic_harness::resources::{ProviderRef, StreamRef};
use acyclic_harness::store::StreamAggregate;
use acyclic_harness::{AgentId, Capabilities, IdempotencyKey, OperationId, Result};
use acyclic_stream::MemoryStream;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

const DEPTH: u16 = 1_024;
const DEEP_RETRY: u16 = DEPTH / 2;

fn identity(value: u16) -> [u8; 16] {
    let mut bytes = [0xA5; 16];
    bytes[..2].copy_from_slice(&value.to_le_bytes());
    bytes
}

fn volume(
    provider: &ProviderRef,
    class: VolumeClass,
    id: String,
    owner: VolumeOwner,
) -> Result<VolumeRef> {
    VolumeRef::new(provider.clone(), id, class, owner, &mut None)
}

fn scope(
    issuer: &AuthorityIssuer,
    private: &VolumeRef,
    shared: &VolumeRef,
    project: &VolumeRef,
) -> Result<Scope> {
    let VolumeOwner::Agent(agent) = private.owner() else {
        unreachable!("private volume has an agent owner")
    };
    let next = project
        .id()
        .rsplit('-')
        .next()
        .and_then(|id| id.parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    let selected = VolumeRef::new(
        project.provider().clone(),
        format!("{}-{next}", "project"),
        VolumeClass::Project,
        project.owner().clone(),
        &mut None,
    )?;
    let source_reads = [selected.capability(VolumeOperation::Read, &mut None)?];
    Ok(issuer
        .issue(
            "agent",
            &Capabilities::new(
                [
                    "conversation:bind".to_owned(),
                    "conversation:append".to_owned(),
                    "fork:publish".to_owned(),
                    "project:merge".to_owned(),
                    project.capability(VolumeOperation::Read, &mut None)?,
                    project.capability(VolumeOperation::Write, &mut None)?,
                    private.capability(VolumeOperation::Read, &mut None)?,
                    private.capability(VolumeOperation::Write, &mut None)?,
                    shared.capability(VolumeOperation::Read, &mut None)?,
                ]
                .into_iter()
                .chain(source_reads),
            ),
            Some(*agent),
            None,
            &mut None,
        )
        .expect("scope proof"))
}

fn command(id: u16, revision: u64, scope: &Scope, action: Action) -> Result<Command> {
    Ok(Command {
        operation_id: OperationId::from_bytes(identity(id)),
        idempotency_key: IdempotencyKey::new(format!("recursive-operation-{id}-{revision}"))?,
        expected_revision: revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    })
}

fn fork_command(id: u16, revision: u64, scope: &Scope, seed: ForkSeed) -> Result<Command> {
    let operation_id = seed.operation_id;
    let mut prepared = command(
        id,
        revision,
        scope,
        Action::PublishFork {
            seed: Box::new(seed),
        },
    )?;
    prepared.operation_id = operation_id;
    Ok(prepared)
}

#[tokio::test(flavor = "current_thread")]
async fn thousand_twenty_four_recursive_forks_keep_files_private_and_merge_only_project()
-> Result<()> {
    run_thousand_twenty_four_recursive_forks().await
}

#[path = "recursive_fork/setup.rs"]
mod setup;
use setup::*;

#[path = "recursive_fork/seed_integrity.rs"]
mod seed_integrity;
use seed_integrity::*;

#[path = "recursive_fork/publication_rejections.rs"]
mod publication_rejections;
use publication_rejections::*;

#[path = "recursive_fork/inherited_context.rs"]
mod inherited_context;
use inherited_context::*;

#[path = "recursive_fork/deep.rs"]
mod deep;
use deep::*;

#[path = "recursive_fork/siblings.rs"]
mod siblings;

fn retained_reducer(reducer: &Reducer, issuer: &AuthorityIssuer) -> Result<Reducer> {
    Reducer::restore(
        reducer.snapshot(usize::MAX, &mut None)?.0,
        issuer.verifier(&mut None)?,
        SchemaRegistry::new(),
        &mut None,
    )
}

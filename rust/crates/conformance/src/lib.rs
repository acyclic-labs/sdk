//! Reusable black-box conformance entrypoints for public providers.

/// Complete filesystem workload taxonomy, selectors, and portable vectors.
pub mod filesystem;

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs};
use acyclic_inference::InferenceProvider;
use acyclic_machines::{
    CreateMachine, IdempotencyKey, Image, MachineState, MachinesProvider, MutationOutcome,
    Performance,
};
use acyclic_objects::{GetRequest, ObjectsProvider, PutRequest, ReadTarget, wire};
use acyclic_stream::{
    AppendOutcome, AppendRequest, CommitCondition, CommitMutation, CommitOutcome, CommitRequest,
    ForkRequest, IdempotencyKey as StreamIdempotencyKey, ReadRequest, StreamError, StreamPath,
    StreamProvider,
};
use bytes::Bytes;
use futures::StreamExt;
use std::num::NonZeroU32;

/// Canonical language-neutral Objects conformance inventory.
pub const OBJECTS_SUITE: &[u8] = include_bytes!("../vectors/objects.json");

/// Canonical language-neutral Machines conformance inventory.
pub const MACHINES_SUITE: &[u8] = include_bytes!("../vectors/machines.json");

/// Canonical language-neutral Stream conformance inventory.
pub const STREAM_SUITE: &[u8] = include_bytes!("../vectors/stream.json");

/// Exercises the minimum customer-level filesystem semantics.
pub async fn filesystem_smoke<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    provider: &Fs<A, O>,
) -> Result<(), String> {
    let workspace = provider
        .create_workspace("conformance")
        .await
        .map_err(|error| error.to_string())?;
    workspace
        .write("/answer", bytes::Bytes::from_static(b"42"))
        .await
        .map_err(|error| error.to_string())?;
    let observed = workspace
        .read("/answer", 2)
        .await
        .map_err(|error| error.to_string())?;
    (observed == bytes::Bytes::from_static(b"42"))
        .then_some(())
        .ok_or_else(|| "written value missing".into())
}

/// Exercises the complete provider-independent hierarchical Stream contract.
pub async fn stream(provider: &dyn StreamProvider) -> Result<(), String> {
    if STREAM_SUITE.is_empty() {
        return Err("Stream conformance inventory is empty".into());
    }
    let parse_path = |value: &str| StreamPath::new(value).map_err(|error| error.to_string());
    let parse_key = |value: &'static [u8]| {
        StreamIdempotencyKey::new(Bytes::from_static(value)).map_err(|error| error.to_string())
    };
    let source = parse_path("conformance/source")?;
    let child = parse_path("conformance/child")?;
    let append_key = parse_key(b"stream-append")?;
    let initial = AppendRequest {
        path: source.clone(),
        records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
        if_tail: Some(0),
        idempotency_key: Some(append_key.clone()),
    };
    let first = provider
        .append(initial.clone())
        .await
        .map_err(|error| error.to_string())?;
    let AppendOutcome::Committed(first_receipt) = &first else {
        return Err("initial tail CAS conflicted".into());
    };
    if first_receipt.start != 0
        || first_receipt.end != 2
        || first_receipt.tail != 2
        || provider
            .append(initial)
            .await
            .map_err(|error| error.to_string())?
            != first
    {
        return Err("atomic append or exact replay changed its receipt".into());
    }
    let mismatch = provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"different")],
            if_tail: Some(0),
            idempotency_key: Some(append_key),
        })
        .await;
    if mismatch != Err(StreamError::IdempotencyMismatch) {
        return Err("changed idempotency arguments were accepted".into());
    }
    let conflict = provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![Bytes::from_static(b"never")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await
        .map_err(|error| error.to_string())?;
    if conflict != (AppendOutcome::TailConflict { actual_tail: 2 }) {
        return Err("tail conflict was not returned as data".into());
    }
    let fork = provider
        .fork(ForkRequest {
            source: source.clone(),
            destination: child.clone(),
            at_tail: Some(1),
            idempotency_key: Some(parse_key(b"stream-fork")?),
        })
        .await
        .map_err(|error| error.to_string())?;
    if fork.forked_at != 1 || fork.tail != 1 {
        return Err("fork did not retain the exact immutable prefix".into());
    }
    let inherited = provider
        .read(ReadRequest {
            path: child.clone(),
            from: 0,
            limit: 8,
        })
        .await
        .map_err(|error| error.to_string())?
        .collect::<Vec<_>>()
        .await;
    if inherited.len() != 1
        || inherited[0].as_ref().map_err(ToString::to_string)?.value != Bytes::from_static(b"one")
    {
        return Err("forked history was not exact".into());
    }
    let mut follow = provider
        .follow(child.clone(), 1)
        .await
        .map_err(|error| error.to_string())?;
    provider
        .append(AppendRequest {
            path: child.clone(),
            records: vec![Bytes::from_static(b"live")],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await
        .map_err(|error| error.to_string())?;
    let live = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
        .await
        .map_err(|_| "follow did not make bounded progress".to_owned())?
        .ok_or_else(|| "follow ended".to_owned())?
        .map_err(|error| error.to_string())?;
    if live.sequence != 1 || live.value != Bytes::from_static(b"live") {
        return Err("follow returned a gap or duplicate".into());
    }
    let commit_request = CommitRequest {
        conditions: vec![
            CommitCondition::Tail {
                path: source.clone(),
                expected: 2,
            },
            CommitCondition::Absent {
                path: parse_path("conformance/committed")?,
            },
        ],
        mutations: vec![CommitMutation::Fork {
            source,
            destination: parse_path("conformance/committed")?,
            at_tail: 2,
        }],
        idempotency_key: parse_key(b"stream-commit")?,
    };
    let committed = provider
        .commit(commit_request.clone())
        .await
        .map_err(|error| error.to_string())?;
    let CommitOutcome::Committed(envelope) = &committed else {
        return Err("valid coordinated commit conflicted".into());
    };
    if provider
        .commit(commit_request)
        .await
        .map_err(|error| error.to_string())?
        != committed
        || provider
            .read_commit(envelope.commit_id)
            .await
            .map_err(|error| error.to_string())?
            != *envelope
    {
        return Err("coordinated commit replay or envelope changed".into());
    }
    Ok(())
}

/// Exercises permanent versioning, exact reads, and delete-marker semantics.
pub async fn objects(provider: &dyn ObjectsProvider) -> Result<(), String> {
    if OBJECTS_SUITE.is_empty() {
        return Err("Objects conformance inventory is empty".into());
    }
    let created = provider
        .create_bucket("conformance".into(), Some("create-1".into()))
        .await
        .map_err(|error| error.to_string())?;
    let bucket = created
        .bucket
        .ok_or_else(|| "created bucket has no identity".to_owned())?;
    let version = provider
        .put(PutRequest {
            bucket: bucket.clone(),
            object_key: "answer".into(),
            body: bytes::Bytes::from_static(b"42"),
            metadata: wire::ObjectMetadata {
                content_type: "text/plain".into(),
                ..Default::default()
            },
            condition: None,
            idempotency_key: Some("put-1".into()),
        })
        .await
        .map_err(|error| error.to_string())?;
    let value = provider
        .get(GetRequest {
            target: ReadTarget::Bucket(bucket.clone()),
            object_key: "answer".into(),
            version_id: Some(version.version_id),
            range: None,
            if_match: Some(version.etag),
            if_none_match: None,
            maximum_bytes: 2,
        })
        .await
        .map_err(|error| error.to_string())?;
    if value.body.as_ref() != b"42" {
        return Err("object body mismatch".into());
    }
    let deletion = provider
        .delete(bucket, "absent".into(), None, None, Some("delete-1".into()))
        .await
        .map_err(|error| error.to_string())?;
    (deletion.existed && deletion.marker.is_some())
        .then_some(())
        .ok_or_else(|| "delete did not publish a marker".into())
}

/// Exercises the public shape-free lifecycle, replay, fork, and observation semantics.
pub async fn machines(provider: &dyn MachinesProvider) -> Result<(), String> {
    if MACHINES_SUITE.is_empty() {
        return Err("Machines conformance inventory is empty".into());
    }
    let key = |suffix: u8| {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}"))
            .map_err(|error| error.to_string())
    };
    let assurance = provider.assurance();
    let create_key = key(1)?;
    let request = CreateMachine::new(
        create_key,
        Image::custom([7; 32]).map_err(|error| error.to_string())?,
        [8; 32],
    );
    let created = provider
        .create(request.clone())
        .await
        .map_err(|error| error.to_string())?;
    let MutationOutcome::Created(machine) = created else {
        return Err("create returned the wrong outcome".into());
    };
    if provider
        .create(request)
        .await
        .map_err(|error| error.to_string())?
        != MutationOutcome::Created(machine.clone())
    {
        return Err("create replay changed its outcome".into());
    }
    if machine.state != MachineState::Running || machine.endpoints.len() != 1 {
        return Err("created machine is not ready with one stable endpoint".into());
    }
    let checkpointed = provider
        .checkpoint(machine.id, key(2)?)
        .await
        .map_err(|error| error.to_string())?;
    let MutationOutcome::Checkpointed(checkpoint) = checkpointed else {
        return Err("checkpoint returned the wrong outcome".into());
    };
    let forked = provider
        .fork(
            checkpoint.id,
            NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            Performance::Elastic,
            key(3)?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let MutationOutcome::Forked(children) = forked else {
        return Err("fork returned the wrong outcome".into());
    };
    if children.len() != 2 || children[0].id == children[1].id || children.contains(&machine) {
        return Err("fork identities are not an exact fresh set".into());
    }
    provider
        .suspend(machine.id, key(4)?)
        .await
        .map_err(|error| error.to_string())?;
    if provider
        .inspect_machine(machine.id)
        .await
        .map_err(|error| error.to_string())?
        .state
        != MachineState::Suspended
    {
        return Err("suspend did not change observable state".into());
    }
    provider
        .wake(machine.id, key(5)?)
        .await
        .map_err(|error| error.to_string())?;
    let events = provider
        .events(machine.id, None, 16)
        .await
        .map_err(|error| error.to_string())?;
    let states = events
        .events
        .iter()
        .filter_map(|event| match event.fact {
            acyclic_machines::EventFact::State(state) => Some(state),
            _ => None,
        })
        .collect::<Vec<_>>();
    if !states.windows(3).any(|states| {
        states
            == [
                MachineState::Running,
                MachineState::Suspended,
                MachineState::Running,
            ]
    }) {
        return Err("lifecycle state event history is incomplete".into());
    }
    let usage = provider
        .usage(machine.id, 1, 2)
        .await
        .map_err(|error| error.to_string())?;
    if usage.machine != machine.id
        || (assurance == acyclic_machines::ProviderAssurance::ProcessLocalSimulation)
            != usage.receipt.is_empty()
    {
        return Err("simulation usage receipt is malformed".into());
    }
    provider
        .destroy_checkpoint(checkpoint.id, key(6)?)
        .await
        .map_err(|error| error.to_string())?;
    if provider
        .inspect_checkpoint(checkpoint.id)
        .await
        .map_err(|error| error.to_string())?
        .forkable
    {
        return Err("destroyed checkpoint still accepts forks".into());
    }
    provider
        .destroy_machine(machine.id, key(7)?)
        .await
        .map_err(|error| error.to_string())?;
    if provider
        .inspect_machine(machine.id)
        .await
        .map_err(|error| error.to_string())?
        .state
        != MachineState::Destroyed
    {
        return Err("destroy did not retain its terminal state".into());
    }
    Ok(())
}

/// Exercises immutable Context, fork, Run, replay, receipt, and lifetime semantics.
pub async fn inference(provider: &dyn InferenceProvider) -> Result<(), String> {
    acyclic_inference::conformance(provider).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_memory::MemoryProfile;

    #[tokio::test]
    async fn memory_profile_conforms() -> Result<(), String> {
        let profile = MemoryProfile::new();
        filesystem_smoke(&profile.filesystem).await?;

        let stream_children = profile
            .stream
            .children(acyclic_stream::ChildrenRequest {
                parent: None,
                limit: 8,
            })
            .await
            .map_err(|error| error.to_string())?
            .collect::<Vec<_>>()
            .await;
        if !stream_children.iter().any(|child| {
            child
                .as_ref()
                .is_ok_and(|child| child.path.as_str() == "fs")
        }) {
            return Err("filesystem did not publish through the profile's public Stream".into());
        }
        let filesystem_objects = profile
            .objects
            .list(
                ReadTarget::Bucket(profile.filesystem_bucket.clone()),
                "fs/v1/".to_owned(),
                None,
                true,
                128,
                None,
            )
            .await
            .map_err(|error| error.to_string())?;
        if filesystem_objects.entries.is_empty() {
            return Err(
                "filesystem did not admit objects through the profile's public Objects".into(),
            );
        }

        stream(&profile.stream).await?;
        objects(&profile.objects).await?;
        machines(&profile.machines).await?;
        inference(&profile.inference).await?;
        Ok(())
    }
}

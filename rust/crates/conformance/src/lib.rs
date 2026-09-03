//! Reusable black-box conformance entrypoints for public providers.

use acyclic_filesystem::FilesystemProvider;
use acyclic_inference::InferenceProvider;
use acyclic_machines::{ExecutionRequest, MachinesProvider};
use acyclic_objects::{GetRequest, ObjectsProvider, PutRequest, ReadTarget, wire};
use acyclic_stream::StreamProvider;

/// Canonical language-neutral Objects conformance inventory.
pub const OBJECTS_SUITE: &[u8] = include_bytes!("../../../../conformance/vectors/objects.json");

/// Exercises the minimum filesystem semantics.
pub async fn filesystem(provider: &dyn FilesystemProvider) -> Result<(), String> {
    let base = provider.create().await.map_err(|error| error.to_string())?;
    let child = provider
        .write(&base.id, "answer".into(), b"42".to_vec())
        .await
        .map_err(|error| error.to_string())?;
    let joined = provider
        .join(&base.id, &[child.id])
        .await
        .map_err(|error| error.to_string())?;
    (joined.files.get("answer") == Some(&b"42".to_vec()))
        .then_some(())
        .ok_or_else(|| "joined value missing".into())
}

/// Exercises ordered append and compare-and-swap semantics.
pub async fn stream(provider: &dyn StreamProvider) -> Result<(), String> {
    provider
        .append("test", 0, b"one".to_vec())
        .await
        .map_err(|error| error.to_string())?;
    let values = provider
        .read("test", 0)
        .await
        .map_err(|error| error.to_string())?;
    (values.len() == 1 && values[0].offset == 0)
        .then_some(())
        .ok_or_else(|| "stream ordering mismatch".into())
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
            body: b"42".to_vec(),
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
        })
        .await
        .map_err(|error| error.to_string())?;
    if value.body != b"42" {
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

/// Exercises portable execution request semantics.
pub async fn machines(provider: &dyn MachinesProvider) -> Result<(), String> {
    let result = provider
        .execute(ExecutionRequest {
            program: "echo".into(),
            args: vec!["ok".into()],
        })
        .await
        .map_err(|error| error.to_string())?;
    (result.code == 0)
        .then_some(())
        .ok_or_else(|| "execution failed".into())
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
    async fn memory_profile_conforms() {
        let profile = MemoryProfile::default();
        assert!(filesystem(&profile.filesystem).await.is_ok());
        assert!(stream(&profile.stream).await.is_ok());
        assert!(objects(&profile.objects).await.is_ok());
        assert!(machines(&profile.machines).await.is_ok());
        assert!(inference(&profile.inference).await.is_ok());
    }
}

//! Reusable black-box conformance entrypoints for public providers.

use acyclic_filesystem::FilesystemProvider;
use acyclic_inference::{InferenceProvider, InferenceRequest};
use acyclic_machines::{ExecutionRequest, MachinesProvider};
use acyclic_objects::ObjectsProvider;
use acyclic_stream::StreamProvider;

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

/// Exercises immutable round-trip semantics.
pub async fn objects(provider: &dyn ObjectsProvider) -> Result<(), String> {
    let reference = provider
        .put(b"value".to_vec())
        .await
        .map_err(|error| error.to_string())?;
    let value = provider
        .get(&reference)
        .await
        .map_err(|error| error.to_string())?;
    (value == b"value")
        .then_some(())
        .ok_or_else(|| "object mismatch".into())
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

/// Exercises deterministic inference request semantics.
pub async fn inference(provider: &dyn InferenceProvider) -> Result<(), String> {
    let result = provider
        .complete(InferenceRequest {
            model: "deterministic".into(),
            messages: vec!["hello".into()],
        })
        .await
        .map_err(|error| error.to_string())?;
    (result.output == "hello")
        .then_some(())
        .ok_or_else(|| "inference mismatch".into())
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

//! The same public histories exercise memory and durable local providers.
use acyclic_stream::{
    MemoryLimits, MemoryStream, StreamPath,
    conformance::wave1::{self, Case},
};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn memory(case: Case) -> TestResult {
    let provider = MemoryStream::new(MemoryLimits::default());
    let namespace = StreamPath::new("wave1-memory")?;
    tokio::time::timeout(Duration::from_secs(9), wave1::verify_case(&provider, &namespace, case))
        .await.map_err(|_| format!("step=memory_history case={} timeout_seconds=9; recovery: build-slot --remote cargo test -p acyclic-stream --test wave1_keys --features local,conformance", case.name()))?
        .map_err(|failure| failure.to_string())?;
    Ok(())
}

#[cfg(feature = "local")]
async fn local(case: Case) -> TestResult {
    use acyclic_stream::{LocalStream, LocalStreamLimits};
    tokio::time::timeout(Duration::from_secs(9), async {
        let directory = tempfile::tempdir().map_err(|error| format!("step=create_local_root error={error}; recovery: build-slot --remote cargo test -p acyclic-stream --test wave1_keys --features local,conformance"))?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await
            .map_err(|error| format!("step=open_local_provider error={error}; recovery: build-slot --remote cargo test -p acyclic-stream --test wave1_keys --features local,conformance"))?;
        let namespace = StreamPath::new("wave1-local")?;
        wave1::verify_case(&provider, &namespace, case).await.map_err(|failure| failure.to_string())?;
        Ok(())
    }).await.map_err(|_| format!("step=local_history case={} timeout_seconds=9; recovery: build-slot --remote cargo test -p acyclic-stream --test wave1_keys --features local,conformance", case.name()))?
}

#[tokio::test]
async fn memory_append_retry() -> TestResult {
    memory(Case::AppendRetry).await
}
#[tokio::test]
async fn memory_commit_retry() -> TestResult {
    memory(Case::CommitRetry).await
}
#[tokio::test]
async fn memory_absent_zero_tail() -> TestResult {
    memory(Case::AbsentZeroTail).await
}
#[tokio::test]
async fn memory_subtree_fork() -> TestResult {
    memory(Case::SubtreeFork).await
}
#[tokio::test]
async fn memory_composed_fork() -> TestResult {
    memory(Case::ComposedFork).await
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local_append_retry() -> TestResult {
    local(Case::AppendRetry).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_commit_retry() -> TestResult {
    local(Case::CommitRetry).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_absent_zero_tail() -> TestResult {
    local(Case::AbsentZeroTail).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_subtree_fork() -> TestResult {
    local(Case::SubtreeFork).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_composed_fork() -> TestResult {
    local(Case::ComposedFork).await
}
#[tokio::test]
async fn memory_spawn() -> TestResult {
    memory(Case::Spawn).await
}
#[tokio::test]
async fn memory_closed_inbox() -> TestResult {
    memory(Case::ClosedInbox).await
}
#[tokio::test]
async fn memory_inherited_follow() -> TestResult {
    memory(Case::InheritedFollow).await
}
#[tokio::test]
async fn memory_fork_lineage() -> TestResult {
    memory(Case::ForkLineage).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_spawn() -> TestResult {
    local(Case::Spawn).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_closed_inbox() -> TestResult {
    local(Case::ClosedInbox).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_inherited_follow() -> TestResult {
    local(Case::InheritedFollow).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_fork_lineage() -> TestResult {
    local(Case::ForkLineage).await
}
#[tokio::test]
async fn memory_record_origin() -> TestResult {
    memory(Case::RecordOrigin).await
}
#[cfg(feature = "local")]
#[tokio::test]
async fn local_record_origin() -> TestResult {
    local(Case::RecordOrigin).await
}

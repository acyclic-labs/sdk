//! One executable filesystem suite over the existing public Fs/Workspace seam.
//! Storage adapters supply setup, never assertions. Backend types stay generic
//! because authority publication carries the caller's concrete guard and work.

use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, EmbeddedCapabilities, ForkOptions,
    Fs, Generation, IdempotencyKey, TransactionCommit, WorkBudget, WorkspaceError,
};
use acyclic_stream::{
    ProviderPlatform, StreamProvider, UnixMillisClock as _,
    testing::{Counted, RequestLedger, Requests},
};
use bytes::Bytes;
use std::sync::Arc;

mod counted;

/// Canonical language-neutral dependency/content-range vector used by existing
/// compatibility and proof producers. Exact bytes ship with this crate.
pub const DEPENDENCY_SUITE: &str = include_str!("../conformance/dependency-content-range-v1.json");

/// Production filesystem composition and accounting at its actual storage seam.
/// Retain any temporary directories, services or mounts in this backend value.
pub trait Backend: ProviderPlatform {
    /// Concrete authority owner; guards and original work are never erased.
    type Authority: AsyncAuthorityStore + 'static;
    /// Concrete immutable object owner.
    type Objects: AsyncObjectStore + 'static;
    /// Fresh disposable filesystem. Native-mount qualification additionally
    /// requires exercising its real mounted interface; this suite is portable.
    fn filesystem(&self) -> &Fs<Self::Authority, Self::Objects>;
    /// Shared ledger for authority and object dispatch, including failures.
    fn requests(&self) -> RequestLedger;
}

/// Retains a composed filesystem and its dispatch ledger.
pub struct Fixture<A: 'static, O: 'static> {
    filesystem: Fs<A, O>,
    requests: RequestLedger,
}
impl<A, O> Fixture<A, O> {
    /// Use an actually instrumented ledger. An uninstrumented ledger cannot
    /// establish request-budget qualification for an external storage backend.
    pub fn new(filesystem: Fs<A, O>, requests: RequestLedger) -> Self {
        Self {
            filesystem,
            requests,
        }
    }
}
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Backend for Fixture<A, O> {
    type Authority = A;
    type Objects = O;
    fn filesystem(&self) -> &Fs<A, O> {
        &self.filesystem
    }
    fn requests(&self) -> RequestLedger {
        self.requests.clone()
    }
}

/// Compose the real public Stream and Objects providers, instrumenting both
/// before type erasure with one shared Counted ledger. All original native work,
/// guard, error and custody forwarding remains in those primitive adapters.
pub fn from_providers<S: StreamProvider, O: acyclic_objects::v1::ObjectsProvider>(
    stream: S,
    objects: O,
    bucket: acyclic_objects::v1::wire::BucketRef,
    capabilities: EmbeddedCapabilities,
) -> Fixture<
    crate::distributed::StreamAuthorityStore<Counted<S>>,
    crate::LogicalObjectStore<Counted<O>>,
> {
    let requests = RequestLedger::new();
    let authority = crate::distributed::StreamAuthorityStore::new(Arc::new(Counted::with_ledger(
        stream,
        requests.clone(),
    )));
    let objects = crate::LogicalObjectStore::new(
        Arc::new(Counted::with_ledger(objects, requests.clone())),
        bucket,
    );
    Fixture::new(Fs::new(authority, objects, capabilities), requests)
}

/// Provider refusal or a contradicted public-contract assertion.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// Original typed workspace error.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Exact successful output did not match the vector.
    #[error("filesystem conformance: {0}")]
    Invariant(&'static str),
    /// A steady-state immutable read exceeded its dispatch budget.
    #[error("filesystem immutable read exceeded its request budget: {0:?}")]
    Budget(Requests),
}
fn value<T>(
    result: crate::MeasuredResult<crate::OperationReceipt<T>, WorkspaceError>,
) -> Result<T, Failure> {
    result
        .map(|receipt| receipt.value)
        .map_err(|failure| Failure::Workspace(failure.error))
}
fn published<A, O>(outcome: TransactionCommit<A, O>) -> Result<Generation<A, O>, Failure> {
    match outcome {
        TransactionCommit::Committed(generation) => Ok(generation),
        _ => Err(Failure::Invariant(
            "fresh single-writer publication did not commit",
        )),
    }
}
fn allow(
    _: WorkBudget,
) -> std::future::Ready<crate::MeasuredResult<crate::OperationReceipt<()>, crate::FsError>> {
    std::future::ready(Ok(crate::OperationReceipt {
        value: (),
        work: crate::WorkCounters::UNCHARGED,
    }))
}

/// Runs exact byte boundaries, immutable generations, typed read refusals,
/// cancellation, fork isolation, removal and steady-state request budgets.
/// Supply a fresh backend independently of bench. The historical workload
/// taxonomy and captured proof inventory remain available to their producers.
pub async fn verify<B: Backend + ?Sized>(backend: &B) -> Result<(), Failure> {
    let cancellation = CancellationToken::new();
    let workspace = value(
        backend
            .filesystem()
            .create_workspace("conformance", WorkBudget::UNBOUNDED, &cancellation)
            .await,
    )?;
    for size in [0usize, 1, 63, 64, 65, 4095, 4096, 4097, 131073] {
        let path = format!("/bytes-{size}");
        let bytes = Bytes::from(
            (0..size)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let old = published(workspace.write(&path, bytes.clone()).await?)?;
        let actual = value(
            workspace
                .read(&path, size as u64, WorkBudget::UNBOUNDED, &cancellation)
                .await,
        )?;
        if actual.as_ref() != bytes.as_ref() {
            return Err(Failure::Invariant(
                "whole-file read changed exact binary bytes",
            ));
        }
        published(
            workspace
                .write(&path, Bytes::from_static(b"replacement"))
                .await?,
        )?;
        let pinned = value(
            old.read(&path, size as u64, WorkBudget::UNBOUNDED, &cancellation)
                .await,
        )?;
        if pinned.as_ref() != bytes.as_ref() {
            return Err(Failure::Invariant(
                "later publication changed an immutable generation",
            ));
        }
        if size > 0 {
            let offset = size / 2;
            let length = (size - offset).min(17);
            let range = value(
                old.read_range(
                    &path,
                    offset as u64,
                    length as u64,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await,
            )?;
            if range.bytes.as_ref() != &bytes[offset..offset + length] {
                return Err(Failure::Invariant(
                    "exact immutable range changed its offset or bytes",
                ));
            }
            let refused = old
                .read(&path, size as u64 - 1, WorkBudget::UNBOUNDED, &cancellation)
                .await;
            if !matches!(refused, Err(ref failure) if matches!(failure.error, WorkspaceError::ReadLimitExceeded))
            {
                return Err(Failure::Invariant(
                    "read bound did not retain its typed refusal",
                ));
            }
        }
    }
    let missing = workspace
        .read("/absent", 1, WorkBudget::UNBOUNDED, &cancellation)
        .await;
    if !matches!(missing, Err(ref failure) if matches!(failure.error, WorkspaceError::NotFound)) {
        return Err(Failure::Invariant("missing read did not retain NotFound"));
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let refused = workspace
        .read("/bytes-1", 32, WorkBudget::UNBOUNDED, &cancelled)
        .await;
    if !matches!(refused, Err(ref failure) if matches!(failure.error, WorkspaceError::Cancelled(_)))
    {
        return Err(Failure::Invariant(
            "already-cancelled read lost its typed cancellation",
        ));
    }
    let base = value(workspace.head(WorkBudget::UNBOUNDED, &cancellation).await)?;
    let child = value(
        workspace
            .fork::<_, _, WorkspaceError>(
                "conformance-child",
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                &mut allow,
            )
            .await,
    )?;
    if value(
        child
            .read("/bytes-1", 32, WorkBudget::UNBOUNDED, &cancellation)
            .await,
    )?
    .as_ref()
        != b"replacement"
    {
        return Err(Failure::Invariant("fork lost inherited bytes"));
    }
    published(
        child
            .write("/bytes-1", Bytes::from_static(b"child-only"))
            .await?,
    )?;
    if value(
        workspace
            .read("/bytes-1", 32, WorkBudget::UNBOUNDED, &cancellation)
            .await,
    )?
    .as_ref()
        != b"replacement"
    {
        return Err(Failure::Invariant("child write changed its parent"));
    }
    published(workspace.remove("/bytes-1").await?)?;
    if !matches!(workspace.read("/bytes-1", 32, WorkBudget::UNBOUNDED, &cancellation).await,
        Err(ref failure) if matches!(failure.error, WorkspaceError::NotFound))
    {
        return Err(Failure::Invariant("removed path remained readable"));
    }
    if value(
        base.read("/bytes-1", 32, WorkBudget::UNBOUNDED, &cancellation)
            .await,
    )?
    .as_ref()
        != b"replacement"
    {
        return Err(Failure::Invariant(
            "removal changed its prior immutable generation",
        ));
    }
    workload(backend, 3).await?;
    Ok(())
}

/// Checked immutable-file read measurement; provision and warmup are excluded.
#[derive(Debug)]
pub struct Measurement {
    /// Exact 4-KiB reads completed.
    pub reads: usize,
    /// Elapsed time at millisecond resolution.
    pub elapsed: std::time::Duration,
    /// Dispatch classes over both storage providers in the measured interval.
    pub requests: Requests,
}
/// Reads 256 exact 4-KiB values from one immutable published generation with no
/// head, metadata, list, lease or token probes. Caller-owned native readers may
/// separately measure resident memory and physical transport requests.
pub async fn bench<B: Backend + ?Sized>(backend: &B) -> Result<Measurement, Failure> {
    workload(backend, 256).await
}
async fn workload<B: Backend + ?Sized>(backend: &B, rounds: usize) -> Result<Measurement, Failure> {
    let cancellation = CancellationToken::new();
    let workspace = value(
        backend
            .filesystem()
            .create_workspace("conformance-bench", WorkBudget::UNBOUNDED, &cancellation)
            .await,
    )?;
    let expected = Bytes::from(vec![0x5a; 4096]);
    let generation = published(workspace.write("/value", expected.clone()).await?)?;
    // Warm decoded metadata and data through the same public operation.
    let warmup = value(
        generation
            .read("/value", 4096, WorkBudget::UNBOUNDED, &cancellation)
            .await,
    )?;
    if warmup.as_ref() != expected.as_ref() {
        return Err(Failure::Invariant("benchmark warmup changed payload"));
    }
    let ledger = backend.requests();
    let before = ledger.requests();
    let clock = acyclic_stream::SystemUnixMillisClock;
    let started = clock.now_unix_millis();
    for _ in 0..rounds {
        let before = ledger.requests();
        let actual = value(
            generation
                .read("/value", 4096, WorkBudget::UNBOUNDED, &cancellation)
                .await,
        )?;
        let requests = ledger.requests().since(before);
        if requests.control() != 0 || requests.data() > 64 {
            return Err(Failure::Budget(requests));
        }
        if actual.as_ref() != expected.as_ref() {
            return Err(Failure::Invariant("benchmark read changed exact payload"));
        }
    }
    Ok(Measurement {
        reads: rounds,
        elapsed: std::time::Duration::from_millis(clock.now_unix_millis().saturating_sub(started)),
        requests: ledger.requests().since(before),
    })
}

/// Register all portable vectors with a fresh async backend factory.
pub use crate::filesystem_conformance_suite as suite;
/// Implementation of [`suite!`].
#[macro_export]
macro_rules! filesystem_conformance_suite {
    ($factory:path) => {
        $crate::filesystem_conformance_suite!(filesystem_conformance, $factory);
    };
    ($name:ident, $factory:path) => {
        #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
        #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
        async fn $name() -> std::result::Result<(), Box<dyn std::error::Error>> {
            let backend = $factory().await?;
            $crate::conformance::verify(&backend).await?;
            Ok(())
        }
    };
}

use super::{histories, verify_contract, wave1};
use crate::{
    AppendOutcome, AppendRequest, IdempotencyKey, OwnedBytes, ProviderPlatform, ReadRequest,
    StreamError, StreamPath, StreamProvider, UnixMillisClock as _,
    testing::{Counted, RequestLedger, Requests},
};
use futures::{StreamExt as _, TryStreamExt as _};

/// Disposable provider setup and dispatch instrumentation for the complete suite.
/// Each invocation requires a fresh domain: retained `conformance` and `wave1`
/// histories are qualification outputs, not reusable test input.
pub trait Backend: ProviderPlatform {
    /// The exact public provider under qualification.
    fn provider(&self) -> &dyn StreamProvider;
    /// Dispatch calls including failures and transport work during cursor pulls.
    fn requests(&self) -> RequestLedger;
}

/// A concrete provider instrumented before any trait-object erasure. A backend
/// counting physical transport traffic can implement [`Backend`] with that ledger.
pub struct Fixture<P> {
    provider: Counted<P>,
}

impl<P> Fixture<P> {
    /// Retain a provider and its shared method-boundary ledger for one invocation.
    pub fn new(provider: P) -> Self {
        Self {
            provider: Counted::new(provider),
        }
    }
}

impl<P: StreamProvider> Backend for Fixture<P> {
    fn provider(&self) -> &dyn StreamProvider {
        &self.provider
    }
    fn requests(&self) -> RequestLedger {
        self.provider.ledger()
    }
}

impl<P: StreamProvider> Backend for Counted<P> {
    fn provider(&self) -> &dyn StreamProvider {
        self
    }
    fn requests(&self) -> RequestLedger {
        self.ledger()
    }
}

/// Instrument the production injected RPC provider before erasing its type.
/// The port must come from the actual host; no provider or response is invented.
#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
pub fn injected(port: wasm_bindgen::JsValue) -> Result<impl Backend, StreamError> {
    Ok(Fixture::new(
        crate::wasm::provider_rpc::InjectedStream::new(port)?,
    ))
}

/// A provider failure, named history violation or steady-state request excess.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// Original provider error.
    #[error(transparent)]
    Provider(#[from] StreamError),
    /// A named key or subtree history contradicted its invariant.
    #[error(transparent)]
    History(#[from] wave1::Failure),
    /// Successful results contradicted the checked workload.
    #[error("Stream conformance: {0}")]
    Invariant(&'static str),
    /// Dispatch calls exceeded the steady-state operation's bound.
    #[error("Stream {case} exceeded its request budget: {requests:?}")]
    Budget {
        /// Operation under measurement.
        case: &'static str,
        /// Calls during that operation.
        requests: Requests,
    },
}

/// Register the complete shared suite with an async disposable backend factory.
pub use crate::stream_conformance_suite as suite;

/// Implementation of [`suite!`].
#[macro_export]
macro_rules! stream_conformance_suite {
    ($factory:path) => {
        $crate::stream_conformance_suite!(stream_conformance, $factory);
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

/// One public entry point for the complete contract, every named key/subtree
/// history, and append/read/follow zero-control request budgets.
pub async fn verify(backend: &dyn Backend) -> Result<(), Failure> {
    verify_contract(backend.provider()).await?;
    histories::wave1::verify(backend.provider(), &StreamPath::new("wave1")?).await?;
    super::atomic::verify(backend.provider()).await?;
    workload(backend, 3, "budget").await?;
    Ok(())
}

/// A checked workload shared by every backend's on-demand benchmark.
#[derive(Debug)]
pub struct Measurement {
    /// Successfully appended, read and followed records.
    pub records: usize,
    /// Elapsed wall-clock time at millisecond resolution; native runners can
    /// additionally time this invocation with a monotonic clock.
    pub elapsed: std::time::Duration,
    /// Calls after initial path creation and follow setup.
    pub requests: Requests,
}

/// Append, follow and read 256 exact 4-KiB records, without tail/head/list/token
/// queries. Supply fresh storage independently of [`verify`]. This is on demand.
pub async fn bench(backend: &dyn Backend) -> Result<Measurement, Failure> {
    workload(backend, 256, "bench").await
}

fn budget(case: &'static str, before: Requests, ledger: &RequestLedger) -> Result<(), Failure> {
    let requests = ledger.requests().since(before);
    if requests.control() != 0 || requests.data() > 1 {
        return Err(Failure::Budget { case, requests });
    }
    Ok(())
}

async fn workload(
    backend: &dyn Backend,
    rounds: usize,
    name: &str,
) -> Result<Measurement, Failure> {
    let provider = backend.provider();
    let ledger = backend.requests();
    let path = StreamPath::new(format!("conformance/{name}/value"))?;
    let warmup = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"warmup").into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(
                format!("conformance/{name}/warmup").into_bytes(),
            )?,
        })
        .await?;
    if !matches!(warmup, AppendOutcome::Committed(ref receipt) if receipt.start == 0 && receipt.end == 1)
    {
        return Err(Failure::Invariant("warmup did not append exactly once"));
    }
    let before = ledger.requests();
    let mut follow = crate::records_only(
        provider
            .follow(crate::FollowRequest::new(path.clone(), 1))
            .await?,
    );
    budget("follow open", before, &ledger)?;
    let before = ledger.requests();
    let clock = crate::SystemUnixMillisClock;
    let started = clock.now_unix_millis();
    let value = OwnedBytes::from(vec![0x5a; 4096]);
    for index in 0..rounds {
        let sequence = index as u64 + 1;
        let before = ledger.requests();
        let result = provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![value.clone().into()],
                if_tail: Some(sequence),
                idempotency_key: IdempotencyKey::new(
                    format!("conformance/{name}/{sequence}").into_bytes(),
                )?,
            })
            .await?;
        budget("append", before, &ledger)?;
        let AppendOutcome::Committed(receipt) = result else {
            return Err(Failure::Invariant("steady append conflicted"));
        };
        if receipt.start != sequence || receipt.end != sequence + 1 {
            return Err(Failure::Invariant("steady append changed its range"));
        }
        let before = ledger.requests();
        let followed = follow
            .next()
            .await
            .ok_or(Failure::Invariant("follow ended before its record"))??;
        budget("follow pull", before, &ledger)?;
        if followed.sequence != sequence
            || followed.value != value
            || followed.commit_id != receipt.commit_id
        {
            return Err(Failure::Invariant("follow changed its record"));
        }
        let before = ledger.requests();
        let read: Vec<_> = provider
            .read(ReadRequest {
                path: path.clone(),
                from: sequence,
                limit: 1,
            })
            .await?
            .try_collect()
            .await?;
        budget("read", before, &ledger)?;
        if read != [followed] {
            return Err(Failure::Invariant("read and follow disagreed"));
        }
    }
    Ok(Measurement {
        records: rounds,
        elapsed: std::time::Duration::from_millis(clock.now_unix_millis().saturating_sub(started)),
        requests: ledger.requests().since(before),
    })
}

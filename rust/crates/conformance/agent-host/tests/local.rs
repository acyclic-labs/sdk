//! The unchanged host vectors over the native durable stream adapter.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_actors::guest::Handler;
use acyclic_agent_host_conformance::{Backend, Result, Storage};
use acyclic_stream::{
    BoxProviderFuture, LocalStream, LocalStreamLimits, StreamError, StreamPath, StreamProvider,
    StreamProviderExt as _, testing::Counted,
};
use std::sync::Arc;

struct Local {
    wire: bool,
}
struct Directory(tempfile::TempDir);
impl Storage for Directory {
    fn staging_directory(&self) -> &std::path::Path {
        self.0.path()
    }
    fn objects(&self) -> BoxProviderFuture<'_, Result<acyclic_agent_host_conformance::Objects>> {
        Box::pin(async { Ok(acyclic_agent_host_conformance::Objects::memory()) })
    }

    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            Ok(
                Arc::new(LocalStream::open(self.0.path(), LocalStreamLimits::default()).await?)
                    as Arc<dyn StreamProvider>,
            )
        })
    }
}
impl Backend for Local {
    fn serving(
        &self,
        policy: acyclic_harness_core::approvals::ApprovalPolicy,
    ) -> BoxProviderFuture<'_, Result<Box<dyn acyclic_agent_host_conformance::protocol::Endpoint>>>
    {
        Box::pin(acyclic_agent_host_conformance::protocol::embedded::open(
            self, policy,
        ))
    }
    fn fresh(&self) -> BoxProviderFuture<'_, Result<Box<dyn Storage>>> {
        Box::pin(async { Ok(Box::new(Directory(tempfile::tempdir()?)) as Box<dyn Storage>) })
    }
    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
        if self.wire {
            acyclic_agent_host_conformance::remote(handler)
        } else {
            Ok(handler)
        }
    }
}

async fn native() -> Result<Local> {
    Ok(Local { wire: false })
}

async fn guest_wire() -> Result<Local> {
    Ok(Local { wire: true })
}

acyclic_agent_host_conformance::suite!(local_native, native);
acyclic_agent_host_conformance::suite!(local_guest_wire, guest_wire);

#[tokio::test]
async fn shared_benchmark_and_scale_check_native_and_guest_work() -> Result {
    // Capture a real one-call regression at the same public provider boundary.
    let directory = tempfile::tempdir()?;
    let provider =
        Counted::new(LocalStream::open(directory.path(), LocalStreamLimits::default()).await?);
    let ledger = provider.ledger();
    let absent = StreamPath::new("scale-negative-absent")?;
    let one = ledger.start_sample();
    assert_eq!(
        provider.tail(absent.clone()).await,
        Err(StreamError::NotFound)
    );
    let one = one.finish();
    let extra = ledger.start_sample();
    for _ in 0..2 {
        assert_eq!(
            provider.tail(absent.clone()).await,
            Err(StreamError::NotFound)
        );
    }
    let extra = extra.finish();
    for backend in [native().await?, guest_wire().await?] {
        let mut samples = acyclic_agent_host_conformance::scale(
            &backend,
            acyclic_agent_host_conformance::Scale {
                small: 4,
                large: 16,
            },
        )
        .await?;
        assert_eq!(samples.small.idle_agents, 4);
        assert_eq!(samples.large.idle_agents, 16);
        // Scale uses the same checked turn workload as bench. Keep its aggregate
        // checks on both retained populations instead of running it a third time.
        for sample in [&samples.small, &samples.large] {
            assert_eq!(
                sample
                    .turns
                    .iter()
                    .map(|turn| turn.requests.data())
                    .sum::<u64>(),
                sample.requests.data()
            );
            assert_eq!(
                sample
                    .turns
                    .iter()
                    .map(|turn| turn.requests.control())
                    .sum::<u64>(),
                sample.requests.control()
            );
            assert!(sample.requests.data() >= sample.deliveries as u64);
        }
        println!("AGENT_SCALE {samples:?}");

        let original_small = samples.small.turns[0];
        let original_large = samples.large.turns[0];
        samples.small.turns[0] = one;
        samples.large.turns[0] = extra;
        assert!(
            samples.validate_dispatch().is_err(),
            "extra native probe passed"
        );
        samples.small.turns[0] = original_small;

        // Equal aggregate totals cannot hide work assigned to the wrong turn.
        samples.large.turns[0].requests = samples.large.requests;
        assert!(samples.validate_dispatch().is_err(), "misbound turn passed");
        samples.large.turns[0] = original_large;

        let missing = samples.large.turns.pop().unwrap();
        assert!(samples.validate_dispatch().is_err(), "missing turn passed");
        samples.large.turns.push(missing);
        samples.validate_dispatch()?;
    }
    Ok(())
}

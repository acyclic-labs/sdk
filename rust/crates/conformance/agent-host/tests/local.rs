//! The unchanged host vectors over the native durable stream adapter.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_actors::guest::Handler;
use acyclic_agent_host_conformance::{Backend, Result, Storage};
use acyclic_stream::{BoxProviderFuture, LocalStream, LocalStreamLimits, StreamProvider};
use std::sync::Arc;

struct Local {
    wire: bool,
}
struct Directory(tempfile::TempDir);
impl Storage for Directory {
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
    for backend in [native().await?, guest_wire().await?] {
        let sample = acyclic_agent_host_conformance::bench(&backend).await?;
        assert_eq!(sample.deliveries, 64);
        assert_eq!(sample.idle_agents, 0);
        assert!(sample.requests.data() >= sample.deliveries as u64);
        println!("AGENT_BENCH {sample:?}");
        let samples = acyclic_agent_host_conformance::scale(
            &backend,
            acyclic_agent_host_conformance::Scale {
                small: 4,
                large: 16,
            },
        )
        .await?;
        assert_eq!(samples.small.idle_agents, 4);
        assert_eq!(samples.large.idle_agents, 16);
        println!("AGENT_SCALE {samples:?}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "on-demand scale qualification: creates 1,000 and 10,000 agents"]
async fn agent_scale() -> Result {
    let samples = acyclic_agent_host_conformance::scale(
        &native().await?,
        acyclic_agent_host_conformance::Scale::default(),
    )
    .await?;
    println!("AGENT_SCALE {samples:?}");
    Ok(())
}

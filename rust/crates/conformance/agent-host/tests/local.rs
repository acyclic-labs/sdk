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

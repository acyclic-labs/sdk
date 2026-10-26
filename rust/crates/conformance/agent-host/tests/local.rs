//! The unchanged host vectors over the native durable stream adapter.
#![cfg(all(feature = "local", not(target_arch = "wasm32")))]

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

#[tokio::test]
async fn local_native() -> Result {
    acyclic_agent_host_conformance::verify(&Local { wire: false }).await
}

#[tokio::test]
async fn local_guest_wire() -> Result {
    acyclic_agent_host_conformance::verify(&Local { wire: true }).await
}

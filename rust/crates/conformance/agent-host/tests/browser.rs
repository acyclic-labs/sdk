//! The unchanged host vectors over browser `IndexedDB` and the guest codec.
#![cfg(target_arch = "wasm32")]

use acyclic_actors::guest::Handler;
use acyclic_agent_host_conformance::{Backend, Result, Storage};
use acyclic_stream::{BoxProviderFuture, BrowserStream, BrowserStreamLimits, StreamProvider};
use std::sync::Arc;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

struct Browser;
struct Database(String);
impl Storage for Database {
    fn open(&self) -> BoxProviderFuture<'_, Result<Arc<dyn StreamProvider>>> {
        Box::pin(async move {
            Ok(
                Arc::new(BrowserStream::open(&self.0, BrowserStreamLimits::default()).await?)
                    as Arc<dyn StreamProvider>,
            )
        })
    }
}
impl Backend for Browser {
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
        Box::pin(async {
            Ok(
                Box::new(Database(format!("agent-host-{}", uuid::Uuid::new_v4())))
                    as Box<dyn Storage>,
            )
        })
    }
    fn handler(&self, handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
        acyclic_agent_host_conformance::remote(handler)
    }
}

async fn browser() -> Result<Browser> {
    Ok(Browser)
}

acyclic_agent_host_conformance::suite!(browser_guest_wire, browser);

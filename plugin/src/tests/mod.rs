#![allow(
    clippy::indexing_slicing,
    reason = "tests index asserted fixture shapes"
)]

use super::*;
pub(crate) use support::configured_test_service_bootstrap;
use support::native_hook_request;
use support::*;

/// Admits a request's dispatch frame and awaits it, as the transport does.
trait DispatchNow: ConcurrentControlRequestDispatcher {
    async fn dispatch_now(
        &self,
        request: ControlRequest,
        work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        self.dispatch_request(request, work)?.await
    }
}

impl<T: ConcurrentControlRequestDispatcher + ?Sized> DispatchNow for T {}

mod git;
mod hosts;
mod installation;
mod paths;
mod protocol;
mod recursive;
mod service;
mod state;
mod support;

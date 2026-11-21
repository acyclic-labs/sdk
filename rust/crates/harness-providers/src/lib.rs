//! Shared streaming adapters and immutable model selection.
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

mod admission;
mod alloy;
mod authorization;
mod call_ids;
#[cfg(all(not(target_arch = "wasm32"), feature = "local"))]
mod configured;
mod counting;
mod dispatch;
#[cfg(feature = "endpoint-presets")]
pub mod endpoints;
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub mod fuzzing;
mod input;
pub use input::validate_input;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(any(all(target_arch = "wasm32", feature = "browser"), all(test, not(loom))))]
mod prepared;
mod projection;
mod reasoning;
mod registry;
mod resolved;
mod retry;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod stream;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;
mod usage_count;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
mod wasm;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod codex_dispatch_tests;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod length_continuation_tests;

#[cfg(target_arch = "wasm32")]
pub use acyclic_harness_provider_core::BrowserTransport;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
pub use acyclic_harness_provider_core::cancellation;
pub use acyclic_harness_provider_core::{
    Adapter, AlloySelection, AlloySpec, AlloyTurn, Authentication, Credentials, HttpRequest,
    HttpResponse, HttpTransport, ModelFeatures, ModelSpec, PromptCache, ProviderConfig,
    ProviderSpec, RetrySpec, ScriptedAdapter, ScriptedProviderSpec, TimeoutSpec, TokenCountField,
    TokenCounterSpec, default_catalog,
};
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_harness_provider_core::{CodexLogin, ReqwestTransport};
pub use acyclic_harness_provider_core::{
    accounting, alloy_spec, catalog, credentials, deadline, diagnostic, event_driver, files,
    fixtures, http, spec, sse, sync, tool_names, usage_snapshot,
};
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_harness_provider_core::{allocation, catalog_document, codex};

pub use alloy::ResolvedAlloy;
pub use authorization::{
    AuthorizationScheme, CredentialBinding, CredentialResolver, CredentialSource,
};
pub use reasoning::{ProjectedReasoning, project_native, project_reasoning};
pub use registry::Registry;
pub use resolved::ResolvedModel;

#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_core::{Error, Result};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness_provider_core::{EXCERPT_BYTES, http_failure};
use acyclic_harness_provider_core::{boundary, invalid, recovery_failure};

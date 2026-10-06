#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )
)]
#![doc = include_str!("../README.md")]
#![doc = include_str!("../docs/quickstart.md")]
#![doc = include_str!("../docs/topics.md")]
#![doc = include_str!("../docs/service-availability.md")]
#![doc = include_str!("../docs/integrations.md")]
#![doc = include_str!("../docs/filesystem.md")]
#![doc = include_str!("../docs/grpc.md")]
#![doc = include_str!("../docs/machines.md")]
#![doc = include_str!("../docs/managed-agent-runtime.md")]
#![doc = include_str!("../docs/objects.md")]

#[cfg(target_arch = "wasm32")]
use std::future::Future;
use futures::Stream;

pub mod agent_loop;
pub mod bundle;
pub mod context;
mod contract;
pub mod conversation;
pub mod core;
pub mod distributed;
pub mod durable_host;
pub mod durable_tool;
pub mod effect_host;
pub mod effects;
pub mod executor;
pub mod extension;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub mod filesystem;
pub mod fork;
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
mod handles;
pub mod integrations;
pub mod interaction;
#[cfg(not(target_arch = "wasm32"))]
pub mod live;
#[cfg(target_arch = "wasm32")]
#[path = "live_wasm.rs"]
pub mod live;
#[cfg(feature = "machines")]
pub mod machines;
pub mod managed_agent_runtime;
#[cfg(any(test, feature = "filesystem", target_arch = "wasm32"))]
pub(crate) mod memory_store;
pub mod merge;
pub mod model;
#[cfg(feature = "objects")]
pub mod objects;
pub mod projection;
pub mod registry;
pub mod resources;
pub mod runtime;
pub mod scheduler;
pub mod store;
pub mod tool;
pub mod turn;
#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
/// Browser capability description returned by the remote Harness service.
pub use acyclic_sdk_remote_web::{BrowserHarnessCapabilities, BrowserHarnessClient};
pub mod wire_api;
mod wire_codec;
/// Encodes a Harness error for a wire response.
pub use wire_codec::encode_error;
pub mod wire_validation;
pub mod wire_values;
pub mod workflow;

/// Future ABI used by the harness traits. Native providers may cross worker
/// threads; browser providers remain on the browser executor and therefore
/// use local futures.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type BoxFuture<'a, T> = futures::future::BoxFuture<'a, T>;
#[cfg(target_arch = "wasm32")]
pub(crate) type BoxFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;
pub(crate) type SendBoxFuture<'a, T> = futures::future::BoxFuture<'a, T>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type PlatformBoxStream<'a, T> = futures::stream::BoxStream<'a, T>;
#[cfg(target_arch = "wasm32")]
pub(crate) type PlatformBoxStream<'a, T> = futures::stream::LocalBoxStream<'a, T>;

/// Box a future using the executor model of the compiled target.  Native
/// builds retain Send futures; browser builds remain on the local executor.
#[cfg(target_arch = "wasm32")]
pub(crate) trait PlatformFutureExt: Future + Sized {
    fn platform_boxed<'a>(self) -> BoxFuture<'a, Self::Output>
    where
        Self: 'a;
}

pub(crate) trait PlatformStreamExt: Stream + Sized {
    fn platform_boxed<'a>(self) -> PlatformBoxStream<'a, Self::Item>
    where
        Self: 'a;
}

#[cfg(not(target_arch = "wasm32"))]
impl<S: Stream + Send> PlatformStreamExt for S {
    fn platform_boxed<'a>(self) -> PlatformBoxStream<'a, Self::Item>
    where
        Self: 'a,
    {
        Box::pin(self)
    }
}

#[cfg(target_arch = "wasm32")]
impl<S: Stream> PlatformStreamExt for S {
    fn platform_boxed<'a>(self) -> PlatformBoxStream<'a, Self::Item>
    where
        Self: 'a,
    {
        Box::pin(self)
    }
}

#[cfg(target_arch = "wasm32")]
impl<F: Future> PlatformFutureExt for F {
    fn platform_boxed<'a>(self) -> BoxFuture<'a, Self::Output>
    where
        Self: 'a,
    {
        Box::pin(self)
    }
}

/// Platform-specific marker bounds for pluggable Harness services.
///
/// Native providers cross worker threads; browser providers remain on the
/// browser executor and therefore intentionally do not require `Send` or
/// `Sync`. Keeping this boundary in Rust lets every generated facade inherit
/// the same platform behavior without consumer feature flags.
#[doc(hidden)]
#[cfg(not(target_arch = "wasm32"))]
pub trait PlatformServiceBounds: Send + Sync {}
#[doc(hidden)]
#[cfg(target_arch = "wasm32")]
pub trait PlatformServiceBounds {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: ?Sized + Send + Sync> PlatformServiceBounds for T {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> PlatformServiceBounds for T {}

/// Bounds for live task callbacks, which are thread-safe only on native
/// targets. Browser callbacks stay on the local executor.
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub trait PlatformTaskCallback: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> PlatformTaskCallback for T {}
#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub trait PlatformTaskCallback {}
#[cfg(target_arch = "wasm32")]
impl<T> PlatformTaskCallback for T {}

#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub trait PlatformTaskFuture: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> PlatformTaskFuture for T {}
#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub trait PlatformTaskFuture {}
#[cfg(target_arch = "wasm32")]
impl<T> PlatformTaskFuture for T {}

/// Generated Protobuf packages, nested as their package names are, so the
/// harness messages resolve the shared protocol handshake they import.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::too_many_lines,
    clippy::large_enum_variant
)]
mod generated {
    /// Generated protobuf package namespace.
    pub mod acyclic {
        /// Harness service messages and envelopes.
        pub mod harness {
            /// Version-two harness wire contract.
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v2.rs"));
            }
        }
        /// Shared protocol negotiation messages.
        pub mod protocol {
            /// Version-one protocol handshake contract.
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.protocol.v1.rs"));
            }
        }
    }
}

/// Generated Protobuf envelopes shared by every transport: the harness
/// contract and the protocol handshake it negotiates with.
pub mod wire {
    /// Version-two Harness wire messages.
    pub use super::generated::acyclic::harness::v2::*;
    /// Version-one protocol handshake messages.
    pub use super::generated::acyclic::protocol::v1::*;
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/harness_descriptor.bin"));

/// Builder for a configured Harness bundle.
pub use bundle::HarnessBuilder;
/// Complete collection of registered Harness services.
pub use bundle::HarnessBundle as Harness;
/// Authenticated task admission payload.
pub use contract::Admission;
/// Stable agent identity.
pub use contract::AgentId;
/// Authority level used by policy evaluation.
pub use contract::AuthorityLevel;
/// Immutable authority policy definition.
pub use contract::AuthorityPolicy;
/// Stable batch identity.
pub use contract::BatchId;
/// Exact component labels that are forbidden by the contract.
pub use contract::COMPONENT_LABEL_FORBIDDEN_EXACT;
/// Separators forbidden in component labels.
pub use contract::COMPONENT_LABEL_FORBIDDEN_SEPARATORS;
/// Maximum encoded component-label length.
pub use contract::COMPONENT_LABEL_MAX_BYTES;
/// Capability set carried by an authority scope.
pub use contract::Capabilities;
/// Stable conversation identity.
pub use contract::ConversationId;
/// Stable effect-attempt identity.
pub use contract::EffectAttemptId;
/// Stable effect identity.
pub use contract::EffectId;
/// Harness error type.
pub use contract::Error;
/// Stable task-group identity.
pub use contract::GroupId;
/// Idempotency key for a durable operation.
pub use contract::IdempotencyKey;
/// Stable interaction identity.
pub use contract::InteractionId;
/// Rejection details for an interaction response.
pub use contract::InteractionRejection;
/// Stable operation identity.
pub use contract::OperationId;
/// Result of a completed Harness operation.
pub use contract::Outcome;
/// One layer in the effective authority policy.
pub use contract::PolicyLayer;
/// Negotiated Harness protocol identity.
pub use contract::ProtocolIdentity;
/// Crate result alias.
pub use contract::Result;
/// Stable session identity.
pub use contract::SessionId;
/// Stable task identity.
pub use contract::TaskId;
/// Stable turn identity.
pub use contract::TurnId;
/// Validates a component label against the Harness contract.
pub use contract::is_valid_component_label;
/// Resolves the effective policy for an authority.
pub use contract::resolve_policies;
/// Resolves policy layers in their canonical order.
pub use contract::resolve_policy_layers;
/// Stable extension identity.
pub use extension::ExtensionIdentity;
/// Lease granted to an extension.
pub use extension::ExtensionLease;
/// Collection of active extension leases.
pub use extension::ExtensionLeases;
/// Links extension dependencies to their runtime implementations.
pub use extension::ExtensionLinker;
/// Registry of installed extensions.
pub use extension::ExtensionRegistry;
/// Runtime used to execute extension hooks.
pub use extension::ExtensionRuntime;
/// Native extension implementation boundary.
pub use extension::NativeExtension;
/// Bundle of native extension implementations.
pub use extension::NativeExtensionBundle;
/// Public agent handle.
pub use handles::Agent;
/// Public conversation handle.
pub use handles::Conversation;
/// Public session handle.
pub use handles::Session;
/// Public task handle.
pub use handles::Task;
/// Public turn handle.
pub use handles::Turn;
/// Group of live tasks sharing one lifecycle.
pub use live::TaskGroup;
/// Handle for observing or controlling a live task.
pub use live::TaskHandle;
/// Streams task completions as they arrive.
pub use live::completion_stream;
/// Returns the first successful live-task result.
pub use live::first_success;
/// Joins all live-task results.
pub use live::join_all;
/// Reduces live-task results in input order.
pub use live::ordered_reduce;
/// Resolves a result once a quorum completes.
pub use live::quorum;
/// Returns the first completed live-task result.
pub use live::race;
/// Recursively combines a live-task result stream.
pub use live::recursive_sum;

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
pub use acyclic_sdk_remote_web::{BrowserHarnessCapabilities, BrowserHarnessClient};
pub mod wire_api;
mod wire_codec;
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

#[cfg(not(target_arch = "wasm32"))]
impl<F: Future + Send> PlatformFutureExt for F {
    fn platform_boxed<'a>(self) -> BoxFuture<'a, Self::Output>
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
pub trait PlatformServiceBounds {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: ?Sized + Send + Sync> PlatformServiceBounds for T {}

#[cfg(target_arch = "wasm32")]
impl<T: ?Sized + Send + Sync> PlatformServiceBounds for T {}

/// Bounds for live task callbacks, which are thread-safe only on native
/// targets. Browser callbacks stay on the local executor.
pub(crate) trait PlatformTaskCallback {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> PlatformTaskCallback for T {}
#[cfg(target_arch = "wasm32")]
impl<T: Send + Sync> PlatformTaskCallback for T {}

pub(crate) trait PlatformTaskFuture {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> PlatformTaskFuture for T {}
#[cfg(target_arch = "wasm32")]
impl<T: Send> PlatformTaskFuture for T {}

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
    pub use super::generated::acyclic::harness::v2::*;
    pub use super::generated::acyclic::protocol::v1::*;
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/harness_descriptor.bin"));

pub use bundle::{HarnessBuilder, HarnessBundle as Harness};
pub use contract::{
    Admission, AgentId, AuthorityLevel, AuthorityPolicy, BatchId, COMPONENT_LABEL_FORBIDDEN_EXACT,
    COMPONENT_LABEL_FORBIDDEN_SEPARATORS, COMPONENT_LABEL_MAX_BYTES, Capabilities, ConversationId,
    EffectAttemptId, EffectId, Error, GroupId, IdempotencyKey, InteractionId, InteractionRejection,
    OperationId, Outcome, PolicyLayer, ProtocolIdentity, Result, SessionId, TaskId, TurnId,
    is_valid_component_label, resolve_policies, resolve_policy_layers,
};
pub use extension::{
    ExtensionIdentity, ExtensionLease, ExtensionLeases, ExtensionLinker, ExtensionRegistry,
    ExtensionRuntime, NativeExtension, NativeExtensionBundle,
};
pub use handles::{Agent, Conversation, Session, Task, Turn};
pub use live::{
    TaskGroup, TaskHandle, completion_stream, first_success, join_all, ordered_reduce, quorum,
    race, recursive_sum,
};

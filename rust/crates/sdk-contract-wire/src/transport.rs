//! Rust-owned transport availability and deterministic selection policy.
//!
//! A family can expose more than one client transport, but a consumer should
//! not have to choose one just to connect.  This module records the transports
//! qualified for each runtime and selects the first compatible option.  The
//! table describes SDK transport capability, not backend deployment
//! availability: an endpoint can still reject a call with its normal
//! authentication or application error.

/// A client transport understood by the public SDKs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// Native HTTP/2 gRPC with protobuf messages.
    Grpc,
    /// Browser-safe gRPC framing over `fetch`.
    GrpcWeb,
    /// The Rust-owned protobuf-JSON/JSON-lines HTTP projection.
    HttpJson,
}

/// The host runtime in which a consumer constructs a client.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClientRuntime {
    /// A native runtime that can open a gRPC channel.
    #[default]
    Native,
    /// A browser runtime. Native gRPC is never selected here.
    Browser,
}

/// Capabilities required by one operation or facade.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportRequirements {
    /// The selected transport must preserve the family's streaming methods.
    pub streaming: bool,
    /// The selected transport must carry the bearer credential policy.
    pub bearer_auth: bool,
}

/// One qualified transport option for a runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportOption {
    /// Wire transport selected by this option.
    pub kind: TransportKind,
    /// Whether this option supports the family's advertised streaming methods.
    pub streaming: bool,
    /// Whether this option carries the family's bearer authentication policy.
    pub bearer_auth: bool,
}

/// Ordered transport options for one family and runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeTransportPolicy {
    /// Options are ordered from preferred to fallback transport.
    pub options: &'static [TransportOption],
}

/// Transport policy attached to one entry in [`crate::FAMILY_VIEWS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilyTransportPolicy {
    /// Qualified options for native consumers.
    pub native: RuntimeTransportPolicy,
    /// Qualified options for browser consumers.
    pub browser: RuntimeTransportPolicy,
}

impl FamilyTransportPolicy {
    /// Return options for a runtime in deterministic preference order.
    pub const fn for_runtime(self, runtime: ClientRuntime) -> RuntimeTransportPolicy {
        match runtime {
            ClientRuntime::Native => self.native,
            ClientRuntime::Browser => self.browser,
        }
    }
}

/// An optional caller preference and the capabilities needed by the call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportSelectionRequest {
    /// Runtime in which the client is executing.
    pub runtime: ClientRuntime,
    /// Required operation capabilities.
    pub requirements: TransportRequirements,
    /// An explicit transport override. Overrides are validated before a call.
    pub override_kind: Option<TransportKind>,
}

/// The result of transport selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportSelection {
    /// Stable family name selected from the Rust family registry.
    pub family: &'static str,
    /// Runtime used for filtering.
    pub runtime: ClientRuntime,
    /// Qualified transport to construct.
    pub kind: TransportKind,
}

/// Why a requested transport could not be selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSelectionError {
    /// The family is absent from the unified Rust registry.
    UnknownFamily,
    /// No qualified option satisfies the requested capabilities.
    NoCompatibleTransport,
    /// An explicit override is unavailable or lacks a required capability.
    UnsupportedOverride,
}

/// Select a transport from one Rust-owned family view.
///
/// Selection is deterministic: native consumers prefer gRPC, while browser
/// consumers consider only gRPC-Web and HTTP JSON. An explicit override is
/// checked against the same capability requirements and fails before any
/// request is sent. Selection does not retry, downgrade after an authorization
/// or application error, or replay a non-idempotent operation.
pub fn select_transport(
    family: &'static crate::family_registry::FamilyView,
    request: TransportSelectionRequest,
) -> Result<TransportSelection, TransportSelectionError> {
    let options = family.transport.for_runtime(request.runtime).options;
    let compatible = |option: &&TransportOption| {
        (!request.requirements.streaming || option.streaming)
            && (!request.requirements.bearer_auth || option.bearer_auth)
    };

    if let Some(override_kind) = request.override_kind {
        let option = options.iter().find(|option| option.kind == override_kind);
        return match option {
            Some(option) if compatible(&option) => Ok(TransportSelection {
                family: family.name,
                runtime: request.runtime,
                kind: option.kind,
            }),
            _ => Err(TransportSelectionError::UnsupportedOverride),
        };
    }

    options
        .iter()
        .find(compatible)
        .map(|option| TransportSelection {
            family: family.name,
            runtime: request.runtime,
            kind: option.kind,
        })
        .ok_or(TransportSelectionError::NoCompatibleTransport)
}

/// Select a transport by the stable family name in the unified registry.
pub fn select_transport_by_name(
    family: &str,
    request: TransportSelectionRequest,
) -> Result<TransportSelection, TransportSelectionError> {
    let family = crate::family_registry::family_view(family)
        .ok_or(TransportSelectionError::UnknownFamily)?;
    select_transport(family, request)
}

const GRPC: TransportOption = TransportOption {
    kind: TransportKind::Grpc,
    streaming: true,
    bearer_auth: true,
};
const GRPC_UNARY: TransportOption = TransportOption {
    kind: TransportKind::Grpc,
    streaming: false,
    bearer_auth: true,
};
const GRPC_WEB: TransportOption = TransportOption {
    kind: TransportKind::GrpcWeb,
    streaming: true,
    bearer_auth: true,
};
const HTTP_JSON: TransportOption = TransportOption {
    kind: TransportKind::HttpJson,
    streaming: true,
    bearer_auth: true,
};
const HTTP_JSON_UNARY: TransportOption = TransportOption {
    kind: TransportKind::HttpJson,
    streaming: false,
    bearer_auth: true,
};

const ACTORS_NATIVE: &[TransportOption] = &[GRPC_UNARY, HTTP_JSON_UNARY];
const ACTORS_BROWSER: &[TransportOption] = &[HTTP_JSON_UNARY];
const WORKERS_NATIVE: &[TransportOption] = &[GRPC_UNARY, HTTP_JSON_UNARY];
const WORKERS_BROWSER: &[TransportOption] = &[HTTP_JSON_UNARY];
const OBJECTS_NATIVE: &[TransportOption] = &[GRPC, HTTP_JSON];
const OBJECTS_BROWSER: &[TransportOption] = &[HTTP_JSON];
const STREAM_NATIVE: &[TransportOption] = &[GRPC, HTTP_JSON];
const STREAM_BROWSER: &[TransportOption] = &[HTTP_JSON];
const INFERENCE_NATIVE: &[TransportOption] = &[GRPC];
const INFERENCE_BROWSER: &[TransportOption] = &[HTTP_JSON];
const MACHINES_NATIVE: &[TransportOption] = &[GRPC];
const FILESYSTEM_NATIVE: &[TransportOption] = &[GRPC];
const FILESYSTEM_BROWSER: &[TransportOption] = &[GRPC_WEB];
const HARNESS_NATIVE: &[TransportOption] = &[GRPC];

/// Transport policy for a family with a native gRPC client and HTTP JSON
/// projection whose operations are unary.
pub const ACTORS_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: ACTORS_NATIVE },
    browser: RuntimeTransportPolicy { options: ACTORS_BROWSER },
};

/// Transport policy for Workers.
pub const WORKERS_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: WORKERS_NATIVE },
    browser: RuntimeTransportPolicy { options: WORKERS_BROWSER },
};

/// Transport policy for Objects.
pub const OBJECTS_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: OBJECTS_NATIVE },
    browser: RuntimeTransportPolicy { options: OBJECTS_BROWSER },
};

/// Transport policy for Stream.
pub const STREAM_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: STREAM_NATIVE },
    browser: RuntimeTransportPolicy { options: STREAM_BROWSER },
};

/// Transport policy for Inference.
pub const INFERENCE_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: INFERENCE_NATIVE },
    browser: RuntimeTransportPolicy { options: INFERENCE_BROWSER },
};

/// Transport policy for Machines. No canonical browser transport is claimed.
pub const MACHINES_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: MACHINES_NATIVE },
    browser: RuntimeTransportPolicy { options: &[] },
};

/// Transport policy for Filesystem's hosted gRPC and browser gRPC-Web clients.
pub const FILESYSTEM_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: FILESYSTEM_NATIVE },
    browser: RuntimeTransportPolicy { options: FILESYSTEM_BROWSER },
};

/// Transport policy for Harness's canonical native gRPC service.
pub const HARNESS_TRANSPORT: FamilyTransportPolicy = FamilyTransportPolicy {
    native: RuntimeTransportPolicy { options: HARNESS_NATIVE },
    browser: RuntimeTransportPolicy { options: &[] },
};

#[cfg(test)]
mod tests {
    use super::*;

    fn request(runtime: ClientRuntime) -> TransportSelectionRequest {
        TransportSelectionRequest {
            runtime,
            requirements: TransportRequirements {
                streaming: false,
                bearer_auth: true,
            },
            override_kind: None,
        }
    }

    #[test]
    fn native_defaults_to_grpc_and_browser_never_selects_native_grpc() {
        assert_eq!(
            select_transport_by_name("objects", request(ClientRuntime::Native))
                .unwrap()
                .kind,
            TransportKind::Grpc
        );
        assert_eq!(
            select_transport_by_name("objects", request(ClientRuntime::Browser))
                .unwrap()
                .kind,
            TransportKind::HttpJson
        );
    }

    #[test]
    fn explicit_override_is_checked_before_selection() {
        let mut browser = request(ClientRuntime::Browser);
        browser.override_kind = Some(TransportKind::Grpc);
        assert_eq!(
            select_transport_by_name("objects", browser),
            Err(TransportSelectionError::UnsupportedOverride)
        );

        let mut native = request(ClientRuntime::Native);
        native.override_kind = Some(TransportKind::HttpJson);
        assert_eq!(
            select_transport_by_name("objects", native).unwrap().kind,
            TransportKind::HttpJson
        );
    }

    #[test]
    fn required_streaming_rejects_unqualified_family_transport() {
        let mut actors = request(ClientRuntime::Browser);
        actors.requirements.streaming = true;
        assert_eq!(
            select_transport_by_name("actors", actors),
            Err(TransportSelectionError::NoCompatibleTransport)
        );
        actors.runtime = ClientRuntime::Native;
        assert_eq!(
            select_transport_by_name("actors", actors),
            Err(TransportSelectionError::NoCompatibleTransport)
        );
        assert_eq!(
            select_transport_by_name("inference", TransportSelectionRequest {
                runtime: ClientRuntime::Browser,
                requirements: TransportRequirements {
                    streaming: true,
                    bearer_auth: true,
                },
                override_kind: None,
            })
            .unwrap()
            .kind,
            TransportKind::HttpJson
        );
    }

    #[test]
    fn unknown_family_is_rejected() {
        assert_eq!(
            select_transport_by_name("missing", request(ClientRuntime::Native)),
            Err(TransportSelectionError::UnknownFamily)
        );
    }

    #[test]
    fn registry_transport_inventory_has_no_unqualified_browser_or_http_option() {
        for family in crate::FAMILY_VIEWS {
            assert!(
                !family.transport.native.options.is_empty(),
                "{} must expose a native transport option",
                family.name
            );
            assert!(family
                .transport
                .browser
                .options
                .iter()
                .all(|option| option.kind != TransportKind::Grpc));
            assert!(family
                .transport
                .native
                .options
                .iter()
                .chain(family.transport.browser.options.iter())
                .filter(|option| option.kind == TransportKind::HttpJson)
                .all(|_| family.has_http_projection()));
        }
    }
}

//! Canonical Stream protobuf messages, services and RPC authorization.

pub mod authorization;
/// Canonical change-record decoding.
pub mod changes;
/// Canonical record header grammar.
pub mod headers;
/// Hosted token operation vocabulary derived from Stream admission.
pub mod token;
/// Canonical input grammar for Stream paths and retry identities.
pub mod validation;

/// Generated canonical Stream v1 protocol.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    // All session data variants have total equality, including repeated headers.
    impl Eq for SessionData {}
    include!(concat!(env!("OUT_DIR"), "/acyclic.stream.v1.rs"));
}
/// Canonical public descriptor set used by compatibility gates.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-stream-v1.bin"));

/// Compiler-linked admission for generated Stream service methods.
pub mod rpc {
    use acyclic_auth_action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/authorization.rs"));
}

macro_rules! hosted_http_routes {
    ($(($path:literal, $response:literal, $method:ident)),+ $(,)?) => {
        /// Route-to-response families for the hosted HTTP projection.
        pub const HTTP_RESPONSE_CONTRACT: &[(&str, &str)] = &[
            $(($path, $response),)+
            ("tokens/create", "token"),
        ];

        /// Resolves a hosted HTTP path to its canonical path and generated RPC identity.
        /// Use the protobuf descriptor for the transport shape of methods such as server-streaming Read.
        /// Token issuance is a hosted operation outside the generated Stream service.
        pub fn hosted_http_rpc(path: &str) -> Option<(&'static str, rpc::StreamRpc)> {
            match path {
                $($path => Some(($path, rpc::StreamRpc::$method)),)+
                _ => None,
            }
        }
    };
}

hosted_http_routes! {
    ("head", "head", Head),
    ("heads", "heads", Heads),
    ("idempotency/inspect", "observation", InspectIdempotency),
    ("tail", "tail", Tail),
    ("append", "append", Append),
    ("fork", "fork", Fork),
    ("read", "read", Read),
    ("children/page", "children_page", ChildrenPage),
    ("commit", "commit", Commit),
    ("commits/read", "envelope", ReadCommit),
}

//! Rust-owned Actors v1 contract.
//!
//! Contract derivation derives the prost wire implementation and the complete protobuf
//! schema from these declarations. The tonic service facade below remains a
//! generated transport adapter and consumes these same message types.

mod generated {
    #![allow(
        missing_docs,
        reason = "Contract derivation emits the public schema declarations and documentation belongs to the semantic Rust source"
    )]

    // Semantic declarations own the message fields and Proto shadows. The
    // registration file below contains only package/file/service wiring.
    #[allow(
        dead_code,
        reason = "Public semantic constructors and accessors are consumed by SDK bridges"
    )]
    #[allow(
        unexpected_cfgs,
        reason = "Kani supplies the cfg predicate during formal verification"
    )]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Contract derivation conversion signatures preserve owned semantic values"
    )]
    #[allow(
        clippy::redundant_closure,
        reason = "Contract derivation conversion attributes require closure-shaped validators"
    )]
    #[allow(
        clippy::unnecessary_fallible_conversions,
        reason = "Contract derivation conversion attributes preserve fallible ingress boundaries"
    )]
    pub mod domain {
        include!("domain.rs");
    }
    pub use domain::*;

    include!("contract_definitions.rs");
}

pub(crate) use generated::ActorsFile;
#[allow(
    unused_imports,
    reason = "The semantic domain module is a public bridge namespace for SDK consumers"
)]
pub use generated::domain;

/// Renders the canonical Actors protobuf input for the maintained prost/tonic
/// build. The rendered file is an intermediate artifact; these Rust
/// declarations remain the contract authority.
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    ActorsFile::render(root.as_ref())
}

#[allow(
    unused_imports,
    reason = "These generated Proto aliases are consumed by the native and WASM bridges"
)]
pub use generated::{
    ActorLimitsProto, ActorObservationProto, ActorState, AddSubscriptionRequestProto,
    AddSubscriptionResponseProto, BindingProto, CheckpointActorRequestProto,
    CheckpointActorResponseProto, CreateActorRequestProto, CreateActorResponseProto,
    DeleteActorRequestProto, DeleteActorResponseProto, ErrorCode, HeaderProto,
    InspectActorRequestProto, InspectActorResponseProto, InvokeActorRequestProto,
    InvokeActorResponseProto, RemoveSubscriptionRequestProto, RemoveSubscriptionResponseProto,
    ResumeSubscriptionRequestProto, ResumeSubscriptionResponseProto, ServiceErrorProto,
    SubscriptionObservationProto, SubscriptionSpecProto, SubscriptionStartProto, SubscriptionState,
    UpdateActorRequestProto, UpdateActorResponseProto, subscription_start,
};

/// Actors service operations generated from the protobuf contract.
#[allow(
    unused_imports,
    reason = "The generated service descriptor is consumed by transport adapters"
)]
pub use generated::ActorsService;

#[cfg(test)]
mod tests {
    use super::render_proto_files;
    use std::{
        fs, io,
        path::{Path, PathBuf},
    };

    fn snapshot(root: &Path, directory: &Path) -> io::Result<Vec<(PathBuf, Vec<u8>)>> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                entries.extend(snapshot(root, &path)?);
            } else {
                entries.push((
                    path.strip_prefix(root)
                        .map_err(io::Error::other)?
                        .to_owned(),
                    fs::read(path)?,
                ));
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }

    #[test]
    fn render_proto_files_is_byte_deterministic() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-actors-contract-determinism-{}",
            std::process::id()
        ));
        let first = root.join("first");
        let second = root.join("second");
        let _ = fs::remove_dir_all(&root);

        render_proto_files(&first).expect("first contract render");
        render_proto_files(&second).expect("second contract render");

        assert_eq!(
            snapshot(&first, &first).expect("first generated snapshot"),
            snapshot(&second, &second).expect("second generated snapshot")
        );
        fs::remove_dir_all(root).expect("remove deterministic-render fixture");
    }
}

#[cfg(test)]
mod fallible_producer_tests {
    use prost::Message;

    #[acyclic_contract_derive::file(
        family = "actors_ingress",
        messages(IngressProto, ChildProto, EnvelopeProto)
    )]
    pub struct FallibleFile;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IngressError;

    impl std::fmt::Display for IngressError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("actor id must not be empty")
        }
    }

    impl std::error::Error for IngressError {}

    impl From<std::convert::Infallible> for IngressError {
        fn from(value: std::convert::Infallible) -> Self {
            match value {}
        }
    }

    fn parse_actor_id(value: String) -> Result<String, IngressError> {
        (!value.is_empty()).then_some(value).ok_or(IngressError)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    #[acyclic_contract_derive::message(error = IngressError, file = FallibleFile)]
    pub struct Ingress {
        #[wire(string, tag = 1, from = parse_actor_id)]
        actor_id: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    #[acyclic_contract_derive::message(error = IngressError, file = FallibleFile)]
    pub struct Child {
        #[wire(string, tag = 1)]
        value: String,
    }

    #[acyclic_contract_derive::oneof(error = IngressError, file = FallibleFile)]
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Selector {
        #[wire(tag = 1)]
        Cursor(u64),
        #[wire(tag = 2)]
        CurrentHead(bool),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    #[acyclic_contract_derive::message(error = IngressError, file = FallibleFile)]
    pub struct Envelope {
        #[wire(message, tag = 3)]
        child: Option<Child>,
        #[wire(message, tag = 4)]
        children: Vec<Child>,
        #[wire(oneof = "1,2")]
        selector: Option<Selector>,
    }

    #[test]
    fn fallible_proxy_rejects_invalid_untrusted_wire() {
        let malformed = IngressProto {
            actor_id: String::new(),
        };
        assert_eq!(Ingress::try_from(malformed), Err(IngressError));
    }

    #[test]
    fn fallible_proxy_round_trips_valid_wire() {
        let authored = Ingress {
            actor_id: String::from("actor-1"),
        };
        let wire: IngressProto = authored.clone().into();
        let encoded = wire.encode_to_vec();
        let decoded = IngressProto::decode(encoded.as_slice()).expect("decode fixture");
        assert_eq!(Ingress::try_from(decoded), Ok(authored));
    }

    #[test]
    fn fallible_nested_and_presence_conversions_are_recursive() {
        let authored = Envelope {
            child: Some(Child {
                value: String::from("one"),
            }),
            children: vec![Child {
                value: String::from("two"),
            }],
            selector: Some(Selector::Cursor(0)),
        };
        let wire: EnvelopeProto = authored.clone().into();
        let decoded = Envelope::try_from(wire).expect("nested conversion");
        assert_eq!(decoded, authored);
    }
}

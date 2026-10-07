//! Rust-owned Actors v1 contract.
//!
//! Protify derives the prost wire implementation and the complete protobuf
//! schema from these declarations. The tonic service facade below remains a
//! generated transport adapter and consumes these same message types.

use protify::*;
use ts_rs::TS;

mod generated {
    #![allow(missing_docs)]

    use super::*;

    include!("contract_definitions.rs");
}

#[doc = "Actors protobuf package schema handle."]
pub use generated::ACTORS_PACKAGE;

/// Renders the canonical Actors protobuf input for the maintained prost/tonic
/// build. The rendered file is an intermediate artifact; these Rust
/// declarations remain the contract authority.
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    let root = root.as_ref();
    std::fs::create_dir_all(root.join("actors/v1"))?;
    ACTORS_PACKAGE::get_package().render_files(root)
}

#[allow(unused_imports)]
pub use generated::{
    ActorLimits, ActorObservation, ActorState, AddSubscriptionRequest, AddSubscriptionResponse,
    Binding, CheckpointActorRequest, CheckpointActorResponse, CreateActorRequest,
    CreateActorResponse, Error, ErrorCode, Header, InspectActorRequest, InspectActorResponse,
    InvokeActorRequest, InvokeActorResponse, RemoveSubscriptionRequest, RemoveSubscriptionResponse,
    ResumeSubscriptionRequest, ResumeSubscriptionResponse, SubscriptionObservation,
    SubscriptionSpec, SubscriptionStart, SubscriptionState, UpdateActorRequest,
    UpdateActorResponse, subscription_start,
};

/// Actors service operations generated from the protobuf contract.
#[allow(unused_imports)]
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
                        .expect("generated path must be below its root")
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
    use super::*;
    use prost::Message;
    use protify::{define_proto_file, proto_message, proto_package};

    proto_package!(
        FALLIBLE_PACKAGE,
        name = "acyclic.actors.test",
        files = [FALLIBLE_FILE]
    );
    define_proto_file!(
        FALLIBLE_FILE,
        name = "actors/test.proto",
        package = FALLIBLE_PACKAGE,
        messages = [IngressProto]
    );

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IngressError;

    impl std::fmt::Display for IngressError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("actor id must not be empty")
        }
    }

    impl std::error::Error for IngressError {}

    fn parse_actor_id(value: String) -> Result<String, IngressError> {
        (!value.is_empty()).then_some(value).ok_or(IngressError)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    #[proto_message(proxied, fallible = IngressError)]
    pub struct Ingress {
        #[proto(string, tag = 1, from_proto = parse_actor_id)]
        actor_id: String,
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
}

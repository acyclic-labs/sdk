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
					path
						.strip_prefix(root)
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

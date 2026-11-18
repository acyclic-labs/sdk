#![doc = include_str!("../README.md")]
#![allow(
    missing_docs,
    reason = "field-level wire semantics are canonical in proto/machines/v1/machines.proto"
)]

#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;
/// Canonical OCI and pagination constants shared with the TypeScript generator.
pub use acyclic_machines_limits::{
    MANAGED_OCI_DIGEST_HEX_LENGTH, MANAGED_OCI_HEX_DIGITS, MANAGED_OCI_REFERENCE_SEPARATOR,
    MANAGED_OCI_ZERO_DIGIT, MAX_EVENT_PAGE_SIZE, MAX_PAGE_SIZE,
};
/// Compiler-linked public method admission.
pub mod authorization {
    use acyclic_auth::action as auth_action;
    include!(concat!(env!("OUT_DIR"), "/authorization.rs"));
}
/// Shared source and inherited-state metadata for every fork.
pub use acyclic_stream::Provenance;
use async_trait::async_trait;
pub use authorization::{AUTHORIZATION, MachinesMethod};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Generated revision-one public transport.
#[doc(hidden)]
pub mod wire {
    #![allow(missing_docs, reason = "generated from the documented public schema")]
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::allow_attributes_without_reason,
        reason = "generated protobuf bindings"
    )]
    pub mod harness {
        pub mod v2 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v2.rs"));
        }
    }
    pub mod machines {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.machines.v1.rs"));
        }
    }
    pub use harness::v2 as selection;
    pub use machines::v1::*;
}

/// Canonical public descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-machines-v1.bin"));
/// Current public protocol major.
pub const PROTOCOL_MAJOR: u32 = 1;
/// Current public protocol minor.
pub const PROTOCOL_MINOR: u32 = 8;
/// Maximum bytes returned by one live guest console read.
pub const MAX_OUTPUT_READ_BYTES: u32 = 65_536;
/// Normalize a positive request hint to one physical page or output chunk.
/// Continue using the returned page cursor or output offset for more data.
///
/// # Errors
/// Rejects zero request hints or zero chunk widths.
pub fn page_limit(requested: u64, chunk: u32) -> Result<u32, ProviderError> {
    if requested == 0 || chunk == 0 {
        return Err(ProviderError::Invalid("page size must be positive".into()));
    }
    Ok(u32::try_from(requested).unwrap_or(chunk).min(chunk))
}

/// Default automatic idle suspension delay.
pub const DEFAULT_IDLE_SUSPEND: Duration = Duration::from_secs(15);

pub mod codec;
mod contract;
pub use contract::*;
mod provider;
#[doc(hidden)]
pub use provider::events;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub use provider::{admitted, deadline};
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
pub use provider::journal_codec::codec_fuzz;
#[cfg(feature = "test-support")]
pub use provider::journal_codec::{
    Completed as JournalCompleted, Intent as JournalIntent, decode as journal_decode,
    encode as journal_encode,
};
pub use provider::*;
/// Canonical HTTP/JSON route names and descriptor-backed serialization.
#[cfg(feature = "http-codec")]
pub mod http_codec;
/// Original-operation result publication and credited immutable reads.
pub mod results;



#[cfg(test)]
mod admission_tests {
    use acyclic_auth::action::{Action, MachinesAction};

    use super::MachinesMethod;

    #[test]
    fn every_machines_method_has_exact_admission() {
        for &method in MachinesMethod::ALL {
            let row = method.authorization();
            let actions: Vec<_> = row.actions().collect();
            assert_eq!(MachinesMethod::from_route(row.route), Some(method));
            match method {
                MachinesMethod::Fork => assert_eq!(
                    actions,
                    [
                        Action::Machines(MachinesAction::ReadCheckpoint),
                        Action::Machines(MachinesAction::Create),
                    ]
                ),
                MachinesMethod::ForkMachine => assert_eq!(
                    actions,
                    [
                        Action::Machines(MachinesAction::Read),
                        Action::Machines(MachinesAction::Create),
                    ]
                ),
                _ => assert!(
                    matches!(actions.as_slice(), [Action::Machines(_)]),
                    "{method:?}: {actions:?}"
                ),
            }
        }
        assert_eq!(
            MachinesMethod::from_route("/acyclic.machines.v1.MachinesService/Unmapped"),
            None
        );
    }
}

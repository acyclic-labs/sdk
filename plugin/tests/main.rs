//! Existing integration scenarios share one executable entrypoint.

#[path = "support/binary.rs"]
mod binary;

#[path = "identity.rs"]
mod identity;

#[path = "identity_contract.rs"]
mod identity_contract;

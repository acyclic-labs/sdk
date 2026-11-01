acyclic_test_support::counting_allocator!();

#[path = "common/operation_context.rs"]
mod operation_context;

#[path = "admission.rs"]
mod admission;

#[path = "authorization.rs"]
mod authorization;

#[path = "authorization_regressions.rs"]
mod authorization_regressions;

#[path = "contract.rs"]
mod contract;

#[path = "credentials.rs"]
mod credentials;

#[path = "minting.rs"]
mod minting;

#[path = "narrowing.rs"]
mod narrowing;

#[path = "operations.rs"]
mod operations;

#[path = "properties.rs"]
mod properties;

#[path = "request_hardening.rs"]
mod request_hardening;

#[path = "signatures.rs"]
mod signatures;

#[path = "verifier.rs"]
mod verifier;

#[path = "credential_cache.rs"]
mod credential_cache;

#[path = "scopes.rs"]
mod scopes;

#[cfg(not(loom))]
mod caller;

#[cfg(not(loom))]
mod channel;

#[cfg(not(loom))]
mod credential_custodian;

#[cfg(not(loom))]
mod revocation;

#[path = "secrets.rs"]
mod secrets;

#[path = "secrets_custody.rs"]
mod secrets_custody;

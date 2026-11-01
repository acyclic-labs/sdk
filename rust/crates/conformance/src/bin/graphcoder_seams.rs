//! `graphcoder` through injected server seams: the managed-host shape of the
//! one harness server. Its verifier is this binary's own, and it reports on
//! stderr when the server consults it, so a test can tell the seam was
//! consulted and was the only thing that differed.
use acyclic_harness::{
    Result,
    core::Scope,
    hosting::VerifiedCaller,
    server::{Custody, KeyFileVerifier, Observation, RootCredential, ServerConfig, Verifier},
};
use futures::future::BoxFuture;
use std::sync::Arc;

/// Admits the tree under its own scope, witnessing it on stderr, and
/// publishes the key file's credential for its clients.
struct Witnessed;

impl Verifier for Witnessed {
    fn caller(&self, custody: &Custody<'_>, owner: &Scope) -> Result<VerifiedCaller> {
        eprintln!("seams: verified the caller");
        Ok(VerifiedCaller {
            audience: custody.issuer.verifier(&mut None)?.audience().clone(),
            scope: custody.whole_tree(owner, "conformance-verified")?,
        })
    }

    fn observation<'a>(
        &'a self,
        root: RootCredential<'a>,
    ) -> BoxFuture<'a, Result<Option<Observation>>> {
        KeyFileVerifier.observation(root)
    }
}

fn main() -> std::process::ExitCode {
    graphcoder::main_with(
        |config| ServerConfig {
            verifier: Arc::new(Witnessed),
            ..config
        },
        None,
    )
    .unwrap_or(std::process::ExitCode::FAILURE)
}

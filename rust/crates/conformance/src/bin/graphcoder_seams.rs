//! `graphcoder` through injected server seams: the managed-host shape of the
//! one harness server. Its verifier is this binary's own, and it reports on
//! stderr when the server consults it, so a test can tell the seam was
//! consulted and was the only thing that differed.
use acyclic_auth::verify::VerifyError;
use acyclic_harness::{
    Error, Result,
    core::Scope,
    hosting::VerifiedCaller,
    server::{
        Custody, KeyFileCredentials, KeyFileVerifier, Observation, RootCredential, ServerConfig,
        Verifier,
    },
};
use acyclic_stream::session::{Admission, Authority, CredentialVerifier, TrustSnapshot};
use futures::future::BoxFuture;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Reports its first use on stderr, once.
struct Witness {
    report: &'static str,
    seen: AtomicBool,
}

impl Witness {
    const fn new(report: &'static str) -> Self {
        Self {
            report,
            seen: AtomicBool::new(false),
        }
    }

    fn saw(&self) {
        if !self.seen.swap(true, Ordering::SeqCst) {
            eprintln!("seams: {}", self.report);
        }
    }
}

/// Admits the tree under its own scope, and verifies every session
/// credential itself: the session's one verifier, which witnesses each
/// credential before verifying it as the key file's. It publishes the key
/// file's credential for its clients.
struct Witnessed;

impl Verifier for Witnessed {
    fn caller(&self, custody: &Custody<'_>, owner: &Scope) -> Result<VerifiedCaller> {
        eprintln!("seams: verified the caller");
        Ok(VerifiedCaller {
            audience: custody.issuer.verifier(&mut None)?.audience().clone(),
            scope: custody.whole_tree(owner, "conformance-verified")?,
        })
    }

    fn session(&self, custody: &Custody<'_>) -> Result<Arc<Authority>> {
        let authority = Authority::new(custody.trust(), custody.issuer.audience_resource()?, None)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(Arc::new(authority.verified_by(Arc::new(Credentials {
            witness: Witness::new("verified a session credential"),
            opaque: Witness::new("verified an opaque credential"),
        }))))
    }

    fn observation<'a>(
        &'a self,
        root: RootCredential<'a>,
    ) -> BoxFuture<'a, Result<Option<Observation>>> {
        KeyFileVerifier.observation(root)
    }
}

struct Credentials {
    witness: Witness,
    /// A credential in another format than the key file's, a JWS's dotted
    /// one say, reached this verifier intact.
    opaque: Witness,
}

impl CredentialVerifier for Credentials {
    fn verify<'a>(
        &'a self,
        credential: &'a [u8],
        trust: &'a TrustSnapshot,
    ) -> BoxFuture<'a, std::result::Result<Arc<dyn Admission>, VerifyError>> {
        if credential.contains(&b'.') {
            self.opaque.saw();
        }
        self.witness.saw();
        KeyFileCredentials.verify(credential, trust)
    }
}

fn main() -> std::process::ExitCode {
    graphcoder::main_with(|config| ServerConfig {
        verifier: Arc::new(Witnessed),
        ..config
    })
}

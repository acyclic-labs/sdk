//! The served tree's caller verification: who the served tree is admitted
//! as, and what each client's session credential is verified against. A
//! self-deployed server takes [`KeyFileVerifier`]; a managed host plugs in
//! its own [`Verifier`], and nothing else about the server changes.
use super::storage;
use crate::{
    Capabilities, Result,
    core::{AuthorityRoot, Scope},
    hosting::VerifiedCaller,
};
use acyclic_auth::verify::VerifyError;
use acyclic_stream::{
    UnixMillisClock,
    session::{
        Admission, Authority, AuthorityState, CapabilityVerifier, CredentialVerifier, TrustSnapshot,
    },
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::future::{self, BoxFuture};
use std::sync::Arc;

/// The credential a server publishes for its clients: what
/// `ACYCLIC_API_KEY` holds, and the principal it authenticates.
#[derive(Debug, serde::Serialize)]
pub struct Observation {
    /// The credential, as the verifier's clients present it.
    pub credential: String,
    /// The principal the credential authenticates, which an approval policy
    /// names to route decisions to its holder.
    pub principal: String,
}

/// The credential the served tree's root can be minted, minted only when a
/// verifier publishes it.
pub struct RootCredential<'a>(pub(crate) BoxFuture<'a, Result<(Vec<u8>, String)>>);

impl RootCredential<'_> {
    /// Mints the root's capability envelope and the principal it
    /// authenticates.
    ///
    /// # Errors
    /// When the credential cannot be minted.
    pub async fn mint(self) -> Result<(Vec<u8>, String)> {
        self.0.await
    }
}

/// The host's custody a verifier admits the served tree and its clients
/// from: the signing root every agent credential of the tree is issued
/// under, and the host's clock.
pub struct Custody<'a> {
    /// The signing root; every agent credential of the tree is issued
    /// under it.
    pub issuer: &'a AuthorityRoot,
    /// The host's clock, for credential time checks.
    pub clock: &'a Arc<dyn UnixMillisClock>,
}

impl Custody<'_> {
    /// `owner`'s grants over every volume of the tree, bound to no one
    /// agent, issued as `id`: the scope each spawned child is admitted under
    /// exactly as the root is.
    ///
    /// # Errors
    /// When the scope cannot be issued.
    pub fn whole_tree(&self, owner: &Scope, id: &str) -> Result<Scope> {
        let tree =
            acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
                .to_scope_label()
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        let grants = Capabilities::new(
            owner
                .capabilities()
                .iter()
                .chain(std::iter::once(tree.as_str())),
        );
        self.issuer
            .bootstrap_scope(id, &grants, None, &mut None)
            .map_err(storage)
    }

    /// The trust every credential [`Self::issuer`] signs is verified
    /// against, the root's minted credential among them.
    #[must_use]
    pub fn trust(&self) -> Arc<dyn AuthorityState> {
        crate::filesystem::session_trust(self.issuer, Arc::clone(self.clock))
    }

    /// The session authority admitting every credential [`Self::issuer`]
    /// signs under its audience, through [`Self::trust`].
    ///
    /// # Errors
    /// When the issuer's audience is not a session resource.
    pub fn minted(&self) -> Result<Authority> {
        crate::filesystem::session_authority(self.issuer, Arc::clone(self.clock))
    }
}

/// Verifies the served tree's caller and its clients' credentials.
pub trait Verifier: Send + Sync + 'static {
    /// The caller every agent of the served tree is admitted under, within
    /// `owner`, the scope of every grant the host's tree may hold.
    ///
    /// # Errors
    /// When the caller cannot be verified.
    fn caller(&self, custody: &Custody<'_>, owner: &Scope) -> Result<VerifiedCaller>;

    /// The session authority every client's credential is verified against.
    /// A host whose clients present its own credential format (an OAuth or
    /// JWS gateway's token) returns one
    /// [`verified_by`](acyclic_stream::session::Authority::verified_by) its
    /// own [`CredentialVerifier`](acyclic_stream::session::CredentialVerifier):
    /// the one place the session verifies credentials.
    ///
    /// # Errors
    /// When the authority cannot be built.
    fn session(&self, custody: &Custody<'_>) -> Result<Arc<Authority>>;

    /// The credential the server publishes for its clients, if this verifier
    /// admits one it can mint from `root`. A host whose clients hold their
    /// own credentials publishes none.
    ///
    /// # Errors
    /// When the credential cannot be minted.
    fn observation<'a>(
        &'a self,
        root: RootCredential<'a>,
    ) -> BoxFuture<'a, Result<Option<Observation>>> {
        drop(root);
        Box::pin(future::ready(Ok(None)))
    }
}

/// The self-deployed verifier: the host's own key file is the caller, its
/// whole tree admitted under the owner's scope, and a client presents the
/// credential minted for the root, as [`KeyFileCredentials`].
pub struct KeyFileVerifier;

impl Verifier for KeyFileVerifier {
    fn caller(&self, custody: &Custody<'_>, owner: &Scope) -> Result<VerifiedCaller> {
        Ok(VerifiedCaller {
            audience: custody.issuer.verifier(&mut None)?.audience().clone(),
            scope: custody.whole_tree(owner, "served")?,
        })
    }

    fn session(&self, custody: &Custody<'_>) -> Result<Arc<Authority>> {
        Ok(Arc::new(
            custody.minted()?.verified_by(Arc::new(KeyFileCredentials)),
        ))
    }

    fn observation<'a>(
        &'a self,
        root: RootCredential<'a>,
    ) -> BoxFuture<'a, Result<Option<Observation>>> {
        Box::pin(async move {
            let (envelope, principal) = root.mint().await?;
            Ok(Some(Observation {
                credential: URL_SAFE_NO_PAD.encode(envelope),
                principal,
            }))
        })
    }
}

/// A key-file server's client credential: the capability envelope minted
/// for the root, base64url without padding, the text `ACYCLIC_API_KEY`
/// holds. Clients carry it opaque; this verifier alone reads it.
pub struct KeyFileCredentials;

impl CredentialVerifier for KeyFileCredentials {
    fn verify<'a>(
        &'a self,
        credential: &'a [u8],
        trust: &'a TrustSnapshot,
    ) -> BoxFuture<'a, std::result::Result<Arc<dyn Admission>, VerifyError>> {
        Box::pin(async move {
            let envelope = URL_SAFE_NO_PAD
                .decode(credential)
                .map_err(|_| VerifyError::Malformed)?;
            CapabilityVerifier.verify(&envelope, trust).await
        })
    }
}

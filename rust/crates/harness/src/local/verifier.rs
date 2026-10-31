//! The served tree's caller verification: who the served tree is admitted
//! as, and the credential it publishes for its clients. A self-deployed
//! server takes [`KeyFileVerifier`]; a managed host plugs in its own
//! [`Verifier`], and nothing else about the server changes. Every client's
//! session credential is the one capability envelope, verified by the
//! session against the issuer's trust.
use super::storage;
use crate::{
    Capabilities, Result,
    core::{AuthorityRoot, Scope},
    hosting::VerifiedCaller,
};
use acyclic_stream::UnixMillisClock;
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
}

/// Verifies the served tree's caller and publishes its clients' credential.
pub trait Verifier: Send + Sync + 'static {
    /// The caller every agent of the served tree is admitted under, within
    /// `owner`, the scope of every grant the host's tree may hold.
    ///
    /// # Errors
    /// When the caller cannot be verified.
    fn caller(&self, custody: &Custody<'_>, owner: &Scope) -> Result<VerifiedCaller>;

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
/// whole tree admitted under the owner's scope, and its clients hold the
/// credential minted for the root.
pub struct KeyFileVerifier;

impl Verifier for KeyFileVerifier {
    fn caller(&self, custody: &Custody<'_>, owner: &Scope) -> Result<VerifiedCaller> {
        Ok(VerifiedCaller {
            audience: custody.issuer.verifier(&mut None)?.audience().clone(),
            scope: custody.whole_tree(owner, "served")?,
        })
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

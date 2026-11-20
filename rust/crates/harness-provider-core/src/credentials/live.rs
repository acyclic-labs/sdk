//! One event-invalidated resolver for native provider credential sources.
#[cfg(not(target_arch = "wasm32"))]
use super::watched::{Watched, events};
use super::*;
#[cfg(target_arch = "wasm32")]
use crate::sync::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_auth::credential::CredentialWatch;
use std::sync::Arc;

/// The credential a provider request authenticates with.
#[derive(Clone)]
pub enum Credential {
    /// A resolved API key.
    Key(Arc<str>),
    /// A Codex login, which only some providers accept.
    #[cfg(not(target_arch = "wasm32"))]
    Codex(Arc<crate::CodexLogin>),
    /// No source holds a credential for this provider.
    Missing,
}

impl Credential {
    /// This credential as `config` admits it: a Codex login only where the
    /// provider accepts an API key or a Codex login.
    pub fn for_config(self, config: &crate::spec::ProviderSpec) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self, Self::Codex(_))
            && config.authentication != crate::spec::Authentication::ApiKeyOrCodexLogin
        {
            return Self::Missing;
        }
        let _ = config;
        self
    }
}

#[derive(Default)]
struct State {
    keys: BTreeMap<String, std::result::Result<Option<Arc<str>>, KeychainDecode>>,
    #[cfg(not(target_arch = "wasm32"))]
    login: Option<Option<Arc<crate::CodexLogin>>>,
}

/// The credential sources one provider set resolves through, cached per source generation.
pub struct CredentialInputs {
    credentials: Credentials,
    #[cfg(not(target_arch = "wasm32"))]
    state: Watched<State>,
    #[cfg(target_arch = "wasm32")]
    state: Mutex<State>,
}

impl CredentialInputs {
    /// A resolver over CREDENTIALS, observing each source once per generation.
    pub fn new(credentials: Credentials) -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            state: Watched::new(credentials.paths(), credentials.watch.clone()),
            #[cfg(target_arch = "wasm32")]
            state: Mutex::new(State::default()),
            credentials,
        }
    }

    /// The credential PROVIDER currently resolves to.
    pub fn select(&self, provider: &str) -> Result<Credential> {
        let select = |state: &mut State| {
            if !state.keys.contains_key(provider) {
                #[cfg(not(target_arch = "wasm32"))]
                let loaded = self
                    .credentials
                    .path
                    .as_ref()
                    .map(|path| Credentials::from_file(path))
                    .transpose()?;
                #[cfg(not(target_arch = "wasm32"))]
                let credentials = loaded.as_ref().unwrap_or(&self.credentials);
                #[cfg(target_arch = "wasm32")]
                let credentials = &self.credentials;
                let key = credentials
                    .observe_key(provider)
                    .map(|key| key.map(Arc::from));
                state.keys.insert(provider.into(), key);
            }
            match state.keys.get(provider) {
                Some(Err(error)) => return Err(error.error()),
                Some(Ok(Some(key))) => return Ok(Credential::Key(key.clone())),
                Some(Ok(None)) | None => {}
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(login) = state
                .login
                .get_or_insert_with(|| {
                    self.credentials.codex_login().map(|login| {
                        Arc::new(match &self.credentials.watch {
                            Some(watch) => login.watched(watch.clone()),
                            None => login,
                        })
                    })
                })
                .clone()
            {
                return Ok(Credential::Codex(login));
            }
            Ok(Credential::Missing)
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.state
                .read(false, select)
                .map(|(_, credential)| credential)
        }
        #[cfg(target_arch = "wasm32")]
        {
            select(
                &mut *self
                    .state
                    .lock()
                    .map_err(|_| crate::invalid("credential resolver lock poisoned"))?,
            )
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Credentials {
    /// Source owners publish environment/keychain changes through the same watch.
    pub fn changed() {
        events().send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    /// Supply an event source, including deterministic host watches.
    pub fn with_watch(
        mut self,
        watch: impl Fn() -> CredentialWatch + Send + Sync + 'static,
    ) -> Self {
        self.watch = Some(Arc::new(watch));
        self
    }

    fn paths(&self) -> Vec<std::path::PathBuf> {
        let mut paths: Vec<_> = self.path.iter().cloned().collect();
        if self.native_sources {
            #[cfg(test)]
            if let Some(source) = &self.source {
                if let Some(root) = source("", Source::CodexHome).ok().flatten() {
                    paths.push(std::path::Path::new(&root).join("auth.json"));
                }
                return paths;
            }
            if let Some(path) = crate::CodexLogin::discover_path() {
                paths.push(path);
            }
        }
        paths
    }
}

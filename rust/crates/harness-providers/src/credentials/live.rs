//! One event-invalidated resolver for native provider credential sources.
#[cfg(not(target_arch = "wasm32"))]
use super::watched::{Watched, events};
use super::*;
#[cfg(target_arch = "wasm32")]
use crate::sync::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_auth::credential::CredentialWatch;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum Credential {
    Key(Arc<str>),
    #[cfg(not(target_arch = "wasm32"))]
    Codex(Arc<crate::CodexLogin>),
    Missing,
}

#[derive(Default)]
struct State {
    keys: BTreeMap<String, std::result::Result<Option<Arc<str>>, KeychainDecode>>,
    #[cfg(not(target_arch = "wasm32"))]
    login: Option<Option<Arc<crate::CodexLogin>>>,
}

pub(crate) struct CredentialInputs {
    credentials: Credentials,
    #[cfg(not(target_arch = "wasm32"))]
    state: Watched<State>,
    #[cfg(target_arch = "wasm32")]
    state: Mutex<State>,
}

impl CredentialInputs {
    pub(crate) fn new(credentials: Credentials) -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            state: Watched::new(credentials.paths(), credentials.watch.clone()),
            #[cfg(target_arch = "wasm32")]
            state: Mutex::new(State::default()),
            credentials,
        }
    }

    pub(crate) fn select(&self, provider: &str) -> Result<Credential> {
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
            match &state.keys[provider] {
                Err(error) => return Err(error.error()),
                Ok(Some(key)) => return Ok(Credential::Key(key.clone())),
                Ok(None) => {}
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
            self.state.read(select).map(|(_, credential)| credential)
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

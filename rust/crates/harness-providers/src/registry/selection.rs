use super::*;

type Observed = std::result::Result<Option<Arc<str>>, crate::credentials::KeychainDecode>;

/// Source observations of one catalog generation. Each provider's sources
/// are read at most once, and only when admission or resolution selects it.
#[derive(Default)]
pub(super) struct Observations {
    keys: BTreeMap<String, Observed>,
    #[cfg(not(target_arch = "wasm32"))]
    login: Option<Option<Arc<crate::CodexLogin>>>,
}

impl Observations {
    /// A rejected update keeps the admitted generation; it only contributes
    /// observations that generation has not made yet.
    pub(super) fn adopt(&mut self, newer: Self) {
        for (provider, key) in newer.keys {
            self.keys.entry(provider).or_insert(key);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if self.login.is_none() {
            self.login = newer.login;
        }
    }
}

#[derive(Clone)]
pub(super) enum Credential {
    Key(Arc<str>),
    #[cfg(not(target_arch = "wasm32"))]
    Codex(Arc<crate::CodexLogin>),
    Missing,
}

impl Credential {
    // Catalog policies can change on a rejected first update. Snapshot source
    // observations independently; apply the policy through this single selector.
    fn for_config(self, config: &ProviderSpec) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self, Self::Codex(_))
            && config.authentication != Authentication::ApiKeyOrCodexLogin
        {
            return Self::Missing;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = config;
        self
    }
}

impl Registry {
    /// The only credential source reader: memoizes into `observations`.
    pub(super) fn observe(
        &self,
        observations: &mut Observations,
        provider: &str,
    ) -> Result<Credential> {
        if !observations.keys.contains_key(provider) {
            let key = self.credentials.observe_key(provider);
            observations
                .keys
                .insert(provider.into(), key.map(|key| key.map(Arc::from)));
        }
        match &observations.keys[provider] {
            Err(failure) => return Err(failure.error()),
            Ok(Some(key)) => return Ok(Credential::Key(key.clone())),
            Ok(None) => {}
        }
        #[cfg(not(target_arch = "wasm32"))]
        // An explicitly supplied login overrides discovery without invalidating
        // the key observations used to admit the current catalog.
        if let Some(login) = self.codex_login.clone().or_else(|| {
            observations
                .login
                .get_or_insert_with(|| self.credentials.codex_login().map(Arc::new))
                .clone()
        }) {
            return Ok(Credential::Codex(login));
        }
        Ok(Credential::Missing)
    }

    pub(super) fn selected_credential(
        &self,
        provider: &str,
        config: &ProviderSpec,
    ) -> Result<Credential> {
        self.selection
            .read(|observations| {
                self.observe(observations, provider)
                    .map(|credential| credential.for_config(config))
            })
            .map_err(|()| invalid("credential selection lock poisoned"))?
    }
}

pub(super) fn counts_input(config: &ProviderSpec, credential: Credential) -> Result<()> {
    if config.token_counter.is_some() {
        return Ok(());
    }
    #[cfg(not(target_arch = "wasm32"))]
    if codex_route(config) {
        match credential.for_config(config) {
            Credential::Codex(_) => return Ok(()),
            Credential::Key(_) if config.authentication == Authentication::ApiKeyOrCodexLogin => {
                return Err(invalid(
                    "catalog default_model requires a verified input token counter: \
                     the provider API key displaces the Codex login that counts input locally",
                ));
            }
            _ => {}
        }
    }
    #[cfg(target_arch = "wasm32")]
    let _ = credential;
    Err(invalid(
        "catalog default_model requires a verified input token counter",
    ))
}

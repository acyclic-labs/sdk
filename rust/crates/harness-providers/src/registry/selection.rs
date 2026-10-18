use super::*;

pub(super) type CredentialSelection =
    std::result::Result<BTreeMap<String, Credential>, crate::credentials::KeychainDecode>;

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
    fn for_config(&self, config: &ProviderSpec) -> &Self {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self, Self::Codex(_))
            && config.authentication != Authentication::ApiKeyOrCodexLogin
        {
            return &Self::Missing;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = config;
        self
    }
}

impl Registry {
    pub(super) fn observe_credentials<'a>(
        &self,
        providers: impl Iterator<Item = (&'a str, &'a ProviderSpec)>,
    ) -> CredentialSelection {
        #[cfg(not(target_arch = "wasm32"))]
        let login = self.codex_login.clone().or_else(|| {
            self.credentials
                .allows_native_sources()
                .then(crate::CodexLogin::discover)
                .flatten()
                .map(Arc::new)
        });
        providers
            .map(|(provider, _)| {
                let key = self.credentials.observe_key(provider)?;
                let credential = match key {
                    Some(key) => Credential::Key(key.into()),
                    #[cfg(not(target_arch = "wasm32"))]
                    None => login
                        .as_ref()
                        .map(|login| Credential::Codex(login.clone()))
                        .unwrap_or(Credential::Missing),
                    #[cfg(target_arch = "wasm32")]
                    None => Credential::Missing,
                };
                Ok((provider.into(), credential))
            })
            .collect()
    }

    pub(super) fn selected_credential(
        &self,
        provider: &str,
        config: &ProviderSpec,
    ) -> Result<Credential> {
        self.selection
            .read(
                || {
                    self.observe_credentials(
                        self.providers
                            .iter()
                            .map(|(id, spec)| (id.as_str(), spec.as_ref())),
                    )
                },
                |selection| {
                    selection
                        .as_ref()
                        .map_err(|failure| failure.error())?
                        .get(provider)
                        .map(|credential| credential.for_config(config).clone())
                        .ok_or_else(|| invalid("provider is absent from credential snapshot"))
                },
            )
            .map_err(|()| invalid("credential selection lock poisoned"))?
    }
}

pub(super) fn counts_input(config: &ProviderSpec, credential: &Credential) -> bool {
    if config.token_counter.is_some() {
        return true;
    }
    #[cfg(not(target_arch = "wasm32"))]
    if codex_route(config) && matches!(credential.for_config(config), Credential::Codex(_)) {
        return true;
    }
    #[cfg(target_arch = "wasm32")]
    let _ = credential;
    false
}

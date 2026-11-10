use super::*;

impl Credential {
    pub(crate) fn for_config(self, config: &ProviderSpec) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self, Self::Codex(_))
            && config.authentication != Authentication::ApiKeyOrCodexLogin
        {
            return Self::Missing;
        }
        let _ = config;
        self
    }
}

impl Registry {
    pub(super) fn selected_credential(
        &self,
        provider: &str,
        config: &ProviderSpec,
    ) -> Result<Credential> {
        if config.authentication.secret_reference().is_some() {
            return Ok(Credential::Missing);
        }
        let credential = self.inputs.select(provider)?;
        #[cfg(not(target_arch = "wasm32"))]
        let credential = match credential {
            Credential::Key(_) => credential,
            _ => self
                .codex_login
                .clone()
                .map(Credential::Codex)
                .unwrap_or(credential),
        };
        Ok(credential.for_config(config))
    }
}

pub(super) fn counts_input(
    config: &ProviderSpec,
    model: &str,
    credential: Credential,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    let credential = credential.for_config(config);
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(credential, Credential::Codex(_)) && !codex_route(config) {
        return Err(invalid(
            "Codex CLI credentials require the built-in Responses route",
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(credential, Credential::Codex(_)) {
        // Resolution replaces the inherited remote counter with local counting.
        return Ok(());
    }
    if config.token_counter.is_some() {
        let counter = crate::counting::specification(config, None)?;
        return crate::counting::wire::endpoint(config, model, counter).map(|_| ());
    }
    #[cfg(not(target_arch = "wasm32"))]
    if codex_route(config)
        && matches!(credential, Credential::Key(_))
        && config.authentication == Authentication::ApiKeyOrCodexLogin
    {
        return Err(invalid(
            "catalog default_model requires a verified input token counter: \
             the provider API key displaces the Codex login that counts input locally",
        ));
    }
    #[cfg(target_arch = "wasm32")]
    let _ = credential;
    Err(invalid(
        "catalog default_model requires a verified input token counter",
    ))
}

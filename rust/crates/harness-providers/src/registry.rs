use crate::{Authentication, Credentials, HttpTransport, ProviderSpec, ResolvedModel, invalid};
use acyclic_harness_core::{Result, model::Model};
use std::{collections::BTreeMap, sync::Arc};
mod catalog_update;
mod selection;
mod snapshot;
use catalog_update::Changes;
use selection::Credential;

/// Registry of protocol endpoints addressed by `provider/model`.
pub struct Registry {
    providers: BTreeMap<String, Arc<ProviderSpec>>,
    default_model: Option<String>,
    credentials: Credentials,
    selection: snapshot::Snapshot<selection::Observations>,
    transport: Arc<dyn HttpTransport>,
    resolver: Option<Arc<dyn crate::CredentialResolver>>,
    files: crate::files::Files,
    #[cfg(not(target_arch = "wasm32"))]
    codex_login: Option<Arc<crate::CodexLogin>>,
}

impl Registry {
    /// Standard endpoints; unknown capacities must be registered explicitly.
    pub fn new(credentials: Credentials) -> Result<Self> {
        Ok(Self::with_transport(credentials, crate::http::platform()?))
    }

    /// Uses the same adapters with a caller-owned transport.
    pub fn with_transport(credentials: Credentials, transport: Arc<dyn HttpTransport>) -> Self {
        let providers = crate::catalog::default_catalog()
            .into_iter()
            .map(|(id, spec)| (id, Arc::new(spec)))
            .collect();
        Self {
            providers,
            default_model: None,
            credentials,
            selection: snapshot::Snapshot::default(),
            transport,
            resolver: None,
            files: crate::files::Files::default(),
            #[cfg(not(target_arch = "wasm32"))]
            codex_login: None,
        }
    }

    /// Inject the deployment owner's protected credential binder. It receives
    /// logical references and admitted native sources, never caller authority.
    pub fn with_credential_resolver(
        mut self,
        resolver: Arc<dyn crate::CredentialResolver>,
    ) -> Self {
        self.set_credential_resolver(resolver);
        self
    }

    pub(crate) fn set_credential_resolver(&mut self, resolver: Arc<dyn crate::CredentialResolver>) {
        self.resolver = Some(resolver);
    }

    /// Binds the owner's bounded content reader used by every adapter.
    pub fn with_content(
        mut self,
        reader: Arc<dyn acyclic_harness_core::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Self {
        self.files = crate::files::Files {
            reader: Some(reader),
            maximum_bytes,
        };
        self
    }

    /// Supplies a specific native Codex CLI login, including for scripted refresh tests.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_codex_login(mut self, login: crate::CodexLogin) -> Self {
        self.codex_login = Some(Arc::new(login));
        self
    }

    /// Catalog-declared fallback when neither argv nor settings select a model.
    pub fn default_model(&self) -> &str {
        self.default_model
            .as_deref()
            .unwrap_or(crate::catalog::DEFAULT_MODEL)
    }

    /// Borrows declarative endpoint, authentication and capacity specifications.
    pub fn catalog(&self) -> &BTreeMap<String, Arc<ProviderSpec>> {
        &self.providers
    }

    /// Adds or replaces one endpoint. Local models require their configured capacities.
    pub fn register(&mut self, provider: String, config: ProviderSpec) -> Result<()> {
        self.update_catalog(Changes::One(provider, config), None)
    }

    /// Loads endpoint, credential policy and exact capacities from a JSON catalog.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_catalog(&mut self, path: &std::path::Path) -> Result<()> {
        let bytes = std::fs::read(path).map_err(|_| invalid("cannot read provider catalog"))?;
        self.load_catalog_bytes(&bytes)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn load_catalog_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let catalog = crate::catalog_document::parse(bytes)?;
        self.update_catalog(Changes::Many(catalog.providers), catalog.default_model)
    }

    /// Resolves a model without doing network I/O. Remaining slashes belong to the model.
    pub fn resolve(&self, selection: &str) -> Result<ResolvedModel> {
        let (provider, name) = selection
            .split_once('/')
            .ok_or_else(|| invalid("expected provider/model"))?;
        let config = self
            .providers
            .get(provider)
            .ok_or_else(|| invalid("unknown model provider"))?
            .clone();
        let capacity = config
            .models
            .get(name)
            .map(|entry| entry.capacity)
            .ok_or_else(|| invalid("register the selected model context capacity"))?;
        let credential = self.selected_credential(provider, &config)?;
        let key = match &credential {
            Credential::Key(key) => Some(key.clone()),
            _ => None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let codex_login = match credential {
            Credential::Codex(login) => Some(Arc::new(login.fresh())),
            _ => None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let config = if codex_login.is_some() {
            if !codex_route(&config) {
                return Err(invalid(
                    "Codex CLI credentials require the built-in Responses route",
                ));
            }
            Arc::new(ProviderSpec {
                base_url: "https://chatgpt.com/backend-api/codex".into(),
                token_counter: None,
                ..config.as_ref().clone()
            })
        } else {
            config
        };
        let binding = crate::authorization::bind(
            &config,
            key,
            self.resolver.as_deref(),
            self.transport.clone(),
        )?;
        let (key, credential_scope, transport) = (binding.key, binding.scope, binding.transport);
        let authenticated = key.is_some() || credential_scope.is_some();
        #[cfg(not(target_arch = "wasm32"))]
        let authenticated = authenticated || codex_login.is_some();
        if !authenticated && config.authentication != Authentication::Optional {
            return Err(invalid("model provider API key is unavailable"));
        }
        // Bind the effective selected route, not the whole catalog. An edit to
        // another model cannot rotate this identity, while an endpoint, header,
        // counter, authority or selected-capacity change cannot reuse it.
        let mut revision = blake3::Hasher::new();
        revision.update(b"acyclic.provider-selection.v1\0");
        serde_json::to_writer(
            &mut revision,
            &(
                config.adapter,
                &config.authentication,
                &config.base_url,
                &config.headers,
                &config.token_counter,
                config.models.get(name),
                credential_scope,
            ),
        )
        .map_err(|_| invalid("cannot encode selected provider identity"))?;
        let model = Model::new(
            provider.into(),
            name.into(),
            format!("provider-v1:{}", revision.finalize().to_hex()),
        )?;
        let transport = crate::deadline::Bounded::new(transport, config.timeouts);
        Ok(ResolvedModel {
            files: self.files.clone(),
            estimator: std::sync::Arc::new(crate::accounting::TokenEstimator::new(config.adapter)),
            model,
            profile: None,
            config,
            capacity,
            key,
            credential_scope,
            transport,
            #[cfg(not(target_arch = "wasm32"))]
            codex_count: std::sync::Arc::default(),
            #[cfg(not(target_arch = "wasm32"))]
            codex_login,
        })
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod credential_tests;

/// The built-in Responses route that a native Codex CLI login may replace.
#[cfg(not(target_arch = "wasm32"))]
fn codex_route(config: &ProviderSpec) -> bool {
    config.adapter == crate::Adapter::Responses && config.base_url == "https://api.openai.com/v1"
}

pub(crate) fn validate_url(base: &str) -> Result<url::Url> {
    let url = url::Url::parse(base).map_err(|_| invalid("invalid provider URL"))?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(url.scheme() == "https" || (url.scheme() == "http" && local))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "provider URL requires HTTPS or loopback HTTP without userinfo, query or fragment",
        ));
    }
    Ok(url)
}

pub(crate) fn validate_spec(provider: &str, config: &ProviderSpec) -> Result<()> {
    if provider.trim().is_empty() || provider.contains('/') || config.max_event_bytes == 0 {
        return Err(invalid("invalid provider registration"));
    }
    validate_url(&config.base_url)?;
    if let Some(reference) = config.authentication.secret_reference() {
        crate::authorization::reference(reference)?;
    }
    config.timeouts.validate()?;
    for (name, value) in &config.headers {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || value.contains(['\r', '\n'])
            || [
                "authorization",
                "x-api-key",
                "x-goog-api-key",
                "cookie",
                "host",
                "content-length",
                "content-type",
            ]
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
        {
            return Err(invalid("invalid or reserved provider header"));
        }
    }
    if let Some(counter) = &config.token_counter {
        if counter.adapter != config.adapter {
            return Err(invalid(
                "count endpoint must accept the provider input dialect",
            ));
        }
        crate::counting::wire::endpoint(&config.base_url, "model", counter)?;
    }
    for (name, model) in &config.models {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let validate_identity =
            !catalog_update::tests::SKIP_MODEL_IDENTITY.with(std::cell::Cell::get);
        #[cfg(not(all(test, not(target_arch = "wasm32"))))]
        let validate_identity = true;
        if validate_identity {
            Model::validate_identity(provider, name, "provider-v1")?;
        }
        model.capacity.validate()?;
        if model
            .max_context_tokens
            .is_some_and(|max| max < model.capacity.context_tokens)
        {
            return Err(invalid(
                "model maximum context is below its selected context",
            ));
        }
    }
    Ok(())
}

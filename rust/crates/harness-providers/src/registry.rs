use crate::{
    Adapter, Authentication, Credentials, HttpTransport, ProviderSpec, ResolvedModel, invalid,
};
use acyclic_harness_core::{Result, model::Model};
use std::{collections::BTreeMap, sync::Arc};
mod catalog_update;
mod selection;
use crate::credentials::{Credential, CredentialInputs};
use catalog_update::Changes;

/// Registry of protocol endpoints addressed by `provider/model`.
pub struct Registry {
    providers: BTreeMap<String, Arc<ProviderSpec>>,
    default_model: Option<String>,
    inputs: Arc<CredentialInputs>,
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
            inputs: Arc::new(CredentialInputs::new(credentials)),
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

    /// Binds the owner's content reader and optional aggregate projection credit.
    /// `None` imposes no aggregate byte policy.
    pub fn with_content(
        mut self,
        reader: Arc<dyn acyclic_harness_core::conversation::ContentResidencyVerifier>,
        maximum_bytes: Option<u64>,
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

    /// Whether this selection needs interactive credentials, using precisely
    /// the resolver's source precedence and authentication policy.
    pub fn needs_credentials(&self, selection: &str) -> Result<bool> {
        let (provider, model) = selection
            .split_once('/')
            .ok_or_else(|| invalid("expected provider/model"))?;
        let spec = self
            .providers
            .get(provider)
            .filter(|spec| spec.models.contains_key(model))
            .ok_or_else(|| invalid("selected provider/model is absent from the catalog"))?;
        Ok(spec.authentication != Authentication::Optional
            && spec.authentication.secret_reference().is_none()
            && matches!(
                self.selected_credential(provider, spec)?,
                Credential::Missing
            ))
    }

    /// One unauthenticated HTTP exchange verifies DNS, connection and TLS to
    /// the selected endpoint. Any HTTP status proves reachability; this never
    /// generates tokens, sends keys, follows redirects or retries.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn check_reachability(&self, selection: &str) -> Result<()> {
        let provider = selection
            .split_once('/')
            .map(|(provider, _)| provider)
            .ok_or_else(|| invalid("expected provider/model"))?;
        let spec = self
            .providers
            .get(provider)
            .ok_or_else(|| invalid("unknown model provider"))?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| invalid("cannot initialize provider network check"))?;
        // A reused Codex login selects its own route, through the same resolver.
        let resolved = self.resolve(selection).ok();
        let base_url = resolved
            .as_ref()
            .map_or(&spec.base_url, |resolved| &resolved.config.base_url);
        let mut request = client.head(base_url);
        if let Some(ms) = spec.timeouts.total_ms {
            request = request.timeout(std::time::Duration::from_millis(ms));
        }
        request.send().await.map(|_| ()).map_err(|_| invalid("provider network is unreachable; recovery: check DNS, proxy, TLS trust and the configured provider endpoint"))
    }

    /// Adds or replaces one endpoint. Local models require their configured capacities.
    pub fn register(&mut self, provider: String, config: ProviderSpec) -> Result<()> {
        self.update_catalog(Changes::One(provider, config), None)
    }

    /// Loads endpoint, credential policy and exact capacities from a JSON catalog.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_catalog(&mut self, path: &std::path::Path) -> Result<()> {
        let bytes = acyclic_fs::native_host::read_regular_file(path)
            .map_err(|_| invalid("cannot read provider catalog"))?;
        let catalog = crate::catalog_document::parse(&bytes)?;
        self.update_catalog(Changes::Many(catalog.providers), catalog.default_model)
    }

    /// Resolves a model without network I/O or requiring credentials for an unused selection.
    /// Authentication is enforced by request dispatch. Remaining slashes belong to the model.
    pub fn resolve(&self, selection: &str) -> Result<ResolvedModel> {
        let (provider, name) = selection
            .split_once('/')
            .ok_or_else(|| invalid("expected provider/model"))?;
        let config = self
            .providers
            .get(provider)
            .ok_or_else(|| invalid("unknown model provider"))?
            .clone();
        let (capacity, tool_profile, thinking_levels) = config
            .models
            .get(name)
            .map(|entry| (entry.capacity, entry.tool_profile, entry.thinking_levels))
            .ok_or_else(|| invalid("register the selected model context capacity"))?;
        let credential = self.selected_credential(provider, &config)?;
        let credential_config = config.clone();
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
                allow_http_host: None,
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
        let (key, credential_scope, transport) = binding.into_parts();
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
            resolver: self.resolver.as_ref().map(Arc::downgrade),
            unbound_transport: (!Arc::ptr_eq(&transport, &self.transport))
                .then(|| self.transport.clone()),
            #[cfg(not(target_arch = "wasm32"))]
            account_pin: codex_login
                .as_ref()
                .map_or_else(|| Arc::new(std::sync::OnceLock::new()), |login| login.pin()),
            inputs: Some(self.inputs.clone()),
            credential_config: (!Arc::ptr_eq(&config, &credential_config))
                .then_some(credential_config),
            files: self.files.clone(),
            accounting: crate::resolved::Accounting::new(config.adapter),
            model,
            profile: None,
            config,
            capacity,
            tool_profile,
            thinking_levels,
            key,
            credential_scope,
            transport,
            #[cfg(not(target_arch = "wasm32"))]
            codex_login,
            #[cfg(not(target_arch = "wasm32"))]
            explicit_codex_login: self.codex_login.clone(),
        })
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod credential_tests;

/// The built-in Responses route that a native Codex CLI login may replace.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn codex_route(config: &ProviderSpec) -> bool {
    config.adapter == crate::Adapter::Responses && config.base_url == "https://api.openai.com/v1"
}

pub(crate) fn validate_url(base: &str, allow_http_host: Option<&str>) -> Result<url::Url> {
    let url = url::Url::parse(base).map_err(|_| invalid("invalid provider URL"))?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    let permitted = allow_http_host.is_some_and(|host| Some(host) == url.host_str());
    if !(url.scheme() == "https" || (url.scheme() == "http" && (local || permitted)))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "provider URL requires HTTPS or loopback HTTP without userinfo, query or fragment; to permit HTTP to one non-loopback host, set that provider's allow_http_host to its exact canonical host",
        ));
    }
    Ok(url)
}

pub(crate) fn validate_spec(provider: &str, config: &ProviderSpec) -> Result<()> {
    if provider.trim().is_empty() || provider.contains('/') || config.max_event_bytes == Some(0) {
        return Err(invalid("invalid provider registration"));
    }
    validate_url(&config.base_url, config.allow_http_host.as_deref())?;
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
        for name in std::iter::once("model").chain(config.models.keys().map(String::as_str)) {
            let count = validate_url(
                &crate::counting::wire::endpoint(config, name, counter)?,
                config.allow_http_host.as_deref(),
            )?;
            let generation = url::Url::parse(&crate::projection::endpoint_for(config, name)?)
                .map_err(|_| invalid("invalid generation endpoint"))?;
            let same = count.origin() == generation.origin()
                && count.path().trim_end_matches('/') == generation.path().trim_end_matches('/');
            if same {
                return Err(invalid(
                    "token counter must not use the generation endpoint",
                ));
            }
        }
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

impl Registry {
    /// Retains one shared cached resolver for every agent in this composition.
    pub fn into_model_resolver(self) -> acyclic_harness_core::model::ModelResolver {
        acyclic_harness_core::model::ModelResolver::new(
            self.default_model().to_owned(),
            move |selection| {
                let provider = Arc::new(self.resolve(selection)?);
                Ok((provider.model().clone(), provider))
            },
        )
    }
}

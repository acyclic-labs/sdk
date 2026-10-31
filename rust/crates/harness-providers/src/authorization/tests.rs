//! Real HTTP exchanges replay the original captured llama.cpp bodies.
use super::*;
use crate::{Authentication, Credentials, HttpRequest, HttpResponse, Registry, ReqwestTransport};
use acyclic_harness_core::{model::ModelProvider as _, wire::ErrorCode};
use acyclic_stream::BoxProviderFuture;
use futures::TryStreamExt as _;
use std::sync::{Mutex, Weak};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    sync::watch,
};

struct Secrets {
    generation: usize,
    revoked: bool,
    native_bindings: usize,
}
struct Host {
    origin: String,
    secrets: Arc<Mutex<Secrets>>,
    lifetime: watch::Sender<()>,
    leak: bool,
}
struct Protected {
    secrets: Weak<Mutex<Secrets>>,
    lifetime: watch::Receiver<()>,
    origin: String,
    scheme: AuthorizationScheme,
    transport: Arc<dyn HttpTransport>,
}

fn unavailable() -> Error {
    Error::ModelDiagnostic {
        code: ErrorCode::Unavailable,
        message: "protected credential owner closed".into(),
        custody: None,
    }
}

impl CredentialResolver for Host {
    fn bind(
        &self,
        source: CredentialSource<'_>,
        origin: &str,
        scheme: AuthorizationScheme,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<CredentialBinding> {
        let reference = match source {
            CredentialSource::ApiKey(key) => {
                self.secrets.lock().unwrap().native_bindings += 1;
                return Ok(CredentialBinding::api_key(key, transport));
            }
            CredentialSource::SecretReference(reference) => reference,
        };
        if origin != self.origin {
            return Err(Error::Unauthorized("origin denied".into()));
        }
        let scope = match reference {
            "secrets:/models/primary" => [1; 32],
            "secrets:/models/other" => [2; 32],
            _ => return Err(Error::NotFound("model credential".into())),
        };
        if self.leak {
            return Ok(CredentialBinding::api_key(
                "forbidden-value".into(),
                transport,
            ));
        }
        Ok(CredentialBinding::protected(
            scope,
            Arc::new(Protected {
                secrets: Arc::downgrade(&self.secrets),
                lifetime: self.lifetime.subscribe(),
                origin: origin.into(),
                scheme,
                transport,
            }),
        ))
    }
}

impl HttpTransport for Protected {
    fn send(&self, mut request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            if crate::registry::validate_url(&request.url)?
                .origin()
                .ascii_serialization()
                != self.origin
            {
                return Err(Error::Unauthorized("origin denied".into()));
            }
            let mut lifetime = self.lifetime.clone();
            if lifetime.has_changed().is_err() {
                return Err(unavailable());
            }
            let key = {
                let owner = self.secrets.upgrade().ok_or_else(unavailable)?;
                let secrets = owner.lock().unwrap();
                if secrets.revoked {
                    return Err(Error::Unauthorized("credential revoked".into()));
                }
                format!("fixture-{}", secrets.generation)
            };
            assert!(!request.headers.contains_key("authorization"));
            assert!(!request.headers.contains_key("x-api-key"));
            let (name, value) = match self.scheme {
                AuthorizationScheme::Bearer => ("authorization", format!("Bearer {key}")),
                AuthorizationScheme::ApiKey => ("x-api-key", key),
                AuthorizationScheme::GoogleApiKey => ("x-goog-api-key", key),
            };
            request.headers.insert(name.into(), value);
            tokio::select! {
                biased;
                _ = lifetime.changed() => Err(unavailable()),
                response = self.transport.send(request) => response,
            }
        })
    }
    fn now(&self) -> std::time::Duration {
        self.transport.now()
    }
    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, Result<()>> {
        self.transport.delay(duration)
    }
}

fn specification(origin: &str, authentication: Authentication) -> crate::ProviderSpec {
    let mut spec = crate::default_catalog()["openai"].clone();
    spec.adapter = crate::Adapter::Chat;
    spec.base_url = format!("{origin}/v1");
    spec.authentication = authentication;
    spec.retry.attempts = 1;
    spec.retry.base_delay_ms = 0;
    spec.retry.max_delay_ms = 0;
    spec.token_counter = Some(crate::TokenCounterSpec {
        path: "count_tokens".into(),
        adapter: crate::Adapter::Chat,
        response_field: crate::TokenCountField::InputTokens,
        framing_tokens: 0,
    });
    spec
}

#[tokio::test]
async fn protected_references_rotate_per_exchange_and_never_escape_the_host() -> Result<()> {
    let listener = (15400..15500)
        .find_map(|port| std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).ok())
        .expect("no available loopback fixture port in 15400..15500");
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let secrets = Arc::new(Mutex::new(Secrets {
        generation: 1,
        revoked: false,
        native_bindings: 0,
    }));
    let (lifetime, _) = watch::channel(());
    let host = Arc::new(Host {
        origin: origin.clone(),
        secrets: secrets.clone(),
        lifetime,
        leak: false,
    });
    let mut registry = Registry::with_transport(
        Credentials::explicit(std::collections::BTreeMap::from([(
            "openai".into(),
            "native-key".into(),
        )])),
        Arc::new(ReqwestTransport::new()?),
    )
    .with_credential_resolver(host.clone());
    let native = registry.resolve("openai/gpt-5")?;
    assert_eq!(native.key.as_deref(), Some("native-key"));
    assert_eq!(secrets.lock().unwrap().native_bindings, 1);
    drop(native);
    let spec = specification(
        &origin,
        Authentication::Bearer {
            secret_reference: "secrets:/models/primary".into(),
        },
    );
    let catalog = serde_json::to_string(&spec).unwrap();
    assert!(!catalog.contains("native-key") && !catalog.contains("fixture-1"));
    registry.register("openai".into(), spec.clone())?;
    let provider = registry.resolve("openai/gpt-5")?;
    assert!(provider.key.is_none() && provider.codex_login.is_none());
    assert_eq!(
        secrets.lock().unwrap().native_bindings,
        1,
        "reference never selects the native key"
    );
    let original_scope = provider.reasoning_origin(None, None);
    let (started, accepted) = tokio::sync::oneshot::channel();
    let server_state = secrets.clone();
    let server = tokio::spawn(async move {
        for index in 1..=7 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            let mut length = 0;
            let mut authorization = String::new();
            loop {
                line.clear();
                assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                if let Some(value) = line.strip_prefix("authorization:") {
                    authorization = value.trim().into();
                }
                if let Some(value) = line.strip_prefix("x-api-key:") {
                    authorization = value.trim().into();
                }
            }
            let mut body = vec![0; length];
            socket.read_exact(&mut body).await.unwrap();
            let expected = if index == 5 || index == 6 {
                format!("fixture-{index}")
            } else {
                format!("Bearer fixture-{index}")
            };
            assert_eq!(authorization, expected);
            assert!(!String::from_utf8_lossy(&body).contains("fixture-"));
            server_state.lock().unwrap().generation += 1;
            if index == 7 {
                started.send(()).unwrap();
                let mut byte = [0];
                assert_eq!(
                    socket.read(&mut byte).await.unwrap(),
                    0,
                    "owner loss cancels physical HTTP"
                );
                return;
            }
            // Original provider captures, with provenance in their README.
            let (status, body) = if index == 2 {
                (
                    200,
                    include_str!("../../../conformance/fixtures/inference/llamacpp/text.body"),
                )
            } else {
                (
                    429,
                    include_str!("../../../conformance/fixtures/inference/llamacpp/rate.body"),
                )
            };
            let response = format!(
                "HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    let (request, dispatch) = crate::tests::request(&provider)?;
    let events = provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    assert!(events.iter().any(|event| matches!(event, acyclic_harness_core::model::ModelEvent::Content { delta } if delta.contains("Hello"))));
    assert_eq!(
        provider.reasoning_origin(None, None),
        original_scope,
        "rotation keeps original account scope"
    );
    assert!(
        provider.count_tokens(&request).await.is_err(),
        "captured 429 retains bounded count retry semantics"
    );
    let mut api_key = specification(
        &origin,
        Authentication::ApiKeySecret {
            secret_reference: "secrets:/models/primary".into(),
        },
    );
    api_key.adapter = crate::Adapter::Anthropic;
    api_key.token_counter.as_mut().unwrap().adapter = crate::Adapter::Anthropic;
    registry.register("openai".into(), api_key)?;
    let api_key = registry.resolve("openai/gpt-5")?;
    let (input, operation) = crate::tests::request(&api_key)?;
    assert!(
        api_key
            .generate(&input, operation)
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    drop(api_key);
    secrets.lock().unwrap().revoked = true;
    let error = provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::Unauthorized);
    secrets.lock().unwrap().revoked = false;
    registry.register(
        "openai".into(),
        specification(
            &origin,
            Authentication::Bearer {
                secret_reference: "secrets:/models/other".into(),
            },
        ),
    )?;
    assert_ne!(
        registry
            .resolve("openai/gpt-5")?
            .reasoning_origin(None, None),
        original_scope
    );
    registry.register(
        "openai".into(),
        specification(
            &origin,
            Authentication::Bearer {
                secret_reference: "secrets:/missing".into(),
            },
        ),
    )?;
    assert!(matches!(
        registry.resolve("openai/gpt-5"),
        Err(Error::NotFound(_))
    ));
    registry.register(
        "openai".into(),
        specification(
            &origin.replacen("http://", "https://", 1),
            spec.authentication.clone(),
        ),
    )?;
    assert!(matches!(
        registry.resolve("openai/gpt-5"),
        Err(Error::Unauthorized(_))
    ));
    let leaky = Arc::new(Host {
        origin: origin.clone(),
        secrets: secrets.clone(),
        lifetime: host.lifetime.clone(),
        leak: true,
    });
    let mut refused = Registry::with_transport(
        Credentials::explicit(Default::default()),
        Arc::new(ReqwestTransport::new()?),
    )
    .with_credential_resolver(leaky);
    refused.register("openai".into(), spec)?;
    assert!(
        matches!(refused.resolve("openai/gpt-5"), Err(Error::Invalid(_))),
        "raw-value resolver negative control"
    );
    drop(refused);
    let pending = provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>();
    tokio::pin!(pending);
    tokio::select! {
        result = &mut pending => panic!("pending HTTP completed before owner loss: {result:?}"),
        ready = accepted => ready.unwrap(),
    }
    drop(registry);
    drop(host);
    assert_eq!(pending.await.unwrap_err().code(), ErrorCode::Unavailable);
    server.await.unwrap();
    Ok(())
}

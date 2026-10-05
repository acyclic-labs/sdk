//! Rust-owned browser admission and response-boundary helpers.
//!
//! Fetch, streaming, and cancellation remain JavaScript platform adapters.
//! Endpoint, credential, request admission, and cumulative response bounds are
//! evaluated here so browser and native projections share one policy.

#[cfg(target_arch = "wasm32")]
#[allow(missing_docs, clippy::all, clippy::pedantic, clippy::too_many_lines)]
mod filesystem_wire {
    pub mod protocol {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.protocol.v1.rs"));
        }
    }

    pub mod filesystem {
        pub mod v2 {
            include!(concat!(env!("OUT_DIR"), "/acyclic.filesystem.v2.rs"));
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use crate::filesystem_wire::{filesystem::v2 as wire, protocol::v1 as protocol};
    use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, credential};
    use blake3::hash;
    use js_sys::{Array, Object, Reflect, Uint8Array};
    use prost::Message;
    use tonic::metadata::{Ascii, MetadataValue};
    use tonic::{Request, Status};
    use url::Url;
    use wasm_bindgen::prelude::*;

    const FILESYSTEM_PROTOCOL_VERSION: &str = "1";
    const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!("../../filesystem/src/generated/acyclic-filesystem-v2.bin");
    const MINIMUM_HANDSHAKE_RESPONSE_BYTES: u64 = 512;

    type Transport = tonic_web_wasm_client::Client;
    type GeneratedClient = wire::filesystem_service_client::FilesystemServiceClient<Transport>;

    fn invalid(message: &str) -> JsValue {
        JsValue::from_str(message)
    }

    fn descriptor_digest() -> String {
        hash(FILE_DESCRIPTOR_SET).to_hex().to_string()
    }

    fn loopback(host: Option<&str>) -> bool {
        matches!(host, Some("localhost" | "127.0.0.1" | "::1" | "[::1]"))
    }

    /// Validates the HTTPS or loopback-HTTP endpoint shared by Actors and Workers.
    #[wasm_bindgen]
    pub fn validate_remote_web_endpoint(endpoint: &str) -> Result<(), JsValue> {
        let parsed = Url::parse(endpoint).map_err(|_| invalid("invalid remote endpoint"))?;
        let secure = parsed.scheme() == "https";
        let local_http = parsed.scheme() == "http" && loopback(parsed.host_str());
        if !(secure || local_http)
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid(
                "endpoint must be HTTPS or loopback HTTP without credentials, query, or fragment",
            ));
        }
        Ok(())
    }

    /// Validates the HTTPS endpoint required by native gRPC transports.
    #[wasm_bindgen]
    pub fn validate_remote_web_grpc_endpoint(endpoint: &str) -> Result<(), JsValue> {
        let parsed = Url::parse(endpoint).map_err(|_| invalid("invalid gRPC endpoint"))?;
        if parsed.scheme() != "https"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid(
                "gRPC endpoint must be HTTPS without credentials, query, or fragment",
            ));
        }
        Ok(())
    }

    /// Validates one bearer credential according to the shared Rust policy.
    #[wasm_bindgen]
    pub fn validate_remote_web_credential(token: &str) -> Result<(), JsValue> {
        credential::validate(BEARER_NO_CRLF, token)
            .then_some(())
            .ok_or_else(|| invalid("invalid bearer credential"))
    }

    /// Validates the optional native TLS CA certificate before it reaches the
    /// platform gRPC adapter.
    #[wasm_bindgen]
    pub fn validate_remote_web_ca_certificate(certificate: &str) -> Result<(), JsValue> {
        let length = certificate.as_bytes().len();
        if length == 0 || length > 64 * 1024 {
            return Err(invalid("invalid private CA certificate"));
        }
        Ok(())
    }

    /// Advances a cumulative response byte count under the caller's configured bound.
    #[wasm_bindgen]
    pub fn validate_remote_web_response_chunk(
        observed: u64,
        chunk: u64,
        maximum: u64,
    ) -> Result<u64, JsValue> {
        if maximum == 0 {
            return Err(invalid("response bound must be positive"));
        }
        let total = observed
            .checked_add(chunk)
            .ok_or_else(|| invalid("response exceeds configured bound"))?;
        if total > maximum {
            return Err(invalid("response exceeds configured bound"));
        }
        Ok(total)
    }

    /// Validates the configured cumulative response bound.
    #[wasm_bindgen]
    pub fn validate_remote_web_response_limit(maximum: u64) -> Result<(), JsValue> {
        if maximum == 0 {
            return Err(invalid("response bound must be positive"));
        }
        Ok(())
    }

    /// Validates the configured native gRPC message bound.
    #[wasm_bindgen]
    pub fn validate_remote_web_message_limit(maximum: u64) -> Result<(), JsValue> {
        if maximum == 0 {
            return Err(invalid("message bound must be positive"));
        }
        Ok(())
    }

    /// Limits negotiated by the authenticated Filesystem handshake.
    #[derive(Clone, Debug)]
    pub struct BrowserFilesystemCapabilities {
        pub(crate) inner: wire::Capabilities,
    }

    /// Authenticated Rust-owned Filesystem grpc-web client for browser WASM.
    ///
    /// This client uses the generated FilesystemService contract directly. It
    /// retains negotiated bounds for every later request and exposes streamed
    /// exports through Rust futures; dropping a stream cancels its fetch.
    #[wasm_bindgen]
    pub struct BrowserFilesystemClient {
        client: GeneratedClient,
        authorization: MetadataValue<Ascii>,
        capabilities: BrowserFilesystemCapabilities,
    }

    impl BrowserFilesystemClient {
        /// Connects and completes the canonical Filesystem handshake.
        pub async fn connect(
            endpoint: String,
            bearer_token: String,
            maximum_request_bytes: u64,
            maximum_response_bytes: u64,
        ) -> Result<Self, Status> {
            validate_remote_web_endpoint(&endpoint)
                .map_err(|_| Status::invalid_argument("invalid remote endpoint"))?;
            validate_remote_web_credential(&bearer_token)
                .map_err(|_| Status::invalid_argument("invalid bearer credential"))?;
            if maximum_request_bytes == 0
                || maximum_response_bytes < MINIMUM_HANDSHAKE_RESPONSE_BYTES
            {
                return Err(Status::invalid_argument(
                    "invalid request or response bound",
                ));
            }
            let authorization: MetadataValue<Ascii> = format!("Bearer {bearer_token}")
                .parse()
                .map_err(|_| Status::invalid_argument("invalid bearer credential"))?;
            let transport = Transport::new(endpoint);
            let mut client = GeneratedClient::new(transport)
                .max_decoding_message_size(
                    usize::try_from(maximum_response_bytes)
                        .map_err(|_| Status::invalid_argument("response bound is too large"))?,
                )
                .max_encoding_message_size(
                    usize::try_from(maximum_request_bytes)
                        .map_err(|_| Status::invalid_argument("request bound is too large"))?,
                );
            let mut request = Request::new(wire::HandshakeRequest {
                protocol: Some(protocol::HandshakeRequest {
                    protocol: Some(protocol::ProtocolIdentity {
                        version: FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                        descriptor_digest: descriptor_digest(),
                    }),
                    required: Some(protocol::CapabilitySet {
                        capabilities: vec![protocol::Capability {
                            name: "filesystem".to_owned(),
                            version: FILESYSTEM_PROTOCOL_VERSION.to_owned(),
                        }],
                    }),
                }),
            });
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
            let response = client.handshake(request).await?.into_inner();
            let capabilities = validate_handshake(response)?;
            let request_limit = capabilities
                .maximum_request_bytes
                .min(maximum_request_bytes);
            let response_limit = capabilities
                .maximum_response_bytes
                .min(maximum_response_bytes);
            client = client
                .max_encoding_message_size(
                    usize::try_from(request_limit)
                        .map_err(|_| Status::invalid_argument("request bound is too large"))?,
                )
                .max_decoding_message_size(
                    usize::try_from(response_limit)
                        .map_err(|_| Status::invalid_argument("response bound is too large"))?,
                );
            Ok(Self {
                client,
                authorization,
                capabilities: BrowserFilesystemCapabilities {
                    inner: capabilities,
                },
            })
        }

        /// Returns the negotiated server limits and capabilities.
        #[must_use]
        pub fn capabilities(&self) -> &wire::Capabilities {
            &self.capabilities.inner
        }

        fn request<T>(&self, value: T) -> Request<T> {
            let mut request = Request::new(value);
            request
                .metadata_mut()
                .insert("authorization", self.authorization.clone());
            request
        }

        /// Starts a bounded streamed export. Dropping the returned stream
        /// aborts the browser fetch through `tonic-web-wasm-client`.
        pub async fn export_stream(
            &self,
            request: wire::ExportRequest,
        ) -> Result<tonic::Streaming<wire::ExportChunk>, Status> {
            let mut client = self.client.clone();
            Ok(client.export(self.request(request)).await?.into_inner())
        }

        /// Sends the canonical cancellation operation for a remote request.
        pub async fn cancel_request(
            &self,
            request: wire::CancelRequest,
        ) -> Result<wire::CancelResponse, Status> {
            let mut client = self.client.clone();
            Ok(client.cancel(self.request(request)).await?.into_inner())
        }
    }

    #[wasm_bindgen]
    impl BrowserFilesystemClient {
        /// Connects from JavaScript using the Rust-owned authenticated handshake.
        #[wasm_bindgen(js_name = connect)]
        pub async fn connect_js(
            endpoint: String,
            bearer_token: String,
            maximum_request_bytes: u64,
            maximum_response_bytes: u64,
        ) -> Result<BrowserFilesystemClient, JsValue> {
            Self::connect(
                endpoint,
                bearer_token,
                maximum_request_bytes,
                maximum_response_bytes,
            )
            .await
            .map_err(|error| JsValue::from_str(&error.to_string()))
        }

        /// Returns negotiated capability limits to JavaScript.
        #[wasm_bindgen(js_name = capabilities)]
        pub fn capabilities_js(&self) -> Result<JsValue, JsValue> {
            let value = Object::new();
            Reflect::set(
                &value,
                &JsValue::from_str("contractVersion"),
                &JsValue::from_str(&self.capabilities().contract_version),
            )?;
            Reflect::set(
                &value,
                &JsValue::from_str("maximumRequestBytes"),
                &JsValue::from_f64(self.capabilities().maximum_request_bytes as f64),
            )?;
            Reflect::set(
                &value,
                &JsValue::from_str("maximumResponseBytes"),
                &JsValue::from_f64(self.capabilities().maximum_response_bytes as f64),
            )?;
            Reflect::set(
                &value,
                &JsValue::from_str("maximumTransactionMutations"),
                &JsValue::from_f64(self.capabilities().maximum_transaction_mutations as f64),
            )?;
            Reflect::set(
                &value,
                &JsValue::from_str("maximumPageItems"),
                &JsValue::from_f64(self.capabilities().maximum_page_items as f64),
            )?;
            Ok(value.into())
        }

        /// Sends a typed cancellation request encoded by the Rust contract.
        #[wasm_bindgen(js_name = cancel)]
        pub async fn cancel_js(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request = wire::CancelRequest::decode(request.as_slice())
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let response = self
                .cancel_request(request)
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let mut encoded = Vec::new();
            response
                .encode(&mut encoded)
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            Ok(encoded)
        }

        /// Collects a typed export stream into encoded chunks.
        #[wasm_bindgen(js_name = export)]
        pub async fn export_js(&self, request: Vec<u8>) -> Result<Array, JsValue> {
            let request = wire::ExportRequest::decode(request.as_slice())
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let mut stream = self
                .export_stream(request)
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let chunks = Array::new();
            while let Some(chunk) = stream
                .message()
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?
            {
                let mut encoded = Vec::new();
                chunk
                    .encode(&mut encoded)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?;
                chunks.push(&Uint8Array::from(encoded.as_slice()));
            }
            Ok(chunks)
        }
    }

    fn validate_handshake(response: wire::HandshakeResponse) -> Result<wire::Capabilities, Status> {
        let handshake = response
            .protocol
            .ok_or_else(|| Status::internal("handshake response is absent"))?;
        let protocol = handshake
            .protocol
            .ok_or_else(|| Status::internal("handshake protocol is absent"))?;
        if protocol.version != FILESYSTEM_PROTOCOL_VERSION {
            return Err(Status::failed_precondition(
                "filesystem protocol version is unsupported",
            ));
        }
        if protocol.descriptor_digest != descriptor_digest() {
            return Err(Status::failed_precondition(
                "filesystem descriptor digest does not match",
            ));
        }
        let supported = handshake
            .supported
            .ok_or_else(|| Status::internal("supported capabilities are absent"))?;
        if !supported.capabilities.iter().any(|capability| {
            capability.name == "filesystem" && capability.version == FILESYSTEM_PROTOCOL_VERSION
        }) {
            return Err(Status::failed_precondition(
                "filesystem capability version is unsupported",
            ));
        }
        let capabilities = response
            .capabilities
            .ok_or_else(|| Status::internal("filesystem capabilities are absent"))?;
        if capabilities.contract_version != FILESYSTEM_PROTOCOL_VERSION
            || capabilities.maximum_request_bytes == 0
            || capabilities.maximum_response_bytes == 0
            || capabilities.maximum_transaction_mutations == 0
            || capabilities.maximum_page_items == 0
        {
            return Err(Status::failed_precondition(
                "filesystem capabilities are invalid",
            ));
        }
        Ok(capabilities)
    }

    /// Checks an HTTP content-length without first narrowing it through a JS number.
    #[wasm_bindgen]
    pub fn validate_remote_web_content_length(
        content_length: &str,
        maximum: u64,
    ) -> Result<(), JsValue> {
        let chunk = content_length
            .parse::<u64>()
            .map_err(|_| invalid("invalid response content length"))?;
        validate_remote_web_response_chunk(0, chunk, maximum).map(|_| ())
    }

    fn validate_method(method: &str) -> Result<(), JsValue> {
        // Protobuf string fields use the empty string as their wire default. The
        // service keeps accepting that default for backwards-compatible request
        // construction; a caller-provided method is still checked for header
        // delimiter bytes before it reaches fetch.
        if method.contains(['\r', '\n']) {
            return Err(invalid("method contains forbidden header delimiter"));
        }
        Ok(())
    }

    fn validate_alias(alias: &str) -> Result<(), JsValue> {
        if alias.is_empty()
            || alias == "."
            || alias == ".."
            || alias.len() > 256
            || !alias
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(invalid("invalid deployment alias"));
        }
        Ok(())
    }

    /// Validates the Rust Workers invoke-version path and request admission rules.
    #[wasm_bindgen]
    pub fn validate_workers_invoke_version(
        version_sha256: &[u8],
        method: &str,
    ) -> Result<(), JsValue> {
        if version_sha256.len() != 32 {
            return Err(invalid("version digest must contain exactly 32 bytes"));
        }
        validate_method(method)
    }

    /// Validates the Rust Workers invoke-deployment path and request admission rules.
    #[wasm_bindgen]
    pub fn validate_workers_invoke_deployment(alias: &str, method: &str) -> Result<(), JsValue> {
        validate_alias(alias)?;
        validate_method(method)
    }

    /// Validates the Rust Actors invoke request admission rules.
    #[wasm_bindgen]
    pub fn validate_actors_invoke(actor_id: &str, method: &str) -> Result<(), JsValue> {
        if actor_id.is_empty() {
            return Err(invalid("actor id must be non-empty"));
        }
        validate_method(method)
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::{BrowserFilesystemCapabilities, BrowserFilesystemClient};

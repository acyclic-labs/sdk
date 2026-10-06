//! Rust-owned browser admission and response-boundary helpers.
//!
//! Fetch, streaming, and cancellation remain JavaScript platform adapters.
//! Endpoint, credential, request admission, and cumulative response bounds are
//! evaluated here so browser and native projections share one policy.

#[cfg(target_arch = "wasm32")]
#[allow(dead_code, missing_docs, clippy::all, clippy::pedantic, clippy::too_many_lines)]
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
#[allow(missing_docs, clippy::all, clippy::pedantic, clippy::too_many_lines)]
mod harness_wire {
    pub mod acyclic {
        pub mod protocol {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.protocol.v1.rs"));
            }
        }

        pub mod harness {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v2.rs"));
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use crate::filesystem_wire::{filesystem::v2 as wire, protocol::v1 as protocol};
    use crate::harness_wire::acyclic::{
        harness::v2 as harness,
        protocol::v1 as harness_protocol,
    };
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
        maximum_response_bytes: u64,
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
                maximum_response_bytes: response_limit,
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
            let mut observed_bytes = 0_u64;
            while let Some(chunk) = stream
                .message()
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?
            {
                let mut encoded = Vec::new();
                chunk
                    .encode(&mut encoded)
                    .map_err(|error| JsValue::from_str(&error.to_string()))?;
                observed_bytes = validate_remote_web_response_chunk(
                    observed_bytes,
                    u64::try_from(encoded.len())
                        .map_err(|_| JsValue::from_str("export chunk is too large"))?,
                    self.maximum_response_bytes,
                )?;
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

    /// Validate a wire handshake and return its canonical Rust-owned capabilities.
    #[wasm_bindgen(js_name = validateRemoteWebFilesystemHandshake)]
    pub fn validate_remote_web_filesystem_handshake(response: &[u8]) -> Result<Vec<u8>, JsValue> {
        let response = wire::HandshakeResponse::decode(response)
            .map_err(|error| invalid(&format!("invalid filesystem handshake: {error}")))?;
        let capabilities = validate_handshake(response)
            .map_err(|error| invalid(&error.to_string()))?;
        Ok(capabilities.encode_to_vec())
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

    const HARNESS_PROTOCOL_VERSION: &str = "2";
    const HARNESS_CAPABILITIES: &[(&str, &str)] = &[
        ("submit", "1"),
        ("replay", "1"),
        ("observe", "1"),
        ("cancel", "1"),
    ];
    const HARNESS_DESCRIPTOR_SET: &[u8] =
        include_bytes!("../../harness/src/generated/harness-archived-v2.bin");
    /// Rust-owned default request bound for browser Harness calls.
    pub const DEFAULT_HARNESS_MAXIMUM_REQUEST_BYTES: u64 = 16 * 1024 * 1024;
    /// Rust-owned default response bound for browser Harness calls.
    pub const DEFAULT_HARNESS_MAXIMUM_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

    type HarnessTransport = tonic_web_wasm_client::Client;
    type HarnessGeneratedClient =
        harness::harness_service_client::HarnessServiceClient<HarnessTransport>;

    fn harness_descriptor_digest() -> String {
        hash(HARNESS_DESCRIPTOR_SET).to_hex().to_string()
    }

    fn harness_protocol_identity() -> harness_protocol::ProtocolIdentity {
        harness_protocol::ProtocolIdentity {
            version: HARNESS_PROTOCOL_VERSION.to_owned(),
            descriptor_digest: harness_descriptor_digest(),
        }
    }

    /// The authenticated Harness protocol identity negotiated by a browser client.
    #[derive(Clone, Debug)]
    pub struct BrowserHarnessCapabilities {
        protocol: harness_protocol::ProtocolIdentity,
    }

    /// Authenticated Rust-owned Harness gRPC-Web client for browser WASM.
    #[wasm_bindgen]
    pub struct BrowserHarnessClient {
        client: HarnessGeneratedClient,
        authorization: MetadataValue<Ascii>,
        capabilities: BrowserHarnessCapabilities,
        maximum_response_bytes: u64,
    }

    impl BrowserHarnessClient {
        pub async fn connect(endpoint: String, bearer_token: String) -> Result<Self, Status> {
            Self::connect_with_limits(
                endpoint,
                bearer_token,
                DEFAULT_HARNESS_MAXIMUM_REQUEST_BYTES,
                DEFAULT_HARNESS_MAXIMUM_RESPONSE_BYTES,
            )
            .await
        }

        pub async fn connect_with_limits(
            endpoint: String,
            bearer_token: String,
            maximum_request_bytes: u64,
            maximum_response_bytes: u64,
        ) -> Result<Self, Status> {
            validate_remote_web_endpoint(&endpoint)
                .map_err(|_| Status::invalid_argument("invalid remote endpoint"))?;
            validate_remote_web_credential(&bearer_token)
                .map_err(|_| Status::invalid_argument("invalid bearer credential"))?;
            if maximum_request_bytes == 0 || maximum_response_bytes == 0 {
                return Err(Status::invalid_argument(
                    "request and response bounds must be positive",
                ));
            }
            let authorization: MetadataValue<Ascii> = format!("Bearer {bearer_token}")
                .parse()
                .map_err(|_| Status::invalid_argument("invalid bearer credential"))?;
            let transport = HarnessTransport::new(endpoint);
            let client = HarnessGeneratedClient::new(transport)
                .max_decoding_message_size(
                    usize::try_from(maximum_response_bytes)
                        .map_err(|_| Status::invalid_argument("response bound is too large"))?,
                )
                .max_encoding_message_size(
                    usize::try_from(maximum_request_bytes)
                        .map_err(|_| Status::invalid_argument("request bound is too large"))?,
                );
            let mut client = client;
            let mut request = Request::new(harness_protocol::HandshakeRequest {
                protocol: Some(harness_protocol_identity()),
                required: Some(harness_protocol::CapabilitySet {
                    capabilities: HARNESS_CAPABILITIES
                        .iter()
                        .map(|(name, version)| harness_protocol::Capability {
                            name: (*name).to_owned(),
                            version: (*version).to_owned(),
                        })
                        .collect(),
                }),
            });
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
            let response = client.handshake(request).await?.into_inner();
            let capabilities = validate_harness_handshake(response)?;
            Ok(Self {
                client,
                authorization,
                capabilities,
                maximum_response_bytes,
            })
        }

        #[must_use]
        pub fn capabilities(&self) -> &BrowserHarnessCapabilities {
            &self.capabilities
        }

        fn request<T>(&self, value: T) -> Request<T> {
            let mut request = Request::new(value);
            request
                .metadata_mut()
                .insert("authorization", self.authorization.clone());
            request
        }

        pub async fn submit(
            &self,
            request: harness::CommandEnvelope,
        ) -> Result<harness::Admission, Status> {
            let mut client = self.client.clone();
            Ok(client.submit(self.request(request)).await?.into_inner())
        }

        pub async fn replay(
            &self,
            request: harness::ResumeRequest,
        ) -> Result<tonic::Streaming<harness::Delivery>, Status> {
            let mut client = self.client.clone();
            Ok(client.replay(self.request(request)).await?.into_inner())
        }

        pub async fn observe(
            &self,
            request: harness::ObserveRequest,
        ) -> Result<harness::OperationStatus, Status> {
            let mut client = self.client.clone();
            Ok(client.observe(self.request(request)).await?.into_inner())
        }

        pub async fn cancel(
            &self,
            request: harness::CancelRequest,
        ) -> Result<harness::CancelResponse, Status> {
            let mut client = self.client.clone();
            Ok(client.cancel(self.request(request)).await?.into_inner())
        }
    }

    #[wasm_bindgen]
    impl BrowserHarnessClient {
        #[wasm_bindgen(js_name = connect)]
        pub async fn connect_js(
            endpoint: String,
            bearer_token: String,
        ) -> Result<BrowserHarnessClient, JsValue> {
            Self::connect(endpoint, bearer_token)
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))
        }

        #[wasm_bindgen(js_name = connectWithLimits)]
        pub async fn connect_with_limits_js(
            endpoint: String,
            bearer_token: String,
            maximum_request_bytes: u64,
            maximum_response_bytes: u64,
        ) -> Result<BrowserHarnessClient, JsValue> {
            Self::connect_with_limits(
                endpoint,
                bearer_token,
                maximum_request_bytes,
                maximum_response_bytes,
            )
            .await
            .map_err(|error| JsValue::from_str(&error.to_string()))
        }

        #[wasm_bindgen(js_name = capabilities)]
        pub fn capabilities_js(&self) -> Result<JsValue, JsValue> {
            let value = Object::new();
            Reflect::set(
                &value,
                &JsValue::from_str("version"),
                &JsValue::from_str(&self.capabilities.protocol.version),
            )?;
            Reflect::set(
                &value,
                &JsValue::from_str("descriptorDigest"),
                &JsValue::from_str(&self.capabilities.protocol.descriptor_digest),
            )?;
            Ok(value.into())
        }

        #[wasm_bindgen(js_name = submit)]
        pub async fn submit_js(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request = harness::CommandEnvelope::decode(request.as_slice())
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let response = self
                .submit(request)
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            Ok(response.encode_to_vec())
        }

        #[wasm_bindgen(js_name = observe)]
        pub async fn observe_js(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request = harness::ObserveRequest::decode(request.as_slice())
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let response = self
                .observe(request)
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            Ok(response.encode_to_vec())
        }

        #[wasm_bindgen(js_name = cancel)]
        pub async fn cancel_js(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request = harness::CancelRequest::decode(request.as_slice())
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let response = self
                .cancel(request)
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            Ok(response.encode_to_vec())
        }

        #[wasm_bindgen(js_name = replay)]
        pub async fn replay_js(&self, request: Vec<u8>) -> Result<Array, JsValue> {
            let request = harness::ResumeRequest::decode(request.as_slice())
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let mut stream = self
                .replay(request)
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            let frames = Array::new();
            let mut observed_bytes = 0_u64;
            while let Some(frame) = stream
                .message()
                .await
                .map_err(|error| JsValue::from_str(&error.to_string()))?
            {
                let encoded = frame.encode_to_vec();
                observed_bytes = validate_remote_web_response_chunk(
                    observed_bytes,
                    u64::try_from(encoded.len())
                        .map_err(|_| JsValue::from_str("replay delivery is too large"))?,
                    self.maximum_response_bytes,
                )?;
                frames.push(&Uint8Array::from(encoded.as_slice()));
            }
            Ok(frames)
        }
    }

    fn validate_harness_handshake(
        response: harness_protocol::HandshakeResponse,
    ) -> Result<BrowserHarnessCapabilities, Status> {
        let expected = harness_protocol_identity();
        let actual = response
            .protocol
            .ok_or_else(|| Status::internal("harness handshake protocol is absent"))?;
        if actual != expected {
            return Err(Status::failed_precondition(
                "harness protocol identity does not match",
            ));
        }
        let supported = response
            .supported
            .ok_or_else(|| Status::internal("harness capabilities are absent"))?;
        if HARNESS_CAPABILITIES.iter().any(|(name, version)| {
            !supported
                .capabilities
                .iter()
                .any(|capability| capability.name == *name && capability.version == *version)
        }) {
            return Err(Status::failed_precondition(
                "harness operation capability is unsupported",
            ));
        }
        Ok(BrowserHarnessCapabilities { protocol: actual })
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm::{
    BrowserFilesystemCapabilities, BrowserFilesystemClient,
    BrowserHarnessCapabilities, BrowserHarnessClient,
    validate_remote_web_endpoint, validate_remote_web_grpc_endpoint,
    validate_remote_web_credential, validate_remote_web_filesystem_handshake,
};

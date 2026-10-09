//! Seven generated Workers unary RPCs through one Rust byte-admission boundary.
use crate::{
    client_config::{
        Failure, WorkersCallOptions, WorkersClientOptions, append_metadata, validate_deadline,
    },
    domain, wire,
};
use prost::Message;
use std::{future::Future, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::codegen::{Body, Bytes, StdError};

/// Monotonic cancellation handle shared by native and browser operations.
#[derive(Default)]
#[cfg_attr(
    all(feature = "node-binding", not(target_arch = "wasm32")),
    napi_derive::napi
)]
#[cfg_attr(
    all(feature = "browser-binding", target_arch = "wasm32"),
    wasm_bindgen::prelude::wasm_bindgen
)]
pub struct WorkersCancellation {
    token: CancellationToken,
}

#[cfg_attr(
    all(feature = "node-binding", not(target_arch = "wasm32")),
    napi_derive::napi
)]
#[cfg_attr(
    all(feature = "browser-binding", target_arch = "wasm32"),
    wasm_bindgen::prelude::wasm_bindgen
)]
impl WorkersCancellation {
    /// Create a fresh operation cancellation handle.
    #[cfg_attr(
        all(feature = "node-binding", not(target_arch = "wasm32")),
        napi_derive::napi(constructor)
    )]
    #[cfg_attr(
        all(feature = "browser-binding", target_arch = "wasm32"),
        wasm_bindgen::prelude::wasm_bindgen(constructor)
    )]
    pub fn new() -> Self {
        Self::default()
    }
    /// Cancel all operations attached to this handle.
    #[cfg_attr(
        all(feature = "node-binding", not(target_arch = "wasm32")),
        napi_derive::napi
    )]
    pub fn cancel(&self) {
        self.token.cancel();
    }
    /// Whether cancellation has occurred.
    #[cfg_attr(
        all(feature = "node-binding", not(target_arch = "wasm32")),
        napi_derive::napi(getter)
    )]
    #[cfg_attr(
        all(feature = "browser-binding", target_arch = "wasm32"),
        wasm_bindgen::prelude::wasm_bindgen(getter)
    )]
    pub fn cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

impl Failure {
    fn status(status: &tonic::Status, maximum: usize) -> Self {
        if status.details().len() > maximum {
            return Self::local(
                "response_too_large",
                "status details exceed message ceiling",
            );
        }
        let detail = wire::Error::decode(status.details())
            .ok()
            .filter(|detail| detail.code != wire::ErrorCode::Unspecified as i32);
        Self {
            code: "service".to_owned(),
            message: status.message().to_owned(),
            grpc_code: Some(status.code() as i32),
            service_code: detail.as_ref().map(|detail| detail.code),
            service_message: detail.map(|detail| detail.message),
            raw_details: status.details().to_vec(),
        }
    }
}

async fn run<T>(
    operation: impl Future<Output = Result<T, Failure>>,
    token: CancellationToken,
    deadline: Option<u32>,
) -> Result<T, Failure> {
    if token.is_cancelled() {
        return Err(Failure::cancelled());
    }
    let expire = async {
        match deadline {
            None => std::future::pending::<()>().await,
            #[cfg(not(target_arch = "wasm32"))]
            Some(millis) => tokio::time::sleep(Duration::from_millis(u64::from(millis))).await,
            #[cfg(target_arch = "wasm32")]
            Some(millis) => gloo_timers::future::TimeoutFuture::new(millis).await,
        }
    };
    tokio::select! {
        biased;
        _ = token.cancelled() => Err(Failure::cancelled()),
        _ = expire => Err(Failure::local("deadline_exceeded", "Workers operation deadline exceeded")),
        result = operation => result,
    }
}

#[cfg(not(target_arch = "wasm32"))]
type Backend = crate::grpc::Client;
#[cfg(target_arch = "wasm32")]
type Backend = wire::workers_service_client::WorkersServiceClient<
    tonic::service::interceptor::InterceptedService<tonic_web_wasm_client::Client, BrowserAuth>,
>;

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
struct BrowserAuth(tonic::metadata::MetadataValue<tonic::metadata::Ascii>);
#[cfg(target_arch = "wasm32")]
impl tonic::service::Interceptor for BrowserAuth {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

/// Connected source-owned Workers transport, cloned per unary RPC.
pub struct Client {
    inner: Backend,
    maximum: usize,
    deadline: Option<u32>,
    metadata: tonic::metadata::MetadataMap,
}

impl Client {
    fn parameters(
        &self,
        options: WorkersCallOptions,
    ) -> Result<(Option<u32>, tonic::metadata::MetadataMap), Failure> {
        Ok((
            validate_deadline(options.deadline_millis.or(self.deadline))?,
            append_metadata(self.metadata.clone(), options.metadata)?,
        ))
    }

    /// Connect with Rust-owned configuration and cancellable construction.
    pub async fn connect(
        options: WorkersClientOptions,
        token: CancellationToken,
    ) -> Result<Self, Failure> {
        if token.is_cancelled() {
            return Err(Failure::cancelled());
        }
        let options = options.validate()?;
        let deadline = options.deadline;
        run(
            async move {
                #[cfg(not(target_arch = "wasm32"))]
                let inner = crate::grpc::connect_with_ca_certificate(
                    &options.endpoint,
                    &options.token,
                    options.ca.as_ref().map(|ca| ca.as_bytes()),
                )
                .await
                .map_err(|error| {
                    let code = if matches!(error, crate::grpc::ConnectError::Transport(_)) {
                        "unavailable"
                    } else {
                        "invalid_argument"
                    };
                    Failure::local(code, error)
                })?;
                #[cfg(target_arch = "wasm32")]
                let inner = {
                    let options_fetch = tonic_web_wasm_client::options::FetchOptions::new()
                        .redirect(tonic_web_wasm_client::options::Redirect::Error)
                        .response_limits(
                            tonic_web_wasm_client::limits::UnaryResponseLimits::new(
                                options.maximum,
                            )
                            .ok_or_else(|| {
                                Failure::local(
                                    "invalid_argument",
                                    "response budget exceeds platform size",
                                )
                            })?,
                        );
                    let transport = tonic_web_wasm_client::Client::new_with_options(
                        options.endpoint,
                        options_fetch,
                    );
                    wire::workers_service_client::WorkersServiceClient::with_interceptor(
                        transport,
                        BrowserAuth(options.authorization),
                    )
                };
                Ok(Self {
                    inner: inner
                        .max_decoding_message_size(options.maximum)
                        .max_encoding_message_size(options.maximum),
                    maximum: options.maximum,
                    deadline,
                    metadata: options.metadata,
                })
            },
            token,
            deadline,
        )
        .await
    }

    /// Actual native/browser transport selected by the compilation target.
    pub const fn transport(&self) -> &'static str {
        #[cfg(not(target_arch = "wasm32"))]
        {
            "grpc"
        }
        #[cfg(target_arch = "wasm32")]
        {
            "grpc-web"
        }
    }

    /// Execute a canonical protobuf service method after checked wire admission.
    pub(crate) async fn call(
        &self,
        method: &str,
        bytes: &[u8],
        options: WorkersCallOptions,
        token: CancellationToken,
    ) -> Result<Vec<u8>, Failure> {
        if bytes.len() > self.maximum {
            return Err(Failure::local(
                "request_too_large",
                "request exceeds message ceiling",
            ));
        }
        let (deadline, metadata) = self.parameters(options)?;
        run(
            dispatch(
                self.inner.clone(),
                method,
                bytes,
                self.maximum,
                deadline,
                &metadata,
            ),
            token,
            deadline,
        )
        .await
    }
}

// One bridge registry binds generated method symbols, semantic types and existing admission.
macro_rules! operations {
    ($apply:ident) => { $apply! {
        ("PublishVersion", publish_version, PublishVersionRequest, PublishVersionResponse, crate::validate_publish),
        ("SelectDeployment", select_deployment, SelectDeploymentRequest, SelectDeploymentResponse, crate::validate_select),
        ("SubmitJob", submit_job, SubmitJobRequest, SubmitJobResponse, crate::validate_submit),
        ("InspectJob", inspect_job, InspectJobRequest, InspectJobResponse, |_| Ok(())),
        ("CancelJob", cancel_job, CancelJobRequest, CancelJobResponse, |_| Ok(())),
        ("InvokeVersion", invoke_version, InvokeVersionRequest, InvokeResponse, crate::validate_invoke_version),
        ("InvokeDeployment", invoke_deployment, InvokeDeploymentRequest, InvokeResponse, crate::validate_invoke_deployment)
    } };
}

macro_rules! typed_methods {
    ($(($name:literal, $method:ident, $input:ident, $output:ident, $validate:expr)),* $(,)?) => {
        impl Client { $(
            #[doc = concat!("Execute the canonical ", $name, " operation with Rust semantic types.")]
            pub async fn $method(&self, input: domain::$input, options: WorkersCallOptions, token: CancellationToken) -> Result<domain::$output, Failure> {
                if token.is_cancelled() { return Err(Failure::cancelled()); }
                let input = checked_request::<wire::$input, domain::$input>(wire::$input::from(input), $validate)?;
                if input.encoded_len() > self.maximum { return Err(Failure::local("request_too_large", "request exceeds message ceiling")); }
                let (deadline, metadata) = self.parameters(options)?;
                let mut client = self.inner.clone();
                run(async move {
                    let output = client.$method(request(input, &metadata, deadline)).await
                        .map_err(|status| Failure::status(&status, self.maximum))?.into_inner();
                    checked_response::<wire::$output, domain::$output>(output)
                }, token, deadline).await
            }
        )* }
    };
}
operations!(typed_methods);

fn admit<W, D>(
    bytes: &[u8],
    validate: impl FnOnce(&W) -> Result<(), crate::ContractError>,
) -> Result<W, Failure>
where
    W: Message + Default + From<D>,
    D: TryFrom<W>,
    D::Error: std::fmt::Display,
{
    let wire = W::decode(bytes).map_err(|error| Failure::local("invalid_argument", error))?;
    checked_request::<W, D>(wire, validate)
}

fn checked_request<W, D>(
    wire: W,
    validate: impl FnOnce(&W) -> Result<(), crate::ContractError>,
) -> Result<W, Failure>
where
    W: From<D>,
    D: TryFrom<W>,
    D::Error: std::fmt::Display,
{
    validate(&wire).map_err(|error| Failure::local("invalid_argument", error))?;
    D::try_from(wire)
        .map(W::from)
        .map_err(|error| Failure::local("invalid_argument", error))
}

fn checked_response<W, D>(wire: W) -> Result<D, Failure>
where
    D: TryFrom<W>,
    D::Error: std::fmt::Display,
{
    D::try_from(wire).map_err(|error| Failure::local("malformed_response", error))
}

fn response<W, D>(wire: W, maximum: usize) -> Result<Vec<u8>, Failure>
where
    W: Message + From<D>,
    D: TryFrom<W>,
    D::Error: std::fmt::Display,
{
    let wire = W::from(checked_response::<W, D>(wire)?);
    if wire.encoded_len() > maximum {
        return Err(Failure::local(
            "response_too_large",
            "response exceeds message ceiling",
        ));
    }
    Ok(wire.encode_to_vec())
}

fn request<T>(
    value: T,
    metadata: &tonic::metadata::MetadataMap,
    deadline: Option<u32>,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    *request.metadata_mut() = metadata.clone();
    if let Some(millis) = deadline {
        request.set_timeout(Duration::from_millis(u64::from(millis)));
    }
    request
}

async fn dispatch<T>(
    mut client: wire::workers_service_client::WorkersServiceClient<T>,
    method: &str,
    bytes: &[u8],
    maximum: usize,
    deadline: Option<u32>,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Vec<u8>, Failure>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Clone,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    macro_rules! invoke {
        ($(($name:literal, $method:ident, $input:ident, $output:ident, $validate:expr)),* $(,)?) => {
            match method { $( $name => {
                let input = admit::<wire::$input, domain::$input>(bytes, $validate)?;
                let output = client.$method(request(input, metadata, deadline)).await
                    .map_err(|status| Failure::status(&status, maximum))?.into_inner();
                response::<wire::$output, domain::$output>(output, maximum)
            }, )* _ => Err(Failure::local("invalid_argument", "unknown Workers service method")) }
        };
    }
    operations!(invoke)
}

#[cfg(all(feature = "node-binding", not(target_arch = "wasm32")))]
mod native {
    use super::*;
    use napi::bindgen_prelude::Buffer;
    use napi_derive::napi;
    use std::sync::Arc;

    /// Native byte projection of the source-owned diagnostic.
    #[napi(object)]
    pub struct NativeFailure {
        /// Stable category.
        pub code: String,
        /// Original status or admission message.
        pub message: String,
        /// Numeric gRPC status.
        #[napi(js_name = "grpcCode")]
        pub grpc_code: Option<i32>,
        /// Decoded signed int32 service code.
        #[napi(js_name = "serviceCode")]
        pub service_code: Option<i32>,
        /// Decoded service message.
        #[napi(js_name = "serviceMessage")]
        pub service_message: Option<String>,
        /// Exact status detail bytes.
        #[napi(js_name = "rawDetails")]
        pub raw_details: Buffer,
    }
    impl From<Failure> for NativeFailure {
        fn from(error: Failure) -> Self {
            Self {
                code: error.code,
                message: error.message,
                grpc_code: error.grpc_code,
                service_code: error.service_code,
                service_message: error.service_message,
                raw_details: error.raw_details.into(),
            }
        }
    }

    /// Connected native Workers transport.
    #[napi]
    pub struct WorkersClient {
        inner: Arc<Client>,
    }
    /// Structured native construction result.
    #[napi(object, object_from_js = false)]
    pub struct ConnectResult {
        /// Connected client on success.
        pub client: Option<WorkersClient>,
        /// Lossless failure on error.
        pub error: Option<NativeFailure>,
    }
    /// Structured native operation result.
    #[napi(object)]
    pub struct OperationResult {
        /// Encoded response on success.
        pub value: Option<Buffer>,
        /// Lossless failure on error.
        pub error: Option<NativeFailure>,
    }
    #[napi]
    impl WorkersClient {
        /// Binding package identity for the maintained loader handshake.
        #[napi]
        pub fn version() -> String {
            env!("CARGO_PKG_VERSION").to_owned()
        }
        /// Admit typed configuration and construct a native connection.
        #[napi(js_name = "connectResult")]
        pub async fn connect_result(
            config_value: serde_json::Value,
            cancellation: &WorkersCancellation,
        ) -> napi::Result<ConnectResult> {
            let config = serde_json::from_value::<WorkersClientOptions>(config_value)
                .map_err(|error| Failure::local("invalid_argument", error));
            let result = match config {
                Ok(config) => Client::connect(config, cancellation.token.clone()).await,
                Err(error) => Err(error),
            };
            Ok(match result {
                Ok(inner) => ConnectResult {
                    client: Some(Self {
                        inner: Arc::new(inner),
                    }),
                    error: None,
                },
                Err(error) => ConnectResult {
                    client: None,
                    error: Some(error.into()),
                },
            })
        }
        /// Actual selected transport.
        #[napi(getter)]
        pub fn transport(&self) -> String {
            self.inner.transport().to_owned()
        }
        /// Call any of the seven generated unary methods with one cancellation handle.
        #[napi]
        pub async fn call(
            &self,
            method: String,
            request: Buffer,
            options_value: serde_json::Value,
            cancellation: &WorkersCancellation,
        ) -> napi::Result<OperationResult> {
            let options = serde_json::from_value::<WorkersCallOptions>(options_value)
                .map_err(|error| Failure::local("invalid_argument", error));
            let result = match options {
                Ok(options) => {
                    self.inner
                        .call(&method, &request, options, cancellation.token.clone())
                        .await
                }
                Err(error) => Err(error),
            };
            Ok(match result {
                Ok(value) => OperationResult {
                    value: Some(value.into()),
                    error: None,
                },
                Err(error) => OperationResult {
                    value: None,
                    error: Some(error.into()),
                },
            })
        }
    }
}

#[cfg(all(feature = "browser-binding", target_arch = "wasm32"))]
mod browser {
    use super::*;
    use wasm_bindgen::prelude::*;
    /// Connected browser Workers transport.
    #[wasm_bindgen]
    pub struct WorkersClient {
        inner: Client,
    }
    #[wasm_bindgen]
    impl WorkersClient {
        /// Binding package identity for the maintained loader handshake.
        pub fn version() -> String {
            env!("CARGO_PKG_VERSION").to_owned()
        }
        /// Admit typed configuration and construct a gRPC-Web connection.
        pub async fn connect(
            config_value: JsValue,
            cancellation: &WorkersCancellation,
        ) -> Result<WorkersClient, JsValue> {
            let config = serde_wasm_bindgen::from_value::<WorkersClientOptions>(config_value)
                .map_err(|error| map_error(Failure::local("invalid_argument", error)))?;
            Client::connect(config, cancellation.token.clone())
                .await
                .map(|inner| Self { inner })
                .map_err(map_error)
        }
        /// Actual selected transport.
        #[wasm_bindgen(getter)]
        pub fn transport(&self) -> String {
            self.inner.transport().to_owned()
        }
        /// Call any of the seven generated unary methods with one cancellation handle.
        pub async fn call(
            &self,
            method: String,
            request: &[u8],
            options_value: JsValue,
            cancellation: &WorkersCancellation,
        ) -> Result<Vec<u8>, JsValue> {
            let options = serde_wasm_bindgen::from_value::<WorkersCallOptions>(options_value)
                .map_err(|error| map_error(Failure::local("invalid_argument", error)))?;
            self.inner
                .call(&method, request, options, cancellation.token.clone())
                .await
                .map_err(map_error)
        }
    }
    fn map_error(failure: Failure) -> JsValue {
        let populate = || -> Result<JsValue, JsValue> {
            let error = js_sys::Error::new(&failure.message);
            let metadata = serde_wasm_bindgen::to_value(&failure)
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            js_sys::Reflect::set(
                &error,
                &JsValue::from_str("code"),
                &JsValue::from_str(&failure.code),
            )?;
            js_sys::Reflect::set(&error, &JsValue::from_str("metadata"), &metadata)?;
            Ok(error.into())
        };
        populate().unwrap_or_else(|error| error)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "client_binding_tests.rs"]
mod tests;

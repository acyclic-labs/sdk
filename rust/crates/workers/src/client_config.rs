//! Rust-owned Workers binding configuration and diagnostics.
use tonic::metadata::{Ascii, MetadataValue};
use tonic::metadata::{MetadataKey, MetadataMap};

/// Unified native/browser default and absolute protobuf-message ceiling.
pub const MAX_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;

/// Requested ABI backend; automatic selection uses the actual compilation target.
#[derive(Debug, serde::Deserialize, ts_rs::TS)]
#[serde(rename_all = "lowercase")]
#[ts(export_to = "workers/WorkersTransportPreference.ts")]
pub enum WorkersTransportPreference {
    /// Select the available native or browser transport.
    Auto,
    /// Require the native ABI and system TLS client.
    Native,
    /// Require the WASM ABI and maintained gRPC-Web client.
    Wasm,
}

/// Source-defined JavaScript configuration, admitted by serde and Rust policy.
#[derive(Debug, serde::Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(
    rename_all = "camelCase",
    export_to = "workers/WorkersClientOptions.ts"
)]
pub struct WorkersClientOptions {
    /// HTTPS service origin and optional path prefix.
    pub endpoint: String,
    /// Sensitive bearer credential.
    pub token: String,
    /// Explicit backend requirement, or automatic selection when absent.
    #[ts(optional)]
    pub transport: Option<WorkersTransportPreference>,
    /// Positive message ceiling; defaults to 16 MiB and cannot exceed it.
    #[ts(optional)]
    pub maximum_message_bytes: Option<u32>,
    /// Positive whole-operation deadline, bounded to i32::MAX milliseconds.
    #[ts(optional)]
    pub deadline_millis: Option<u32>,
    /// Additional native PEM root, at most 64 KiB; browser trust is platform-owned.
    #[ts(optional)]
    pub ca_certificate: Option<String>,
    /// ASCII request metadata pairs; protocol/authentication fields are reserved.
    #[ts(optional)]
    pub metadata: Option<Vec<(String, String)>>,
}

/// Source-defined per-call overrides for deadlines and ASCII metadata.
#[derive(Debug, Default, serde::Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(rename_all = "camelCase", export_to = "workers/WorkersCallOptions.ts")]
pub struct WorkersCallOptions {
    /// Optional replacement for the configured operation deadline.
    #[ts(optional)]
    pub deadline_millis: Option<u32>,
    /// Additional caller metadata pairs.
    #[ts(optional)]
    pub metadata: Option<Vec<(String, String)>>,
}

/// Shared diagnostic exported from Rust for both byte bridges.
#[derive(Debug, thiserror::Error, serde::Serialize, ts_rs::TS)]
#[error("{message}")]
#[serde(rename_all = "camelCase")]
#[ts(
    rename = "WorkersFailure",
    rename_all = "camelCase",
    export_to = "workers/WorkersFailure.ts"
)]
pub struct Failure {
    /// Stable category.
    pub code: String,
    /// Original gRPC status or admission diagnostic.
    pub message: String,
    /// Numeric gRPC status.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub grpc_code: Option<i32>,
    /// Decoded service code, retaining future signed int32 values.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub service_code: Option<i32>,
    /// Decoded service message, separate from the original status message.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub service_message: Option<String>,
    /// Exact status detail bytes.
    #[serde(with = "serde_bytes")]
    #[ts(type = "Uint8Array")]
    pub raw_details: Vec<u8>,
}

impl Failure {
    /// Canonical cancellation diagnostics used by Rust dispatch and generated public facades.
    pub fn cancelled() -> Self {
        Self::local("cancelled", "Workers operation cancelled")
    }

    pub(crate) fn local(code: &str, message: impl std::fmt::Display) -> Self {
        Self {
            code: code.to_owned(),
            message: message.to_string(),
            grpc_code: None,
            service_code: None,
            service_message: None,
            raw_details: Vec::new(),
        }
    }
}

pub(crate) fn valid_endpoint(endpoint: &str) -> bool {
    url::Url::parse(endpoint).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

pub(crate) fn authorization(token: &str) -> Result<MetadataValue<Ascii>, ()> {
    if !crate::valid_token(token) {
        return Err(());
    }
    let mut value: MetadataValue<Ascii> = format!("Bearer {token}").parse().map_err(|_| ())?;
    value.set_sensitive(true);
    Ok(value)
}

pub(crate) struct Validated {
    pub endpoint: String,
    #[cfg(not(target_arch = "wasm32"))]
    pub token: String,
    #[cfg(target_arch = "wasm32")]
    pub authorization: MetadataValue<Ascii>,
    pub metadata: MetadataMap,
    pub maximum: usize,
    pub deadline: Option<u32>,
    #[cfg(not(target_arch = "wasm32"))]
    pub ca: Option<String>,
}

impl WorkersClientOptions {
    pub(crate) fn validate(self) -> Result<Validated, Failure> {
        let invalid = |message| Failure::local("invalid_argument", message);
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self.transport, Some(WorkersTransportPreference::Wasm)) {
            return Err(invalid("WASM transport requires the WASM ABI"));
        }
        #[cfg(target_arch = "wasm32")]
        if matches!(self.transport, Some(WorkersTransportPreference::Native)) {
            return Err(invalid("native transport requires the native ABI"));
        }
        if !valid_endpoint(&self.endpoint) {
            return Err(invalid(
                "endpoint must be HTTPS without credentials, query or fragment",
            ));
        }
        let authorization =
            authorization(&self.token).map_err(|()| invalid("invalid bearer credential"))?;
        #[cfg(not(target_arch = "wasm32"))]
        drop(authorization);
        let maximum = self.maximum_message_bytes.unwrap_or(MAX_MESSAGE_BYTES);
        if maximum == 0 || maximum > MAX_MESSAGE_BYTES {
            return Err(invalid("message ceiling must be within 1..=16 MiB"));
        }
        let deadline = validate_deadline(self.deadline_millis)?;
        if self
            .ca_certificate
            .as_ref()
            .is_some_and(|ca| ca.is_empty() || ca.len() > 64 * 1024)
        {
            return Err(invalid("invalid additional native CA certificate"));
        }
        #[cfg(target_arch = "wasm32")]
        if self.ca_certificate.is_some() {
            return Err(invalid(
                "browser trust cannot be configured by a caller-provided CA",
            ));
        }
        let metadata = append_metadata(MetadataMap::new(), self.metadata)?;
        Ok(Validated {
            endpoint: self.endpoint.trim_end_matches('/').to_owned(),
            #[cfg(not(target_arch = "wasm32"))]
            token: self.token,
            #[cfg(target_arch = "wasm32")]
            authorization,
            metadata,
            maximum: maximum as usize,
            deadline,
            #[cfg(not(target_arch = "wasm32"))]
            ca: self.ca_certificate,
        })
    }
}

pub(crate) fn validate_deadline(value: Option<u32>) -> Result<Option<u32>, Failure> {
    if value.is_some_and(|value| value == 0 || value > i32::MAX as u32) {
        return Err(Failure::local(
            "invalid_argument",
            "deadline must be within 1..=i32::MAX milliseconds",
        ));
    }
    Ok(value)
}

pub(crate) fn append_metadata(
    mut metadata: MetadataMap,
    entries: Option<Vec<(String, String)>>,
) -> Result<MetadataMap, Failure> {
    let invalid = |message| Failure::local("invalid_argument", message);
    for (name, value) in entries.unwrap_or_default() {
        let key: MetadataKey<Ascii> = name.parse().map_err(|_| invalid("invalid metadata key"))?;
        if key.as_str() != name
            || name.ends_with("-bin")
            || name.starts_with("grpc-")
            || matches!(
                name.as_str(),
                "authorization"
                    | "content-type"
                    | "content-length"
                    | "host"
                    | "user-agent"
                    | "te"
                    | "x-grpc-web"
            )
        {
            return Err(invalid("reserved or noncanonical metadata key"));
        }
        let value = value
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| invalid("invalid metadata value"))?;
        metadata.append(key, value);
    }
    Ok(metadata)
}

/// Export actual runtime DTO declarations and paths for the shared semantic registry.
pub fn export_client_typescript(
    path: impl AsRef<std::path::Path>,
) -> Result<Vec<(String, std::path::PathBuf)>, ts_rs::ExportError> {
    use ts_rs::TS;
    let config = ts_rs::Config::default()
        .with_out_dir(path)
        .with_import_extension(Some("js"));
    let mut exports = Vec::new();
    macro_rules! roots {
        ($($root:ty),+ $(,)?) => { $(
            <$root as TS>::export_all(&config)?;
            exports.push((<$root as TS>::ident(&config), <$root as TS>::output_path().ok_or(ts_rs::ExportError::CannotBeExported(std::any::type_name::<$root>()))?));
        )+ };
    }
    roots!(
        WorkersClientOptions,
        WorkersCallOptions,
        WorkersTransportPreference,
        Failure
    );
    Ok(exports)
}

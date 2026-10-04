//! Rust-owned authenticated HTTP/JSON Inference client.
//!
//! Routes, request message names, response message names, and stream shape
//! are read from the Inference descriptor. The module therefore contains no
//! second operation contract for the 14 public RPCs.

use crate::{MAXIMUM_HTTP_JSON_BYTES, MAXIMUM_MESSAGE_BYTES, http_codec, wire};
use futures::StreamExt;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use reqwest::{Client as Transport, Url};
use serde_json::Value;

/// HTTP configuration, transport, or canonical service failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid endpoint, credential, request, or descriptor route.
    #[error("invalid Inference HTTP client configuration or request")]
    InvalidArgument,
    /// Network failure.
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    /// Response exceeded the Rust-owned transport bound.
    #[error("Inference HTTP response exceeds configured bound")]
    ResponseTooLarge,
    /// Response was not valid canonical protobuf JSON.
    #[error("malformed Inference protobuf JSON response")]
    MalformedResponse,
    /// Service rejection with optional canonical detail.
    #[error("Inference HTTP service returned status {status}")]
    Service {
        /// HTTP status code.
        status: u16,
        /// Canonical Inference error detail when supplied.
        detail: Option<String>,
    },
}

/// Typed HTTP/JSON operations for the Inference customer contract.
#[derive(Clone)]
pub struct Client {
    transport: Transport,
    endpoint: Url,
    token: String,
    maximum: usize,
    descriptors: DescriptorPool,
}

impl Client {
    /// Construct a browser-safe or native HTTP client.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        Self::new_with_ca(endpoint, token, maximum_response_bytes, None)
    }

    /// Construct an HTTP client with an optional native trust anchor.
    ///
    /// Browser fetch cannot install a caller-provided CA, so browser callers
    /// should pass `None` and rely on the browser's configured trust store.
    pub fn new_with_ca(
        endpoint: &str,
        token: &str,
        maximum_response_bytes: usize,
        ca_pem: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let mut endpoint = Url::parse(endpoint).map_err(|_| Error::InvalidArgument)?;
        let loopback = matches!(endpoint.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || token.trim().is_empty()
            || token.contains(['\r', '\n'])
            || maximum_response_bytes == 0
        {
            return Err(Error::InvalidArgument);
        }
        reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Error::InvalidArgument)?;
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let builder = Transport::builder();
        #[cfg(not(target_arch = "wasm32"))]
        let builder = {
            let mut builder = builder;
            builder = builder
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(60));
            if let Some(ca_pem) = ca_pem {
                if ca_pem.is_empty() || ca_pem.len() > 64 * 1024 {
                    return Err(Error::InvalidArgument);
                }
                builder = builder.add_root_certificate(
                    reqwest::Certificate::from_pem(ca_pem).map_err(Error::Transport)?,
                );
            }
            builder
        };
        #[cfg(target_arch = "wasm32")]
        if ca_pem.is_some() {
            return Err(Error::InvalidArgument);
        }
        Ok(Self {
            transport: builder.build()?,
            endpoint,
            token: token.to_owned(),
            maximum: maximum_response_bytes.min(MAXIMUM_HTTP_JSON_BYTES),
            descriptors: DescriptorPool::decode(include_bytes!("../inference_descriptor.bin").as_slice())
                .map_err(|_| Error::MalformedResponse)?,
        })
    }

    fn route(&self, method_name: &str, input_name: &str) -> Result<(String, String), Error> {
        let route = http_codec::routes()
            .map_err(|_| Error::MalformedResponse)?
            .into_iter()
            .find(|route| {
                route.method.name() == method_name
                    && route.method.input().full_name() == input_name
            })
            .ok_or(Error::InvalidArgument)?;
        Ok((route.path, route.method.output().full_name().to_owned()))
    }

    async fn body(&self, response: reqwest::Response) -> Result<(u16, Vec<u8>), Error> {
        let status = response.status().as_u16();
        if response
            .content_length()
            .is_some_and(|length| length > self.maximum as u64)
        {
            return Err(Error::ResponseTooLarge);
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() > self.maximum.saturating_sub(body.len()) {
                return Err(Error::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, body))
    }

    fn decode<O: Message + Default>(&self, name: &str, bytes: &[u8]) -> Result<O, Error> {
        let descriptor = self
            .descriptors
            .get_message_by_name(name)
            .ok_or(Error::MalformedResponse)?;
        let mut json = serde_json::Deserializer::from_slice(bytes);
        let message = DynamicMessage::deserialize(descriptor, &mut json)
            .map_err(|_| Error::MalformedResponse)?;
        json.end().map_err(|_| Error::MalformedResponse)?;
        message.transcode_to().map_err(|_| Error::MalformedResponse)
    }

    async fn call<I: Message, O: Message + Default>(
        &self,
        method_name: &str,
        input_name: &str,
        request: &I,
    ) -> Result<O, Error> {
        let (route, output) = self.route(method_name, input_name)?;
        let descriptor = self
            .descriptors
            .get_message_by_name(input_name)
            .ok_or(Error::MalformedResponse)?;
        let request_bytes = request.encode_to_vec();
        if request_bytes.len() > MAXIMUM_MESSAGE_BYTES {
            return Err(Error::ResponseTooLarge);
        }
        let message = DynamicMessage::decode(descriptor, request_bytes.as_slice())
            .map_err(|_| Error::InvalidArgument)?;
        let body = serde_json::to_vec(&message).map_err(|_| Error::InvalidArgument)?;
        if body.len() > self.maximum {
            return Err(Error::ResponseTooLarge);
        }
        let response = self
            .transport
            .post(self.endpoint.join(&route).map_err(|_| Error::InvalidArgument)?)
            .timeout(std::time::Duration::from_secs(60))
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        let (status, bytes) = self.body(response).await?;
        if !(200..300).contains(&status) {
            let detail = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .map(|value| value.to_string());
            return Err(Error::Service { status, detail });
        }
        self.decode(output.as_str(), &bytes)
    }

    async fn stream(
        &self,
        request: &wire::WatchRunRequest,
    ) -> Result<Vec<wire::RunEvent>, Error> {
        let input = "inference.customer.v1.WatchRunRequest";
        let (route, output) = self.route("Watch", input)?;
        let descriptor = self
            .descriptors
            .get_message_by_name(input)
            .ok_or(Error::MalformedResponse)?;
        let request_bytes = request.encode_to_vec();
        if request_bytes.len() > MAXIMUM_MESSAGE_BYTES {
            return Err(Error::ResponseTooLarge);
        }
        let message = DynamicMessage::decode(descriptor, request_bytes.as_slice())
            .map_err(|_| Error::InvalidArgument)?;
        let body = serde_json::to_vec(&message).map_err(|_| Error::InvalidArgument)?;
        if body.len() > self.maximum {
            return Err(Error::ResponseTooLarge);
        }
        let response = self
            .transport
            .post(self.endpoint.join(&route).map_err(|_| Error::InvalidArgument)?)
            .timeout(std::time::Duration::from_secs(60))
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .header("accept", "application/x-ndjson, application/json")
            .body(body)
            .send()
            .await?;
        let (status, bytes) = self.body(response).await?;
        if !(200..300).contains(&status) {
            let detail = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .map(|value| value.to_string());
            return Err(Error::Service { status, detail });
        }
        let output_descriptor = self
            .descriptors
            .get_message_by_name(output.as_str())
            .ok_or(Error::MalformedResponse)?;
        let mut values = Vec::<Value>::new();
        if bytes.windows(1).any(|part| part == b"\n") {
            for line in bytes.split(|byte| *byte == b'\n') {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                if !line.is_empty() {
                    values.push(serde_json::from_slice(line).map_err(|_| Error::MalformedResponse)?);
                }
            }
        } else {
            let value: Value = serde_json::from_slice(&bytes).map_err(|_| Error::MalformedResponse)?;
            match value {
                Value::Array(items) => values = items,
                value => values.push(value),
            }
        }
        values
            .into_iter()
            .map(|value| {
                let bytes = serde_json::to_vec(&value).map_err(|_| Error::MalformedResponse)?;
                let mut json = serde_json::Deserializer::from_slice(&bytes);
                let dynamic = DynamicMessage::deserialize(output_descriptor.clone(), &mut json)
                    .map_err(|_| Error::MalformedResponse)?;
                json.end().map_err(|_| Error::MalformedResponse)?;
                dynamic.transcode_to().map_err(|_| Error::MalformedResponse)
            })
            .collect()
    }

    /// Models/List.
    pub async fn list(&self, request: &wire::ListModelsRequest) -> Result<wire::ListModelsResponse, Error> {
        self.call("List", "inference.customer.v1.ListModelsRequest", request).await
    }
    /// Contexts/Create.
    pub async fn create_context(&self, request: &wire::CreateContextRequest) -> Result<wire::MutationReceipt, Error> {
        self.call("Create", "inference.customer.v1.CreateContextRequest", request).await
    }
    /// Contexts/Inspect.
    pub async fn inspect_context(&self, request: &wire::InspectContextRequest) -> Result<wire::ContextView, Error> {
        self.call("Inspect", "inference.customer.v1.InspectContextRequest", request).await
    }
    /// Contexts/Mutate.
    pub async fn mutate_context(&self, request: &wire::MutateContextRequest) -> Result<wire::MutationReceipt, Error> {
        self.call("Mutate", "inference.customer.v1.MutateContextRequest", request).await
    }
    /// WarmContexts/Retain.
    pub async fn retain_warm(&self, request: &wire::RetainWarmRequest) -> Result<wire::WarmView, Error> {
        self.call("Retain", "inference.customer.v1.RetainWarmRequest", request).await
    }
    /// WarmContexts/Inspect.
    pub async fn inspect_warm(&self, request: &wire::InspectWarmRequest) -> Result<wire::WarmView, Error> {
        self.call("Inspect", "inference.customer.v1.InspectWarmRequest", request).await
    }
    /// WarmContexts/Renew.
    pub async fn renew_warm(&self, request: &wire::RenewWarmRequest) -> Result<wire::WarmView, Error> {
        self.call("Renew", "inference.customer.v1.RenewWarmRequest", request).await
    }
    /// WarmContexts/Release.
    pub async fn release_warm(&self, request: &wire::ReleaseWarmRequest) -> Result<wire::WarmView, Error> {
        self.call("Release", "inference.customer.v1.ReleaseWarmRequest", request).await
    }
    /// Runs/Generate.
    pub async fn generate_run(&self, request: &wire::GenerateRunRequest) -> Result<wire::GenerateRunResponse, Error> {
        self.call("Generate", "inference.customer.v1.GenerateRunRequest", request).await
    }
    /// Runs/Inspect.
    pub async fn inspect_run(&self, request: &wire::InspectRunRequest) -> Result<wire::RunView, Error> {
        self.call("Inspect", "inference.customer.v1.InspectRunRequest", request).await
    }
    /// Runs/Watch, returned as an ordered finite stream.
    pub async fn watch_run(&self, request: &wire::WatchRunRequest) -> Result<Vec<wire::RunEvent>, Error> {
        self.stream(request).await
    }
    /// Runs/Cancel.
    pub async fn cancel_run(&self, request: &wire::InspectRunRequest) -> Result<wire::RunView, Error> {
        self.call("Cancel", "inference.customer.v1.InspectRunRequest", request).await
    }
    /// Evaluations/Create.
    pub async fn create_evaluation(&self, request: &wire::CreateEvaluationRequest) -> Result<wire::EvaluationView, Error> {
        self.call("Create", "inference.customer.v1.CreateEvaluationRequest", request).await
    }
    /// Evaluations/Inspect.
    pub async fn inspect_evaluation(&self, request: &wire::InspectEvaluationRequest) -> Result<wire::EvaluationView, Error> {
        self.call("Inspect", "inference.customer.v1.InspectEvaluationRequest", request).await
    }
}

#[cfg(test)]
mod tests {
    use super::Client;

    #[test]
    fn descriptor_routes_disambiguate_repeated_method_names() {
        let client = Client::new("http://127.0.0.1:7878", "fixture-token", 4096).unwrap();
        let (contexts, _) = client
            .route("Inspect", "inference.customer.v1.InspectContextRequest")
            .unwrap();
        let (runs, _) = client
            .route("Inspect", "inference.customer.v1.InspectRunRequest")
            .unwrap();
        let (evaluations, _) = client
            .route("Inspect", "inference.customer.v1.InspectEvaluationRequest")
            .unwrap();
        assert_ne!(contexts, runs);
        assert_ne!(runs, evaluations);
        assert_eq!(super::http_codec::routes().unwrap().len(), 14);
    }
}

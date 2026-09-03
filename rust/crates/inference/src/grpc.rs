//! Authenticated gRPC adapter for managed and customer-hosted Inference services.

use crate::{
    ContextEdit, ContextMutation, ContextSnapshot, Error, GenerateRequest, InferenceProvider, Item,
    ItemKind, ModelCapabilities, MutateContextRequest, Provenance, Result, Retention, RunEvent,
    RunEventKind, RunResult, RunSnapshot, RunTerminal, Usage, UsageReceipt, wire,
};
use async_trait::async_trait;
use std::collections::BTreeSet;
use tonic::{
    Code, Request,
    metadata::{Ascii, MetadataValue},
    transport::{Channel, Endpoint},
};

use wire::acyclic::inference::v1 as protocol;

/// Authenticated network provider implementing the same logical surface as local engines.
#[derive(Clone)]
pub struct ManagedInference {
    channel: Channel,
    authorization: MetadataValue<Ascii>,
}

impl ManagedInference {
    /// Connects over TLS and binds an account credential to every request.
    pub async fn connect(endpoint: impl AsRef<str>, bearer_token: impl AsRef<str>) -> Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint.as_ref().to_owned())
            .map_err(|error| Error::Invalid(format!("invalid Inference endpoint: {error}")))?;
        if endpoint.uri().scheme_str() != Some("https") {
            return Err(Error::Invalid(
                "customer Inference endpoints must use https".to_owned(),
            ));
        }
        let authorization = format!("Bearer {}", bearer_token.as_ref())
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| Error::Invalid("invalid Inference bearer credential".to_owned()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| Error::Invalid(format!("Inference connection failed: {error}")))?;
        Ok(Self {
            channel,
            authorization,
        })
    }

    fn client(&self) -> protocol::inference_service_client::InferenceServiceClient<Channel> {
        protocol::inference_service_client::InferenceServiceClient::new(self.channel.clone())
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
    }
}

fn rpc(error: tonic::Status) -> Error {
    match error.code() {
        Code::NotFound => Error::NotFound(error.message().to_owned()),
        Code::AlreadyExists | Code::Aborted | Code::FailedPrecondition => {
            Error::Conflict(error.message().to_owned())
        }
        Code::Unimplemented => Error::Unsupported(error.message().to_owned()),
        _ => Error::Invalid(format!("Inference request failed: {error}")),
    }
}

fn operation(request_id: String) -> protocol::OperationIdentity {
    protocol::OperationIdentity {
        operation_id: request_id.clone(),
        idempotency_key: request_id,
    }
}

fn context_ref(id: String) -> protocol::ContextRef {
    protocol::ContextRef { context_id: id }
}

fn encode_item(item: Item) -> protocol::Item {
    protocol::Item {
        item_id: item.id,
        kind: match item.kind {
            ItemKind::Instruction => protocol::ItemKind::Instruction,
            ItemKind::System => protocol::ItemKind::System,
            ItemKind::Developer => protocol::ItemKind::Developer,
            ItemKind::User => protocol::ItemKind::User,
            ItemKind::Assistant => protocol::ItemKind::Assistant,
            ItemKind::ToolDefinition => protocol::ItemKind::ToolDefinition,
            ItemKind::ToolCall => protocol::ItemKind::ToolCall,
            ItemKind::ToolResult => protocol::ItemKind::ToolResult,
            ItemKind::Image => protocol::ItemKind::Image,
            ItemKind::Audio => protocol::ItemKind::Audio,
            ItemKind::File => protocol::ItemKind::File,
            ItemKind::Continuation => protocol::ItemKind::Continuation,
        }
        .into(),
        content: item.content,
        link: item.link,
        continuation_profile: item.continuation_profile,
    }
}

fn decode_item(item: protocol::Item) -> Result<Item> {
    let kind = match protocol::ItemKind::try_from(item.kind)
        .map_err(|_| Error::Invalid("unknown Context item kind".to_owned()))?
    {
        protocol::ItemKind::Unspecified => {
            return Err(Error::Invalid("unspecified Context item kind".to_owned()));
        }
        protocol::ItemKind::Instruction => ItemKind::Instruction,
        protocol::ItemKind::System => ItemKind::System,
        protocol::ItemKind::Developer => ItemKind::Developer,
        protocol::ItemKind::User => ItemKind::User,
        protocol::ItemKind::Assistant => ItemKind::Assistant,
        protocol::ItemKind::ToolDefinition => ItemKind::ToolDefinition,
        protocol::ItemKind::ToolCall => ItemKind::ToolCall,
        protocol::ItemKind::ToolResult => ItemKind::ToolResult,
        protocol::ItemKind::Image => ItemKind::Image,
        protocol::ItemKind::Audio => ItemKind::Audio,
        protocol::ItemKind::File => ItemKind::File,
        protocol::ItemKind::Continuation => ItemKind::Continuation,
    };
    Ok(Item {
        id: item.item_id,
        kind,
        content: item.content,
        link: item.link,
        continuation_profile: item.continuation_profile,
    })
}

fn encode_retention(retention: Retention) -> protocol::Retention {
    match retention {
        Retention::Durable => protocol::Retention {
            kind: protocol::RetentionKind::Durable.into(),
            expires_at_unix_ms: None,
        },
        Retention::WarmUntil(expires_at_unix_ms) => protocol::Retention {
            kind: protocol::RetentionKind::WarmUntil.into(),
            expires_at_unix_ms: Some(expires_at_unix_ms),
        },
    }
}

fn decode_retention(retention: Option<protocol::Retention>) -> Result<Retention> {
    let retention = retention.ok_or_else(|| Error::Invalid("missing retention".to_owned()))?;
    match protocol::RetentionKind::try_from(retention.kind)
        .map_err(|_| Error::Invalid("unknown retention kind".to_owned()))?
    {
        protocol::RetentionKind::Durable => Ok(Retention::Durable),
        protocol::RetentionKind::WarmUntil => retention
            .expires_at_unix_ms
            .map(Retention::WarmUntil)
            .ok_or_else(|| Error::Invalid("warm retention has no expiry".to_owned())),
        protocol::RetentionKind::Unspecified => {
            Err(Error::Invalid("unspecified retention kind".to_owned()))
        }
    }
}

fn decode_context(context: protocol::Context) -> Result<ContextSnapshot> {
    let id = context
        .context
        .ok_or_else(|| Error::Invalid("missing Context identity".to_owned()))?
        .context_id;
    let provenance = context
        .provenance
        .ok_or_else(|| Error::Invalid("missing Context provenance".to_owned()))?;
    let source = provenance.source.map(|value| value.context_id);
    let provenance = match protocol::ContextProvenanceKind::try_from(provenance.kind)
        .map_err(|_| Error::Invalid("unknown Context provenance".to_owned()))?
    {
        protocol::ContextProvenanceKind::Created => Provenance::Created,
        protocol::ContextProvenanceKind::Derived => Provenance::Derived,
        protocol::ContextProvenanceKind::Forked => Provenance::Forked {
            source: source.ok_or_else(|| Error::Invalid("fork source missing".to_owned()))?,
        },
        protocol::ContextProvenanceKind::Transferred => Provenance::Transferred {
            source: source.ok_or_else(|| Error::Invalid("transfer source missing".to_owned()))?,
            reused_compatible_state: provenance.reused_compatible_state,
        },
        protocol::ContextProvenanceKind::Generated => Provenance::Generated {
            run: provenance
                .run_id
                .ok_or_else(|| Error::Invalid("generation Run missing".to_owned()))?,
        },
        protocol::ContextProvenanceKind::Unspecified => {
            return Err(Error::Invalid("unspecified Context provenance".to_owned()));
        }
    };
    let items = context
        .items
        .into_iter()
        .map(decode_item)
        .collect::<Result<Vec<_>>>()?;
    Ok(ContextSnapshot {
        id,
        lineage: context.lineage_id,
        parent: context.parent.map(|value| value.context_id),
        model: context.model,
        items: items.into(),
        retention: decode_retention(context.retention)?,
        provenance,
    })
}

fn decode_usage(usage: Option<protocol::Usage>) -> Result<Usage> {
    let usage = usage.ok_or_else(|| Error::Invalid("missing usage".to_owned()))?;
    Ok(Usage {
        new_prefill: usage.new_prefill,
        generated_output: usage.generated_output,
        effective_context_reads: usage.effective_context_reads,
        retained_byte_millis: usage.retained_byte_millis,
    })
}

fn decode_terminal(value: i32) -> Result<RunTerminal> {
    match protocol::RunTerminal::try_from(value)
        .map_err(|_| Error::Invalid("unknown Run terminal".to_owned()))?
    {
        protocol::RunTerminal::Completed => Ok(RunTerminal::Completed),
        protocol::RunTerminal::OutputLimited => Ok(RunTerminal::OutputLimited),
        protocol::RunTerminal::ToolCall => Ok(RunTerminal::ToolCall),
        protocol::RunTerminal::Refusal => Ok(RunTerminal::Refusal),
        protocol::RunTerminal::Cancelled => Ok(RunTerminal::Cancelled),
        protocol::RunTerminal::Failed => Ok(RunTerminal::Failed),
        protocol::RunTerminal::Indeterminate => Ok(RunTerminal::Indeterminate),
        protocol::RunTerminal::Unspecified => {
            Err(Error::Invalid("unspecified Run terminal".to_owned()))
        }
    }
}

fn decode_event(event: protocol::RunEvent) -> Result<RunEvent> {
    let kind = match event
        .event
        .ok_or_else(|| Error::Invalid("missing Run event".to_owned()))?
    {
        protocol::run_event::Event::Output(output) => RunEventKind::Output(output),
        protocol::run_event::Event::Usage(usage) => RunEventKind::Usage(decode_usage(Some(usage))?),
        protocol::run_event::Event::Terminal(terminal) => {
            RunEventKind::Terminal(decode_terminal(terminal)?)
        }
    };
    Ok(RunEvent {
        sequence: event.sequence,
        kind,
    })
}

fn decode_run(run: protocol::Run) -> Result<RunSnapshot> {
    let id = run
        .run
        .ok_or_else(|| Error::Invalid("missing Run identity".to_owned()))?
        .run_id;
    let input = run
        .input
        .ok_or_else(|| Error::Invalid("missing Run input".to_owned()))?
        .context_id;
    let events = run
        .events
        .into_iter()
        .map(decode_event)
        .collect::<Result<Vec<_>>>()?;
    let result = run
        .result
        .map(|result| {
            let receipt = result
                .receipt
                .ok_or_else(|| Error::Invalid("missing usage receipt".to_owned()))?;
            Ok(RunResult {
                output: result.output,
                context: result.context.map(decode_context).transpose()?,
                terminal: decode_terminal(result.terminal)?,
                receipt: UsageReceipt {
                    id: receipt.receipt_id,
                    model: receipt.model,
                    meter_revision: receipt.meter_revision,
                    usage: decode_usage(receipt.usage)?,
                },
            })
        })
        .transpose()?;
    Ok(RunSnapshot {
        id,
        input,
        events: events.into(),
        result,
    })
}

fn admitted(admission: Option<protocol::Admission>) -> Result<()> {
    let admission = admission.ok_or_else(|| Error::Invalid("missing admission".to_owned()))?;
    match protocol::AdmissionState::try_from(admission.state)
        .map_err(|_| Error::Invalid("unknown admission state".to_owned()))?
    {
        protocol::AdmissionState::Accepted => Ok(()),
        protocol::AdmissionState::Rejected => {
            let message = admission
                .error
                .map(|error| error.message)
                .unwrap_or_else(|| "request rejected".to_owned());
            Err(Error::Invalid(message))
        }
        protocol::AdmissionState::Indeterminate => Err(Error::Conflict(
            "admission is indeterminate; reconcile the same request identity".to_owned(),
        )),
        protocol::AdmissionState::Unspecified => {
            Err(Error::Invalid("unspecified admission state".to_owned()))
        }
    }
}

fn encode_edit(edit: ContextEdit) -> protocol::ContextEdit {
    let action = match edit {
        ContextEdit::Append(item) => protocol::context_edit::Action::Append(encode_item(item)),
        ContextEdit::InsertBefore { target, item } => {
            protocol::context_edit::Action::InsertBefore(protocol::Insert {
                target_item_id: target,
                item: Some(encode_item(item)),
            })
        }
        ContextEdit::InsertAfter { target, item } => {
            protocol::context_edit::Action::InsertAfter(protocol::Insert {
                target_item_id: target,
                item: Some(encode_item(item)),
            })
        }
        ContextEdit::Replace { target, content } => {
            protocol::context_edit::Action::Replace(protocol::Replace {
                target_item_id: target,
                content,
            })
        }
        ContextEdit::Delete { target } => protocol::context_edit::Action::DeleteItemId(target),
    };
    protocol::ContextEdit {
        action: Some(action),
    }
}

#[async_trait]
impl InferenceProvider for ManagedInference {
    async fn models(&self) -> Result<Vec<ModelCapabilities>> {
        let response = self
            .client()
            .list_models(self.request(protocol::ListModelsRequest {}))
            .await
            .map_err(rpc)?
            .into_inner();
        Ok(response
            .models
            .into_iter()
            .map(|model| ModelCapabilities {
                model: model.model,
                maximum_context_bytes: model.maximum_context_bytes,
                maximum_output: model.maximum_output,
                features: model.features.into_iter().collect::<BTreeSet<_>>(),
            })
            .collect())
    }

    async fn create_context(
        &self,
        request: crate::CreateContextRequest,
    ) -> Result<ContextSnapshot> {
        let response = self
            .client()
            .create_context(self.request(protocol::CreateContextRequest {
                operation: Some(operation(request.request_id)),
                model: request.model,
                items: request.items.into_iter().map(encode_item).collect(),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        admitted(response.admission)?;
        decode_context(
            response
                .context
                .ok_or_else(|| Error::Invalid("missing created Context".to_owned()))?,
        )
    }

    async fn inspect_context(&self, id: &str) -> Result<ContextSnapshot> {
        let response = self
            .client()
            .inspect_context(self.request(protocol::InspectContextRequest {
                context: Some(context_ref(id.to_owned())),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        decode_context(response)
    }

    async fn mutate_context(&self, request: MutateContextRequest) -> Result<ContextSnapshot> {
        let mutation = match request.mutation {
            ContextMutation::Fork => protocol::mutate_context_request::Mutation::Fork(true),
            ContextMutation::Edit(edits) => {
                protocol::mutate_context_request::Mutation::Edit(protocol::EditContext {
                    edits: edits.into_iter().map(encode_edit).collect(),
                })
            }
            ContextMutation::Truncate(through_item_id) => {
                protocol::mutate_context_request::Mutation::Truncate(protocol::TruncateContext {
                    through_item_id,
                })
            }
            ContextMutation::Compact {
                selected,
                replacement,
            } => protocol::mutate_context_request::Mutation::Compact(protocol::CompactContext {
                selected_item_ids: selected,
                replacement: replacement.into_iter().map(encode_item).collect(),
            }),
            ContextMutation::Transfer { model } => {
                protocol::mutate_context_request::Mutation::Transfer(protocol::TransferContext {
                    model,
                })
            }
        };
        let response = self
            .client()
            .mutate_context(self.request(protocol::MutateContextRequest {
                operation: Some(operation(request.request_id)),
                source: Some(context_ref(request.source)),
                mutation: Some(mutation),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        admitted(response.admission)?;
        decode_context(
            response
                .context
                .ok_or_else(|| Error::Invalid("missing mutated Context".to_owned()))?,
        )
    }

    async fn retain_context(&self, id: &str, retention: Retention) -> Result<ContextSnapshot> {
        let response = self
            .client()
            .retain_context(self.request(protocol::RetainContextRequest {
                operation: Some(operation(crate::request_id())),
                context: Some(context_ref(id.to_owned())),
                retention: Some(encode_retention(retention)),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        admitted(response.admission)?;
        decode_context(
            response
                .context
                .ok_or_else(|| Error::Invalid("missing retained Context".to_owned()))?,
        )
    }

    async fn delete_context(&self, id: &str) -> Result<bool> {
        let response = self
            .client()
            .delete_context(self.request(protocol::DeleteContextRequest {
                operation: Some(operation(crate::request_id())),
                context: Some(context_ref(id.to_owned())),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        admitted(response.admission)?;
        Ok(response.deleted)
    }

    async fn generate(&self, request: GenerateRequest) -> Result<RunSnapshot> {
        let response = self
            .client()
            .generate(self.request(protocol::GenerateRequest {
                operation: Some(operation(request.request_id)),
                context: Some(context_ref(request.context)),
                input: Some(encode_item(request.input)),
                settings: Some(protocol::GenerationSettings {
                    maximum_output: request.settings.maximum_output,
                    seed: request.settings.seed,
                }),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        admitted(response.admission)?;
        decode_run(
            response
                .run
                .ok_or_else(|| Error::Invalid("missing admitted Run".to_owned()))?,
        )
    }

    async fn inspect_run(&self, id: &str) -> Result<RunSnapshot> {
        let response = self
            .client()
            .inspect_run(self.request(protocol::InspectRunRequest {
                run: Some(protocol::RunRef {
                    run_id: id.to_owned(),
                }),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        decode_run(response)
    }

    async fn run_events(&self, id: &str, from_sequence: u64) -> Result<Vec<RunEvent>> {
        let mut stream = self
            .client()
            .watch_run(self.request(protocol::WatchRunRequest {
                run: Some(protocol::RunRef {
                    run_id: id.to_owned(),
                }),
                from_sequence,
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        let mut events = Vec::new();
        while let Some(event) = stream.message().await.map_err(rpc)? {
            events.push(decode_event(event)?);
        }
        Ok(events)
    }

    async fn cancel_run(&self, id: &str) -> Result<RunSnapshot> {
        let response = self
            .client()
            .cancel_run(self.request(protocol::InspectRunRequest {
                run: Some(protocol::RunRef {
                    run_id: id.to_owned(),
                }),
            }))
            .await
            .map_err(rpc)?
            .into_inner();
        decode_run(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn insecure_endpoints_are_rejected_before_connection() {
        let result = ManagedInference::connect("http://127.0.0.1:1", "token").await;
        assert!(matches!(result, Err(Error::Invalid(_))));
    }

    #[test]
    fn byte_payloads_cross_the_wire_without_copying() {
        let bytes = Bytes::from_static(b"shared");
        let encoded = encode_item(Item {
            id: "item".to_owned(),
            kind: ItemKind::User,
            content: bytes.clone(),
            link: None,
            continuation_profile: None,
        });
        assert_eq!(bytes.as_ptr(), encoded.content.as_ptr());
        let decoded = decode_item(encoded).unwrap_or_else(|error| std::unreachable!("{error}"));
        assert_eq!(bytes.as_ptr(), decoded.content.as_ptr());
    }
}

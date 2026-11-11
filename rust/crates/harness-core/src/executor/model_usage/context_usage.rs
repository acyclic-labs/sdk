//! Bounded accounting projection of the versioned context record. The request
//! header precedes the projection and can contain arbitrarily large tool schemas.
use super::*;
use crate::executor::json::{JsonBounds, JsonSpans, SpanEvent, SpanKind};

// Selected keys and scalar values fit the decimal width of a u64. Overlap
// pages by this width so a selected token can straddle a read boundary.
const TOKEN_BYTES: u64 = 20;

pub(super) async fn read(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
) -> Result<ContextUsage> {
    const PAGE_BYTES: u64 = 4096;
    let length = reference.descriptor().byte_length();
    let mut header = Header::default();
    let mut offset = 0u64;
    while offset < length {
        let origin = offset.saturating_sub(TOKEN_BYTES);
        let bytes = journal
            .load_range(
                reference,
                acyclic_fs::ByteRange {
                    offset: origin,
                    length: (length - origin).min(PAGE_BYTES + offset - origin),
                },
                None,
            )?
            .await?;
        if bytes.len() as u64 <= offset - origin {
            return Err(malformed());
        }
        if let Some(usage) = header.feed(&bytes, origin, offset, length)? {
            return Ok(usage);
        }
        offset = origin + bytes.len() as u64;
    }
    Err(malformed())
}

fn malformed() -> Error {
    Error::Storage("canonical context accounting header is malformed".into())
}

#[derive(Default)]
struct Header {
    cursor: JsonSpans,
    fields: Fields,
}

#[derive(Default)]
struct Fields {
    projection: bool,
    accounting: bool,
    capacity_section: Option<bool>,
    input: bool,
    capacity: Option<u32>,
    result: Option<ContextUsage>,
}

impl Header {
    fn feed(
        &mut self,
        bytes: &[u8],
        origin: u64,
        offset: u64,
        length: u64,
    ) -> Result<Option<ContextUsage>> {
        let fields = &mut self.fields;
        self.cursor.feed(
            &bytes[(offset - origin) as usize..],
            JsonBounds {
                maximum_bytes: Some(length),
                maximum_items: Some(length),
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            None,
            &mut |_| Ok(()),
            &mut |event| {
                let token = |start: u64, end: u64| {
                    if end - start > TOKEN_BYTES || start < origin {
                        None
                    } else {
                        bytes.get((start - origin) as usize..(end - origin) as usize)
                    }
                };
                match event {
                    SpanEvent::Key {
                        depth: 1,
                        start,
                        end,
                    } => {
                        fields.projection = token(start, end) == Some(br#""projection""#);
                    }
                    SpanEvent::Key {
                        depth: 2,
                        start,
                        end,
                    } if fields.projection => {
                        fields.accounting = token(start, end) == Some(br#""accounting""#);
                    }
                    SpanEvent::Key {
                        depth: 3,
                        start,
                        end,
                    } if fields.projection && fields.accounting => {
                        fields.capacity_section = match token(start, end) {
                            Some(b"\"capacity\"") => Some(true),
                            Some(b"\"count\"") => Some(false),
                            _ => None,
                        };
                    }
                    SpanEvent::Key {
                        depth: 4,
                        start,
                        end,
                    } if fields.projection && fields.accounting => {
                        fields.input = match fields.capacity_section {
                            Some(true) => token(start, end) == Some(br#""context_tokens""#),
                            Some(false) => token(start, end) == Some(br#""input_tokens""#),
                            None => false,
                        };
                    }
                    SpanEvent::End {
                        depth: 2,
                        kind: SpanKind::Scalar,
                        start,
                        end,
                    } if fields.projection && fields.accounting => {
                        if token(start, end) == Some(b"null") {
                            fields.result = Some(ContextUsage::Unavailable);
                            return Ok(true);
                        }
                    }
                    SpanEvent::End {
                        depth: 3,
                        kind: SpanKind::Scalar,
                        start,
                        end,
                    } if fields.projection
                        && fields.accounting
                        && fields.capacity_section == Some(false)
                        && token(start, end) == Some(b"null") =>
                    {
                        fields.result = Some(ContextUsage::Unavailable);
                        return Ok(true);
                    }
                    SpanEvent::End {
                        depth: 4,
                        kind: SpanKind::Scalar,
                        start,
                        end,
                    } if fields.projection && fields.accounting && fields.input => {
                        let scalar = token(start, end).ok_or_else(malformed)?;
                        if fields.capacity_section == Some(true) {
                            fields.capacity = Some(
                                crate::contract::json_from_slice::<u32>(scalar)
                                    .map_err(|error| Error::Invalid(error.to_string()))?,
                            );
                        } else if let Some(capacity_tokens) = fields.capacity.filter(|n| *n != 0) {
                            fields.result = Some(ContextUsage::Counted {
                                input_tokens: crate::contract::json_from_slice(scalar)
                                    .map_err(|error| Error::Invalid(error.to_string()))?,
                                capacity_tokens,
                            });
                            return Ok(true);
                        }
                    }
                    _ => {}
                }
                Ok(false)
            },
        )?;
        Ok(fields.result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn usage_reads_the_original_projection_with_deep_tool_schemas() -> Result<()> {
        use crate::{
            context::{Context, ModelContextCapacity, ModelTokenCount},
            executor::{ContextAccounting, ResponseProjection, transcript},
            model::{
                Model, ModelContent, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest,
            },
        };
        let storage =
            crate::filesystem::MemoryHarnessStorage::new(crate::AgentId::new(), 131_072).await?;
        let journal = storage.journal();
        let maximum_depth = crate::executor::json::RECORD_MAXIMUM_DEPTH;
        // Record, header, tool list, tool, schema and properties precede the
        // nested schema's zero-based depth in the production projection.
        let boundary_depth = maximum_depth - 6;
        for depth in [160, boundary_depth, maximum_depth] {
            let mut schema = serde_json::json!({});
            for _ in 0..depth {
                schema = serde_json::json!({"items": schema});
            }
            let mut tool = crate::tools::catalog()
                .find(|tool| tool.name == "read")
                .expect("stock read capability")
                .definition()?;
            tool.input_schema = serde_json::json!({
                "type": "object", "properties": {"nested": schema},
            });
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
            let io = PreparationIo::from_budget(budget, &cancellation)?;
            let prepared = PreparedModelRequest::prepare(
                ModelRequest {
                    summary: false,
                    compose_tools: false,
                    model: Model::new("test".into(), "deep-schema".into(), "1".into())?,
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("input".into()),
                    }],
                    tools: vec![tool],
                    sampling: Default::default(),
                    max_output_tokens: None,
                },
                Limits::default(),
                Some(&io),
            );
            if depth == maximum_depth {
                assert!(
                    prepared.is_err(),
                    "the producer still refuses excessive nesting"
                );
                continue;
            }
            let prepared = prepared?;
            let context = Context {
                messages: prepared.request().messages.clone(),
                metadata: Default::default(),
                current_input_index: Some(0),
            };
            let accounting = ContextAccounting {
                capacity: ModelContextCapacity {
                    context_tokens: 65_536,
                    output_tokens: None,
                },
                count: Some(ModelTokenCount::from_upper_bounds(
                    prepared.request_digest(),
                    7,
                    vec![1],
                )),
            };
            let reference = transcript::stage_projection(
                journal.as_ref(),
                crate::OperationId::new(),
                "deep-projection",
                &ResponseProjection {
                    context,
                    steering: None,
                    canonical: None,
                    checkpoint: None,
                    working: None,
                    accounting: None,
                },
                &prepared,
                Some(&accounting),
                Some(&io),
            )
            .await?;
            assert_eq!(
                read(journal.as_ref(), &reference).await?,
                ContextUsage::Counted {
                    input_tokens: 8,
                    capacity_tokens: 65_536,
                }
            );
            // The same original record remains recoverable through the saved
            // projection reader, rather than only through the usage projection.
            let (retained, _) = transcript::load_projection(
                journal.as_ref(),
                &reference,
                Limits::default(),
                Some(&io),
            )
            .await?;
            assert_eq!(retained.context.messages, prepared.request().messages);
        }
        Ok(())
    }

    proptest! {
        #[test]
        fn accounting_header_is_independent_of_the_unread_context(
            input_tokens in any::<u64>(), capacity_tokens in 1..=u32::MAX,
            header_bytes in 0..8192usize, page in 1..128usize, accounting_mode in 0u8..3,
        ) {
            let accounting = (accounting_mode != 0).then(|| serde_json::json!({
                "capacity": {"context_tokens": capacity_tokens, "output_tokens": null},
                "count": (accounting_mode == 1).then(|| serde_json::json!({"fixed_tokens": 0, "input_tokens": input_tokens, "message_tokens": []})),
            }));
            let record = serde_json::json!({
                "$version": 1,
                "header": {"schema": "x".repeat(header_bytes)},
                "projection": {"accounting": accounting, "context": "unread".repeat(8192)},
            });
            let bytes = crate::contract::canonical_json_bytes(&record).unwrap();
            let mut header = Header::default();
            let mut result = None;
            let mut offset = 0usize;
            while offset < bytes.len() && result.is_none() {
                let origin = offset.saturating_sub(TOKEN_BYTES as usize);
                let end = bytes.len().min(offset + page);
                result = header.feed(&bytes[origin..end], origin as u64, offset as u64, bytes.len() as u64).unwrap();
                offset = end;
            }
            prop_assert_eq!(result, Some(if accounting_mode == 1 {
                ContextUsage::Counted { input_tokens, capacity_tokens }
            } else {
                ContextUsage::Unavailable
            }));
            prop_assert!(offset < bytes.len(), "context payload was read");
        }
    }
}

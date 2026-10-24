use super::*;

/// Provider-independent maximum, including the omitted-bytes marker.
pub(crate) const TOOL_OUTPUT_PREVIEW_BYTES: usize = 16 * 1024;
const OMITTED: &str = "\n[full output retained in file]\n";

#[cfg(test)]
fn preview_bounds(text: &str) -> (usize, usize) {
    preview_bounds_with_limit(text, TOOL_OUTPUT_PREVIEW_BYTES)
}

fn preview_bounds_with_limit(text: &str, maximum: usize) -> (usize, usize) {
    let half = (maximum - OMITTED.len()) / 2;
    let mut head = half.min(text.len());
    while !text.is_char_boundary(head) {
        head -= 1;
    }
    let mut tail = text.len().saturating_sub(half);
    while !text.is_char_boundary(tail) {
        tail += 1;
    }
    (head, tail)
}

fn preview_parts(text: &str, maximum: usize) -> Result<(&str, &str)> {
    let (head, tail) = preview_bounds_with_limit(text, maximum);
    text.get(..head)
        .zip(text.get(tail..))
        .ok_or_else(|| Error::Invalid("invalid UTF-8 preview boundary".into()))
}

impl StockExecution<'_> {
    pub(super) fn demote_stored_tool_outputs(
        &self,
        context: &mut crate::context::Context,
    ) -> Result<()> {
        let mut rank = 0usize;
        for message in context.messages.iter_mut().rev() {
            let parts = match &mut message.content {
                ModelContent::Text(_) => continue,
                ModelContent::Part(part) => std::slice::from_mut(part),
                ModelContent::Parts(parts) => parts.as_mut_slice(),
            };
            for part in parts.iter_mut().rev() {
                if let crate::model::ModelContentPart::ToolResult {
                    content: crate::model::ToolResultContent::Stored { preview, .. },
                    ..
                } = part
                {
                    let limit = match rank {
                        0 => TOOL_OUTPUT_PREVIEW_BYTES,
                        1 => 2048,
                        _ => 0,
                    };
                    rank = rank.saturating_add(1);
                    if preview.len() <= limit {
                        continue;
                    }
                    let replacement = if limit == 0 {
                        String::new()
                    } else {
                        let (head, tail) = preview_parts(preview, limit)?;
                        with_history_io(self.io, |work| {
                            preparation_format(format_args!("{head}{OMITTED}{tail}"), work)
                        })?
                    };
                    let retired = std::mem::replace(preview, replacement);
                    #[cfg(feature = "filesystem")]
                    if let Some(io) = self.io {
                        io.release(retired.capacity() as u64)?;
                    }
                    drop(retired);
                }
            }
        }
        Ok(())
    }

    pub(super) async fn bounded_tool_content(
        &self,
        operation: OperationId,
        step: u32,
        call_id: &str,
        projection: &Value,
        content: crate::model::ToolResultContent,
    ) -> Result<crate::model::ToolResultContent> {
        let mut expanded = stock_json_length(projection, self.stock.limits.render_bytes, self.io)?;
        if let crate::model::ToolResultContent::Parts { parts } = &content {
            for part in parts {
                if let crate::model::ModelDataPart::File { file, policy } = part
                    && !matches!(policy, crate::model::FileProjectionPolicy::Reference)
                {
                    expanded = expanded.saturating_add(file.descriptor().byte_length());
                }
            }
        }
        if expanded <= TOOL_OUTPUT_PREVIEW_BYTES as u64 {
            return Ok(content);
        }
        {
            let io = self.io;
            let context = self.original_context().ok_or_else(|| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Unsupported,
                    "large tool output requires the task's private volume",
                )
            })?;
            let destination = context.private_summaries(io)?;
            stock_json_bound(projection, self.stock.limits.file_bytes, io)?;
            let bytes: acyclic_stream::OwnedBytes = match io {
                #[cfg(feature = "filesystem")]
                Some(io) => io.encode(projection, self.stock.limits.file_bytes, false)?,
                _ => crate::contract::canonical_json_bytes(projection)?.into(),
            };
            #[cfg(feature = "filesystem")]
            let _bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Storage,
                    "tool projection encoding is not UTF-8",
                )
            })?;
            let (head, tail) = preview_parts(text, TOOL_OUTPUT_PREVIEW_BYTES)?;
            let preview = with_history_io(self.io, |work| {
                preparation_format(format_args!("{head}{OMITTED}{tail}"), work)
            })?;
            const DOMAIN: &[u8] = b"harness/v2/tool-output\0";
            #[cfg(feature = "filesystem")]
            let charge = acyclic_fs::WorkCounters {
                bytes_hashed: DOMAIN.len() as u64 + 16 + 4 + 8 + call_id.len() as u64,
                ..Default::default()
            };
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.preflight(charge)?;
            }
            let mut hash = blake3::Hasher::new();
            hash.update(DOMAIN);
            hash.update(&operation.into_bytes());
            hash.update(&step.to_le_bytes());
            hash.update(&(call_id.len() as u64).to_le_bytes());
            hash.update(call_id.as_bytes());
            let digest = hash.finalize();
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.charge(charge)?;
            }
            let mut id = [0; 16];
            id.copy_from_slice(&digest.as_bytes()[..16]);
            let publication = OperationId::from_bytes(id);
            let path = with_history_io(self.io, |work| {
                preparation_format(format_args!("tool-output/{publication}.json"), work)
            })?;
            self.verify_execution_owner().await?;
            let file = context
                .stage_private_content_once(
                    destination,
                    publication,
                    (&path, "application/json", "tool-output.json"),
                    &bytes,
                    io,
                )
                .await?;
            self.verify_execution_owner().await?;
            let file = with_history_io(self.io, |work| {
                if let Some(work) = work.as_deref_mut() {
                    let bytes = std::mem::size_of::<FileRef>() as u64;
                    work.retain_owned(bytes)?;
                    work.allocation_attempt(bytes)?;
                }
                Ok::<_, Error>(Box::new(file))
            })?;
            Ok(crate::model::ToolResultContent::Stored { file, preview })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    #[test]
    fn older_stored_results_demote_without_expanding_their_files() -> Result<()> {
        use crate::conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef};
        use crate::model::{ModelContentPart, ToolResultContent};
        use crate::{AgentId, resources::ProviderRef};
        let file = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
                &mut None,
            )?,
            "output.json".into(),
            "1".into(),
            FileDescriptor::from_bytes(b"{}", "application/json", &mut None)?,
            "output.json".into(),
            &mut None,
        )?;
        let mut context = crate::context::Context {
            messages: (0..3)
                .map(|index| ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: format!("call-{index}"),
                        name: "read".into(),
                        content: ToolResultContent::Stored {
                            file: Box::new(file.clone()),
                            preview: "猫".repeat(5000),
                        },
                    }),
                })
                .collect(),
            ..Default::default()
        };
        let stock = StockExecutor::new(
            Model::new("test".into(), "model".into(), "1".into(), Value::Null)?,
            Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ContextPipeline::default(),
            ToolRegistry::default(),
        );
        StockExecution::ordinary(&stock).demote_stored_tool_outputs(&mut context)?;
        for (index, message) in context.messages.iter().enumerate() {
            let ModelContent::Part(ModelContentPart::ToolResult {
                content:
                    ToolResultContent::Stored {
                        file: retained,
                        preview,
                    },
                ..
            }) = &message.content
            else {
                panic!("stored history changed shape")
            };
            assert_eq!(retained.as_ref(), &file);
            assert!(preview.len() <= [0, 2048, TOOL_OUTPUT_PREVIEW_BYTES][index]);
        }
        Ok(())
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn head_and_tail_are_utf8_and_never_exceed_the_fixed_bound(seed in ".{1,16}", repeats in 16385usize..17000) {
            let text = seed.repeat(repeats);
            let (head, tail) = preview_bounds(&text);
            prop_assert!(text.is_char_boundary(head) && text.is_char_boundary(tail));
            prop_assert!(head + OMITTED.len() + text.len() - tail <= TOOL_OUTPUT_PREVIEW_BYTES);
            prop_assert!(head > 0 && tail < text.len());
            // The original full-output representation exceeds this same bound.
            prop_assert!(text.len() > TOOL_OUTPUT_PREVIEW_BYTES);
        }
    }
}
